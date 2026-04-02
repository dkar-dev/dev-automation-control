#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

require_cmd git
require_cmd mktemp
require_cmd python3

TMP_ROOT="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

TMP_CONTROL="$TMP_ROOT/control"
TMP_PROJECT="$TMP_ROOT/projects/demo"
TMP_EXECUTOR="$TMP_ROOT/runtime/worktrees/demo-executor"
TMP_REVIEWER="$TMP_ROOT/runtime/worktrees/demo-reviewer"
TMP_INSTRUCTIONS="$TMP_ROOT/instructions"
PAYLOAD_PATH="$TMP_ROOT/payload.json"
EXECUTOR_PROMPT_PATH="$TMP_ROOT/executor-prompt.md"
REVIEWER_PROMPT_PATH="$TMP_ROOT/reviewer-prompt.md"
EXECUTOR_REPORT_INPUT="$TMP_ROOT/executor-report.md"

cp -a "$CONTROL_DIR" "$TMP_CONTROL"
rm -rf "$TMP_CONTROL/.git" "$TMP_CONTROL/bridge/__pycache__"

mkdir -p \
  "$TMP_ROOT/projects" \
  "$TMP_ROOT/runtime/worktrees" \
  "$TMP_INSTRUCTIONS"

git -C "$TMP_ROOT/projects" init -b main demo >/dev/null
git -C "$TMP_PROJECT" config user.name "Smoke Test"
git -C "$TMP_PROJECT" config user.email "smoke@example.com"
mkdir -p "$TMP_PROJECT/docs"
cat > "$TMP_PROJECT/.gitignore" <<'EOF'
.codex/
.codex-run/
EOF
cat > "$TMP_PROJECT/README.md" <<'EOF'
# Demo Project
EOF
cat > "$TMP_PROJECT/docs/control-pipeline-smoke.md" <<'EOF'
# Control Pipeline Smoke
EOF
git -C "$TMP_PROJECT" add .
git -C "$TMP_PROJECT" commit -m "Initial smoke fixture" >/dev/null
git -C "$TMP_PROJECT" worktree add --detach "$TMP_EXECUTOR" HEAD >/dev/null
git -C "$TMP_PROJECT" worktree add --detach "$TMP_REVIEWER" HEAD >/dev/null

cp -a "$CONTROL_DIR/fixtures/instructions-repo/." "$TMP_INSTRUCTIONS/"
git -C "$TMP_INSTRUCTIONS" init -b main >/dev/null
git -C "$TMP_INSTRUCTIONS" config user.name "Smoke Test"
git -C "$TMP_INSTRUCTIONS" config user.email "smoke@example.com"
git -C "$TMP_INSTRUCTIONS" add .
git -C "$TMP_INSTRUCTIONS" commit -m "Initial instructions fixture" >/dev/null
INSTRUCTIONS_REV="$(git -C "$TMP_INSTRUCTIONS" rev-parse HEAD)"

cat > "$PAYLOAD_PATH" <<EOF
{
  "project": "demo",
  "task_text": "Validate instruction resolution plumbing only.",
  "mode": "executor+reviewer",
  "branch_base": "main",
  "auto_commit": false,
  "source": "instruction-smoke",
  "thread_label": "instruction-smoke",
  "instruction_profile": "default",
  "instruction_overlays": ["docs-only", "strict-review"],
  "instructions_repo_path": "$TMP_INSTRUCTIONS"
}
EOF

"$TMP_CONTROL/scripts/prepare-run.sh" "$PAYLOAD_PATH" >/dev/null
"$TMP_CONTROL/scripts/validate-instructions-repo.sh" "$TMP_INSTRUCTIONS" default docs-only strict-review >/dev/null
"$TMP_CONTROL/scripts/resolve-instructions.sh" executor >/dev/null
"$TMP_CONTROL/scripts/resolve-instructions.sh" reviewer >/dev/null

cat > "$EXECUTOR_REPORT_INPUT" <<'EOF'
# Executor Report

Synthetic executor report for reviewer prompt assembly.
EOF

"$TMP_CONTROL/scripts/build-executor-prompt.sh" "$EXECUTOR_PROMPT_PATH"
"$TMP_CONTROL/scripts/build-reviewer-prompt.sh" "$REVIEWER_PROMPT_PATH" "$EXECUTOR_REPORT_INPUT"

CURRENT_RUN_JSON="$("$TMP_CONTROL/scripts/export-current-run.sh")"

python3 - <<'PY' \
  "$CURRENT_RUN_JSON" \
  "$EXECUTOR_PROMPT_PATH" \
  "$REVIEWER_PROMPT_PATH" \
  "$INSTRUCTIONS_REV" \
  "$TMP_INSTRUCTIONS" \
  "$TMP_INSTRUCTIONS/profiles/default/shared.md" \
  "$TMP_INSTRUCTIONS/profiles/default/executor.md" \
  "$TMP_INSTRUCTIONS/profiles/default/reviewer.md" \
  "$TMP_INSTRUCTIONS/overlays/docs-only.md" \
  "$TMP_INSTRUCTIONS/overlays/strict-review/shared.md" \
  "$TMP_INSTRUCTIONS/overlays/strict-review/reviewer.md"
import json
import sys
from pathlib import Path

(
    current_run_json,
    executor_prompt_path,
    reviewer_prompt_path,
    instructions_rev,
    instructions_repo,
    shared_instruction,
    executor_instruction,
    reviewer_instruction,
    docs_overlay,
    strict_review_shared,
    strict_review_reviewer,
) = sys.argv[1:]

current_run = json.loads(current_run_json)
executor_prompt = Path(executor_prompt_path).read_text(encoding="utf-8")
reviewer_prompt = Path(reviewer_prompt_path).read_text(encoding="utf-8")
instructions_repo = str(Path(instructions_repo))

executor_expected = [
    str(Path(shared_instruction)),
    str(Path(executor_instruction)),
    str(Path(docs_overlay)),
    str(Path(strict_review_shared)),
]
reviewer_expected = [
    str(Path(shared_instruction)),
    str(Path(reviewer_instruction)),
    str(Path(docs_overlay)),
    str(Path(strict_review_shared)),
    str(Path(strict_review_reviewer)),
]
expected_union = set(executor_expected + reviewer_expected)

assert current_run["instruction_profile"] == "default", current_run
assert current_run["instruction_overlays"] == ["docs-only", "strict-review"], current_run
assert current_run["instructions_repo_path"] == instructions_repo, current_run
assert current_run["instructions_revision"] == instructions_rev, current_run
assert set(current_run["resolved_instruction_files"]) == expected_union, current_run

for needle in [
    "Shared profile instruction marker.",
    "Executor profile instruction marker.",
    "Docs-only flat overlay marker.",
    "Strict-review shared overlay marker.",
]:
    assert needle in executor_prompt, needle

for needle in [
    "Shared profile instruction marker.",
    "Reviewer profile instruction marker.",
    "Docs-only flat overlay marker.",
    "Strict-review shared overlay marker.",
    "Strict-review reviewer overlay marker.",
]:
    assert needle in reviewer_prompt, needle

assert executor_prompt.index("Shared profile instruction marker.") < executor_prompt.index("Executor profile instruction marker.")
assert executor_prompt.index("Executor profile instruction marker.") < executor_prompt.index("Docs-only flat overlay marker.")
assert executor_prompt.index("Docs-only flat overlay marker.") < executor_prompt.index("Strict-review shared overlay marker.")

assert reviewer_prompt.index("Shared profile instruction marker.") < reviewer_prompt.index("Reviewer profile instruction marker.")
assert reviewer_prompt.index("Reviewer profile instruction marker.") < reviewer_prompt.index("Docs-only flat overlay marker.")
assert reviewer_prompt.index("Docs-only flat overlay marker.") < reviewer_prompt.index("Strict-review shared overlay marker.")
assert reviewer_prompt.index("Strict-review shared overlay marker.") < reviewer_prompt.index("Strict-review reviewer overlay marker.")

print(json.dumps(
    {
        "instruction_profile": current_run["instruction_profile"],
        "instruction_overlays": current_run["instruction_overlays"],
        "instructions_revision": current_run["instructions_revision"],
        "resolved_instruction_files": current_run["resolved_instruction_files"],
        "executor_prompt": str(executor_prompt_path),
        "reviewer_prompt": str(reviewer_prompt_path),
    },
    ensure_ascii=False,
    indent=2,
))
PY
