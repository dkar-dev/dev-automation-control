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

mkdir -p \
  "$TMP_ROOT/projects/demo" \
  "$TMP_ROOT/runtime/worktrees" \
  "$TMP_ROOT/instructions" \
  "$TMP_ROOT/packages/demo" \
  "$TMP_ROOT/fakebin" \
  "$TMP_ROOT/artifacts"

cp -a "$CONTROL_DIR/projects/sample-project/." "$TMP_ROOT/packages/demo/"

git -C "$TMP_ROOT/projects/demo" init -b main >/dev/null
git -C "$TMP_ROOT/projects/demo" config user.name "Smoke Test"
git -C "$TMP_ROOT/projects/demo" config user.email "smoke@example.com"
mkdir -p "$TMP_ROOT/projects/demo/docs"
cat > "$TMP_ROOT/projects/demo/.gitignore" <<'EOF'
.codex/
.codex-run/
EOF
cat > "$TMP_ROOT/projects/demo/README.md" <<'EOF'
# Demo Project
EOF
cat > "$TMP_ROOT/projects/demo/docs/control-pipeline-smoke.md" <<'EOF'
# Control Pipeline Smoke

Initial content.
EOF
git -C "$TMP_ROOT/projects/demo" add .gitignore README.md docs/control-pipeline-smoke.md
git -C "$TMP_ROOT/projects/demo" commit -m "Initial smoke fixture" >/dev/null
git -C "$TMP_ROOT/projects/demo" worktree add --detach "$TMP_ROOT/runtime/worktrees/demo-executor" HEAD >/dev/null
git -C "$TMP_ROOT/projects/demo" worktree add --detach "$TMP_ROOT/runtime/worktrees/demo-reviewer" HEAD >/dev/null

printf 'stale executor scratch\n' > "$TMP_ROOT/runtime/worktrees/demo-executor/stale-untracked.txt"
mkdir -p "$TMP_ROOT/runtime/worktrees/demo-executor/.codex-run"
printf 'stale executor artifact\n' > "$TMP_ROOT/runtime/worktrees/demo-executor/.codex-run/stale-before-run.md"
printf 'stale reviewer scratch\n' > "$TMP_ROOT/runtime/worktrees/demo-reviewer/stale-untracked.txt"
mkdir -p "$TMP_ROOT/runtime/worktrees/demo-reviewer/.codex-run"
printf 'stale reviewer artifact\n' > "$TMP_ROOT/runtime/worktrees/demo-reviewer/.codex-run/stale-before-run.md"

git -C "$TMP_ROOT/instructions" init -b main >/dev/null
git -C "$TMP_ROOT/instructions" config user.name "Smoke Test"
git -C "$TMP_ROOT/instructions" config user.email "smoke@example.com"
mkdir -p \
  "$TMP_ROOT/instructions/profiles/default" \
  "$TMP_ROOT/instructions/overlays/strict-review"
cat > "$TMP_ROOT/instructions/profiles/default/shared.md" <<'EOF'
Shared profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/profiles/default/executor.md" <<'EOF'
Executor profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/profiles/default/reviewer.md" <<'EOF'
Reviewer profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/overlays/docs-only.md" <<'EOF'
Docs overlay instruction marker.
EOF
cat > "$TMP_ROOT/instructions/overlays/strict-review/reviewer.md" <<'EOF'
Strict review overlay instruction marker.
EOF
git -C "$TMP_ROOT/instructions" add .
git -C "$TMP_ROOT/instructions" commit -m "Initial instructions fixture" >/dev/null

cat > "$TMP_ROOT/fakebin/codex" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

[ "${1:-}" = "exec" ] || {
  echo "unsupported command: ${1:-}" >&2
  exit 64
}
shift

WORKTREE=""
LAST_MESSAGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -C)
      WORKTREE="$2"
      shift 2
      ;;
    --output-last-message)
      LAST_MESSAGE="$2"
      shift 2
      ;;
    -s|-c)
      shift 2
      ;;
    -)
      shift
      ;;
    *)
      shift
      ;;
  esac
done

PROMPT="$(cat)"
mkdir -p "$WORKTREE/.codex-run"

if printf '%s' "$PROMPT" | grep -q 'You are the executor'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || {
    echo "executor worktree still has stale untracked file" >&2
    exit 25
  }
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || {
    echo "executor worktree still has stale .codex-run artifact" >&2
    exit 25
  }
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Executor profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Docs overlay instruction marker\.' <<<"$PROMPT" || exit 26
  if grep -q 'Strict review overlay instruction marker\.' <<<"$PROMPT"; then
    echo "executor prompt unexpectedly includes reviewer-only overlay instructions" >&2
    exit 26
  fi
  printf '\nExecutor smoke handoff validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor smoke handoff validated.\n' >> "$WORKTREE/docs/control-pipeline-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor completed dispatch smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/control-pipeline-smoke.md.

## Files changed
- README.md
- docs/control-pipeline-smoke.md

## Commands run
- synthetic dispatch smoke executor

## Verification results
- synthetic dispatch smoke verification

## Open issues
- none

## Recommended next action
- reviewer should validate the handoff commit
REPORT
  exit 0
fi

if printf '%s' "$PROMPT" | grep -q 'You are the reviewer'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || {
    echo "reviewer worktree still has stale untracked file" >&2
    exit 27
  }
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || {
    echo "reviewer worktree still has stale .codex-run artifact" >&2
    exit 27
  }
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Reviewer profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Docs overlay instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Strict review overlay instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Executor smoke handoff validated\.' "$WORKTREE/README.md" || exit 29
  grep -q 'Executor smoke handoff validated\.' "$WORKTREE/docs/control-pipeline-smoke.md" || exit 29
  [[ "$(git -C "$WORKTREE" rev-parse HEAD^)" = "$(git -C "$WORKTREE" rev-parse main)" ]] || {
    echo "handoff commit parent does not match branch_base" >&2
    exit 30
  }
  mapfile -t COMMITTED_FILES < <(git -C "$WORKTREE" diff-tree --no-commit-id --name-only -r HEAD | sort)
  [[ "${#COMMITTED_FILES[@]}" -eq 2 ]] || exit 31
  [[ "${COMMITTED_FILES[0]}" = "README.md" ]] || exit 31
  [[ "${COMMITTED_FILES[1]}" = "docs/control-pipeline-smoke.md" ]] || exit 31
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Reviewer completed dispatch smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/reviewer-report.md" <<'REPORT'
Verdict: changes_requested
Summary: reviewer requests one synthetic follow-up cycle
Commit SHA: __COMMIT_SHA__

## Defects found
- synthetic follow-up request for v2 dispatch smoke

## Verification performed
- reviewer observed committed README.md and docs/control-pipeline-smoke.md changes

## Risk assessment
- low

## Required fixes
- synthetic follow-up cycle

## Recommended next action
- complete reviewer outcome separately
REPORT
  sed -i "s/__COMMIT_SHA__/$COMMIT_SHA/" "$WORKTREE/.codex-run/reviewer-report.md"
  exit 0
fi

echo "unexpected prompt role" >&2
exit 32
EOF
chmod +x "$TMP_ROOT/fakebin/codex"

export PATH="$TMP_ROOT/fakebin:$PATH"

python3 - "$CONTROL_DIR" "$TMP_ROOT" <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys


control_dir = Path(sys.argv[1]).resolve()
tmp_root = Path(sys.argv[2]).resolve()
db_path = tmp_root / "control-plane-v2.sqlite"
artifact_root = tmp_root / "artifacts"
context_path = tmp_root / "dispatch-context.json"
claim_path = tmp_root / "claim.json"

scripts = {
    "init": control_dir / "scripts" / "init-sqlite-v1",
    "register": control_dir / "scripts" / "register-project-package",
    "create": control_dir / "scripts" / "create-root-run",
    "claim": control_dir / "scripts" / "claim-next-run",
    "dispatch_next": control_dir / "scripts" / "dispatch-next-for-claimed-run",
    "dispatch_executor": control_dir / "scripts" / "dispatch-executor-run",
    "complete_reviewer": control_dir / "scripts" / "complete-reviewer-outcome",
}

base_env = os.environ.copy()


def run_command(*args: object, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(arg) for arg in args],
        cwd=control_dir,
        text=True,
        capture_output=True,
        env=base_env,
    )
    if expect_success and proc.returncode != 0:
        raise SystemExit(
            f"Command failed unexpectedly: {' '.join(str(arg) for arg in args)}\n"
            f"stdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        )
    if not expect_success and proc.returncode == 0:
        raise SystemExit(
            f"Command was expected to fail but succeeded: {' '.join(str(arg) for arg in args)}\n"
            f"stdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        )
    return proc


dispatch_context = {
    "project": "demo",
    "task_text": "Smoke-test the v2 dispatch adapter using the real executor/reviewer backend scripts.",
    "mode": "executor+reviewer",
    "branch_base": "main",
    "auto_commit": False,
    "source": "dispatch-smoke",
    "thread_label": "dispatch-smoke",
    "constraints": [
        "Only modify README.md and docs/control-pipeline-smoke.md",
        "Do not change package files or source code",
    ],
    "expected_output": [
        "README.md updated in executor handoff commit",
        "docs/control-pipeline-smoke.md updated in executor handoff commit",
        "reviewer requests one synthetic follow-up cycle",
    ],
    "project_repo_path": str(tmp_root / "projects" / "demo"),
    "executor_worktree_path": str(tmp_root / "runtime" / "worktrees" / "demo-executor"),
    "reviewer_worktree_path": str(tmp_root / "runtime" / "worktrees" / "demo-reviewer"),
    "instruction_profile": "default",
    "instruction_overlays": ["docs-only", "strict-review"],
    "instructions_repo_path": str(tmp_root / "instructions"),
}
context_path.write_text(json.dumps(dispatch_context, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

run_command(scripts["init"], db_path, "--json")
run_command(
    scripts["register"],
    tmp_root / "packages" / "demo",
    "--sqlite-db",
    db_path,
    "--json",
)
create_payload = json.loads(
    run_command(
        scripts["create"],
        "--sqlite-db",
        db_path,
        "--project-key",
        "demo",
        "--project-profile",
        "default",
        "--workflow-id",
        "build",
        "--milestone",
        "dispatch-smoke",
        "--artifact-root",
        artifact_root,
        "--json",
    ).stdout
)
run_id = create_payload["run_details"]["run"]["id"]
claim_payload = json.loads(run_command(scripts["claim"], "--sqlite-db", db_path, "--json").stdout)
claim_path.write_text(json.dumps(claim_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
claimed_run_id = claim_payload["claim"]["dispatch_run"]["run"]["id"]
assert claimed_run_id == run_id, claim_payload

executor_dispatch = json.loads(
    run_command(
        scripts["dispatch_next"],
        "--sqlite-db",
        db_path,
        "--claim-json",
        claim_path,
        "--context-json",
        context_path,
        "--artifact-root",
        artifact_root,
        "--json",
    ).stdout
)
executor_result = executor_dispatch["dispatch"]
assert executor_result["role_decision"]["resolved_role"] == "executor", executor_result
assert executor_result["technical_success"] is True, executor_result
assert executor_result["step_run"]["step_run"]["status"] == "succeeded", executor_result
executor_step_run_id = executor_result["step_run"]["step_run"]["id"]
executor_artifact_kinds = {artifact["artifact_kind"] for artifact in executor_result["artifacts"]}
for kind in {
    "dispatch_context_manifest",
    "dispatch_result_manifest",
    "resolved_context_manifest",
    "stdout_log",
    "stderr_log",
    "step_report",
    "prompt_copy",
    "step_state_json",
}:
    assert kind in executor_artifact_kinds, executor_result
for artifact in executor_result["artifacts"]:
    assert Path(artifact["filesystem_path"]).exists(), artifact

reviewer_dispatch = json.loads(
    run_command(
        scripts["dispatch_next"],
        "--sqlite-db",
        db_path,
        "--run-id",
        run_id,
        "--artifact-root",
        artifact_root,
        "--json",
    ).stdout
)
reviewer_result = reviewer_dispatch["dispatch"]
assert reviewer_result["role_decision"]["resolved_role"] == "reviewer", reviewer_result
assert reviewer_result["technical_success"] is True, reviewer_result
assert reviewer_result["step_run"]["step_run"]["status"] == "succeeded", reviewer_result
reviewer_step_run_id = reviewer_result["step_run"]["step_run"]["id"]
reviewer_artifact_kinds = {artifact["artifact_kind"] for artifact in reviewer_result["artifacts"]}
for kind in {
    "dispatch_context_manifest",
    "dispatch_result_manifest",
    "resolved_context_manifest",
    "stdout_log",
    "stderr_log",
    "step_report",
    "prompt_copy",
    "step_state_json",
}:
    assert kind in reviewer_artifact_kinds, reviewer_result
for artifact in reviewer_result["artifacts"]:
    assert Path(artifact["filesystem_path"]).exists(), artifact

reviewer_outcome = json.loads(
    run_command(
        scripts["complete_reviewer"],
        "--sqlite-db",
        db_path,
        reviewer_step_run_id,
        "--verdict",
        "changes_requested",
        "--summary",
        "dispatch smoke requests one follow-up cycle",
        "--json",
    ).stdout
)
follow_up_run_id = reviewer_outcome["reviewer_outcome"]["flow_summary"]["created_follow_up_run_id"]
assert follow_up_run_id, reviewer_outcome
follow_up_claim = json.loads(run_command(scripts["claim"], "--sqlite-db", db_path, "--json").stdout)
assert follow_up_claim["claim"]["dispatch_run"]["run"]["id"] == follow_up_run_id, follow_up_claim

broken_run = json.loads(
    run_command(
        scripts["create"],
        "--sqlite-db",
        db_path,
        "--project-key",
        "demo",
        "--project-profile",
        "default",
        "--workflow-id",
        "build",
        "--milestone",
        "dispatch-broken",
        "--artifact-root",
        artifact_root,
        "--json",
    ).stdout
)["run_details"]["run"]["id"]
broken_claim = json.loads(run_command(scripts["claim"], "--sqlite-db", db_path, "--json").stdout)
assert broken_claim["claim"]["dispatch_run"]["run"]["id"] == broken_run, broken_claim
broken_dispatch_proc = run_command(
    scripts["dispatch_executor"],
    "--sqlite-db",
    db_path,
    "--run-id",
    broken_run,
    "--context-json",
    context_path,
    "--artifact-root",
    artifact_root,
    "--executor-runner",
    tmp_root / "missing-runner.sh",
    "--json",
    expect_success=False,
)
assert broken_dispatch_proc.stdout, broken_dispatch_proc
broken_dispatch = json.loads(broken_dispatch_proc.stdout)
broken_result = broken_dispatch["dispatch"]
assert broken_result["technical_success"] is False, broken_result
assert broken_result["step_run"] is None, broken_result
assert broken_result["queue_requeue"] is not None, broken_result
assert broken_result["queue_requeue"]["dispatch_run"]["queue_item"]["status"] == "queued", broken_result

reclaimed_broken = json.loads(run_command(scripts["claim"], "--sqlite-db", db_path, "--json").stdout)
assert reclaimed_broken["claim"]["dispatch_run"]["run"]["id"] == broken_run, reclaimed_broken

conn = sqlite3.connect(db_path)
try:
    executor_artifact_count = conn.execute(
        "SELECT COUNT(*) FROM artifact_refs WHERE step_run_id = ?",
        (executor_step_run_id,),
    ).fetchone()[0]
    reviewer_artifact_count = conn.execute(
        "SELECT COUNT(*) FROM artifact_refs WHERE step_run_id = ?",
        (reviewer_step_run_id,),
    ).fetchone()[0]
    broken_artifact_count = conn.execute(
        "SELECT COUNT(*) FROM artifact_refs WHERE run_id = ? AND step_run_id IS NULL",
        (broken_run,),
    ).fetchone()[0]
finally:
    conn.close()

assert executor_artifact_count >= 6, executor_artifact_count
assert reviewer_artifact_count >= 6, reviewer_artifact_count
assert broken_artifact_count >= 2, broken_artifact_count

print(json.dumps({
    "executor_step_run_id": executor_step_run_id,
    "reviewer_step_run_id": reviewer_step_run_id,
    "follow_up_run_id": follow_up_run_id,
    "broken_run_id": broken_run,
    "executor_artifact_count": executor_artifact_count,
    "reviewer_artifact_count": reviewer_artifact_count,
    "broken_artifact_count": broken_artifact_count,
}, ensure_ascii=False, indent=2))
PY
