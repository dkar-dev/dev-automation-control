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

require_cmd curl
require_cmd git
require_cmd python3
require_cmd mktemp

TMP_ROOT="$(mktemp -d)"
BRIDGE_PID=""

cleanup() {
  if [[ -n "$BRIDGE_PID" ]]; then
    kill "$BRIDGE_PID" >/dev/null 2>&1 || true
    wait "$BRIDGE_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

TMP_CONTROL="$TMP_ROOT/control"
cp -a "$CONTROL_DIR" "$TMP_CONTROL"
rm -rf "$TMP_CONTROL/.git" "$TMP_CONTROL/bridge/__pycache__"

mkdir -p \
  "$TMP_ROOT/projects/demo" \
  "$TMP_ROOT/instructions" \
  "$TMP_ROOT/runtime/worktrees" \
  "$TMP_ROOT/fakebin"

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
INSTRUCTIONS_REV="$(git -C "$TMP_ROOT/instructions" rev-parse HEAD)"

printf '\nStale executor branch state.\n' >> "$TMP_ROOT/runtime/worktrees/demo-executor/README.md"
git -C "$TMP_ROOT/runtime/worktrees/demo-executor" add README.md
git -C "$TMP_ROOT/runtime/worktrees/demo-executor" commit -m "Stale executor state" >/dev/null
printf 'stale scratch file\n' > "$TMP_ROOT/runtime/worktrees/demo-executor/stale-untracked.txt"
mkdir -p "$TMP_ROOT/runtime/worktrees/demo-executor/.codex-run"
printf 'stale executor artifact\n' > "$TMP_ROOT/runtime/worktrees/demo-executor/.codex-run/stale-before-run.md"

printf 'stale reviewer scratch file\n' > "$TMP_ROOT/runtime/worktrees/demo-reviewer/stale-untracked.txt"
mkdir -p "$TMP_ROOT/runtime/worktrees/demo-reviewer/.codex-run"
printf 'stale reviewer artifact\n' > "$TMP_ROOT/runtime/worktrees/demo-reviewer/.codex-run/stale-before-run.md"

cat > "$TMP_ROOT/fakebin/codex" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="${FAKE_CODEX_STATE_DIR:?}"
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
printf 'Synthetic final message\n' > "$LAST_MESSAGE"

ROLE="unknown"
if printf '%s' "$PROMPT" | grep -q 'You are the executor'; then
  ROLE="executor"
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || {
    echo "executor prompt is missing shared profile instructions" >&2
    exit 24
  }
  grep -q 'Executor profile instruction marker\.' <<<"$PROMPT" || {
    echo "executor prompt is missing executor profile instructions" >&2
    exit 24
  }
  grep -q 'Docs overlay instruction marker\.' <<<"$PROMPT" || {
    echo "executor prompt is missing overlay instructions" >&2
    exit 24
  }
  if grep -q 'Strict review overlay instruction marker\.' <<<"$PROMPT"; then
    echo "executor prompt unexpectedly includes reviewer-only overlay instructions" >&2
    exit 24
  fi
  printf '\nExecutor smoke handoff validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor smoke handoff validated.\n' >> "$WORKTREE/docs/control-pipeline-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor finished, but local handoff commit creation was blocked.
Git metadata appears read-only in this environment.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/control-pipeline-smoke.md.

## Files changed
- README.md
- docs/control-pipeline-smoke.md

## Commands run
- synthetic smoke executor

## Verification results
- synthetic smoke verification

## Open issues
- The handoff commit could not be created from the executor sandbox.
- Git metadata appears read-only in this environment.

## Recommended next action
- Create the handoff commit outside the executor sandbox.
REPORT
elif printf '%s' "$PROMPT" | grep -q 'You are the reviewer'; then
  ROLE="reviewer"
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || {
    echo "reviewer prompt is missing shared profile instructions" >&2
    exit 24
  }
  grep -q 'Reviewer profile instruction marker\.' <<<"$PROMPT" || {
    echo "reviewer prompt is missing reviewer profile instructions" >&2
    exit 24
  }
  grep -q 'Docs overlay instruction marker\.' <<<"$PROMPT" || {
    echo "reviewer prompt is missing shared overlay instructions" >&2
    exit 24
  }
  grep -q 'Strict review overlay instruction marker\.' <<<"$PROMPT" || {
    echo "reviewer prompt is missing reviewer overlay instructions" >&2
    exit 24
  }
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || {
    echo "reviewer worktree still has stale untracked file" >&2
    exit 24
  }
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || {
    echo "reviewer worktree still has stale .codex-run artifact" >&2
    exit 24
  }
  grep -q 'Executor smoke handoff validated\.' "$WORKTREE/README.md" || {
    echo "reviewer cannot see README handoff change" >&2
    exit 24
  }
  grep -q 'Executor smoke handoff validated\.' "$WORKTREE/docs/control-pipeline-smoke.md" || {
    echo "reviewer cannot see docs handoff change" >&2
    exit 24
  }
  [[ "$(git -C "$WORKTREE" rev-parse HEAD^)" = "$(git -C "$WORKTREE" rev-parse main)" ]] || {
    echo "handoff commit parent does not match branch_base" >&2
    exit 24
  }
  mapfile -t COMMITTED_FILES < <(git -C "$WORKTREE" diff-tree --no-commit-id --name-only -r HEAD | sort)
  [[ "${#COMMITTED_FILES[@]}" -eq 2 ]] || {
    echo "handoff commit includes unexpected files" >&2
    printf '%s\n' "${COMMITTED_FILES[@]}" >&2
    exit 24
  }
  [[ "${COMMITTED_FILES[0]}" = "README.md" ]] || {
    echo "handoff commit is missing README.md" >&2
    exit 24
  }
  [[ "${COMMITTED_FILES[1]}" = "docs/control-pipeline-smoke.md" ]] || {
    echo "handoff commit is missing docs/control-pipeline-smoke.md" >&2
    exit 24
  }
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  cat > "$WORKTREE/.codex-run/reviewer-report.md" <<'REPORT'
Verdict: approved
Summary: reviewer approved the synthetic run
Commit SHA: __COMMIT_SHA__

## Defects found
- none

## Verification performed
- synthetic smoke verification
- reviewer observed committed README.md and docs/control-pipeline-smoke.md changes

## Risk assessment
- low

## Required fixes
- none

## Recommended next action
- finalize automatically
REPORT
  sed -i "s/__COMMIT_SHA__/$COMMIT_SHA/" "$WORKTREE/.codex-run/reviewer-report.md"
fi

if [[ -f "$STATE_DIR/fail-role" ]] && [[ "$(cat "$STATE_DIR/fail-role")" = "$ROLE" ]]; then
  echo "simulated $ROLE stdout"
  echo "simulated $ROLE stderr" >&2
  exit 23
fi

echo "synthetic $ROLE run complete"
EOF
chmod +x "$TMP_ROOT/fakebin/codex"

PATH="$TMP_ROOT/fakebin:$PATH" \
FAKE_CODEX_STATE_DIR="$TMP_ROOT" \
python3 "$TMP_CONTROL/bridge/http_bridge.py" --host 127.0.0.1 --port 18787 \
  > "$TMP_ROOT/bridge.log" 2>&1 &
BRIDGE_PID=$!

for _ in 1 2 3 4 5 6 7 8 9 10; do
  if curl -fsS http://127.0.0.1:18787/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 0.3
done

PAYLOAD="$(cat <<EOF
{"project":"demo","task_text":"Smoke test real host-side runner flow.","mode":"executor+reviewer","branch_base":"main","auto_commit":false,"source":"n8n","thread_label":"smoke-test","instruction_profile":"default","instruction_overlays":["docs-only","strict-review"],"instructions_repo_path":"$TMP_ROOT/instructions"}
EOF
)"

PREPARE_RESP="$(curl -sS -X POST http://127.0.0.1:18787/prepare-run -H 'Content-Type: application/json' -d "$PAYLOAD")"
EXECUTOR_RESP="$(curl -sS -X POST http://127.0.0.1:18787/run-executor -H 'Content-Type: application/json' -d '{}')"
REVIEWER_RESP="$(curl -sS -X POST http://127.0.0.1:18787/run-reviewer -H 'Content-Type: application/json' -d '{}')"
CURRENT_RESP="$(curl -sS http://127.0.0.1:18787/current-run)"

printf 'executor' > "$TMP_ROOT/fail-role"
sleep 1
PREPARE_FAIL_RESP="$(curl -sS -X POST http://127.0.0.1:18787/prepare-run -H 'Content-Type: application/json' -d "$PAYLOAD")"
FAIL_BODY="$(mktemp)"
FAIL_CODE="$(curl -sS -o "$FAIL_BODY" -w '%{http_code}' -X POST http://127.0.0.1:18787/run-executor -H 'Content-Type: application/json' -d '{}')"
FAIL_RESP="$(cat "$FAIL_BODY")"
rm -f "$FAIL_BODY"

python3 - <<'PY' \
  "$PREPARE_RESP" \
  "$EXECUTOR_RESP" \
  "$REVIEWER_RESP" \
  "$CURRENT_RESP" \
  "$PREPARE_FAIL_RESP" \
  "$FAIL_CODE" \
  "$FAIL_RESP" \
  "$TMP_ROOT/runtime/worktrees/demo-reviewer" \
  "$TMP_ROOT/instructions" \
  "$INSTRUCTIONS_REV" \
  "$TMP_ROOT/instructions/profiles/default/shared.md" \
  "$TMP_ROOT/instructions/profiles/default/executor.md" \
  "$TMP_ROOT/instructions/profiles/default/reviewer.md" \
  "$TMP_ROOT/instructions/overlays/docs-only.md" \
  "$TMP_ROOT/instructions/overlays/strict-review/reviewer.md"
import json
import re
import sys
from pathlib import Path

(
    prepare,
    executor,
    reviewer,
    current_run,
    prepare_fail,
    fail_code,
    fail_resp,
    reviewer_worktree,
    instructions_repo,
    instructions_rev,
    shared_instruction,
    executor_instruction,
    reviewer_instruction,
    docs_overlay,
    strict_review_overlay,
) = sys.argv[1:]
prepare = json.loads(prepare)
executor = json.loads(executor)
reviewer = json.loads(reviewer)
current_run = json.loads(current_run)
prepare_fail = json.loads(prepare_fail)
fail_resp = json.loads(fail_resp)
reviewer_worktree = Path(reviewer_worktree)
instructions_repo = str(Path(instructions_repo))
executor_instruction_files = {
    str(Path(shared_instruction)),
    str(Path(executor_instruction)),
    str(Path(docs_overlay)),
}
reviewer_instruction_files = executor_instruction_files | {
    str(Path(reviewer_instruction)),
    str(Path(strict_review_overlay)),
}

assert prepare["ok"] is True
assert executor["ok"] is True
assert reviewer["ok"] is True
assert current_run["ok"] is True
assert prepare_fail["ok"] is True
assert prepare["data"]["instruction_profile"] == "default", prepare
assert prepare["data"]["instruction_overlays"] == ["docs-only", "strict-review"], prepare
assert prepare["data"]["instructions_repo_path"] == instructions_repo, prepare
assert prepare["data"]["instructions_revision"] is None, prepare
assert prepare["data"]["resolved_instruction_files"] == [], prepare
assert executor["data"]["status"] == "executor_done", executor
assert executor["data"]["instruction_profile"] == "default", executor
assert executor["data"]["instruction_overlays"] == ["docs-only", "strict-review"], executor
assert executor["data"]["instructions_repo_path"] == instructions_repo, executor
assert executor["data"]["instructions_revision"] == instructions_rev, executor
assert set(executor["data"]["resolved_instruction_files"]) == executor_instruction_files, executor
assert executor["data"]["outbox"]["executor_report"], executor
assert executor["data"]["outbox"]["executor_last_message"], executor
assert "Commit SHA: " + executor["data"]["result"]["commit_sha"] in executor["data"]["outbox"]["executor_report"], executor
assert "Status: created" in executor["data"]["outbox"]["executor_report"], executor
assert "commit could not be created" not in executor["data"]["outbox"]["executor_report"].lower(), executor
assert "read-only git metadata" not in executor["data"]["outbox"]["executor_report"].lower(), executor
assert ".codex-run/executor-report.md" not in executor["data"]["outbox"]["executor_report"], executor
assert "Commit SHA: " + executor["data"]["result"]["commit_sha"] in executor["data"]["outbox"]["executor_last_message"], executor
assert "blocked" not in executor["data"]["outbox"]["executor_last_message"].lower(), executor
assert "read-only git metadata" not in executor["data"]["outbox"]["executor_last_message"].lower(), executor
assert reviewer["data"]["status"] == "completed", reviewer
assert reviewer["data"]["instructions_revision"] == instructions_rev, reviewer
assert set(reviewer["data"]["resolved_instruction_files"]) == reviewer_instruction_files, reviewer
assert reviewer["data"]["outbox"]["reviewer_report"], reviewer
assert reviewer["data"]["result"]["verdict"] == "approved", reviewer
assert reviewer["data"]["result"]["summary"] == "reviewer approved the synthetic run", reviewer
assert re.fullmatch(r"[0-9a-f]{40}", executor["data"]["result"]["commit_sha"]), executor
assert reviewer["data"]["result"]["commit_sha"] == executor["data"]["result"]["commit_sha"], reviewer
assert current_run["data"]["status"] == "completed", current_run
assert current_run["data"]["instruction_profile"] == "default", current_run
assert current_run["data"]["instruction_overlays"] == ["docs-only", "strict-review"], current_run
assert current_run["data"]["instructions_repo_path"] == instructions_repo, current_run
assert current_run["data"]["instructions_revision"] == instructions_rev, current_run
assert set(current_run["data"]["resolved_instruction_files"]) == reviewer_instruction_files, current_run
assert current_run["data"]["result"]["verdict"] == "approved", current_run
assert current_run["data"]["result"]["commit_sha"] == executor["data"]["result"]["commit_sha"], current_run
assert fail_code == "500", fail_code
assert fail_resp["ok"] is False
assert "run-executor.sh exited with code 23" in fail_resp["error"], fail_resp
assert "stdout:" in fail_resp["details"], fail_resp
assert "stderr:" in fail_resp["details"], fail_resp

readme_text = (reviewer_worktree / "README.md").read_text(encoding="utf-8")
smoke_doc_text = (reviewer_worktree / "docs" / "control-pipeline-smoke.md").read_text(encoding="utf-8")
assert "Executor smoke handoff validated." in readme_text
assert "Executor smoke handoff validated." in smoke_doc_text

print(json.dumps({
    "prepare_status": prepare["data"]["status"],
    "executor_status": executor["data"]["status"],
    "reviewer_status": reviewer["data"]["status"],
    "final_verdict": reviewer["data"]["result"]["verdict"],
    "commit_sha": reviewer["data"]["result"]["commit_sha"],
    "error_http": fail_code,
    "error_message": fail_resp["error"],
}, ensure_ascii=False, indent=2))
PY
