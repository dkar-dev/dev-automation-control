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
  "$TMP_ROOT/runtime/worktrees/demo-executor" \
  "$TMP_ROOT/runtime/worktrees/demo-reviewer" \
  "$TMP_ROOT/fakebin"

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
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Status
success
REPORT
elif printf '%s' "$PROMPT" | grep -q 'You are the reviewer'; then
  ROLE="reviewer"
  cat > "$WORKTREE/.codex-run/reviewer-report.md" <<'REPORT'
Verdict: approved
Summary: reviewer approved the synthetic run
Commit SHA: deadbeef

## Defects found
- none

## Verification performed
- synthetic smoke verification

## Risk assessment
- low

## Required fixes
- none

## Recommended next action
- finalize automatically
REPORT
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

PAYLOAD='{"project":"demo","task_text":"Smoke test real host-side runner flow.","mode":"executor+reviewer","branch_base":"main","auto_commit":false,"source":"n8n","thread_label":"smoke-test"}'

PREPARE_RESP="$(curl -sS -X POST http://127.0.0.1:18787/prepare-run -H 'Content-Type: application/json' -d "$PAYLOAD")"
EXECUTOR_RESP="$(curl -sS -X POST http://127.0.0.1:18787/run-executor -H 'Content-Type: application/json' -d '{}')"
REVIEWER_RESP="$(curl -sS -X POST http://127.0.0.1:18787/run-reviewer -H 'Content-Type: application/json' -d '{}')"
CURRENT_RESP="$(curl -sS http://127.0.0.1:18787/current-run)"

printf 'executor' > "$TMP_ROOT/fail-role"
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
  "$FAIL_RESP"
import json
import sys

prepare, executor, reviewer, current_run, prepare_fail, fail_code, fail_resp = sys.argv[1:]
prepare = json.loads(prepare)
executor = json.loads(executor)
reviewer = json.loads(reviewer)
current_run = json.loads(current_run)
prepare_fail = json.loads(prepare_fail)
fail_resp = json.loads(fail_resp)

assert prepare["ok"] is True
assert executor["ok"] is True
assert reviewer["ok"] is True
assert current_run["ok"] is True
assert prepare_fail["ok"] is True
assert executor["data"]["status"] == "executor_done", executor
assert executor["data"]["outbox"]["executor_report"], executor
assert reviewer["data"]["status"] == "completed", reviewer
assert reviewer["data"]["outbox"]["reviewer_report"], reviewer
assert reviewer["data"]["result"]["verdict"] == "approved", reviewer
assert reviewer["data"]["result"]["summary"] == "reviewer approved the synthetic run", reviewer
assert reviewer["data"]["result"]["commit_sha"] == "deadbeef", reviewer
assert current_run["data"]["status"] == "completed", current_run
assert current_run["data"]["result"]["verdict"] == "approved", current_run
assert current_run["data"]["result"]["commit_sha"] == "deadbeef", current_run
assert fail_code == "500", fail_code
assert fail_resp["ok"] is False
assert "run-executor.sh exited with code 23" in fail_resp["error"], fail_resp
assert "stdout:" in fail_resp["details"], fail_resp
assert "stderr:" in fail_resp["details"], fail_resp

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
