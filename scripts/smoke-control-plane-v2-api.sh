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
API_PID=""
API_LOG=""

cleanup() {
  local status=$?
  if [[ -n "$API_PID" ]]; then
    kill "$API_PID" >/dev/null 2>&1 || true
    wait "$API_PID" >/dev/null 2>&1 || true
  fi
  if [[ $status -ne 0 && -n "$API_LOG" && -f "$API_LOG" ]]; then
    echo "----- control plane api log -----" >&2
    cat "$API_LOG" >&2 || true
    echo "--------------------------------" >&2
  fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

mkdir -p \
  "$TMP_ROOT/projects/demo" \
  "$TMP_ROOT/runtime/worktrees" \
  "$TMP_ROOT/instructions" \
  "$TMP_ROOT/packages/demo" \
  "$TMP_ROOT/fakebin" \
  "$TMP_ROOT/artifacts" \
  "$TMP_ROOT/worker-logs"

cp -a "$CONTROL_DIR/projects/sample-project/." "$TMP_ROOT/packages/demo/"
cat > "$TMP_ROOT/packages/demo/runtime.yaml" <<'EOF'
bounded_task_runtime_v1:
  branch_base: main
  mode: executor+reviewer
  auto_commit: false
  source: api-config-source
  thread_label: api-config-thread
EOF
cat > "$TMP_ROOT/packages/demo/instructions.yaml" <<'EOF'
bounded_task_intake_v1:
  instruction_profile: default
  instruction_overlays:
    - docs-only
EOF

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
cat > "$TMP_ROOT/projects/demo/docs/control-api-smoke.md" <<'EOF'
# Control API Smoke

Initial content.
EOF
git -C "$TMP_ROOT/projects/demo" add .gitignore README.md docs/control-api-smoke.md
git -C "$TMP_ROOT/projects/demo" commit -m "Initial API smoke fixture" >/dev/null
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
mkdir -p "$TMP_ROOT/instructions/profiles/default" "$TMP_ROOT/instructions/overlays/strict-review"
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
Docs-only overlay marker.
EOF
cat > "$TMP_ROOT/instructions/overlays/strict-review/shared.md" <<'EOF'
Strict shared overlay marker.
EOF
git -C "$TMP_ROOT/instructions" add .
git -C "$TMP_ROOT/instructions" commit -m "Initial instructions fixture" >/dev/null

cat > "$TMP_ROOT/fakebin/codex" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

[ "${1:-}" = "exec" ] || exit 64
shift

WORKTREE=""
LAST_MESSAGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -C) WORKTREE="$2"; shift 2 ;;
    --output-last-message) LAST_MESSAGE="$2"; shift 2 ;;
    -s|-c) shift 2 ;;
    -) shift ;;
    *) shift ;;
  esac
done

PROMPT="$(cat)"
mkdir -p "$WORKTREE/.codex-run"

if printf '%s' "$PROMPT" | grep -q 'You are the executor'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 25
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 25
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Executor profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Strict shared overlay marker\.' <<<"$PROMPT" || exit 26
  ! grep -q 'Docs-only overlay marker\.' <<<"$PROMPT" || exit 26
  printf '\nExecutor API smoke validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor API smoke validated.\n' >> "$WORKTREE/docs/control-api-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor completed API smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/control-api-smoke.md.
REPORT
  exit 0
fi

if printf '%s' "$PROMPT" | grep -q 'You are the reviewer'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 27
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 27
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Reviewer profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Strict shared overlay marker\.' <<<"$PROMPT" || exit 28
  ! grep -q 'Docs-only overlay marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Executor API smoke validated\.' "$WORKTREE/README.md" || exit 29
  grep -q 'Executor API smoke validated\.' "$WORKTREE/docs/control-api-smoke.md" || exit 29
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Reviewer completed API smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/reviewer-report.md" <<REPORT
Verdict: approved
Summary: synthetic api reviewer summary for approved
Commit SHA: __COMMIT_SHA__
REPORT
  sed -i "s/__COMMIT_SHA__/$COMMIT_SHA/" "$WORKTREE/.codex-run/reviewer-report.md"
  exit 0
fi

exit 35
EOF
chmod +x "$TMP_ROOT/fakebin/codex"

export PATH="$TMP_ROOT/fakebin:$PATH"

DB_PATH="$TMP_ROOT/control-plane-v2.sqlite"
"$CONTROL_DIR/scripts/init-sqlite-v1" "$DB_PATH" >/dev/null
"$CONTROL_DIR/scripts/register-project-package" "$TMP_ROOT/packages/demo" --sqlite-db "$DB_PATH" >/dev/null

API_PORT="$(python3 - <<'PY'
from __future__ import annotations

import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)"

API_LOG="$TMP_ROOT/control-plane-api.log"
"$CONTROL_DIR/scripts/run-control-plane-api" \
  --sqlite-db "$DB_PATH" \
  --host 127.0.0.1 \
  --port "$API_PORT" \
  --artifact-root "$TMP_ROOT/artifacts" \
  --workspace-root "$TMP_ROOT" \
  --worker-log-root "$TMP_ROOT/worker-logs" \
  >"$API_LOG" 2>&1 &
API_PID="$!"

python3 - "$CONTROL_DIR" "$TMP_ROOT" "$API_PORT" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


control_dir = Path(sys.argv[1]).resolve()
tmp_root = Path(sys.argv[2]).resolve()
port = int(sys.argv[3])
base_url = f"http://127.0.0.1:{port}"


def request_json(
    method: str,
    path: str,
    *,
    payload: dict | None = None,
    expected_status: int = 200,
    raw_body: bytes | None = None,
    content_type: str = "application/json",
) -> dict:
    data = raw_body
    headers: dict[str, str] = {}
    if method == "POST":
        if data is None:
            data = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = content_type
    request = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            status = response.status
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8")
    payload_json = json.loads(body)
    if status != expected_status:
        raise SystemExit(
            f"Unexpected status for {method} {path}: expected {expected_status}, got {status}\n{json.dumps(payload_json, ensure_ascii=False, indent=2)}"
        )
    if "request_id" not in payload_json or not payload_json["request_id"]:
        raise SystemExit(f"Missing request_id in response for {method} {path}")
    return payload_json


for _ in range(60):
    try:
        health = request_json("GET", "/v1/health")
    except Exception:
        time.sleep(0.2)
        continue
    if health["ok"]:
        break
else:
    raise SystemExit("API health endpoint did not become ready in time")

assert health["data"]["service"] == "control-plane-v2-api", health
assert health["data"]["sqlite_db_exists"] is True, health

bad_json = request_json(
    "POST",
    "/v1/tasks/submit",
    raw_body=b"{broken",
    expected_status=400,
)
assert bad_json["ok"] is False, bad_json
assert bad_json["error"]["code"] == "INVALID_JSON_BODY", bad_json

submit_payload = {
    "project_key": "demo",
    "task_text": "Drive the bounded task through the local HTTP API.",
    "project_profile": "default",
    "workflow_id": "build",
    "milestone": "api-approved",
    "instruction_overlays": ["strict-review"],
    "source": "http-submit",
    "thread_label": "api-http-submit",
    "constraints": ["Only modify README.md and docs/control-api-smoke.md"],
    "expected_output": ["The worker tick should finish the submitted run through HTTP."],
}
submit_response = request_json("POST", "/v1/tasks/submit", payload=submit_payload)
assert submit_response["ok"] is True, submit_response
submitted_task = submit_response["data"]["submitted_task"]
run_id = submitted_task["run_details"]["run"]["id"]
assert submitted_task["runtime_context"]["workspace_root"] == str(tmp_root), submitted_task
assert submitted_task["runtime_context"]["artifact_root"] == str(tmp_root / "artifacts"), submitted_task
assert submitted_task["runtime_context"]["source"] == "http-submit", submitted_task

task_list = request_json("GET", "/v1/tasks?project_key=demo&limit=10")
assert any(item["run_id"] == run_id for item in task_list["data"]["submitted_tasks"]), task_list

task_detail = request_json("GET", f"/v1/tasks/{urllib.parse.quote(run_id)}")
runtime_context = task_detail["data"]["submitted_task"]["runtime_context_manifest"]["runtime_context"]
assert runtime_context["thread_label"] == "api-http-submit", task_detail
assert runtime_context["instruction_overlays"] == ["strict-review"], task_detail

worker_tick = request_json("POST", "/v1/worker/tick", payload={})
tick_data = worker_tick["data"]["worker_tick"]
assert tick_data["status"] == "progressed", worker_tick
assert tick_data["claimed_run_id"] == run_id, worker_tick
assert tick_data["roles_dispatched"] == ["executor", "reviewer"], worker_tick
assert tick_data["reviewer_ingestion_happened"] is True, worker_tick
assert tick_data["final_run_status"] == "completed", worker_tick

idle_loop = request_json("POST", "/v1/worker/run-until-idle", payload={"max_ticks": 5})
assert idle_loop["data"]["worker_loop"]["ended_reason"] == "idle", idle_loop

paused_submit = request_json(
    "POST",
    "/v1/tasks/submit",
    payload={
        "project_key": "demo",
        "task_text": "Pause this queued run through HTTP.",
        "project_profile": "default",
        "workflow_id": "build",
        "milestone": "api-pause",
    },
)
paused_run_id = paused_submit["data"]["submitted_task"]["run_details"]["run"]["id"]

pause_response = request_json(
    "POST",
    f"/v1/runs/{urllib.parse.quote(paused_run_id)}/pause",
    payload={"note": "api pause", "operator": "smoke-http"},
)
assert pause_response["data"]["manual_control"]["run"]["run"]["status"] == "paused", pause_response

paused_state = request_json("GET", f"/v1/runs/{urllib.parse.quote(paused_run_id)}/control-state")
assert paused_state["data"]["control_state"]["paused"] is True, paused_state
assert paused_state["data"]["control_state"]["queue_status"] == "paused", paused_state

resume_response = request_json(
    "POST",
    f"/v1/runs/{urllib.parse.quote(paused_run_id)}/resume",
    payload={"mode": "stabilize_to_green", "note": "recover", "operator": "smoke-http"},
)
assert resume_response["data"]["manual_control"]["run"]["run"]["status"] == "queued", resume_response

resumed_state = request_json("GET", f"/v1/runs/{urllib.parse.quote(paused_run_id)}/control-state")
assert resumed_state["data"]["control_state"]["paused"] is False, resumed_state
assert resumed_state["data"]["control_state"]["latest_resume_mode"] == "stabilize_to_green", resumed_state

stopped_submit = request_json(
    "POST",
    "/v1/tasks/submit",
    payload={
        "project_key": "demo",
        "task_text": "Force-stop this queued run through HTTP.",
        "project_profile": "default",
        "workflow_id": "build",
        "milestone": "api-force-stop",
    },
)
stopped_run_id = stopped_submit["data"]["submitted_task"]["run_details"]["run"]["id"]

force_stop = request_json(
    "POST",
    f"/v1/runs/{urllib.parse.quote(stopped_run_id)}/force-stop",
    payload={"note": "api force stop", "operator": "smoke-http"},
)
assert force_stop["data"]["manual_control"]["run"]["run"]["status"] == "stopped", force_stop

stopped_state = request_json("GET", f"/v1/runs/{urllib.parse.quote(stopped_run_id)}/control-state")
assert stopped_state["data"]["control_state"]["terminal"] is True, stopped_state
assert stopped_state["data"]["control_state"]["queue_status"] == "cancelled", stopped_state

cleanup_response = request_json(
    "POST",
    "/v1/cleanup/run-once",
    payload={
        "dry_run": True,
        "now": "2035-01-01T00:00:00Z",
    },
)
cleanup_pass = cleanup_response["data"]["cleanup_pass"]
assert cleanup_pass["dry_run"] is True, cleanup_response
assert cleanup_pass["summary"]["artifacts"]["processed"] > 0, cleanup_response

print(
    json.dumps(
        {
            "base_url": base_url,
            "approved_run_id": run_id,
            "paused_run_id": paused_run_id,
            "force_stopped_run_id": stopped_run_id,
            "cleanup_artifact_candidates": cleanup_pass["summary"]["artifacts"]["processed"],
            "idle_loop_ended_reason": idle_loop["data"]["worker_loop"]["ended_reason"],
        },
        ensure_ascii=False,
        indent=2,
    )
)
PY
