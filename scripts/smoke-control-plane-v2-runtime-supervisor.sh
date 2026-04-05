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
  "$TMP_ROOT/artifacts" \
  "$TMP_ROOT/worker-logs"

cp -a "$CONTROL_DIR/projects/sample-project/." "$TMP_ROOT/packages/demo/"
cat > "$TMP_ROOT/packages/demo/runtime.yaml" <<'EOF'
bounded_task_runtime_v1:
  branch_base: main
  mode: executor+reviewer
  auto_commit: false
  source: runtime-supervisor-smoke
  thread_label: runtime-supervisor-thread
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
cat > "$TMP_ROOT/projects/demo/docs/runtime-supervisor-smoke.md" <<'EOF'
# Runtime Supervisor Smoke

Initial content.
EOF
git -C "$TMP_ROOT/projects/demo" add .gitignore README.md docs/runtime-supervisor-smoke.md
git -C "$TMP_ROOT/projects/demo" commit -m "Initial runtime supervisor smoke fixture" >/dev/null
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
mkdir -p "$TMP_ROOT/instructions/profiles/default" "$TMP_ROOT/instructions/overlays/docs-only"
cat > "$TMP_ROOT/instructions/profiles/default/shared.md" <<'EOF'
Shared profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/profiles/default/executor.md" <<'EOF'
Executor profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/profiles/default/reviewer.md" <<'EOF'
Reviewer profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/overlays/docs-only/shared.md" <<'EOF'
Docs-only overlay marker.
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
  grep -q 'Docs-only overlay marker\.' <<<"$PROMPT" || exit 26
  printf '\nExecutor runtime supervisor smoke validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor runtime supervisor smoke validated.\n' >> "$WORKTREE/docs/runtime-supervisor-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor completed runtime supervisor smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/runtime-supervisor-smoke.md.
REPORT
  exit 0
fi

if printf '%s' "$PROMPT" | grep -q 'You are the reviewer'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 27
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 27
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Reviewer profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Docs-only overlay marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Executor runtime supervisor smoke validated\.' "$WORKTREE/README.md" || exit 29
  grep -q 'Executor runtime supervisor smoke validated\.' "$WORKTREE/docs/runtime-supervisor-smoke.md" || exit 29
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Reviewer completed runtime supervisor smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/reviewer-report.md" <<REPORT
Verdict: approved
Summary: synthetic runtime supervisor reviewer summary
Commit SHA: __COMMIT_SHA__
REPORT
  sed -i "s/__COMMIT_SHA__/$COMMIT_SHA/" "$WORKTREE/.codex-run/reviewer-report.md"
  exit 0
fi

exit 35
EOF
chmod +x "$TMP_ROOT/fakebin/codex"

export PATH="$TMP_ROOT/fakebin:$PATH"

python3 - "$CONTROL_DIR" "$TMP_ROOT" <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


control_dir = Path(sys.argv[1]).resolve()
tmp_root = Path(sys.argv[2]).resolve()
db_path = tmp_root / "control-plane-v2.sqlite"
artifact_root = tmp_root / "artifacts"
worker_log_root = tmp_root / "worker-logs"
runtime_root = tmp_root / "runtime-root"
foreground_runtime_root = tmp_root / "runtime-root-foreground"
foreground_console_log = tmp_root / "foreground-runtime.log"

scripts = {
    "init": control_dir / "scripts" / "init-sqlite-v1",
    "register": control_dir / "scripts" / "register-project-package",
    "start": control_dir / "scripts" / "start-control-plane-runtime",
    "stop": control_dir / "scripts" / "stop-control-plane-runtime",
    "restart": control_dir / "scripts" / "restart-control-plane-runtime",
    "status": control_dir / "scripts" / "status-control-plane-runtime",
    "foreground": control_dir / "scripts" / "run-control-plane-runtime-foreground",
}

base_env = os.environ.copy()


def run_command(*args: object, expect_success: bool = True, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(arg) for arg in args],
        cwd=control_dir,
        text=True,
        capture_output=True,
        env=(base_env | env) if env else base_env,
    )
    if expect_success and proc.returncode != 0:
        raise SystemExit(
            f"Command failed unexpectedly: {' '.join(str(arg) for arg in args)}\nstdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        )
    if not expect_success and proc.returncode == 0:
        raise SystemExit(
            f"Command was expected to fail but succeeded: {' '.join(str(arg) for arg in args)}\nstdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        )
    return proc


def load_json_payload(proc: subprocess.CompletedProcess[str]) -> dict:
    payload_text = proc.stdout.strip() or proc.stderr.strip()
    if not payload_text:
        raise SystemExit("Expected JSON payload but command returned no output")
    return json.loads(payload_text)


def run_json(*args: object, expect_success: bool = True) -> dict:
    return load_json_payload(run_command(*args, expect_success=expect_success))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(method: str, url: str, payload: dict | None = None) -> dict:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_health(base_url: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            payload = request_json("GET", f"{base_url}/v1/health")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.2)
            continue
        if payload["ok"] is True:
            return payload
        last_error = payload
        time.sleep(0.2)
    raise SystemExit(f"Timed out waiting for health endpoint: {last_error}")


def wait_for_run_completion(base_url: str, run_id: str, timeout: float = 45.0) -> dict:
    deadline = time.monotonic() + timeout
    last_payload = None
    while time.monotonic() < deadline:
        payload = request_json("GET", f"{base_url}/v1/tasks/{run_id}")
        last_payload = payload
        status = payload["data"]["submitted_task"]["run_details"]["run"]["status"]
        if status == "completed":
            return payload
        time.sleep(0.4)
    raise SystemExit(f"Timed out waiting for run completion: {last_payload}")


def wait_for_health_to_fail(base_url: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            request_json("GET", f"{base_url}/v1/health")
        except Exception:  # noqa: BLE001
            return
        time.sleep(0.2)
    raise SystemExit("Health endpoint still responds after runtime stop")


run_command(scripts["init"], db_path)
run_command(scripts["register"], tmp_root / "packages" / "demo", "--sqlite-db", db_path)

api_port = free_port()
start_payload = run_json(
    scripts["start"],
    "--sqlite-db",
    db_path,
    "--host",
    "127.0.0.1",
    "--port",
    api_port,
    "--artifact-root",
    artifact_root,
    "--workspace-root",
    tmp_root,
    "--worker-log-root",
    worker_log_root,
    "--worker-poll-interval-seconds",
    "0.2",
    "--max-claims-per-cycle",
    "1",
    "--runtime-root",
    runtime_root,
    "--json",
)
runtime_status = start_payload["runtime_status"]
assert runtime_status["supervisor_running"] is True, start_payload
assert runtime_status["pid"], start_payload
assert runtime_status["sqlite_db"] == str(db_path), start_payload
assert runtime_status["worker_mode"] == "executor+reviewer", start_payload

status_payload = run_json(scripts["status"], "--runtime-root", runtime_root, "--json")
assert status_payload["runtime_status"]["supervisor_running"] is True, status_payload
assert status_payload["runtime_status"]["lock_path"].endswith("supervisor.lock"), status_payload
assert status_payload["runtime_status"]["log_paths"]["event_log_path"].endswith("runtime-events.jsonl"), status_payload

second_start_payload = load_json_payload(
    run_command(
        scripts["start"],
        "--sqlite-db",
        db_path,
        "--host",
        "127.0.0.1",
        "--port",
        api_port,
        "--artifact-root",
        artifact_root,
        "--workspace-root",
        tmp_root,
        "--worker-log-root",
        worker_log_root,
        "--worker-poll-interval-seconds",
        "0.2",
        "--max-claims-per-cycle",
        "1",
        "--runtime-root",
        runtime_root,
        "--json",
        expect_success=False,
    )
)
assert second_start_payload["error"]["code"] == "RUNTIME_SUPERVISOR_ALREADY_RUNNING", second_start_payload

base_url = runtime_status["api_base_url"]
health_payload = wait_for_health(base_url)
assert health_payload["data"]["service"] == "control-plane-v2-api", health_payload

submit_payload = request_json(
    "POST",
    f"{base_url}/v1/tasks/submit",
    {
        "project_key": "demo",
        "task_text": "Implement the bounded runtime supervisor smoke task.",
        "project_profile": "default",
        "workflow_id": "build",
        "milestone": "runtime-supervisor-smoke",
        "instruction_overlays": ["docs-only"],
        "source": "runtime-supervisor-smoke",
        "thread_label": "runtime-supervisor-smoke",
    },
)
run_id = submit_payload["data"]["submitted_task"]["run_details"]["run"]["id"]
completed_payload = wait_for_run_completion(base_url, run_id)
assert completed_payload["data"]["submitted_task"]["run_details"]["run"]["status"] == "completed", completed_payload

status_after_work = run_json(scripts["status"], "--runtime-root", runtime_root, "--json")
last_cycle = status_after_work["runtime_status"]["last_worker_cycle"]
assert isinstance(last_cycle, dict), status_after_work
assert last_cycle["summary_paths"]["json_path"] is not None or last_cycle["ended_reason"] in {"idle", "exception"}, status_after_work

restart_payload = run_json(scripts["restart"], "--runtime-root", runtime_root, "--json")
assert restart_payload["restart"]["after"]["supervisor_running"] is True, restart_payload
assert restart_payload["restart"]["before"]["pid"] != restart_payload["restart"]["after"]["pid"], restart_payload
wait_for_health(restart_payload["restart"]["after"]["api_base_url"])

stop_payload = run_json(scripts["stop"], "--runtime-root", runtime_root, "--json")
assert stop_payload["runtime_status"]["supervisor_running"] is False, stop_payload
wait_for_health_to_fail(base_url)

stopped_status = run_json(scripts["status"], "--runtime-root", runtime_root, "--json")
assert stopped_status["runtime_status"]["supervisor_running"] is False, stopped_status

foreground_port = free_port()
with foreground_console_log.open("w", encoding="utf-8") as log_handle:
    foreground_proc = subprocess.Popen(
        [
            str(scripts["foreground"]),
            "--sqlite-db",
            str(db_path),
            "--host",
            "127.0.0.1",
            "--port",
            str(foreground_port),
            "--artifact-root",
            str(artifact_root),
            "--workspace-root",
            str(tmp_root),
            "--worker-log-root",
            str(worker_log_root),
            "--worker-poll-interval-seconds",
            "0.2",
            "--max-claims-per-cycle",
            "1",
            "--runtime-root",
            str(foreground_runtime_root),
        ],
        cwd=control_dir,
        env=base_env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )

foreground_base_url = f"http://127.0.0.1:{foreground_port}"
try:
    wait_for_health(foreground_base_url)
    foreground_status = run_json(scripts["status"], "--runtime-root", foreground_runtime_root, "--json")
    assert foreground_status["runtime_status"]["supervisor_running"] is True, foreground_status
finally:
    foreground_proc.terminate()
    try:
        foreground_return_code = foreground_proc.wait(timeout=20)
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"Foreground runtime did not stop after SIGTERM. Log:\n{foreground_console_log.read_text(encoding='utf-8')}") from exc

assert foreground_return_code == 0, foreground_console_log.read_text(encoding="utf-8")
foreground_final_status = run_json(scripts["status"], "--runtime-root", foreground_runtime_root, "--json")
assert foreground_final_status["runtime_status"]["supervisor_running"] is False, foreground_final_status

print(
    json.dumps(
        {
            "background_pid": runtime_status["pid"],
            "restart_pid": restart_payload["restart"]["after"]["pid"],
            "run_id": run_id,
            "runtime_root": str(runtime_root),
            "foreground_runtime_root": str(foreground_runtime_root),
            "lock_path": status_payload["runtime_status"]["lock_path"],
            "event_log_path": status_payload["runtime_status"]["log_paths"]["event_log_path"],
            "worker_cycle_ended_reason": last_cycle["ended_reason"],
            "foreground_console_log": str(foreground_console_log),
        },
        ensure_ascii=False,
        indent=2,
    )
)
PY
