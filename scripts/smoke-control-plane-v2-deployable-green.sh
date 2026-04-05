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

require_cmd mktemp
require_cmd python3

TMP_ROOT="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

python3 - "$CONTROL_DIR" "$TMP_ROOT" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from urllib import error as urllib_error
from urllib import request as urllib_request


control_dir = Path(sys.argv[1]).resolve()
tmp_root = Path(sys.argv[2]).resolve()
package_root = tmp_root / "packages" / "demo"
artifact_root = tmp_root / "artifacts"
db_path = tmp_root / "control.sqlite"

shutil.copytree(control_dir / "projects" / "sample-project", package_root)
runtime_path = package_root / "runtime.yaml"
runtime_path.write_text(
    """host_checks_v1:
  checks:
    - id: required_pass
      kind: command_check
      enabled: true
      severity: required
      command:
        - python3
        - -c
        - "import sys; sys.exit(0)"
      timeout_seconds: 5
      success:
        exit_code: 0
    - id: required_fail
      kind: command_check
      enabled: true
      severity: required
      command:
        - python3
        - -c
        - "import sys; sys.exit(1)"
      timeout_seconds: 5
      success:
        exit_code: 0
""",
    encoding="utf-8",
)

scripts = {
    "init": control_dir / "scripts" / "init-sqlite-v1",
    "register": control_dir / "scripts" / "register-project-package",
    "create": control_dir / "scripts" / "create-root-run",
    "start_step": control_dir / "scripts" / "start-step-run",
    "finish_step": control_dir / "scripts" / "finish-step-run",
    "complete_review": control_dir / "scripts" / "complete-reviewer-outcome",
    "run_checks": control_dir / "scripts" / "run-host-checks",
    "decide_green": control_dir / "scripts" / "decide-deployable-green",
    "show_green": control_dir / "scripts" / "show-deployable-green-decision",
    "run_api": control_dir / "scripts" / "run-control-plane-api",
}


def run_command(*args: object, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(arg) for arg in args],
        cwd=control_dir,
        text=True,
        capture_output=True,
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


def load_json(proc: subprocess.CompletedProcess[str]) -> dict:
    payload_text = proc.stdout.strip() or proc.stderr.strip()
    if not payload_text:
        raise SystemExit("Expected JSON payload but command returned no output")
    return json.loads(payload_text)


def run_json(*args: object, expect_success: bool = True) -> dict:
    return load_json(run_command(*args, expect_success=expect_success))


def create_reviewed_run(*, project_key: str, milestone: str, reviewer_verdict: str) -> str:
    run_payload = run_json(
        scripts["create"],
        "--sqlite-db",
        db_path,
        "--project-key",
        project_key,
        "--project-profile",
        "default",
        "--workflow-id",
        "build",
        "--milestone",
        milestone,
        "--artifact-root",
        artifact_root,
        "--json",
    )["run_details"]["run"]
    step_payload = run_json(
        scripts["start_step"],
        "--sqlite-db",
        db_path,
        "--run-id",
        run_payload["id"],
        "--step-key",
        "reviewer",
        "--json",
    )["step_run_details"]["step_run"]
    run_json(
        scripts["finish_step"],
        "--sqlite-db",
        db_path,
        step_payload["id"],
        "--status",
        "succeeded",
        "--json",
    )
    run_json(
        scripts["complete_review"],
        "--sqlite-db",
        db_path,
        step_payload["id"],
        "--verdict",
        reviewer_verdict,
        "--summary",
        f"{reviewer_verdict} for {milestone}",
        "--json",
    )
    return str(run_payload["id"])


def reserve_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    _, port = sock.getsockname()
    sock.close()
    return int(port)


def wait_for_api(base_url: str, *, timeout_seconds: float = 15.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib_request.urlopen(f"{base_url}/v1/health", timeout=1.0) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if payload.get("ok") is True:
                    return
        except Exception:
            time.sleep(0.1)
    raise SystemExit(f"API did not become ready: {base_url}")


def http_json(method: str, url: str, payload: dict | None = None) -> dict:
    request = urllib_request.Request(url, method=method)
    if payload is not None:
        request.add_header("Content-Type", "application/json")
        body = json.dumps(payload).encode("utf-8")
    else:
        body = None
    try:
        with urllib_request.urlopen(request, data=body, timeout=10.0) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        raise SystemExit(f"HTTP {exc.code} for {url}: {exc.read().decode('utf-8', errors='replace')}") from exc


run_json(scripts["init"], db_path, "--json")
registration = run_json(
    scripts["register"],
    package_root,
    "--sqlite-db",
    db_path,
    "--json",
)
project_key = registration["registration"]["project"]["project_key"]

green_run_id = create_reviewed_run(project_key=project_key, milestone="green-run", reviewer_verdict="approved")
green_checks = run_json(
    scripts["run_checks"],
    "--sqlite-db",
    db_path,
    "--run-id",
    green_run_id,
    "--check-id",
    "required_pass",
    "--artifact-root",
    artifact_root,
    "--json",
)
assert green_checks["host_checks"]["verdict"] == "green", green_checks
green_decision = run_json(
    scripts["decide_green"],
    "--sqlite-db",
    db_path,
    "--run-id",
    green_run_id,
    "--artifact-root",
    artifact_root,
    "--json",
)
assert green_decision["deployable_green_decision"]["decision_status"] == "deployable_green", green_decision

not_green_run_id = create_reviewed_run(project_key=project_key, milestone="not-green-run", reviewer_verdict="approved")
not_green_checks = run_json(
    scripts["run_checks"],
    "--sqlite-db",
    db_path,
    "--run-id",
    not_green_run_id,
    "--check-id",
    "required_fail",
    "--artifact-root",
    artifact_root,
    "--json",
    expect_success=False,
)
assert not_green_checks["host_checks"]["verdict"] == "not_green", not_green_checks
not_green_decision = run_json(
    scripts["decide_green"],
    "--sqlite-db",
    db_path,
    "--run-id",
    not_green_run_id,
    "--artifact-root",
    artifact_root,
    "--json",
    expect_success=False,
)
assert not_green_decision["deployable_green_decision"]["decision_status"] == "not_green", not_green_decision

missing_checks_run_id = create_reviewed_run(project_key=project_key, milestone="missing-checks-run", reviewer_verdict="approved")
missing_checks_decision = run_json(
    scripts["decide_green"],
    "--sqlite-db",
    db_path,
    "--run-id",
    missing_checks_run_id,
    "--artifact-root",
    artifact_root,
    "--json",
    expect_success=False,
)
assert missing_checks_decision["deployable_green_decision"]["decision_status"] == "blocked", missing_checks_decision

reviewer_blocked_run_id = create_reviewed_run(project_key=project_key, milestone="reviewer-blocked-run", reviewer_verdict="blocked")
reviewer_blocked_decision = run_json(
    scripts["decide_green"],
    "--sqlite-db",
    db_path,
    "--run-id",
    reviewer_blocked_run_id,
    "--artifact-root",
    artifact_root,
    "--json",
    expect_success=False,
)
assert reviewer_blocked_decision["deployable_green_decision"]["decision_status"] == "blocked", reviewer_blocked_decision

api_port = reserve_port()
api_process = subprocess.Popen(
    [
        str(scripts["run_api"]),
        "--sqlite-db",
        str(db_path),
        "--host",
        "127.0.0.1",
        "--port",
        str(api_port),
        "--artifact-root",
        str(artifact_root),
    ],
    cwd=control_dir,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)

try:
    base_url = f"http://127.0.0.1:{api_port}"
    wait_for_api(base_url)
    http_decision = http_json("POST", f"{base_url}/v1/green/decide", {"run_id": green_run_id})
    assert http_decision["ok"] is True, http_decision
    assert http_decision["data"]["deployable_green_decision"]["decision_status"] == "deployable_green", http_decision

    show_green = run_json(
        scripts["show_green"],
        "--sqlite-db",
        db_path,
        green_run_id,
        "--json",
    )
    history = show_green["deployable_green_decisions"]["history"]
    assert len(history) == 2, show_green
    assert show_green["deployable_green_decisions"]["latest_decision"]["decision_status"] == "deployable_green", show_green

    http_history = http_json("GET", f"{base_url}/v1/green/{green_run_id}")
    assert http_history["ok"] is True, http_history
    assert len(http_history["data"]["deployable_green_decisions"]["history"]) == 2, http_history
    assert (
        http_history["data"]["deployable_green_decisions"]["latest_decision"]["decision_status"] == "deployable_green"
    ), http_history
finally:
    api_process.terminate()
    try:
        api_process.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        api_process.kill()
        api_process.wait(timeout=10.0)

connection = sqlite3.connect(db_path)
try:
    decision_count = connection.execute("SELECT COUNT(*) FROM green_decisions").fetchone()[0]
    artifact_count = connection.execute(
        "SELECT COUNT(*) FROM artifact_refs WHERE artifact_kind = ?",
        ("deployable_green_decision_manifest",),
    ).fetchone()[0]
finally:
    connection.close()

assert decision_count == 5, decision_count
assert artifact_count == 5, artifact_count

print("deployable green smoke passed")
PY
