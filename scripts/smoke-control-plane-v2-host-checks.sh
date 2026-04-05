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

require_cmd python3
require_cmd git

python3 - "$CONTROL_DIR" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


control_dir = Path(sys.argv[1]).resolve()
sample_project = control_dir / "projects" / "sample-project"
init_sqlite_script = control_dir / "scripts" / "init-sqlite-v1"
register_project_script = control_dir / "scripts" / "register-project-package"
create_root_run_script = control_dir / "scripts" / "create-root-run"
run_host_checks_script = control_dir / "scripts" / "run-host-checks"
show_host_check_results_script = control_dir / "scripts" / "show-host-check-results"
list_host_checks_script = control_dir / "scripts" / "list-host-checks"
run_api_script = control_dir / "scripts" / "run-control-plane-api"


def run_command(*args: object, expect_success: bool = True, expected_returncode: int | None = None) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(arg) for arg in args],
        text=True,
        capture_output=True,
        cwd=control_dir,
    )
    if expected_returncode is not None:
        if proc.returncode != expected_returncode:
            raise SystemExit(
                f"Unexpected return code for {' '.join(str(arg) for arg in args)}: {proc.returncode} != {expected_returncode}\n"
                f"stdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
            )
        return proc
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


def load_stdout_payload(proc: subprocess.CompletedProcess[str]) -> dict:
    return json.loads(proc.stdout)


def find_open_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request_json(base_url: str, method: str, path: str, payload: dict | None = None, expected_status: int = 200) -> dict:
    headers: dict[str, str] = {}
    data: bytes | None = None
    if method == "POST":
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            status = response.status
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8")
    payload_obj = json.loads(body)
    if status != expected_status:
        raise SystemExit(
            f"Unexpected HTTP status for {method} {path}: expected {expected_status}, got {status}\n"
            f"{json.dumps(payload_obj, ensure_ascii=False, indent=2)}"
        )
    return payload_obj


def wait_for_api(base_url: str, timeout_seconds: float = 10.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            payload = request_json(base_url, "GET", "/v1/health", expected_status=200)
        except Exception:
            time.sleep(0.1)
            continue
        if payload.get("ok") is True:
            return
    raise SystemExit(f"Timed out waiting for API at {base_url}")


def create_run(db_path: Path, project_key: str, artifact_root: Path, milestone: str) -> dict:
    payload = load_stdout_payload(
        run_command(
            create_root_run_script,
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
        )
    )
    return payload["run_details"]["run"]


with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_root = Path(tmp_dir)
    package_root = tmp_root / "packages" / "demo"
    project_repo = tmp_root / "workspace" / "projects" / "demo"
    artifacts_root = tmp_root / "artifacts"
    api_log_path = tmp_root / "api.log"
    db_path = tmp_root / "control-plane-v2.sqlite"
    runtime_context_path = tmp_root / "runtime-context.json"

    package_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(sample_project, package_root)
    package_root.joinpath("runtime.yaml").write_text(
        """bounded_task_runtime_v1:
  branch_base: main
  mode: executor+reviewer
  auto_commit: false

host_checks_v1:
  checks:
    - id: required_command_pass
      kind: command_check
      enabled: true
      severity: required
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      command: "python3 -c 'print(\\"host-check-pass\\")'"
      timeout_seconds: 5
      success:
        exit_code: 0
        stdout_contains: host-check-pass

    - id: required_command_fail
      kind: command_check
      enabled: true
      severity: required
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      command: "python3 -c 'import sys; sys.exit(7)'"
      timeout_seconds: 5
      success:
        exit_code: 0

    - id: advisory_missing_file
      kind: file_check
      enabled: true
      severity: advisory
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      path: "{{project_repo_path}}/missing-advisory.txt"
      timeout_seconds: 5
      success:
        exists: true

    - id: required_timeout
      kind: command_check
      enabled: true
      severity: required
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      command: "python3 -c 'import time; time.sleep(2)'"
      timeout_seconds: 1
      success:
        exit_code: 0

    - id: required_http_ok
      kind: http_check
      enabled: true
      severity: required
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      url: "http://127.0.0.1:{{smoke_http_port}}/health"
      timeout_seconds: 5
      success:
        status_code: 200
        body_contains: ok

    - id: required_process_ok
      kind: process_check
      enabled: true
      severity: required
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      process_selector: "{{smoke_process_marker}}"
      timeout_seconds: 5
      success:
        min_matches: 1
""",
        encoding="utf-8",
    )

    project_repo.mkdir(parents=True, exist_ok=True)
    run_command("git", "-C", project_repo, "init", "-b", "main")
    run_command("git", "-C", project_repo, "config", "user.name", "Smoke Test")
    run_command("git", "-C", project_repo, "config", "user.email", "smoke@example.com")
    project_repo.joinpath("README.md").write_text("# Demo project\n", encoding="utf-8")
    run_command("git", "-C", project_repo, "add", "README.md")
    run_command("git", "-C", project_repo, "commit", "-m", "Initial demo project")

    http_port = find_open_port()
    http_server = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            """
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return

HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
""",
            str(http_port),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    process_marker = "host-check-marker-12345"
    marker_process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(120)", process_marker],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    api_process: subprocess.Popen[str] | None = None

    try:
        runtime_context = {
            "project_repo_path": str(project_repo),
            "smoke_http_port": http_port,
            "smoke_process_marker": process_marker,
        }
        runtime_context_path.write_text(json.dumps(runtime_context, ensure_ascii=False, indent=2), encoding="utf-8")

        load_stdout_payload(run_command(init_sqlite_script, db_path, "--json"))
        load_stdout_payload(run_command(register_project_script, package_root, "--sqlite-db", db_path, "--json"))

        selection_run = create_run(db_path, "demo", artifacts_root, "host-check-selection")
        selection_payload = load_stdout_payload(
            run_command(
                list_host_checks_script,
                "--sqlite-db",
                db_path,
                "--run-id",
                selection_run["id"],
                "--json",
            )
        )
        assert selection_payload["host_check_selection"]["config_block_present"] is True, selection_payload
        assert len(selection_payload["host_check_selection"]["selected_checks"]) == 6, selection_payload

        pass_run = create_run(db_path, "demo", artifacts_root, "required-command-pass")
        pass_payload = load_stdout_payload(
            run_command(
                run_host_checks_script,
                "--sqlite-db",
                db_path,
                "--run-id",
                pass_run["id"],
                "--runtime-context-json",
                runtime_context_path,
                "--check-id",
                "required_command_pass",
                "--json",
            )
        )
        assert pass_payload["host_checks"]["verdict"] == "green", pass_payload

        fail_run = create_run(db_path, "demo", artifacts_root, "required-command-fail")
        fail_proc = run_command(
            run_host_checks_script,
            "--sqlite-db",
            db_path,
            "--run-id",
            fail_run["id"],
            "--runtime-context-json",
            runtime_context_path,
            "--check-id",
            "required_command_fail",
            "--json",
            expected_returncode=1,
        )
        fail_payload = load_stdout_payload(fail_proc)
        assert fail_payload["host_checks"]["verdict"] == "not_green", fail_payload
        assert fail_payload["host_checks"]["check_results"][0]["status"] == "failed", fail_payload

        advisory_run = create_run(db_path, "demo", artifacts_root, "advisory-does-not-block")
        advisory_payload = load_stdout_payload(
            run_command(
                run_host_checks_script,
                "--sqlite-db",
                db_path,
                "--run-id",
                advisory_run["id"],
                "--runtime-context-json",
                runtime_context_path,
                "--check-id",
                "required_command_pass",
                "--check-id",
                "required_http_ok",
                "--check-id",
                "required_process_ok",
                "--check-id",
                "advisory_missing_file",
                "--json",
            )
        )
        assert advisory_payload["host_checks"]["verdict"] == "green", advisory_payload
        assert advisory_payload["host_checks"]["summary"]["advisory_failed"] == 1, advisory_payload

        timeout_run = create_run(db_path, "demo", artifacts_root, "timeout")
        timeout_proc = run_command(
            run_host_checks_script,
            "--sqlite-db",
            db_path,
            "--run-id",
            timeout_run["id"],
            "--runtime-context-json",
            runtime_context_path,
            "--check-id",
            "required_timeout",
            "--json",
            expected_returncode=1,
        )
        timeout_payload = load_stdout_payload(timeout_proc)
        assert timeout_payload["host_checks"]["verdict"] == "not_green", timeout_payload
        assert timeout_payload["host_checks"]["check_results"][0]["observed"]["timed_out"] is True, timeout_payload

        api_port = find_open_port()
        api_log = api_log_path.open("w", encoding="utf-8")
        api_process = subprocess.Popen(
            [
                str(run_api_script),
                "--sqlite-db",
                str(db_path),
                "--host",
                "127.0.0.1",
                "--port",
                str(api_port),
                "--artifact-root",
                str(artifacts_root),
            ],
            cwd=control_dir,
            stdout=api_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        base_url = f"http://127.0.0.1:{api_port}"
        wait_for_api(base_url)

        http_run = create_run(db_path, "demo", artifacts_root, "http-roundtrip")
        http_run_payload = request_json(
            base_url,
            "POST",
            "/v1/checks/run",
            payload={
                "run_id": http_run["id"],
                "runtime_context": runtime_context,
                "check_ids": ["required_command_pass"],
            },
        )
        assert http_run_payload["ok"] is True, http_run_payload
        assert http_run_payload["data"]["host_checks"]["verdict"] == "green", http_run_payload

        cli_show_payload = load_stdout_payload(
            run_command(
                show_host_check_results_script,
                "--sqlite-db",
                db_path,
                http_run["id"],
                "--json",
            )
        )
        assert cli_show_payload["host_check_results"]["latest_result"]["verdict"] == "green", cli_show_payload

        http_history_payload = request_json(base_url, "GET", f"/v1/checks/{http_run['id']}")
        assert http_history_payload["ok"] is True, http_history_payload
        assert http_history_payload["data"]["host_check_results"]["latest_result"]["verdict"] == "green", http_history_payload
        assert len(http_history_payload["data"]["host_check_results"]["history"]) == 1, http_history_payload

        with sqlite3.connect(db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM host_check_runs").fetchone()[0]
            assert count == 5, count
            manifest_count = conn.execute(
                "SELECT COUNT(*) FROM artifact_refs WHERE artifact_kind = 'host_check_manifest'"
            ).fetchone()[0]
            assert manifest_count == 5, manifest_count
    finally:
        if api_process is not None:
            api_process.terminate()
            api_process.wait(timeout=10)
        marker_process.terminate()
        marker_process.wait(timeout=10)
        http_server.terminate()
        http_server.wait(timeout=10)

print("host checks smoke passed")
PY
