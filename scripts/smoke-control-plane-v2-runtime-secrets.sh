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

python3 - "$CONTROL_DIR" <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


control_dir = Path(sys.argv[1]).resolve()
sample_project = control_dir / "projects" / "sample-project"
init_sqlite_script = control_dir / "scripts" / "init-sqlite-v1"
register_project_script = control_dir / "scripts" / "register-project-package"
create_root_run_script = control_dir / "scripts" / "create-root-run"
claim_next_run_script = control_dir / "scripts" / "claim-next-run"
dispatch_executor_script = control_dir / "scripts" / "dispatch-executor-run"
run_host_checks_script = control_dir / "scripts" / "run-host-checks"
resolve_runtime_secrets_script = control_dir / "scripts" / "resolve-runtime-secrets"
check_runtime_secrets_script = control_dir / "scripts" / "check-runtime-secrets"
run_api_script = control_dir / "scripts" / "run-control-plane-api"


def run_command(
    *args: object,
    env: dict[str, str] | None = None,
    expect_success: bool = True,
    expected_returncode: int | None = None,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(arg) for arg in args],
        text=True,
        capture_output=True,
        cwd=control_dir,
        env=env,
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


def load_payload(proc: subprocess.CompletedProcess[str]) -> dict:
    stream = proc.stdout if proc.stdout.strip() else proc.stderr
    return json.loads(stream)


def find_open_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(base_url: str, method: str, path: str, payload: dict | None = None, expected_status: int = 200) -> dict:
    headers: dict[str, str] = {}
    body: bytes | None = None
    if method == "POST":
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(base_url + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            status = response.status
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read().decode("utf-8")
    payload_obj = json.loads(raw)
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


def render_runtime_yaml(host_base_url: str) -> str:
    return f"""runtime_value_refs_v1:
  values:
    dispatch_token:
      classification: secret
      required: true
      refs:
        - env:SMOKE_DISPATCH_TOKEN
      dispatch_env: SMOKE_DISPATCH_TOKEN
      description: Executor dispatch token from env
    host_check_token:
      classification: secret
      required: true
      refs:
        - file:host_check.token
      host_check_context_key: host_check_token
      description: Host-check auth token from local secrets file
    host_base_url:
      classification: plain_config
      required: true
      inline_value: "{host_base_url}"
      host_check_context_key: host_base_url
      description: Plain localhost base URL for smoke checks
    default_branch_name:
      classification: plain_config
      required: true
      inline_value: "main"
      dispatch_env: SMOKE_BRANCH_NAME
      description: Plain branch name exposed to executor env

host_checks_v1:
  checks:
    - id: secret_header_http
      kind: http_check
      enabled: true
      severity: required
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      url: "{{{{host_base_url}}}}/health"
      headers:
        Authorization: "Bearer {{{{host_check_token}}}}"
      timeout_seconds: 5
      success:
        status_code: 200
        body_contains: ok
"""


def write_fake_codex(path: Path, expected_secret: str) -> None:
    path.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail

[ "${{1:-}}" = "exec" ] || {{
  echo "unsupported command: ${{1:-}}" >&2
  exit 64
}}
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

[[ "${{SMOKE_DISPATCH_TOKEN:-}}" = "{expected_secret}" ]] || {{
  echo "missing dispatch token in executor env" >&2
  exit 70
}}
[[ "${{SMOKE_BRANCH_NAME:-}}" = "main" ]] || {{
  echo "missing plain config env export" >&2
  exit 71
}}

mkdir -p "$WORKTREE/.codex-run"
echo "dispatch secret stdout: $SMOKE_DISPATCH_TOKEN"
echo "dispatch secret stderr: $SMOKE_DISPATCH_TOKEN" >&2
printf '\\nDispatch smoke touched README.\\n' >> "$WORKTREE/README.md"
cat > "$LAST_MESSAGE" <<'EOF'
Executor saw dispatch token: {expected_secret}
EOF
cat > "$WORKTREE/.codex-run/executor-report.md" <<'EOF'
# Executor Report

## Summary
Dispatch token observed: {expected_secret}

## Files changed
- README.md
EOF
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_root = Path(tmp_dir)
    package_root = tmp_root / "packages" / "demo"
    workspace_root = tmp_root / "workspace"
    project_repo = workspace_root / "projects" / "demo"
    worktree_root = workspace_root / "runtime" / "worktrees"
    instructions_root = workspace_root / "instructions"
    runtime_root = tmp_root / "runtime-root"
    artifacts_root = tmp_root / "artifacts"
    fakebin = tmp_root / "fakebin"
    db_path = tmp_root / "control-plane-v2.sqlite"
    api_log_path = tmp_root / "api.log"

    dispatch_secret = "dispatch-secret-env"
    host_check_secret = "file-secret-token"

    package_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(sample_project, package_root)

    host_port = find_open_port()
    host_base_url = f"http://127.0.0.1:{host_port}"
    package_root.joinpath("runtime.yaml").write_text(render_runtime_yaml(host_base_url), encoding="utf-8")

    project_repo.mkdir(parents=True, exist_ok=True)
    run_command("git", "-C", project_repo, "init", "-b", "main")
    run_command("git", "-C", project_repo, "config", "user.name", "Smoke Test")
    run_command("git", "-C", project_repo, "config", "user.email", "smoke@example.com")
    project_repo.joinpath("README.md").write_text("# Runtime secrets smoke\n", encoding="utf-8")
    run_command("git", "-C", project_repo, "add", "README.md")
    run_command("git", "-C", project_repo, "commit", "-m", "Initial runtime secrets fixture")

    worktree_root.mkdir(parents=True, exist_ok=True)
    executor_worktree = worktree_root / "demo-executor"
    run_command("git", "-C", project_repo, "worktree", "add", "--detach", executor_worktree, "HEAD")

    instructions_root.mkdir(parents=True, exist_ok=True)
    run_command("git", "-C", instructions_root, "init", "-b", "main")
    run_command("git", "-C", instructions_root, "config", "user.name", "Smoke Test")
    run_command("git", "-C", instructions_root, "config", "user.email", "smoke@example.com")
    (instructions_root / "profiles" / "default").mkdir(parents=True, exist_ok=True)
    (instructions_root / "profiles" / "default" / "shared.md").write_text("Shared runtime-secrets smoke profile.\n", encoding="utf-8")
    (instructions_root / "profiles" / "default" / "executor.md").write_text("Executor runtime-secrets smoke profile.\n", encoding="utf-8")
    run_command("git", "-C", instructions_root, "add", ".")
    run_command("git", "-C", instructions_root, "commit", "-m", "Initial instructions fixture")

    fakebin.mkdir(parents=True, exist_ok=True)
    write_fake_codex(fakebin / "codex", dispatch_secret)

    runtime_secrets_dir = runtime_root / "secrets"
    runtime_secrets_dir.mkdir(parents=True, exist_ok=True)
    runtime_secrets_file = runtime_secrets_dir / "runtime-secrets.json"
    runtime_secrets_file.write_text(
        json.dumps({"host_check.token": host_check_secret}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    base_env = os.environ.copy()
    base_env["PATH"] = str(fakebin) + os.pathsep + base_env["PATH"]
    base_env["SMOKE_DISPATCH_TOKEN"] = dispatch_secret

    missing_env = dict(base_env)
    missing_env.pop("SMOKE_DISPATCH_TOKEN", None)

    http_server = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            """
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

expected = sys.argv[2]

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return
        header = self.headers.get("Authorization")
        if header != f"Bearer {expected}":
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"forbidden")
            return
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return

HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
""",
            str(host_port),
            host_check_secret,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    api_process: subprocess.Popen[str] | None = None

    try:
        load_payload(run_command(init_sqlite_script, db_path, "--json", env=base_env))
        load_payload(run_command(register_project_script, package_root, "--sqlite-db", db_path, "--json", env=base_env))

        resolve_payload = load_payload(
            run_command(
                resolve_runtime_secrets_script,
                "--sqlite-db",
                db_path,
                "--project-key",
                "demo",
                "--runtime-root",
                runtime_root,
                "--json",
                env=base_env,
            )
        )
        values = {item["key"]: item for item in resolve_payload["runtime_values"]["values"]}
        assert values["dispatch_token"]["value"] == "[redacted:secret:dispatch_token]", resolve_payload
        assert values["host_check_token"]["value"] == "[redacted:secret:host_check_token]", resolve_payload
        assert values["host_base_url"]["value"] == host_base_url, resolve_payload
        assert values["default_branch_name"]["value"] == "main", resolve_payload

        failed_check = run_command(
            check_runtime_secrets_script,
            "--sqlite-db",
            db_path,
            "--project-key",
            "demo",
            "--selection",
            "dispatch_env",
            "--runtime-root",
            runtime_root,
            "--json",
            env=missing_env,
            expected_returncode=1,
        )
        failed_check_payload = load_payload(failed_check)
        assert failed_check_payload["error"]["code"] == "RUNTIME_VALUE_REQUIRED_MISSING", failed_check_payload

        api_port = find_open_port()
        api_env = dict(base_env)
        api_process = subprocess.Popen(
            [
                str(run_api_script),
                "--sqlite-db",
                str(db_path),
                "--runtime-root",
                str(runtime_root),
                "--host",
                "127.0.0.1",
                "--port",
                str(api_port),
            ],
            cwd=control_dir,
            stdout=api_log_path.open("w", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            text=True,
            env=api_env,
        )
        api_base_url = f"http://127.0.0.1:{api_port}"
        wait_for_api(api_base_url)

        status_payload = request_json(
            api_base_url,
            "GET",
            "/v1/runtime/secrets/status?" + urllib.parse.urlencode({"project_key": "demo"}),
            expected_status=200,
        )
        api_values = {item["key"]: item for item in status_payload["data"]["runtime_values"]["values"]}
        assert api_values["dispatch_token"]["value"] == "[redacted:secret:dispatch_token]", status_payload
        assert api_values["host_base_url"]["value"] == host_base_url, status_payload

        check_payload = request_json(
            api_base_url,
            "POST",
            "/v1/runtime/secrets/check",
            payload={"project_key": "demo"},
            expected_status=200,
        )
        assert check_payload["data"]["runtime_values"]["all_required_resolved"] is True, check_payload

        run_payload = load_payload(
            run_command(
                create_root_run_script,
                "--sqlite-db",
                db_path,
                "--project-key",
                "demo",
                "--project-profile",
                "default",
                "--workflow-id",
                "build",
                "--milestone",
                "runtime-secrets-smoke",
                "--artifact-root",
                artifacts_root,
                "--json",
                env=base_env,
            )
        )
        run_id = run_payload["run_details"]["run"]["id"]

        claim_payload = load_payload(
            run_command(
                claim_next_run_script,
                "--sqlite-db",
                db_path,
                "--json",
                env=base_env,
            )
        )
        assert claim_payload["claim"]["dispatch_run"]["run"]["id"] == run_id, claim_payload

        dispatch_payload = load_payload(
            run_command(
                dispatch_executor_script,
                "--sqlite-db",
                db_path,
                "--run-id",
                run_id,
                "--artifact-root",
                artifacts_root,
                "--workspace-root",
                workspace_root,
                "--runtime-root",
                runtime_root,
                "--project-repo-path",
                project_repo,
                "--executor-worktree-path",
                executor_worktree,
                "--instructions-repo-path",
                instructions_root,
                "--instruction-profile",
                "default",
                "--task-text",
                "Smoke dispatch runtime secret injection",
                "--mode",
                "executor-only",
                "--json",
                env=base_env,
            )
        )
        assert dispatch_payload["dispatch"]["technical_success"] is True, dispatch_payload
        step_run_id = dispatch_payload["dispatch"]["step_run"]["step_run"]["id"]
        attempt_dir = Path(dispatch_payload["dispatch"]["attempt_paths"]["attempt_directory"]).resolve()
        stdout_log_text = (attempt_dir / "stdout.log").read_text(encoding="utf-8")
        stderr_log_text = (attempt_dir / "stderr.log").read_text(encoding="utf-8")
        dispatch_manifest_text = (attempt_dir / "dispatch-result.json").read_text(encoding="utf-8")
        assert dispatch_secret not in stdout_log_text, stdout_log_text
        assert dispatch_secret not in stderr_log_text, stderr_log_text
        assert dispatch_secret not in dispatch_manifest_text, dispatch_manifest_text
        assert "[redacted:secret:dispatch_token]" in stdout_log_text, stdout_log_text
        assert "[redacted:secret:dispatch_token]" in stderr_log_text, stderr_log_text
        assert "[redacted:secret:dispatch_token]" in dispatch_manifest_text, dispatch_manifest_text

        host_checks_payload = load_payload(
            run_command(
                run_host_checks_script,
                "--sqlite-db",
                db_path,
                "--run-id",
                run_id,
                "--artifact-root",
                artifacts_root,
                "--runtime-root",
                runtime_root,
                "--json",
                env=base_env,
            )
        )
        assert host_checks_payload["host_checks"]["verdict"] == "green", host_checks_payload
        manifest_path = Path(host_checks_payload["host_checks"]["manifest_path"]).resolve()
        manifest_text = manifest_path.read_text(encoding="utf-8")
        assert host_check_secret not in manifest_text, manifest_text
        assert "[redacted:secret:host_check_token]" in manifest_text, manifest_text

        leaked_tokens: list[str] = []
        for path in artifacts_root.rglob("*"):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if dispatch_secret in text or host_check_secret in text:
                leaked_tokens.append(str(path))
        if leaked_tokens:
            raise SystemExit("Runtime secret leak detected in artifact files: " + ", ".join(leaked_tokens))

        print(
            json.dumps(
                {
                    "dispatch_run_id": run_id,
                    "dispatch_step_run_id": step_run_id,
                    "dispatch_attempt_dir": str(attempt_dir),
                    "host_check_manifest_path": str(manifest_path),
                    "runtime_secrets_file": str(runtime_secrets_file),
                    "api_base_url": api_base_url,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        if api_process is not None:
            api_process.terminate()
            try:
                api_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                api_process.kill()
                api_process.wait(timeout=10)
        http_server.terminate()
        try:
            http_server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            http_server.kill()
            http_server.wait(timeout=10)
PY
