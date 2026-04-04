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

python3 - "$CONTROL_DIR" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


control_dir = Path(sys.argv[1]).resolve()
init_sqlite_script = control_dir / "scripts" / "init-sqlite-v1"
register_project_script = control_dir / "scripts" / "register-project-package"
create_root_run_script = control_dir / "scripts" / "create-root-run"
pause_run_script = control_dir / "scripts" / "pause-run"
generate_contract_script = control_dir / "scripts" / "generate-bounded-contract"
show_contract_script = control_dir / "scripts" / "show-bounded-contract"
list_templates_script = control_dir / "scripts" / "list-contract-templates"
run_api_script = control_dir / "scripts" / "run-control-plane-api"
sample_project = control_dir / "projects" / "sample-project"


def run_command(*args: object, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(arg) for arg in args],
        text=True,
        capture_output=True,
        cwd=control_dir,
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


def load_stdout_payload(proc: subprocess.CompletedProcess[str]) -> dict:
    return json.loads(proc.stdout)


def load_stderr_payload(proc: subprocess.CompletedProcess[str]) -> dict:
    return json.loads(proc.stderr)


def canonicalize_normalized_contract(contract: dict) -> dict:
    normalized = json.loads(json.dumps(contract))
    normalized.pop("contract_id", None)
    normalized.pop("generated_at", None)
    return normalized


def canonicalize_prompt_text(prompt_text: str) -> str:
    lines = []
    for line in prompt_text.splitlines():
        if line.startswith("Contract ID: "):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def request_json(
    base_url: str,
    method: str,
    path: str,
    *,
    payload: dict | None = None,
    expected_status: int = 200,
) -> dict:
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
    response_payload = json.loads(body)
    if status != expected_status:
        raise SystemExit(
            f"Unexpected HTTP status for {method} {path}: expected {expected_status}, got {status}\n"
            f"{json.dumps(response_payload, ensure_ascii=False, indent=2)}"
        )
    return response_payload


def find_open_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def create_root_run(db_path: Path, artifacts_root: Path, milestone: str) -> dict:
    payload = load_stdout_payload(
        run_command(
            create_root_run_script,
            "--sqlite-db",
            db_path,
            "--project-key",
            "sample-project",
            "--project-profile",
            "default",
            "--workflow-id",
            "build",
            "--milestone",
            milestone,
            "--artifact-root",
            artifacts_root,
            "--json",
        )
    )
    return payload["run_details"]


with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_root = Path(tmp_dir)
    artifacts_root = tmp_root / "artifacts"
    runtime_root = tmp_root / "runtime"
    instructions_root = tmp_root / "instructions"
    project_repo_path = tmp_root / "project"
    executor_worktree_path = runtime_root / "executor"
    reviewer_worktree_path = runtime_root / "reviewer"
    worker_logs_root = tmp_root / "worker-logs"
    api_log_path = tmp_root / "control-plane-contract-api.log"

    for path in (
        artifacts_root,
        instructions_root,
        project_repo_path,
        executor_worktree_path,
        reviewer_worktree_path,
        worker_logs_root,
    ):
        path.mkdir(parents=True, exist_ok=True)

    db_path = tmp_root / "control-plane-v2.sqlite"
    load_stdout_payload(run_command(init_sqlite_script, db_path, "--json"))
    load_stdout_payload(
        run_command(
            register_project_script,
            sample_project,
            "--sqlite-db",
            db_path,
            "--json",
        )
    )

    templates_payload = load_stdout_payload(
        run_command(
            list_templates_script,
            "--sqlite-db",
            db_path,
            "--project-key",
            "sample-project",
            "--json",
        )
    )
    policy = templates_payload["bounded_contract_policy"]
    assert policy["storage_model"] == "project_package_policy_v1", policy
    assert set(policy["defaults"]) == {
        "implementation_step",
        "inspection_step",
        "recovery_step",
        "manual_followup_step",
    }, policy
    assert len(policy["templates"]) == 4, policy

    implementation_run = create_root_run(db_path, artifacts_root, "bounded-contract-implementation")
    implementation_run_id = implementation_run["run"]["id"]

    runtime_context = {
        "task_text": "Implement bounded-contract engine behavior from the approved template.",
        "project_repo_path": str(project_repo_path),
        "executor_worktree_path": str(executor_worktree_path),
        "reviewer_worktree_path": str(reviewer_worktree_path),
        "instructions_repo_path": str(instructions_root),
        "instruction_profile": "default",
        "source": "contract-smoke",
        "thread_label": "bounded-contract-smoke",
    }
    operator_request = {
        "requested_capability_sections": ["implementation", "repo_read", "tests"],
        "requested_actions": ["read_repo", "run_repo_checks"],
    }
    implementation_request = {
        "run_id": implementation_run_id,
        "contract_type": "implementation_step",
        "runtime_context": runtime_context,
        "operator_request": operator_request,
        "artifact_root": str(artifacts_root),
    }
    implementation_request_path = tmp_root / "implementation-request.json"
    implementation_request_path.write_text(
        json.dumps(implementation_request, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    implementation_cli = load_stdout_payload(
        run_command(
            generate_contract_script,
            "--sqlite-db",
            db_path,
            "--request-json",
            implementation_request_path,
            "--json",
        )
    )["bounded_contract"]
    implementation_show = load_stdout_payload(
        run_command(
            show_contract_script,
            "--sqlite-db",
            db_path,
            implementation_cli["contract_id"],
            "--json",
        )
    )["bounded_contract"]
    assert implementation_show == implementation_cli, (implementation_show, implementation_cli)
    assert implementation_cli["contract_type"] == "implementation_step", implementation_cli
    assert implementation_cli["template_key"] == "implementation_default", implementation_cli
    assert implementation_cli["normalized_contract"]["task"]["summary"] == "Implementation step for sample-project / build", implementation_cli
    assert implementation_cli["prompt_text"] == implementation_show["prompt_text"], implementation_cli

    artifact_by_kind = {
        artifact["artifact_kind"]: Path(artifact["filesystem_path"])
        for artifact in implementation_cli["artifacts"]
    }
    assert set(artifact_by_kind) == {
        "bounded_contract_json",
        "bounded_contract_prompt",
        "bounded_contract_manifest",
    }, artifact_by_kind
    assert json.loads(artifact_by_kind["bounded_contract_json"].read_text(encoding="utf-8")) == implementation_cli["normalized_contract"]
    assert artifact_by_kind["bounded_contract_prompt"].read_text(encoding="utf-8") == implementation_cli["prompt_text"]
    manifest_doc = json.loads(artifact_by_kind["bounded_contract_manifest"].read_text(encoding="utf-8"))
    assert manifest_doc["contract_id"] == implementation_cli["contract_id"], manifest_doc

    invalid_request_path = tmp_root / "invalid-request.json"
    invalid_request_path.write_text(
        json.dumps(
            {
                "run_id": implementation_run_id,
                "contract_type": "recovery_step",
                "runtime_context": runtime_context,
                "artifact_root": str(artifacts_root),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    invalid_generation = run_command(
        generate_contract_script,
        "--sqlite-db",
        db_path,
        "--request-json",
        invalid_request_path,
        "--json",
        expect_success=False,
    )
    invalid_payload = load_stderr_payload(invalid_generation)
    assert invalid_payload["stage"] == "bounded_contracts", invalid_payload
    assert invalid_payload["error"]["code"] == "CONTRACT_STATE_NOT_ALLOWED", invalid_payload

    recovery_run = create_root_run(db_path, artifacts_root, "bounded-contract-recovery")
    recovery_run_id = recovery_run["run"]["id"]
    load_stdout_payload(
        run_command(
            pause_run_script,
            "--sqlite-db",
            db_path,
            recovery_run_id,
            "--note",
            "pause for recovery contract smoke",
            "--json",
        )
    )
    recovery_request_path = tmp_root / "recovery-request.json"
    recovery_request_path.write_text(
        json.dumps(
            {
                "run_id": recovery_run_id,
                "contract_type": "recovery_step",
                "runtime_context": runtime_context,
                "artifact_root": str(artifacts_root),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    recovery_cli = load_stdout_payload(
        run_command(
            generate_contract_script,
            "--sqlite-db",
            db_path,
            "--request-json",
            recovery_request_path,
            "--json",
        )
    )["bounded_contract"]
    recovery_state_tags = set(recovery_cli["normalized_contract"]["runtime"]["state_tags"])
    assert recovery_cli["contract_type"] == "recovery_step", recovery_cli
    assert recovery_cli["template_key"] == "recovery_default", recovery_cli
    assert "manual_paused" in recovery_state_tags, recovery_cli

    api_port = find_open_port()
    api_log_handle = api_log_path.open("w", encoding="utf-8")
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
            "--worker-log-root",
            str(worker_logs_root),
        ],
        cwd=control_dir,
        stdout=api_log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{api_port}"

    try:
        for _ in range(60):
            try:
                health = request_json(base_url, "GET", "/v1/health")
            except Exception:
                time.sleep(0.2)
                continue
            if health["ok"]:
                break
        else:
            raise SystemExit(f"HTTP API did not become ready in time.\n{api_log_path.read_text(encoding='utf-8')}")

        api_generation = request_json(
            base_url,
            "POST",
            "/v1/contracts/generate",
            payload={
                "run_id": implementation_run_id,
                "contract_type": "implementation_step",
                "runtime_context": runtime_context,
                "operator_request": operator_request,
            },
        )
        api_contract = api_generation["data"]["bounded_contract"]
        assert api_contract["contract_type"] == "implementation_step", api_contract
        assert api_contract["template_key"] == "implementation_default", api_contract
        assert canonicalize_normalized_contract(api_contract["normalized_contract"]) == canonicalize_normalized_contract(
            implementation_cli["normalized_contract"]
        ), (api_contract, implementation_cli)
        assert canonicalize_prompt_text(api_contract["prompt_text"]) == canonicalize_prompt_text(
            implementation_cli["prompt_text"]
        ), (api_contract, implementation_cli)

        api_invalid = request_json(
            base_url,
            "POST",
            "/v1/contracts/generate",
            payload={
                "run_id": implementation_run_id,
                "contract_type": "recovery_step",
                "runtime_context": runtime_context,
            },
            expected_status=409,
        )
        assert api_invalid["ok"] is False, api_invalid
        assert api_invalid["error"]["code"] == "CONTRACT_STATE_NOT_ALLOWED", api_invalid

        api_contract_detail = request_json(
            base_url,
            "GET",
            f"/v1/contracts/{urllib.parse.quote(api_contract['contract_id'])}",
        )
        api_contract_show = api_contract_detail["data"]["bounded_contract"]
        cli_contract_show = load_stdout_payload(
            run_command(
                show_contract_script,
                "--sqlite-db",
                db_path,
                api_contract["contract_id"],
                "--json",
            )
        )["bounded_contract"]
        assert api_contract_show == cli_contract_show, (api_contract_show, cli_contract_show)
    finally:
        api_process.terminate()
        try:
            api_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            api_process.kill()
            api_process.wait(timeout=10)
        api_log_handle.close()

    artifact_kinds = (
        "bounded_contract_json",
        "bounded_contract_prompt",
        "bounded_contract_manifest",
    )
    with sqlite3.connect(db_path) as conn:
        implementation_manifest_count = conn.execute(
            "SELECT COUNT(*) FROM contract_manifests WHERE run_id = ?",
            (implementation_run_id,),
        ).fetchone()[0]
        assert implementation_manifest_count == 2, implementation_manifest_count

        recovery_manifest_count = conn.execute(
            "SELECT COUNT(*) FROM contract_manifests WHERE run_id = ?",
            (recovery_run_id,),
        ).fetchone()[0]
        assert recovery_manifest_count == 1, recovery_manifest_count

        linked_artifact_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM artifact_refs
            WHERE artifact_kind IN (?, ?, ?)
              AND run_id IN (?, ?)
            """,
            (*artifact_kinds, implementation_run_id, recovery_run_id),
        ).fetchone()[0]
        assert linked_artifact_count == 9, linked_artifact_count

        append_only_history = conn.execute(
            """
            SELECT run_id, contract_type, template_key
            FROM contract_manifests
            ORDER BY created_at, id
            """
        ).fetchall()

    print(
        json.dumps(
            {
                "storage_model": policy["storage_model"],
                "taxonomy": sorted(policy["defaults"].keys()),
                "implementation_contract_id": implementation_cli["contract_id"],
                "api_contract_id": api_contract["contract_id"],
                "recovery_contract_id": recovery_cli["contract_id"],
                "implementation_prompt_consistent": implementation_cli["prompt_text"] == implementation_show["prompt_text"],
                "api_cli_prompt_consistent": canonicalize_prompt_text(api_contract["prompt_text"]) == canonicalize_prompt_text(
                    implementation_cli["prompt_text"]
                ),
                "append_only_history": [
                    {"run_id": row[0], "contract_type": row[1], "template_key": row[2]}
                    for row in append_only_history
                ],
                "linked_artifact_count": linked_artifact_count,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
PY
