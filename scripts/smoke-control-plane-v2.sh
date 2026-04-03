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
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path


control_dir = Path(sys.argv[1]).resolve()
validate_script = control_dir / "scripts" / "validate-project-package"
sqlite_script = control_dir / "scripts" / "init-sqlite-v1"
register_script = control_dir / "scripts" / "register-project-package"
list_script = control_dir / "scripts" / "list-registered-projects"
create_run_script = control_dir / "scripts" / "create-root-run"
finish_step_script = control_dir / "scripts" / "finish-step-run"
list_runs_script = control_dir / "scripts" / "list-runs"
list_step_runs_script = control_dir / "scripts" / "list-step-runs"
retry_step_script = control_dir / "scripts" / "retry-step-run"
show_run_script = control_dir / "scripts" / "show-run"
show_step_run_script = control_dir / "scripts" / "show-step-run"
start_step_script = control_dir / "scripts" / "start-step-run"
sample_project = control_dir / "projects" / "sample-project"


def run_command(*args: str, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
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


def load_error_payload(proc: subprocess.CompletedProcess[str]) -> dict:
    return json.loads(proc.stderr)


with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_root = Path(tmp_dir)

    valid_proc = run_command(validate_script, "sample-project", "--projects-root", control_dir / "projects", "--json")
    valid_payload = json.loads(valid_proc.stdout)
    assert valid_payload["ok"] is True, valid_payload
    assert valid_payload["package"]["project_key"] == "sample-project", valid_payload
    assert valid_payload["package"]["schema_version"] == "v2-draft", valid_payload

    missing_package = tmp_root / "missing-runtime"
    shutil.copytree(sample_project, missing_package)
    (missing_package / "runtime.yaml").unlink()
    missing_proc = run_command(validate_script, missing_package, "--json", expect_success=False)
    missing_payload = load_error_payload(missing_proc)
    assert missing_payload["ok"] is False, missing_payload
    assert any(error["code"] == "FILE_MISSING" and error["file_path"].endswith("runtime.yaml") for error in missing_payload["errors"]), missing_payload

    bad_root_package = tmp_root / "bad-root"
    shutil.copytree(sample_project, bad_root_package)
    (bad_root_package / "workflow.yaml").write_text("- invalid\n- root\n", encoding="utf-8")
    bad_root_proc = run_command(validate_script, bad_root_package, "--json", expect_success=False)
    bad_root_payload = load_error_payload(bad_root_proc)
    assert any(error["code"] == "WRONG_ROOT_TYPE" and error["file_path"].endswith("workflow.yaml") for error in bad_root_payload["errors"]), bad_root_payload

    invalid_yaml_package = tmp_root / "invalid-yaml"
    shutil.copytree(sample_project, invalid_yaml_package)
    (invalid_yaml_package / "policy.yaml").write_text("not: [valid\n", encoding="utf-8")
    invalid_yaml_proc = run_command(validate_script, invalid_yaml_package, "--json", expect_success=False)
    invalid_yaml_payload = load_error_payload(invalid_yaml_proc)
    assert any(error["code"] == "INVALID_YAML" and error["file_path"].endswith("policy.yaml") for error in invalid_yaml_payload["errors"]), invalid_yaml_payload

    missing_key_package = tmp_root / "missing-key"
    shutil.copytree(sample_project, missing_key_package)
    (missing_key_package / "project.yaml").write_text("{}", encoding="utf-8")
    missing_key_proc = run_command(validate_script, missing_key_package, "--json", expect_success=False)
    missing_key_payload = load_error_payload(missing_key_proc)
    assert any(error["code"] == "MISSING_REQUIRED_KEY" and error["key_path"] == "schema_version" for error in missing_key_payload["errors"]), missing_key_payload

    wrong_type_package = tmp_root / "wrong-key-type"
    shutil.copytree(sample_project, wrong_type_package)
    (wrong_type_package / "capabilities.yaml").write_text("sections: []\n", encoding="utf-8")
    wrong_type_proc = run_command(validate_script, wrong_type_package, "--json", expect_success=False)
    wrong_type_payload = load_error_payload(wrong_type_proc)
    assert any(error["code"] == "WRONG_KEY_TYPE" and error["key_path"] == "sections" for error in wrong_type_payload["errors"]), wrong_type_payload

    db_path = tmp_root / "control-plane-v2.sqlite"
    sqlite_proc = run_command(sqlite_script, db_path, "--json")
    sqlite_payload = json.loads(sqlite_proc.stdout)
    assert sqlite_payload["ok"] is True, sqlite_payload
    expected_tables = {
        "projects",
        "runs",
        "step_runs",
        "queue_items",
        "artifact_refs",
        "state_transitions",
        "run_snapshots",
    }
    assert expected_tables.issubset(set(sqlite_payload["database"]["tables"])), sqlite_payload

    register_proc = run_command(
        register_script,
        "sample-project",
        "--projects-root",
        control_dir / "projects",
        "--sqlite-db",
        db_path,
        "--json",
    )
    register_payload = json.loads(register_proc.stdout)
    assert register_payload["ok"] is True, register_payload
    assert register_payload["registration"]["action"] == "inserted", register_payload
    initial_project = register_payload["registration"]["project"]
    assert initial_project["project_key"] == "sample-project", register_payload
    assert initial_project["package_root"] == str(sample_project), register_payload

    second_register_proc = run_command(
        register_script,
        "sample-project",
        "--projects-root",
        control_dir / "projects",
        "--sqlite-db",
        db_path,
        "--json",
    )
    second_register_payload = json.loads(second_register_proc.stdout)
    assert second_register_payload["registration"]["action"] == "updated", second_register_payload
    second_project = second_register_payload["registration"]["project"]
    assert second_project["id"] == initial_project["id"], second_register_payload

    relocated_package = tmp_root / "sample-project"
    shutil.copytree(sample_project, relocated_package)
    relocated_register_proc = run_command(
        register_script,
        relocated_package,
        "--sqlite-db",
        db_path,
        "--json",
    )
    relocated_register_payload = json.loads(relocated_register_proc.stdout)
    relocated_project = relocated_register_payload["registration"]["project"]
    assert relocated_project["id"] == initial_project["id"], relocated_register_payload
    assert relocated_project["package_root"] == str(relocated_package), relocated_register_payload
    assert relocated_project["created_at"] == initial_project["created_at"], relocated_register_payload

    list_proc = run_command(list_script, "--sqlite-db", db_path, "--json")
    list_payload = json.loads(list_proc.stdout)
    assert list_payload["ok"] is True, list_payload
    assert len(list_payload["projects"]) == 1, list_payload
    listed_project = list_payload["projects"][0]
    assert listed_project["project_key"] == "sample-project", list_payload
    assert listed_project["package_root"] == str(relocated_package), list_payload

    invalid_register_proc = run_command(
        register_script,
        missing_package,
        "--sqlite-db",
        db_path,
        "--json",
        expect_success=False,
    )
    invalid_register_payload = load_error_payload(invalid_register_proc)
    assert invalid_register_payload["stage"] == "validation", invalid_register_payload
    assert any(error["code"] == "FILE_MISSING" for error in invalid_register_payload["errors"]), invalid_register_payload

    unregistered_db_path = tmp_root / "unregistered.sqlite"
    run_command(sqlite_script, unregistered_db_path, "--json")
    unregistered_create_proc = run_command(
        create_run_script,
        "--sqlite-db",
        unregistered_db_path,
        "--project-key",
        "sample-project",
        "--project-profile",
        "default",
        "--workflow-id",
        "build",
        "--milestone",
        "initial",
        "--json",
        expect_success=False,
    )
    unregistered_create_payload = load_error_payload(unregistered_create_proc)
    assert unregistered_create_payload["stage"] == "run_persistence", unregistered_create_payload
    assert unregistered_create_payload["error"]["code"] == "PROJECT_NOT_REGISTERED", unregistered_create_payload

    artifact_root = tmp_root / "artifacts"
    create_run_proc = run_command(
        create_run_script,
        "--sqlite-db",
        db_path,
        "--project-key",
        "sample-project",
        "--project-profile",
        "default",
        "--workflow-id",
        "build",
        "--milestone",
        "initial",
        "--artifact-root",
        artifact_root,
        "--json",
    )
    create_run_payload = json.loads(create_run_proc.stdout)
    assert create_run_payload["ok"] is True, create_run_payload
    run_details = create_run_payload["run_details"]
    created_run = run_details["run"]
    created_queue_item = created_run["queue_item"]
    assert created_run["project_key"] == "sample-project", create_run_payload
    assert created_run["status"] == "queued", create_run_payload
    assert created_run["origin_type"] == "root_manual", create_run_payload
    assert created_run["parent_run_id"] is None, create_run_payload
    assert created_run["origin_run_id"] is None, create_run_payload
    assert created_run["origin_step_run_id"] is None, create_run_payload
    assert created_queue_item["priority_class"] == "interactive", create_run_payload
    assert created_queue_item["status"] == "queued", create_run_payload
    artifact_directory = Path(run_details["artifact_directory"])
    assert artifact_directory == artifact_root / "sample-project" / created_run["flow_id"] / created_run["id"], create_run_payload
    assert artifact_directory.is_dir(), create_run_payload

    list_runs_proc = run_command(
        list_runs_script,
        "--sqlite-db",
        db_path,
        "--project-key",
        "sample-project",
        "--json",
    )
    list_runs_payload = json.loads(list_runs_proc.stdout)
    assert list_runs_payload["ok"] is True, list_runs_payload
    assert len(list_runs_payload["runs"]) == 1, list_runs_payload
    assert list_runs_payload["runs"][0]["id"] == created_run["id"], list_runs_payload

    show_run_proc = run_command(show_run_script, "--sqlite-db", db_path, created_run["id"], "--json")
    show_run_payload = json.loads(show_run_proc.stdout)
    assert show_run_payload["ok"] is True, show_run_payload
    assert show_run_payload["run_details"]["run"]["id"] == created_run["id"], show_run_payload
    assert len(show_run_payload["run_details"]["state_transitions"]) == 2, show_run_payload
    assert show_run_payload["run_details"]["run_snapshots"] == [], show_run_payload

    start_executor_proc = run_command(
        start_step_script,
        "--sqlite-db",
        db_path,
        "--run-id",
        created_run["id"],
        "--step-key",
        "executor",
        "--json",
    )
    start_executor_payload = json.loads(start_executor_proc.stdout)
    executor_step = start_executor_payload["step_run_details"]["step_run"]
    assert executor_step["step_key"] == "executor", start_executor_payload
    assert executor_step["attempt_no"] == 1, start_executor_payload
    assert executor_step["status"] == "running", start_executor_payload
    assert start_executor_payload["step_run_details"]["run"]["status"] == "running", start_executor_payload
    assert start_executor_payload["step_run_details"]["run"]["queue_item"]["status"] == "claimed", start_executor_payload

    invalid_retry_proc = run_command(
        retry_step_script,
        "--sqlite-db",
        db_path,
        executor_step["id"],
        "--json",
        expect_success=False,
    )
    invalid_retry_payload = load_error_payload(invalid_retry_proc)
    assert invalid_retry_payload["stage"] == "step_run_persistence", invalid_retry_payload
    assert invalid_retry_payload["error"]["code"] == "STEP_RUN_NOT_TERMINAL", invalid_retry_payload

    finish_executor_proc = run_command(
        finish_step_script,
        "--sqlite-db",
        db_path,
        executor_step["id"],
        "--status",
        "succeeded",
        "--json",
    )
    finish_executor_payload = json.loads(finish_executor_proc.stdout)
    assert finish_executor_payload["step_run_details"]["step_run"]["status"] == "succeeded", finish_executor_payload

    start_reviewer_proc = run_command(
        start_step_script,
        "--sqlite-db",
        db_path,
        "--run-id",
        created_run["id"],
        "--step-key",
        "reviewer",
        "--json",
    )
    start_reviewer_payload = json.loads(start_reviewer_proc.stdout)
    reviewer_step = start_reviewer_payload["step_run_details"]["step_run"]
    assert reviewer_step["step_key"] == "reviewer", start_reviewer_payload
    assert reviewer_step["attempt_no"] == 1, start_reviewer_payload
    assert reviewer_step["status"] == "running", start_reviewer_payload

    finish_reviewer_proc = run_command(
        finish_step_script,
        "--sqlite-db",
        db_path,
        reviewer_step["id"],
        "--status",
        "failed",
        "--json",
    )
    finish_reviewer_payload = json.loads(finish_reviewer_proc.stdout)
    assert finish_reviewer_payload["step_run_details"]["step_run"]["status"] == "failed", finish_reviewer_payload

    retry_reviewer_proc = run_command(
        retry_step_script,
        "--sqlite-db",
        db_path,
        reviewer_step["id"],
        "--json",
    )
    retry_reviewer_payload = json.loads(retry_reviewer_proc.stdout)
    reviewer_retry_step = retry_reviewer_payload["step_run_details"]["step_run"]
    assert reviewer_retry_step["step_key"] == "reviewer", retry_reviewer_payload
    assert reviewer_retry_step["attempt_no"] == 2, retry_reviewer_payload
    assert reviewer_retry_step["previous_step_run_id"] == reviewer_step["id"], retry_reviewer_payload
    assert reviewer_retry_step["status"] == "running", retry_reviewer_payload

    finish_retry_reviewer_proc = run_command(
        finish_step_script,
        "--sqlite-db",
        db_path,
        reviewer_retry_step["id"],
        "--status",
        "succeeded",
        "--json",
    )
    finish_retry_reviewer_payload = json.loads(finish_retry_reviewer_proc.stdout)
    assert finish_retry_reviewer_payload["step_run_details"]["step_run"]["status"] == "succeeded", finish_retry_reviewer_payload

    list_step_runs_proc = run_command(
        list_step_runs_script,
        "--sqlite-db",
        db_path,
        "--run-id",
        created_run["id"],
        "--json",
    )
    list_step_runs_payload = json.loads(list_step_runs_proc.stdout)
    assert list_step_runs_payload["ok"] is True, list_step_runs_payload
    assert [step_run["id"] for step_run in list_step_runs_payload["step_runs"]] == [
        executor_step["id"],
        reviewer_step["id"],
        reviewer_retry_step["id"],
    ], list_step_runs_payload

    show_step_run_proc = run_command(
        show_step_run_script,
        "--sqlite-db",
        db_path,
        reviewer_retry_step["id"],
        "--json",
    )
    show_step_run_payload = json.loads(show_step_run_proc.stdout)
    assert show_step_run_payload["ok"] is True, show_step_run_payload
    assert show_step_run_payload["step_run_details"]["step_run"]["id"] == reviewer_retry_step["id"], show_step_run_payload
    assert len(show_step_run_payload["step_run_details"]["state_transitions"]) == 2, show_step_run_payload

    with sqlite3.connect(db_path) as conn:
        run_row = conn.execute(
            "SELECT id, status, origin_type, parent_run_id, origin_run_id, origin_step_run_id, started_at FROM runs WHERE id = ?",
            (created_run["id"],),
        ).fetchone()
        assert run_row is not None, created_run
        assert run_row[1] == "running", run_row
        assert run_row[2] == "root_manual", run_row
        assert run_row[3] is None and run_row[4] is None and run_row[5] is None, run_row
        assert run_row[6] is not None, run_row

        queue_row = conn.execute(
            "SELECT id, run_id, priority_class, status, claimed_at FROM queue_items WHERE run_id = ?",
            (created_run["id"],),
        ).fetchone()
        assert queue_row is not None, created_run
        assert queue_row[2] == "interactive" and queue_row[3] == "claimed", queue_row
        assert queue_row[4] is not None, queue_row

        step_rows = conn.execute(
            """
            SELECT id, step_key, attempt_no, previous_step_run_id, status
            FROM step_runs
            WHERE run_id = ?
            ORDER BY created_at, id
            """,
            (created_run["id"],),
        ).fetchall()
        assert [(row[1], row[2], row[3], row[4]) for row in step_rows] == [
            ("executor", 1, None, "succeeded"),
            ("reviewer", 1, None, "failed"),
            ("reviewer", 2, reviewer_step["id"], "succeeded"),
        ], step_rows

        transition_count = conn.execute(
            "SELECT COUNT(*) FROM state_transitions WHERE run_id = ? OR queue_item_id = ? OR step_run_id IN (?, ?, ?)",
            (created_run["id"], created_queue_item["id"], executor_step["id"], reviewer_step["id"], reviewer_retry_step["id"]),
        ).fetchone()[0]
        assert transition_count == 10, transition_count

        actual_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    assert expected_tables.issubset(actual_tables), actual_tables

    print(
        json.dumps(
            {
                "validated_project": valid_payload["package"]["package_root"],
                "checked_error_codes": [
                    "FILE_MISSING",
                    "WRONG_ROOT_TYPE",
                    "INVALID_YAML",
                    "MISSING_REQUIRED_KEY",
                    "WRONG_KEY_TYPE",
                ],
                "registered_project_id": initial_project["id"],
                "registered_projects": [project["project_key"] for project in list_payload["projects"]],
                "created_run_id": created_run["id"],
                "created_flow_id": created_run["flow_id"],
                "run_transition_count": transition_count,
                "step_run_chain": [
                    {
                        "id": step_run["id"],
                        "step_key": step_run["step_key"],
                        "attempt_no": step_run["attempt_no"],
                        "previous_step_run_id": step_run["previous_step_run_id"],
                        "status": step_run["status"],
                    }
                    for step_run in list_step_runs_payload["step_runs"]
                ],
                "sqlite_tables": sorted(actual_tables),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
PY
