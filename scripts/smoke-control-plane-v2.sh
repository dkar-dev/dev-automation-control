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
complete_reviewer_script = control_dir / "scripts" / "complete-reviewer-outcome"
claim_next_script = control_dir / "scripts" / "claim-next-run"
finish_step_script = control_dir / "scripts" / "finish-step-run"
list_flow_runs_script = control_dir / "scripts" / "list-flow-runs"
list_runs_script = control_dir / "scripts" / "list-runs"
list_step_runs_script = control_dir / "scripts" / "list-step-runs"
mark_dispatch_failed_script = control_dir / "scripts" / "mark-claimed-run-dispatch-failed"
release_claimed_script = control_dir / "scripts" / "release-claimed-run"
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


def load_stdout_payload(proc: subprocess.CompletedProcess[str]) -> dict:
    return json.loads(proc.stdout)


def create_root_run(
    milestone: str,
    *,
    workflow_id: str = "build",
    project_profile: str = "default",
    priority_class: str = "interactive",
    sqlite_db: Path | None = None,
) -> dict:
    target_db = sqlite_db or db_path
    return load_stdout_payload(
        run_command(
            create_run_script,
            "--sqlite-db",
            target_db,
            "--project-key",
            "sample-project",
            "--project-profile",
            project_profile,
            "--workflow-id",
            workflow_id,
            "--milestone",
            milestone,
            "--priority-class",
            priority_class,
            "--json",
        )
    )["run_details"]


def start_step(run_id: str, step_key: str) -> dict:
    return load_stdout_payload(
        run_command(
            start_step_script,
            "--sqlite-db",
            db_path,
            "--run-id",
            run_id,
            "--step-key",
            step_key,
            "--json",
        )
    )["step_run_details"]


def finish_step(step_run_id: str, status: str) -> dict:
    return load_stdout_payload(
        run_command(
            finish_step_script,
            "--sqlite-db",
            db_path,
            step_run_id,
            "--status",
            status,
            "--json",
        )
    )["step_run_details"]


def complete_reviewer(step_run_id: str, verdict: str, summary: str) -> dict:
    return load_stdout_payload(
        run_command(
            complete_reviewer_script,
            "--sqlite-db",
            db_path,
            step_run_id,
            "--verdict",
            verdict,
            "--summary",
            summary,
            "--json",
        )
    )["reviewer_outcome"]


def list_flow(flow_id: str) -> dict:
    return load_stdout_payload(
        run_command(
            list_flow_runs_script,
            "--sqlite-db",
            db_path,
            flow_id,
            "--json",
        )
    )


def claim_next(sqlite_db: Path, *, now: str | None = None) -> dict | None:
    args: list[object] = [
        claim_next_script,
        "--sqlite-db",
        sqlite_db,
        "--json",
    ]
    if now is not None:
        args.extend(["--now", now])
    payload = load_stdout_payload(run_command(*args))
    return payload["claim"]


def release_claim(
    sqlite_db: Path,
    *,
    run_id: str | None = None,
    queue_item_id: str | None = None,
    available_at: str | None = None,
    note: str | None = None,
) -> dict:
    args: list[object] = [
        release_claimed_script,
        "--sqlite-db",
        sqlite_db,
        "--json",
    ]
    if run_id is not None:
        args.extend(["--run-id", run_id])
    if queue_item_id is not None:
        args.extend(["--queue-item-id", queue_item_id])
    if available_at is not None:
        args.extend(["--available-at", available_at])
    if note is not None:
        args.extend(["--note", note])
    return load_stdout_payload(run_command(*args))["release"]


def dispatch_fail_claim(
    sqlite_db: Path,
    *,
    run_id: str | None = None,
    queue_item_id: str | None = None,
    reason_code: str,
    available_at: str | None = None,
    note: str | None = None,
) -> dict:
    args: list[object] = [
        mark_dispatch_failed_script,
        "--sqlite-db",
        sqlite_db,
        "--reason-code",
        reason_code,
        "--json",
    ]
    if run_id is not None:
        args.extend(["--run-id", run_id])
    if queue_item_id is not None:
        args.extend(["--queue-item-id", queue_item_id])
    if available_at is not None:
        args.extend(["--available-at", available_at])
    if note is not None:
        args.extend(["--note", note])
    return load_stdout_payload(run_command(*args))["dispatch_failure"]


def prepare_run_for_reviewer_outcome(milestone: str, *, reviewer_terminal_status: str = "succeeded") -> tuple[dict, dict]:
    run_details = create_root_run(milestone)
    run_payload = run_details["run"]
    executor_step = start_step(run_payload["id"], "executor")["step_run"]
    finish_step(executor_step["id"], "succeeded")
    reviewer_step = start_step(run_payload["id"], "reviewer")["step_run"]
    reviewer_step = finish_step(reviewer_step["id"], reviewer_terminal_status)["step_run"]
    return run_payload, reviewer_step


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

    approved_run, approved_reviewer_step = prepare_run_for_reviewer_outcome("reviewer-approved")
    approved_outcome = complete_reviewer(
        approved_reviewer_step["id"],
        "approved",
        "ready to merge",
    )
    assert approved_outcome["verdict"] == "approved", approved_outcome
    assert approved_outcome["current_run"]["run"]["id"] == approved_run["id"], approved_outcome
    assert approved_outcome["current_run"]["run"]["status"] == "completed", approved_outcome
    assert approved_outcome["current_run"]["run"]["queue_item"]["status"] == "completed", approved_outcome
    assert approved_outcome["follow_up_run"] is None, approved_outcome
    assert approved_outcome["flow_summary"]["continuation_allowed"] is False, approved_outcome
    assert approved_outcome["flow_summary"]["total_runs"] == 1, approved_outcome

    approved_show_run_payload = load_stdout_payload(
        run_command(show_run_script, "--sqlite-db", db_path, approved_run["id"], "--json")
    )
    assert approved_show_run_payload["run_details"]["run"]["status"] == "completed", approved_show_run_payload
    assert len(approved_show_run_payload["run_details"]["run_snapshots"]) == 2, approved_show_run_payload

    blocked_run, blocked_reviewer_step = prepare_run_for_reviewer_outcome(
        "reviewer-blocked",
        reviewer_terminal_status="failed",
    )
    blocked_outcome = complete_reviewer(
        blocked_reviewer_step["id"],
        "blocked",
        "missing required evidence",
    )
    assert blocked_outcome["verdict"] == "blocked", blocked_outcome
    assert blocked_outcome["current_run"]["run"]["status"] == "stopped", blocked_outcome
    assert blocked_outcome["current_run"]["run"]["queue_item"]["status"] == "cancelled", blocked_outcome
    assert blocked_outcome["follow_up_run"] is None, blocked_outcome
    assert blocked_outcome["flow_summary"]["stop_reason_code"] == "blocked", blocked_outcome

    changes_root_run, changes_root_reviewer_step = prepare_run_for_reviewer_outcome("reviewer-changes-cycle-1")
    changes_root_outcome = complete_reviewer(
        changes_root_reviewer_step["id"],
        "changes_requested",
        "apply requested revisions",
    )
    assert changes_root_outcome["current_run"]["run"]["status"] == "completed", changes_root_outcome
    assert changes_root_outcome["follow_up_run"] is not None, changes_root_outcome
    follow_up_cycle2 = changes_root_outcome["follow_up_run"]["run"]
    assert follow_up_cycle2["origin_type"] == "reviewer_followup", changes_root_outcome
    assert follow_up_cycle2["parent_run_id"] == changes_root_run["id"], changes_root_outcome
    assert follow_up_cycle2["origin_run_id"] == changes_root_run["id"], changes_root_outcome
    assert follow_up_cycle2["origin_step_run_id"] == changes_root_reviewer_step["id"], changes_root_outcome
    assert follow_up_cycle2["flow_id"] == changes_root_run["flow_id"], changes_root_outcome
    assert follow_up_cycle2["queue_item"]["priority_class"] == "interactive", changes_root_outcome
    assert follow_up_cycle2["queue_item"]["status"] == "queued", changes_root_outcome
    assert changes_root_outcome["flow_summary"]["created_follow_up_run_id"] == follow_up_cycle2["id"], changes_root_outcome
    assert changes_root_outcome["flow_summary"]["total_runs"] == 2, changes_root_outcome

    cycle2_run_id = follow_up_cycle2["id"]
    cycle2_executor = start_step(cycle2_run_id, "executor")["step_run"]
    finish_step(cycle2_executor["id"], "succeeded")
    cycle2_reviewer = start_step(cycle2_run_id, "reviewer")["step_run"]
    cycle2_reviewer = finish_step(cycle2_reviewer["id"], "succeeded")["step_run"]
    changes_cycle2_outcome = complete_reviewer(
        cycle2_reviewer["id"],
        "changes_requested",
        "second revision requested",
    )
    assert changes_cycle2_outcome["follow_up_run"] is not None, changes_cycle2_outcome
    follow_up_cycle3 = changes_cycle2_outcome["follow_up_run"]["run"]
    assert follow_up_cycle3["parent_run_id"] == follow_up_cycle2["id"], changes_cycle2_outcome
    assert follow_up_cycle3["flow_id"] == changes_root_run["flow_id"], changes_cycle2_outcome
    assert changes_cycle2_outcome["flow_summary"]["total_runs"] == 3, changes_cycle2_outcome

    cycle3_executor = start_step(follow_up_cycle3["id"], "executor")["step_run"]
    finish_step(cycle3_executor["id"], "succeeded")
    cycle3_reviewer = start_step(follow_up_cycle3["id"], "reviewer")["step_run"]
    cycle3_reviewer = finish_step(cycle3_reviewer["id"], "succeeded")["step_run"]
    changes_cycle3_outcome = complete_reviewer(
        cycle3_reviewer["id"],
        "changes_requested",
        "third revision requested",
    )
    assert changes_cycle3_outcome["follow_up_run"] is None, changes_cycle3_outcome
    assert changes_cycle3_outcome["current_run"]["run"]["status"] == "stopped", changes_cycle3_outcome
    assert changes_cycle3_outcome["current_run"]["run"]["queue_item"]["status"] == "cancelled", changes_cycle3_outcome
    assert changes_cycle3_outcome["flow_summary"]["stop_reason_code"] == "max_cycles_exceeded", changes_cycle3_outcome
    assert changes_cycle3_outcome["flow_summary"]["total_runs"] == 3, changes_cycle3_outcome

    changes_flow_payload = list_flow(changes_root_run["flow_id"])
    assert changes_flow_payload["ok"] is True, changes_flow_payload
    assert [flow_run["cycle_no"] for flow_run in changes_flow_payload["flow_runs"]] == [1, 2, 3], changes_flow_payload
    assert [flow_run["run"]["id"] for flow_run in changes_flow_payload["flow_runs"]] == [
        changes_root_run["id"],
        follow_up_cycle2["id"],
        follow_up_cycle3["id"],
    ], changes_flow_payload
    assert [flow_run["run"]["status"] for flow_run in changes_flow_payload["flow_runs"]] == [
        "completed",
        "completed",
        "stopped",
    ], changes_flow_payload

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

        approved_row = conn.execute(
            "SELECT status, terminal_at FROM runs WHERE id = ?",
            (approved_run["id"],),
        ).fetchone()
        assert approved_row == ("completed", approved_row[1]), approved_row
        assert approved_row[1] is not None, approved_row
        approved_queue_row = conn.execute(
            "SELECT status, terminal_at FROM queue_items WHERE run_id = ?",
            (approved_run["id"],),
        ).fetchone()
        assert approved_queue_row == ("completed", approved_queue_row[1]), approved_queue_row
        assert approved_queue_row[1] is not None, approved_queue_row

        blocked_row = conn.execute(
            "SELECT status, terminal_at FROM runs WHERE id = ?",
            (blocked_run["id"],),
        ).fetchone()
        assert blocked_row == ("stopped", blocked_row[1]), blocked_row
        assert blocked_row[1] is not None, blocked_row
        blocked_queue_row = conn.execute(
            "SELECT status, terminal_at FROM queue_items WHERE run_id = ?",
            (blocked_run["id"],),
        ).fetchone()
        assert blocked_queue_row == ("cancelled", blocked_queue_row[1]), blocked_queue_row
        assert blocked_queue_row[1] is not None, blocked_queue_row

        changes_flow_rows = conn.execute(
            """
            SELECT id, parent_run_id, origin_type, origin_run_id, origin_step_run_id, status
            FROM runs
            WHERE flow_id = ?
            ORDER BY created_at, id
            """,
            (changes_root_run["flow_id"],),
        ).fetchall()
        assert [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in changes_flow_rows] == [
            (changes_root_run["id"], None, "root_manual", None, None, "completed"),
            (
                follow_up_cycle2["id"],
                changes_root_run["id"],
                "reviewer_followup",
                changes_root_run["id"],
                changes_root_reviewer_step["id"],
                "completed",
            ),
            (
                follow_up_cycle3["id"],
                follow_up_cycle2["id"],
                "reviewer_followup",
                follow_up_cycle2["id"],
                cycle2_reviewer["id"],
                "stopped",
            ),
        ], changes_flow_rows

        changes_flow_queue_rows = conn.execute(
            """
            SELECT runs.id, queue_items.priority_class, queue_items.status
            FROM queue_items
            JOIN runs ON runs.id = queue_items.run_id
            WHERE runs.flow_id = ?
            ORDER BY runs.created_at, runs.id
            """,
            (changes_root_run["flow_id"],),
        ).fetchall()
        assert changes_flow_queue_rows == [
            (changes_root_run["id"], "interactive", "completed"),
            (follow_up_cycle2["id"], "interactive", "completed"),
            (follow_up_cycle3["id"], "interactive", "cancelled"),
        ], changes_flow_queue_rows

        run_snapshot_counts = {
            "approved": conn.execute(
                "SELECT COUNT(*) FROM run_snapshots WHERE snapshot_scope = 'run' AND run_id = ?",
                (approved_run["id"],),
            ).fetchone()[0],
            "blocked": conn.execute(
                "SELECT COUNT(*) FROM run_snapshots WHERE snapshot_scope = 'run' AND run_id = ?",
                (blocked_run["id"],),
            ).fetchone()[0],
            "changes_cycle_1": conn.execute(
                "SELECT COUNT(*) FROM run_snapshots WHERE snapshot_scope = 'run' AND run_id = ?",
                (changes_root_run["id"],),
            ).fetchone()[0],
            "changes_cycle_2": conn.execute(
                "SELECT COUNT(*) FROM run_snapshots WHERE snapshot_scope = 'run' AND run_id = ?",
                (follow_up_cycle2["id"],),
            ).fetchone()[0],
            "changes_cycle_3": conn.execute(
                "SELECT COUNT(*) FROM run_snapshots WHERE snapshot_scope = 'run' AND run_id = ?",
                (follow_up_cycle3["id"],),
            ).fetchone()[0],
        }
        assert run_snapshot_counts == {
            "approved": 1,
            "blocked": 1,
            "changes_cycle_1": 1,
            "changes_cycle_2": 1,
            "changes_cycle_3": 1,
        }, run_snapshot_counts

        flow_snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM run_snapshots WHERE snapshot_scope = 'flow' AND flow_id = ?",
            (changes_root_run["flow_id"],),
        ).fetchone()[0]
        assert flow_snapshot_count == 3, flow_snapshot_count

        actual_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    assert expected_tables.issubset(actual_tables), actual_tables

    scheduler_db_path = tmp_root / "scheduler-control-plane-v2.sqlite"
    run_command(sqlite_script, scheduler_db_path, "--json")
    run_command(
        register_script,
        "sample-project",
        "--projects-root",
        control_dir / "projects",
        "--sqlite-db",
        scheduler_db_path,
        "--json",
    )

    background_run = create_root_run(
        "scheduler-background",
        priority_class="background",
        sqlite_db=scheduler_db_path,
    )["run"]
    system_run = create_root_run(
        "scheduler-system",
        priority_class="system",
        sqlite_db=scheduler_db_path,
    )["run"]
    interactive_a_run = create_root_run(
        "scheduler-interactive-a",
        priority_class="interactive",
        sqlite_db=scheduler_db_path,
    )["run"]
    interactive_b_run = create_root_run(
        "scheduler-interactive-b",
        priority_class="interactive",
        sqlite_db=scheduler_db_path,
    )["run"]
    future_system_run = create_root_run(
        "scheduler-system-future",
        priority_class="system",
        sqlite_db=scheduler_db_path,
    )["run"]

    background_timestamp = "2026-01-01T00:00:00.000000Z"
    interactive_timestamp = "2026-01-02T00:00:00.000000Z"
    system_timestamp = "2026-01-03T00:00:00.000000Z"
    future_timestamp = "2030-01-01T00:00:00.000000Z"
    released_future_timestamp = "2030-01-02T00:00:00.000000Z"
    dispatch_retry_timestamp = "2026-01-02T00:00:00.000000Z"

    with sqlite3.connect(scheduler_db_path) as conn:
        conn.execute(
            "UPDATE queue_items SET enqueued_at = ?, available_at = ? WHERE run_id = ?",
            (background_timestamp, background_timestamp, background_run["id"]),
        )
        conn.execute(
            "UPDATE queue_items SET enqueued_at = ?, available_at = ? WHERE run_id = ?",
            (system_timestamp, system_timestamp, system_run["id"]),
        )
        conn.execute(
            "UPDATE queue_items SET enqueued_at = ?, available_at = ? WHERE run_id = ?",
            (interactive_timestamp, interactive_timestamp, interactive_a_run["id"]),
        )
        conn.execute(
            "UPDATE queue_items SET enqueued_at = ?, available_at = ? WHERE run_id = ?",
            (interactive_timestamp, interactive_timestamp, interactive_b_run["id"]),
        )
        conn.execute(
            "UPDATE queue_items SET enqueued_at = ?, available_at = ? WHERE run_id = ?",
            (future_timestamp, future_timestamp, future_system_run["id"]),
        )
        conn.commit()

    expected_interactive_order = sorted(
        [
            interactive_a_run["queue_item"]["id"],
            interactive_b_run["queue_item"]["id"],
        ]
    )

    claim_one = claim_next(scheduler_db_path, now="2026-01-04T00:00:00.000000Z")
    assert claim_one is not None
    assert claim_one["dispatch_run"]["run"]["id"] == system_run["id"], claim_one
    assert claim_one["dispatch_run"]["queue_item"]["priority_class"] == "system", claim_one
    assert claim_one["dispatch_run"]["project"]["project_key"] == "sample-project", claim_one
    assert claim_one["dispatch_run"]["project_package_root"] == str(sample_project), claim_one
    assert claim_one["dispatch_run"]["flow_context"]["cycle_no"] == 1, claim_one
    assert claim_one["transition"]["transition_type"] == "queue_item_claimed_for_dispatch", claim_one

    claim_two = claim_next(scheduler_db_path, now="2026-01-04T00:00:01.000000Z")
    assert claim_two is not None
    assert claim_two["dispatch_run"]["queue_item"]["id"] != claim_one["dispatch_run"]["queue_item"]["id"], (
        claim_one,
        claim_two,
    )
    assert claim_two["dispatch_run"]["queue_item"]["priority_class"] == "interactive", claim_two
    assert claim_two["dispatch_run"]["queue_item"]["id"] == expected_interactive_order[0], claim_two

    claim_three = claim_next(scheduler_db_path, now="2026-01-04T00:00:02.000000Z")
    assert claim_three is not None
    assert claim_three["dispatch_run"]["queue_item"]["priority_class"] == "interactive", claim_three
    assert claim_three["dispatch_run"]["queue_item"]["id"] == expected_interactive_order[1], claim_three

    release_result = release_claim(
        scheduler_db_path,
        run_id=claim_one["dispatch_run"]["run"]["id"],
        available_at=released_future_timestamp,
        note="dispatch never started",
    )
    assert release_result["dispatch_run"]["run"]["id"] == system_run["id"], release_result
    assert release_result["dispatch_run"]["queue_item"]["status"] == "queued", release_result
    assert release_result["dispatch_run"]["queue_item"]["available_at"] == released_future_timestamp, release_result
    assert release_result["dispatch_run"]["queue_item"]["claimed_at"] is None, release_result
    assert release_result["transition"]["transition_type"] == "queue_item_released_to_queue", release_result

    dispatch_failed_result = dispatch_fail_claim(
        scheduler_db_path,
        queue_item_id=claim_two["dispatch_run"]["queue_item"]["id"],
        reason_code="dispatch_failed",
        available_at=dispatch_retry_timestamp,
        note="executor stub unavailable",
    )
    assert dispatch_failed_result["dispatch_run"]["queue_item"]["status"] == "queued", dispatch_failed_result
    assert dispatch_failed_result["dispatch_run"]["queue_item"]["available_at"] == dispatch_retry_timestamp, dispatch_failed_result
    assert dispatch_failed_result["transition"]["transition_type"] == "queue_item_dispatch_failed_requeued", dispatch_failed_result
    assert dispatch_failed_result["transition"]["reason_code"] == "dispatch_failed", dispatch_failed_result

    claim_four = claim_next(scheduler_db_path, now="2026-01-04T00:00:03.000000Z")
    assert claim_four is not None
    assert claim_four["dispatch_run"]["queue_item"]["id"] == claim_two["dispatch_run"]["queue_item"]["id"], claim_four

    claim_five = claim_next(scheduler_db_path, now="2026-01-04T00:00:04.000000Z")
    assert claim_five is not None
    assert claim_five["dispatch_run"]["run"]["id"] == background_run["id"], claim_five
    assert claim_five["dispatch_run"]["queue_item"]["priority_class"] == "background", claim_five

    claim_six = claim_next(scheduler_db_path, now="2026-01-04T00:00:05.000000Z")
    assert claim_six is None, claim_six

    with sqlite3.connect(scheduler_db_path) as conn:
        released_queue_row = conn.execute(
            "SELECT status, available_at, claimed_at FROM queue_items WHERE id = ?",
            (claim_one["dispatch_run"]["queue_item"]["id"],),
        ).fetchone()
        assert released_queue_row == ("queued", released_future_timestamp, None), released_queue_row

        dispatch_transition = conn.execute(
            """
            SELECT from_state, to_state, transition_type, reason_code, metadata_json
            FROM state_transitions
            WHERE queue_item_id = ? AND transition_type = 'queue_item_dispatch_failed_requeued'
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (claim_two["dispatch_run"]["queue_item"]["id"],),
        ).fetchone()
        assert dispatch_transition is not None, claim_two
        assert dispatch_transition[0:4] == (
            "claimed",
            "queued",
            "queue_item_dispatch_failed_requeued",
            "dispatch_failed",
        ), dispatch_transition
        dispatch_transition_metadata = json.loads(dispatch_transition[4])
        assert dispatch_transition_metadata["previous_available_at"] == interactive_timestamp, dispatch_transition_metadata
        assert dispatch_transition_metadata["requeued_available_at"] == dispatch_retry_timestamp, dispatch_transition_metadata
        assert dispatch_transition_metadata["note"] == "executor stub unavailable", dispatch_transition_metadata

        released_transition = conn.execute(
            """
            SELECT from_state, to_state, transition_type, reason_code, metadata_json
            FROM state_transitions
            WHERE queue_item_id = ? AND transition_type = 'queue_item_released_to_queue'
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (claim_one["dispatch_run"]["queue_item"]["id"],),
        ).fetchone()
        assert released_transition is not None, claim_one
        assert released_transition[0:4] == (
            "claimed",
            "queued",
            "queue_item_released_to_queue",
            None,
        ), released_transition
        released_transition_metadata = json.loads(released_transition[4])
        assert released_transition_metadata["previous_available_at"] == system_timestamp, released_transition_metadata
        assert released_transition_metadata["requeued_available_at"] == released_future_timestamp, released_transition_metadata
        assert released_transition_metadata["note"] == "dispatch never started", released_transition_metadata

        claim_transition_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM state_transitions
            WHERE queue_item_id = ?
              AND transition_type = 'queue_item_claimed_for_dispatch'
            """,
            (claim_two["dispatch_run"]["queue_item"]["id"],),
        ).fetchone()[0]
        assert claim_transition_count == 2, claim_transition_count

        scheduler_queue_states = conn.execute(
            """
            SELECT runs.id, queue_items.priority_class, queue_items.status, queue_items.available_at
            FROM queue_items
            JOIN runs ON runs.id = queue_items.run_id
            ORDER BY runs.created_at, runs.id
            """
        ).fetchall()

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
                "reviewer_outcomes": {
                    "approved_run_id": approved_run["id"],
                    "blocked_run_id": blocked_run["id"],
                    "changes_flow_id": changes_root_run["flow_id"],
                    "changes_flow_run_ids": [
                        changes_root_run["id"],
                        follow_up_cycle2["id"],
                        follow_up_cycle3["id"],
                    ],
                    "changes_flow_statuses": [
                        flow_run["run"]["status"] for flow_run in changes_flow_payload["flow_runs"]
                    ],
                    "changes_flow_stop_reason_code": changes_cycle3_outcome["flow_summary"]["stop_reason_code"],
                },
                "run_snapshot_counts": run_snapshot_counts,
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
                "scheduler_claims": {
                    "claim_order_run_ids": [
                        claim_one["dispatch_run"]["run"]["id"],
                        claim_two["dispatch_run"]["run"]["id"],
                        claim_three["dispatch_run"]["run"]["id"],
                        claim_four["dispatch_run"]["run"]["id"],
                        claim_five["dispatch_run"]["run"]["id"],
                    ],
                    "claim_order_queue_item_ids": [
                        claim_one["dispatch_run"]["queue_item"]["id"],
                        claim_two["dispatch_run"]["queue_item"]["id"],
                        claim_three["dispatch_run"]["queue_item"]["id"],
                        claim_four["dispatch_run"]["queue_item"]["id"],
                        claim_five["dispatch_run"]["queue_item"]["id"],
                    ],
                    "expected_interactive_order": expected_interactive_order,
                    "released_run_id": release_result["dispatch_run"]["run"]["id"],
                    "dispatch_failed_queue_item_id": dispatch_failed_result["dispatch_run"]["queue_item"]["id"],
                    "final_queue_states": [
                        {
                            "run_id": row[0],
                            "priority_class": row[1],
                            "status": row[2],
                            "available_at": row[3],
                        }
                        for row in scheduler_queue_states
                    ],
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
PY
