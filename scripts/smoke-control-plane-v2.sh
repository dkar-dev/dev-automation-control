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

    with sqlite3.connect(db_path) as conn:
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
                "sqlite_tables": sorted(actual_tables),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
PY
