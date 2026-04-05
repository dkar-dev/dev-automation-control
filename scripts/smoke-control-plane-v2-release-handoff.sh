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

import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import uuid


control_dir = Path(sys.argv[1]).resolve()
tmp_root = Path(sys.argv[2]).resolve()
package_root = tmp_root / "packages" / "demo"
artifact_root = tmp_root / "artifacts"
db_path = tmp_root / "control.sqlite"

shutil.copytree(control_dir / "projects" / "sample-project", package_root)
(package_root / "runtime.yaml").write_text(
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
    "create_handoff": control_dir / "scripts" / "create-release-handoff",
    "show_handoff": control_dir / "scripts" / "show-release-handoff",
    "list_handoffs": control_dir / "scripts" / "list-release-handoffs",
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


def create_reviewed_run(*, project_key: str, milestone: str, reviewer_verdict: str) -> dict:
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
    executor_step = run_json(
        scripts["start_step"],
        "--sqlite-db",
        db_path,
        "--run-id",
        run_payload["id"],
        "--step-key",
        "executor",
        "--json",
    )["step_run_details"]["step_run"]
    run_json(
        scripts["finish_step"],
        "--sqlite-db",
        db_path,
        executor_step["id"],
        "--status",
        "succeeded",
        "--json",
    )
    reviewer_step = run_json(
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
        reviewer_step["id"],
        "--status",
        "succeeded",
        "--json",
    )
    run_json(
        scripts["complete_review"],
        "--sqlite-db",
        db_path,
        reviewer_step["id"],
        "--verdict",
        reviewer_verdict,
        "--summary",
        f"{reviewer_verdict} for {milestone}",
        "--json",
    )
    return {
        "run": run_payload,
        "executor_step": executor_step,
        "reviewer_step": reviewer_step,
    }


def insert_executor_dispatch_manifest(*, run: dict, step_run_id: str, commit_sha: str) -> Path:
    manifest_path = (
        artifact_root
        / run["project_key"]
        / run["flow_id"]
        / run["id"]
        / "dispatch-manifests"
        / "executor"
        / "manifest.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_payload = {
        "dispatch_run": {"run": {"id": run["id"]}},
        "role_decision": {"resolved_role": "executor"},
        "dispatch_outcome": {"commit_sha": commit_sha},
    }
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    created_at = "2026-04-05T10:00:00Z"
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO artifact_refs (
              id,
              project_id,
              flow_id,
              run_id,
              step_run_id,
              artifact_kind,
              filesystem_path,
              media_type,
              size_bytes,
              checksum_sha256,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                run["project_id"],
                run["flow_id"],
                run["id"],
                step_run_id,
                "dispatch_result_manifest",
                str(manifest_path),
                "application/json",
                manifest_path.stat().st_size,
                digest,
                created_at,
            ),
        )
        connection.commit()
    return manifest_path


run_json(scripts["init"], db_path, "--json")
registration = run_json(
    scripts["register"],
    package_root,
    "--sqlite-db",
    db_path,
    "--json",
)
project_key = registration["registration"]["project"]["project_key"]

green_run = create_reviewed_run(project_key=project_key, milestone="green-release-handoff", reviewer_verdict="approved")
commit_sha = "a" * 40
dispatch_manifest_path = insert_executor_dispatch_manifest(
    run=green_run["run"],
    step_run_id=green_run["executor_step"]["id"],
    commit_sha=commit_sha,
)
green_checks = run_json(
    scripts["run_checks"],
    "--sqlite-db",
    db_path,
    "--run-id",
    green_run["run"]["id"],
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
    green_run["run"]["id"],
    "--artifact-root",
    artifact_root,
    "--json",
)
assert green_decision["deployable_green_decision"]["decision_status"] == "deployable_green", green_decision

handoff = run_json(
    scripts["create_handoff"],
    "--sqlite-db",
    db_path,
    "--run-id",
    green_run["run"]["id"],
    "--artifact-root",
    artifact_root,
    "--operator-note",
    "operator validates release checklist externally",
    "--json",
)
bundle = handoff["release_handoff"]
assert bundle["commit_sha"] == commit_sha, bundle
assert bundle["decision_status"] == "deployable_green", bundle
assert bundle["commit_source"]["container_artifact_path"] == str(dispatch_manifest_path), bundle
assert "release-ready" not in bundle["summary"].lower() or bundle["summary"], bundle

show_bundle = run_json(
    scripts["show_handoff"],
    "--sqlite-db",
    db_path,
    green_run["run"]["id"],
    "--json",
)
assert show_bundle["release_handoffs"]["latest_bundle"]["bundle_id"] == bundle["bundle_id"], show_bundle
assert len(show_bundle["release_handoffs"]["history"]) == 1, show_bundle

list_bundles = run_json(
    scripts["list_handoffs"],
    "--sqlite-db",
    db_path,
    "--project-key",
    project_key,
    "--json",
)
assert len(list_bundles["release_handoffs"]) == 1, list_bundles
assert list_bundles["release_handoffs"][0]["bundle_id"] == bundle["bundle_id"], list_bundles

manifest_path = Path(bundle["manifest_path"])
summary_path = Path(bundle["summary_path"])
artifact_index_path = Path(bundle["artifact_index_path"])
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
summary_text = summary_path.read_text(encoding="utf-8")
artifact_index = json.loads(artifact_index_path.read_text(encoding="utf-8"))
assert manifest["bundle_id"] == bundle["bundle_id"], manifest
assert manifest["commit_sha"] == commit_sha, manifest
assert manifest["deployable_green_decision_source"]["decision_id"] == green_decision["deployable_green_decision"]["decision_id"], manifest
assert commit_sha in summary_text, summary_text
assert bundle["bundle_id"] in summary_text, summary_text
assert artifact_index["generated_files"][0]["artifact_kind"] == "release_handoff_manifest", artifact_index

missing_commit_run = create_reviewed_run(project_key=project_key, milestone="missing-commit", reviewer_verdict="approved")
run_json(
    scripts["run_checks"],
    "--sqlite-db",
    db_path,
    "--run-id",
    missing_commit_run["run"]["id"],
    "--check-id",
    "required_pass",
    "--artifact-root",
    artifact_root,
    "--json",
)
run_json(
    scripts["decide_green"],
    "--sqlite-db",
    db_path,
    "--run-id",
    missing_commit_run["run"]["id"],
    "--artifact-root",
    artifact_root,
    "--json",
)
missing_commit_handoff = run_json(
    scripts["create_handoff"],
    "--sqlite-db",
    db_path,
    "--run-id",
    missing_commit_run["run"]["id"],
    "--artifact-root",
    artifact_root,
    "--json",
    expect_success=False,
)
assert missing_commit_handoff["error"]["code"] == "RELEASE_HANDOFF_COMMIT_MISSING", missing_commit_handoff

not_green_run = create_reviewed_run(project_key=project_key, milestone="not-green-handoff", reviewer_verdict="approved")
not_green_checks = run_json(
    scripts["run_checks"],
    "--sqlite-db",
    db_path,
    "--run-id",
    not_green_run["run"]["id"],
    "--check-id",
    "required_fail",
    "--artifact-root",
    artifact_root,
    "--json",
    expect_success=False,
)
assert not_green_checks["host_checks"]["verdict"] == "not_green", not_green_checks
run_json(
    scripts["decide_green"],
    "--sqlite-db",
    db_path,
    "--run-id",
    not_green_run["run"]["id"],
    "--artifact-root",
    artifact_root,
    "--json",
    expect_success=False,
)
not_green_handoff = run_json(
    scripts["create_handoff"],
    "--sqlite-db",
    db_path,
    "--run-id",
    not_green_run["run"]["id"],
    "--artifact-root",
    artifact_root,
    "--json",
    expect_success=False,
)
assert not_green_handoff["error"]["code"] == "RELEASE_HANDOFF_NOT_ELIGIBLE", not_green_handoff

with sqlite3.connect(db_path) as connection:
    handoff_rows = connection.execute("SELECT COUNT(*) FROM release_handoffs").fetchone()[0]
    handoff_artifacts = connection.execute(
        "SELECT COUNT(*) FROM artifact_refs WHERE artifact_kind IN (?, ?, ?)",
        (
            "release_handoff_manifest",
            "release_handoff_summary_markdown",
            "release_handoff_artifact_index",
        ),
    ).fetchone()[0]

assert handoff_rows == 1, handoff_rows
assert handoff_artifacts == 3, handoff_artifacts

print("release handoff smoke passed")
PY
