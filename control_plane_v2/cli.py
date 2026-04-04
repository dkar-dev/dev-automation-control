from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .project_package import load_project_package, resolve_project_package_root
from .project_package_validator import ProjectPackageValidationFailed
from .project_registry import ProjectRegistryError, list_registered_projects, register_project_package
from .run_persistence import (
    PRIORITY_CLASSES,
    RUN_STATUSES,
    RootRunCreateRequest,
    RunPersistenceError,
    create_root_run,
    get_run,
    list_runs,
)
from .reviewer_outcome_persistence import (
    REVIEWER_VERDICTS,
    ReviewerOutcomeError,
    complete_reviewer_outcome,
    list_flow_runs,
)
from .reviewer_result_ingestion import (
    ReviewerResultIngestionError,
    ingest_reviewer_result,
    inspect_reviewer_result,
)
from .manual_control import (
    MANUAL_RESUME_MODES,
    ManualControlError,
    force_stop_run,
    pause_run,
    rerun_run_step,
    resume_run,
    show_run_control_state,
)
from .scheduler_persistence import (
    SchedulerPersistenceError,
    claim_next_run,
    mark_claimed_run_dispatch_failed,
    release_claimed_run,
)
from .step_run_persistence import (
    STEP_KEYS,
    STEP_RUN_TERMINAL_STATUSES,
    STEP_RUN_STATUSES,
    StepRunPersistenceError,
    finish_step_run,
    get_step_run,
    list_step_runs,
    retry_step_run,
    start_step_run,
)
from .sqlite_bootstrap import initialize_sqlite_v1
from .sqlite_migrations import (
    SQLiteMigrationError,
    get_sqlite_schema_version,
    list_sqlite_migrations,
    migrate_sqlite_v1,
)
from .dispatch_adapter import (
    DISPATCH_PAYLOAD_INVALID,
    DispatchAdapterError,
    dispatch_claimed_run,
)
from .worker_loop import (
    WorkerLoopError,
    WorkerRuntimeConfig,
    run_worker_tick,
    run_worker_until_idle,
)
from .runtime_cleanup_manager import (
    CLEANUP_SCOPES,
    CleanupManagerError,
    list_cleanup_candidates,
    run_cleanup_once,
    show_cleanup_status,
)
from .http_api import (
    API_DEFAULT_HOST,
    API_DEFAULT_PORT,
    API_ENV_ARTIFACT_ROOT,
    API_ENV_HOST,
    API_ENV_PORT,
    API_ENV_SQLITE_DB,
    API_ENV_WORKER_LOG_ROOT,
    API_ENV_WORKSPACE_ROOT,
    ControlPlaneApiConfigError,
    create_control_plane_api_config,
    serve_control_plane_api,
)
from .task_intake import (
    TaskIntakeError,
    list_submitted_tasks,
    show_submitted_task,
    submit_bounded_task,
)


CONTROL_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PROJECTS_ROOT = CONTROL_DIR / "projects"
DEFAULT_SQLITE_SCHEMA = CONTROL_DIR / "schemas" / "sqlite-v1.sql"
DEFAULT_SQLITE_MIGRATIONS = CONTROL_DIR / "schemas" / "migrations"


def main_validate_project_package(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a Control Plane v2 project package.")
    parser.add_argument("package", help="Project key under projects/ or an explicit package path")
    parser.add_argument(
        "--projects-root",
        default=str(DEFAULT_PROJECTS_ROOT),
        help=f"Projects root used when <package> is a project key (default: {DEFAULT_PROJECTS_ROOT})",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    package_root = resolve_project_package_root(args.package, args.projects_root)

    try:
        project_package = load_project_package(package_root)
    except ProjectPackageValidationFailed as exc:
        payload = {
            "ok": False,
            "package_root": str(exc.package_root),
            "errors": [error.to_dict() for error in exc.errors],
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Project package validation failed: {exc.package_root}", file=sys.stderr)
            for error in exc.errors:
                print(_format_validation_error(error), file=sys.stderr)
        return 1

    payload = {"ok": True, "package": project_package.to_dict()}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Project package is valid: {project_package.package_root}")
        print(f"Project key: {project_package.project_key}")
        print(f"Schema version: {project_package.schema_version}")
        print("Files: " + ", ".join(sorted(project_package.files)))
    return 0


def main_init_sqlite_v1(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Initialize the SQLite v1 schema.")
    parser.add_argument("database_path", help="SQLite database file to create or initialize")
    parser.add_argument(
        "--schema",
        default=str(DEFAULT_SQLITE_SCHEMA),
        help=f"Schema SQL file to apply (default: {DEFAULT_SQLITE_SCHEMA})",
    )
    parser.add_argument(
        "--migrations-root",
        default=str(DEFAULT_SQLITE_MIGRATIONS),
        help=f"SQLite migrations directory (default: {DEFAULT_SQLITE_MIGRATIONS})",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = initialize_sqlite_v1(args.database_path, args.schema, args.migrations_root)
    except SQLiteMigrationError as exc:
        payload = {"ok": False, "stage": "sqlite_migrations", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"SQLite init failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1
    payload = {"ok": True, "database": result.to_dict()}

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"SQLite schema ready: {result.database_path}")
        print(f"Schema file: {result.schema_path}")
        print(f"Migrations root: {result.migrations_root}")
        print(f"Operation: {result.operation}")
        print(f"Current version: {result.current_version:04d} ({result.current_name})")
        print("Tables: " + ", ".join(result.tables))
    return 0


def main_migrate_sqlite_v1(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Migrate an existing SQLite database to the latest Control Plane v2 schema.")
    parser.add_argument("database_path", help="SQLite database file to migrate")
    parser.add_argument(
        "--schema",
        default=str(DEFAULT_SQLITE_SCHEMA),
        help=f"Latest schema snapshot used for fresh bootstrap policy (default: {DEFAULT_SQLITE_SCHEMA})",
    )
    parser.add_argument(
        "--migrations-root",
        default=str(DEFAULT_SQLITE_MIGRATIONS),
        help=f"SQLite migrations directory (default: {DEFAULT_SQLITE_MIGRATIONS})",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = migrate_sqlite_v1(args.database_path, schema_path=args.schema, migrations_root=args.migrations_root)
    except SQLiteMigrationError as exc:
        payload = {"ok": False, "stage": "sqlite_migrations", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"SQLite migrate failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {"ok": True, "migration": result.to_dict()}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"SQLite schema ready: {result.database_path}")
        print(f"Operation: {result.operation}")
        print(f"Version: {result.version_before:04d} -> {result.version_after:04d}")
        print("Recorded migrations: " + ", ".join(migration.version_label for migration in result.recorded_migrations) if result.recorded_migrations else "Recorded migrations: none")
        print("Executed migrations: " + ", ".join(migration.version_label for migration in result.executed_migrations) if result.executed_migrations else "Executed migrations: none")
    return 0


def main_show_sqlite_schema_version(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show the current SQLite schema version for Control Plane v2.")
    parser.add_argument("database_path", help="SQLite database file to inspect")
    parser.add_argument(
        "--migrations-root",
        default=str(DEFAULT_SQLITE_MIGRATIONS),
        help=f"SQLite migrations directory (default: {DEFAULT_SQLITE_MIGRATIONS})",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = get_sqlite_schema_version(args.database_path, migrations_root=args.migrations_root)
    except SQLiteMigrationError as exc:
        payload = {"ok": False, "stage": "sqlite_migrations", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"SQLite schema version lookup failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {"ok": True, "schema_version": result.to_dict()}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        tracking = "tracked" if result.tracked else "untracked"
        print(f"SQLite schema: {result.database_path}")
        print(f"State: {result.detected_state} ({tracking})")
        print(f"Current version: {result.current_version:04d} ({result.current_name})")
        print(f"Latest version: {result.latest_version:04d} ({result.latest_name})")
        print(
            "Pending migrations: "
            + (", ".join(migration.version_label for migration in result.pending_migrations) if result.pending_migrations else "none")
        )
    return 0


def main_list_sqlite_migrations(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List discovered Control Plane v2 SQLite migrations.")
    parser.add_argument(
        "--migrations-root",
        default=str(DEFAULT_SQLITE_MIGRATIONS),
        help=f"SQLite migrations directory (default: {DEFAULT_SQLITE_MIGRATIONS})",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        migrations = list_sqlite_migrations(args.migrations_root)
    except SQLiteMigrationError as exc:
        payload = {"ok": False, "stage": "sqlite_migrations", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"SQLite migration discovery failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "migrations_root": str(Path(args.migrations_root).expanduser().resolve()),
        "migrations": [migration.to_dict() for migration in migrations],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"SQLite migrations: {payload['migrations_root']}")
        for migration in migrations:
            print(f"- {migration.version_label} {migration.name}: {migration.path}")
    return 0


def main_register_project_package(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Register a validated project package in SQLite.")
    parser.add_argument("package", help="Project key under projects/ or an explicit package path")
    parser.add_argument(
        "--sqlite-db",
        required=True,
        help="SQLite database path bootstrapped with init-sqlite-v1",
    )
    parser.add_argument(
        "--projects-root",
        default=str(DEFAULT_PROJECTS_ROOT),
        help=f"Projects root used when <package> is a project key (default: {DEFAULT_PROJECTS_ROOT})",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    package_root = resolve_project_package_root(args.package, args.projects_root)

    try:
        project_package = load_project_package(package_root)
    except ProjectPackageValidationFailed as exc:
        payload = {
            "ok": False,
            "stage": "validation",
            "package_root": str(exc.package_root),
            "errors": [error.to_dict() for error in exc.errors],
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Project package validation failed: {exc.package_root}", file=sys.stderr)
            for error in exc.errors:
                print(_format_validation_error(error), file=sys.stderr)
        return 1

    try:
        result = register_project_package(args.sqlite_db, project_package)
    except ProjectRegistryError as exc:
        payload = {
            "ok": False,
            "stage": "registry",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Project registry failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "registration": result.to_dict(),
        "package": {
            "project_key": project_package.project_key,
            "package_root": str(project_package.package_root),
            "schema_version": project_package.schema_version,
        },
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Project registered: {result.project.project_key}")
        print(f"Action: {result.action}")
        print(f"ID: {result.project.id}")
        print(f"Package root: {result.project.package_root}")
        print(f"Created at: {result.project.created_at}")
        print(f"Updated at: {result.project.updated_at}")
    return 0


def main_list_registered_projects(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List registered projects from SQLite.")
    parser.add_argument(
        "--sqlite-db",
        required=True,
        help="SQLite database path bootstrapped with init-sqlite-v1",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        projects = list_registered_projects(args.sqlite_db)
    except ProjectRegistryError as exc:
        payload = {
            "ok": False,
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to list registered projects: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "projects": [project.to_dict() for project in projects],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Registered projects: {len(projects)}")
        for project in projects:
            print(
                f"- {project.project_key} | {project.package_root} | "
                f"created_at={project.created_at} | updated_at={project.updated_at}"
            )
    return 0


def main_create_root_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create a root run for a registered project.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("--project-key", required=True, help="Registered project key")
    parser.add_argument("--project-profile", required=True, help="Immutable project profile for the run")
    parser.add_argument("--workflow-id", required=True, help="Immutable workflow identifier for the run")
    parser.add_argument("--milestone", required=True, help="Immutable milestone value for the run")
    parser.add_argument(
        "--priority-class",
        default="interactive",
        choices=PRIORITY_CLASSES,
        help="Queue priority class for the root run (default: interactive)",
    )
    parser.add_argument(
        "--artifact-root",
        help="Optional artifact root where <project-key>/<flow_id>/<run_id>/ will be created",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    request = RootRunCreateRequest(
        project_key=args.project_key,
        project_profile=args.project_profile,
        workflow_id=args.workflow_id,
        milestone=args.milestone,
        priority_class=args.priority_class,
        artifact_root=Path(args.artifact_root).expanduser().resolve() if args.artifact_root else None,
    )

    try:
        result = create_root_run(args.sqlite_db, request)
    except RunPersistenceError as exc:
        payload = {
            "ok": False,
            "stage": "run_persistence",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Root run creation failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "run_details": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Root run created: {result.run.id}")
        print(f"Project key: {result.run.project_key}")
        print(f"Flow ID: {result.run.flow_id}")
        print(f"Status: {result.run.status}")
        if result.run.queue_item is not None:
            print(f"Queue item: {result.run.queue_item.id} ({result.run.queue_item.priority_class}/{result.run.queue_item.status})")
        if result.artifact_directory is not None:
            print(f"Artifact directory: {result.artifact_directory}")
    return 0


def main_list_runs(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List persisted Control Plane v2 runs.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("--project-key", help="Filter by registered project key")
    parser.add_argument("--status", choices=RUN_STATUSES, help="Filter by run status")
    parser.add_argument("--project-profile", help="Filter by project profile")
    parser.add_argument("--workflow-id", help="Filter by workflow id")
    parser.add_argument("--milestone", help="Filter by milestone")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of runs to return")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        runs = list_runs(
            args.sqlite_db,
            project_key=args.project_key,
            status=args.status,
            project_profile=args.project_profile,
            workflow_id=args.workflow_id,
            milestone=args.milestone,
            limit=args.limit,
        )
    except RunPersistenceError as exc:
        payload = {
            "ok": False,
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to list runs: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "runs": [run.to_dict() for run in runs],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Runs: {len(runs)}")
        for run in runs:
            queue_suffix = ""
            if run.queue_item is not None:
                queue_suffix = f" | queue={run.queue_item.priority_class}/{run.queue_item.status}"
            print(
                f"- {run.id} | {run.project_key} | {run.project_profile} | {run.workflow_id} | "
                f"{run.milestone} | status={run.status}{queue_suffix}"
            )
    return 0


def main_show_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show one persisted Control Plane v2 run.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("run_id", help="Run identifier")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        run_details = get_run(args.sqlite_db, args.run_id)
    except RunPersistenceError as exc:
        payload = {
            "ok": False,
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to load run: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "run_details": run_details.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Run: {run_details.run.id}")
        print(f"Project key: {run_details.run.project_key}")
        print(f"Flow ID: {run_details.run.flow_id}")
        print(f"Status: {run_details.run.status}")
        print(f"Transitions: {len(run_details.state_transitions)}")
        print(f"Snapshots: {len(run_details.run_snapshots)}")
    return 0


def main_start_step_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start a new step_run for an existing run.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("--run-id", required=True, help="Run identifier")
    parser.add_argument("--step-key", required=True, choices=STEP_KEYS, help="Step key to start")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        step_run_details = start_step_run(args.sqlite_db, args.run_id, args.step_key)
    except StepRunPersistenceError as exc:
        payload = {
            "ok": False,
            "stage": "step_run_persistence",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to start step_run: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "step_run_details": step_run_details.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"step_run started: {step_run_details.step_run.id}")
        print(f"Run ID: {step_run_details.step_run.run_id}")
        print(f"Step key: {step_run_details.step_run.step_key}")
        print(f"Attempt: {step_run_details.step_run.attempt_no}")
        print(f"Status: {step_run_details.step_run.status}")
    return 0


def main_finish_step_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Finish a running step_run with a terminal status.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("step_run_id", help="step_run identifier")
    parser.add_argument(
        "--status",
        required=True,
        choices=STEP_RUN_TERMINAL_STATUSES,
        help="Terminal status to persist",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        step_run_details = finish_step_run(args.sqlite_db, args.step_run_id, args.status)
    except StepRunPersistenceError as exc:
        payload = {
            "ok": False,
            "stage": "step_run_persistence",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to finish step_run: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "step_run_details": step_run_details.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"step_run finished: {step_run_details.step_run.id}")
        print(f"Status: {step_run_details.step_run.status}")
        print(f"Terminal at: {step_run_details.step_run.terminal_at}")
    return 0


def main_retry_step_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create a retry step_run from a terminal predecessor.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("step_run_id", help="Terminal predecessor step_run identifier")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        step_run_details = retry_step_run(args.sqlite_db, args.step_run_id)
    except StepRunPersistenceError as exc:
        payload = {
            "ok": False,
            "stage": "step_run_persistence",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to retry step_run: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "step_run_details": step_run_details.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"retry step_run created: {step_run_details.step_run.id}")
        print(f"Previous step_run: {step_run_details.step_run.previous_step_run_id}")
        print(f"Attempt: {step_run_details.step_run.attempt_no}")
        print(f"Status: {step_run_details.step_run.status}")
    return 0


def main_list_step_runs(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List persisted step_runs.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("--run-id", help="Filter by run id")
    parser.add_argument("--step-key", choices=STEP_KEYS, help="Filter by step key")
    parser.add_argument("--status", choices=STEP_RUN_STATUSES, help="Filter by step_run status")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of rows to return")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        step_runs = list_step_runs(
            args.sqlite_db,
            run_id=args.run_id,
            step_key=args.step_key,
            status=args.status,
            limit=args.limit,
        )
    except StepRunPersistenceError as exc:
        payload = {
            "ok": False,
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to list step_runs: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "step_runs": [step_run.to_dict() for step_run in step_runs],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"step_runs: {len(step_runs)}")
        for step_run in step_runs:
            print(
                f"- {step_run.id} | run={step_run.run_id} | {step_run.step_key} "
                f"attempt={step_run.attempt_no} | status={step_run.status}"
            )
    return 0


def main_show_step_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show one persisted step_run.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("step_run_id", help="step_run identifier")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        step_run_details = get_step_run(args.sqlite_db, args.step_run_id)
    except StepRunPersistenceError as exc:
        payload = {
            "ok": False,
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to load step_run: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "step_run_details": step_run_details.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"step_run: {step_run_details.step_run.id}")
        print(f"Run ID: {step_run_details.step_run.run_id}")
        print(f"Step key: {step_run_details.step_run.step_key}")
        print(f"Attempt: {step_run_details.step_run.attempt_no}")
        print(f"Status: {step_run_details.step_run.status}")
        print(f"Transitions: {len(step_run_details.state_transitions)}")
    return 0


def main_complete_reviewer_outcome(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Complete a terminal reviewer step_run with a semantic verdict.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("step_run_id", help="Terminal reviewer step_run identifier")
    parser.add_argument("--verdict", required=True, choices=REVIEWER_VERDICTS, help="Reviewer verdict")
    parser.add_argument("--summary", help="Optional short reviewer outcome summary or reason text")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = complete_reviewer_outcome(args.sqlite_db, args.step_run_id, args.verdict, summary_text=args.summary)
    except ReviewerOutcomeError as exc:
        payload = {
            "ok": False,
            "stage": "reviewer_outcome_persistence",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to complete reviewer outcome: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "reviewer_outcome": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Reviewer outcome completed: {result.verdict}")
        print(f"Current run: {result.current_run.run.id} ({result.current_run.run.status})")
        if result.follow_up_run is not None:
            print(f"Follow-up run: {result.follow_up_run.run.id} ({result.follow_up_run.run.status})")
        else:
            print("Follow-up run: none")
        print(f"Flow: {result.flow_summary.flow_id} | total_runs={result.flow_summary.total_runs}")
    return 0


def main_ingest_reviewer_result(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract reviewer semantic verdict from dispatch artifacts and complete the v2 reviewer outcome."
    )
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--step-run-id", help="Terminal reviewer step_run identifier")
    target_group.add_argument("--dispatch-result-manifest", help="Dispatch result manifest JSON path")
    parser.add_argument(
        "--verdict",
        choices=REVIEWER_VERDICTS,
        help="Optional explicit override verdict for manual recovery or debug mode",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = ingest_reviewer_result(
            args.sqlite_db,
            reviewer_step_run_id=args.step_run_id,
            dispatch_result_manifest_path=args.dispatch_result_manifest,
            override_verdict=args.verdict,
        )
    except (ReviewerResultIngestionError, ReviewerOutcomeError) as exc:
        payload = {
            "ok": False,
            "stage": "reviewer_result_ingestion",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Reviewer result ingestion failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "ingestion": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Reviewer result ingested: {result.reviewer_outcome.verdict}")
        print(f"step_run: {result.inspection.reviewer_step_run_id}")
        print(f"Outcome source: {result.inspection.selected_result.verdict_source_kind}")
        print(f"Current run: {result.reviewer_outcome.current_run.run.id} ({result.reviewer_outcome.current_run.run.status})")
        if result.reviewer_outcome.follow_up_run is not None:
            print(f"Follow-up run: {result.reviewer_outcome.follow_up_run.run.id}")
    return 0


def main_show_dispatch_result(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inspect reviewer dispatch result artifacts and show the extracted semantic reviewer verdict."
    )
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--step-run-id", help="Terminal reviewer step_run identifier")
    target_group.add_argument("--dispatch-result-manifest", help="Dispatch result manifest JSON path")
    parser.add_argument(
        "--verdict",
        choices=REVIEWER_VERDICTS,
        help="Optional explicit override verdict to preview manual recovery mode",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = inspect_reviewer_result(
            args.sqlite_db,
            reviewer_step_run_id=args.step_run_id,
            dispatch_result_manifest_path=args.dispatch_result_manifest,
            override_verdict=args.verdict,
        )
    except ReviewerResultIngestionError as exc:
        payload = {
            "ok": False,
            "stage": "reviewer_result_ingestion",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to inspect reviewer dispatch result: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "inspection": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"step_run: {result.reviewer_step_run_id}")
        print(f"Selected verdict: {result.selected_result.verdict}")
        print(f"Verdict source: {result.selected_result.verdict_source_kind}")
        print(f"Warnings: {len(result.warnings)}")
    return 0


def main_pause_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pause a queued or claimed-but-not-started run in Control Plane v2.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("run_id", help="Run identifier")
    parser.add_argument("--note", help="Optional operator note")
    parser.add_argument("--operator", help="Optional operator identity")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = pause_run(args.sqlite_db, args.run_id, note=args.note, operator=args.operator)
    except ManualControlError as exc:
        payload = {"ok": False, "stage": "manual_control", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Pause failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "manual_control": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Paused run: {result.run.run.id}")
        print(f"Run status: {result.run.run.status}")
        print(f"Queue status: {result.control_state.queue_status}")
    return 0


def main_resume_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resume a paused Control Plane v2 run.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("run_id", help="Run identifier")
    parser.add_argument(
        "--mode",
        default="normal",
        choices=MANUAL_RESUME_MODES,
        help="Resume mode (default: normal)",
    )
    parser.add_argument("--note", help="Optional operator note")
    parser.add_argument("--operator", help="Optional operator identity")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = resume_run(
            args.sqlite_db,
            args.run_id,
            mode=args.mode,
            note=args.note,
            operator=args.operator,
        )
    except ManualControlError as exc:
        payload = {"ok": False, "stage": "manual_control", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Resume failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "manual_control": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Resumed run: {result.run.run.id}")
        print(f"Resume mode: {result.control_state.latest_resume_mode or 'normal'}")
        print(f"Queue status: {result.control_state.queue_status}")
    return 0


def main_force_stop_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Force-stop a non-terminal Control Plane v2 run.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("run_id", help="Run identifier")
    parser.add_argument("--note", help="Optional operator note")
    parser.add_argument("--operator", help="Optional operator identity")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = force_stop_run(args.sqlite_db, args.run_id, note=args.note, operator=args.operator)
    except ManualControlError as exc:
        payload = {"ok": False, "stage": "manual_control", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Force-stop failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "manual_control": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Force-stopped run: {result.run.run.id}")
        print(f"Run status: {result.run.run.status}")
        print(f"Queue status: {result.control_state.queue_status}")
    return 0


def main_rerun_run_step(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Request a narrow rerun from one terminal executor/reviewer step path.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("step_run_id", help="Terminal step_run identifier")
    parser.add_argument("--note", help="Optional operator note")
    parser.add_argument("--operator", help="Optional operator identity")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = rerun_run_step(args.sqlite_db, args.step_run_id, note=args.note, operator=args.operator)
    except ManualControlError as exc:
        payload = {"ok": False, "stage": "manual_control", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Rerun failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "manual_control": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Rerun requested for run: {result.run.run.id}")
        print(f"Source step_run: {result.source_step_run.step_run.id if result.source_step_run is not None else 'none'}")
        print(f"Pending rerun: {'yes' if result.control_state.pending_rerun is not None else 'no'}")
    return 0


def main_show_run_control_state(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show derived Control Plane v2 manual-control state for one run.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("run_id", help="Run identifier")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        control_state = show_run_control_state(args.sqlite_db, args.run_id)
    except ManualControlError as exc:
        payload = {"ok": False, "stage": "manual_control", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to load run control state: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "run_control_state": control_state.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Run: {control_state.run_id}")
        print(f"Run status: {control_state.run_status}")
        print(f"Queue status: {control_state.queue_status or 'none'}")
        print(f"Scheduling eligible: {control_state.scheduling_eligible}")
        print(f"Pending rerun: {'yes' if control_state.pending_rerun is not None else 'no'}")
    return 0


def main_run_worker_tick(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one single-worker Control Plane v2 orchestration tick.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    _add_worker_runtime_arguments(parser)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = run_worker_tick(
            args.sqlite_db,
            runtime_config=_build_worker_runtime_config_from_args(args),
        )
    except WorkerLoopError as exc:
        payload = {
            "ok": False,
            "stage": "worker_loop",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Worker tick failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "worker_tick": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Worker tick status: {result.status}")
        print(f"Claimed run: {result.claimed_run_id or 'none'}")
        print(f"Roles dispatched: {', '.join(result.roles_dispatched) if result.roles_dispatched else 'none'}")
        print(f"Reviewer ingestion: {'yes' if result.reviewer_ingestion_happened else 'no'}")
        print(f"Follow-up run: {result.follow_up_run_id or 'none'}")
        print(f"Summary JSON: {result.summary_paths.json_path}")
    return 0 if result.status in {"idle", "progressed", "stopped"} else 1


def main_run_worker_until_idle(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the single-worker Control Plane v2 loop until idle or a bound is hit.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    _add_worker_runtime_arguments(parser)
    parser.add_argument("--max-ticks", type=int, default=100, help="Maximum ticks to execute (default: 100)")
    parser.add_argument("--max-claims", type=int, help="Maximum claimed runs to process")
    parser.add_argument("--max-flows", type=int, help="Maximum distinct flow_ids to process")
    parser.add_argument("--max-wall-clock-seconds", type=float, help="Maximum wall-clock duration for this loop")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = run_worker_until_idle(
            args.sqlite_db,
            runtime_config=_build_worker_runtime_config_from_args(args),
            max_ticks=args.max_ticks,
            max_claims=args.max_claims,
            max_flows=args.max_flows,
            max_wall_clock_seconds=args.max_wall_clock_seconds,
        )
    except WorkerLoopError as exc:
        payload = {
            "ok": False,
            "stage": "worker_loop",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Worker loop failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "worker_loop": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Worker loop ended: {result.ended_reason}")
        print(f"Ticks executed: {result.ticks_executed}")
        print(f"Claims processed: {result.claims_processed}")
        print(f"Follow-ups created: {result.follow_ups_created}")
        print(f"Summary JSON: {result.summary_paths.json_path}")
    return 0 if result.ended_reason not in {"dispatch_failed", "ingestion_failed"} else 1


def main_list_cleanup_candidates(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List terminal Control Plane v2 cleanup candidates eligible by TTL.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument(
        "--scope",
        action="append",
        choices=CLEANUP_SCOPES,
        help="Restrict to one cleanup scope; may be repeated",
    )
    parser.add_argument("--now", help="Optional ISO-8601 timestamp used for TTL eligibility evaluation")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = list_cleanup_candidates(
            args.sqlite_db,
            now=args.now,
            scopes=args.scope,
        )
    except CleanupManagerError as exc:
        payload = {
            "ok": False,
            "stage": "runtime_cleanup_manager",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Cleanup candidate listing failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "cleanup_candidates": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        counts = payload["cleanup_candidates"]["counts"]
        print(f"Cleanup candidates as of: {result.as_of}")
        print(f"Scopes: {', '.join(result.scopes)}")
        print(
            "Counts: "
            + ", ".join(f"{scope}={counts.get(scope, 0)}" for scope in CLEANUP_SCOPES)
        )
        for candidate in result.candidates:
            location = candidate.filesystem_path or candidate.branch_name or candidate.target_identity
            print(
                f"- {candidate.scope} | target={candidate.target_identity} | "
                f"expires_at={candidate.expires_at} | location={location}"
            )
    return 0


def main_run_cleanup_once(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one bounded Control Plane v2 cleanup pass.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument(
        "--scope",
        action="append",
        choices=CLEANUP_SCOPES,
        help="Restrict to one cleanup scope; may be repeated",
    )
    parser.add_argument("--dry-run", action="store_true", help="List and simulate cleanup without deleting anything")
    parser.add_argument("--now", help="Optional ISO-8601 timestamp used for TTL eligibility evaluation")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = run_cleanup_once(
            args.sqlite_db,
            dry_run=args.dry_run,
            now=args.now,
            scopes=args.scope,
        )
    except CleanupManagerError as exc:
        payload = {
            "ok": False,
            "stage": "runtime_cleanup_manager",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Cleanup pass failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "cleanup_pass": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Cleanup pass as of: {result.as_of}")
        print(f"Dry run: {result.dry_run}")
        for scope in CLEANUP_SCOPES:
            summary = payload["cleanup_pass"]["summary"][scope]
            print(
                f"- {scope}: processed={summary['processed']} deleted={summary['deleted']} "
                f"errors={summary['errors']} missing={summary['missing']} dry_run={summary['dry_run']}"
            )
        if result.results:
            print("Results:")
            for item in result.results:
                print(
                    f"- {item.scope} | target={item.target_identity} | "
                    f"status={item.status} | action={item.action}"
                )
    return 0 if all(item.status != "error" for item in result.results) else 1


def main_show_cleanup_status(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show persisted Control Plane v2 cleanup audit state.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("--run-id", help="Optional run identifier filter")
    parser.add_argument("--limit", type=int, default=200, help="Maximum number of audit entries to return")
    parser.add_argument("--now", help="Optional ISO-8601 timestamp used for eligible candidate evaluation")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = show_cleanup_status(
            args.sqlite_db,
            run_id=args.run_id,
            limit=args.limit,
            now=args.now,
        )
    except CleanupManagerError as exc:
        payload = {
            "ok": False,
            "stage": "runtime_cleanup_manager",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Cleanup status lookup failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "cleanup_status": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Cleanup status as of: {result.as_of}")
        print(f"Run filter: {result.run_id or 'none'}")
        print(f"Audit entries: {len(result.entries)}")
        print(f"Eligible candidates: {len(result.eligible_candidates)}")
        for entry in result.entries:
            location = entry.filesystem_path or entry.branch_name or entry.target_identity
            print(
                f"- {entry.scope} | run={entry.run_id} | target={entry.target_identity} | "
                f"status={entry.cleanup_status or 'pending'} | location={location}"
            )
    return 0


def main_run_control_plane_api(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the local Control Plane v2 HTTP API.")
    _add_control_plane_api_arguments(parser)
    args = parser.parse_args(argv)

    try:
        config = _build_control_plane_api_config_from_args(args)
    except ControlPlaneApiConfigError as exc:
        print(f"Control Plane API config failed: {exc.message}", file=sys.stderr)
        if exc.details:
            print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    serve_control_plane_api(config)
    return 0


def main_show_control_plane_config(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show the effective local Control Plane v2 HTTP API config.")
    _add_control_plane_api_arguments(parser)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        config = _build_control_plane_api_config_from_args(args)
    except ControlPlaneApiConfigError as exc:
        payload = {"ok": False, "stage": "http_api", "error": exc.to_dict()}
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Control Plane API config failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {"ok": True, "control_plane_api": config.to_dict()}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Base URL: {config.base_url}")
        print(f"SQLite DB: {config.sqlite_db}")
        print(f"Default artifact root: {config.default_artifact_root or 'none'}")
        print(f"Default workspace root: {config.default_workspace_root or 'none'}")
        print(f"Default worker log root: {config.default_worker_log_root or 'none'}")
        print("Bind policy: localhost-only")
    return 0


def main_submit_bounded_task(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Submit one bounded task into Control Plane v2.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("--submission-json", help="Optional JSON file (or - for stdin) with the bounded task submission payload")
    parser.add_argument("--project-key", help="Registered project key")
    parser.add_argument("--task-text", help="Bounded task text")
    parser.add_argument("--project-profile", help="Immutable project profile for the run")
    parser.add_argument("--workflow-id", help="Immutable workflow identifier for the run")
    parser.add_argument("--milestone", help="Immutable milestone value for the run")
    parser.add_argument(
        "--priority-class",
        choices=PRIORITY_CLASSES,
        help="Optional priority class override",
    )
    parser.add_argument("--instruction-profile", help="Optional instruction_profile override")
    parser.add_argument("--instruction-overlay", action="append", dest="instruction_overlays", help="Append one instruction overlay override")
    parser.add_argument("--source", help="Optional submission source override")
    parser.add_argument("--thread-label", help="Optional thread_label override")
    parser.add_argument("--constraint", action="append", dest="constraints", help="Append one task constraint")
    parser.add_argument("--expected-output", action="append", dest="expected_output", help="Append one expected-output line")
    parser.add_argument("--artifact-root", help="Optional artifact root override")
    parser.add_argument("--workspace-root", help="Optional workspace root override")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        payload = _load_json_argument(args.submission_json) if args.submission_json else {}
        payload = _merge_submission_cli_overrides(payload, args)
        result = submit_bounded_task(args.sqlite_db, payload)
    except DispatchAdapterError as exc:
        payload = {
            "ok": False,
            "stage": "task_intake",
            "error": {
                "code": exc.code,
                "message": exc.message,
                "database_path": str(exc.database_path),
                "details": exc.details,
            },
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Bounded task submission failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1
    except TaskIntakeError as exc:
        payload = {
            "ok": False,
            "stage": "task_intake",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Bounded task submission failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "submitted_task": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Submitted run: {result.run_details.run.id}")
        print(f"Flow: {result.run_details.run.flow_id}")
        print(f"Queue status: {result.run_details.run.queue_item.status if result.run_details.run.queue_item is not None else 'missing'}")
        print(f"Task source: {result.runtime_context['source']}")
    return 0


def main_show_submitted_task(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show the persisted bounded task submission manifests for one run.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("run_id", help="Run identifier")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = show_submitted_task(args.sqlite_db, args.run_id)
    except TaskIntakeError as exc:
        payload = {
            "ok": False,
            "stage": "task_intake",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Submitted task lookup failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "submitted_task": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Run: {result.run_details.run.id}")
        print(f"Task text: {result.submission_manifest['submission']['task_text']}")
        print(f"Runtime source: {result.runtime_context_manifest['runtime_context']['source']}")
        print(f"Artifacts: {len(result.artifacts)}")
    return 0


def main_list_submitted_tasks(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List persisted bounded task submissions.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("--project-key", help="Optional registered project key filter")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of submitted tasks to return")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = list_submitted_tasks(
            args.sqlite_db,
            project_key=args.project_key,
            limit=args.limit,
        )
    except TaskIntakeError as exc:
        payload = {
            "ok": False,
            "stage": "task_intake",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Submitted task listing failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "submitted_tasks": [item.to_dict() for item in result],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Submitted tasks: {len(result)}")
        for item in result:
            print(
                f"- {item.run_id} | {item.project_key} | {item.workflow_id} | "
                f"status={item.run_status} queue={item.queue_status or 'none'}"
            )
    return 0


def main_list_flow_runs(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List all runs inside one flow_id chain.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument("flow_id", help="Flow identifier")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of rows to return")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        flow_runs = list_flow_runs(args.sqlite_db, args.flow_id, limit=args.limit)
    except ReviewerOutcomeError as exc:
        payload = {
            "ok": False,
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to list flow runs: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "flow_id": args.flow_id,
        "flow_runs": [flow_run.to_dict() for flow_run in flow_runs],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Flow runs: {len(flow_runs)}")
        for flow_run in flow_runs:
            queue_suffix = ""
            if flow_run.run.queue_item is not None:
                queue_suffix = f" | queue={flow_run.run.queue_item.priority_class}/{flow_run.run.queue_item.status}"
            print(
                f"- cycle={flow_run.cycle_no} | {flow_run.run.id} | status={flow_run.run.status} "
                f"| origin={flow_run.run.origin_type}{queue_suffix}"
            )
    return 0


def main_claim_next_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Atomically claim the next runnable queued run.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    parser.add_argument(
        "--now",
        help="Optional ISO-8601 timestamp used for eligibility and effective-age evaluation",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        claim_result = claim_next_run(args.sqlite_db, now=args.now)
    except SchedulerPersistenceError as exc:
        payload = {
            "ok": False,
            "stage": "scheduler_persistence",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to claim next run: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "claim": claim_result.to_dict() if claim_result is not None else None,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        if claim_result is None:
            print("No runnable run is currently eligible for claim.")
        else:
            print(f"Claimed run: {claim_result.dispatch_run.run.id}")
            print(f"Queue item: {claim_result.dispatch_run.queue_item.id}")
            print(
                f"Priority: {claim_result.dispatch_run.queue_item.priority_class} "
                f"(rank={claim_result.priority_rank})"
            )
            print(f"Flow: {claim_result.dispatch_run.flow_context.flow_id}")
            print(f"Effective age seconds: {claim_result.effective_age_seconds}")
    return 0


def main_release_claimed_run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Return a claimed queue item back to queued.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--run-id", help="Claimed run identifier to release")
    target_group.add_argument("--queue-item-id", help="Claimed queue item identifier to release")
    parser.add_argument(
        "--available-at",
        help="Optional ISO-8601 timestamp to use as the next available_at value (defaults to now)",
    )
    parser.add_argument("--note", help="Optional short note stored in transition metadata")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = release_claimed_run(
            args.sqlite_db,
            run_id=args.run_id,
            queue_item_id=args.queue_item_id,
            available_at=args.available_at,
            note=args.note,
        )
    except SchedulerPersistenceError as exc:
        payload = {
            "ok": False,
            "stage": "scheduler_persistence",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to release claimed run: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "release": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Released run: {result.dispatch_run.run.id}")
        print(f"Queue item: {result.dispatch_run.queue_item.id}")
        print(f"Available at: {result.dispatch_run.queue_item.available_at}")
    return 0


def main_mark_claimed_run_dispatch_failed(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Record a dispatch failure or abandoned claim and requeue the claimed run."
    )
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--run-id", help="Claimed run identifier to mark")
    target_group.add_argument("--queue-item-id", help="Claimed queue item identifier to mark")
    parser.add_argument(
        "--reason-code",
        required=True,
        help="Machine-readable dispatch-failure reason (for example: dispatch_failed or claim_abandoned)",
    )
    parser.add_argument(
        "--available-at",
        help="Optional ISO-8601 timestamp to use as the next available_at value (defaults to now)",
    )
    parser.add_argument("--note", help="Optional short note stored in transition metadata")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        result = mark_claimed_run_dispatch_failed(
            args.sqlite_db,
            run_id=args.run_id,
            queue_item_id=args.queue_item_id,
            reason_code=args.reason_code,
            available_at=args.available_at,
            note=args.note,
        )
    except SchedulerPersistenceError as exc:
        payload = {
            "ok": False,
            "stage": "scheduler_persistence",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Failed to mark dispatch failure: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "dispatch_failure": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Dispatch failure recorded for run: {result.dispatch_run.run.id}")
        print(f"Queue item: {result.dispatch_run.queue_item.id}")
        print(f"Reason code: {result.transition.reason_code}")
        print(f"Available at: {result.dispatch_run.queue_item.available_at}")
    return 0


def main_dispatch_executor_run(argv: list[str] | None = None) -> int:
    return _main_dispatch_claimed_run(argv, requested_role="executor")


def main_dispatch_reviewer_run(argv: list[str] | None = None) -> int:
    return _main_dispatch_claimed_run(argv, requested_role="reviewer")


def main_dispatch_next_for_claimed_run(argv: list[str] | None = None) -> int:
    return _main_dispatch_claimed_run(argv, requested_role="auto")


def _main_dispatch_claimed_run(argv: list[str] | None, *, requested_role: str) -> int:
    parser = argparse.ArgumentParser(description="Dispatch a claimed run through the v2 legacy-runtime adapter.")
    parser.add_argument("--sqlite-db", required=True, help="SQLite database path bootstrapped with init-sqlite-v1")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--run-id", help="Claimed run identifier to dispatch")
    target_group.add_argument("--queue-item-id", help="Claimed queue item identifier to dispatch")
    target_group.add_argument("--claim-json", help="JSON file produced by claim-next-run or an equivalent envelope")
    parser.add_argument("--context-json", help="Optional JSON file with legacy runtime context fields")
    parser.add_argument("--artifact-root", help="Optional run artifact root (<project>/<flow>/<run>/ will be used)")
    parser.add_argument("--workspace-root", help="Optional workspace root used to derive conventional project/worktree paths")
    parser.add_argument("--project-repo-path", help="Override project_repo_path")
    parser.add_argument("--executor-worktree-path", help="Override executor_worktree_path")
    parser.add_argument("--reviewer-worktree-path", help="Override reviewer_worktree_path")
    parser.add_argument("--instructions-repo-path", help="Override instructions_repo_path")
    parser.add_argument("--branch-base", help="Override branch_base")
    parser.add_argument("--instruction-profile", help="Override instruction_profile")
    parser.add_argument("--instruction-overlay", action="append", dest="instruction_overlays", help="Append one instruction overlay")
    parser.add_argument("--task-text", help="Override task_text")
    parser.add_argument("--mode", choices=("executor-only", "executor+reviewer"), help="Override legacy runtime mode")
    parser.add_argument("--source", help="Override legacy runtime source")
    parser.add_argument("--thread-label", help="Override legacy runtime thread_label")
    parser.add_argument("--constraint", action="append", dest="constraints", help="Append one task constraint")
    parser.add_argument("--expected-output", action="append", dest="expected_output", help="Append one expected-output line")
    parser.add_argument("--legacy-control-dir", help="Optional control repo root used to source legacy scripts/templates")
    parser.add_argument("--executor-runner", help="Override executor backend runner path")
    parser.add_argument("--reviewer-runner", help="Override reviewer backend runner path")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    try:
        claim_payload = _load_json_argument(args.claim_json) if args.claim_json else None
        runtime_context = _load_json_argument(args.context_json) if args.context_json else None
        result = dispatch_claimed_run(
            args.sqlite_db,
            requested_role=requested_role,
            claim_payload=claim_payload,
            runtime_context=runtime_context,
            run_id=args.run_id,
            queue_item_id=args.queue_item_id,
            artifact_root=args.artifact_root,
            workspace_root=args.workspace_root,
            project_repo_path=args.project_repo_path,
            executor_worktree_path=args.executor_worktree_path,
            reviewer_worktree_path=args.reviewer_worktree_path,
            instructions_repo_path=args.instructions_repo_path,
            branch_base=args.branch_base,
            instruction_profile=args.instruction_profile,
            instruction_overlays=args.instruction_overlays,
            task_text=args.task_text,
            mode=args.mode,
            source=args.source,
            thread_label=args.thread_label,
            constraints=args.constraints,
            expected_output=args.expected_output,
            legacy_control_dir=args.legacy_control_dir,
            executor_runner_path=args.executor_runner,
            reviewer_runner_path=args.reviewer_runner,
        )
    except (DispatchAdapterError, StepRunPersistenceError, SchedulerPersistenceError) as exc:
        payload = {
            "ok": False,
            "stage": "dispatch_adapter",
            "error": exc.to_dict(),
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        else:
            print(f"Dispatch failed: {exc.message}", file=sys.stderr)
            if exc.details:
                print(f"Details: {exc.details}", file=sys.stderr)
        return 1

    payload = {
        "ok": True,
        "sqlite_db": str(Path(args.sqlite_db).expanduser().resolve()),
        "dispatch": result.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"Dispatched run: {result.dispatch_run.run.id}")
        print(f"Role: {result.role_decision.resolved_role}")
        print(f"Technical success: {result.technical_success}")
        if result.step_run is not None:
            print(f"step_run: {result.step_run.step_run.id} ({result.step_run.step_run.status})")
        if result.queue_requeue is not None:
            print(f"Queue item requeued: {result.queue_requeue.dispatch_run.queue_item.id}")
        print(f"Artifacts: {len(result.artifacts)}")
    return 0 if result.technical_success else 1


def _add_worker_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--context-json", help="Optional JSON file with legacy runtime context fields")
    parser.add_argument("--artifact-root", help="Optional run artifact root (<project>/<flow>/<run>/ will be used)")
    parser.add_argument("--worker-log-root", help="Optional worker summary/log root")
    parser.add_argument("--workspace-root", help="Optional workspace root used to derive conventional project/worktree paths")
    parser.add_argument("--project-repo-path", help="Override project_repo_path")
    parser.add_argument("--executor-worktree-path", help="Override executor_worktree_path")
    parser.add_argument("--reviewer-worktree-path", help="Override reviewer_worktree_path")
    parser.add_argument("--instructions-repo-path", help="Override instructions_repo_path")
    parser.add_argument("--branch-base", help="Override branch_base")
    parser.add_argument("--instruction-profile", help="Override instruction_profile")
    parser.add_argument("--instruction-overlay", action="append", dest="instruction_overlays", help="Append one instruction overlay")
    parser.add_argument("--task-text", help="Override task_text")
    parser.add_argument("--mode", choices=("executor-only", "executor+reviewer"), help="Override legacy runtime mode")
    parser.add_argument("--source", help="Override legacy runtime source")
    parser.add_argument("--thread-label", help="Override legacy runtime thread_label")
    parser.add_argument("--constraint", action="append", dest="constraints", help="Append one task constraint")
    parser.add_argument("--expected-output", action="append", dest="expected_output", help="Append one expected-output line")
    parser.add_argument("--legacy-control-dir", help="Optional control repo root used to source legacy scripts/templates")
    parser.add_argument("--executor-runner", help="Override executor backend runner path")
    parser.add_argument("--reviewer-runner", help="Override reviewer backend runner path")
    parser.add_argument("--claim-now", help="Optional ISO-8601 timestamp used for scheduler claim evaluation")


def _build_worker_runtime_config_from_args(args: argparse.Namespace) -> WorkerRuntimeConfig:
    runtime_context = _load_json_argument(args.context_json) if getattr(args, "context_json", None) else None
    return WorkerRuntimeConfig(
        runtime_context=runtime_context,
        artifact_root=Path(args.artifact_root).expanduser().resolve() if getattr(args, "artifact_root", None) else None,
        worker_log_root=Path(args.worker_log_root).expanduser().resolve() if getattr(args, "worker_log_root", None) else None,
        workspace_root=Path(args.workspace_root).expanduser().resolve() if getattr(args, "workspace_root", None) else None,
        project_repo_path=Path(args.project_repo_path).expanduser().resolve() if getattr(args, "project_repo_path", None) else None,
        executor_worktree_path=Path(args.executor_worktree_path).expanduser().resolve() if getattr(args, "executor_worktree_path", None) else None,
        reviewer_worktree_path=Path(args.reviewer_worktree_path).expanduser().resolve() if getattr(args, "reviewer_worktree_path", None) else None,
        instructions_repo_path=Path(args.instructions_repo_path).expanduser().resolve() if getattr(args, "instructions_repo_path", None) else None,
        branch_base=getattr(args, "branch_base", None),
        instruction_profile=getattr(args, "instruction_profile", None),
        instruction_overlays=tuple(args.instruction_overlays) if getattr(args, "instruction_overlays", None) else None,
        task_text=getattr(args, "task_text", None),
        mode=getattr(args, "mode", None),
        source=getattr(args, "source", None),
        thread_label=getattr(args, "thread_label", None),
        constraints=tuple(args.constraints) if getattr(args, "constraints", None) else None,
        expected_output=tuple(args.expected_output) if getattr(args, "expected_output", None) else None,
        legacy_control_dir=Path(args.legacy_control_dir).expanduser().resolve() if getattr(args, "legacy_control_dir", None) else None,
        executor_runner_path=Path(args.executor_runner).expanduser().resolve() if getattr(args, "executor_runner", None) else None,
        reviewer_runner_path=Path(args.reviewer_runner).expanduser().resolve() if getattr(args, "reviewer_runner", None) else None,
        claim_now=getattr(args, "claim_now", None),
    )


def _add_control_plane_api_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--host",
        default=None,
        help=f"Bind host (defaults to {API_ENV_HOST} or {API_DEFAULT_HOST}; localhost-only in v1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"Bind port (defaults to {API_ENV_PORT} or {API_DEFAULT_PORT})",
    )
    parser.add_argument(
        "--sqlite-db",
        default=None,
        help=f"SQLite database path (or set {API_ENV_SQLITE_DB})",
    )
    parser.add_argument(
        "--artifact-root",
        default=None,
        help=f"Optional default artifact root for intake/worker requests (or set {API_ENV_ARTIFACT_ROOT})",
    )
    parser.add_argument(
        "--workspace-root",
        default=None,
        help=f"Optional default workspace root for intake/worker requests (or set {API_ENV_WORKSPACE_ROOT})",
    )
    parser.add_argument(
        "--worker-log-root",
        default=None,
        help=f"Optional default worker log root (or set {API_ENV_WORKER_LOG_ROOT})",
    )


def _build_control_plane_api_config_from_args(args: argparse.Namespace):
    return create_control_plane_api_config(
        host=args.host,
        port=args.port,
        sqlite_db=args.sqlite_db,
        default_artifact_root=args.artifact_root,
        default_workspace_root=args.workspace_root,
        default_worker_log_root=args.worker_log_root,
    )


def _merge_submission_cli_overrides(base_payload: dict[str, object], args: argparse.Namespace) -> dict[str, object]:
    payload = dict(base_payload)
    for key in (
        "project_key",
        "task_text",
        "project_profile",
        "workflow_id",
        "milestone",
        "priority_class",
        "instruction_profile",
        "source",
        "thread_label",
        "artifact_root",
        "workspace_root",
    ):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    if getattr(args, "instruction_overlays", None) is not None:
        payload["instruction_overlays"] = list(args.instruction_overlays)
    if getattr(args, "constraints", None) is not None:
        payload["constraints"] = list(args.constraints)
    if getattr(args, "expected_output", None) is not None:
        payload["expected_output"] = list(args.expected_output)
    return payload


def _load_json_argument(path: str) -> dict[str, object]:
    source_path = Path(path if path != "-" else ".").expanduser().resolve()
    try:
        raw = sys.stdin.read() if path == "-" else source_path.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise DispatchAdapterError(
            code=DISPATCH_PAYLOAD_INVALID,
            message=f"Failed to load JSON payload: {path}",
            database_path=source_path,
            details=str(exc),
        ) from exc
    if not isinstance(payload, dict):
        raise DispatchAdapterError(
            code=DISPATCH_PAYLOAD_INVALID,
            message="JSON payload must be an object",
            database_path=source_path,
        )
    return payload


def _format_validation_error(error: object) -> str:
    error_dict = error.to_dict()
    location = error_dict["file_path"] or error_dict["package_root"]
    suffix = f" ({error_dict['key_path']})" if error_dict["key_path"] else ""
    details = f" [{error_dict['details']}]" if error_dict["details"] else ""
    return f"- [{error_dict['code']}] {location}{suffix}: {error_dict['message']}{details}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Control Plane v2 utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate-project-package")
    validate_parser.add_argument("args", nargs=argparse.REMAINDER)

    sqlite_parser = subparsers.add_parser("init-sqlite-v1")
    sqlite_parser.add_argument("args", nargs=argparse.REMAINDER)

    migrate_sqlite_parser = subparsers.add_parser("migrate-sqlite-v1")
    migrate_sqlite_parser.add_argument("args", nargs=argparse.REMAINDER)

    show_sqlite_version_parser = subparsers.add_parser("show-sqlite-schema-version")
    show_sqlite_version_parser.add_argument("args", nargs=argparse.REMAINDER)

    list_sqlite_migrations_parser = subparsers.add_parser("list-sqlite-migrations")
    list_sqlite_migrations_parser.add_argument("args", nargs=argparse.REMAINDER)

    register_parser = subparsers.add_parser("register-project-package")
    register_parser.add_argument("args", nargs=argparse.REMAINDER)

    list_parser = subparsers.add_parser("list-registered-projects")
    list_parser.add_argument("args", nargs=argparse.REMAINDER)

    create_run_parser = subparsers.add_parser("create-root-run")
    create_run_parser.add_argument("args", nargs=argparse.REMAINDER)

    list_runs_parser = subparsers.add_parser("list-runs")
    list_runs_parser.add_argument("args", nargs=argparse.REMAINDER)

    show_run_parser = subparsers.add_parser("show-run")
    show_run_parser.add_argument("args", nargs=argparse.REMAINDER)

    start_step_parser = subparsers.add_parser("start-step-run")
    start_step_parser.add_argument("args", nargs=argparse.REMAINDER)

    finish_step_parser = subparsers.add_parser("finish-step-run")
    finish_step_parser.add_argument("args", nargs=argparse.REMAINDER)

    retry_step_parser = subparsers.add_parser("retry-step-run")
    retry_step_parser.add_argument("args", nargs=argparse.REMAINDER)

    list_step_parser = subparsers.add_parser("list-step-runs")
    list_step_parser.add_argument("args", nargs=argparse.REMAINDER)

    show_step_parser = subparsers.add_parser("show-step-run")
    show_step_parser.add_argument("args", nargs=argparse.REMAINDER)

    complete_reviewer_parser = subparsers.add_parser("complete-reviewer-outcome")
    complete_reviewer_parser.add_argument("args", nargs=argparse.REMAINDER)

    ingest_reviewer_parser = subparsers.add_parser("ingest-reviewer-result")
    ingest_reviewer_parser.add_argument("args", nargs=argparse.REMAINDER)

    show_dispatch_result_parser = subparsers.add_parser("show-dispatch-result")
    show_dispatch_result_parser.add_argument("args", nargs=argparse.REMAINDER)

    pause_run_parser = subparsers.add_parser("pause-run")
    pause_run_parser.add_argument("args", nargs=argparse.REMAINDER)

    resume_run_parser = subparsers.add_parser("resume-run")
    resume_run_parser.add_argument("args", nargs=argparse.REMAINDER)

    force_stop_run_parser = subparsers.add_parser("force-stop-run")
    force_stop_run_parser.add_argument("args", nargs=argparse.REMAINDER)

    rerun_run_step_parser = subparsers.add_parser("rerun-run-step")
    rerun_run_step_parser.add_argument("args", nargs=argparse.REMAINDER)

    show_run_control_state_parser = subparsers.add_parser("show-run-control-state")
    show_run_control_state_parser.add_argument("args", nargs=argparse.REMAINDER)

    list_flow_parser = subparsers.add_parser("list-flow-runs")
    list_flow_parser.add_argument("args", nargs=argparse.REMAINDER)

    claim_next_parser = subparsers.add_parser("claim-next-run")
    claim_next_parser.add_argument("args", nargs=argparse.REMAINDER)

    release_claimed_parser = subparsers.add_parser("release-claimed-run")
    release_claimed_parser.add_argument("args", nargs=argparse.REMAINDER)

    mark_dispatch_failed_parser = subparsers.add_parser("mark-claimed-run-dispatch-failed")
    mark_dispatch_failed_parser.add_argument("args", nargs=argparse.REMAINDER)

    dispatch_executor_parser = subparsers.add_parser("dispatch-executor-run")
    dispatch_executor_parser.add_argument("args", nargs=argparse.REMAINDER)

    dispatch_reviewer_parser = subparsers.add_parser("dispatch-reviewer-run")
    dispatch_reviewer_parser.add_argument("args", nargs=argparse.REMAINDER)

    dispatch_next_parser = subparsers.add_parser("dispatch-next-for-claimed-run")
    dispatch_next_parser.add_argument("args", nargs=argparse.REMAINDER)

    worker_tick_parser = subparsers.add_parser("run-worker-tick")
    worker_tick_parser.add_argument("args", nargs=argparse.REMAINDER)

    worker_until_idle_parser = subparsers.add_parser("run-worker-until-idle")
    worker_until_idle_parser.add_argument("args", nargs=argparse.REMAINDER)

    args = parser.parse_args()

    if args.command == "validate-project-package":
        return main_validate_project_package(args.args)
    if args.command == "init-sqlite-v1":
        return main_init_sqlite_v1(args.args)
    if args.command == "migrate-sqlite-v1":
        return main_migrate_sqlite_v1(args.args)
    if args.command == "show-sqlite-schema-version":
        return main_show_sqlite_schema_version(args.args)
    if args.command == "list-sqlite-migrations":
        return main_list_sqlite_migrations(args.args)
    if args.command == "register-project-package":
        return main_register_project_package(args.args)
    if args.command == "list-registered-projects":
        return main_list_registered_projects(args.args)
    if args.command == "create-root-run":
        return main_create_root_run(args.args)
    if args.command == "list-runs":
        return main_list_runs(args.args)
    if args.command == "show-run":
        return main_show_run(args.args)
    if args.command == "start-step-run":
        return main_start_step_run(args.args)
    if args.command == "finish-step-run":
        return main_finish_step_run(args.args)
    if args.command == "retry-step-run":
        return main_retry_step_run(args.args)
    if args.command == "list-step-runs":
        return main_list_step_runs(args.args)
    if args.command == "show-step-run":
        return main_show_step_run(args.args)
    if args.command == "complete-reviewer-outcome":
        return main_complete_reviewer_outcome(args.args)
    if args.command == "ingest-reviewer-result":
        return main_ingest_reviewer_result(args.args)
    if args.command == "show-dispatch-result":
        return main_show_dispatch_result(args.args)
    if args.command == "pause-run":
        return main_pause_run(args.args)
    if args.command == "resume-run":
        return main_resume_run(args.args)
    if args.command == "force-stop-run":
        return main_force_stop_run(args.args)
    if args.command == "rerun-run-step":
        return main_rerun_run_step(args.args)
    if args.command == "show-run-control-state":
        return main_show_run_control_state(args.args)
    if args.command == "list-flow-runs":
        return main_list_flow_runs(args.args)
    if args.command == "claim-next-run":
        return main_claim_next_run(args.args)
    if args.command == "release-claimed-run":
        return main_release_claimed_run(args.args)
    if args.command == "mark-claimed-run-dispatch-failed":
        return main_mark_claimed_run_dispatch_failed(args.args)
    if args.command == "dispatch-executor-run":
        return main_dispatch_executor_run(args.args)
    if args.command == "dispatch-reviewer-run":
        return main_dispatch_reviewer_run(args.args)
    if args.command == "dispatch-next-for-claimed-run":
        return main_dispatch_next_for_claimed_run(args.args)
    if args.command == "run-worker-tick":
        return main_run_worker_tick(args.args)
    if args.command == "run-worker-until-idle":
        return main_run_worker_until_idle(args.args)

    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
