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
from .sqlite_bootstrap import initialize_sqlite_v1


CONTROL_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PROJECTS_ROOT = CONTROL_DIR / "projects"
DEFAULT_SQLITE_SCHEMA = CONTROL_DIR / "schemas" / "sqlite-v1.sql"


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
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    args = parser.parse_args(argv)

    result = initialize_sqlite_v1(args.database_path, args.schema)
    payload = {"ok": True, "database": result.to_dict()}

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"SQLite schema initialized: {result.database_path}")
        print(f"Schema file: {result.schema_path}")
        print("Tables: " + ", ".join(result.tables))
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

    args = parser.parse_args()

    if args.command == "validate-project-package":
        return main_validate_project_package(args.args)
    if args.command == "init-sqlite-v1":
        return main_init_sqlite_v1(args.args)
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

    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
