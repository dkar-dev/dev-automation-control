from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .project_package import load_project_package, resolve_project_package_root
from .project_package_validator import ProjectPackageValidationFailed
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

    args = parser.parse_args()

    if args.command == "validate-project-package":
        return main_validate_project_package(args.args)
    if args.command == "init-sqlite-v1":
        return main_init_sqlite_v1(args.args)

    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
