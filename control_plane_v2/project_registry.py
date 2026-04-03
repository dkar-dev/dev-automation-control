from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import uuid

from .project_package import ProjectPackage


PROJECTS_TABLE_MISSING = "PROJECTS_TABLE_MISSING"
PROJECT_REGISTRY_CONFLICT = "PROJECT_REGISTRY_CONFLICT"
PROJECT_REGISTRY_STORAGE_ERROR = "PROJECT_REGISTRY_STORAGE_ERROR"


@dataclass(frozen=True)
class RegisteredProject:
    id: str
    project_key: str
    package_root: Path
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, str]:
        return {
            "id": self.id,
            "project_key": self.project_key,
            "package_root": str(self.package_root),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class RegisterProjectResult:
    action: str
    project: RegisteredProject

    def to_dict(self) -> dict[str, object]:
        return {
            "action": self.action,
            "project": self.project.to_dict(),
        }


class ProjectRegistryError(Exception):
    def __init__(self, code: str, message: str, database_path: Path, details: str | None = None) -> None:
        self.code = code
        self.message = message
        self.database_path = database_path
        self.details = details
        super().__init__(message)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "message": self.message,
            "database_path": str(self.database_path),
            "details": self.details,
        }


def register_project_package(
    database_path: str | Path,
    project_package: ProjectPackage,
) -> RegisterProjectResult:
    resolved_db_path = _resolve_database_path(database_path)
    connection = _connect_registry_db(resolved_db_path)

    try:
        _ensure_projects_table(connection, resolved_db_path)

        existing = _select_registered_project(connection, project_package.project_key)
        now = _utc_now()
        connection.execute(
            """
            INSERT INTO projects (id, project_key, package_root, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(project_key) DO UPDATE SET
              package_root = excluded.package_root,
              updated_at = excluded.updated_at
            """,
            (
                existing.id if existing is not None else str(uuid.uuid4()),
                project_package.project_key,
                str(project_package.package_root),
                existing.created_at if existing is not None else now,
                now,
            ),
        )
        connection.commit()

        registered_project = _select_registered_project(connection, project_package.project_key)
        if registered_project is None:
            raise ProjectRegistryError(
                code=PROJECT_REGISTRY_STORAGE_ERROR,
                message=f"Registered project row is missing after upsert: {project_package.project_key}",
                database_path=resolved_db_path,
            )

        return RegisterProjectResult(
            action="inserted" if existing is None else "updated",
            project=registered_project,
        )
    except sqlite3.IntegrityError as exc:
        raise ProjectRegistryError(
            code=PROJECT_REGISTRY_CONFLICT,
            message=f"Failed to register project {project_package.project_key}",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    except sqlite3.Error as exc:
        raise ProjectRegistryError(
            code=PROJECT_REGISTRY_STORAGE_ERROR,
            message="SQLite registry operation failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def list_registered_projects(database_path: str | Path) -> list[RegisteredProject]:
    resolved_db_path = _resolve_database_path(database_path)
    connection = _connect_registry_db(resolved_db_path)

    try:
        _ensure_projects_table(connection, resolved_db_path)
        rows = connection.execute(
            """
            SELECT id, project_key, package_root, created_at, updated_at
            FROM projects
            ORDER BY project_key
            """
        ).fetchall()
        return [_row_to_registered_project(row) for row in rows]
    except sqlite3.Error as exc:
        raise ProjectRegistryError(
            code=PROJECT_REGISTRY_STORAGE_ERROR,
            message="SQLite registry query failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _resolve_database_path(database_path: str | Path) -> Path:
    resolved_db_path = Path(database_path).expanduser().resolve()
    if not resolved_db_path.exists():
        raise ProjectRegistryError(
            code=PROJECT_REGISTRY_STORAGE_ERROR,
            message=f"SQLite database does not exist: {resolved_db_path}",
            database_path=resolved_db_path,
            details="Run init-sqlite-v1 before registering projects.",
        )
    if not resolved_db_path.is_file():
        raise ProjectRegistryError(
            code=PROJECT_REGISTRY_STORAGE_ERROR,
            message=f"SQLite database path is not a file: {resolved_db_path}",
            database_path=resolved_db_path,
        )
    return resolved_db_path


def _connect_registry_db(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def _ensure_projects_table(connection: sqlite3.Connection, database_path: Path) -> None:
    row = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'projects'
        """
    ).fetchone()
    if row is None:
        raise ProjectRegistryError(
            code=PROJECTS_TABLE_MISSING,
            message=f"SQLite database is missing the projects registry table: {database_path}",
            database_path=database_path,
            details="Run init-sqlite-v1 before registering projects.",
        )


def _select_registered_project(connection: sqlite3.Connection, project_key: str) -> RegisteredProject | None:
    row = connection.execute(
        """
        SELECT id, project_key, package_root, created_at, updated_at
        FROM projects
        WHERE project_key = ?
        """,
        (project_key,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_registered_project(row)


def _row_to_registered_project(row: sqlite3.Row) -> RegisteredProject:
    return RegisteredProject(
        id=row["id"],
        project_key=row["project_key"],
        package_root=Path(row["package_root"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
