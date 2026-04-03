from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sqlite3

from .id_generation import (
    generate_flow_id,
    generate_queue_item_id,
    generate_run_id,
    generate_state_transition_id,
)


PRIORITY_CLASSES = ("system", "interactive", "background")
RUN_STATUSES = ("queued", "running", "completed", "failed", "stopped", "cancelled")
QUEUE_ITEM_STATUSES = ("queued", "claimed", "completed", "cancelled")

PROVISIONAL_ROOT_ORIGIN_TYPE = "root_manual"
PROVISIONAL_RUN_CREATE_TRANSITION_TYPE = "root_run_created"
PROVISIONAL_QUEUE_ENQUEUE_TRANSITION_TYPE = "root_run_enqueued"

ARTIFACT_DIRECTORY_CREATE_FAILED = "ARTIFACT_DIRECTORY_CREATE_FAILED"
INVALID_PRIORITY_CLASS = "INVALID_PRIORITY_CLASS"
INVALID_RUN_SCOPE = "INVALID_RUN_SCOPE"
PROJECT_NOT_REGISTERED = "PROJECT_NOT_REGISTERED"
REQUIRED_TABLES_MISSING = "REQUIRED_TABLES_MISSING"
RUN_NOT_FOUND = "RUN_NOT_FOUND"
RUN_STORAGE_ERROR = "RUN_STORAGE_ERROR"


@dataclass(frozen=True)
class QueueItemRecord:
    id: str
    run_id: str
    priority_class: str
    status: str
    enqueued_at: str
    available_at: str
    claimed_at: str | None
    terminal_at: str | None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "priority_class": self.priority_class,
            "status": self.status,
            "enqueued_at": self.enqueued_at,
            "available_at": self.available_at,
            "claimed_at": self.claimed_at,
            "terminal_at": self.terminal_at,
        }


@dataclass(frozen=True)
class StateTransitionRecord:
    id: str
    entity_type: str
    run_id: str | None
    step_run_id: str | None
    queue_item_id: str | None
    from_state: str | None
    to_state: str
    transition_type: str
    reason_code: str | None
    metadata_json: str | None
    created_at: str

    def to_dict(self) -> dict[str, str | None]:
        return {
            "id": self.id,
            "entity_type": self.entity_type,
            "run_id": self.run_id,
            "step_run_id": self.step_run_id,
            "queue_item_id": self.queue_item_id,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "transition_type": self.transition_type,
            "reason_code": self.reason_code,
            "metadata_json": self.metadata_json,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class RunSnapshotRecord:
    id: str
    snapshot_scope: str
    project_id: str
    flow_id: str
    run_id: str | None
    state_transition_id: str
    snapshot_json: str
    created_at: str

    def to_dict(self) -> dict[str, str | None]:
        return {
            "id": self.id,
            "snapshot_scope": self.snapshot_scope,
            "project_id": self.project_id,
            "flow_id": self.flow_id,
            "run_id": self.run_id,
            "state_transition_id": self.state_transition_id,
            "snapshot_json": self.snapshot_json,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class RunSummary:
    id: str
    project_id: str
    project_key: str
    package_root: Path
    project_profile: str
    workflow_id: str
    milestone: str
    flow_id: str
    parent_run_id: str | None
    origin_type: str
    origin_run_id: str | None
    origin_step_run_id: str | None
    status: str
    created_at: str
    updated_at: str
    queued_at: str | None
    started_at: str | None
    terminal_at: str | None
    queue_item: QueueItemRecord | None

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "project_id": self.project_id,
            "project_key": self.project_key,
            "package_root": str(self.package_root),
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "milestone": self.milestone,
            "flow_id": self.flow_id,
            "parent_run_id": self.parent_run_id,
            "origin_type": self.origin_type,
            "origin_run_id": self.origin_run_id,
            "origin_step_run_id": self.origin_step_run_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "queued_at": self.queued_at,
            "started_at": self.started_at,
            "terminal_at": self.terminal_at,
            "queue_item": self.queue_item.to_dict() if self.queue_item is not None else None,
        }


@dataclass(frozen=True)
class RunDetails:
    run: RunSummary
    state_transitions: tuple[StateTransitionRecord, ...]
    run_snapshots: tuple[RunSnapshotRecord, ...]
    artifact_directory: Path | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "run": self.run.to_dict(),
            "state_transitions": [transition.to_dict() for transition in self.state_transitions],
            "run_snapshots": [snapshot.to_dict() for snapshot in self.run_snapshots],
            "artifact_directory": str(self.artifact_directory) if self.artifact_directory else None,
        }


@dataclass(frozen=True)
class RootRunCreateRequest:
    project_key: str
    project_profile: str
    workflow_id: str
    milestone: str
    priority_class: str = "interactive"
    artifact_root: Path | None = None


class RunPersistenceError(Exception):
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


def create_root_run(database_path: str | Path, request: RootRunCreateRequest) -> RunDetails:
    resolved_db_path = _resolve_database_path(database_path)
    _validate_create_request(request, resolved_db_path)

    connection = _connect_run_db(resolved_db_path)
    artifact_directory: Path | None = None

    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "queue_items", "state_transitions", "run_snapshots"),
        )
        project_row = _select_registered_project_row(connection, request.project_key)
        if project_row is None:
            raise RunPersistenceError(
                code=PROJECT_NOT_REGISTERED,
                message=f"Project is not registered in SQLite: {request.project_key}",
                database_path=resolved_db_path,
                details="Register the project package before creating a root run.",
            )

        now = _utc_now()
        run_id = generate_run_id()
        flow_id = generate_flow_id()
        queue_item_id = generate_queue_item_id()
        run_transition_id = generate_state_transition_id()
        queue_transition_id = generate_state_transition_id()

        if request.artifact_root is not None:
            artifact_directory = _prepare_artifact_directory(
                request.artifact_root,
                request.project_key,
                flow_id,
                run_id,
                resolved_db_path,
            )

        try:
            connection.execute("BEGIN")
            connection.execute(
                """
                INSERT INTO runs (
                  id,
                  project_id,
                  project_profile,
                  workflow_id,
                  milestone,
                  flow_id,
                  parent_run_id,
                  origin_type,
                  origin_run_id,
                  origin_step_run_id,
                  status,
                  created_at,
                  updated_at,
                  queued_at,
                  started_at,
                  terminal_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, ?, ?, NULL, NULL)
                """,
                (
                    run_id,
                    project_row["id"],
                    request.project_profile,
                    request.workflow_id,
                    request.milestone,
                    flow_id,
                    PROVISIONAL_ROOT_ORIGIN_TYPE,
                    "queued",
                    now,
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO queue_items (
                  id,
                  run_id,
                  priority_class,
                  status,
                  enqueued_at,
                  available_at,
                  claimed_at,
                  terminal_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                (
                    queue_item_id,
                    run_id,
                    request.priority_class,
                    "queued",
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO state_transitions (
                  id,
                  entity_type,
                  run_id,
                  step_run_id,
                  queue_item_id,
                  from_state,
                  to_state,
                  transition_type,
                  reason_code,
                  metadata_json,
                  created_at
                )
                VALUES (?, 'run', ?, NULL, NULL, NULL, ?, ?, NULL, ?, ?)
                """,
                (
                    run_transition_id,
                    run_id,
                    "queued",
                    PROVISIONAL_RUN_CREATE_TRANSITION_TYPE,
                    json.dumps({"origin_type": PROVISIONAL_ROOT_ORIGIN_TYPE}, sort_keys=True),
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO state_transitions (
                  id,
                  entity_type,
                  run_id,
                  step_run_id,
                  queue_item_id,
                  from_state,
                  to_state,
                  transition_type,
                  reason_code,
                  metadata_json,
                  created_at
                )
                VALUES (?, 'queue_item', NULL, NULL, ?, NULL, ?, ?, NULL, ?, ?)
                """,
                (
                    queue_transition_id,
                    queue_item_id,
                    "queued",
                    PROVISIONAL_QUEUE_ENQUEUE_TRANSITION_TYPE,
                    json.dumps({"priority_class": request.priority_class}, sort_keys=True),
                    now,
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        run_details = _load_run_details(connection, run_id, artifact_directory)
        if run_details is None:
            raise RunPersistenceError(
                code=RUN_STORAGE_ERROR,
                message=f"Run row is missing after create: {run_id}",
                database_path=resolved_db_path,
            )
        return run_details
    except sqlite3.Error as exc:
        if artifact_directory is not None:
            _cleanup_artifact_directory(artifact_directory, request.artifact_root)
        raise RunPersistenceError(
            code=RUN_STORAGE_ERROR,
            message="SQLite run persistence operation failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    except Exception:
        if artifact_directory is not None:
            _cleanup_artifact_directory(artifact_directory, request.artifact_root)
        raise
    finally:
        connection.close()


def get_run(database_path: str | Path, run_id: str) -> RunDetails:
    resolved_db_path = _resolve_database_path(database_path)
    if not isinstance(run_id, str) or not run_id.strip():
        raise RunPersistenceError(
            code=RUN_NOT_FOUND,
            message="run_id must be a non-empty string",
            database_path=resolved_db_path,
        )

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "queue_items", "state_transitions", "run_snapshots"),
        )
        run_details = _load_run_details(connection, run_id.strip())
        if run_details is None:
            raise RunPersistenceError(
                code=RUN_NOT_FOUND,
                message=f"Run is not present in SQLite: {run_id.strip()}",
                database_path=resolved_db_path,
            )
        return run_details
    except sqlite3.Error as exc:
        raise RunPersistenceError(
            code=RUN_STORAGE_ERROR,
            message="SQLite run lookup failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def list_runs(
    database_path: str | Path,
    *,
    project_key: str | None = None,
    status: str | None = None,
    project_profile: str | None = None,
    workflow_id: str | None = None,
    milestone: str | None = None,
    limit: int = 100,
) -> list[RunSummary]:
    resolved_db_path = _resolve_database_path(database_path)
    if limit <= 0:
        raise RunPersistenceError(
            code=INVALID_RUN_SCOPE,
            message="limit must be greater than zero",
            database_path=resolved_db_path,
        )

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("projects", "runs", "queue_items"))

        filters: list[str] = []
        params: list[object] = []

        if project_key is not None:
            filters.append("projects.project_key = ?")
            params.append(_require_non_empty_string("project_key", project_key, resolved_db_path))
        if status is not None:
            filters.append("runs.status = ?")
            params.append(_validate_status(status, resolved_db_path))
        if project_profile is not None:
            filters.append("runs.project_profile = ?")
            params.append(_require_non_empty_string("project_profile", project_profile, resolved_db_path))
        if workflow_id is not None:
            filters.append("runs.workflow_id = ?")
            params.append(_require_non_empty_string("workflow_id", workflow_id, resolved_db_path))
        if milestone is not None:
            filters.append("runs.milestone = ?")
            params.append(_require_non_empty_string("milestone", milestone, resolved_db_path))

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        rows = connection.execute(
            f"""
            SELECT
              runs.id,
              runs.project_id,
              projects.project_key,
              projects.package_root,
              runs.project_profile,
              runs.workflow_id,
              runs.milestone,
              runs.flow_id,
              runs.parent_run_id,
              runs.origin_type,
              runs.origin_run_id,
              runs.origin_step_run_id,
              runs.status,
              runs.created_at,
              runs.updated_at,
              runs.queued_at,
              runs.started_at,
              runs.terminal_at,
              queue_items.id AS queue_item_id,
              queue_items.priority_class,
              queue_items.status AS queue_status,
              queue_items.enqueued_at,
              queue_items.available_at,
              queue_items.claimed_at,
              queue_items.terminal_at AS queue_terminal_at
            FROM runs
            JOIN projects ON projects.id = runs.project_id
            LEFT JOIN queue_items ON queue_items.run_id = runs.id
            {where_sql}
            ORDER BY runs.created_at DESC, runs.id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [_row_to_run_summary(row) for row in rows]
    except sqlite3.Error as exc:
        raise RunPersistenceError(
            code=RUN_STORAGE_ERROR,
            message="SQLite run listing failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _resolve_database_path(database_path: str | Path) -> Path:
    resolved_db_path = Path(database_path).expanduser().resolve()
    if not resolved_db_path.exists():
        raise RunPersistenceError(
            code=RUN_STORAGE_ERROR,
            message=f"SQLite database does not exist: {resolved_db_path}",
            database_path=resolved_db_path,
            details="Run init-sqlite-v1 before creating or reading runs.",
        )
    if not resolved_db_path.is_file():
        raise RunPersistenceError(
            code=RUN_STORAGE_ERROR,
            message=f"SQLite database path is not a file: {resolved_db_path}",
            database_path=resolved_db_path,
        )
    return resolved_db_path


def _connect_run_db(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def _ensure_required_tables(
    connection: sqlite3.Connection,
    database_path: Path,
    required_tables: tuple[str, ...],
) -> None:
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()
    present_tables = {row["name"] for row in rows}
    missing_tables = [table_name for table_name in required_tables if table_name not in present_tables]
    if missing_tables:
        raise RunPersistenceError(
            code=REQUIRED_TABLES_MISSING,
            message=f"SQLite database is missing required tables: {', '.join(missing_tables)}",
            database_path=database_path,
            details="Run init-sqlite-v1 before using run persistence utilities.",
        )


def _select_registered_project_row(connection: sqlite3.Connection, project_key: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id, project_key, package_root, created_at, updated_at
        FROM projects
        WHERE project_key = ?
        """,
        (project_key,),
    ).fetchone()


def _load_run_details(
    connection: sqlite3.Connection,
    run_id: str,
    artifact_directory: Path | None = None,
) -> RunDetails | None:
    run_row = connection.execute(
        """
        SELECT
          runs.id,
          runs.project_id,
          projects.project_key,
          projects.package_root,
          runs.project_profile,
          runs.workflow_id,
          runs.milestone,
          runs.flow_id,
          runs.parent_run_id,
          runs.origin_type,
          runs.origin_run_id,
          runs.origin_step_run_id,
          runs.status,
          runs.created_at,
          runs.updated_at,
          runs.queued_at,
          runs.started_at,
          runs.terminal_at,
          queue_items.id AS queue_item_id,
          queue_items.priority_class,
          queue_items.status AS queue_status,
          queue_items.enqueued_at,
          queue_items.available_at,
          queue_items.claimed_at,
          queue_items.terminal_at AS queue_terminal_at
        FROM runs
        JOIN projects ON projects.id = runs.project_id
        LEFT JOIN queue_items ON queue_items.run_id = runs.id
        WHERE runs.id = ?
        """,
        (run_id,),
    ).fetchone()
    if run_row is None:
        return None

    run_summary = _row_to_run_summary(run_row)
    queue_item_id = run_summary.queue_item.id if run_summary.queue_item is not None else None

    transition_rows = connection.execute(
        """
        SELECT
          id,
          entity_type,
          run_id,
          step_run_id,
          queue_item_id,
          from_state,
          to_state,
          transition_type,
          reason_code,
          metadata_json,
          created_at
        FROM state_transitions
        WHERE run_id = ? OR queue_item_id = ?
        ORDER BY created_at, id
        """,
        (run_id, queue_item_id),
    ).fetchall()
    snapshot_rows = connection.execute(
        """
        SELECT
          id,
          snapshot_scope,
          project_id,
          flow_id,
          run_id,
          state_transition_id,
          snapshot_json,
          created_at
        FROM run_snapshots
        WHERE run_id = ? OR (snapshot_scope = 'flow' AND flow_id = ?)
        ORDER BY created_at, id
        """,
        (run_id, run_summary.flow_id),
    ).fetchall()

    return RunDetails(
        run=run_summary,
        state_transitions=tuple(_row_to_state_transition(row) for row in transition_rows),
        run_snapshots=tuple(_row_to_run_snapshot(row) for row in snapshot_rows),
        artifact_directory=artifact_directory,
    )


def _row_to_run_summary(row: sqlite3.Row) -> RunSummary:
    queue_item = None
    if row["queue_item_id"] is not None:
        queue_item = QueueItemRecord(
            id=row["queue_item_id"],
            run_id=row["id"],
            priority_class=row["priority_class"],
            status=row["queue_status"],
            enqueued_at=row["enqueued_at"],
            available_at=row["available_at"],
            claimed_at=row["claimed_at"],
            terminal_at=row["queue_terminal_at"],
        )

    return RunSummary(
        id=row["id"],
        project_id=row["project_id"],
        project_key=row["project_key"],
        package_root=Path(row["package_root"]),
        project_profile=row["project_profile"],
        workflow_id=row["workflow_id"],
        milestone=row["milestone"],
        flow_id=row["flow_id"],
        parent_run_id=row["parent_run_id"],
        origin_type=row["origin_type"],
        origin_run_id=row["origin_run_id"],
        origin_step_run_id=row["origin_step_run_id"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        queued_at=row["queued_at"],
        started_at=row["started_at"],
        terminal_at=row["terminal_at"],
        queue_item=queue_item,
    )


def _row_to_state_transition(row: sqlite3.Row) -> StateTransitionRecord:
    return StateTransitionRecord(
        id=row["id"],
        entity_type=row["entity_type"],
        run_id=row["run_id"],
        step_run_id=row["step_run_id"],
        queue_item_id=row["queue_item_id"],
        from_state=row["from_state"],
        to_state=row["to_state"],
        transition_type=row["transition_type"],
        reason_code=row["reason_code"],
        metadata_json=row["metadata_json"],
        created_at=row["created_at"],
    )


def _row_to_run_snapshot(row: sqlite3.Row) -> RunSnapshotRecord:
    return RunSnapshotRecord(
        id=row["id"],
        snapshot_scope=row["snapshot_scope"],
        project_id=row["project_id"],
        flow_id=row["flow_id"],
        run_id=row["run_id"],
        state_transition_id=row["state_transition_id"],
        snapshot_json=row["snapshot_json"],
        created_at=row["created_at"],
    )


def _validate_create_request(request: RootRunCreateRequest, database_path: Path) -> None:
    _require_non_empty_string("project_key", request.project_key, database_path)
    _require_non_empty_string("project_profile", request.project_profile, database_path)
    _require_non_empty_string("workflow_id", request.workflow_id, database_path)
    _require_non_empty_string("milestone", request.milestone, database_path)
    _validate_priority_class(request.priority_class, database_path)


def _require_non_empty_string(name: str, value: str, database_path: Path) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RunPersistenceError(
            code=INVALID_RUN_SCOPE,
            message=f"{name} must be a non-empty string",
            database_path=database_path,
        )
    return value.strip()


def _validate_priority_class(priority_class: str, database_path: Path) -> str:
    normalized = _require_non_empty_string("priority_class", priority_class, database_path)
    if normalized not in PRIORITY_CLASSES:
        raise RunPersistenceError(
            code=INVALID_PRIORITY_CLASS,
            message=f"priority_class must be one of: {', '.join(PRIORITY_CLASSES)}",
            database_path=database_path,
            details=f"actual={normalized}",
        )
    return normalized


def _validate_status(status: str, database_path: Path) -> str:
    normalized = _require_non_empty_string("status", status, database_path)
    if normalized not in RUN_STATUSES:
        raise RunPersistenceError(
            code=INVALID_RUN_SCOPE,
            message=f"status must be one of: {', '.join(RUN_STATUSES)}",
            database_path=database_path,
            details=f"actual={normalized}",
        )
    return normalized


def _prepare_artifact_directory(
    artifact_root: Path,
    project_key: str,
    flow_id: str,
    run_id: str,
    database_path: Path,
) -> Path:
    resolved_root = artifact_root.expanduser().resolve()
    artifact_directory = resolved_root / project_key / flow_id / run_id
    try:
        artifact_directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RunPersistenceError(
            code=ARTIFACT_DIRECTORY_CREATE_FAILED,
            message=f"Failed to create run artifact directory: {artifact_directory}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    return artifact_directory


def _cleanup_artifact_directory(artifact_directory: Path, artifact_root: Path | None) -> None:
    shutil.rmtree(artifact_directory, ignore_errors=True)
    if artifact_root is None:
        return

    resolved_root = artifact_root.expanduser().resolve()
    current = artifact_directory.parent
    while current != resolved_root and resolved_root in current.parents:
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
