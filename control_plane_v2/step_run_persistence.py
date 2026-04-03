from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3

from .id_generation import generate_opaque_id, generate_state_transition_id
from .run_persistence import (
    QUEUE_ITEM_STATUSES,
    RUN_STATUSES,
    QueueItemRecord,
    RunPersistenceError,
    RunSummary,
    StateTransitionRecord,
    _connect_run_db,
    _ensure_required_tables,
    _require_non_empty_string,
    _resolve_database_path,
    _row_to_run_summary,
    _utc_now,
)


STEP_KEYS = ("executor", "reviewer")
STEP_RUN_ACTIVE_STATUS = "running"
STEP_RUN_TERMINAL_STATUSES = ("succeeded", "failed", "timed_out", "cancelled")
STEP_RUN_STATUSES = (STEP_RUN_ACTIVE_STATUS, *STEP_RUN_TERMINAL_STATUSES)

PROVISIONAL_QUEUE_CLAIM_TRANSITION_TYPE = "queue_item_claimed_by_step_run"
PROVISIONAL_RUN_START_TRANSITION_TYPE = "run_started_by_step_run"
PROVISIONAL_STEP_RUN_FINISH_TRANSITION_TYPE = "step_run_finished"
PROVISIONAL_STEP_RUN_RETRY_TRANSITION_TYPE = "step_run_retried"
PROVISIONAL_STEP_RUN_START_TRANSITION_TYPE = "step_run_started"

ACTIVE_STEP_RUN_EXISTS = "ACTIVE_STEP_RUN_EXISTS"
INVALID_STEP_KEY = "INVALID_STEP_KEY"
INVALID_STEP_RUN_STATUS = "INVALID_STEP_RUN_STATUS"
MISSING_QUEUE_ITEM = "MISSING_QUEUE_ITEM"
RUN_NOT_STARTABLE = "RUN_NOT_STARTABLE"
STEP_KEY_ALREADY_STARTED = "STEP_KEY_ALREADY_STARTED"
STEP_RUN_NOT_FOUND = "STEP_RUN_NOT_FOUND"
STEP_RUN_NOT_RUNNING = "STEP_RUN_NOT_RUNNING"
STEP_RUN_NOT_TERMINAL = "STEP_RUN_NOT_TERMINAL"
STEP_RUN_STORAGE_ERROR = "STEP_RUN_STORAGE_ERROR"


@dataclass(frozen=True)
class StepRunSummary:
    id: str
    run_id: str
    project_id: str
    project_key: str
    flow_id: str
    project_profile: str
    workflow_id: str
    milestone: str
    run_status: str
    step_key: str
    attempt_no: int
    previous_step_run_id: str | None
    status: str
    created_at: str
    started_at: str
    terminal_at: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "project_id": self.project_id,
            "project_key": self.project_key,
            "flow_id": self.flow_id,
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "milestone": self.milestone,
            "run_status": self.run_status,
            "step_key": self.step_key,
            "attempt_no": self.attempt_no,
            "previous_step_run_id": self.previous_step_run_id,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "terminal_at": self.terminal_at,
        }


@dataclass(frozen=True)
class StepRunDetails:
    step_run: StepRunSummary
    run: RunSummary
    state_transitions: tuple[StateTransitionRecord, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "step_run": self.step_run.to_dict(),
            "run": self.run.to_dict(),
            "state_transitions": [transition.to_dict() for transition in self.state_transitions],
        }


class StepRunPersistenceError(Exception):
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


def start_step_run(database_path: str | Path, run_id: str, step_key: str) -> StepRunDetails:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_run_id = _require_non_empty_string("run_id", run_id, resolved_db_path)
    normalized_step_key = _validate_step_key(step_key, resolved_db_path)

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "step_runs", "queue_items", "state_transitions"),
        )

        run_row = _load_run_context(connection, normalized_run_id)
        if run_row is None:
            raise StepRunPersistenceError(
                code=STEP_RUN_NOT_FOUND,
                message=f"Run is not present in SQLite: {normalized_run_id}",
                database_path=resolved_db_path,
            )

        _validate_run_startable(run_row, resolved_db_path)
        _ensure_queue_item_present(run_row, resolved_db_path)
        _ensure_no_active_step_runs(connection, normalized_run_id, resolved_db_path)
        _ensure_step_key_not_started(connection, normalized_run_id, normalized_step_key, resolved_db_path)

        now = _utc_now()
        step_run_id = generate_opaque_id()

        try:
            connection.execute("BEGIN")

            if run_row["status"] == "queued":
                _transition_run_status(
                    connection,
                    run_id=normalized_run_id,
                    from_state="queued",
                    to_state="running",
                    transition_type=PROVISIONAL_RUN_START_TRANSITION_TYPE,
                    created_at=now,
                )
                run_row = _load_run_context(connection, normalized_run_id)
                if run_row is None:
                    raise StepRunPersistenceError(
                        code=STEP_RUN_STORAGE_ERROR,
                        message=f"Run row disappeared during step start: {normalized_run_id}",
                        database_path=resolved_db_path,
                    )

            if run_row["queue_status"] == "queued":
                _transition_queue_item_status(
                    connection,
                    queue_item_id=run_row["queue_item_id"],
                    from_state="queued",
                    to_state="claimed",
                    transition_type=PROVISIONAL_QUEUE_CLAIM_TRANSITION_TYPE,
                    created_at=now,
                )

            connection.execute(
                """
                INSERT INTO step_runs (
                  id,
                  run_id,
                  step_key,
                  attempt_no,
                  previous_step_run_id,
                  status,
                  created_at,
                  started_at,
                  terminal_at
                )
                VALUES (?, ?, ?, 1, NULL, ?, ?, ?, NULL)
                """,
                (
                    step_run_id,
                    normalized_run_id,
                    normalized_step_key,
                    STEP_RUN_ACTIVE_STATUS,
                    now,
                    now,
                ),
            )
            _insert_state_transition(
                connection,
                entity_type="step_run",
                step_run_id=step_run_id,
                from_state=None,
                to_state=STEP_RUN_ACTIVE_STATUS,
                transition_type=PROVISIONAL_STEP_RUN_START_TRANSITION_TYPE,
                created_at=now,
                metadata={"attempt_no": 1, "step_key": normalized_step_key},
            )

            connection.commit()
        except Exception:
            connection.rollback()
            raise

        details = _load_step_run_details(connection, step_run_id)
        if details is None:
            raise StepRunPersistenceError(
                code=STEP_RUN_STORAGE_ERROR,
                message=f"step_run row is missing after start: {step_run_id}",
                database_path=resolved_db_path,
            )
        return details
    except sqlite3.Error as exc:
        raise StepRunPersistenceError(
            code=STEP_RUN_STORAGE_ERROR,
            message="SQLite step_run start failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def finish_step_run(database_path: str | Path, step_run_id: str, status: str) -> StepRunDetails:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_step_run_id = _require_non_empty_string("step_run_id", step_run_id, resolved_db_path)
    terminal_status = _validate_terminal_step_status(status, resolved_db_path)

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "step_runs", "queue_items", "state_transitions"),
        )

        existing_step_run = _load_step_run_summary(connection, normalized_step_run_id)
        if existing_step_run is None:
            raise StepRunPersistenceError(
                code=STEP_RUN_NOT_FOUND,
                message=f"step_run is not present in SQLite: {normalized_step_run_id}",
                database_path=resolved_db_path,
            )
        if existing_step_run.status != STEP_RUN_ACTIVE_STATUS:
            raise StepRunPersistenceError(
                code=STEP_RUN_NOT_RUNNING,
                message=f"step_run is not running: {normalized_step_run_id}",
                database_path=resolved_db_path,
                details=f"actual_status={existing_step_run.status}",
            )

        now = _utc_now()

        try:
            connection.execute("BEGIN")
            connection.execute(
                """
                UPDATE step_runs
                SET status = ?, terminal_at = ?
                WHERE id = ?
                """,
                (terminal_status, now, normalized_step_run_id),
            )
            _insert_state_transition(
                connection,
                entity_type="step_run",
                step_run_id=normalized_step_run_id,
                from_state=STEP_RUN_ACTIVE_STATUS,
                to_state=terminal_status,
                transition_type=PROVISIONAL_STEP_RUN_FINISH_TRANSITION_TYPE,
                created_at=now,
                metadata={"step_key": existing_step_run.step_key},
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        details = _load_step_run_details(connection, normalized_step_run_id)
        if details is None:
            raise StepRunPersistenceError(
                code=STEP_RUN_STORAGE_ERROR,
                message=f"step_run row is missing after finish: {normalized_step_run_id}",
                database_path=resolved_db_path,
            )
        return details
    except sqlite3.Error as exc:
        raise StepRunPersistenceError(
            code=STEP_RUN_STORAGE_ERROR,
            message="SQLite step_run finish failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def retry_step_run(database_path: str | Path, previous_step_run_id: str) -> StepRunDetails:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_previous_step_run_id = _require_non_empty_string(
        "previous_step_run_id",
        previous_step_run_id,
        resolved_db_path,
    )

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "step_runs", "queue_items", "state_transitions"),
        )

        previous_step_run = _load_step_run_summary(connection, normalized_previous_step_run_id)
        if previous_step_run is None:
            raise StepRunPersistenceError(
                code=STEP_RUN_NOT_FOUND,
                message=f"step_run is not present in SQLite: {normalized_previous_step_run_id}",
                database_path=resolved_db_path,
            )
        if previous_step_run.status not in STEP_RUN_TERMINAL_STATUSES:
            raise StepRunPersistenceError(
                code=STEP_RUN_NOT_TERMINAL,
                message=f"step_run retry requires a terminal predecessor: {normalized_previous_step_run_id}",
                database_path=resolved_db_path,
                details=f"actual_status={previous_step_run.status}",
            )

        run_row = _load_run_context(connection, previous_step_run.run_id)
        if run_row is None:
            raise StepRunPersistenceError(
                code=STEP_RUN_STORAGE_ERROR,
                message=f"Run row is missing for retry source: {previous_step_run.run_id}",
                database_path=resolved_db_path,
            )

        _validate_run_startable(run_row, resolved_db_path)
        _ensure_queue_item_present(run_row, resolved_db_path)
        _ensure_no_active_step_runs(connection, previous_step_run.run_id, resolved_db_path)

        now = _utc_now()
        new_step_run_id = generate_opaque_id()
        next_attempt_no = previous_step_run.attempt_no + 1

        try:
            connection.execute("BEGIN")

            if run_row["status"] == "queued":
                _transition_run_status(
                    connection,
                    run_id=previous_step_run.run_id,
                    from_state="queued",
                    to_state="running",
                    transition_type=PROVISIONAL_RUN_START_TRANSITION_TYPE,
                    created_at=now,
                )
                run_row = _load_run_context(connection, previous_step_run.run_id)
                if run_row is None:
                    raise StepRunPersistenceError(
                        code=STEP_RUN_STORAGE_ERROR,
                        message=f"Run row disappeared during retry start: {previous_step_run.run_id}",
                        database_path=resolved_db_path,
                    )

            if run_row["queue_status"] == "queued":
                _transition_queue_item_status(
                    connection,
                    queue_item_id=run_row["queue_item_id"],
                    from_state="queued",
                    to_state="claimed",
                    transition_type=PROVISIONAL_QUEUE_CLAIM_TRANSITION_TYPE,
                    created_at=now,
                )

            connection.execute(
                """
                INSERT INTO step_runs (
                  id,
                  run_id,
                  step_key,
                  attempt_no,
                  previous_step_run_id,
                  status,
                  created_at,
                  started_at,
                  terminal_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    new_step_run_id,
                    previous_step_run.run_id,
                    previous_step_run.step_key,
                    next_attempt_no,
                    previous_step_run.id,
                    STEP_RUN_ACTIVE_STATUS,
                    now,
                    now,
                ),
            )
            _insert_state_transition(
                connection,
                entity_type="step_run",
                step_run_id=new_step_run_id,
                from_state=None,
                to_state=STEP_RUN_ACTIVE_STATUS,
                transition_type=PROVISIONAL_STEP_RUN_RETRY_TRANSITION_TYPE,
                created_at=now,
                metadata={
                    "attempt_no": next_attempt_no,
                    "previous_step_run_id": previous_step_run.id,
                    "step_key": previous_step_run.step_key,
                },
            )

            connection.commit()
        except Exception:
            connection.rollback()
            raise

        details = _load_step_run_details(connection, new_step_run_id)
        if details is None:
            raise StepRunPersistenceError(
                code=STEP_RUN_STORAGE_ERROR,
                message=f"retry step_run row is missing after create: {new_step_run_id}",
                database_path=resolved_db_path,
            )
        return details
    except sqlite3.Error as exc:
        raise StepRunPersistenceError(
            code=STEP_RUN_STORAGE_ERROR,
            message="SQLite step_run retry failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def get_step_run(database_path: str | Path, step_run_id: str) -> StepRunDetails:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_step_run_id = _require_non_empty_string("step_run_id", step_run_id, resolved_db_path)

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "step_runs", "queue_items", "state_transitions"),
        )
        details = _load_step_run_details(connection, normalized_step_run_id)
        if details is None:
            raise StepRunPersistenceError(
                code=STEP_RUN_NOT_FOUND,
                message=f"step_run is not present in SQLite: {normalized_step_run_id}",
                database_path=resolved_db_path,
            )
        return details
    except sqlite3.Error as exc:
        raise StepRunPersistenceError(
            code=STEP_RUN_STORAGE_ERROR,
            message="SQLite step_run lookup failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def list_step_runs(
    database_path: str | Path,
    *,
    run_id: str | None = None,
    step_key: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[StepRunSummary]:
    resolved_db_path = _resolve_database_path(database_path)
    if limit <= 0:
        raise StepRunPersistenceError(
            code=STEP_RUN_STORAGE_ERROR,
            message="limit must be greater than zero",
            database_path=resolved_db_path,
        )

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "step_runs"),
        )

        filters: list[str] = []
        params: list[object] = []

        if run_id is not None:
            filters.append("step_runs.run_id = ?")
            params.append(_require_non_empty_string("run_id", run_id, resolved_db_path))
        if step_key is not None:
            filters.append("step_runs.step_key = ?")
            params.append(_validate_step_key(step_key, resolved_db_path))
        if status is not None:
            filters.append("step_runs.status = ?")
            params.append(_validate_step_run_status(status, resolved_db_path))

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        rows = connection.execute(
            f"""
            SELECT
              step_runs.id,
              step_runs.run_id,
              runs.project_id,
              projects.project_key,
              runs.flow_id,
              runs.project_profile,
              runs.workflow_id,
              runs.milestone,
              runs.status AS run_status,
              step_runs.step_key,
              step_runs.attempt_no,
              step_runs.previous_step_run_id,
              step_runs.status,
              step_runs.created_at,
              step_runs.started_at,
              step_runs.terminal_at
            FROM step_runs
            JOIN runs ON runs.id = step_runs.run_id
            JOIN projects ON projects.id = runs.project_id
            {where_sql}
            ORDER BY step_runs.created_at, step_runs.id
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [_row_to_step_run_summary(row) for row in rows]
    except sqlite3.Error as exc:
        raise StepRunPersistenceError(
            code=STEP_RUN_STORAGE_ERROR,
            message="SQLite step_run listing failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _load_step_run_summary(connection: sqlite3.Connection, step_run_id: str) -> StepRunSummary | None:
    row = connection.execute(
        """
        SELECT
          step_runs.id,
          step_runs.run_id,
          runs.project_id,
          projects.project_key,
          runs.flow_id,
          runs.project_profile,
          runs.workflow_id,
          runs.milestone,
          runs.status AS run_status,
          step_runs.step_key,
          step_runs.attempt_no,
          step_runs.previous_step_run_id,
          step_runs.status,
          step_runs.created_at,
          step_runs.started_at,
          step_runs.terminal_at
        FROM step_runs
        JOIN runs ON runs.id = step_runs.run_id
        JOIN projects ON projects.id = runs.project_id
        WHERE step_runs.id = ?
        """,
        (step_run_id,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_step_run_summary(row)


def _row_to_step_run_summary(row: sqlite3.Row) -> StepRunSummary:
    return StepRunSummary(
        id=row["id"],
        run_id=row["run_id"],
        project_id=row["project_id"],
        project_key=row["project_key"],
        flow_id=row["flow_id"],
        project_profile=row["project_profile"],
        workflow_id=row["workflow_id"],
        milestone=row["milestone"],
        run_status=row["run_status"],
        step_key=row["step_key"],
        attempt_no=row["attempt_no"],
        previous_step_run_id=row["previous_step_run_id"],
        status=row["status"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        terminal_at=row["terminal_at"],
    )


def _load_step_run_details(connection: sqlite3.Connection, step_run_id: str) -> StepRunDetails | None:
    step_run = _load_step_run_summary(connection, step_run_id)
    if step_run is None:
        return None

    run_row = _load_run_context(connection, step_run.run_id)
    if run_row is None:
        raise StepRunPersistenceError(
            code=STEP_RUN_STORAGE_ERROR,
            message=f"Run row is missing for step_run: {step_run.id}",
            database_path=Path(connection.execute("PRAGMA database_list").fetchone()[2]).resolve(),
        )
    run = _row_to_run_summary(run_row)

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
        WHERE step_run_id = ?
        ORDER BY created_at, id
        """,
        (step_run.id,),
    ).fetchall()

    return StepRunDetails(
        step_run=step_run,
        run=run,
        state_transitions=tuple(_row_to_state_transition(row) for row in transition_rows),
    )


def _load_run_context(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return connection.execute(
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


def _validate_step_key(step_key: str, database_path: Path) -> str:
    normalized = _require_non_empty_string("step_key", step_key, database_path)
    if normalized not in STEP_KEYS:
        raise StepRunPersistenceError(
            code=INVALID_STEP_KEY,
            message=f"step_key must be one of: {', '.join(STEP_KEYS)}",
            database_path=database_path,
            details=f"actual={normalized}",
        )
    return normalized


def _validate_step_run_status(status: str, database_path: Path) -> str:
    normalized = _require_non_empty_string("status", status, database_path)
    if normalized not in STEP_RUN_STATUSES:
        raise StepRunPersistenceError(
            code=INVALID_STEP_RUN_STATUS,
            message=f"step_run status must be one of: {', '.join(STEP_RUN_STATUSES)}",
            database_path=database_path,
            details=f"actual={normalized}",
        )
    return normalized


def _validate_terminal_step_status(status: str, database_path: Path) -> str:
    normalized = _validate_step_run_status(status, database_path)
    if normalized not in STEP_RUN_TERMINAL_STATUSES:
        raise StepRunPersistenceError(
            code=INVALID_STEP_RUN_STATUS,
            message=f"finish-step-run status must be one of: {', '.join(STEP_RUN_TERMINAL_STATUSES)}",
            database_path=database_path,
            details=f"actual={normalized}",
        )
    return normalized


def _validate_run_startable(run_row: sqlite3.Row, database_path: Path) -> None:
    if run_row["status"] not in {"queued", "running"}:
        raise StepRunPersistenceError(
            code=RUN_NOT_STARTABLE,
            message=f"Run cannot accept a step_run in status {run_row['status']}: {run_row['id']}",
            database_path=database_path,
        )
    if run_row["queue_status"] not in {"queued", "claimed"}:
        raise StepRunPersistenceError(
            code=RUN_NOT_STARTABLE,
            message=f"Queue item cannot back an active step_run in status {run_row['queue_status']}: {run_row['queue_item_id']}",
            database_path=database_path,
        )


def _ensure_queue_item_present(run_row: sqlite3.Row, database_path: Path) -> None:
    if run_row["queue_item_id"] is None:
        raise StepRunPersistenceError(
            code=MISSING_QUEUE_ITEM,
            message=f"Run is missing its queue_item row: {run_row['id']}",
            database_path=database_path,
        )


def _ensure_no_active_step_runs(connection: sqlite3.Connection, run_id: str, database_path: Path) -> None:
    active_row = connection.execute(
        """
        SELECT id, step_key
        FROM step_runs
        WHERE run_id = ? AND status = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (run_id, STEP_RUN_ACTIVE_STATUS),
    ).fetchone()
    if active_row is not None:
        raise StepRunPersistenceError(
            code=ACTIVE_STEP_RUN_EXISTS,
            message=f"Run already has an active step_run: {active_row['id']}",
            database_path=database_path,
            details=f"step_key={active_row['step_key']}",
        )


def _ensure_step_key_not_started(
    connection: sqlite3.Connection,
    run_id: str,
    step_key: str,
    database_path: Path,
) -> None:
    row = connection.execute(
        """
        SELECT id
        FROM step_runs
        WHERE run_id = ? AND step_key = ?
        LIMIT 1
        """,
        (run_id, step_key),
    ).fetchone()
    if row is not None:
        raise StepRunPersistenceError(
            code=STEP_KEY_ALREADY_STARTED,
            message=f"step_key already exists on this run; use retry-step-run for additional attempts: {step_key}",
            database_path=database_path,
            details=f"existing_step_run_id={row['id']}",
        )


def _transition_run_status(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    from_state: str,
    to_state: str,
    transition_type: str,
    created_at: str,
) -> None:
    connection.execute(
        """
        UPDATE runs
        SET status = ?, updated_at = ?, started_at = COALESCE(started_at, ?)
        WHERE id = ?
        """,
        (to_state, created_at, created_at, run_id),
    )
    _insert_state_transition(
        connection,
        entity_type="run",
        run_id=run_id,
        from_state=from_state,
        to_state=to_state,
        transition_type=transition_type,
        created_at=created_at,
    )


def _transition_queue_item_status(
    connection: sqlite3.Connection,
    *,
    queue_item_id: str,
    from_state: str,
    to_state: str,
    transition_type: str,
    created_at: str,
) -> None:
    claimed_at = created_at if to_state == "claimed" else None
    terminal_at = created_at if to_state in {"completed", "cancelled"} else None
    connection.execute(
        """
        UPDATE queue_items
        SET status = ?, claimed_at = COALESCE(claimed_at, ?), terminal_at = ?
        WHERE id = ?
        """,
        (to_state, claimed_at, terminal_at, queue_item_id),
    )
    _insert_state_transition(
        connection,
        entity_type="queue_item",
        queue_item_id=queue_item_id,
        from_state=from_state,
        to_state=to_state,
        transition_type=transition_type,
        created_at=created_at,
    )


def _insert_state_transition(
    connection: sqlite3.Connection,
    *,
    entity_type: str,
    from_state: str | None,
    to_state: str,
    transition_type: str,
    created_at: str,
    run_id: str | None = None,
    step_run_id: str | None = None,
    queue_item_id: str | None = None,
    reason_code: str | None = None,
    metadata: dict[str, object] | None = None,
) -> str:
    metadata_json = None if metadata is None else json.dumps(metadata, sort_keys=True)
    transition_id = generate_state_transition_id()
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            transition_id,
            entity_type,
            run_id,
            step_run_id,
            queue_item_id,
            from_state,
            to_state,
            transition_type,
            reason_code,
            metadata_json,
            created_at,
        ),
    )
    return transition_id
