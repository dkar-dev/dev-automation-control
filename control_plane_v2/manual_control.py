from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3

from .run_persistence import (
    RunDetails,
    RunPersistenceError,
    _connect_run_db,
    _ensure_required_tables,
    _resolve_database_path,
    _utc_now,
    get_run,
)
from .step_run_persistence import (
    STEP_RUN_ACTIVE_STATUS,
    STEP_RUN_TERMINAL_STATUSES,
    StepRunDetails,
    StepRunPersistenceError,
    get_step_run,
    _ensure_queue_item_present,
    _insert_state_transition,
    _load_run_context,
)


MANUAL_RESUME_MODES = ("normal", "stabilize_to_green")

PROVISIONAL_MANUAL_RUN_PAUSED_TRANSITION_TYPE = "manual_run_paused"
PROVISIONAL_MANUAL_QUEUE_PAUSED_TRANSITION_TYPE = "manual_queue_paused"
PROVISIONAL_MANUAL_RUN_RESUMED_TRANSITION_TYPE = "manual_run_resumed"
PROVISIONAL_MANUAL_QUEUE_RESUMED_TRANSITION_TYPE = "manual_queue_resumed"
PROVISIONAL_MANUAL_RUN_FORCE_STOPPED_TRANSITION_TYPE = "manual_run_force_stopped"
PROVISIONAL_MANUAL_QUEUE_FORCE_STOPPED_TRANSITION_TYPE = "manual_queue_force_stopped"
PROVISIONAL_MANUAL_RUN_RERUN_REQUESTED_TRANSITION_TYPE = "manual_run_rerun_requested"
PROVISIONAL_MANUAL_QUEUE_RERUN_REQUEUED_TRANSITION_TYPE = "manual_queue_rerun_requeued"

MANUAL_ACTIVE_STEP_NOT_SAFE = "MANUAL_ACTIVE_STEP_NOT_SAFE"
MANUAL_CONTROL_STORAGE_ERROR = "MANUAL_CONTROL_STORAGE_ERROR"
MANUAL_RUN_NOT_FOUND = "MANUAL_RUN_NOT_FOUND"
MANUAL_RUN_NOT_PAUSABLE = "MANUAL_RUN_NOT_PAUSABLE"
MANUAL_RUN_NOT_PAUSED = "MANUAL_RUN_NOT_PAUSED"
MANUAL_RUN_NOT_FORCE_STOPPABLE = "MANUAL_RUN_NOT_FORCE_STOPPABLE"
MANUAL_STEP_NOT_FOUND = "MANUAL_STEP_NOT_FOUND"
MANUAL_STEP_NOT_RERUNNABLE = "MANUAL_STEP_NOT_RERUNNABLE"
MANUAL_UNSUPPORTED_RESUME_MODE = "MANUAL_UNSUPPORTED_RESUME_MODE"

_RUN_TERMINAL_STATUSES = {"completed", "failed", "stopped", "cancelled"}
_QUEUE_TERMINAL_STATUSES = {"completed", "cancelled"}


@dataclass(frozen=True)
class PendingRerunIntent:
    transition_id: str
    run_id: str
    source_step_run_id: str
    step_key: str
    source_attempt_no: int
    source_status: str
    created_at: str
    note: str | None
    operator: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "transition_id": self.transition_id,
            "run_id": self.run_id,
            "source_step_run_id": self.source_step_run_id,
            "step_key": self.step_key,
            "source_attempt_no": self.source_attempt_no,
            "source_status": self.source_status,
            "created_at": self.created_at,
            "note": self.note,
            "operator": self.operator,
        }


@dataclass(frozen=True)
class RunControlState:
    run_id: str
    run_status: str
    queue_item_id: str | None
    queue_status: str | None
    active_step_run_id: str | None
    active_step_key: str | None
    scheduling_eligible: bool
    paused: bool
    terminal: bool
    latest_manual_transition_type: str | None
    latest_manual_transition_at: str | None
    latest_resume_mode: str | None
    pending_rerun: PendingRerunIntent | None

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "run_status": self.run_status,
            "queue_item_id": self.queue_item_id,
            "queue_status": self.queue_status,
            "active_step_run_id": self.active_step_run_id,
            "active_step_key": self.active_step_key,
            "scheduling_eligible": self.scheduling_eligible,
            "paused": self.paused,
            "terminal": self.terminal,
            "latest_manual_transition_type": self.latest_manual_transition_type,
            "latest_manual_transition_at": self.latest_manual_transition_at,
            "latest_resume_mode": self.latest_resume_mode,
            "pending_rerun": self.pending_rerun.to_dict() if self.pending_rerun is not None else None,
        }


@dataclass(frozen=True)
class ManualControlResult:
    operation: str
    run: RunDetails
    control_state: RunControlState
    source_step_run: StepRunDetails | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "operation": self.operation,
            "run": self.run.to_dict(),
            "control_state": self.control_state.to_dict(),
            "source_step_run": self.source_step_run.to_dict() if self.source_step_run is not None else None,
        }


class ManualControlError(Exception):
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


def show_run_control_state(database_path: str | Path, run_id: str) -> RunControlState:
    resolved_db_path = _resolve_manual_database_path(database_path)
    normalized_run_id = _normalize_required_text("run_id", run_id, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runs", "queue_items", "step_runs", "state_transitions"))
        run_row = _load_run_context(connection, normalized_run_id)
        if run_row is None:
            raise ManualControlError(
                code=MANUAL_RUN_NOT_FOUND,
                message=f"run is not present in SQLite: {normalized_run_id}",
                database_path=resolved_db_path,
            )
        _ensure_queue_item_present(run_row, resolved_db_path)
        return _build_run_control_state(connection, run_row)
    except ManualControlError:
        raise
    except sqlite3.Error as exc:
        raise ManualControlError(
            code=MANUAL_CONTROL_STORAGE_ERROR,
            message="SQLite run control state lookup failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def get_pending_rerun_intent(database_path: str | Path, run_id: str) -> PendingRerunIntent | None:
    resolved_db_path = _resolve_manual_database_path(database_path)
    normalized_run_id = _normalize_required_text("run_id", run_id, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runs", "step_runs", "state_transitions"))
        return _load_pending_rerun_intent(connection, normalized_run_id)
    except sqlite3.Error as exc:
        raise ManualControlError(
            code=MANUAL_CONTROL_STORAGE_ERROR,
            message="SQLite rerun intent lookup failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def pause_run(
    database_path: str | Path,
    run_id: str,
    *,
    note: str | None = None,
    operator: str | None = None,
) -> ManualControlResult:
    resolved_db_path = _resolve_manual_database_path(database_path)
    normalized_run_id = _normalize_required_text("run_id", run_id, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runs", "queue_items", "step_runs", "state_transitions"))
        run_row = _require_run_row(connection, normalized_run_id, resolved_db_path)
        active_step = _load_active_step_row(connection, normalized_run_id)
        if active_step is not None:
            raise ManualControlError(
                code=MANUAL_ACTIVE_STEP_NOT_SAFE,
                message=f"pause is not safe while an active step_run exists: {active_step['id']}",
                database_path=resolved_db_path,
                details=f"step_key={active_step['step_key']}",
            )
        if run_row["status"] == "paused" or run_row["queue_status"] == "paused":
            raise ManualControlError(
                code=MANUAL_RUN_NOT_PAUSABLE,
                message=f"run is already paused: {normalized_run_id}",
                database_path=resolved_db_path,
            )
        if run_row["status"] not in {"queued", "running"} or run_row["queue_status"] not in {"queued", "claimed"}:
            raise ManualControlError(
                code=MANUAL_RUN_NOT_PAUSABLE,
                message=f"run cannot be paused in its current state: {normalized_run_id}",
                database_path=resolved_db_path,
                details=f"run_status={run_row['status']} queue_status={run_row['queue_status']}",
            )

        now = _utc_now()
        metadata = _manual_metadata(note=note, operator=operator)
        try:
            connection.execute("BEGIN")
            _update_run_state(
                connection,
                run_id=normalized_run_id,
                to_state="paused",
                updated_at=now,
                queued_at=run_row["queued_at"],
                terminal_at=None,
            )
            _insert_state_transition(
                connection,
                entity_type="run",
                run_id=normalized_run_id,
                from_state=run_row["status"],
                to_state="paused",
                transition_type=PROVISIONAL_MANUAL_RUN_PAUSED_TRANSITION_TYPE,
                created_at=now,
                reason_code="pause_requested",
                metadata=metadata,
            )
            _update_queue_state(
                connection,
                queue_item_id=run_row["queue_item_id"],
                to_state="paused",
                enqueued_at=run_row["enqueued_at"],
                available_at=run_row["available_at"],
                claimed_at=None,
                terminal_at=None,
            )
            _insert_state_transition(
                connection,
                entity_type="queue_item",
                queue_item_id=run_row["queue_item_id"],
                from_state=run_row["queue_status"],
                to_state="paused",
                transition_type=PROVISIONAL_MANUAL_QUEUE_PAUSED_TRANSITION_TYPE,
                created_at=now,
                reason_code="pause_requested",
                metadata=metadata,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return _build_manual_result(resolved_db_path, operation="pause", run_id=normalized_run_id)
    except ManualControlError:
        raise
    except sqlite3.Error as exc:
        raise ManualControlError(
            code=MANUAL_CONTROL_STORAGE_ERROR,
            message="SQLite pause operation failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def resume_run(
    database_path: str | Path,
    run_id: str,
    *,
    mode: str = "normal",
    note: str | None = None,
    operator: str | None = None,
) -> ManualControlResult:
    resolved_db_path = _resolve_manual_database_path(database_path)
    normalized_run_id = _normalize_required_text("run_id", run_id, resolved_db_path)
    normalized_mode = _normalize_resume_mode(mode, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runs", "queue_items", "step_runs", "state_transitions"))
        run_row = _require_run_row(connection, normalized_run_id, resolved_db_path)
        active_step = _load_active_step_row(connection, normalized_run_id)
        if active_step is not None:
            raise ManualControlError(
                code=MANUAL_ACTIVE_STEP_NOT_SAFE,
                message=f"resume is not safe while an active step_run exists: {active_step['id']}",
                database_path=resolved_db_path,
                details=f"step_key={active_step['step_key']}",
            )
        if run_row["status"] != "paused" or run_row["queue_status"] != "paused":
            raise ManualControlError(
                code=MANUAL_RUN_NOT_PAUSED,
                message=f"run is not paused and cannot be resumed: {normalized_run_id}",
                database_path=resolved_db_path,
                details=f"run_status={run_row['status']} queue_status={run_row['queue_status']}",
            )

        now = _utc_now()
        metadata = _manual_metadata(note=note, operator=operator, extra={"resume_mode": normalized_mode})
        try:
            connection.execute("BEGIN")
            _update_run_state(
                connection,
                run_id=normalized_run_id,
                to_state="queued",
                updated_at=now,
                queued_at=now,
                terminal_at=None,
            )
            _insert_state_transition(
                connection,
                entity_type="run",
                run_id=normalized_run_id,
                from_state="paused",
                to_state="queued",
                transition_type=PROVISIONAL_MANUAL_RUN_RESUMED_TRANSITION_TYPE,
                created_at=now,
                reason_code=f"resume_{normalized_mode}",
                metadata=metadata,
            )
            _update_queue_state(
                connection,
                queue_item_id=run_row["queue_item_id"],
                to_state="queued",
                enqueued_at=now,
                available_at=now,
                claimed_at=None,
                terminal_at=None,
            )
            _insert_state_transition(
                connection,
                entity_type="queue_item",
                queue_item_id=run_row["queue_item_id"],
                from_state="paused",
                to_state="queued",
                transition_type=PROVISIONAL_MANUAL_QUEUE_RESUMED_TRANSITION_TYPE,
                created_at=now,
                reason_code=f"resume_{normalized_mode}",
                metadata=metadata,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return _build_manual_result(resolved_db_path, operation="resume", run_id=normalized_run_id)
    except ManualControlError:
        raise
    except sqlite3.Error as exc:
        raise ManualControlError(
            code=MANUAL_CONTROL_STORAGE_ERROR,
            message="SQLite resume operation failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def force_stop_run(
    database_path: str | Path,
    run_id: str,
    *,
    note: str | None = None,
    operator: str | None = None,
) -> ManualControlResult:
    resolved_db_path = _resolve_manual_database_path(database_path)
    normalized_run_id = _normalize_required_text("run_id", run_id, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runs", "queue_items", "step_runs", "state_transitions"))
        run_row = _require_run_row(connection, normalized_run_id, resolved_db_path)
        if run_row["status"] in _RUN_TERMINAL_STATUSES:
            raise ManualControlError(
                code=MANUAL_RUN_NOT_FORCE_STOPPABLE,
                message=f"run is already terminal and cannot be force-stopped: {normalized_run_id}",
                database_path=resolved_db_path,
                details=f"run_status={run_row['status']}",
            )
        if run_row["queue_status"] in _QUEUE_TERMINAL_STATUSES:
            raise ManualControlError(
                code=MANUAL_RUN_NOT_FORCE_STOPPABLE,
                message=f"queue item is already terminal and cannot be force-stopped: {run_row['queue_item_id']}",
                database_path=resolved_db_path,
                details=f"queue_status={run_row['queue_status']}",
            )

        active_step = _load_active_step_row(connection, normalized_run_id)
        now = _utc_now()
        metadata = _manual_metadata(
            note=note,
            operator=operator,
            extra={
                "active_step_run_id": active_step["id"] if active_step is not None else None,
                "active_step_key": active_step["step_key"] if active_step is not None else None,
                "backend_interrupt_performed": False,
            },
        )
        try:
            connection.execute("BEGIN")
            _update_run_state(
                connection,
                run_id=normalized_run_id,
                to_state="stopped",
                updated_at=now,
                queued_at=run_row["queued_at"],
                terminal_at=now,
            )
            _insert_state_transition(
                connection,
                entity_type="run",
                run_id=normalized_run_id,
                from_state=run_row["status"],
                to_state="stopped",
                transition_type=PROVISIONAL_MANUAL_RUN_FORCE_STOPPED_TRANSITION_TYPE,
                created_at=now,
                reason_code="force_stopped",
                metadata=metadata,
            )
            _update_queue_state(
                connection,
                queue_item_id=run_row["queue_item_id"],
                to_state="cancelled",
                enqueued_at=run_row["enqueued_at"],
                available_at=run_row["available_at"],
                claimed_at=run_row["claimed_at"],
                terminal_at=now,
            )
            _insert_state_transition(
                connection,
                entity_type="queue_item",
                queue_item_id=run_row["queue_item_id"],
                from_state=run_row["queue_status"],
                to_state="cancelled",
                transition_type=PROVISIONAL_MANUAL_QUEUE_FORCE_STOPPED_TRANSITION_TYPE,
                created_at=now,
                reason_code="force_stopped",
                metadata=metadata,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return _build_manual_result(resolved_db_path, operation="force_stop", run_id=normalized_run_id)
    except ManualControlError:
        raise
    except sqlite3.Error as exc:
        raise ManualControlError(
            code=MANUAL_CONTROL_STORAGE_ERROR,
            message="SQLite force-stop operation failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def rerun_run_step(
    database_path: str | Path,
    step_run_id: str,
    *,
    note: str | None = None,
    operator: str | None = None,
) -> ManualControlResult:
    resolved_db_path = _resolve_manual_database_path(database_path)
    normalized_step_run_id = _normalize_required_text("step_run_id", step_run_id, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runs", "queue_items", "step_runs", "state_transitions"))
        step_row = _load_step_run_row(connection, normalized_step_run_id)
        if step_row is None:
            raise ManualControlError(
                code=MANUAL_STEP_NOT_FOUND,
                message=f"step_run is not present in SQLite: {normalized_step_run_id}",
                database_path=resolved_db_path,
            )
        if step_row["status"] not in STEP_RUN_TERMINAL_STATUSES:
            raise ManualControlError(
                code=MANUAL_STEP_NOT_RERUNNABLE,
                message=f"step_run must be terminal before rerun: {normalized_step_run_id}",
                database_path=resolved_db_path,
                details=f"actual_status={step_row['status']}",
            )

        run_row = _require_run_row(connection, step_row["run_id"], resolved_db_path)
        active_step = _load_active_step_row(connection, step_row["run_id"])
        if active_step is not None:
            raise ManualControlError(
                code=MANUAL_ACTIVE_STEP_NOT_SAFE,
                message=f"rerun is not safe while an active step_run exists: {active_step['id']}",
                database_path=resolved_db_path,
                details=f"step_key={active_step['step_key']}",
            )

        step_rows = connection.execute(
            """
            SELECT id, step_key, status, previous_step_run_id
            FROM step_runs
            WHERE run_id = ?
            ORDER BY created_at, id
            """,
            (step_row["run_id"],),
        ).fetchall()
        _validate_rerun_source(step_row, run_row, step_rows, resolved_db_path)

        now = _utc_now()
        metadata = _manual_metadata(
            note=note,
            operator=operator,
            extra={
                "rerun_step_key": step_row["step_key"],
                "source_step_run_id": step_row["id"],
                "source_attempt_no": step_row["attempt_no"],
                "source_step_status": step_row["status"],
            },
        )
        try:
            connection.execute("BEGIN")
            _update_run_state(
                connection,
                run_id=step_row["run_id"],
                to_state="queued",
                updated_at=now,
                queued_at=now,
                terminal_at=None,
            )
            _insert_state_transition(
                connection,
                entity_type="run",
                run_id=step_row["run_id"],
                from_state=run_row["status"],
                to_state="queued",
                transition_type=PROVISIONAL_MANUAL_RUN_RERUN_REQUESTED_TRANSITION_TYPE,
                created_at=now,
                reason_code="rerun_requested",
                metadata=metadata,
            )
            _update_queue_state(
                connection,
                queue_item_id=run_row["queue_item_id"],
                to_state="queued",
                enqueued_at=now,
                available_at=now,
                claimed_at=None,
                terminal_at=None,
            )
            _insert_state_transition(
                connection,
                entity_type="queue_item",
                queue_item_id=run_row["queue_item_id"],
                from_state=run_row["queue_status"],
                to_state="queued",
                transition_type=PROVISIONAL_MANUAL_QUEUE_RERUN_REQUEUED_TRANSITION_TYPE,
                created_at=now,
                reason_code="rerun_requested",
                metadata=metadata,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return _build_manual_result(
            resolved_db_path,
            operation="rerun_step",
            run_id=step_row["run_id"],
            source_step_run_id=step_row["id"],
        )
    except ManualControlError:
        raise
    except sqlite3.Error as exc:
        raise ManualControlError(
            code=MANUAL_CONTROL_STORAGE_ERROR,
            message="SQLite rerun operation failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _build_manual_result(
    database_path: Path,
    *,
    operation: str,
    run_id: str,
    source_step_run_id: str | None = None,
) -> ManualControlResult:
    run_details = _load_run_details_or_raise(database_path, run_id)
    control_state = show_run_control_state(database_path, run_id)
    source_step_run = _load_step_run_details_or_none(database_path, source_step_run_id)
    return ManualControlResult(
        operation=operation,
        run=run_details,
        control_state=control_state,
        source_step_run=source_step_run,
    )


def _build_run_control_state(connection: sqlite3.Connection, run_row: sqlite3.Row) -> RunControlState:
    active_step = _load_active_step_row(connection, run_row["id"])
    latest_manual_transition = connection.execute(
        """
        SELECT id, transition_type, created_at
        FROM state_transitions
        WHERE run_id = ?
          AND entity_type = 'run'
          AND transition_type IN (?, ?, ?, ?)
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (
            run_row["id"],
            PROVISIONAL_MANUAL_RUN_PAUSED_TRANSITION_TYPE,
            PROVISIONAL_MANUAL_RUN_RESUMED_TRANSITION_TYPE,
            PROVISIONAL_MANUAL_RUN_FORCE_STOPPED_TRANSITION_TYPE,
            PROVISIONAL_MANUAL_RUN_RERUN_REQUESTED_TRANSITION_TYPE,
        ),
    ).fetchone()
    latest_resume_mode = _load_latest_resume_mode(connection, run_row["id"])
    pending_rerun = _load_pending_rerun_intent(connection, run_row["id"])
    return RunControlState(
        run_id=run_row["id"],
        run_status=run_row["status"],
        queue_item_id=run_row["queue_item_id"],
        queue_status=run_row["queue_status"],
        active_step_run_id=active_step["id"] if active_step is not None else None,
        active_step_key=active_step["step_key"] if active_step is not None else None,
        scheduling_eligible=run_row["status"] in {"queued", "running"} and run_row["queue_status"] == "queued",
        paused=run_row["status"] == "paused" or run_row["queue_status"] == "paused",
        terminal=run_row["status"] in _RUN_TERMINAL_STATUSES,
        latest_manual_transition_type=(latest_manual_transition["transition_type"] if latest_manual_transition is not None else None),
        latest_manual_transition_at=(latest_manual_transition["created_at"] if latest_manual_transition is not None else None),
        latest_resume_mode=latest_resume_mode,
        pending_rerun=pending_rerun,
    )


def _load_pending_rerun_intent(connection: sqlite3.Connection, run_id: str) -> PendingRerunIntent | None:
    rows = connection.execute(
        """
        SELECT id, metadata_json, created_at
        FROM state_transitions
        WHERE run_id = ?
          AND entity_type = 'run'
          AND transition_type = ?
        ORDER BY created_at DESC, id DESC
        """,
        (run_id, PROVISIONAL_MANUAL_RUN_RERUN_REQUESTED_TRANSITION_TYPE),
    ).fetchall()
    for row in rows:
        metadata = _parse_metadata_json(row["metadata_json"])
        source_step_run_id = _normalize_optional_text(metadata.get("source_step_run_id"))
        rerun_step_key = _normalize_optional_text(metadata.get("rerun_step_key"))
        if source_step_run_id is None or rerun_step_key not in {"executor", "reviewer"}:
            continue
        step_row = _load_step_run_row(connection, source_step_run_id)
        if step_row is None or step_row["status"] not in STEP_RUN_TERMINAL_STATUSES:
            continue
        if step_row["step_key"] != rerun_step_key:
            continue
        consumed = connection.execute(
            """
            SELECT id
            FROM step_runs
            WHERE previous_step_run_id = ?
            LIMIT 1
            """,
            (source_step_run_id,),
        ).fetchone()
        if consumed is not None:
            continue
        return PendingRerunIntent(
            transition_id=row["id"],
            run_id=run_id,
            source_step_run_id=source_step_run_id,
            step_key=rerun_step_key,
            source_attempt_no=int(step_row["attempt_no"]),
            source_status=step_row["status"],
            created_at=row["created_at"],
            note=_normalize_optional_text(metadata.get("note")),
            operator=_normalize_optional_text(metadata.get("operator")),
        )
    return None


def _load_latest_resume_mode(connection: sqlite3.Connection, run_id: str) -> str | None:
    row = connection.execute(
        """
        SELECT metadata_json
        FROM state_transitions
        WHERE run_id = ?
          AND entity_type = 'run'
          AND transition_type = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (run_id, PROVISIONAL_MANUAL_RUN_RESUMED_TRANSITION_TYPE),
    ).fetchone()
    if row is None:
        return None
    metadata = _parse_metadata_json(row["metadata_json"])
    mode = _normalize_optional_text(metadata.get("resume_mode"))
    return mode if mode in MANUAL_RESUME_MODES else None


def _require_run_row(connection: sqlite3.Connection, run_id: str, database_path: Path) -> sqlite3.Row:
    run_row = _load_run_context(connection, run_id)
    if run_row is None:
        raise ManualControlError(
            code=MANUAL_RUN_NOT_FOUND,
            message=f"run is not present in SQLite: {run_id}",
            database_path=database_path,
        )
    _ensure_queue_item_present(run_row, database_path)
    return run_row


def _load_active_step_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id, step_key
        FROM step_runs
        WHERE run_id = ? AND status = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (run_id, STEP_RUN_ACTIVE_STATUS),
    ).fetchone()


def _load_step_run_row(connection: sqlite3.Connection, step_run_id: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id, run_id, step_key, attempt_no, previous_step_run_id, status, created_at, started_at, terminal_at
        FROM step_runs
        WHERE id = ?
        """,
        (step_run_id,),
    ).fetchone()


def _validate_rerun_source(
    step_row: sqlite3.Row,
    run_row: sqlite3.Row,
    step_rows: list[sqlite3.Row],
    database_path: Path,
) -> None:
    if any(row["previous_step_run_id"] == step_row["id"] for row in step_rows):
        raise ManualControlError(
            code=MANUAL_STEP_NOT_RERUNNABLE,
            message=f"step_run already has a retry attempt and is not the latest rerunnable source: {step_row['id']}",
            database_path=database_path,
        )
    if step_row["step_key"] == "executor":
        if step_row["status"] not in {"failed", "timed_out", "cancelled"}:
            raise ManualControlError(
                code=MANUAL_STEP_NOT_RERUNNABLE,
                message=f"executor rerun is limited to failed/timed_out/cancelled step paths in v1: {step_row['id']}",
                database_path=database_path,
                details=f"step_status={step_row['status']}",
            )
        if any(row["step_key"] == "reviewer" for row in step_rows):
            raise ManualControlError(
                code=MANUAL_STEP_NOT_RERUNNABLE,
                message=f"executor rerun is not supported once reviewer history exists on the same run: {step_row['run_id']}",
                database_path=database_path,
            )
        return
    if step_row["step_key"] == "reviewer":
        if run_row["status"] == "completed":
            raise ManualControlError(
                code=MANUAL_STEP_NOT_RERUNNABLE,
                message=f"reviewer rerun is not supported after completed reviewer outcomes in v1: {step_row['run_id']}",
                database_path=database_path,
            )
        return
    raise ManualControlError(
        code=MANUAL_STEP_NOT_RERUNNABLE,
        message=f"unsupported step_key for rerun: {step_row['step_key']}",
        database_path=database_path,
    )


def _update_run_state(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    to_state: str,
    updated_at: str,
    queued_at: str | None,
    terminal_at: str | None,
) -> None:
    connection.execute(
        """
        UPDATE runs
        SET status = ?, updated_at = ?, queued_at = ?, terminal_at = ?
        WHERE id = ?
        """,
        (to_state, updated_at, queued_at, terminal_at, run_id),
    )


def _update_queue_state(
    connection: sqlite3.Connection,
    *,
    queue_item_id: str,
    to_state: str,
    enqueued_at: str,
    available_at: str,
    claimed_at: str | None,
    terminal_at: str | None,
) -> None:
    connection.execute(
        """
        UPDATE queue_items
        SET status = ?, enqueued_at = ?, available_at = ?, claimed_at = ?, terminal_at = ?
        WHERE id = ?
        """,
        (to_state, enqueued_at, available_at, claimed_at, terminal_at, queue_item_id),
    )


def _manual_metadata(
    *,
    note: str | None,
    operator: str | None,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {}
    normalized_note = _normalize_optional_text(note)
    normalized_operator = _normalize_optional_text(operator)
    if normalized_note is not None:
        metadata["note"] = normalized_note
    if normalized_operator is not None:
        metadata["operator"] = normalized_operator
    if extra:
        for key, value in extra.items():
            if value is not None:
                metadata[key] = value
    return metadata


def _normalize_resume_mode(mode: str, database_path: Path) -> str:
    normalized_mode = _normalize_required_text("mode", mode, database_path)
    if normalized_mode not in MANUAL_RESUME_MODES:
        raise ManualControlError(
            code=MANUAL_UNSUPPORTED_RESUME_MODE,
            message=f"resume mode must be one of: {', '.join(MANUAL_RESUME_MODES)}",
            database_path=database_path,
            details=f"actual={mode}",
        )
    return normalized_mode


def _normalize_required_text(name: str, value: str, database_path: Path) -> str:
    normalized = (value or "").strip()
    if not normalized:
        raise ManualControlError(
            code=MANUAL_CONTROL_STORAGE_ERROR,
            message=f"{name} must not be empty",
            database_path=database_path,
        )
    return normalized


def _normalize_optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _parse_metadata_json(metadata_json: str | None) -> dict[str, object]:
    if metadata_json is None:
        return {}
    try:
        payload = json.loads(metadata_json)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_manual_database_path(database_path: str | Path) -> Path:
    try:
        return _resolve_database_path(database_path)
    except RunPersistenceError as exc:
        raise ManualControlError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _load_run_details_or_raise(database_path: Path, run_id: str) -> RunDetails:
    try:
        return get_run(database_path, run_id)
    except RunPersistenceError as exc:
        raise ManualControlError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _load_step_run_details_or_none(database_path: Path, step_run_id: str | None) -> StepRunDetails | None:
    if step_run_id is None:
        return None
    try:
        return get_step_run(database_path, step_run_id)
    except StepRunPersistenceError:
        return None
