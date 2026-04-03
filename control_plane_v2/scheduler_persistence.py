from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from .project_registry import RegisteredProject
from .run_persistence import PRIORITY_CLASSES, QueueItemRecord, RunSummary, StateTransitionRecord, _row_to_run_summary
from .step_run_persistence import _insert_state_transition


PROVISIONAL_QUEUE_CLAIM_FOR_DISPATCH_TRANSITION_TYPE = "queue_item_claimed_for_dispatch"
PROVISIONAL_QUEUE_RELEASE_TRANSITION_TYPE = "queue_item_released_to_queue"
PROVISIONAL_QUEUE_DISPATCH_FAILED_TRANSITION_TYPE = "queue_item_dispatch_failed_requeued"
PROVISIONAL_V1_AGING_FORMULA = "effective_age_seconds = max(0, now_utc - available_at_utc)"

CLAIM_TARGET_REQUIRED = "CLAIM_TARGET_REQUIRED"
CLAIMED_RUN_NOT_FOUND = "CLAIMED_RUN_NOT_FOUND"
CLAIMED_RUN_NOT_RELEASABLE = "CLAIMED_RUN_NOT_RELEASABLE"
INVALID_AVAILABLE_AT = "INVALID_AVAILABLE_AT"
INVALID_REASON_CODE = "INVALID_REASON_CODE"
QUEUE_ITEM_NOT_CLAIMED = "QUEUE_ITEM_NOT_CLAIMED"
REQUIRED_TABLES_MISSING = "REQUIRED_TABLES_MISSING"
SCHEDULER_STORAGE_ERROR = "SCHEDULER_STORAGE_ERROR"

_PRIORITY_CLASS_RANK = {priority_class: index for index, priority_class in enumerate(PRIORITY_CLASSES)}


@dataclass(frozen=True)
class FlowContextSummary:
    flow_id: str
    current_run_id: str
    root_run_id: str
    cycle_no: int
    total_runs: int
    parent_run_id: str | None
    origin_type: str
    origin_run_id: str | None
    origin_step_run_id: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "flow_id": self.flow_id,
            "current_run_id": self.current_run_id,
            "root_run_id": self.root_run_id,
            "cycle_no": self.cycle_no,
            "total_runs": self.total_runs,
            "parent_run_id": self.parent_run_id,
            "origin_type": self.origin_type,
            "origin_run_id": self.origin_run_id,
            "origin_step_run_id": self.origin_step_run_id,
        }


@dataclass(frozen=True)
class DispatchRunPayload:
    run: RunSummary
    queue_item: QueueItemRecord
    project: RegisteredProject
    project_package_root: Path
    flow_context: FlowContextSummary

    def to_dict(self) -> dict[str, object]:
        return {
            "run": self.run.to_dict(),
            "queue_item": self.queue_item.to_dict(),
            "project": self.project.to_dict(),
            "project_package_root": str(self.project_package_root),
            "flow_context": self.flow_context.to_dict(),
        }


@dataclass(frozen=True)
class RunnableRunCandidate:
    dispatch_run: DispatchRunPayload
    evaluated_at: str
    effective_age_seconds: float
    priority_rank: int

    def to_dict(self) -> dict[str, object]:
        return {
            "dispatch_run": self.dispatch_run.to_dict(),
            "evaluated_at": self.evaluated_at,
            "effective_age_seconds": self.effective_age_seconds,
            "priority_rank": self.priority_rank,
            "aging_formula": PROVISIONAL_V1_AGING_FORMULA,
        }


@dataclass(frozen=True)
class ClaimNextRunResult:
    dispatch_run: DispatchRunPayload
    transition: StateTransitionRecord
    evaluated_at: str
    effective_age_seconds: float
    priority_rank: int

    def to_dict(self) -> dict[str, object]:
        return {
            "dispatch_run": self.dispatch_run.to_dict(),
            "transition": self.transition.to_dict(),
            "evaluated_at": self.evaluated_at,
            "effective_age_seconds": self.effective_age_seconds,
            "priority_rank": self.priority_rank,
            "aging_formula": PROVISIONAL_V1_AGING_FORMULA,
        }


@dataclass(frozen=True)
class ClaimedRunMutationResult:
    operation: str
    dispatch_run: DispatchRunPayload
    transition: StateTransitionRecord

    def to_dict(self) -> dict[str, object]:
        return {
            "operation": self.operation,
            "dispatch_run": self.dispatch_run.to_dict(),
            "transition": self.transition.to_dict(),
        }


class SchedulerPersistenceError(Exception):
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


def find_next_runnable_run(
    database_path: str | Path,
    *,
    now: str | datetime | None = None,
) -> RunnableRunCandidate | None:
    resolved_db_path = _resolve_database_path(database_path)
    evaluated_at = _normalize_timestamp("now", now, resolved_db_path)

    connection = _connect_scheduler_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "queue_items"),
        )
        row = _select_next_runnable_row(connection, evaluated_at)
        if row is None:
            return None
        payload = _row_to_dispatch_payload(connection, row)
        return RunnableRunCandidate(
            dispatch_run=payload,
            evaluated_at=evaluated_at,
            effective_age_seconds=_round_effective_age(row["effective_age_seconds"]),
            priority_rank=_priority_rank_for_class(payload.queue_item.priority_class),
        )
    except sqlite3.Error as exc:
        raise SchedulerPersistenceError(
            code=SCHEDULER_STORAGE_ERROR,
            message="SQLite scheduler lookup failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def claim_next_run(
    database_path: str | Path,
    *,
    now: str | datetime | None = None,
) -> ClaimNextRunResult | None:
    resolved_db_path = _resolve_database_path(database_path)
    evaluated_at = _normalize_timestamp("now", now, resolved_db_path)

    connection = _connect_scheduler_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "queue_items", "state_transitions"),
        )

        try:
            connection.execute("BEGIN IMMEDIATE")
            row = _select_next_runnable_row(connection, evaluated_at)
            if row is None:
                connection.rollback()
                return None

            cursor = connection.execute(
                """
                UPDATE queue_items
                SET status = ?, claimed_at = ?, terminal_at = NULL
                WHERE id = ? AND status = ? AND available_at <= ?
                """,
                ("claimed", evaluated_at, row["queue_item_id"], "queued", evaluated_at),
            )
            if cursor.rowcount != 1:
                raise SchedulerPersistenceError(
                    code=SCHEDULER_STORAGE_ERROR,
                    message=f"Failed to atomically claim queue item: {row['queue_item_id']}",
                    database_path=resolved_db_path,
                    details="The queue item changed state during claim.",
                )

            transition_metadata = {
                "priority_class": row["priority_class"],
                "effective_age_seconds": _round_effective_age(row["effective_age_seconds"]),
                "aging_formula": PROVISIONAL_V1_AGING_FORMULA,
                "scheduler_evaluated_at": evaluated_at,
            }
            transition_id = _insert_state_transition(
                connection,
                entity_type="queue_item",
                queue_item_id=row["queue_item_id"],
                from_state="queued",
                to_state="claimed",
                transition_type=PROVISIONAL_QUEUE_CLAIM_FOR_DISPATCH_TRANSITION_TYPE,
                created_at=evaluated_at,
                metadata=transition_metadata,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        payload = _load_dispatch_payload(connection, row["id"], resolved_db_path)
        if payload is None:
            raise SchedulerPersistenceError(
                code=SCHEDULER_STORAGE_ERROR,
                message=f"Claimed run row is missing after claim: {row['id']}",
                database_path=resolved_db_path,
            )
        transition = _build_transition_record(
            transition_id=transition_id,
            queue_item_id=row["queue_item_id"],
            from_state="queued",
            to_state="claimed",
            transition_type=PROVISIONAL_QUEUE_CLAIM_FOR_DISPATCH_TRANSITION_TYPE,
            reason_code=None,
            metadata=transition_metadata,
            created_at=evaluated_at,
        )
        return ClaimNextRunResult(
            dispatch_run=payload,
            transition=transition,
            evaluated_at=evaluated_at,
            effective_age_seconds=_round_effective_age(row["effective_age_seconds"]),
            priority_rank=_priority_rank_for_class(payload.queue_item.priority_class),
        )
    except SchedulerPersistenceError:
        raise
    except sqlite3.Error as exc:
        raise SchedulerPersistenceError(
            code=SCHEDULER_STORAGE_ERROR,
            message="SQLite scheduler claim failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def release_claimed_run(
    database_path: str | Path,
    *,
    run_id: str | None = None,
    queue_item_id: str | None = None,
    available_at: str | datetime | None = None,
    note: str | None = None,
) -> ClaimedRunMutationResult:
    return _requeue_claimed_run(
        database_path,
        operation="released",
        transition_type=PROVISIONAL_QUEUE_RELEASE_TRANSITION_TYPE,
        run_id=run_id,
        queue_item_id=queue_item_id,
        available_at=available_at,
        reason_code=None,
        note=note,
    )


def mark_claimed_run_dispatch_failed(
    database_path: str | Path,
    *,
    run_id: str | None = None,
    queue_item_id: str | None = None,
    reason_code: str,
    available_at: str | datetime | None = None,
    note: str | None = None,
) -> ClaimedRunMutationResult:
    normalized_reason_code = _require_non_empty_string("reason_code", reason_code, _resolve_database_path(database_path))
    return _requeue_claimed_run(
        database_path,
        operation="dispatch_failed",
        transition_type=PROVISIONAL_QUEUE_DISPATCH_FAILED_TRANSITION_TYPE,
        run_id=run_id,
        queue_item_id=queue_item_id,
        available_at=available_at,
        reason_code=normalized_reason_code,
        note=note,
    )


def _requeue_claimed_run(
    database_path: str | Path,
    *,
    operation: str,
    transition_type: str,
    run_id: str | None,
    queue_item_id: str | None,
    available_at: str | datetime | None,
    reason_code: str | None,
    note: str | None,
) -> ClaimedRunMutationResult:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_run_id, normalized_queue_item_id = _normalize_claim_target(
        resolved_db_path,
        run_id=run_id,
        queue_item_id=queue_item_id,
    )
    requeued_at = _utc_now()
    next_available_at = _normalize_timestamp("available_at", available_at, resolved_db_path, default_value=requeued_at)
    normalized_note = _normalize_optional_text(note)

    connection = _connect_scheduler_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "queue_items", "state_transitions"),
        )

        try:
            connection.execute("BEGIN IMMEDIATE")
            row = _select_claim_target_row(
                connection,
                run_id=normalized_run_id,
                queue_item_id=normalized_queue_item_id,
            )
            if row is None:
                raise SchedulerPersistenceError(
                    code=CLAIMED_RUN_NOT_FOUND,
                    message="Claim target is not present in SQLite",
                    database_path=resolved_db_path,
                    details=f"run_id={normalized_run_id} queue_item_id={normalized_queue_item_id}",
                )
            _ensure_claim_can_be_requeued(row, resolved_db_path)

            cursor = connection.execute(
                """
                UPDATE queue_items
                SET status = ?, available_at = ?, claimed_at = NULL, terminal_at = NULL
                WHERE id = ? AND status = ?
                """,
                ("queued", next_available_at, row["queue_item_id"], "claimed"),
            )
            if cursor.rowcount != 1:
                raise SchedulerPersistenceError(
                    code=SCHEDULER_STORAGE_ERROR,
                    message=f"Failed to requeue claimed queue item: {row['queue_item_id']}",
                    database_path=resolved_db_path,
                    details="The queue item changed state during requeue.",
                )

            transition_metadata: dict[str, object] = {
                "previous_available_at": row["available_at"],
                "requeued_available_at": next_available_at,
            }
            if normalized_note is not None:
                transition_metadata["note"] = normalized_note

            transition_id = _insert_state_transition(
                connection,
                entity_type="queue_item",
                queue_item_id=row["queue_item_id"],
                from_state="claimed",
                to_state="queued",
                transition_type=transition_type,
                created_at=requeued_at,
                reason_code=reason_code,
                metadata=transition_metadata,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        payload = _load_dispatch_payload(connection, row["id"], resolved_db_path)
        if payload is None:
            raise SchedulerPersistenceError(
                code=SCHEDULER_STORAGE_ERROR,
                message=f"Requeued run row is missing after {operation}: {row['id']}",
                database_path=resolved_db_path,
            )
        transition = _build_transition_record(
            transition_id=transition_id,
            queue_item_id=row["queue_item_id"],
            from_state="claimed",
            to_state="queued",
            transition_type=transition_type,
            reason_code=reason_code,
            metadata=transition_metadata,
            created_at=requeued_at,
        )
        return ClaimedRunMutationResult(
            operation=operation,
            dispatch_run=payload,
            transition=transition,
        )
    except SchedulerPersistenceError:
        raise
    except sqlite3.Error as exc:
        raise SchedulerPersistenceError(
            code=SCHEDULER_STORAGE_ERROR,
            message=f"SQLite scheduler {operation} failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _resolve_database_path(database_path: str | Path) -> Path:
    resolved_db_path = Path(database_path).expanduser().resolve()
    if not resolved_db_path.exists():
        raise SchedulerPersistenceError(
            code=SCHEDULER_STORAGE_ERROR,
            message=f"SQLite database does not exist: {resolved_db_path}",
            database_path=resolved_db_path,
            details="Run init-sqlite-v1 before using scheduler persistence utilities.",
        )
    if not resolved_db_path.is_file():
        raise SchedulerPersistenceError(
            code=SCHEDULER_STORAGE_ERROR,
            message=f"SQLite database path is not a file: {resolved_db_path}",
            database_path=resolved_db_path,
        )
    return resolved_db_path


def _connect_scheduler_db(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute("PRAGMA busy_timeout = 5000;")
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
        raise SchedulerPersistenceError(
            code=REQUIRED_TABLES_MISSING,
            message=f"SQLite database is missing required tables: {', '.join(missing_tables)}",
            database_path=database_path,
            details="Run init-sqlite-v1 before using scheduler persistence utilities.",
        )


def _select_next_runnable_row(connection: sqlite3.Connection, evaluated_at: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
          runs.id,
          runs.project_id,
          projects.project_key,
          projects.package_root,
          projects.created_at AS project_created_at,
          projects.updated_at AS project_updated_at,
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
          queue_items.terminal_at AS queue_terminal_at,
          ((julianday(?) - julianday(queue_items.available_at)) * 86400.0) AS effective_age_seconds
        FROM queue_items
        JOIN runs ON runs.id = queue_items.run_id
        JOIN projects ON projects.id = runs.project_id
        WHERE queue_items.status = 'queued'
          AND queue_items.available_at <= ?
          AND runs.status IN ('queued', 'running')
        ORDER BY
          CASE queue_items.priority_class
            WHEN 'system' THEN 0
            WHEN 'interactive' THEN 1
            WHEN 'background' THEN 2
            ELSE 99
          END,
          effective_age_seconds DESC,
          queue_items.enqueued_at ASC,
          queue_items.id ASC
        LIMIT 1
        """,
        (evaluated_at, evaluated_at),
    ).fetchone()


def _select_claim_target_row(
    connection: sqlite3.Connection,
    *,
    run_id: str | None,
    queue_item_id: str | None,
) -> sqlite3.Row | None:
    filters: list[str] = []
    params: list[object] = []

    if run_id is not None:
        filters.append("runs.id = ?")
        params.append(run_id)
    if queue_item_id is not None:
        filters.append("queue_items.id = ?")
        params.append(queue_item_id)

    where_sql = " AND ".join(filters)
    return connection.execute(
        f"""
        SELECT
          runs.id,
          runs.project_id,
          projects.project_key,
          projects.package_root,
          projects.created_at AS project_created_at,
          projects.updated_at AS project_updated_at,
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
        FROM queue_items
        JOIN runs ON runs.id = queue_items.run_id
        JOIN projects ON projects.id = runs.project_id
        WHERE {where_sql}
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()


def _load_dispatch_payload(
    connection: sqlite3.Connection,
    run_id: str,
    database_path: Path,
) -> DispatchRunPayload | None:
    row = connection.execute(
        """
        SELECT
          runs.id,
          runs.project_id,
          projects.project_key,
          projects.package_root,
          projects.created_at AS project_created_at,
          projects.updated_at AS project_updated_at,
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
        JOIN queue_items ON queue_items.run_id = runs.id
        WHERE runs.id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_dispatch_payload(connection, row, database_path=database_path)


def _row_to_dispatch_payload(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    database_path: Path | None = None,
) -> DispatchRunPayload:
    run = _row_to_run_summary(row)
    if run.queue_item is None:
        raise SchedulerPersistenceError(
            code=SCHEDULER_STORAGE_ERROR,
            message=f"Run is missing its queue item row: {run.id}",
            database_path=database_path or _database_path_from_connection(connection),
        )
    project = RegisteredProject(
        id=row["project_id"],
        project_key=row["project_key"],
        package_root=Path(row["package_root"]),
        created_at=row["project_created_at"],
        updated_at=row["project_updated_at"],
    )
    flow_context = _load_flow_context(connection, run)
    return DispatchRunPayload(
        run=run,
        queue_item=run.queue_item,
        project=project,
        project_package_root=project.package_root,
        flow_context=flow_context,
    )


def _load_flow_context(connection: sqlite3.Connection, run: RunSummary) -> FlowContextSummary:
    rows = connection.execute(
        """
        SELECT id
        FROM runs
        WHERE flow_id = ?
        ORDER BY created_at, id
        """,
        (run.flow_id,),
    ).fetchall()
    run_ids = [row["id"] for row in rows]
    if not run_ids or run.id not in run_ids:
        raise SchedulerPersistenceError(
            code=SCHEDULER_STORAGE_ERROR,
            message=f"Flow context is missing the current run: {run.id}",
            database_path=_database_path_from_connection(connection),
            details=f"flow_id={run.flow_id}",
        )
    return FlowContextSummary(
        flow_id=run.flow_id,
        current_run_id=run.id,
        root_run_id=run_ids[0],
        cycle_no=run_ids.index(run.id) + 1,
        total_runs=len(run_ids),
        parent_run_id=run.parent_run_id,
        origin_type=run.origin_type,
        origin_run_id=run.origin_run_id,
        origin_step_run_id=run.origin_step_run_id,
    )


def _ensure_claim_can_be_requeued(row: sqlite3.Row, database_path: Path) -> None:
    if row["queue_status"] != "claimed":
        raise SchedulerPersistenceError(
            code=QUEUE_ITEM_NOT_CLAIMED,
            message=f"Queue item is not currently claimed: {row['queue_item_id']}",
            database_path=database_path,
            details=f"actual_status={row['queue_status']}",
        )
    if row["status"] not in {"queued", "running"}:
        raise SchedulerPersistenceError(
            code=CLAIMED_RUN_NOT_RELEASABLE,
            message=f"Run cannot be requeued after claim in status {row['status']}: {row['id']}",
            database_path=database_path,
            details="Scheduler claim/release primitives only support active runs that are still dispatchable.",
        )


def _normalize_claim_target(
    database_path: Path,
    *,
    run_id: str | None,
    queue_item_id: str | None,
) -> tuple[str | None, str | None]:
    if bool(run_id) == bool(queue_item_id):
        raise SchedulerPersistenceError(
            code=CLAIM_TARGET_REQUIRED,
            message="Provide exactly one of run_id or queue_item_id",
            database_path=database_path,
        )
    normalized_run_id = None
    normalized_queue_item_id = None
    if run_id is not None:
        normalized_run_id = _require_non_empty_string("run_id", run_id, database_path)
    if queue_item_id is not None:
        normalized_queue_item_id = _require_non_empty_string("queue_item_id", queue_item_id, database_path)
    return normalized_run_id, normalized_queue_item_id


def _build_transition_record(
    *,
    transition_id: str,
    queue_item_id: str,
    from_state: str,
    to_state: str,
    transition_type: str,
    reason_code: str | None,
    metadata: dict[str, object] | None,
    created_at: str,
) -> StateTransitionRecord:
    return StateTransitionRecord(
        id=transition_id,
        entity_type="queue_item",
        run_id=None,
        step_run_id=None,
        queue_item_id=queue_item_id,
        from_state=from_state,
        to_state=to_state,
        transition_type=transition_type,
        reason_code=reason_code,
        metadata_json=None if metadata is None else json.dumps(metadata, sort_keys=True),
        created_at=created_at,
    )


def _priority_rank_for_class(priority_class: str) -> int:
    return _PRIORITY_CLASS_RANK.get(priority_class, len(PRIORITY_CLASSES))


def _round_effective_age(value: object) -> float:
    if value is None:
        return 0.0
    return round(float(value), 6)


def _database_path_from_connection(connection: sqlite3.Connection) -> Path:
    return Path(connection.execute("PRAGMA database_list").fetchone()[2]).resolve()


def _normalize_timestamp(
    name: str,
    value: str | datetime | None,
    database_path: Path,
    *,
    default_value: str | None = None,
) -> str:
    if value is None:
        return default_value or _utc_now()

    if isinstance(value, datetime):
        normalized_dt = value
    elif isinstance(value, str):
        raw_value = value.strip()
        if not raw_value:
            raise SchedulerPersistenceError(
                code=INVALID_AVAILABLE_AT if name == "available_at" else SCHEDULER_STORAGE_ERROR,
                message=f"{name} must be a non-empty ISO-8601 timestamp",
                database_path=database_path,
            )
        if raw_value.lower() == "now":
            return _utc_now()
        iso_value = raw_value[:-1] + "+00:00" if raw_value.endswith("Z") else raw_value
        try:
            normalized_dt = datetime.fromisoformat(iso_value)
        except ValueError as exc:
            raise SchedulerPersistenceError(
                code=INVALID_AVAILABLE_AT if name == "available_at" else SCHEDULER_STORAGE_ERROR,
                message=f"{name} must be a valid ISO-8601 timestamp",
                database_path=database_path,
                details=str(exc),
            ) from exc
    else:
        raise SchedulerPersistenceError(
            code=INVALID_AVAILABLE_AT if name == "available_at" else SCHEDULER_STORAGE_ERROR,
            message=f"{name} must be a string or datetime value",
            database_path=database_path,
            details=f"actual_type={type(value).__name__}",
        )

    if normalized_dt.tzinfo is None:
        raise SchedulerPersistenceError(
            code=INVALID_AVAILABLE_AT if name == "available_at" else SCHEDULER_STORAGE_ERROR,
            message=f"{name} must include an explicit timezone offset",
            database_path=database_path,
        )
    return normalized_dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _require_non_empty_string(name: str, value: str, database_path: Path) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SchedulerPersistenceError(
            code=INVALID_REASON_CODE if name == "reason_code" else CLAIM_TARGET_REQUIRED,
            message=f"{name} must be a non-empty string",
            database_path=database_path,
        )
    return value.strip()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
