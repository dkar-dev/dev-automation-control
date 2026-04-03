from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from .id_generation import generate_opaque_id, generate_queue_item_id, generate_run_id
from .run_persistence import (
    RunDetails,
    RunSummary,
    _connect_run_db,
    _ensure_required_tables,
    _load_run_details,
    _require_non_empty_string,
    _resolve_database_path,
    _row_to_run_summary,
    _utc_now,
)
from .step_run_persistence import (
    _ensure_queue_item_present,
    _insert_state_transition,
    _load_run_context,
    _load_step_run_details,
    _load_step_run_summary,
)


REVIEWER_VERDICTS = ("approved", "changes_requested", "blocked")

PROVISIONAL_REVIEWER_FOLLOWUP_ORIGIN_TYPE = "reviewer_followup"
PROVISIONAL_REVIEWER_FOLLOWUP_PRIORITY_CLASS = "interactive"
PROVISIONAL_MAX_CYCLES = 3
PROVISIONAL_MAX_RUN_ATTEMPTS = 3
PROVISIONAL_MAX_WALL_CLOCK_TIME_SECONDS = 24 * 60 * 60

PROVISIONAL_REVIEWER_APPROVED_TRANSITION_TYPE = "reviewer_outcome_approved"
PROVISIONAL_REVIEWER_BLOCKED_TRANSITION_TYPE = "reviewer_outcome_blocked"
PROVISIONAL_REVIEWER_CHANGES_REQUESTED_TRANSITION_TYPE = "reviewer_outcome_changes_requested"
PROVISIONAL_REVIEWER_CHANGES_REQUESTED_STOPPED_TRANSITION_TYPE = "reviewer_outcome_changes_requested_stopped"
PROVISIONAL_REVIEWER_FOLLOWUP_CREATED_TRANSITION_TYPE = "reviewer_followup_created"
PROVISIONAL_REVIEWER_FOLLOWUP_ENQUEUED_TRANSITION_TYPE = "reviewer_followup_enqueued"

GUARDRAIL_MAX_CYCLES_EXCEEDED = "max_cycles_exceeded"
GUARDRAIL_MAX_RUN_ATTEMPTS_EXCEEDED = "max_run_attempts_exceeded"
GUARDRAIL_MAX_WALL_CLOCK_TIME_EXCEEDED = "max_wall_clock_time_exceeded"
INVALID_REVIEWER_VERDICT = "INVALID_REVIEWER_VERDICT"
REVIEWER_OUTCOME_STORAGE_ERROR = "REVIEWER_OUTCOME_STORAGE_ERROR"
REVIEWER_STEP_NOT_FOUND = "REVIEWER_STEP_NOT_FOUND"
REVIEWER_STEP_NOT_REVIEWER = "REVIEWER_STEP_NOT_REVIEWER"
REVIEWER_STEP_NOT_TERMINAL = "REVIEWER_STEP_NOT_TERMINAL"
RUN_ALREADY_TERMINAL = "RUN_ALREADY_TERMINAL"
RUN_HAS_ACTIVE_STEP_RUN = "RUN_HAS_ACTIVE_STEP_RUN"


@dataclass(frozen=True)
class FlowRunSummary:
    cycle_no: int
    run: RunSummary

    def to_dict(self) -> dict[str, object]:
        return {
            "cycle_no": self.cycle_no,
            "run": self.run.to_dict(),
        }


@dataclass(frozen=True)
class FlowStatusSummary:
    flow_id: str
    current_run_id: str
    current_cycle: int
    next_cycle: int
    total_runs: int
    max_cycles: int
    next_run_attempt_count: int
    max_run_attempts: int
    elapsed_wall_clock_seconds: int
    max_wall_clock_time_seconds: int
    continuation_allowed: bool
    stop_reason_code: str | None
    stop_reason: str | None
    created_follow_up_run_id: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "flow_id": self.flow_id,
            "current_run_id": self.current_run_id,
            "current_cycle": self.current_cycle,
            "next_cycle": self.next_cycle,
            "total_runs": self.total_runs,
            "max_cycles": self.max_cycles,
            "next_run_attempt_count": self.next_run_attempt_count,
            "max_run_attempts": self.max_run_attempts,
            "elapsed_wall_clock_seconds": self.elapsed_wall_clock_seconds,
            "max_wall_clock_time_seconds": self.max_wall_clock_time_seconds,
            "continuation_allowed": self.continuation_allowed,
            "stop_reason_code": self.stop_reason_code,
            "stop_reason": self.stop_reason,
            "created_follow_up_run_id": self.created_follow_up_run_id,
        }


@dataclass(frozen=True)
class ReviewerOutcomeResult:
    verdict: str
    reviewer_step_run: dict[str, object]
    current_run: RunDetails
    follow_up_run: RunDetails | None
    flow_summary: FlowStatusSummary
    flow_runs: tuple[FlowRunSummary, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "verdict": self.verdict,
            "reviewer_step_run": self.reviewer_step_run,
            "current_run": self.current_run.to_dict(),
            "follow_up_run": self.follow_up_run.to_dict() if self.follow_up_run is not None else None,
            "flow_summary": self.flow_summary.to_dict(),
            "flow_runs": [flow_run.to_dict() for flow_run in self.flow_runs],
        }


class ReviewerOutcomeError(Exception):
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


@dataclass(frozen=True)
class _GuardrailEvaluation:
    flow_id: str
    current_run_id: str
    current_cycle: int
    next_cycle: int
    total_runs: int
    next_run_attempt_count: int
    elapsed_wall_clock_seconds: int
    continuation_allowed: bool
    stop_reason_code: str | None
    stop_reason: str | None


def complete_reviewer_outcome(
    database_path: str | Path,
    reviewer_step_run_id: str,
    verdict: str,
    summary_text: str | None = None,
) -> ReviewerOutcomeResult:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_step_run_id = _require_non_empty_string("reviewer_step_run_id", reviewer_step_run_id, resolved_db_path)
    normalized_verdict = _validate_reviewer_verdict(verdict, resolved_db_path)
    normalized_summary = _normalize_optional_text(summary_text)

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(
            connection,
            resolved_db_path,
            ("projects", "runs", "step_runs", "queue_items", "state_transitions", "run_snapshots"),
        )

        reviewer_step = _load_step_run_summary(connection, normalized_step_run_id)
        if reviewer_step is None:
            raise ReviewerOutcomeError(
                code=REVIEWER_STEP_NOT_FOUND,
                message=f"reviewer step_run is not present in SQLite: {normalized_step_run_id}",
                database_path=resolved_db_path,
            )
        if reviewer_step.step_key != "reviewer":
            raise ReviewerOutcomeError(
                code=REVIEWER_STEP_NOT_REVIEWER,
                message=f"step_run is not a reviewer step: {normalized_step_run_id}",
                database_path=resolved_db_path,
                details=f"actual_step_key={reviewer_step.step_key}",
            )
        if reviewer_step.status not in {"succeeded", "failed", "timed_out", "cancelled"}:
            raise ReviewerOutcomeError(
                code=REVIEWER_STEP_NOT_TERMINAL,
                message=f"reviewer step_run must be terminal before outcome completion: {normalized_step_run_id}",
                database_path=resolved_db_path,
                details=f"actual_status={reviewer_step.status}",
            )

        run_row = _load_run_context(connection, reviewer_step.run_id)
        if run_row is None:
            raise ReviewerOutcomeError(
                code=REVIEWER_OUTCOME_STORAGE_ERROR,
                message=f"run row is missing for reviewer step_run: {reviewer_step.run_id}",
                database_path=resolved_db_path,
            )
        _ensure_queue_item_present(run_row, resolved_db_path)
        _ensure_run_not_terminal(run_row, resolved_db_path)
        _ensure_no_active_step_runs(connection, reviewer_step.run_id, resolved_db_path)

        now = _utc_now()
        flow_runs_before = _list_flow_runs_in_connection(connection, reviewer_step.flow_id)
        if not flow_runs_before:
            raise ReviewerOutcomeError(
                code=REVIEWER_OUTCOME_STORAGE_ERROR,
                message=f"flow listing is empty for reviewer step_run flow: {reviewer_step.flow_id}",
                database_path=resolved_db_path,
            )
        guardrails = _evaluate_follow_up_guardrails(flow_runs_before, reviewer_step.run_id, now)

        follow_up_run_id: str | None = None
        run_transition_id: str | None = None
        follow_up_run_transition_id: str | None = None
        outcome_stop_reason_code: str | None = None
        outcome_stop_reason: str | None = None

        try:
            connection.execute("BEGIN")

            if normalized_verdict == "approved":
                run_transition_id = _transition_run_to_terminal(
                    connection,
                    run_id=reviewer_step.run_id,
                    from_state=run_row["status"],
                    to_state="completed",
                    transition_type=PROVISIONAL_REVIEWER_APPROVED_TRANSITION_TYPE,
                    created_at=now,
                    reason_code="approved",
                    metadata=_build_reviewer_metadata(
                        reviewer_step_id=reviewer_step.id,
                        reviewer_step_status=reviewer_step.status,
                        verdict=normalized_verdict,
                        summary_text=normalized_summary,
                    ),
                )
                _transition_queue_item_to_terminal(
                    connection,
                    queue_item_id=run_row["queue_item_id"],
                    from_state=run_row["queue_status"],
                    to_state="completed",
                    transition_type=PROVISIONAL_REVIEWER_APPROVED_TRANSITION_TYPE,
                    created_at=now,
                    reason_code="approved",
                    metadata=_build_reviewer_metadata(
                        reviewer_step_id=reviewer_step.id,
                        reviewer_step_status=reviewer_step.status,
                        verdict=normalized_verdict,
                        summary_text=normalized_summary,
                    ),
                )
            elif normalized_verdict == "blocked":
                outcome_stop_reason_code = "blocked"
                outcome_stop_reason = normalized_summary or "reviewer blocked"
                run_transition_id = _transition_run_to_terminal(
                    connection,
                    run_id=reviewer_step.run_id,
                    from_state=run_row["status"],
                    to_state="stopped",
                    transition_type=PROVISIONAL_REVIEWER_BLOCKED_TRANSITION_TYPE,
                    created_at=now,
                    reason_code="blocked",
                    metadata=_build_reviewer_metadata(
                        reviewer_step_id=reviewer_step.id,
                        reviewer_step_status=reviewer_step.status,
                        verdict=normalized_verdict,
                        summary_text=normalized_summary,
                    ),
                )
                _transition_queue_item_to_terminal(
                    connection,
                    queue_item_id=run_row["queue_item_id"],
                    from_state=run_row["queue_status"],
                    to_state="cancelled",
                    transition_type=PROVISIONAL_REVIEWER_BLOCKED_TRANSITION_TYPE,
                    created_at=now,
                    reason_code="blocked",
                    metadata=_build_reviewer_metadata(
                        reviewer_step_id=reviewer_step.id,
                        reviewer_step_status=reviewer_step.status,
                        verdict=normalized_verdict,
                        summary_text=normalized_summary,
                    ),
                )
            else:
                if guardrails.continuation_allowed:
                    follow_up_run_id = generate_run_id()
                    follow_up_run_transition_id = _handle_changes_requested_followup(
                        connection,
                        reviewer_step=reviewer_step,
                        run_row=run_row,
                        summary_text=normalized_summary,
                        created_at=now,
                        follow_up_run_id=follow_up_run_id,
                    )
                    run_transition_id = _transition_run_to_terminal(
                        connection,
                        run_id=reviewer_step.run_id,
                        from_state=run_row["status"],
                        to_state="completed",
                        transition_type=PROVISIONAL_REVIEWER_CHANGES_REQUESTED_TRANSITION_TYPE,
                        created_at=now,
                        reason_code="changes_requested",
                        metadata=_build_reviewer_metadata(
                            reviewer_step_id=reviewer_step.id,
                            reviewer_step_status=reviewer_step.status,
                            verdict=normalized_verdict,
                            summary_text=normalized_summary,
                            follow_up_run_id=follow_up_run_id,
                        ),
                    )
                    _transition_queue_item_to_terminal(
                        connection,
                        queue_item_id=run_row["queue_item_id"],
                        from_state=run_row["queue_status"],
                        to_state="completed",
                        transition_type=PROVISIONAL_REVIEWER_CHANGES_REQUESTED_TRANSITION_TYPE,
                        created_at=now,
                        reason_code="changes_requested",
                        metadata=_build_reviewer_metadata(
                            reviewer_step_id=reviewer_step.id,
                            reviewer_step_status=reviewer_step.status,
                            verdict=normalized_verdict,
                            summary_text=normalized_summary,
                            follow_up_run_id=follow_up_run_id,
                        ),
                    )
                else:
                    outcome_stop_reason_code = guardrails.stop_reason_code
                    outcome_stop_reason = guardrails.stop_reason
                    run_transition_id = _transition_run_to_terminal(
                        connection,
                        run_id=reviewer_step.run_id,
                        from_state=run_row["status"],
                        to_state="stopped",
                        transition_type=PROVISIONAL_REVIEWER_CHANGES_REQUESTED_STOPPED_TRANSITION_TYPE,
                        created_at=now,
                        reason_code=guardrails.stop_reason_code,
                        metadata=_build_reviewer_metadata(
                            reviewer_step_id=reviewer_step.id,
                            reviewer_step_status=reviewer_step.status,
                            verdict=normalized_verdict,
                            summary_text=normalized_summary,
                            stop_reason_code=guardrails.stop_reason_code,
                            stop_reason=guardrails.stop_reason,
                        ),
                    )
                    _transition_queue_item_to_terminal(
                        connection,
                        queue_item_id=run_row["queue_item_id"],
                        from_state=run_row["queue_status"],
                        to_state="cancelled",
                        transition_type=PROVISIONAL_REVIEWER_CHANGES_REQUESTED_STOPPED_TRANSITION_TYPE,
                        created_at=now,
                        reason_code=guardrails.stop_reason_code,
                        metadata=_build_reviewer_metadata(
                            reviewer_step_id=reviewer_step.id,
                            reviewer_step_status=reviewer_step.status,
                            verdict=normalized_verdict,
                            summary_text=normalized_summary,
                            stop_reason_code=guardrails.stop_reason_code,
                            stop_reason=guardrails.stop_reason,
                        ),
                    )

            if run_transition_id is None:
                raise ReviewerOutcomeError(
                    code=REVIEWER_OUTCOME_STORAGE_ERROR,
                    message="reviewer outcome did not create a terminal run transition",
                    database_path=resolved_db_path,
                )

            _insert_run_snapshot(
                connection,
                snapshot_scope="run",
                project_id=run_row["project_id"],
                flow_id=run_row["flow_id"],
                run_id=reviewer_step.run_id,
                state_transition_id=run_transition_id,
                created_at=now,
                snapshot_payload=_build_run_snapshot_payload(
                    reviewer_step=reviewer_step,
                    verdict=normalized_verdict,
                    summary_text=normalized_summary,
                    current_run_id=reviewer_step.run_id,
                    follow_up_run_id=follow_up_run_id,
                    guardrails=guardrails,
                    stop_reason_code=outcome_stop_reason_code,
                    stop_reason=outcome_stop_reason,
                ),
            )
            _insert_run_snapshot(
                connection,
                snapshot_scope="flow",
                project_id=run_row["project_id"],
                flow_id=run_row["flow_id"],
                run_id=None,
                state_transition_id=follow_up_run_transition_id or run_transition_id,
                created_at=now,
                snapshot_payload=_build_flow_snapshot_payload(
                    reviewer_step=reviewer_step,
                    verdict=normalized_verdict,
                    summary_text=normalized_summary,
                    current_run_id=reviewer_step.run_id,
                    follow_up_run_id=follow_up_run_id,
                    guardrails=guardrails,
                    stop_reason_code=outcome_stop_reason_code,
                    stop_reason=outcome_stop_reason,
                ),
            )

            connection.commit()
        except Exception:
            connection.rollback()
            raise

        current_run = _load_run_details(connection, reviewer_step.run_id)
        if current_run is None:
            raise ReviewerOutcomeError(
                code=REVIEWER_OUTCOME_STORAGE_ERROR,
                message=f"current run row is missing after reviewer outcome completion: {reviewer_step.run_id}",
                database_path=resolved_db_path,
            )
        follow_up_run = None
        if follow_up_run_id is not None:
            follow_up_run = _load_run_details(connection, follow_up_run_id)
            if follow_up_run is None:
                raise ReviewerOutcomeError(
                    code=REVIEWER_OUTCOME_STORAGE_ERROR,
                    message=f"follow-up run row is missing after create: {follow_up_run_id}",
                    database_path=resolved_db_path,
                )

        flow_runs_after = _list_flow_runs_in_connection(connection, reviewer_step.flow_id)
        current_cycle = _find_flow_cycle(flow_runs_after, reviewer_step.run_id, resolved_db_path)
        return ReviewerOutcomeResult(
            verdict=normalized_verdict,
            reviewer_step_run=_load_step_run_details(connection, reviewer_step.id).to_dict(),
            current_run=current_run,
            follow_up_run=follow_up_run,
            flow_summary=FlowStatusSummary(
                flow_id=reviewer_step.flow_id,
                current_run_id=reviewer_step.run_id,
                current_cycle=current_cycle,
                next_cycle=current_cycle + 1,
                total_runs=len(flow_runs_after),
                max_cycles=PROVISIONAL_MAX_CYCLES,
                next_run_attempt_count=len(flow_runs_after) + 1,
                max_run_attempts=PROVISIONAL_MAX_RUN_ATTEMPTS,
                elapsed_wall_clock_seconds=guardrails.elapsed_wall_clock_seconds,
                max_wall_clock_time_seconds=PROVISIONAL_MAX_WALL_CLOCK_TIME_SECONDS,
                continuation_allowed=follow_up_run is not None,
                stop_reason_code=None if follow_up_run is not None else outcome_stop_reason_code,
                stop_reason=None if follow_up_run is not None else outcome_stop_reason,
                created_follow_up_run_id=follow_up_run.run.id if follow_up_run is not None else None,
            ),
            flow_runs=tuple(flow_runs_after),
        )
    except sqlite3.Error as exc:
        raise ReviewerOutcomeError(
            code=REVIEWER_OUTCOME_STORAGE_ERROR,
            message="SQLite reviewer outcome persistence failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def list_flow_runs(
    database_path: str | Path,
    flow_id: str,
    *,
    limit: int = 100,
) -> list[FlowRunSummary]:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_flow_id = _require_non_empty_string("flow_id", flow_id, resolved_db_path)
    if limit <= 0:
        raise ReviewerOutcomeError(
            code=REVIEWER_OUTCOME_STORAGE_ERROR,
            message="limit must be greater than zero",
            database_path=resolved_db_path,
        )

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("projects", "runs", "queue_items"))
        return _list_flow_runs_in_connection(connection, normalized_flow_id, limit=limit)
    except sqlite3.Error as exc:
        raise ReviewerOutcomeError(
            code=REVIEWER_OUTCOME_STORAGE_ERROR,
            message="SQLite flow run listing failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _validate_reviewer_verdict(verdict: str, database_path: Path) -> str:
    normalized = _require_non_empty_string("verdict", verdict, database_path)
    if normalized not in REVIEWER_VERDICTS:
        raise ReviewerOutcomeError(
            code=INVALID_REVIEWER_VERDICT,
            message=f"verdict must be one of: {', '.join(REVIEWER_VERDICTS)}",
            database_path=database_path,
            details=f"actual={normalized}",
        )
    return normalized


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _ensure_run_not_terminal(run_row: sqlite3.Row, database_path: Path) -> None:
    if run_row["status"] in {"completed", "failed", "stopped", "cancelled"}:
        raise ReviewerOutcomeError(
            code=RUN_ALREADY_TERMINAL,
            message=f"run is already terminal: {run_row['id']}",
            database_path=database_path,
            details=f"actual_status={run_row['status']}",
        )
    if run_row["queue_status"] in {"completed", "cancelled"}:
        raise ReviewerOutcomeError(
            code=RUN_ALREADY_TERMINAL,
            message=f"queue_item is already terminal for run: {run_row['id']}",
            database_path=database_path,
            details=f"actual_queue_status={run_row['queue_status']}",
        )


def _ensure_no_active_step_runs(connection: sqlite3.Connection, run_id: str, database_path: Path) -> None:
    active_row = connection.execute(
        """
        SELECT id, step_key
        FROM step_runs
        WHERE run_id = ? AND status = 'running'
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    if active_row is not None:
        raise ReviewerOutcomeError(
            code=RUN_HAS_ACTIVE_STEP_RUN,
            message=f"run still has an active step_run: {active_row['id']}",
            database_path=database_path,
            details=f"step_key={active_row['step_key']}",
        )


def _parse_utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _evaluate_follow_up_guardrails(
    flow_runs: list[FlowRunSummary],
    current_run_id: str,
    now_timestamp: str,
) -> _GuardrailEvaluation:
    current_cycle = _find_flow_cycle(flow_runs, current_run_id, Path("<guardrail-check>"))
    total_runs = len(flow_runs)
    next_cycle = current_cycle + 1
    next_run_attempt_count = total_runs + 1
    root_created_at = _parse_utc_timestamp(flow_runs[0].run.created_at)
    now_dt = _parse_utc_timestamp(now_timestamp)
    elapsed_seconds = max(0, int((now_dt - root_created_at).total_seconds()))

    if next_cycle > PROVISIONAL_MAX_CYCLES:
        return _GuardrailEvaluation(
            flow_id=flow_runs[0].run.flow_id,
            current_run_id=current_run_id,
            current_cycle=current_cycle,
            next_cycle=next_cycle,
            total_runs=total_runs,
            next_run_attempt_count=next_run_attempt_count,
            elapsed_wall_clock_seconds=elapsed_seconds,
            continuation_allowed=False,
            stop_reason_code=GUARDRAIL_MAX_CYCLES_EXCEEDED,
            stop_reason=f"next cycle {next_cycle} would exceed max_cycles={PROVISIONAL_MAX_CYCLES}",
        )
    if next_run_attempt_count > PROVISIONAL_MAX_RUN_ATTEMPTS:
        return _GuardrailEvaluation(
            flow_id=flow_runs[0].run.flow_id,
            current_run_id=current_run_id,
            current_cycle=current_cycle,
            next_cycle=next_cycle,
            total_runs=total_runs,
            next_run_attempt_count=next_run_attempt_count,
            elapsed_wall_clock_seconds=elapsed_seconds,
            continuation_allowed=False,
            stop_reason_code=GUARDRAIL_MAX_RUN_ATTEMPTS_EXCEEDED,
            stop_reason=(
                f"next run attempt count {next_run_attempt_count} "
                f"would exceed max_run_attempts={PROVISIONAL_MAX_RUN_ATTEMPTS}"
            ),
        )
    if elapsed_seconds > PROVISIONAL_MAX_WALL_CLOCK_TIME_SECONDS:
        return _GuardrailEvaluation(
            flow_id=flow_runs[0].run.flow_id,
            current_run_id=current_run_id,
            current_cycle=current_cycle,
            next_cycle=next_cycle,
            total_runs=total_runs,
            next_run_attempt_count=next_run_attempt_count,
            elapsed_wall_clock_seconds=elapsed_seconds,
            continuation_allowed=False,
            stop_reason_code=GUARDRAIL_MAX_WALL_CLOCK_TIME_EXCEEDED,
            stop_reason=(
                f"elapsed wall clock {elapsed_seconds}s "
                f"would exceed max_wall_clock_time={PROVISIONAL_MAX_WALL_CLOCK_TIME_SECONDS}s"
            ),
        )
    return _GuardrailEvaluation(
        flow_id=flow_runs[0].run.flow_id,
        current_run_id=current_run_id,
        current_cycle=current_cycle,
        next_cycle=next_cycle,
        total_runs=total_runs,
        next_run_attempt_count=next_run_attempt_count,
        elapsed_wall_clock_seconds=elapsed_seconds,
        continuation_allowed=True,
        stop_reason_code=None,
        stop_reason=None,
    )


def _find_flow_cycle(flow_runs: list[FlowRunSummary], run_id: str, database_path: Path) -> int:
    for flow_run in flow_runs:
        if flow_run.run.id == run_id:
            return flow_run.cycle_no
    raise ReviewerOutcomeError(
        code=REVIEWER_OUTCOME_STORAGE_ERROR,
        message=f"run is not present in its own flow listing: {run_id}",
        database_path=database_path,
    )


def _transition_run_to_terminal(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    from_state: str,
    to_state: str,
    transition_type: str,
    created_at: str,
    reason_code: str | None = None,
    metadata: dict[str, object] | None = None,
) -> str:
    connection.execute(
        """
        UPDATE runs
        SET status = ?, updated_at = ?, started_at = COALESCE(started_at, ?), terminal_at = ?
        WHERE id = ?
        """,
        (to_state, created_at, created_at, created_at, run_id),
    )
    return _insert_state_transition(
        connection,
        entity_type="run",
        run_id=run_id,
        from_state=from_state,
        to_state=to_state,
        transition_type=transition_type,
        reason_code=reason_code,
        metadata=metadata,
        created_at=created_at,
    )


def _transition_queue_item_to_terminal(
    connection: sqlite3.Connection,
    *,
    queue_item_id: str,
    from_state: str,
    to_state: str,
    transition_type: str,
    created_at: str,
    reason_code: str | None = None,
    metadata: dict[str, object] | None = None,
) -> str:
    connection.execute(
        """
        UPDATE queue_items
        SET status = ?, terminal_at = ?
        WHERE id = ?
        """,
        (to_state, created_at, queue_item_id),
    )
    return _insert_state_transition(
        connection,
        entity_type="queue_item",
        queue_item_id=queue_item_id,
        from_state=from_state,
        to_state=to_state,
        transition_type=transition_type,
        reason_code=reason_code,
        metadata=metadata,
        created_at=created_at,
    )


def _handle_changes_requested_followup(
    connection: sqlite3.Connection,
    *,
    reviewer_step,
    run_row: sqlite3.Row,
    summary_text: str | None,
    created_at: str,
    follow_up_run_id: str,
) -> str:
    queue_item_id = generate_queue_item_id()
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, NULL, NULL)
        """,
        (
            follow_up_run_id,
            run_row["project_id"],
            run_row["project_profile"],
            run_row["workflow_id"],
            run_row["milestone"],
            run_row["flow_id"],
            run_row["id"],
            PROVISIONAL_REVIEWER_FOLLOWUP_ORIGIN_TYPE,
            run_row["id"],
            reviewer_step.id,
            created_at,
            created_at,
            created_at,
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
        VALUES (?, ?, ?, 'queued', ?, ?, NULL, NULL)
        """,
        (
            queue_item_id,
            follow_up_run_id,
            PROVISIONAL_REVIEWER_FOLLOWUP_PRIORITY_CLASS,
            created_at,
            created_at,
        ),
    )
    run_transition_id = _insert_state_transition(
        connection,
        entity_type="run",
        run_id=follow_up_run_id,
        from_state=None,
        to_state="queued",
        transition_type=PROVISIONAL_REVIEWER_FOLLOWUP_CREATED_TRANSITION_TYPE,
        reason_code="changes_requested",
        metadata={
            "origin_type": PROVISIONAL_REVIEWER_FOLLOWUP_ORIGIN_TYPE,
            "origin_run_id": run_row["id"],
            "origin_step_run_id": reviewer_step.id,
            "reviewer_verdict": "changes_requested",
            "summary_text": summary_text,
        },
        created_at=created_at,
    )
    _insert_state_transition(
        connection,
        entity_type="queue_item",
        queue_item_id=queue_item_id,
        from_state=None,
        to_state="queued",
        transition_type=PROVISIONAL_REVIEWER_FOLLOWUP_ENQUEUED_TRANSITION_TYPE,
        reason_code="changes_requested",
        metadata={
            "priority_class": PROVISIONAL_REVIEWER_FOLLOWUP_PRIORITY_CLASS,
            "origin_run_id": run_row["id"],
            "origin_step_run_id": reviewer_step.id,
        },
        created_at=created_at,
    )
    return run_transition_id


def _insert_run_snapshot(
    connection: sqlite3.Connection,
    *,
    snapshot_scope: str,
    project_id: str,
    flow_id: str,
    run_id: str | None,
    state_transition_id: str,
    created_at: str,
    snapshot_payload: dict[str, object],
) -> str:
    snapshot_id = generate_opaque_id()
    connection.execute(
        """
        INSERT INTO run_snapshots (
          id,
          snapshot_scope,
          project_id,
          flow_id,
          run_id,
          state_transition_id,
          snapshot_json,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            snapshot_scope,
            project_id,
            flow_id,
            run_id,
            state_transition_id,
            json.dumps(snapshot_payload, sort_keys=True),
            created_at,
        ),
    )
    return snapshot_id


def _build_reviewer_metadata(
    *,
    reviewer_step_id: str,
    reviewer_step_status: str,
    verdict: str,
    summary_text: str | None,
    follow_up_run_id: str | None = None,
    stop_reason_code: str | None = None,
    stop_reason: str | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "reviewer_step_run_id": reviewer_step_id,
        "reviewer_step_status": reviewer_step_status,
        "verdict": verdict,
    }
    if summary_text is not None:
        metadata["summary_text"] = summary_text
    if follow_up_run_id is not None:
        metadata["follow_up_run_id"] = follow_up_run_id
    if stop_reason_code is not None:
        metadata["stop_reason_code"] = stop_reason_code
    if stop_reason is not None:
        metadata["stop_reason"] = stop_reason
    return metadata


def _build_run_snapshot_payload(
    *,
    reviewer_step,
    verdict: str,
    summary_text: str | None,
    current_run_id: str,
    follow_up_run_id: str | None,
    guardrails: _GuardrailEvaluation,
    stop_reason_code: str | None,
    stop_reason: str | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "reviewer_outcome",
        "reviewer_step_run_id": reviewer_step.id,
        "reviewer_step_status": reviewer_step.status,
        "verdict": verdict,
        "current_run_id": current_run_id,
        "current_cycle": guardrails.current_cycle,
        "next_cycle": guardrails.next_cycle,
        "max_cycles": PROVISIONAL_MAX_CYCLES,
        "next_run_attempt_count": guardrails.next_run_attempt_count,
        "max_run_attempts": PROVISIONAL_MAX_RUN_ATTEMPTS,
        "elapsed_wall_clock_seconds": guardrails.elapsed_wall_clock_seconds,
        "max_wall_clock_time_seconds": PROVISIONAL_MAX_WALL_CLOCK_TIME_SECONDS,
        "created_follow_up_run_id": follow_up_run_id,
        "continuation_allowed": follow_up_run_id is not None,
        "stop_reason_code": None if follow_up_run_id is not None else stop_reason_code,
        "stop_reason": None if follow_up_run_id is not None else stop_reason,
    }
    if summary_text is not None:
        payload["summary_text"] = summary_text
    return payload


def _build_flow_snapshot_payload(
    *,
    reviewer_step,
    verdict: str,
    summary_text: str | None,
    current_run_id: str,
    follow_up_run_id: str | None,
    guardrails: _GuardrailEvaluation,
    stop_reason_code: str | None,
    stop_reason: str | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "reviewer_flow_outcome",
        "flow_id": guardrails.flow_id,
        "reviewer_step_run_id": reviewer_step.id,
        "verdict": verdict,
        "current_run_id": current_run_id,
        "current_cycle": guardrails.current_cycle,
        "next_cycle": guardrails.next_cycle,
        "created_follow_up_run_id": follow_up_run_id,
        "continuation_allowed": follow_up_run_id is not None,
        "stop_reason_code": None if follow_up_run_id is not None else stop_reason_code,
        "stop_reason": None if follow_up_run_id is not None else stop_reason,
        "elapsed_wall_clock_seconds": guardrails.elapsed_wall_clock_seconds,
        "max_wall_clock_time_seconds": PROVISIONAL_MAX_WALL_CLOCK_TIME_SECONDS,
    }
    if summary_text is not None:
        payload["summary_text"] = summary_text
    return payload


def _list_flow_runs_in_connection(
    connection: sqlite3.Connection,
    flow_id: str,
    *,
    limit: int = 100,
) -> list[FlowRunSummary]:
    rows = connection.execute(
        """
        WITH RECURSIVE flow_chain (cycle_no, run_id) AS (
          SELECT 1 AS cycle_no, runs.id AS run_id
          FROM runs
          WHERE runs.flow_id = ? AND runs.parent_run_id IS NULL

          UNION ALL

          SELECT flow_chain.cycle_no + 1, child.id
          FROM runs AS child
          JOIN flow_chain ON child.parent_run_id = flow_chain.run_id
          WHERE child.flow_id = ?
        )
        SELECT
          flow_chain.cycle_no,
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
        FROM flow_chain
        JOIN runs ON runs.id = flow_chain.run_id
        JOIN projects ON projects.id = runs.project_id
        LEFT JOIN queue_items ON queue_items.run_id = runs.id
        ORDER BY flow_chain.cycle_no, runs.created_at, runs.id
        LIMIT ?
        """,
        (flow_id, flow_id, limit),
    ).fetchall()
    return [
        FlowRunSummary(
            cycle_no=row["cycle_no"],
            run=_row_to_run_summary(row),
        )
        for row in rows
    ]
