from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from .dispatch_adapter import DispatchAdapterError, DispatchResult, determine_dispatch_role, dispatch_claimed_run
from .id_generation import generate_opaque_id
from .reviewer_outcome_persistence import ReviewerOutcomeError
from .reviewer_result_ingestion import ReviewerResultIngestionError, ReviewerResultIngestionResult, ingest_reviewer_result
from .run_persistence import RunDetails, RunPersistenceError, _resolve_database_path, get_run
from .scheduler_persistence import ClaimNextRunResult, SchedulerPersistenceError, claim_next_run


WORKER_TICK_STATUSES = ("idle", "progressed", "dispatch_failed", "ingestion_failed", "stopped")
WORKER_ENDED_REASONS = (
    "idle",
    "dispatch_failed",
    "ingestion_failed",
    "max_ticks_reached",
    "max_claims_reached",
    "max_flows_reached",
    "max_wall_clock_time_reached",
)

INVALID_WORKER_LIMIT = "INVALID_WORKER_LIMIT"
WORKER_CLAIM_FAILED = "WORKER_CLAIM_FAILED"
WORKER_DISPATCH_FAILED = "WORKER_DISPATCH_FAILED"
WORKER_FINAL_STATE_LOAD_FAILED = "WORKER_FINAL_STATE_LOAD_FAILED"
WORKER_REVIEWER_DISPATCH_UNEXPECTED = "WORKER_REVIEWER_DISPATCH_UNEXPECTED"
WORKER_ROLE_DETERMINATION_FAILED = "WORKER_ROLE_DETERMINATION_FAILED"


@dataclass(frozen=True)
class WorkerSummaryPaths:
    root_directory: Path | None
    json_path: Path | None
    markdown_path: Path | None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "root_directory": str(self.root_directory) if self.root_directory is not None else None,
            "json_path": str(self.json_path) if self.json_path is not None else None,
            "markdown_path": str(self.markdown_path) if self.markdown_path is not None else None,
        }


@dataclass(frozen=True)
class WorkerOperationError:
    stage: str
    code: str
    message: str
    details: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "stage": self.stage,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


@dataclass(frozen=True)
class WorkerRuntimeConfig:
    runtime_context: Mapping[str, object] | None = None
    artifact_root: Path | None = None
    worker_log_root: Path | None = None
    workspace_root: Path | None = None
    project_repo_path: Path | None = None
    executor_worktree_path: Path | None = None
    reviewer_worktree_path: Path | None = None
    instructions_repo_path: Path | None = None
    branch_base: str | None = None
    instruction_profile: str | None = None
    instruction_overlays: tuple[str, ...] | None = None
    task_text: str | None = None
    mode: str | None = None
    source: str | None = None
    thread_label: str | None = None
    constraints: tuple[str, ...] | None = None
    expected_output: tuple[str, ...] | None = None
    legacy_control_dir: Path | None = None
    executor_runner_path: Path | None = None
    reviewer_runner_path: Path | None = None
    claim_now: str | None = None

    def dispatch_kwargs(self) -> dict[str, object]:
        return {
            "runtime_context": dict(self.runtime_context) if self.runtime_context is not None else None,
            "artifact_root": self.artifact_root,
            "workspace_root": self.workspace_root,
            "project_repo_path": self.project_repo_path,
            "executor_worktree_path": self.executor_worktree_path,
            "reviewer_worktree_path": self.reviewer_worktree_path,
            "instructions_repo_path": self.instructions_repo_path,
            "branch_base": self.branch_base,
            "instruction_profile": self.instruction_profile,
            "instruction_overlays": list(self.instruction_overlays) if self.instruction_overlays is not None else None,
            "task_text": self.task_text,
            "mode": self.mode,
            "source": self.source,
            "thread_label": self.thread_label,
            "constraints": list(self.constraints) if self.constraints is not None else None,
            "expected_output": list(self.expected_output) if self.expected_output is not None else None,
            "legacy_control_dir": self.legacy_control_dir,
            "executor_runner_path": self.executor_runner_path,
            "reviewer_runner_path": self.reviewer_runner_path,
        }

    def effective_mode(self) -> str:
        if self.mode is not None and self.mode.strip():
            return self.mode.strip()
        if self.runtime_context is not None:
            raw_mode = self.runtime_context.get("mode")
            if raw_mode is not None and str(raw_mode).strip():
                return str(raw_mode).strip()
        return "executor+reviewer"

    def summary_root(self) -> Path | None:
        if self.worker_log_root is not None:
            return self.worker_log_root
        if self.artifact_root is not None:
            return self.artifact_root / "_worker"
        return None


@dataclass(frozen=True)
class WorkerTickResult:
    status: str
    claimed_run_id: str | None
    claimed_queue_item_id: str | None
    claimed_flow_id: str | None
    claim: ClaimNextRunResult | None
    initial_role: str | None
    roles_dispatched: tuple[str, ...]
    reviewer_ingestion_happened: bool
    follow_up_run_created: bool
    follow_up_run_id: str | None
    final_run_status: str | None
    final_run: RunDetails | None
    dispatch_results: tuple[DispatchResult, ...]
    reviewer_ingestion: ReviewerResultIngestionResult | None
    queue_requeued: bool
    error: WorkerOperationError | None
    warnings: tuple[str, ...]
    summary_paths: WorkerSummaryPaths

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "claimed_run_id": self.claimed_run_id,
            "claimed_queue_item_id": self.claimed_queue_item_id,
            "claimed_flow_id": self.claimed_flow_id,
            "claim": self.claim.to_dict() if self.claim is not None else None,
            "initial_role": self.initial_role,
            "roles_dispatched": list(self.roles_dispatched),
            "reviewer_ingestion_happened": self.reviewer_ingestion_happened,
            "follow_up_run_created": self.follow_up_run_created,
            "follow_up_run_id": self.follow_up_run_id,
            "final_run_status": self.final_run_status,
            "final_run": self.final_run.to_dict() if self.final_run is not None else None,
            "dispatch_results": [result.to_dict() for result in self.dispatch_results],
            "reviewer_ingestion": self.reviewer_ingestion.to_dict() if self.reviewer_ingestion is not None else None,
            "queue_requeued": self.queue_requeued,
            "error": self.error.to_dict() if self.error is not None else None,
            "warnings": list(self.warnings),
            "summary_paths": self.summary_paths.to_dict(),
        }


@dataclass(frozen=True)
class WorkerLoopResult:
    ticks_executed: int
    claims_processed: int
    unique_flows_processed: int
    runs_progressed: int
    runs_failed_technically: int
    ingestion_failures: int
    runs_stopped: int
    follow_ups_created: int
    ended_reason: str
    tick_results: tuple[WorkerTickResult, ...]
    summary_paths: WorkerSummaryPaths

    def to_dict(self) -> dict[str, object]:
        return {
            "ticks_executed": self.ticks_executed,
            "claims_processed": self.claims_processed,
            "unique_flows_processed": self.unique_flows_processed,
            "runs_progressed": self.runs_progressed,
            "runs_failed_technically": self.runs_failed_technically,
            "ingestion_failures": self.ingestion_failures,
            "runs_stopped": self.runs_stopped,
            "follow_ups_created": self.follow_ups_created,
            "ended_reason": self.ended_reason,
            "tick_results": [result.to_dict() for result in self.tick_results],
            "summary_paths": self.summary_paths.to_dict(),
        }


class WorkerLoopError(Exception):
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


def run_worker_tick(
    database_path: str | Path,
    *,
    runtime_config: WorkerRuntimeConfig | None = None,
) -> WorkerTickResult:
    resolved_db_path = _resolve_database_path(database_path)
    config = runtime_config or WorkerRuntimeConfig()
    warnings: list[str] = []
    roles_dispatched: list[str] = []
    dispatch_results: list[DispatchResult] = []
    reviewer_ingestion: ReviewerResultIngestionResult | None = None
    claim_result: ClaimNextRunResult | None = None
    initial_role: str | None = None

    try:
        claim_result = claim_next_run(resolved_db_path, now=config.claim_now)
    except SchedulerPersistenceError as exc:
        raise WorkerLoopError(
            code=WORKER_CLAIM_FAILED,
            message=exc.message,
            database_path=resolved_db_path,
            details=exc.details,
        ) from exc

    if claim_result is None:
        return _finalize_tick_result(
            config,
            WorkerTickResult(
                status="idle",
                claimed_run_id=None,
                claimed_queue_item_id=None,
                claimed_flow_id=None,
                claim=None,
                initial_role=None,
                roles_dispatched=tuple(),
                reviewer_ingestion_happened=False,
                follow_up_run_created=False,
                follow_up_run_id=None,
                final_run_status=None,
                final_run=None,
                dispatch_results=tuple(),
                reviewer_ingestion=None,
                queue_requeued=False,
                error=None,
                warnings=tuple(),
                summary_paths=WorkerSummaryPaths(None, None, None),
            ),
        )

    run_id = claim_result.dispatch_run.run.id
    queue_item_id = claim_result.dispatch_run.queue_item.id
    flow_id = claim_result.dispatch_run.flow_context.flow_id

    try:
        initial_role = determine_dispatch_role(resolved_db_path, run_id).resolved_role
    except DispatchAdapterError as exc:
        raise WorkerLoopError(
            code=WORKER_ROLE_DETERMINATION_FAILED,
            message=exc.message,
            database_path=resolved_db_path,
            details=exc.details,
        ) from exc

    if initial_role == "executor":
        executor_dispatch = _dispatch_one_role(
            resolved_db_path,
            requested_role="executor",
            claim_result=claim_result,
            runtime_config=config,
        )
        roles_dispatched.append("executor")
        dispatch_results.append(executor_dispatch)
        if not executor_dispatch.technical_success:
            return _build_dispatch_failed_tick(
                resolved_db_path,
                config,
                claim_result=claim_result,
                initial_role=initial_role,
                roles_dispatched=roles_dispatched,
                dispatch_results=dispatch_results,
                warnings=warnings,
            )

        if config.effective_mode() == "executor-only":
            final_run = _load_run_details_or_raise(resolved_db_path, run_id)
            return _finalize_tick_result(
                config,
                WorkerTickResult(
                    status="progressed",
                    claimed_run_id=run_id,
                    claimed_queue_item_id=queue_item_id,
                    claimed_flow_id=flow_id,
                    claim=claim_result,
                    initial_role=initial_role,
                    roles_dispatched=tuple(roles_dispatched),
                    reviewer_ingestion_happened=False,
                    follow_up_run_created=False,
                    follow_up_run_id=None,
                    final_run_status=final_run.run.status,
                    final_run=final_run,
                    dispatch_results=tuple(dispatch_results),
                    reviewer_ingestion=None,
                    queue_requeued=False,
                    error=None,
                    warnings=tuple(warnings),
                    summary_paths=WorkerSummaryPaths(None, None, None),
                ),
            )

        try:
            next_role = determine_dispatch_role(
                resolved_db_path,
                run_id,
                requested_role="reviewer",
            ).resolved_role
        except DispatchAdapterError as exc:
            raise WorkerLoopError(
                code=WORKER_REVIEWER_DISPATCH_UNEXPECTED,
                message=exc.message,
                database_path=resolved_db_path,
                details=exc.details,
            ) from exc
        if next_role != "reviewer":
            raise WorkerLoopError(
                code=WORKER_REVIEWER_DISPATCH_UNEXPECTED,
                message=f"expected reviewer dispatch after successful executor step, got: {next_role}",
                database_path=resolved_db_path,
            )

        reviewer_dispatch = _dispatch_one_role(
            resolved_db_path,
            requested_role="reviewer",
            claim_result=None,
            runtime_config=config,
            run_id=run_id,
        )
        roles_dispatched.append("reviewer")
        dispatch_results.append(reviewer_dispatch)
        if not reviewer_dispatch.technical_success:
            return _build_dispatch_failed_tick(
                resolved_db_path,
                config,
                claim_result=claim_result,
                initial_role=initial_role,
                roles_dispatched=roles_dispatched,
                dispatch_results=dispatch_results,
                warnings=warnings,
            )

        reviewer_ingestion = _ingest_reviewer_or_build_failure(
            resolved_db_path,
            config,
            claim_result=claim_result,
            initial_role=initial_role,
            roles_dispatched=roles_dispatched,
            dispatch_results=dispatch_results,
            warnings=warnings,
            reviewer_dispatch=reviewer_dispatch,
        )
        if isinstance(reviewer_ingestion, WorkerTickResult):
            return reviewer_ingestion
    else:
        reviewer_dispatch = _dispatch_one_role(
            resolved_db_path,
            requested_role="reviewer",
            claim_result=claim_result,
            runtime_config=config,
        )
        roles_dispatched.append("reviewer")
        dispatch_results.append(reviewer_dispatch)
        if not reviewer_dispatch.technical_success:
            return _build_dispatch_failed_tick(
                resolved_db_path,
                config,
                claim_result=claim_result,
                initial_role=initial_role,
                roles_dispatched=roles_dispatched,
                dispatch_results=dispatch_results,
                warnings=warnings,
            )

        reviewer_ingestion = _ingest_reviewer_or_build_failure(
            resolved_db_path,
            config,
            claim_result=claim_result,
            initial_role=initial_role,
            roles_dispatched=roles_dispatched,
            dispatch_results=dispatch_results,
            warnings=warnings,
            reviewer_dispatch=reviewer_dispatch,
        )
        if isinstance(reviewer_ingestion, WorkerTickResult):
            return reviewer_ingestion

    assert reviewer_ingestion is not None
    final_run = reviewer_ingestion.reviewer_outcome.current_run
    follow_up_run_id = reviewer_ingestion.reviewer_outcome.flow_summary.created_follow_up_run_id
    final_status = "stopped" if final_run.run.status == "stopped" else "progressed"
    return _finalize_tick_result(
        config,
        WorkerTickResult(
            status=final_status,
            claimed_run_id=run_id,
            claimed_queue_item_id=queue_item_id,
            claimed_flow_id=flow_id,
            claim=claim_result,
            initial_role=initial_role,
            roles_dispatched=tuple(roles_dispatched),
            reviewer_ingestion_happened=True,
            follow_up_run_created=follow_up_run_id is not None,
            follow_up_run_id=follow_up_run_id,
            final_run_status=final_run.run.status,
            final_run=final_run,
            dispatch_results=tuple(dispatch_results),
            reviewer_ingestion=reviewer_ingestion,
            queue_requeued=any(item.queue_requeue is not None for item in dispatch_results),
            error=None,
            warnings=tuple(warnings),
            summary_paths=WorkerSummaryPaths(None, None, None),
        ),
    )


def run_worker_until_idle(
    database_path: str | Path,
    *,
    runtime_config: WorkerRuntimeConfig | None = None,
    max_ticks: int = 100,
    max_claims: int | None = None,
    max_flows: int | None = None,
    max_wall_clock_seconds: int | float | None = None,
) -> WorkerLoopResult:
    resolved_db_path = _resolve_database_path(database_path)
    config = runtime_config or WorkerRuntimeConfig()

    _validate_limit("max_ticks", max_ticks, resolved_db_path, allow_none=False)
    _validate_limit("max_claims", max_claims, resolved_db_path, allow_none=True)
    _validate_limit("max_flows", max_flows, resolved_db_path, allow_none=True)
    _validate_limit("max_wall_clock_seconds", max_wall_clock_seconds, resolved_db_path, allow_none=True)

    started_monotonic = time.monotonic()
    tick_results: list[WorkerTickResult] = []
    seen_flows: set[str] = set()
    claims_processed = 0
    runs_progressed = 0
    runs_failed_technically = 0
    ingestion_failures = 0
    runs_stopped = 0
    follow_ups_created = 0
    ended_reason: str | None = None

    while True:
        if len(tick_results) >= max_ticks:
            ended_reason = "max_ticks_reached"
            break
        if max_claims is not None and claims_processed >= max_claims:
            ended_reason = "max_claims_reached"
            break
        if max_flows is not None and len(seen_flows) >= max_flows:
            ended_reason = "max_flows_reached"
            break
        if max_wall_clock_seconds is not None and (time.monotonic() - started_monotonic) >= float(max_wall_clock_seconds):
            ended_reason = "max_wall_clock_time_reached"
            break

        tick_result = run_worker_tick(resolved_db_path, runtime_config=config)
        tick_results.append(tick_result)

        if tick_result.claimed_run_id is not None:
            claims_processed += 1
        if tick_result.claimed_flow_id is not None:
            seen_flows.add(tick_result.claimed_flow_id)
        if tick_result.status in {"progressed", "stopped"}:
            runs_progressed += 1
        if tick_result.status == "stopped":
            runs_stopped += 1
        if tick_result.status == "dispatch_failed":
            runs_failed_technically += 1
            ended_reason = "dispatch_failed"
            break
        if tick_result.status == "ingestion_failed":
            ingestion_failures += 1
            ended_reason = "ingestion_failed"
            break
        if tick_result.follow_up_run_created:
            follow_ups_created += 1
        if tick_result.status == "idle":
            ended_reason = "idle"
            break

    assert ended_reason is not None
    return _finalize_loop_result(
        config,
        WorkerLoopResult(
            ticks_executed=len(tick_results),
            claims_processed=claims_processed,
            unique_flows_processed=len(seen_flows),
            runs_progressed=runs_progressed,
            runs_failed_technically=runs_failed_technically,
            ingestion_failures=ingestion_failures,
            runs_stopped=runs_stopped,
            follow_ups_created=follow_ups_created,
            ended_reason=ended_reason,
            tick_results=tuple(tick_results),
            summary_paths=WorkerSummaryPaths(None, None, None),
        ),
    )


def _ingest_reviewer_or_build_failure(
    database_path: Path,
    runtime_config: WorkerRuntimeConfig,
    *,
    claim_result: ClaimNextRunResult,
    initial_role: str,
    roles_dispatched: list[str],
    dispatch_results: list[DispatchResult],
    warnings: list[str],
    reviewer_dispatch: DispatchResult,
) -> ReviewerResultIngestionResult | WorkerTickResult:
    try:
        return ingest_reviewer_result(
            database_path,
            reviewer_step_run_id=_require_step_run_id(reviewer_dispatch, database_path),
        )
    except (ReviewerResultIngestionError, ReviewerOutcomeError) as exc:
        final_run = _load_run_details_or_raise(database_path, claim_result.dispatch_run.run.id)
        return _finalize_tick_result(
            runtime_config,
            WorkerTickResult(
                status="ingestion_failed",
                claimed_run_id=claim_result.dispatch_run.run.id,
                claimed_queue_item_id=claim_result.dispatch_run.queue_item.id,
                claimed_flow_id=claim_result.dispatch_run.flow_context.flow_id,
                claim=claim_result,
                initial_role=initial_role,
                roles_dispatched=tuple(roles_dispatched),
                reviewer_ingestion_happened=True,
                follow_up_run_created=False,
                follow_up_run_id=None,
                final_run_status=final_run.run.status,
                final_run=final_run,
                dispatch_results=tuple(dispatch_results),
                reviewer_ingestion=None,
                queue_requeued=any(item.queue_requeue is not None for item in dispatch_results),
                error=WorkerOperationError(
                    stage="reviewer_ingestion",
                    code=exc.code,
                    message=exc.message,
                    details=exc.details,
                ),
                warnings=tuple(warnings),
                summary_paths=WorkerSummaryPaths(None, None, None),
            ),
        )


def _build_dispatch_failed_tick(
    database_path: Path,
    runtime_config: WorkerRuntimeConfig,
    *,
    claim_result: ClaimNextRunResult,
    initial_role: str,
    roles_dispatched: list[str],
    dispatch_results: list[DispatchResult],
    warnings: list[str],
) -> WorkerTickResult:
    final_run = _load_run_details_or_raise(database_path, claim_result.dispatch_run.run.id)
    return _finalize_tick_result(
        runtime_config,
        WorkerTickResult(
            status="dispatch_failed",
            claimed_run_id=claim_result.dispatch_run.run.id,
            claimed_queue_item_id=claim_result.dispatch_run.queue_item.id,
            claimed_flow_id=claim_result.dispatch_run.flow_context.flow_id,
            claim=claim_result,
            initial_role=initial_role,
            roles_dispatched=tuple(roles_dispatched),
            reviewer_ingestion_happened=False,
            follow_up_run_created=False,
            follow_up_run_id=None,
            final_run_status=final_run.run.status,
            final_run=final_run,
            dispatch_results=tuple(dispatch_results),
            reviewer_ingestion=None,
            queue_requeued=any(item.queue_requeue is not None for item in dispatch_results),
            error=None,
            warnings=tuple(warnings),
            summary_paths=WorkerSummaryPaths(None, None, None),
        ),
    )


def _dispatch_one_role(
    database_path: Path,
    *,
    requested_role: str,
    claim_result: ClaimNextRunResult | None,
    runtime_config: WorkerRuntimeConfig,
    run_id: str | None = None,
) -> DispatchResult:
    dispatch_kwargs = runtime_config.dispatch_kwargs()
    if claim_result is not None:
        dispatch_kwargs["claim_payload"] = claim_result.to_dict()
    if run_id is not None:
        dispatch_kwargs["run_id"] = run_id
    try:
        return dispatch_claimed_run(
            database_path,
            requested_role=requested_role,
            **dispatch_kwargs,
        )
    except DispatchAdapterError as exc:
        raise WorkerLoopError(
            code=WORKER_DISPATCH_FAILED,
            message=exc.message,
            database_path=database_path,
            details=exc.details,
        ) from exc


def _require_step_run_id(dispatch_result: DispatchResult, database_path: Path) -> str:
    if dispatch_result.step_run is None:
        raise WorkerLoopError(
            code=WORKER_DISPATCH_FAILED,
            message="reviewer dispatch succeeded without a terminal step_run payload",
            database_path=database_path,
        )
    return dispatch_result.step_run.step_run.id


def _load_run_details_or_raise(database_path: Path, run_id: str) -> RunDetails:
    try:
        return get_run(database_path, run_id)
    except RunPersistenceError as exc:
        raise WorkerLoopError(
            code=WORKER_FINAL_STATE_LOAD_FAILED,
            message=exc.message,
            database_path=database_path,
            details=exc.details,
        ) from exc


def _finalize_tick_result(runtime_config: WorkerRuntimeConfig, result: WorkerTickResult) -> WorkerTickResult:
    summary_paths = _write_summary_artifacts(
        runtime_config,
        kind="ticks",
        summary=_build_tick_summary_payload(result),
        markdown=_render_tick_markdown(result),
    )
    return WorkerTickResult(
        status=result.status,
        claimed_run_id=result.claimed_run_id,
        claimed_queue_item_id=result.claimed_queue_item_id,
        claimed_flow_id=result.claimed_flow_id,
        claim=result.claim,
        initial_role=result.initial_role,
        roles_dispatched=result.roles_dispatched,
        reviewer_ingestion_happened=result.reviewer_ingestion_happened,
        follow_up_run_created=result.follow_up_run_created,
        follow_up_run_id=result.follow_up_run_id,
        final_run_status=result.final_run_status,
        final_run=result.final_run,
        dispatch_results=result.dispatch_results,
        reviewer_ingestion=result.reviewer_ingestion,
        queue_requeued=result.queue_requeued,
        error=result.error,
        warnings=result.warnings,
        summary_paths=summary_paths,
    )


def _finalize_loop_result(runtime_config: WorkerRuntimeConfig, result: WorkerLoopResult) -> WorkerLoopResult:
    summary_paths = _write_summary_artifacts(
        runtime_config,
        kind="loops",
        summary=result.to_dict(),
        markdown=_render_loop_markdown(result),
    )
    return WorkerLoopResult(
        ticks_executed=result.ticks_executed,
        claims_processed=result.claims_processed,
        unique_flows_processed=result.unique_flows_processed,
        runs_progressed=result.runs_progressed,
        runs_failed_technically=result.runs_failed_technically,
        ingestion_failures=result.ingestion_failures,
        runs_stopped=result.runs_stopped,
        follow_ups_created=result.follow_ups_created,
        ended_reason=result.ended_reason,
        tick_results=result.tick_results,
        summary_paths=summary_paths,
    )


def _write_summary_artifacts(
    runtime_config: WorkerRuntimeConfig,
    *,
    kind: str,
    summary: Mapping[str, object],
    markdown: str,
) -> WorkerSummaryPaths:
    root = runtime_config.summary_root()
    if root is None:
        return WorkerSummaryPaths(None, None, None)
    resolved_root = root.expanduser().resolve() / kind
    resolved_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    token = generate_opaque_id()
    json_path = resolved_root / f"{stamp}-{token}.json"
    markdown_path = resolved_root / f"{stamp}-{token}.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(markdown, encoding="utf-8")
    return WorkerSummaryPaths(
        root_directory=resolved_root,
        json_path=json_path,
        markdown_path=markdown_path,
    )


def _build_tick_summary_payload(result: WorkerTickResult) -> dict[str, object]:
    payload = result.to_dict()
    payload["summary_paths"] = {
        "root_directory": None,
        "json_path": None,
        "markdown_path": None,
    }
    return payload


def _render_tick_markdown(result: WorkerTickResult) -> str:
    lines = [
        "# Worker Tick Summary",
        "",
        f"- Status: {result.status}",
        f"- Claimed run: {result.claimed_run_id or 'none'}",
        f"- Claimed queue item: {result.claimed_queue_item_id or 'none'}",
        f"- Claimed flow: {result.claimed_flow_id or 'none'}",
        f"- Initial role: {result.initial_role or 'none'}",
        f"- Roles dispatched: {', '.join(result.roles_dispatched) if result.roles_dispatched else 'none'}",
        f"- Reviewer ingestion happened: {'yes' if result.reviewer_ingestion_happened else 'no'}",
        f"- Follow-up run created: {'yes' if result.follow_up_run_created else 'no'}",
        f"- Follow-up run id: {result.follow_up_run_id or 'none'}",
        f"- Final run status: {result.final_run_status or 'none'}",
        f"- Queue requeued: {'yes' if result.queue_requeued else 'no'}",
    ]
    if result.error is not None:
        lines.extend(
            [
                "",
                "## Error",
                "",
                f"- Stage: {result.error.stage}",
                f"- Code: {result.error.code}",
                f"- Message: {result.error.message}",
                f"- Details: {result.error.details or 'none'}",
            ]
        )
    if result.warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result.warnings)
    return "\n".join(lines) + "\n"


def _render_loop_markdown(result: WorkerLoopResult) -> str:
    lines = [
        "# Worker Loop Summary",
        "",
        f"- Ended reason: {result.ended_reason}",
        f"- Ticks executed: {result.ticks_executed}",
        f"- Claims processed: {result.claims_processed}",
        f"- Unique flows processed: {result.unique_flows_processed}",
        f"- Runs progressed: {result.runs_progressed}",
        f"- Runs failed technically: {result.runs_failed_technically}",
        f"- Ingestion failures: {result.ingestion_failures}",
        f"- Runs stopped: {result.runs_stopped}",
        f"- Follow-ups created: {result.follow_ups_created}",
    ]
    return "\n".join(lines) + "\n"


def _validate_limit(name: str, value: int | float | None, database_path: Path, *, allow_none: bool) -> None:
    if value is None:
        if allow_none:
            return
        raise WorkerLoopError(
            code=INVALID_WORKER_LIMIT,
            message=f"{name} is required",
            database_path=database_path,
        )
    if isinstance(value, bool) or value < 0:
        raise WorkerLoopError(
            code=INVALID_WORKER_LIMIT,
            message=f"{name} must be >= 0",
            database_path=database_path,
            details=f"actual={value}",
        )
