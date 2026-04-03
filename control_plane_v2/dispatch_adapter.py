from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess

from .id_generation import generate_opaque_id
from .manual_control import ManualControlError, PendingRerunIntent, get_pending_rerun_intent
from .run_persistence import RunDetails, RunPersistenceError, _connect_run_db, _ensure_required_tables, _resolve_database_path
from .scheduler_persistence import ClaimedRunMutationResult, DispatchRunPayload, SchedulerPersistenceError, mark_claimed_run_dispatch_failed
from .step_run_persistence import STEP_RUN_ACTIVE_STATUS, STEP_RUN_TERMINAL_STATUSES, StepRunDetails, finish_step_run, list_step_runs, retry_step_run, start_step_run


CONTROL_DIR = Path(__file__).resolve().parents[1]
DISPATCH_ROLES = ("executor", "reviewer")
DISPATCH_ROLE_AUTODETECT = "auto"
DISPATCH_REQUESTED_ROLES = (*DISPATCH_ROLES, DISPATCH_ROLE_AUTODETECT)
LEGACY_REQUIRED_COMMANDS = ("codex", "git", "python3", "awk")
RUN_TERMINAL_STATUSES = ("completed", "failed", "stopped", "cancelled")

ARTIFACT_KIND_DISPATCH_CONTEXT_MANIFEST = "dispatch_context_manifest"
ARTIFACT_KIND_DISPATCH_FAILURE_MANIFEST = "dispatch_failure_manifest"
ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST = "dispatch_result_manifest"
ARTIFACT_KIND_LAST_MESSAGE = "last_message"
ARTIFACT_KIND_PROMPT_COPY = "prompt_copy"
ARTIFACT_KIND_RESOLVED_CONTEXT_MANIFEST = "resolved_context_manifest"
ARTIFACT_KIND_STDERR_LOG = "stderr_log"
ARTIFACT_KIND_STDOUT_LOG = "stdout_log"
ARTIFACT_KIND_STEP_REPORT = "step_report"
ARTIFACT_KIND_STEP_RESULT_JSON = "step_result_json"
ARTIFACT_KIND_STEP_STATE_JSON = "step_state_json"

DISPATCH_NOT_ALLOWED = "DISPATCH_NOT_ALLOWED"
DISPATCH_PAYLOAD_INVALID = "DISPATCH_PAYLOAD_INVALID"
DISPATCH_TARGET_NOT_CLAIMED = "DISPATCH_TARGET_NOT_CLAIMED"
DISPATCH_TARGET_REQUIRED = "DISPATCH_TARGET_REQUIRED"
INVALID_DISPATCH_ROLE = "INVALID_DISPATCH_ROLE"
LEGACY_BACKEND_PRECHECK_FAILED = "LEGACY_BACKEND_PRECHECK_FAILED"
REVIEWER_HANDOFF_MISSING = "REVIEWER_HANDOFF_MISSING"
RUN_CONTEXT_INVALID = "RUN_CONTEXT_INVALID"
MANUAL_CONTROL_LOOKUP_FAILED = "MANUAL_CONTROL_LOOKUP_FAILED"

PROVISIONAL_RUN_REQUEUE_ROLLBACK_TRANSITION_TYPE = "dispatch_step_start_rolled_back"


@dataclass(frozen=True)
class DispatchArtifactRecord:
    id: str
    project_id: str
    flow_id: str
    run_id: str
    step_run_id: str | None
    artifact_kind: str
    filesystem_path: Path
    media_type: str | None
    size_bytes: int | None
    checksum_sha256: str | None
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "project_id": self.project_id,
            "flow_id": self.flow_id,
            "run_id": self.run_id,
            "step_run_id": self.step_run_id,
            "artifact_kind": self.artifact_kind,
            "filesystem_path": str(self.filesystem_path),
            "media_type": self.media_type,
            "size_bytes": self.size_bytes,
            "checksum_sha256": self.checksum_sha256,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class LegacyDispatchBackendConfig:
    control_dir: Path
    executor_runner_path: Path
    reviewer_runner_path: Path

    def runner_path_for_role(self, role: str) -> Path:
        return self.executor_runner_path if role == "executor" else self.reviewer_runner_path

    def to_dict(self) -> dict[str, str]:
        return {
            "control_dir": str(self.control_dir),
            "executor_runner_path": str(self.executor_runner_path),
            "reviewer_runner_path": str(self.reviewer_runner_path),
        }


@dataclass(frozen=True)
class LegacyDispatchRuntimeContext:
    project: str
    task_text: str
    mode: str
    branch_base: str
    auto_commit: bool
    source: str
    thread_label: str
    project_repo_path: Path
    executor_worktree_path: Path
    reviewer_worktree_path: Path | None
    instruction_profile: str
    instruction_overlays: tuple[str, ...]
    instructions_repo_path: Path
    constraints: tuple[str, ...]
    expected_output: tuple[str, ...]
    commit_sha: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "project": self.project,
            "task_text": self.task_text,
            "mode": self.mode,
            "branch_base": self.branch_base,
            "auto_commit": self.auto_commit,
            "source": self.source,
            "thread_label": self.thread_label,
            "project_repo_path": str(self.project_repo_path),
            "executor_worktree_path": str(self.executor_worktree_path),
            "reviewer_worktree_path": str(self.reviewer_worktree_path) if self.reviewer_worktree_path else None,
            "instruction_profile": self.instruction_profile,
            "instruction_overlays": list(self.instruction_overlays),
            "instructions_repo_path": str(self.instructions_repo_path),
            "constraints": list(self.constraints),
            "expected_output": list(self.expected_output),
            "commit_sha": self.commit_sha,
        }


@dataclass(frozen=True)
class DispatchRoleDecision:
    requested_role: str
    resolved_role: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "requested_role": self.requested_role,
            "resolved_role": self.resolved_role,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class DispatchAttemptPaths:
    run_artifact_directory: Path | None
    attempt_directory: Path
    context_manifest_path: Path
    result_manifest_path: Path
    stdout_log_path: Path
    stderr_log_path: Path
    failure_manifest_path: Path
    legacy_control_directory: Path | None = None
    legacy_runtime_directory: Path | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "run_artifact_directory": str(self.run_artifact_directory) if self.run_artifact_directory else None,
            "attempt_directory": str(self.attempt_directory),
            "context_manifest_path": str(self.context_manifest_path),
            "result_manifest_path": str(self.result_manifest_path),
            "stdout_log_path": str(self.stdout_log_path),
            "stderr_log_path": str(self.stderr_log_path),
            "failure_manifest_path": str(self.failure_manifest_path),
            "legacy_control_directory": str(self.legacy_control_directory) if self.legacy_control_directory else None,
            "legacy_runtime_directory": str(self.legacy_runtime_directory) if self.legacy_runtime_directory else None,
        }


@dataclass(frozen=True)
class DispatchExecutionOutcome:
    backend_started: bool
    backend_exit_code: int | None
    technical_success: bool
    step_terminal_status: str | None
    commit_sha: str | None
    state_status: str | None
    state_result: dict[str, object] | None

    def to_dict(self) -> dict[str, object]:
        return {
            "backend_started": self.backend_started,
            "backend_exit_code": self.backend_exit_code,
            "technical_success": self.technical_success,
            "step_terminal_status": self.step_terminal_status,
            "commit_sha": self.commit_sha,
            "state_status": self.state_status,
            "state_result": self.state_result,
        }


@dataclass(frozen=True)
class DispatchResult:
    dispatch_run: DispatchRunPayload
    role_decision: DispatchRoleDecision
    step_run: StepRunDetails | None
    technical_success: bool
    backend_started: bool
    backend_exit_code: int | None
    queue_requeue: ClaimedRunMutationResult | None
    artifacts: tuple[DispatchArtifactRecord, ...]
    warnings: tuple[str, ...]
    attempt_paths: DispatchAttemptPaths

    def to_dict(self) -> dict[str, object]:
        return {
            "dispatch_run": self.dispatch_run.to_dict(),
            "role_decision": self.role_decision.to_dict(),
            "step_run": self.step_run.to_dict() if self.step_run is not None else None,
            "technical_success": self.technical_success,
            "backend_started": self.backend_started,
            "backend_exit_code": self.backend_exit_code,
            "queue_requeue": self.queue_requeue.to_dict() if self.queue_requeue is not None else None,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "warnings": list(self.warnings),
            "attempt_paths": self.attempt_paths.to_dict(),
        }


class DispatchAdapterError(Exception):
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


def determine_dispatch_role(
    database_path: str | Path,
    run_id: str,
    *,
    requested_role: str = DISPATCH_ROLE_AUTODETECT,
) -> DispatchRoleDecision:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_requested_role = _normalize_requested_role(requested_role, resolved_db_path)
    run_details = _load_run_details(resolved_db_path, run_id)
    step_runs = list_step_runs(resolved_db_path, run_id=run_id, limit=1000)

    if run_details.run.status in RUN_TERMINAL_STATUSES:
        raise DispatchAdapterError(
            code=DISPATCH_NOT_ALLOWED,
            message=f"Run is already terminal and cannot be dispatched: {run_id}",
            database_path=resolved_db_path,
            details=f"run_status={run_details.run.status}",
        )
    if run_details.run.queue_item is None or run_details.run.queue_item.status != "claimed":
        raise DispatchAdapterError(
            code=DISPATCH_TARGET_NOT_CLAIMED,
            message=f"Run is not currently claimed for dispatch: {run_id}",
            database_path=resolved_db_path,
            details=f"queue_status={run_details.run.queue_item.status if run_details.run.queue_item else 'missing'}",
        )

    active_step = next((step for step in step_runs if step.status == STEP_RUN_ACTIVE_STATUS), None)
    if active_step is not None:
        raise DispatchAdapterError(
            code=DISPATCH_NOT_ALLOWED,
            message=f"Run already has an active step_run: {active_step.id}",
            database_path=resolved_db_path,
            details=f"step_key={active_step.step_key}",
        )

    pending_rerun = _load_pending_rerun_intent_or_raise(resolved_db_path, run_id)
    if pending_rerun is not None:
        resolved_role = pending_rerun.step_key
        reason = f"manual_rerun_{pending_rerun.step_key}"
    else:
        reviewer_steps = [step for step in step_runs if step.step_key == "reviewer"]
        if reviewer_steps:
            reviewer_step = reviewer_steps[-1]
            raise DispatchAdapterError(
                code=DISPATCH_NOT_ALLOWED,
                message=f"Reviewer dispatch is already recorded on this run: {reviewer_step.id}",
                database_path=resolved_db_path,
                details=f"reviewer_status={reviewer_step.status}",
            )

        executor_steps = [step for step in step_runs if step.step_key == "executor"]
        if not step_runs:
            resolved_role = "executor"
            reason = "run_has_no_step_runs"
        elif executor_steps and executor_steps[-1].status in STEP_RUN_TERMINAL_STATUSES:
            resolved_role = "reviewer"
            reason = "terminal_executor_present_without_reviewer"
        else:
            raise DispatchAdapterError(
                code=DISPATCH_NOT_ALLOWED,
                message=f"Run has no dispatchable next role: {run_id}",
                database_path=resolved_db_path,
            )

    if normalized_requested_role != DISPATCH_ROLE_AUTODETECT and normalized_requested_role != resolved_role:
        raise DispatchAdapterError(
            code=DISPATCH_NOT_ALLOWED,
            message=f"Requested role does not match the next dispatchable role: {normalized_requested_role}",
            database_path=resolved_db_path,
            details=f"next_dispatchable_role={resolved_role}",
        )

    return DispatchRoleDecision(
        requested_role=normalized_requested_role,
        resolved_role=resolved_role,
        reason=reason,
    )


def load_dispatch_target(
    database_path: str | Path,
    *,
    run_id: str | None = None,
    queue_item_id: str | None = None,
) -> DispatchRunPayload:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_run_id, normalized_queue_item_id = _normalize_target_identifiers(
        resolved_db_path,
        run_id=run_id,
        queue_item_id=queue_item_id,
    )
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("projects", "runs", "queue_items"))
        filters: list[str] = []
        params: list[object] = []
        if normalized_run_id is not None:
            filters.append("runs.id = ?")
            params.append(normalized_run_id)
        if normalized_queue_item_id is not None:
            filters.append("queue_items.id = ?")
            params.append(normalized_queue_item_id)

        row = connection.execute(
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
            FROM runs
            JOIN projects ON projects.id = runs.project_id
            JOIN queue_items ON queue_items.run_id = runs.id
            WHERE {" AND ".join(filters)}
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
        if row is None:
            raise DispatchAdapterError(
                code=DISPATCH_TARGET_REQUIRED,
                message="Claim target is not present in SQLite",
                database_path=resolved_db_path,
                details=f"run_id={normalized_run_id} queue_item_id={normalized_queue_item_id}",
            )
        flow_rows = connection.execute(
            """
            SELECT id
            FROM runs
            WHERE flow_id = ?
            ORDER BY created_at, id
            """,
            (row["flow_id"],),
        ).fetchall()
        flow_run_ids = [flow_row["id"] for flow_row in flow_rows]
        current_index = flow_run_ids.index(row["id"])

        from .project_registry import RegisteredProject
        from .run_persistence import QueueItemRecord, RunSummary
        from .scheduler_persistence import FlowContextSummary

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
        run = RunSummary(
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
        project = RegisteredProject(
            id=row["project_id"],
            project_key=row["project_key"],
            package_root=Path(row["package_root"]),
            created_at=row["project_created_at"],
            updated_at=row["project_updated_at"],
        )
        flow_context = FlowContextSummary(
            flow_id=row["flow_id"],
            current_run_id=row["id"],
            root_run_id=flow_run_ids[0],
            cycle_no=current_index + 1,
            total_runs=len(flow_run_ids),
            parent_run_id=row["parent_run_id"],
            origin_type=row["origin_type"],
            origin_run_id=row["origin_run_id"],
            origin_step_run_id=row["origin_step_run_id"],
        )
        return DispatchRunPayload(
            run=run,
            queue_item=queue_item,
            project=project,
            project_package_root=project.package_root,
            flow_context=flow_context,
        )
    except sqlite3.Error as exc:
        raise DispatchAdapterError(
            code=DISPATCH_PAYLOAD_INVALID,
            message="Failed to load dispatch target",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def dispatch_claimed_run(
    database_path: str | Path,
    *,
    requested_role: str = DISPATCH_ROLE_AUTODETECT,
    claim_payload: Mapping[str, object] | None = None,
    runtime_context: Mapping[str, object] | None = None,
    run_id: str | None = None,
    queue_item_id: str | None = None,
    artifact_root: str | Path | None = None,
    workspace_root: str | Path | None = None,
    project_repo_path: str | Path | None = None,
    executor_worktree_path: str | Path | None = None,
    reviewer_worktree_path: str | Path | None = None,
    instructions_repo_path: str | Path | None = None,
    branch_base: str | None = None,
    instruction_profile: str | None = None,
    instruction_overlays: list[str] | tuple[str, ...] | None = None,
    task_text: str | None = None,
    mode: str | None = None,
    source: str | None = None,
    thread_label: str | None = None,
    constraints: list[str] | tuple[str, ...] | None = None,
    expected_output: list[str] | tuple[str, ...] | None = None,
    legacy_control_dir: str | Path | None = None,
    executor_runner_path: str | Path | None = None,
    reviewer_runner_path: str | Path | None = None,
) -> DispatchResult:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_requested_role = _normalize_requested_role(requested_role, resolved_db_path)
    resolved_run_id, resolved_queue_item_id = _resolve_dispatch_target(
        resolved_db_path,
        claim_payload=claim_payload,
        run_id=run_id,
        queue_item_id=queue_item_id,
    )
    dispatch_run = load_dispatch_target(
        resolved_db_path,
        run_id=resolved_run_id,
        queue_item_id=resolved_queue_item_id,
    )
    role_decision = determine_dispatch_role(
        resolved_db_path,
        dispatch_run.run.id,
        requested_role=normalized_requested_role,
    )
    backend_config = _resolve_backend_config(
        control_dir=legacy_control_dir,
        executor_runner_path=executor_runner_path,
        reviewer_runner_path=reviewer_runner_path,
    )
    merged_context = _merge_runtime_context_inputs(
        claim_payload=claim_payload,
        runtime_context=runtime_context,
        workspace_root=workspace_root,
        project_key=dispatch_run.run.project_key,
        project_repo_path=project_repo_path,
        executor_worktree_path=executor_worktree_path,
        reviewer_worktree_path=reviewer_worktree_path,
        instructions_repo_path=instructions_repo_path,
        branch_base=branch_base,
        instruction_profile=instruction_profile,
        instruction_overlays=instruction_overlays,
        task_text=task_text,
        mode=mode,
        source=source,
        thread_label=thread_label,
        constraints=constraints,
        expected_output=expected_output,
    )
    previous_executor_manifest = _load_latest_dispatch_result_manifest(
        resolved_db_path,
        dispatch_run.run.id,
        step_key="executor" if role_decision.resolved_role == "reviewer" else None,
    )
    legacy_context = _build_legacy_runtime_context(
        resolved_db_path,
        dispatch_run=dispatch_run,
        role=role_decision.resolved_role,
        merged_context=merged_context,
        previous_manifest=previous_executor_manifest,
    )
    run_artifact_directory = (
        _resolve_run_artifact_directory(artifact_root, dispatch_run)
        if artifact_root is not None
        else None
    )

    preflight_paths = _prepare_preflight_paths(run_artifact_directory, role_decision.resolved_role)
    _write_json(
        preflight_paths.context_manifest_path,
        {
            "phase": "preflight",
            "requested_role": normalized_requested_role,
            "resolved_role": role_decision.resolved_role,
            "dispatch_run": dispatch_run.to_dict(),
            "runtime_context": legacy_context.to_dict(),
            "backend": backend_config.to_dict(),
        },
    )
    try:
        _preflight_backend(resolved_db_path, legacy_context, role_decision.resolved_role, backend_config)
    except DispatchAdapterError as exc:
        queue_requeue = _mark_dispatch_failed(
            resolved_db_path,
            dispatch_run,
            reason_code=LEGACY_BACKEND_PRECHECK_FAILED,
            note=exc.message,
        )
        _write_json(
            preflight_paths.failure_manifest_path,
            {
                "dispatch_run": dispatch_run.to_dict(),
                "role_decision": role_decision.to_dict(),
                "error": exc.to_dict(),
                "queue_requeue": queue_requeue.to_dict(),
            },
        )
        warnings, artifacts = _record_dispatch_artifacts(
            resolved_db_path,
            dispatch_run,
            step_run_id=None,
            role=None,
            attempt_paths=preflight_paths,
        )
        return DispatchResult(
            dispatch_run=dispatch_run,
            role_decision=role_decision,
            step_run=None,
            technical_success=False,
            backend_started=False,
            backend_exit_code=None,
            queue_requeue=queue_requeue,
            artifacts=artifacts,
            warnings=warnings,
            attempt_paths=preflight_paths,
        )

    pre_start_run_status = dispatch_run.run.status
    pending_rerun = _load_pending_rerun_intent_or_raise(resolved_db_path, dispatch_run.run.id)
    started_step_run = _start_dispatch_step_run(
        resolved_db_path,
        run_id=dispatch_run.run.id,
        role=role_decision.resolved_role,
        pending_rerun=pending_rerun,
    )
    attempt_paths = _prepare_attempt_paths(run_artifact_directory, role_decision.resolved_role, started_step_run.step_run.id)
    _write_json(
        attempt_paths.context_manifest_path,
        {
            "phase": "started",
            "requested_role": normalized_requested_role,
            "resolved_role": role_decision.resolved_role,
            "dispatch_run": dispatch_run.to_dict(),
            "runtime_context": legacy_context.to_dict(),
            "backend": backend_config.to_dict(),
            "step_run_id": started_step_run.step_run.id,
            "pre_start_run_status": pre_start_run_status,
        },
    )

    try:
        _materialize_legacy_control_sandbox(
            attempt_paths,
            dispatch_run=dispatch_run,
            role=role_decision.resolved_role,
            context=legacy_context,
            backend=backend_config,
        )
        backend_started, backend_exit_code = _run_legacy_backend(
            role=role_decision.resolved_role,
            backend=backend_config,
            attempt_paths=attempt_paths,
        )
    except OSError as exc:
        _rollback_started_step_run(resolved_db_path, started_step_run.step_run.id, pre_start_run_status=pre_start_run_status)
        queue_requeue = _mark_dispatch_failed(
            resolved_db_path,
            dispatch_run,
            reason_code=LEGACY_BACKEND_PRECHECK_FAILED,
            note=str(exc),
        )
        _write_json(
            attempt_paths.failure_manifest_path,
            {
                "dispatch_run": dispatch_run.to_dict(),
                "role_decision": role_decision.to_dict(),
                "error": {"message": str(exc)},
                "queue_requeue": queue_requeue.to_dict(),
            },
        )
        warnings, artifacts = _record_dispatch_artifacts(
            resolved_db_path,
            dispatch_run,
            step_run_id=None,
            role=None,
            attempt_paths=attempt_paths,
        )
        return DispatchResult(
            dispatch_run=dispatch_run,
            role_decision=role_decision,
            step_run=None,
            technical_success=False,
            backend_started=False,
            backend_exit_code=None,
            queue_requeue=queue_requeue,
            artifacts=artifacts,
            warnings=warnings,
            attempt_paths=attempt_paths,
        )

    sandbox_state = _read_json_optional(attempt_paths.legacy_control_directory / "state" / "current.json") or {}
    outcome = _build_dispatch_outcome(
        backend_started=backend_started,
        backend_exit_code=backend_exit_code,
        sandbox_state=sandbox_state,
    )
    finished_step_run = finish_step_run(
        resolved_db_path,
        started_step_run.step_run.id,
        outcome.step_terminal_status or "failed",
    )
    _write_json(
        attempt_paths.result_manifest_path,
        {
            "dispatch_run": dispatch_run.to_dict(),
            "role_decision": role_decision.to_dict(),
            "step_run_id": finished_step_run.step_run.id,
            "runtime_context": legacy_context.to_dict(),
            "backend": backend_config.to_dict(),
            "dispatch_outcome": outcome.to_dict(),
        },
    )
    warnings, artifacts = _record_dispatch_artifacts(
        resolved_db_path,
        dispatch_run,
        step_run_id=finished_step_run.step_run.id,
        role=role_decision.resolved_role,
        attempt_paths=attempt_paths,
    )
    return DispatchResult(
        dispatch_run=dispatch_run,
        role_decision=role_decision,
        step_run=finished_step_run,
        technical_success=outcome.technical_success,
        backend_started=backend_started,
        backend_exit_code=backend_exit_code,
        queue_requeue=None,
        artifacts=artifacts,
        warnings=warnings,
        attempt_paths=attempt_paths,
    )


def _resolve_dispatch_target(
    database_path: Path,
    *,
    claim_payload: Mapping[str, object] | None,
    run_id: str | None,
    queue_item_id: str | None,
) -> tuple[str | None, str | None]:
    if run_id or queue_item_id:
        return _normalize_target_identifiers(database_path, run_id=run_id, queue_item_id=queue_item_id)
    if claim_payload is None:
        raise DispatchAdapterError(
            code=DISPATCH_TARGET_REQUIRED,
            message="Provide claim_payload or one of run_id / queue_item_id",
            database_path=database_path,
        )
    payload_run_id, payload_queue_item_id = _extract_target_from_claim_payload(claim_payload)
    if payload_run_id and payload_queue_item_id:
        payload_queue_item_id = None
    return _normalize_target_identifiers(
        database_path,
        run_id=payload_run_id,
        queue_item_id=payload_queue_item_id,
    )


def _normalize_requested_role(requested_role: str, database_path: Path) -> str:
    normalized_role = (requested_role or "").strip().lower()
    if normalized_role not in DISPATCH_REQUESTED_ROLES:
        raise DispatchAdapterError(
            code=INVALID_DISPATCH_ROLE,
            message=f"requested_role must be one of: {', '.join(DISPATCH_REQUESTED_ROLES)}",
            database_path=database_path,
            details=f"actual={requested_role}",
        )
    return normalized_role


def _load_pending_rerun_intent_or_raise(database_path: Path, run_id: str) -> PendingRerunIntent | None:
    try:
        return get_pending_rerun_intent(database_path, run_id)
    except ManualControlError as exc:
        raise DispatchAdapterError(
            code=MANUAL_CONTROL_LOOKUP_FAILED,
            message=exc.message,
            database_path=database_path,
            details=exc.details,
        ) from exc


def _start_dispatch_step_run(
    database_path: Path,
    *,
    run_id: str,
    role: str,
    pending_rerun: PendingRerunIntent | None,
) -> StepRunDetails:
    if pending_rerun is not None and pending_rerun.step_key == role:
        return retry_step_run(database_path, pending_rerun.source_step_run_id)
    return start_step_run(database_path, run_id, role)


def _normalize_target_identifiers(
    database_path: Path,
    *,
    run_id: str | None,
    queue_item_id: str | None,
) -> tuple[str | None, str | None]:
    if bool(run_id) == bool(queue_item_id):
        raise DispatchAdapterError(
            code=DISPATCH_TARGET_REQUIRED,
            message="Provide exactly one of run_id or queue_item_id",
            database_path=database_path,
        )
    normalized_run_id = run_id.strip() if isinstance(run_id, str) and run_id.strip() else None
    normalized_queue_item_id = queue_item_id.strip() if isinstance(queue_item_id, str) and queue_item_id.strip() else None
    return normalized_run_id, normalized_queue_item_id


def _extract_target_from_claim_payload(claim_payload: Mapping[str, object]) -> tuple[str | None, str | None]:
    payload = _extract_claim_object(claim_payload)
    dispatch_run = payload.get("dispatch_run")
    if isinstance(dispatch_run, Mapping):
        run = dispatch_run.get("run")
        queue_item = dispatch_run.get("queue_item")
        run_id = run.get("id") if isinstance(run, Mapping) else None
        queue_item_id = queue_item.get("id") if isinstance(queue_item, Mapping) else None
        return _string_or_none(run_id), _string_or_none(queue_item_id)
    return _string_or_none(payload.get("run_id")), _string_or_none(payload.get("queue_item_id"))


def _extract_claim_object(claim_payload: Mapping[str, object]) -> Mapping[str, object]:
    claim_object = claim_payload.get("claim")
    return claim_object if isinstance(claim_object, Mapping) else claim_payload


def _merge_runtime_context_inputs(
    *,
    claim_payload: Mapping[str, object] | None,
    runtime_context: Mapping[str, object] | None,
    workspace_root: str | Path | None,
    project_key: str,
    project_repo_path: str | Path | None,
    executor_worktree_path: str | Path | None,
    reviewer_worktree_path: str | Path | None,
    instructions_repo_path: str | Path | None,
    branch_base: str | None,
    instruction_profile: str | None,
    instruction_overlays: list[str] | tuple[str, ...] | None,
    task_text: str | None,
    mode: str | None,
    source: str | None,
    thread_label: str | None,
    constraints: list[str] | tuple[str, ...] | None,
    expected_output: list[str] | tuple[str, ...] | None,
) -> dict[str, object]:
    merged: dict[str, object] = {}
    if claim_payload is not None:
        merged.update(_extract_runtime_context_from_mapping(claim_payload))
    if runtime_context is not None:
        merged.update(dict(runtime_context))

    resolved_workspace_root = Path(workspace_root).expanduser().resolve() if workspace_root is not None else None
    if resolved_workspace_root is not None:
        merged.setdefault("project_repo_path", str(resolved_workspace_root / "projects" / project_key))
        merged.setdefault("executor_worktree_path", str(resolved_workspace_root / "runtime" / "worktrees" / f"{project_key}-executor"))
        merged.setdefault("reviewer_worktree_path", str(resolved_workspace_root / "runtime" / "worktrees" / f"{project_key}-reviewer"))

    if project_repo_path is not None:
        merged["project_repo_path"] = str(Path(project_repo_path).expanduser().resolve())
    if executor_worktree_path is not None:
        merged["executor_worktree_path"] = str(Path(executor_worktree_path).expanduser().resolve())
    if reviewer_worktree_path is not None:
        merged["reviewer_worktree_path"] = str(Path(reviewer_worktree_path).expanduser().resolve())
    if instructions_repo_path is not None:
        merged["instructions_repo_path"] = str(Path(instructions_repo_path).expanduser().resolve())
    if branch_base is not None:
        merged["branch_base"] = branch_base
    if instruction_profile is not None:
        merged["instruction_profile"] = instruction_profile
    if instruction_overlays is not None:
        merged["instruction_overlays"] = list(instruction_overlays)
    if task_text is not None:
        merged["task_text"] = task_text
    if mode is not None:
        merged["mode"] = mode
    if source is not None:
        merged["source"] = source
    if thread_label is not None:
        merged["thread_label"] = thread_label
    if constraints is not None:
        merged["constraints"] = list(constraints)
    if expected_output is not None:
        merged["expected_output"] = list(expected_output)
    return merged


def _extract_runtime_context_from_mapping(payload: Mapping[str, object]) -> dict[str, object]:
    extracted = _extract_runtime_context_from_claim_object(payload)
    if extracted:
        return extracted
    if any(key in payload for key in ("task_text", "project_repo_path", "executor_worktree_path", "instructions_repo_path")):
        return dict(payload)
    return {}


def _extract_runtime_context_from_claim_object(payload: Mapping[str, object]) -> dict[str, object]:
    claim_object = _extract_claim_object(payload)
    for key in ("runtime_context", "dispatch_context", "context", "legacy_input"):
        value = claim_object.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _build_legacy_runtime_context(
    database_path: Path,
    *,
    dispatch_run: DispatchRunPayload,
    role: str,
    merged_context: Mapping[str, object],
    previous_manifest: Mapping[str, object] | None,
) -> LegacyDispatchRuntimeContext:
    manifest_context: dict[str, object] = {}
    manifest_outcome: dict[str, object] = {}
    if previous_manifest is not None:
        runtime_context = previous_manifest.get("runtime_context")
        if isinstance(runtime_context, Mapping):
            manifest_context.update(runtime_context)
        dispatch_outcome = previous_manifest.get("dispatch_outcome")
        if isinstance(dispatch_outcome, Mapping):
            manifest_outcome.update(dispatch_outcome)

    effective_context = dict(manifest_context)
    effective_context.update(dict(merged_context))
    effective_context.setdefault("project", dispatch_run.run.project_key)
    effective_context.setdefault("branch_base", "main")
    effective_context.setdefault("auto_commit", False)
    effective_context.setdefault("source", "control-plane-v2-dispatch")
    effective_context.setdefault("thread_label", f"{dispatch_run.run.project_key}-dispatch")
    effective_context.setdefault("mode", "executor+reviewer")
    effective_context.setdefault("constraints", [])
    effective_context.setdefault("expected_output", [])

    task_text = _required_string(effective_context.get("task_text"), "task_text", database_path)
    project_repo_path = _required_path(effective_context.get("project_repo_path"), "project_repo_path", database_path)
    executor_worktree = _required_path(
        effective_context.get("executor_worktree_path"),
        "executor_worktree_path",
        database_path,
    )
    reviewer_worktree = _optional_path(effective_context.get("reviewer_worktree_path"))
    instructions_repo_path = _required_path(
        effective_context.get("instructions_repo_path"),
        "instructions_repo_path",
        database_path,
    )
    instruction_profile = _required_string(
        effective_context.get("instruction_profile"),
        "instruction_profile",
        database_path,
    )

    raw_overlays = effective_context.get("instruction_overlays") or []
    if not isinstance(raw_overlays, list) and not isinstance(raw_overlays, tuple):
        raise DispatchAdapterError(
            code=RUN_CONTEXT_INVALID,
            message="instruction_overlays must be an array",
            database_path=database_path,
        )
    instruction_overlays = tuple(str(item).strip() for item in raw_overlays if str(item).strip())
    mode = str(effective_context.get("mode") or "executor+reviewer").strip()
    if mode not in {"executor-only", "executor+reviewer"}:
        raise DispatchAdapterError(
            code=RUN_CONTEXT_INVALID,
            message="mode must be 'executor-only' or 'executor+reviewer'",
            database_path=database_path,
            details=f"actual={mode}",
        )
    commit_sha = _string_or_none(effective_context.get("commit_sha")) or _string_or_none(manifest_outcome.get("commit_sha"))
    if role == "reviewer" and commit_sha is None:
        raise DispatchAdapterError(
            code=REVIEWER_HANDOFF_MISSING,
            message=f"Reviewer dispatch requires a persisted executor handoff commit for run {dispatch_run.run.id}",
            database_path=database_path,
        )

    return LegacyDispatchRuntimeContext(
        project=dispatch_run.run.project_key,
        task_text=task_text,
        mode=mode,
        branch_base=str(effective_context["branch_base"]).strip(),
        auto_commit=bool(effective_context.get("auto_commit", False)),
        source=str(effective_context["source"]).strip(),
        thread_label=str(effective_context["thread_label"]).strip(),
        project_repo_path=project_repo_path,
        executor_worktree_path=executor_worktree,
        reviewer_worktree_path=reviewer_worktree,
        instruction_profile=instruction_profile,
        instruction_overlays=instruction_overlays,
        instructions_repo_path=instructions_repo_path,
        constraints=tuple(_coerce_string_list(effective_context.get("constraints"))),
        expected_output=tuple(_coerce_string_list(effective_context.get("expected_output"))),
        commit_sha=commit_sha,
    )


def _resolve_backend_config(
    *,
    control_dir: str | Path | None,
    executor_runner_path: str | Path | None,
    reviewer_runner_path: str | Path | None,
) -> LegacyDispatchBackendConfig:
    resolved_control_dir = Path(control_dir).expanduser().resolve() if control_dir is not None else CONTROL_DIR
    resolved_executor_runner = (
        Path(executor_runner_path).expanduser().resolve()
        if executor_runner_path is not None
        else resolved_control_dir / "scripts" / "run-executor.sh"
    )
    resolved_reviewer_runner = (
        Path(reviewer_runner_path).expanduser().resolve()
        if reviewer_runner_path is not None
        else resolved_control_dir / "scripts" / "run-reviewer.sh"
    )
    return LegacyDispatchBackendConfig(
        control_dir=resolved_control_dir,
        executor_runner_path=resolved_executor_runner,
        reviewer_runner_path=resolved_reviewer_runner,
    )


def _preflight_backend(
    database_path: Path,
    context: LegacyDispatchRuntimeContext,
    role: str,
    backend: LegacyDispatchBackendConfig,
) -> None:
    runner_path = backend.runner_path_for_role(role)
    if not runner_path.is_file():
        raise DispatchAdapterError(
            code=LEGACY_BACKEND_PRECHECK_FAILED,
            message=f"Legacy backend runner does not exist: {runner_path}",
            database_path=database_path,
        )
    if not os.access(runner_path, os.X_OK):
        raise DispatchAdapterError(
            code=LEGACY_BACKEND_PRECHECK_FAILED,
            message=f"Legacy backend runner is not executable: {runner_path}",
            database_path=database_path,
        )
    templates_dir = backend.control_dir / "templates"
    if not templates_dir.is_dir():
        raise DispatchAdapterError(
            code=LEGACY_BACKEND_PRECHECK_FAILED,
            message=f"Legacy control templates directory is missing: {templates_dir}",
            database_path=database_path,
        )
    for command_name in LEGACY_REQUIRED_COMMANDS:
        if shutil.which(command_name) is None:
            raise DispatchAdapterError(
                code=LEGACY_BACKEND_PRECHECK_FAILED,
                message=f"Missing command required by legacy backend: {command_name}",
                database_path=database_path,
            )
    if not context.project_repo_path.is_dir():
        raise DispatchAdapterError(
            code=LEGACY_BACKEND_PRECHECK_FAILED,
            message=f"project_repo_path does not exist: {context.project_repo_path}",
            database_path=database_path,
        )
    if not context.executor_worktree_path.is_dir():
        raise DispatchAdapterError(
            code=LEGACY_BACKEND_PRECHECK_FAILED,
            message=f"executor_worktree_path does not exist: {context.executor_worktree_path}",
            database_path=database_path,
        )
    if role == "reviewer":
        if context.reviewer_worktree_path is None or not context.reviewer_worktree_path.is_dir():
            raise DispatchAdapterError(
                code=LEGACY_BACKEND_PRECHECK_FAILED,
                message="reviewer_worktree_path is required for reviewer dispatch",
                database_path=database_path,
            )
        assert context.commit_sha is not None
        try:
            subprocess.run(
                ["git", "-C", str(context.reviewer_worktree_path), "rev-parse", "--verify", f"{context.commit_sha}^{{commit}}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise DispatchAdapterError(
                code=LEGACY_BACKEND_PRECHECK_FAILED,
                message=f"reviewer_worktree_path cannot resolve commit_sha {context.commit_sha}",
                database_path=database_path,
                details=str(exc),
            ) from exc
    if not context.instructions_repo_path.is_dir():
        raise DispatchAdapterError(
            code=LEGACY_BACKEND_PRECHECK_FAILED,
            message=f"instructions_repo_path does not exist: {context.instructions_repo_path}",
            database_path=database_path,
        )
    profile_dir = context.instructions_repo_path / "profiles" / context.instruction_profile
    if not profile_dir.is_dir():
        raise DispatchAdapterError(
            code=LEGACY_BACKEND_PRECHECK_FAILED,
            message=f"instruction_profile does not exist in instructions repo: {profile_dir}",
            database_path=database_path,
        )


def _prepare_preflight_paths(run_artifact_directory: Path | None, role: str) -> DispatchAttemptPaths:
    if run_artifact_directory is None:
        failure_directory = CONTROL_DIR / ".logs" / "dispatch-failures" / role / _slug_timestamp()
    else:
        failure_directory = run_artifact_directory / "dispatch-failures" / role / _slug_timestamp()
    failure_directory.mkdir(parents=True, exist_ok=True)
    return DispatchAttemptPaths(
        run_artifact_directory=run_artifact_directory,
        attempt_directory=failure_directory,
        context_manifest_path=failure_directory / "dispatch-context.json",
        result_manifest_path=failure_directory / "dispatch-result.json",
        stdout_log_path=failure_directory / "stdout.log",
        stderr_log_path=failure_directory / "stderr.log",
        failure_manifest_path=failure_directory / "dispatch-failure.json",
    )


def _prepare_attempt_paths(run_artifact_directory: Path | None, role: str, step_run_id: str) -> DispatchAttemptPaths:
    attempt_directory = (
        run_artifact_directory / "step_runs" / step_run_id
        if run_artifact_directory is not None
        else CONTROL_DIR / ".logs" / "step-runs" / step_run_id
    )
    attempt_directory.mkdir(parents=True, exist_ok=True)
    return DispatchAttemptPaths(
        run_artifact_directory=run_artifact_directory,
        attempt_directory=attempt_directory,
        context_manifest_path=attempt_directory / "dispatch-context.json",
        result_manifest_path=attempt_directory / "dispatch-result.json",
        stdout_log_path=attempt_directory / "stdout.log",
        stderr_log_path=attempt_directory / "stderr.log",
        failure_manifest_path=attempt_directory / "dispatch-failure.json",
        legacy_control_directory=attempt_directory / "legacy-control",
        legacy_runtime_directory=attempt_directory / "legacy-runtime",
    )


def _materialize_legacy_control_sandbox(
    attempt_paths: DispatchAttemptPaths,
    *,
    dispatch_run: DispatchRunPayload,
    role: str,
    context: LegacyDispatchRuntimeContext,
    backend: LegacyDispatchBackendConfig,
) -> None:
    assert attempt_paths.legacy_control_directory is not None
    assert attempt_paths.legacy_runtime_directory is not None

    control_dir = attempt_paths.legacy_control_directory
    runtime_dir = attempt_paths.legacy_runtime_directory
    inbox_dir = control_dir / "inbox"
    state_dir = control_dir / "state"
    outbox_dir = control_dir / "outbox"

    inbox_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    outbox_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    _ensure_symlink(backend.control_dir / "scripts", control_dir / "scripts")
    _ensure_symlink(backend.control_dir / "templates", control_dir / "templates")

    now_iso = _utc_now()
    state_payload = {
        "run_id": dispatch_run.run.id,
        "project": context.project,
        "mode": context.mode,
        "status": "queued" if role == "executor" else "executor_done",
        "branch_base": context.branch_base,
        "auto_commit": context.auto_commit,
        "source": context.source,
        "thread_label": context.thread_label,
        "instruction_profile": context.instruction_profile,
        "instruction_overlays": list(context.instruction_overlays),
        "instructions_repo_path": str(context.instructions_repo_path),
        "instructions_revision": None,
        "resolved_instruction_files": [],
        "paths": {
            "task_file": "inbox/current-task.md",
            "active_outbox_dir": "outbox",
            "run_dir": str(runtime_dir),
            "executor_worktree": str(context.executor_worktree_path),
            "reviewer_worktree": str(context.reviewer_worktree_path) if context.reviewer_worktree_path else "",
            "project_dir": str(context.project_repo_path),
        },
        "timestamps": {
            "created_at": now_iso,
            "updated_at": now_iso,
            "executor_started_at": None,
            "executor_finished_at": None,
            "reviewer_started_at": None,
            "reviewer_finished_at": None,
        },
        "result": {
            "verdict": None,
            "commit_sha": context.commit_sha,
            "summary": None,
            "error": None,
        },
        "last_error": "",
        "next_action": "dispatch_started",
    }
    _write_json(state_dir / "current.json", state_payload)
    _write_json(runtime_dir / "state.json", state_payload)
    _write_text(inbox_dir / "current-task.md", _render_task_markdown(dispatch_run.run.id, context))
    for artifact_name in ("executor-last-message.md", "executor-report.md", "reviewer-report.md"):
        _write_text(outbox_dir / artifact_name, "")


def _run_legacy_backend(
    *,
    role: str,
    backend: LegacyDispatchBackendConfig,
    attempt_paths: DispatchAttemptPaths,
) -> tuple[bool, int]:
    assert attempt_paths.legacy_control_directory is not None
    runner_name = backend.runner_path_for_role(role).name
    runner_path = attempt_paths.legacy_control_directory / "scripts" / runner_name
    env = os.environ.copy()
    if role == "reviewer":
        env["CONTROL_REVIEWER_SKIP_COMPLETION"] = "1"
    with attempt_paths.stdout_log_path.open("w", encoding="utf-8") as stdout_handle, attempt_paths.stderr_log_path.open("w", encoding="utf-8") as stderr_handle:
        completed = subprocess.run(
            [str(runner_path)],
            cwd=str(attempt_paths.legacy_control_directory),
            env=env,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
            check=False,
        )
    return True, completed.returncode


def _build_dispatch_outcome(
    *,
    backend_started: bool,
    backend_exit_code: int | None,
    sandbox_state: Mapping[str, object],
) -> DispatchExecutionOutcome:
    state_status = _string_or_none(sandbox_state.get("status"))
    state_result = sandbox_state.get("result") if isinstance(sandbox_state.get("result"), Mapping) else None
    commit_sha = _string_or_none(state_result.get("commit_sha")) if state_result else None
    if not backend_started:
        return DispatchExecutionOutcome(
            backend_started=False,
            backend_exit_code=backend_exit_code,
            technical_success=False,
            step_terminal_status=None,
            commit_sha=commit_sha,
            state_status=state_status,
            state_result=dict(state_result) if state_result else None,
        )
    terminal_status = "succeeded" if backend_exit_code == 0 else "failed"
    return DispatchExecutionOutcome(
        backend_started=True,
        backend_exit_code=backend_exit_code,
        technical_success=backend_exit_code == 0,
        step_terminal_status=terminal_status,
        commit_sha=commit_sha,
        state_status=state_status,
        state_result=dict(state_result) if state_result else None,
    )


def _rollback_started_step_run(database_path: Path, step_run_id: str, *, pre_start_run_status: str) -> None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("runs", "step_runs", "state_transitions"))
        step_row = connection.execute(
            """
            SELECT id, run_id
            FROM step_runs
            WHERE id = ? AND status = ?
            """,
            (step_run_id, STEP_RUN_ACTIVE_STATUS),
        ).fetchone()
        if step_row is None:
            return
        rollback_at = _utc_now()
        try:
            connection.execute("BEGIN")
            connection.execute("DELETE FROM state_transitions WHERE step_run_id = ?", (step_run_id,))
            connection.execute("DELETE FROM step_runs WHERE id = ?", (step_run_id,))
            if pre_start_run_status == "queued":
                connection.execute(
                    """
                    UPDATE runs
                    SET status = 'queued', started_at = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (rollback_at, step_row["run_id"]),
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
                    VALUES (?, 'run', ?, NULL, NULL, 'running', 'queued', ?, NULL, ?, ?)
                    """,
                    (
                        generate_opaque_id(),
                        step_row["run_id"],
                        PROVISIONAL_RUN_REQUEUE_ROLLBACK_TRANSITION_TYPE,
                        json.dumps({"rolled_back_step_run_id": step_run_id}, sort_keys=True),
                        rollback_at,
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()


def _mark_dispatch_failed(
    database_path: Path,
    dispatch_run: DispatchRunPayload,
    *,
    reason_code: str,
    note: str | None,
) -> ClaimedRunMutationResult:
    try:
        return mark_claimed_run_dispatch_failed(
            database_path,
            run_id=dispatch_run.run.id,
            reason_code=reason_code,
            note=note,
        )
    except SchedulerPersistenceError as exc:
        raise DispatchAdapterError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _record_dispatch_artifacts(
    database_path: Path,
    dispatch_run: DispatchRunPayload,
    *,
    step_run_id: str | None,
    role: str | None,
    attempt_paths: DispatchAttemptPaths,
) -> tuple[tuple[str, ...], tuple[DispatchArtifactRecord, ...]]:
    warnings: list[str] = []
    records: list[DispatchArtifactRecord] = []
    candidates: list[tuple[str, Path]] = []

    if attempt_paths.context_manifest_path.is_file():
        candidates.append((ARTIFACT_KIND_DISPATCH_CONTEXT_MANIFEST, attempt_paths.context_manifest_path))
    if attempt_paths.result_manifest_path.is_file():
        candidates.append((ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST, attempt_paths.result_manifest_path))
    if attempt_paths.failure_manifest_path.is_file():
        candidates.append((ARTIFACT_KIND_DISPATCH_FAILURE_MANIFEST, attempt_paths.failure_manifest_path))
    if attempt_paths.stdout_log_path.is_file():
        candidates.append((ARTIFACT_KIND_STDOUT_LOG, attempt_paths.stdout_log_path))
    if attempt_paths.stderr_log_path.is_file():
        candidates.append((ARTIFACT_KIND_STDERR_LOG, attempt_paths.stderr_log_path))

    if attempt_paths.legacy_runtime_directory is not None:
        if role is not None:
            resolved_manifest_path = attempt_paths.legacy_runtime_directory / f"resolved-{role}-instructions.json"
            if resolved_manifest_path.is_file():
                candidates.append((ARTIFACT_KIND_RESOLVED_CONTEXT_MANIFEST, resolved_manifest_path))
        state_path = attempt_paths.legacy_runtime_directory / "state.json"
        if state_path.is_file():
            candidates.append((ARTIFACT_KIND_STEP_STATE_JSON, state_path))
        result_path = attempt_paths.legacy_runtime_directory / "result.json"
        if result_path.is_file():
            candidates.append((ARTIFACT_KIND_STEP_RESULT_JSON, result_path))
        runtime_outbox = attempt_paths.legacy_runtime_directory / "outbox"
        if runtime_outbox.is_dir() and role is not None:
            report_name = "executor-report.md" if role == "executor" else "reviewer-report.md"
            report_path = runtime_outbox / report_name
            if report_path.is_file():
                candidates.append((ARTIFACT_KIND_STEP_REPORT, report_path))
            last_message_name = "executor-last-message.md" if role == "executor" else "reviewer-last-message.md"
            last_message_path = runtime_outbox / last_message_name
            if last_message_path.is_file():
                candidates.append((ARTIFACT_KIND_LAST_MESSAGE, last_message_path))

    if attempt_paths.legacy_control_directory is not None and role is not None:
        sandbox_state = _read_json_optional(attempt_paths.legacy_control_directory / "state" / "current.json") or {}
        paths_payload = sandbox_state.get("paths")
        worktree_key = "executor_worktree" if role == "executor" else "reviewer_worktree"
        worktree_value = (
            (paths_payload.get(worktree_key) or "").strip()
            if isinstance(paths_payload, Mapping)
            else ""
        )
        if worktree_value:
            prompt_source = Path(worktree_value) / ".codex-run" / ("executor-prompt.md" if role == "executor" else "reviewer-prompt.md")
            if prompt_source.is_file():
                prompt_copy_path = attempt_paths.attempt_directory / prompt_source.name
                shutil.copy2(prompt_source, prompt_copy_path)
                candidates.append((ARTIFACT_KIND_PROMPT_COPY, prompt_copy_path))

    if not candidates:
        return tuple(warnings), tuple(records)

    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("artifact_refs",))
        for artifact_kind, filesystem_path in candidates:
            try:
                records.append(
                    _insert_artifact_ref(
                        connection,
                        dispatch_run=dispatch_run,
                        step_run_id=step_run_id,
                        artifact_kind=artifact_kind,
                        filesystem_path=filesystem_path,
                    )
                )
            except sqlite3.Error as exc:
                warnings.append(f"artifact_ref insert failed for {filesystem_path}: {exc}")
        if records:
            connection.commit()
    except sqlite3.Error as exc:
        warnings.append(f"artifact recording failed: {exc}")
    finally:
        connection.close()
    return tuple(warnings), tuple(records)


def _insert_artifact_ref(
    connection: sqlite3.Connection,
    *,
    dispatch_run: DispatchRunPayload,
    step_run_id: str | None,
    artifact_kind: str,
    filesystem_path: Path,
) -> DispatchArtifactRecord:
    resolved_path = filesystem_path.expanduser().resolve()
    created_at = _utc_now()
    media_type = mimetypes.guess_type(str(resolved_path))[0]
    size_bytes = resolved_path.stat().st_size
    checksum_sha256 = _sha256_for_path(resolved_path)
    artifact_id = generate_opaque_id()
    connection.execute(
        """
        INSERT INTO artifact_refs (
          id,
          project_id,
          flow_id,
          run_id,
          step_run_id,
          artifact_kind,
          filesystem_path,
          media_type,
          size_bytes,
          checksum_sha256,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            artifact_id,
            dispatch_run.project.id,
            dispatch_run.flow_context.flow_id,
            dispatch_run.run.id,
            step_run_id,
            artifact_kind,
            str(resolved_path),
            media_type,
            size_bytes,
            checksum_sha256,
            created_at,
        ),
    )
    return DispatchArtifactRecord(
        id=artifact_id,
        project_id=dispatch_run.project.id,
        flow_id=dispatch_run.flow_context.flow_id,
        run_id=dispatch_run.run.id,
        step_run_id=step_run_id,
        artifact_kind=artifact_kind,
        filesystem_path=resolved_path,
        media_type=media_type,
        size_bytes=size_bytes,
        checksum_sha256=checksum_sha256,
        created_at=created_at,
    )


def _load_latest_dispatch_result_manifest(
    database_path: Path,
    run_id: str,
    *,
    step_key: str | None,
) -> Mapping[str, object] | None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("artifact_refs", "step_runs"))
        filters = ["artifact_refs.run_id = ?", "artifact_refs.artifact_kind = ?"]
        params: list[object] = [run_id, ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST]
        if step_key is not None:
            filters.append("step_runs.step_key = ?")
            params.append(step_key)
        row = connection.execute(
            f"""
            SELECT artifact_refs.filesystem_path
            FROM artifact_refs
            LEFT JOIN step_runs ON step_runs.id = artifact_refs.step_run_id
            WHERE {" AND ".join(filters)}
            ORDER BY artifact_refs.created_at DESC, artifact_refs.id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
        if row is None:
            return None
        return _read_json_optional(Path(row["filesystem_path"]))
    except sqlite3.Error:
        return None
    finally:
        connection.close()


def _resolve_run_artifact_directory(artifact_root: str | Path, dispatch_run: DispatchRunPayload) -> Path:
    root = Path(artifact_root).expanduser().resolve()
    run_artifact_directory = root / dispatch_run.run.project_key / dispatch_run.run.flow_id / dispatch_run.run.id
    run_artifact_directory.mkdir(parents=True, exist_ok=True)
    return run_artifact_directory


def _render_task_markdown(run_id: str, context: LegacyDispatchRuntimeContext) -> str:
    return (
        f"---\n"
        f"run_id: {run_id}\n"
        f"project: {context.project}\n"
        f"mode: {context.mode}\n"
        f"branch_base: {context.branch_base}\n"
        f"auto_commit: {'true' if context.auto_commit else 'false'}\n"
        f"source: {context.source}\n"
        f"thread_label: {context.thread_label}\n"
        f"instruction_profile: {context.instruction_profile}\n"
        f"instruction_overlays: {json.dumps(list(context.instruction_overlays), ensure_ascii=False)}\n"
        f"instructions_repo_path: {context.instructions_repo_path}\n"
        f"project_repo_path: {context.project_repo_path}\n"
        f"executor_worktree_path: {context.executor_worktree_path}\n"
        f"reviewer_worktree_path: {context.reviewer_worktree_path or ''}\n"
        f"---\n\n"
        f"# Task\n\n"
        f"{context.task_text}\n\n"
        f"# Constraints\n\n"
        f"{_render_markdown_list(context.constraints)}\n\n"
        f"# Expected output\n\n"
        f"{_render_markdown_list(context.expected_output)}\n"
    )


def _render_markdown_list(items: tuple[str, ...]) -> str:
    if not items:
        return "- none"
    return "\n".join(f"- {item}" for item in items)


def _ensure_symlink(source: Path, target: Path) -> None:
    if target.exists() or target.is_symlink():
        return
    target.symlink_to(source, target_is_directory=source.is_dir())


def _load_run_details(database_path: Path, run_id: str) -> RunDetails:
    from .run_persistence import get_run

    try:
        return get_run(database_path, run_id)
    except RunPersistenceError as exc:
        raise DispatchAdapterError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _required_string(value: object, field_name: str, database_path: Path) -> str:
    normalized = _string_or_none(value)
    if normalized is None:
        raise DispatchAdapterError(
            code=RUN_CONTEXT_INVALID,
            message=f"{field_name} is required to launch the legacy backend",
            database_path=database_path,
        )
    return normalized


def _required_path(value: object, field_name: str, database_path: Path) -> Path:
    normalized = _required_string(value, field_name, database_path)
    return Path(normalized).expanduser().resolve()


def _optional_path(value: object) -> Path | None:
    normalized = _string_or_none(value)
    return Path(normalized).expanduser().resolve() if normalized is not None else None


def _coerce_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list) or isinstance(value, tuple):
        return [str(item) for item in value]
    return [str(value)]


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _slug_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json_optional(path: Path) -> dict[str, object] | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _sha256_for_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
