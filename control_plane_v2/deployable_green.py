from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
from pathlib import Path
import sqlite3

from .host_checks import HostCheckError, HostCheckSummary, show_host_check_results
from .id_generation import generate_opaque_id
from .runtime_event_journal import RuntimeEventAppendRequest, insert_runtime_event_in_connection
from .reviewer_outcome_persistence import (
    PROVISIONAL_REVIEWER_APPROVED_TRANSITION_TYPE,
    PROVISIONAL_REVIEWER_BLOCKED_TRANSITION_TYPE,
    PROVISIONAL_REVIEWER_CHANGES_REQUESTED_STOPPED_TRANSITION_TYPE,
    PROVISIONAL_REVIEWER_CHANGES_REQUESTED_TRANSITION_TYPE,
    REVIEWER_VERDICTS,
)
from .run_persistence import RunDetails, RunPersistenceError, _connect_run_db, _ensure_required_tables, _resolve_database_path, get_run


CONTROL_DIR = Path(__file__).resolve().parents[1]

DEPLOYABLE_GREEN_STATUSES = ("deployable_green", "not_green", "blocked")
HOST_CHECK_VERDICTS = ("green", "not_green", "blocked")

ARTIFACT_KIND_DEPLOYABLE_GREEN_DECISION_MANIFEST = "deployable_green_decision_manifest"

DEPLOYABLE_GREEN_NOT_FOUND = "DEPLOYABLE_GREEN_NOT_FOUND"
DEPLOYABLE_GREEN_REQUEST_INVALID = "DEPLOYABLE_GREEN_REQUEST_INVALID"
DEPLOYABLE_GREEN_RUN_SCOPE_INVALID = "DEPLOYABLE_GREEN_RUN_SCOPE_INVALID"
DEPLOYABLE_GREEN_STORAGE_ERROR = "DEPLOYABLE_GREEN_STORAGE_ERROR"

_REVIEWER_TRANSITION_TO_VERDICT = {
    PROVISIONAL_REVIEWER_APPROVED_TRANSITION_TYPE: "approved",
    PROVISIONAL_REVIEWER_BLOCKED_TRANSITION_TYPE: "blocked",
    PROVISIONAL_REVIEWER_CHANGES_REQUESTED_TRANSITION_TYPE: "changes_requested",
    PROVISIONAL_REVIEWER_CHANGES_REQUESTED_STOPPED_TRANSITION_TYPE: "changes_requested",
}

DEPLOYABLE_GREEN_DECISION_TABLE = (
    {
        "rule": "reviewer_missing",
        "reviewer_verdict": "missing_or_invalid",
        "host_checks_verdict": "*",
        "decision_status": "blocked",
        "reason": "reviewer outcome state is required before a formal deployable-green decision can be made",
    },
    {
        "rule": "reviewer_changes_requested",
        "reviewer_verdict": "changes_requested",
        "host_checks_verdict": "*",
        "decision_status": "not_green",
        "reason": "reviewer did not approve the run",
    },
    {
        "rule": "reviewer_blocked",
        "reviewer_verdict": "blocked",
        "host_checks_verdict": "*",
        "decision_status": "blocked",
        "reason": "reviewer explicitly blocked the run",
    },
    {
        "rule": "host_checks_missing",
        "reviewer_verdict": "approved",
        "host_checks_verdict": "missing_or_invalid",
        "decision_status": "blocked",
        "reason": "latest host-side checks result is required before a formal deployable-green decision can be made",
    },
    {
        "rule": "host_checks_not_green",
        "reviewer_verdict": "approved",
        "host_checks_verdict": "not_green",
        "decision_status": "not_green",
        "reason": "latest host-side checks result contains required failures",
    },
    {
        "rule": "host_checks_blocked",
        "reviewer_verdict": "approved",
        "host_checks_verdict": "blocked",
        "decision_status": "blocked",
        "reason": "latest host-side checks result is blocked",
    },
    {
        "rule": "deployable_green",
        "reviewer_verdict": "approved",
        "host_checks_verdict": "green",
        "decision_status": "deployable_green",
        "reason": "reviewer approved and latest host-side checks are green",
    },
)


@dataclass(frozen=True)
class ReviewerVerdictSource:
    verdict: str | None
    source_kind: str
    source_ref: str | None
    created_at: str | None
    reviewer_step_run_id: str | None
    summary_text: str | None
    stop_reason_code: str | None
    stop_reason: str | None
    error: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "verdict": self.verdict,
            "source_kind": self.source_kind,
            "source_ref": self.source_ref,
            "created_at": self.created_at,
            "reviewer_step_run_id": self.reviewer_step_run_id,
            "summary_text": self.summary_text,
            "stop_reason_code": self.stop_reason_code,
            "stop_reason": self.stop_reason,
            "error": self.error,
        }


@dataclass(frozen=True)
class HostChecksVerdictSource:
    verdict: str | None
    source_kind: str
    source_ref: str | None
    created_at: str | None
    check_run_id: str | None
    manifest_path: Path | None
    summary: HostCheckSummary | None
    error: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "verdict": self.verdict,
            "source_kind": self.source_kind,
            "source_ref": self.source_ref,
            "created_at": self.created_at,
            "check_run_id": self.check_run_id,
            "manifest_path": str(self.manifest_path) if self.manifest_path is not None else None,
            "summary": self.summary.to_dict() if self.summary is not None else None,
            "error": self.error,
        }


@dataclass(frozen=True)
class DeployableGreenArtifact:
    artifact_kind: str
    filesystem_path: Path
    created_at: str
    artifact_ref_id: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_kind": self.artifact_kind,
            "filesystem_path": str(self.filesystem_path),
            "created_at": self.created_at,
            "artifact_ref_id": self.artifact_ref_id,
        }


@dataclass(frozen=True)
class DeployableGreenDecisionResult:
    decision_id: str
    created_at: str
    decision_status: str
    project_key: str
    package_root: Path
    project_profile: str
    workflow_id: str
    run_id: str
    flow_id: str
    summary: str
    rationale: str
    next_action_hint: str | None
    reviewer_verdict_source: ReviewerVerdictSource
    host_checks_verdict_source: HostChecksVerdictSource
    manifest: dict[str, object]
    manifest_path: Path
    artifacts: tuple[DeployableGreenArtifact, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_id": self.decision_id,
            "created_at": self.created_at,
            "decision_status": self.decision_status,
            "project_key": self.project_key,
            "package_root": str(self.package_root),
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "summary": self.summary,
            "rationale": self.rationale,
            "next_action_hint": self.next_action_hint,
            "reviewer_verdict_source": self.reviewer_verdict_source.to_dict(),
            "host_checks_verdict_source": self.host_checks_verdict_source.to_dict(),
            "manifest": self.manifest,
            "manifest_path": str(self.manifest_path),
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }


@dataclass(frozen=True)
class DeployableGreenDecisionRecord:
    decision_id: str
    created_at: str
    decision_status: str
    project_key: str
    project_profile: str
    workflow_id: str
    run_id: str
    flow_id: str
    summary: str
    rationale: str
    next_action_hint: str | None
    reviewer_verdict_source: ReviewerVerdictSource
    host_checks_verdict_source: HostChecksVerdictSource
    manifest_path: Path
    manifest: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_id": self.decision_id,
            "created_at": self.created_at,
            "decision_status": self.decision_status,
            "project_key": self.project_key,
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "summary": self.summary,
            "rationale": self.rationale,
            "next_action_hint": self.next_action_hint,
            "reviewer_verdict_source": self.reviewer_verdict_source.to_dict(),
            "host_checks_verdict_source": self.host_checks_verdict_source.to_dict(),
            "manifest_path": str(self.manifest_path),
            "manifest": self.manifest,
        }


@dataclass(frozen=True)
class DeployableGreenDecisionInspection:
    run: RunDetails
    latest_decision: DeployableGreenDecisionRecord | None
    history: tuple[DeployableGreenDecisionRecord, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "run": self.run.to_dict(),
            "latest_decision": self.latest_decision.to_dict() if self.latest_decision is not None else None,
            "history": [record.to_dict() for record in self.history],
            "decision_table": list(DEPLOYABLE_GREEN_DECISION_TABLE),
        }


@dataclass(frozen=True)
class _DecisionEvaluation:
    decision_status: str
    summary: str
    rationale: str
    next_action_hint: str | None
    rule: str


class DeployableGreenError(Exception):
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


def decide_deployable_green(
    database_path: str | Path,
    request_payload: Mapping[str, object],
) -> DeployableGreenDecisionResult:
    resolved_db_path = _resolve_database_path(database_path)
    request = _normalize_request(request_payload, resolved_db_path)
    run_details = _load_run_details_or_raise(resolved_db_path, request["run_id"])
    _validate_run_scope(run_details, request["flow_id"], resolved_db_path)

    created_at = _utc_now()
    decision_id = generate_opaque_id()
    reviewer_source = _load_reviewer_verdict_source(run_details)
    host_checks_source = _load_host_checks_verdict_source(resolved_db_path, run_details.run.id)
    evaluation = _evaluate_decision(reviewer_source, host_checks_source)
    manifest_path = _resolve_manifest_path(
        explicit_artifact_root=request["artifact_root"],
        database_path=resolved_db_path,
        run_details=run_details,
        decision_id=decision_id,
    )

    manifest = {
        "decision_id": decision_id,
        "created_at": created_at,
        "decision": {
            "decision_status": evaluation.decision_status,
            "summary": evaluation.summary,
            "rationale": evaluation.rationale,
            "next_action_hint": evaluation.next_action_hint,
            "rule": evaluation.rule,
        },
        "scope": {
            "project_key": run_details.run.project_key,
            "package_root": str(run_details.run.package_root),
            "project_profile": run_details.run.project_profile,
            "workflow_id": run_details.run.workflow_id,
            "run_id": run_details.run.id,
            "flow_id": run_details.run.flow_id,
        },
        "inputs": {
            "reviewer_verdict_source": reviewer_source.to_dict(),
            "host_checks_verdict_source": host_checks_source.to_dict(),
        },
        "decision_table": list(DEPLOYABLE_GREEN_DECISION_TABLE),
        "notes": {
            "workflow": "reviewer-approved -> run host checks -> decide deployable green",
            "boundary": "v1 is host-side only and does not perform rollout or deployment orchestration",
        },
    }

    _write_json(manifest_path, manifest)
    _insert_decision_row(
        resolved_db_path,
        decision_id=decision_id,
        run_details=run_details,
        decision_status=evaluation.decision_status,
        reviewer_verdict_source=reviewer_source,
        host_checks_verdict_source=host_checks_source,
        summary=evaluation.summary,
        rationale=evaluation.rationale,
        next_action_hint=evaluation.next_action_hint,
        manifest_path=manifest_path,
        created_at=created_at,
        decision_rule=evaluation.rule,
    )
    artifacts = _record_decision_artifacts(
        resolved_db_path,
        run_details=run_details,
        created_at=created_at,
        artifact_paths=((ARTIFACT_KIND_DEPLOYABLE_GREEN_DECISION_MANIFEST, manifest_path),),
    )

    return DeployableGreenDecisionResult(
        decision_id=decision_id,
        created_at=created_at,
        decision_status=evaluation.decision_status,
        project_key=run_details.run.project_key,
        package_root=run_details.run.package_root,
        project_profile=run_details.run.project_profile,
        workflow_id=run_details.run.workflow_id,
        run_id=run_details.run.id,
        flow_id=run_details.run.flow_id,
        summary=evaluation.summary,
        rationale=evaluation.rationale,
        next_action_hint=evaluation.next_action_hint,
        reviewer_verdict_source=reviewer_source,
        host_checks_verdict_source=host_checks_source,
        manifest=manifest,
        manifest_path=manifest_path,
        artifacts=artifacts,
    )


def show_deployable_green_decision(
    database_path: str | Path,
    run_id: str,
    *,
    limit: int = 20,
) -> DeployableGreenDecisionInspection:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_run_id = _require_text("run_id", run_id, resolved_db_path)
    if limit <= 0:
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_REQUEST_INVALID,
            message="limit must be greater than zero",
            database_path=resolved_db_path,
        )
    run_details = _load_run_details_or_raise(resolved_db_path, normalized_run_id)

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_tables(connection, resolved_db_path, ("green_decisions",))
        rows = connection.execute(
            """
            SELECT
              green_decisions.id,
              green_decisions.decision_status,
              green_decisions.created_at,
              green_decisions.workflow_id,
              green_decisions.project_profile,
              green_decisions.run_id,
              green_decisions.flow_id,
              green_decisions.reviewer_verdict,
              green_decisions.reviewer_source_kind,
              green_decisions.reviewer_source_ref,
              green_decisions.reviewer_created_at,
              green_decisions.reviewer_step_run_id,
              green_decisions.host_checks_verdict,
              green_decisions.host_checks_source_kind,
              green_decisions.host_checks_source_ref,
              green_decisions.host_checks_created_at,
              green_decisions.host_check_run_id,
              green_decisions.summary_text,
              green_decisions.rationale_text,
              green_decisions.next_action_hint,
              green_decisions.manifest_json_path,
              projects.project_key
            FROM green_decisions
            JOIN projects ON projects.id = green_decisions.project_id
            WHERE green_decisions.run_id = ?
            ORDER BY green_decisions.created_at DESC, green_decisions.id DESC
            LIMIT ?
            """,
            (normalized_run_id, limit),
        ).fetchall()
    except sqlite3.Error as exc:
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_STORAGE_ERROR,
            message="Failed to load deployable-green decision history",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()

    history = tuple(_row_to_decision_record(row, resolved_db_path) for row in rows)
    return DeployableGreenDecisionInspection(
        run=run_details,
        latest_decision=history[0] if history else None,
        history=history,
    )


def _normalize_request(payload: Mapping[str, object], database_path: Path) -> dict[str, object]:
    request = dict(payload)
    return {
        "run_id": _require_text("run_id", request.get("run_id"), database_path),
        "flow_id": _optional_text(request.get("flow_id")),
        "artifact_root": _optional_path(request.get("artifact_root")),
    }


def _load_run_details_or_raise(database_path: Path, run_id: str) -> RunDetails:
    try:
        return get_run(database_path, run_id)
    except RunPersistenceError as exc:
        raise DeployableGreenError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _ensure_tables(connection: sqlite3.Connection, database_path: Path, required_tables: tuple[str, ...]) -> None:
    try:
        _ensure_required_tables(connection, database_path, required_tables)
    except RunPersistenceError as exc:
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_STORAGE_ERROR,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _validate_run_scope(run_details: RunDetails, requested_flow_id: str | None, database_path: Path) -> None:
    if requested_flow_id is None:
        return
    if run_details.run.flow_id != requested_flow_id:
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_RUN_SCOPE_INVALID,
            message="flow_id does not belong to the supplied run_id",
            database_path=database_path,
            details=f"run_flow_id={run_details.run.flow_id} requested_flow_id={requested_flow_id}",
        )


def _load_reviewer_verdict_source(run_details: RunDetails) -> ReviewerVerdictSource:
    snapshot_issues: list[str] = []
    reviewer_snapshots = [
        snapshot
        for snapshot in run_details.run_snapshots
        if snapshot.snapshot_scope == "run" and snapshot.run_id == run_details.run.id
    ]
    for snapshot in reversed(reviewer_snapshots):
        payload = _parse_json_object(snapshot.snapshot_json)
        if payload is None:
            snapshot_issues.append(f"invalid reviewer snapshot json: {snapshot.id}")
            continue
        if _optional_text(payload.get("kind")) != "reviewer_outcome":
            continue
        verdict = _optional_text(payload.get("verdict"))
        if verdict not in REVIEWER_VERDICTS:
            snapshot_issues.append(f"invalid reviewer snapshot verdict: {snapshot.id}")
            continue
        return ReviewerVerdictSource(
            verdict=verdict,
            source_kind="run_snapshot.reviewer_outcome",
            source_ref=snapshot.id,
            created_at=snapshot.created_at,
            reviewer_step_run_id=_optional_text(payload.get("reviewer_step_run_id")),
            summary_text=_optional_text(payload.get("summary_text")),
            stop_reason_code=_optional_text(payload.get("stop_reason_code")),
            stop_reason=_optional_text(payload.get("stop_reason")),
            error=None,
        )

    transition_issues: list[str] = []
    for transition in reversed(run_details.state_transitions):
        if transition.entity_type != "run":
            continue
        default_verdict = _REVIEWER_TRANSITION_TO_VERDICT.get(transition.transition_type)
        if default_verdict is None:
            continue
        metadata = _parse_json_object(transition.metadata_json)
        if transition.metadata_json is not None and metadata is None:
            transition_issues.append(f"invalid reviewer transition metadata: {transition.id}")
            continue
        verdict = _optional_text(metadata.get("verdict")) if metadata is not None else None
        if verdict is None:
            verdict = default_verdict
        if verdict not in REVIEWER_VERDICTS:
            transition_issues.append(f"invalid reviewer transition verdict: {transition.id}")
            continue
        return ReviewerVerdictSource(
            verdict=verdict,
            source_kind="state_transition.reviewer_outcome",
            source_ref=transition.id,
            created_at=transition.created_at,
            reviewer_step_run_id=_optional_text(metadata.get("reviewer_step_run_id")) if metadata is not None else None,
            summary_text=_optional_text(metadata.get("summary_text")) if metadata is not None else None,
            stop_reason_code=_optional_text(metadata.get("stop_reason_code")) if metadata is not None else None,
            stop_reason=_optional_text(metadata.get("stop_reason")) if metadata is not None else None,
            error=None,
        )

    all_issues = snapshot_issues + transition_issues
    return ReviewerVerdictSource(
        verdict=None,
        source_kind="missing",
        source_ref=None,
        created_at=None,
        reviewer_step_run_id=None,
        summary_text=None,
        stop_reason_code=None,
        stop_reason=None,
        error="; ".join(all_issues) if all_issues else None,
    )


def _load_host_checks_verdict_source(database_path: Path, run_id: str) -> HostChecksVerdictSource:
    try:
        result = show_host_check_results(database_path, run_id, limit=1)
    except HostCheckError as exc:
        return HostChecksVerdictSource(
            verdict=None,
            source_kind="error",
            source_ref=None,
            created_at=None,
            check_run_id=None,
            manifest_path=None,
            summary=None,
            error=f"{exc.code}: {exc.message}" + (f" ({exc.details})" if exc.details else ""),
        )

    if result.latest_result is None:
        return HostChecksVerdictSource(
            verdict=None,
            source_kind="missing",
            source_ref=None,
            created_at=None,
            check_run_id=None,
            manifest_path=None,
            summary=None,
            error=None,
        )

    latest = result.latest_result
    return HostChecksVerdictSource(
        verdict=latest.verdict,
        source_kind="host_check_runs.latest",
        source_ref=latest.check_run_id,
        created_at=latest.created_at,
        check_run_id=latest.check_run_id,
        manifest_path=latest.manifest_path,
        summary=latest.summary,
        error=None,
    )


def _evaluate_decision(
    reviewer_source: ReviewerVerdictSource,
    host_checks_source: HostChecksVerdictSource,
) -> _DecisionEvaluation:
    if reviewer_source.verdict is None:
        reason = reviewer_source.error or "reviewer outcome state is missing"
        return _DecisionEvaluation(
            decision_status="blocked",
            summary="Reviewer outcome is unavailable",
            rationale=(
                "A formal deployable-green decision requires a persisted reviewer outcome for the target run. "
                f"Current reviewer source is unavailable: {reason}."
            ),
            next_action_hint="Complete reviewer outcome persistence for the run before deciding deployable green.",
            rule="reviewer_missing",
        )

    if reviewer_source.verdict == "changes_requested":
        return _DecisionEvaluation(
            decision_status="not_green",
            summary="Reviewer requested changes",
            rationale=(
                "The latest reviewer verdict for the run is changes_requested, so the path is not reviewer-approved "
                "and cannot be treated as deployable green."
            ),
            next_action_hint="Address reviewer feedback and complete the next review cycle before promotion.",
            rule="reviewer_changes_requested",
        )

    if reviewer_source.verdict == "blocked":
        return _DecisionEvaluation(
            decision_status="blocked",
            summary="Reviewer blocked the run",
            rationale=(
                "The latest reviewer verdict for the run is blocked, which reserves the final decision for blocked "
                "until the reviewer issue is resolved."
            ),
            next_action_hint="Resolve the reviewer blocking issue before promotion.",
            rule="reviewer_blocked",
        )

    assert reviewer_source.verdict == "approved"

    if host_checks_source.verdict is None:
        reason = host_checks_source.error or "no host-side checks have been recorded for the run"
        return _DecisionEvaluation(
            decision_status="blocked",
            summary="Host-side checks are unavailable",
            rationale=(
                "Reviewer approval alone is not sufficient for deployable green in v1. "
                f"The latest host-side checks result is unavailable: {reason}."
            ),
            next_action_hint="Run host-side checks for the run before deciding deployable green.",
            rule="host_checks_missing",
        )

    if host_checks_source.verdict == "not_green":
        return _DecisionEvaluation(
            decision_status="not_green",
            summary="Required host-side checks failed",
            rationale=(
                "Reviewer approved the run, but the latest host-side checks verdict is not_green, which means "
                "at least one required check failed."
            ),
            next_action_hint="Fix the failing required host-side checks and rerun them before promotion.",
            rule="host_checks_not_green",
        )

    if host_checks_source.verdict == "blocked":
        return _DecisionEvaluation(
            decision_status="blocked",
            summary="Host-side checks are blocked",
            rationale=(
                "Reviewer approved the run, but the latest host-side checks verdict is blocked, which indicates "
                "invalid config, missing prerequisites, or an impossible host-side execution path."
            ),
            next_action_hint="Fix the host-check configuration or runtime prerequisites and rerun host-side checks.",
            rule="host_checks_blocked",
        )

    return _DecisionEvaluation(
        decision_status="deployable_green",
        summary="Run is deployable green",
        rationale=(
            "The latest reviewer verdict is approved and the latest host-side checks verdict is green, so the "
            "formal v1 gate marks the run as deployable_green."
        ),
        next_action_hint="Proceed with the explicit deployment or promotion step outside Control Plane v2.",
        rule="deployable_green",
    )


def _resolve_manifest_path(
    *,
    explicit_artifact_root: Path | None,
    database_path: Path,
    run_details: RunDetails,
    decision_id: str,
) -> Path:
    if explicit_artifact_root is not None:
        output_root = (
            explicit_artifact_root
            / run_details.run.project_key
            / run_details.run.flow_id
            / run_details.run.id
            / "green-decisions"
            / decision_id
        )
    else:
        inferred_run_directory = _infer_run_artifact_directory(database_path, run_details.run.id)
        if inferred_run_directory is not None:
            output_root = inferred_run_directory / "green-decisions" / decision_id
        else:
            output_root = (
                CONTROL_DIR
                / ".logs"
                / "deployable-green"
                / run_details.run.project_key
                / run_details.run.flow_id
                / run_details.run.id
                / decision_id
            )
    output_root.mkdir(parents=True, exist_ok=True)
    return output_root / "manifest.json"


def _infer_run_artifact_directory(database_path: Path, run_id: str) -> Path | None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("artifact_refs",))
        row = connection.execute(
            """
            SELECT filesystem_path
            FROM artifact_refs
            WHERE run_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_STORAGE_ERROR,
            message="Failed to infer deployable-green artifact directory",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()

    if row is None:
        return None
    artifact_path = Path(str(row["filesystem_path"])).expanduser().resolve()
    for parent in artifact_path.parents:
        if parent.name == run_id:
            return parent
    return None


def _insert_decision_row(
    database_path: Path,
    *,
    decision_id: str,
    run_details: RunDetails,
    decision_status: str,
    reviewer_verdict_source: ReviewerVerdictSource,
    host_checks_verdict_source: HostChecksVerdictSource,
    summary: str,
    rationale: str,
    next_action_hint: str | None,
    manifest_path: Path,
    created_at: str,
    decision_rule: str,
) -> None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("projects", "runs", "green_decisions"))
        connection.execute("BEGIN")
        connection.execute(
            """
            INSERT INTO green_decisions (
              id,
              project_id,
              flow_id,
              run_id,
              workflow_id,
              project_profile,
              decision_status,
              reviewer_verdict,
              reviewer_source_kind,
              reviewer_source_ref,
              reviewer_created_at,
              reviewer_step_run_id,
              host_checks_verdict,
              host_checks_source_kind,
              host_checks_source_ref,
              host_checks_created_at,
              host_check_run_id,
              summary_text,
              rationale_text,
              next_action_hint,
              manifest_json_path,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                run_details.run.project_id,
                run_details.run.flow_id,
                run_details.run.id,
                run_details.run.workflow_id,
                run_details.run.project_profile,
                decision_status,
                reviewer_verdict_source.verdict,
                reviewer_verdict_source.source_kind,
                reviewer_verdict_source.source_ref,
                reviewer_verdict_source.created_at,
                reviewer_verdict_source.reviewer_step_run_id,
                host_checks_verdict_source.verdict,
                host_checks_verdict_source.source_kind,
                host_checks_verdict_source.source_ref,
                host_checks_verdict_source.created_at,
                host_checks_verdict_source.check_run_id,
                summary,
                rationale,
                next_action_hint,
                str(manifest_path),
                created_at,
            ),
        )
        insert_runtime_event_in_connection(
            connection,
            database_path=database_path,
            request=RuntimeEventAppendRequest(
                event_type="deployable_green_decided",
                entity_type="run",
                entity_id=run_details.run.id,
                project_key=run_details.run.project_key,
                flow_id=run_details.run.flow_id,
                run_id=run_details.run.id,
                severity=(
                    "info"
                    if decision_status == "deployable_green"
                    else ("error" if decision_status == "blocked" else "warning")
                ),
                summary=f"Deployable-green decided for run {run_details.run.id}: {decision_status}",
                payload_redacted={
                    "decision_id": decision_id,
                    "decision_status": decision_status,
                    "rule": decision_rule,
                    "reviewer_verdict": reviewer_verdict_source.verdict,
                    "host_checks_verdict": host_checks_verdict_source.verdict,
                    "next_action_hint": next_action_hint,
                    "manifest_path": manifest_path,
                },
                source_module="deployable_green",
                created_at=created_at,
            ),
        )
        connection.commit()
    except sqlite3.Error as exc:
        connection.rollback()
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_STORAGE_ERROR,
            message=f"Failed to persist deployable-green decision row: {decision_id}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _record_decision_artifacts(
    database_path: Path,
    *,
    run_details: RunDetails,
    created_at: str,
    artifact_paths: Sequence[tuple[str, Path]],
) -> tuple[DeployableGreenArtifact, ...]:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("artifact_refs",))
        connection.execute("BEGIN")
        artifacts: list[DeployableGreenArtifact] = []
        for artifact_kind, filesystem_path in artifact_paths:
            resolved_path = filesystem_path.expanduser().resolve()
            artifact_ref_id = generate_opaque_id()
            media_type = mimetypes.guess_type(str(resolved_path))[0]
            size_bytes = resolved_path.stat().st_size
            checksum_sha256 = _sha256_for_path(resolved_path)
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
                VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_ref_id,
                    run_details.run.project_id,
                    run_details.run.flow_id,
                    run_details.run.id,
                    artifact_kind,
                    str(resolved_path),
                    media_type,
                    size_bytes,
                    checksum_sha256,
                    created_at,
                ),
            )
            artifacts.append(
                DeployableGreenArtifact(
                    artifact_kind=artifact_kind,
                    filesystem_path=resolved_path,
                    created_at=created_at,
                    artifact_ref_id=artifact_ref_id,
                )
            )
        connection.commit()
        return tuple(artifacts)
    except sqlite3.Error as exc:
        connection.rollback()
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_STORAGE_ERROR,
            message="Failed to persist deployable-green decision artifact refs",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _row_to_decision_record(row: sqlite3.Row, database_path: Path) -> DeployableGreenDecisionRecord:
    manifest_path = Path(str(row["manifest_json_path"])).expanduser().resolve()
    manifest = _read_json_required(manifest_path, database_path, str(row["id"]))
    reviewer_source = _reviewer_source_from_manifest_or_row(manifest, row)
    host_checks_source = _host_checks_source_from_manifest_or_row(manifest, row)
    return DeployableGreenDecisionRecord(
        decision_id=str(row["id"]),
        created_at=str(row["created_at"]),
        decision_status=str(row["decision_status"]),
        project_key=str(row["project_key"]),
        project_profile=str(row["project_profile"]),
        workflow_id=str(row["workflow_id"]),
        run_id=str(row["run_id"]),
        flow_id=str(row["flow_id"]),
        summary=str(row["summary_text"]),
        rationale=str(row["rationale_text"]),
        next_action_hint=_optional_text(row["next_action_hint"]),
        reviewer_verdict_source=reviewer_source,
        host_checks_verdict_source=host_checks_source,
        manifest_path=manifest_path,
        manifest=manifest,
    )


def _reviewer_source_from_manifest_or_row(
    manifest: Mapping[str, object],
    row: sqlite3.Row,
) -> ReviewerVerdictSource:
    inputs = manifest.get("inputs")
    if isinstance(inputs, Mapping):
        reviewer_source = inputs.get("reviewer_verdict_source")
        if isinstance(reviewer_source, Mapping):
            return ReviewerVerdictSource(
                verdict=_optional_text(reviewer_source.get("verdict")),
                source_kind=_optional_text(reviewer_source.get("source_kind")) or "unknown",
                source_ref=_optional_text(reviewer_source.get("source_ref")),
                created_at=_optional_text(reviewer_source.get("created_at")),
                reviewer_step_run_id=_optional_text(reviewer_source.get("reviewer_step_run_id")),
                summary_text=_optional_text(reviewer_source.get("summary_text")),
                stop_reason_code=_optional_text(reviewer_source.get("stop_reason_code")),
                stop_reason=_optional_text(reviewer_source.get("stop_reason")),
                error=_optional_text(reviewer_source.get("error")),
            )
    return ReviewerVerdictSource(
        verdict=_optional_text(row["reviewer_verdict"]),
        source_kind=_optional_text(row["reviewer_source_kind"]) or "unknown",
        source_ref=_optional_text(row["reviewer_source_ref"]),
        created_at=_optional_text(row["reviewer_created_at"]),
        reviewer_step_run_id=_optional_text(row["reviewer_step_run_id"]),
        summary_text=None,
        stop_reason_code=None,
        stop_reason=None,
        error=None,
    )


def _host_checks_source_from_manifest_or_row(
    manifest: Mapping[str, object],
    row: sqlite3.Row,
) -> HostChecksVerdictSource:
    inputs = manifest.get("inputs")
    if isinstance(inputs, Mapping):
        host_source = inputs.get("host_checks_verdict_source")
        if isinstance(host_source, Mapping):
            summary_mapping = host_source.get("summary")
            summary = _host_check_summary_from_mapping(summary_mapping) if isinstance(summary_mapping, Mapping) else None
            manifest_path = _optional_path(host_source.get("manifest_path"))
            return HostChecksVerdictSource(
                verdict=_optional_text(host_source.get("verdict")),
                source_kind=_optional_text(host_source.get("source_kind")) or "unknown",
                source_ref=_optional_text(host_source.get("source_ref")),
                created_at=_optional_text(host_source.get("created_at")),
                check_run_id=_optional_text(host_source.get("check_run_id")),
                manifest_path=manifest_path,
                summary=summary,
                error=_optional_text(host_source.get("error")),
            )
    return HostChecksVerdictSource(
        verdict=_optional_text(row["host_checks_verdict"]),
        source_kind=_optional_text(row["host_checks_source_kind"]) or "unknown",
        source_ref=_optional_text(row["host_checks_source_ref"]),
        created_at=_optional_text(row["host_checks_created_at"]),
        check_run_id=_optional_text(row["host_check_run_id"]),
        manifest_path=None,
        summary=None,
        error=None,
    )


def _host_check_summary_from_mapping(summary: Mapping[str, object]) -> HostCheckSummary:
    return HostCheckSummary(
        selected_total=int(summary.get("selected_total", 0)),
        required_total=int(summary.get("required_total", 0)),
        required_passed=int(summary.get("required_passed", 0)),
        required_failed=int(summary.get("required_failed", 0)),
        advisory_total=int(summary.get("advisory_total", 0)),
        advisory_failed=int(summary.get("advisory_failed", 0)),
        blocked_total=int(summary.get("blocked_total", 0)),
    )


def _parse_json_object(raw_json: str | None) -> dict[str, object] | None:
    if raw_json is None:
        return {}
    try:
        value = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, Mapping):
        return None
    return dict(value)


def _require_text(field_name: str, value: object, database_path: Path) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_REQUEST_INVALID,
            message=f"{field_name} must be a non-empty string",
            database_path=database_path,
        )
    return normalized


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _optional_path(value: object) -> Path | None:
    normalized = _optional_text(value)
    if normalized is None:
        return None
    return Path(normalized).expanduser().resolve()


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json_required(path: Path, database_path: Path, decision_id: str) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_STORAGE_ERROR,
            message=f"Deployable-green manifest is missing on disk: {decision_id}",
            database_path=database_path,
            details=str(path),
        ) from exc
    except json.JSONDecodeError as exc:
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_STORAGE_ERROR,
            message=f"Deployable-green manifest is invalid JSON: {decision_id}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    if not isinstance(raw, Mapping):
        raise DeployableGreenError(
            code=DEPLOYABLE_GREEN_STORAGE_ERROR,
            message=f"Deployable-green manifest root must be an object: {decision_id}",
            database_path=database_path,
            details=str(path),
        )
    return dict(raw)


def _sha256_for_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
