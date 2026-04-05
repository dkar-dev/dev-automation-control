from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
from pathlib import Path
import re
import sqlite3

from .deployable_green import (
    DeployableGreenDecisionRecord,
    DeployableGreenError,
    HostChecksVerdictSource,
    ReviewerVerdictSource,
    show_deployable_green_decision,
)
from .dispatch_adapter import ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST
from .id_generation import generate_opaque_id
from .runtime_event_journal import RuntimeEventAppendRequest, insert_runtime_event_in_connection
from .reviewer_result_ingestion import ReviewerResultIngestionError, inspect_reviewer_result
from .run_persistence import RunDetails, RunPersistenceError, _connect_run_db, _ensure_required_tables, _resolve_database_path, get_run


CONTROL_DIR = Path(__file__).resolve().parents[1]

ARTIFACT_KIND_RELEASE_HANDOFF_ARTIFACT_INDEX = "release_handoff_artifact_index"
ARTIFACT_KIND_RELEASE_HANDOFF_MANIFEST = "release_handoff_manifest"
ARTIFACT_KIND_RELEASE_HANDOFF_SUMMARY_MARKDOWN = "release_handoff_summary_markdown"

RELEASE_HANDOFF_NOT_FOUND = "RELEASE_HANDOFF_NOT_FOUND"
RELEASE_HANDOFF_REQUEST_INVALID = "RELEASE_HANDOFF_REQUEST_INVALID"
RELEASE_HANDOFF_RUN_SCOPE_INVALID = "RELEASE_HANDOFF_RUN_SCOPE_INVALID"
RELEASE_HANDOFF_DECISION_REQUIRED = "RELEASE_HANDOFF_DECISION_REQUIRED"
RELEASE_HANDOFF_NOT_ELIGIBLE = "RELEASE_HANDOFF_NOT_ELIGIBLE"
RELEASE_HANDOFF_COMMIT_MISSING = "RELEASE_HANDOFF_COMMIT_MISSING"
RELEASE_HANDOFF_STORAGE_ERROR = "RELEASE_HANDOFF_STORAGE_ERROR"

_COMMIT_SHA_RE = re.compile(r"[0-9a-fA-F]{7,40}")
_DECISION_SOURCE_KIND = "green_decisions.latest"
_COMMIT_SOURCE_RULE_TEXT = (
    "v1 requires a persisted commit_sha before export. Resolution order: latest executor "
    "dispatch_result_manifest for the run, latest executor dispatch_result_manifest for the flow, "
    "latest reviewer dispatch_result_manifest or reviewer-derived commit source for the run, then "
    "latest reviewer dispatch_result_manifest or reviewer-derived commit source for the flow. If no "
    "persisted commit_sha is found, bundle creation fails and no release handoff artifacts are written."
)


@dataclass(frozen=True)
class CommitSource:
    commit_sha: str
    source_kind: str
    source_ref: str | None
    source_path: Path | None
    container_artifact_ref_id: str | None
    container_artifact_path: Path | None
    created_at: str | None
    role: str | None
    step_run_id: str | None
    source_run_id: str | None
    extraction_method: str

    def to_dict(self) -> dict[str, object]:
        return {
            "commit_sha": self.commit_sha,
            "source_kind": self.source_kind,
            "source_ref": self.source_ref,
            "source_path": str(self.source_path) if self.source_path is not None else None,
            "container_artifact_ref_id": self.container_artifact_ref_id,
            "container_artifact_path": (
                str(self.container_artifact_path) if self.container_artifact_path is not None else None
            ),
            "created_at": self.created_at,
            "role": self.role,
            "step_run_id": self.step_run_id,
            "source_run_id": self.source_run_id,
            "extraction_method": self.extraction_method,
        }


@dataclass(frozen=True)
class ReleaseDecisionSource:
    decision_id: str
    source_kind: str
    source_ref: str
    created_at: str
    decision_status: str
    summary: str
    rationale: str
    next_action_hint: str | None
    manifest_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_id": self.decision_id,
            "source_kind": self.source_kind,
            "source_ref": self.source_ref,
            "created_at": self.created_at,
            "decision_status": self.decision_status,
            "summary": self.summary,
            "rationale": self.rationale,
            "next_action_hint": self.next_action_hint,
            "manifest_path": str(self.manifest_path),
        }


@dataclass(frozen=True)
class ReleaseHandoffArtifact:
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
class ReleaseHandoffBundleResult:
    bundle_id: str
    created_at: str
    run_id: str
    flow_id: str
    project_key: str
    workflow_id: str
    project_profile: str
    milestone: str
    decision_status: str
    summary: str
    rationale: str
    commit_sha: str
    commit_source: CommitSource
    reviewer_verdict_source: ReviewerVerdictSource
    host_checks_source: HostChecksVerdictSource
    deployable_green_decision_source: ReleaseDecisionSource
    next_action_instructions: tuple[str, ...]
    operator_notes: tuple[str, ...]
    key_artifact_refs: dict[str, str | None]
    manifest: dict[str, object]
    manifest_path: Path
    summary_path: Path
    artifact_index_path: Path
    artifacts: tuple[ReleaseHandoffArtifact, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "bundle_id": self.bundle_id,
            "created_at": self.created_at,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "project_key": self.project_key,
            "workflow_id": self.workflow_id,
            "project_profile": self.project_profile,
            "milestone": self.milestone,
            "decision_status": self.decision_status,
            "summary": self.summary,
            "rationale": self.rationale,
            "commit_sha": self.commit_sha,
            "commit_source": self.commit_source.to_dict(),
            "reviewer_verdict_source": self.reviewer_verdict_source.to_dict(),
            "host_checks_source": self.host_checks_source.to_dict(),
            "deployable_green_decision_source": self.deployable_green_decision_source.to_dict(),
            "next_action_instructions": list(self.next_action_instructions),
            "operator_notes": list(self.operator_notes),
            "key_artifact_refs": dict(self.key_artifact_refs),
            "manifest": self.manifest,
            "manifest_path": str(self.manifest_path),
            "summary_path": str(self.summary_path),
            "artifact_index_path": str(self.artifact_index_path),
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }


@dataclass(frozen=True)
class ReleaseHandoffBundleRecord:
    bundle_id: str
    created_at: str
    run_id: str
    flow_id: str
    project_key: str
    workflow_id: str
    project_profile: str
    milestone: str
    decision_status: str
    summary: str
    rationale: str
    commit_sha: str
    commit_source: CommitSource
    reviewer_verdict_source: ReviewerVerdictSource
    host_checks_source: HostChecksVerdictSource
    deployable_green_decision_source: ReleaseDecisionSource
    next_action_instructions: tuple[str, ...]
    operator_notes: tuple[str, ...]
    key_artifact_refs: dict[str, str | None]
    manifest_path: Path
    summary_path: Path
    artifact_index_path: Path
    manifest: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "bundle_id": self.bundle_id,
            "created_at": self.created_at,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "project_key": self.project_key,
            "workflow_id": self.workflow_id,
            "project_profile": self.project_profile,
            "milestone": self.milestone,
            "decision_status": self.decision_status,
            "summary": self.summary,
            "rationale": self.rationale,
            "commit_sha": self.commit_sha,
            "commit_source": self.commit_source.to_dict(),
            "reviewer_verdict_source": self.reviewer_verdict_source.to_dict(),
            "host_checks_source": self.host_checks_source.to_dict(),
            "deployable_green_decision_source": self.deployable_green_decision_source.to_dict(),
            "next_action_instructions": list(self.next_action_instructions),
            "operator_notes": list(self.operator_notes),
            "key_artifact_refs": dict(self.key_artifact_refs),
            "manifest_path": str(self.manifest_path),
            "summary_path": str(self.summary_path),
            "artifact_index_path": str(self.artifact_index_path),
            "manifest": self.manifest,
        }


@dataclass(frozen=True)
class ReleaseHandoffInspection:
    run: RunDetails
    latest_bundle: ReleaseHandoffBundleRecord | None
    history: tuple[ReleaseHandoffBundleRecord, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "run": self.run.to_dict(),
            "latest_bundle": self.latest_bundle.to_dict() if self.latest_bundle is not None else None,
            "history": [record.to_dict() for record in self.history],
            "commit_source_rule": _COMMIT_SOURCE_RULE_TEXT,
        }


@dataclass(frozen=True)
class ReleaseHandoffSummary:
    bundle_id: str
    created_at: str
    run_id: str
    flow_id: str
    project_key: str
    workflow_id: str
    project_profile: str
    milestone: str
    decision_status: str
    commit_sha: str
    manifest_path: Path
    summary_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "bundle_id": self.bundle_id,
            "created_at": self.created_at,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "project_key": self.project_key,
            "workflow_id": self.workflow_id,
            "project_profile": self.project_profile,
            "milestone": self.milestone,
            "decision_status": self.decision_status,
            "commit_sha": self.commit_sha,
            "manifest_path": str(self.manifest_path),
            "summary_path": str(self.summary_path),
        }


@dataclass(frozen=True)
class _DispatchManifestCandidate:
    artifact_ref_id: str
    filesystem_path: Path
    created_at: str
    step_run_id: str | None
    step_key: str | None
    run_id: str | None
    scope: str


class ReleaseHandoffError(Exception):
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


def create_release_handoff(
    database_path: str | Path,
    request_payload: Mapping[str, object],
) -> ReleaseHandoffBundleResult:
    resolved_db_path = _resolve_database_path(database_path)
    request = _normalize_request(request_payload, resolved_db_path)
    run_details = _load_run_details_or_raise(resolved_db_path, request["run_id"])
    _validate_run_scope(run_details, request["flow_id"], resolved_db_path)

    latest_decision = _load_latest_deployable_green_or_raise(resolved_db_path, run_details.run.id)
    if latest_decision.decision_status != "deployable_green":
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_NOT_ELIGIBLE,
            message="release handoff bundle can be created only from a deployable_green decision",
            database_path=resolved_db_path,
            details=(
                f"run_id={run_details.run.id} latest_decision_id={latest_decision.decision_id} "
                f"decision_status={latest_decision.decision_status}"
            ),
        )

    commit_source = _resolve_commit_source(resolved_db_path, run_details)
    created_at = _utc_now()
    bundle_id = generate_opaque_id()
    manifest_path, summary_path, artifact_index_path = _resolve_bundle_paths(
        explicit_artifact_root=request["artifact_root"],
        database_path=resolved_db_path,
        run_details=run_details,
        bundle_id=bundle_id,
    )
    decision_source = _decision_source_from_record(latest_decision)
    operator_notes = _build_operator_notes(
        decision_source=decision_source,
        commit_source=commit_source,
        request_notes=request["operator_notes"],
    )
    next_action_instructions = _build_next_action_instructions(
        run_details=run_details,
        commit_source=commit_source,
        decision_source=decision_source,
        host_checks_source=latest_decision.host_checks_verdict_source,
    )
    summary = "Release handoff bundle is ready"
    rationale = (
        f"Latest deployable-green decision {decision_source.decision_id} marked the run as deployable_green and "
        f"a persisted commit_sha was resolved from {commit_source.extraction_method}."
    )
    key_artifact_refs = {
        "release_handoff_manifest": str(manifest_path),
        "release_handoff_summary_markdown": str(summary_path),
        "release_handoff_artifact_index": str(artifact_index_path),
        "deployable_green_decision_manifest": str(decision_source.manifest_path),
        "host_check_manifest": (
            str(latest_decision.host_checks_verdict_source.manifest_path)
            if latest_decision.host_checks_verdict_source.manifest_path is not None
            else None
        ),
        "commit_source_artifact": (
            str(commit_source.container_artifact_path)
            if commit_source.container_artifact_path is not None
            else (str(commit_source.source_path) if commit_source.source_path is not None else None)
        ),
    }
    artifact_index = {
        "bundle_id": bundle_id,
        "created_at": created_at,
        "run_id": run_details.run.id,
        "flow_id": run_details.run.flow_id,
        "generated_files": [
            {"artifact_kind": ARTIFACT_KIND_RELEASE_HANDOFF_MANIFEST, "filesystem_path": str(manifest_path)},
            {
                "artifact_kind": ARTIFACT_KIND_RELEASE_HANDOFF_SUMMARY_MARKDOWN,
                "filesystem_path": str(summary_path),
            },
            {
                "artifact_kind": ARTIFACT_KIND_RELEASE_HANDOFF_ARTIFACT_INDEX,
                "filesystem_path": str(artifact_index_path),
            },
        ],
        "source_files": [
            {
                "artifact_kind": "deployable_green_decision_manifest",
                "filesystem_path": str(decision_source.manifest_path),
            },
            {
                "artifact_kind": "host_check_manifest",
                "filesystem_path": (
                    str(latest_decision.host_checks_verdict_source.manifest_path)
                    if latest_decision.host_checks_verdict_source.manifest_path is not None
                    else None
                ),
            },
            {
                "artifact_kind": "commit_source_artifact",
                "filesystem_path": (
                    str(commit_source.container_artifact_path)
                    if commit_source.container_artifact_path is not None
                    else (str(commit_source.source_path) if commit_source.source_path is not None else None)
                ),
            },
        ],
    }
    manifest = {
        "bundle_id": bundle_id,
        "created_at": created_at,
        "run_id": run_details.run.id,
        "flow_id": run_details.run.flow_id,
        "project_key": run_details.run.project_key,
        "workflow_id": run_details.run.workflow_id,
        "project_profile": run_details.run.project_profile,
        "milestone": run_details.run.milestone,
        "decision_status": latest_decision.decision_status,
        "summary": summary,
        "rationale": rationale,
        "commit_sha": commit_source.commit_sha,
        "commit_source": commit_source.to_dict(),
        "reviewer_verdict_source": latest_decision.reviewer_verdict_source.to_dict(),
        "host_checks_source": latest_decision.host_checks_verdict_source.to_dict(),
        "deployable_green_decision_source": decision_source.to_dict(),
        "next_action_instructions": list(next_action_instructions),
        "operator_notes": list(operator_notes),
        "artifact_refs": dict(key_artifact_refs),
        "notes": {
            "handoff_semantics": (
                "This bundle is an explicit export-only handoff layer after the deployable-green decision. "
                "It does not deploy, roll back, or mutate reviewer, host-check, or green-decision state."
            ),
            "commit_source_rule": _COMMIT_SOURCE_RULE_TEXT,
            "consumption_hint": (
                "Operators or an external deployment system should consume this bundle as the release-ready "
                "approval packet, then execute rollout outside Control Plane v2."
            ),
        },
    }
    summary_markdown = _render_summary_markdown(
        manifest=manifest,
        manifest_path=manifest_path,
        summary_path=summary_path,
        artifact_index_path=artifact_index_path,
    )
    _write_text(summary_path, summary_markdown)
    _write_json(artifact_index_path, artifact_index)
    _write_json(manifest_path, manifest)
    _insert_release_handoff_row(
        resolved_db_path,
        bundle_id=bundle_id,
        run_details=run_details,
        decision_status=latest_decision.decision_status,
        summary=summary,
        rationale=rationale,
        commit_source=commit_source,
        reviewer_source=latest_decision.reviewer_verdict_source,
        host_checks_source=latest_decision.host_checks_verdict_source,
        decision_source=decision_source,
        manifest_path=manifest_path,
        summary_path=summary_path,
        artifact_index_path=artifact_index_path,
        created_at=created_at,
    )
    artifacts = _record_release_handoff_artifacts(
        resolved_db_path,
        run_details=run_details,
        created_at=created_at,
        artifact_paths=(
            (ARTIFACT_KIND_RELEASE_HANDOFF_MANIFEST, manifest_path),
            (ARTIFACT_KIND_RELEASE_HANDOFF_SUMMARY_MARKDOWN, summary_path),
            (ARTIFACT_KIND_RELEASE_HANDOFF_ARTIFACT_INDEX, artifact_index_path),
        ),
    )
    return ReleaseHandoffBundleResult(
        bundle_id=bundle_id,
        created_at=created_at,
        run_id=run_details.run.id,
        flow_id=run_details.run.flow_id,
        project_key=run_details.run.project_key,
        workflow_id=run_details.run.workflow_id,
        project_profile=run_details.run.project_profile,
        milestone=run_details.run.milestone,
        decision_status=latest_decision.decision_status,
        summary=summary,
        rationale=rationale,
        commit_sha=commit_source.commit_sha,
        commit_source=commit_source,
        reviewer_verdict_source=latest_decision.reviewer_verdict_source,
        host_checks_source=latest_decision.host_checks_verdict_source,
        deployable_green_decision_source=decision_source,
        next_action_instructions=next_action_instructions,
        operator_notes=operator_notes,
        key_artifact_refs=key_artifact_refs,
        manifest=manifest,
        manifest_path=manifest_path,
        summary_path=summary_path,
        artifact_index_path=artifact_index_path,
        artifacts=artifacts,
    )


def show_release_handoff(
    database_path: str | Path,
    run_id: str,
    *,
    limit: int = 20,
) -> ReleaseHandoffInspection:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_run_id = _require_text("run_id", run_id, resolved_db_path)
    if limit <= 0:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_REQUEST_INVALID,
            message="limit must be greater than zero",
            database_path=resolved_db_path,
        )
    run_details = _load_run_details_or_raise(resolved_db_path, normalized_run_id)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_tables(connection, resolved_db_path, ("release_handoffs",))
        rows = connection.execute(
            """
            SELECT
              release_handoffs.id,
              release_handoffs.flow_id,
              release_handoffs.run_id,
              release_handoffs.workflow_id,
              release_handoffs.project_profile,
              release_handoffs.milestone,
              release_handoffs.decision_status,
              release_handoffs.summary_text,
              release_handoffs.rationale_text,
              release_handoffs.commit_sha,
              release_handoffs.manifest_json_path,
              release_handoffs.summary_markdown_path,
              release_handoffs.artifact_index_json_path,
              release_handoffs.created_at,
              projects.project_key
            FROM release_handoffs
            JOIN projects ON projects.id = release_handoffs.project_id
            WHERE release_handoffs.run_id = ?
            ORDER BY release_handoffs.created_at DESC, release_handoffs.id DESC
            LIMIT ?
            """,
            (normalized_run_id, limit),
        ).fetchall()
    except sqlite3.Error as exc:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="Failed to load release handoff history",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()
    history = tuple(_row_to_handoff_record(row, resolved_db_path) for row in rows)
    return ReleaseHandoffInspection(
        run=run_details,
        latest_bundle=history[0] if history else None,
        history=history,
    )


def list_release_handoffs(
    database_path: str | Path,
    *,
    project_key: str | None = None,
    limit: int = 100,
) -> tuple[ReleaseHandoffSummary, ...]:
    resolved_db_path = _resolve_database_path(database_path)
    if limit <= 0:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_REQUEST_INVALID,
            message="limit must be greater than zero",
            database_path=resolved_db_path,
        )
    filters: list[str] = []
    params: list[object] = []
    normalized_project_key = _optional_text(project_key)
    if normalized_project_key is not None:
        filters.append("projects.project_key = ?")
        params.append(normalized_project_key)
    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_tables(connection, resolved_db_path, ("projects", "release_handoffs"))
        rows = connection.execute(
            f"""
            SELECT
              release_handoffs.id,
              release_handoffs.created_at,
              release_handoffs.run_id,
              release_handoffs.flow_id,
              release_handoffs.workflow_id,
              release_handoffs.project_profile,
              release_handoffs.milestone,
              release_handoffs.decision_status,
              release_handoffs.commit_sha,
              release_handoffs.manifest_json_path,
              release_handoffs.summary_markdown_path,
              projects.project_key
            FROM release_handoffs
            JOIN projects ON projects.id = release_handoffs.project_id
            {where_sql}
            ORDER BY release_handoffs.created_at DESC, release_handoffs.id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
    except sqlite3.Error as exc:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="Failed to list release handoff bundles",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()
    return tuple(
        ReleaseHandoffSummary(
            bundle_id=str(row["id"]),
            created_at=str(row["created_at"]),
            run_id=str(row["run_id"]),
            flow_id=str(row["flow_id"]),
            project_key=str(row["project_key"]),
            workflow_id=str(row["workflow_id"]),
            project_profile=str(row["project_profile"]),
            milestone=str(row["milestone"]),
            decision_status=str(row["decision_status"]),
            commit_sha=str(row["commit_sha"]),
            manifest_path=Path(str(row["manifest_json_path"])).expanduser().resolve(),
            summary_path=Path(str(row["summary_markdown_path"])).expanduser().resolve(),
        )
        for row in rows
    )


def _normalize_request(payload: Mapping[str, object], database_path: Path) -> dict[str, object]:
    request = dict(payload)
    return {
        "run_id": _require_text("run_id", request.get("run_id"), database_path),
        "flow_id": _optional_text(request.get("flow_id")),
        "artifact_root": _optional_path(request.get("artifact_root")),
        "operator_notes": _normalize_operator_notes(request.get("operator_notes"), database_path),
    }


def _normalize_operator_notes(value: object, database_path: Path) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        normalized = _optional_text(value)
        return (normalized,) if normalized is not None else ()
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            normalized = _optional_text(item)
            if normalized is None:
                raise ReleaseHandoffError(
                    code=RELEASE_HANDOFF_REQUEST_INVALID,
                    message="operator_notes must contain only non-empty strings",
                    database_path=database_path,
                )
            result.append(normalized)
        return tuple(result)
    raise ReleaseHandoffError(
        code=RELEASE_HANDOFF_REQUEST_INVALID,
        message="operator_notes must be a string or list of strings",
        database_path=database_path,
        details=f"actual_type={type(value).__name__}",
    )


def _load_run_details_or_raise(database_path: Path, run_id: str) -> RunDetails:
    try:
        return get_run(database_path, run_id)
    except RunPersistenceError as exc:
        raise ReleaseHandoffError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _validate_run_scope(run_details: RunDetails, requested_flow_id: str | None, database_path: Path) -> None:
    if requested_flow_id is None:
        return
    if run_details.run.flow_id != requested_flow_id:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_RUN_SCOPE_INVALID,
            message="flow_id does not belong to the supplied run_id",
            database_path=database_path,
            details=f"run_flow_id={run_details.run.flow_id} requested_flow_id={requested_flow_id}",
        )


def _load_latest_deployable_green_or_raise(database_path: Path, run_id: str) -> DeployableGreenDecisionRecord:
    try:
        inspection = show_deployable_green_decision(database_path, run_id, limit=1)
    except DeployableGreenError as exc:
        raise ReleaseHandoffError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc
    if inspection.latest_decision is None:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_DECISION_REQUIRED,
            message="release handoff bundle requires a persisted deployable-green decision",
            database_path=database_path,
            details=f"run_id={run_id}",
        )
    return inspection.latest_decision


def _decision_source_from_record(record: DeployableGreenDecisionRecord) -> ReleaseDecisionSource:
    return ReleaseDecisionSource(
        decision_id=record.decision_id,
        source_kind=_DECISION_SOURCE_KIND,
        source_ref=record.decision_id,
        created_at=record.created_at,
        decision_status=record.decision_status,
        summary=record.summary,
        rationale=record.rationale,
        next_action_hint=record.next_action_hint,
        manifest_path=record.manifest_path,
    )


def _resolve_commit_source(database_path: Path, run_details: RunDetails) -> CommitSource:
    attempts: list[str] = []
    candidates = (
        _load_latest_dispatch_manifest_candidate(database_path, run_id=run_details.run.id, step_key="executor", scope="run"),
        _load_latest_dispatch_manifest_candidate(database_path, flow_id=run_details.run.flow_id, step_key="executor", scope="flow"),
        _load_latest_dispatch_manifest_candidate(database_path, run_id=run_details.run.id, step_key="reviewer", scope="run"),
        _load_latest_dispatch_manifest_candidate(database_path, flow_id=run_details.run.flow_id, step_key="reviewer", scope="flow"),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        manifest = _read_json_optional(candidate.filesystem_path)
        if manifest is None:
            attempts.append(f"{candidate.scope}:{candidate.step_key}:invalid_json:{candidate.filesystem_path}")
            continue
        dispatch_outcome = manifest.get("dispatch_outcome")
        if isinstance(dispatch_outcome, Mapping):
            commit_sha = _normalize_commit_sha(dispatch_outcome.get("commit_sha"))
            if commit_sha is not None:
                return CommitSource(
                    commit_sha=commit_sha,
                    source_kind="dispatch_result_manifest.dispatch_outcome.commit_sha",
                    source_ref=candidate.artifact_ref_id,
                    source_path=candidate.filesystem_path,
                    container_artifact_ref_id=candidate.artifact_ref_id,
                    container_artifact_path=candidate.filesystem_path,
                    created_at=candidate.created_at,
                    role=candidate.step_key,
                    step_run_id=candidate.step_run_id,
                    source_run_id=candidate.run_id,
                    extraction_method="dispatch_outcome.commit_sha",
                )
        if candidate.step_key == "reviewer":
            try:
                inspection = inspect_reviewer_result(
                    database_path,
                    dispatch_result_manifest_path=candidate.filesystem_path,
                )
            except ReviewerResultIngestionError as exc:
                attempts.append(
                    f"{candidate.scope}:reviewer:inspect_error:{exc.code}:{candidate.filesystem_path}"
                )
                continue
            selected = inspection.selected_result
            if selected.commit_sha is not None:
                source_path = (
                    Path(selected.commit_sha_source_path).expanduser().resolve()
                    if selected.commit_sha_source_path is not None
                    else candidate.filesystem_path
                )
                return CommitSource(
                    commit_sha=selected.commit_sha,
                    source_kind=selected.commit_sha_source_kind or "reviewer_result.selected_result.commit_sha",
                    source_ref=candidate.artifact_ref_id,
                    source_path=source_path,
                    container_artifact_ref_id=candidate.artifact_ref_id,
                    container_artifact_path=candidate.filesystem_path,
                    created_at=candidate.created_at,
                    role="reviewer",
                    step_run_id=inspection.reviewer_step_run_id,
                    source_run_id=candidate.run_id,
                    extraction_method="reviewer_result.selected_result.commit_sha",
                )
        attempts.append(f"{candidate.scope}:{candidate.step_key}:missing_commit:{candidate.filesystem_path}")
    raise ReleaseHandoffError(
        code=RELEASE_HANDOFF_COMMIT_MISSING,
        message="release handoff bundle requires a persisted commit_sha source",
        database_path=database_path,
        details="; ".join(attempts) if attempts else f"run_id={run_details.run.id}",
    )


def _load_latest_dispatch_manifest_candidate(
    database_path: Path,
    *,
    run_id: str | None = None,
    flow_id: str | None = None,
    step_key: str,
    scope: str,
) -> _DispatchManifestCandidate | None:
    if run_id is None and flow_id is None:
        return None
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("artifact_refs", "step_runs"))
        filters = ["artifact_refs.artifact_kind = ?", "step_runs.step_key = ?"]
        params: list[object] = [ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST, step_key]
        if run_id is not None:
            filters.append("artifact_refs.run_id = ?")
            params.append(run_id)
        elif flow_id is not None:
            filters.append("artifact_refs.flow_id = ?")
            params.append(flow_id)
        row = connection.execute(
            f"""
            SELECT
              artifact_refs.id,
              artifact_refs.filesystem_path,
              artifact_refs.created_at,
              artifact_refs.step_run_id,
              artifact_refs.run_id,
              step_runs.step_key
            FROM artifact_refs
            JOIN step_runs ON step_runs.id = artifact_refs.step_run_id
            WHERE {' AND '.join(filters)}
            ORDER BY artifact_refs.created_at DESC, artifact_refs.id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    except sqlite3.Error as exc:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="Failed to resolve dispatch manifest candidate for release handoff",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()
    if row is None:
        return None
    return _DispatchManifestCandidate(
        artifact_ref_id=str(row["id"]),
        filesystem_path=Path(str(row["filesystem_path"])).expanduser().resolve(),
        created_at=str(row["created_at"]),
        step_run_id=_optional_text(row["step_run_id"]),
        step_key=_optional_text(row["step_key"]),
        run_id=_optional_text(row["run_id"]),
        scope=scope,
    )


def _build_operator_notes(
    *,
    decision_source: ReleaseDecisionSource,
    commit_source: CommitSource,
    request_notes: Sequence[str],
) -> tuple[str, ...]:
    notes: list[str] = []
    if decision_source.next_action_hint is not None:
        notes.append(decision_source.next_action_hint)
    notes.append(
        f"Persisted commit source: {commit_source.extraction_method} ({commit_source.source_kind})."
    )
    notes.extend(request_notes)
    return tuple(notes)


def _build_next_action_instructions(
    *,
    run_details: RunDetails,
    commit_source: CommitSource,
    decision_source: ReleaseDecisionSource,
    host_checks_source: HostChecksVerdictSource,
) -> tuple[str, ...]:
    instructions = [
        (
            f"Use commit {commit_source.commit_sha} as the release snapshot for run {run_details.run.id} "
            f"and workflow {run_details.run.workflow_id}."
        ),
        f"Review the formal deployable-green decision manifest at {decision_source.manifest_path}.",
        (
            f"Review the latest host-check manifest at {host_checks_source.manifest_path}."
            if host_checks_source.manifest_path is not None
            else "Review the referenced host-check source from the bundle before deployment."
        ),
        (
            "Execute rollout or deployment in the operator workflow or an external deployment system. "
            "Control Plane v2 does not perform rollout, deployment, or rollback orchestration."
        ),
    ]
    return tuple(instructions)


def _resolve_bundle_paths(
    *,
    explicit_artifact_root: Path | None,
    database_path: Path,
    run_details: RunDetails,
    bundle_id: str,
) -> tuple[Path, Path, Path]:
    if explicit_artifact_root is not None:
        output_root = (
            explicit_artifact_root
            / run_details.run.project_key
            / run_details.run.flow_id
            / run_details.run.id
            / "release-handoffs"
            / bundle_id
        )
    else:
        inferred_run_directory = _infer_run_artifact_directory(database_path, run_details.run.id)
        if inferred_run_directory is not None:
            output_root = inferred_run_directory / "release-handoffs" / bundle_id
        else:
            output_root = (
                CONTROL_DIR
                / ".logs"
                / "release-handoffs"
                / run_details.run.project_key
                / run_details.run.flow_id
                / run_details.run.id
                / bundle_id
            )
    output_root.mkdir(parents=True, exist_ok=True)
    return (
        output_root / "manifest.json",
        output_root / "handoff.md",
        output_root / "artifact-index.json",
    )


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
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="Failed to infer release handoff artifact directory",
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


def _insert_release_handoff_row(
    database_path: Path,
    *,
    bundle_id: str,
    run_details: RunDetails,
    decision_status: str,
    summary: str,
    rationale: str,
    commit_source: CommitSource,
    reviewer_source: ReviewerVerdictSource,
    host_checks_source: HostChecksVerdictSource,
    decision_source: ReleaseDecisionSource,
    manifest_path: Path,
    summary_path: Path,
    artifact_index_path: Path,
    created_at: str,
) -> None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("projects", "runs", "release_handoffs"))
        connection.execute("BEGIN")
        connection.execute(
            """
            INSERT INTO release_handoffs (
              id,
              project_id,
              flow_id,
              run_id,
              workflow_id,
              project_profile,
              milestone,
              decision_status,
              commit_sha,
              commit_source_kind,
              commit_source_ref,
              reviewer_source_kind,
              reviewer_source_ref,
              reviewer_created_at,
              reviewer_step_run_id,
              host_checks_source_kind,
              host_checks_source_ref,
              host_checks_created_at,
              host_check_run_id,
              green_decision_id,
              green_decision_created_at,
              summary_text,
              rationale_text,
              manifest_json_path,
              summary_markdown_path,
              artifact_index_json_path,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bundle_id,
                run_details.run.project_id,
                run_details.run.flow_id,
                run_details.run.id,
                run_details.run.workflow_id,
                run_details.run.project_profile,
                run_details.run.milestone,
                decision_status,
                commit_source.commit_sha,
                commit_source.source_kind,
                commit_source.source_ref,
                reviewer_source.source_kind,
                reviewer_source.source_ref,
                reviewer_source.created_at,
                reviewer_source.reviewer_step_run_id,
                host_checks_source.source_kind,
                host_checks_source.source_ref,
                host_checks_source.created_at,
                host_checks_source.check_run_id,
                decision_source.decision_id,
                decision_source.created_at,
                summary,
                rationale,
                str(manifest_path),
                str(summary_path),
                str(artifact_index_path),
                created_at,
            ),
        )
        insert_runtime_event_in_connection(
            connection,
            database_path=database_path,
            request=RuntimeEventAppendRequest(
                event_type="release_handoff_created",
                entity_type="run",
                entity_id=run_details.run.id,
                project_key=run_details.run.project_key,
                flow_id=run_details.run.flow_id,
                run_id=run_details.run.id,
                severity="info",
                summary=f"Release handoff created for run {run_details.run.id}",
                payload_redacted={
                    "bundle_id": bundle_id,
                    "decision_status": decision_status,
                    "commit_sha": commit_source.commit_sha,
                    "decision_id": decision_source.decision_id,
                    "reviewer_verdict": reviewer_source.verdict,
                    "host_checks_verdict": host_checks_source.verdict,
                    "manifest_path": manifest_path,
                    "summary_path": summary_path,
                    "artifact_index_path": artifact_index_path,
                },
                source_module="release_handoff",
                created_at=created_at,
            ),
        )
        connection.commit()
    except sqlite3.Error as exc:
        connection.rollback()
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message=f"Failed to persist release handoff row: {bundle_id}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _record_release_handoff_artifacts(
    database_path: Path,
    *,
    run_details: RunDetails,
    created_at: str,
    artifact_paths: Sequence[tuple[str, Path]],
) -> tuple[ReleaseHandoffArtifact, ...]:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("artifact_refs",))
        connection.execute("BEGIN")
        artifacts: list[ReleaseHandoffArtifact] = []
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
                ReleaseHandoffArtifact(
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
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="Failed to persist release handoff artifact refs",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _row_to_handoff_record(row: sqlite3.Row, database_path: Path) -> ReleaseHandoffBundleRecord:
    manifest_path = Path(str(row["manifest_json_path"])).expanduser().resolve()
    manifest = _read_json_required(manifest_path, database_path, str(row["id"]))
    commit_source = _commit_source_from_payload(manifest.get("commit_source"), database_path, str(row["id"]))
    reviewer_source = _reviewer_source_from_payload(
        manifest.get("reviewer_verdict_source"),
        database_path,
        str(row["id"]),
    )
    host_checks_source = _host_checks_source_from_payload(
        manifest.get("host_checks_source"),
        database_path,
        str(row["id"]),
    )
    decision_source = _decision_source_from_payload(
        manifest.get("deployable_green_decision_source"),
        database_path,
        str(row["id"]),
    )
    key_artifact_refs = manifest.get("artifact_refs")
    if not isinstance(key_artifact_refs, Mapping):
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="release handoff manifest is missing artifact_refs",
            database_path=database_path,
            details=f"bundle_id={row['id']}",
        )
    return ReleaseHandoffBundleRecord(
        bundle_id=str(row["id"]),
        created_at=str(row["created_at"]),
        run_id=str(row["run_id"]),
        flow_id=str(row["flow_id"]),
        project_key=str(row["project_key"]),
        workflow_id=str(row["workflow_id"]),
        project_profile=str(row["project_profile"]),
        milestone=str(row["milestone"]),
        decision_status=str(row["decision_status"]),
        summary=str(row["summary_text"]),
        rationale=str(row["rationale_text"]),
        commit_sha=str(row["commit_sha"]),
        commit_source=commit_source,
        reviewer_verdict_source=reviewer_source,
        host_checks_source=host_checks_source,
        deployable_green_decision_source=decision_source,
        next_action_instructions=_string_tuple_from_payload(manifest.get("next_action_instructions")),
        operator_notes=_string_tuple_from_payload(manifest.get("operator_notes")),
        key_artifact_refs={str(key): _optional_text(value) for key, value in key_artifact_refs.items()},
        manifest_path=manifest_path,
        summary_path=Path(str(row["summary_markdown_path"])).expanduser().resolve(),
        artifact_index_path=Path(str(row["artifact_index_json_path"])).expanduser().resolve(),
        manifest=manifest,
    )


def _commit_source_from_payload(payload: object, database_path: Path, bundle_id: str) -> CommitSource:
    if not isinstance(payload, Mapping):
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="release handoff manifest is missing commit_source",
            database_path=database_path,
            details=f"bundle_id={bundle_id}",
        )
    commit_sha = _normalize_commit_sha(payload.get("commit_sha"))
    if commit_sha is None:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="release handoff manifest has an invalid commit_source.commit_sha",
            database_path=database_path,
            details=f"bundle_id={bundle_id}",
        )
    return CommitSource(
        commit_sha=commit_sha,
        source_kind=_optional_text(payload.get("source_kind")) or "unknown",
        source_ref=_optional_text(payload.get("source_ref")),
        source_path=_optional_path(payload.get("source_path")),
        container_artifact_ref_id=_optional_text(payload.get("container_artifact_ref_id")),
        container_artifact_path=_optional_path(payload.get("container_artifact_path")),
        created_at=_optional_text(payload.get("created_at")),
        role=_optional_text(payload.get("role")),
        step_run_id=_optional_text(payload.get("step_run_id")),
        source_run_id=_optional_text(payload.get("source_run_id")),
        extraction_method=_optional_text(payload.get("extraction_method")) or "unknown",
    )


def _reviewer_source_from_payload(payload: object, database_path: Path, bundle_id: str) -> ReviewerVerdictSource:
    if not isinstance(payload, Mapping):
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="release handoff manifest is missing reviewer_verdict_source",
            database_path=database_path,
            details=f"bundle_id={bundle_id}",
        )
    return ReviewerVerdictSource(
        verdict=_optional_text(payload.get("verdict")),
        source_kind=_optional_text(payload.get("source_kind")) or "unknown",
        source_ref=_optional_text(payload.get("source_ref")),
        created_at=_optional_text(payload.get("created_at")),
        reviewer_step_run_id=_optional_text(payload.get("reviewer_step_run_id")),
        summary_text=_optional_text(payload.get("summary_text")),
        stop_reason_code=_optional_text(payload.get("stop_reason_code")),
        stop_reason=_optional_text(payload.get("stop_reason")),
        error=_optional_text(payload.get("error")),
    )


def _host_checks_source_from_payload(payload: object, database_path: Path, bundle_id: str) -> HostChecksVerdictSource:
    if not isinstance(payload, Mapping):
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="release handoff manifest is missing host_checks_source",
            database_path=database_path,
            details=f"bundle_id={bundle_id}",
        )
    return HostChecksVerdictSource(
        verdict=_optional_text(payload.get("verdict")),
        source_kind=_optional_text(payload.get("source_kind")) or "unknown",
        source_ref=_optional_text(payload.get("source_ref")),
        created_at=_optional_text(payload.get("created_at")),
        check_run_id=_optional_text(payload.get("check_run_id")),
        manifest_path=_optional_path(payload.get("manifest_path")),
        summary=None,
        error=_optional_text(payload.get("error")),
    )


def _decision_source_from_payload(payload: object, database_path: Path, bundle_id: str) -> ReleaseDecisionSource:
    if not isinstance(payload, Mapping):
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="release handoff manifest is missing deployable_green_decision_source",
            database_path=database_path,
            details=f"bundle_id={bundle_id}",
        )
    manifest_path = _optional_path(payload.get("manifest_path"))
    if manifest_path is None:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="release handoff manifest is missing deployable_green_decision_source.manifest_path",
            database_path=database_path,
            details=f"bundle_id={bundle_id}",
        )
    return ReleaseDecisionSource(
        decision_id=_optional_text(payload.get("decision_id")) or "unknown",
        source_kind=_optional_text(payload.get("source_kind")) or "unknown",
        source_ref=_optional_text(payload.get("source_ref")) or "unknown",
        created_at=_optional_text(payload.get("created_at")) or "unknown",
        decision_status=_optional_text(payload.get("decision_status")) or "unknown",
        summary=_optional_text(payload.get("summary")) or "",
        rationale=_optional_text(payload.get("rationale")) or "",
        next_action_hint=_optional_text(payload.get("next_action_hint")),
        manifest_path=manifest_path,
    )


def _string_tuple_from_payload(value: object) -> tuple[str, ...]:
    if value is None or not isinstance(value, list):
        return ()
    result: list[str] = []
    for item in value:
        normalized = _optional_text(item)
        if normalized is not None:
            result.append(normalized)
    return tuple(result)


def _render_summary_markdown(
    *,
    manifest: Mapping[str, object],
    manifest_path: Path,
    summary_path: Path,
    artifact_index_path: Path,
) -> str:
    instructions = _string_tuple_from_payload(manifest.get("next_action_instructions"))
    operator_notes = _string_tuple_from_payload(manifest.get("operator_notes"))
    reviewer_source = manifest.get("reviewer_verdict_source") if isinstance(manifest.get("reviewer_verdict_source"), Mapping) else {}
    host_checks_source = manifest.get("host_checks_source") if isinstance(manifest.get("host_checks_source"), Mapping) else {}
    decision_source = (
        manifest.get("deployable_green_decision_source")
        if isinstance(manifest.get("deployable_green_decision_source"), Mapping)
        else {}
    )
    artifact_refs = manifest.get("artifact_refs") if isinstance(manifest.get("artifact_refs"), Mapping) else {}
    lines = [
        f"# Release Handoff Bundle {manifest['bundle_id']}",
        "",
        "## Scope",
        f"- Created at: {manifest['created_at']}",
        f"- Run: {manifest['run_id']}",
        f"- Flow: {manifest['flow_id']}",
        f"- Project: {manifest['project_key']}",
        f"- Workflow: {manifest['workflow_id']}",
        f"- Project profile: {manifest['project_profile']}",
        f"- Milestone: {manifest['milestone']}",
        "",
        "## Release Decision",
        f"- Decision status: {manifest['decision_status']}",
        f"- Bundle summary: {manifest['summary']}",
        f"- Bundle rationale: {manifest['rationale']}",
        f"- Deployable-green decision id: {decision_source.get('decision_id') or 'unknown'}",
        f"- Deployable-green manifest: {decision_source.get('manifest_path') or 'unknown'}",
        "",
        "## Commit Snapshot",
        f"- Commit SHA: {manifest['commit_sha']}",
        (
            f"- Commit source: {manifest['commit_source']['source_kind']}"
            if isinstance(manifest.get("commit_source"), Mapping)
            else "- Commit source: unknown"
        ),
        (
            f"- Commit source path: {manifest['commit_source'].get('source_path') or 'n/a'}"
            if isinstance(manifest.get("commit_source"), Mapping)
            else "- Commit source path: n/a"
        ),
        "",
        "## Reviewer And Checks",
        f"- Reviewer verdict: {reviewer_source.get('verdict') or 'unknown'}",
        f"- Reviewer source: {reviewer_source.get('source_kind') or 'unknown'}",
        f"- Host checks verdict: {host_checks_source.get('verdict') or 'unknown'}",
        f"- Host checks source: {host_checks_source.get('source_kind') or 'unknown'}",
        "",
        "## Next Action",
    ]
    if instructions:
        lines.extend(f"- {instruction}" for instruction in instructions)
    else:
        lines.append("- No next_action_instructions were recorded.")
    lines.extend(["", "## Operator Notes"])
    if operator_notes:
        lines.extend(f"- {note}" for note in operator_notes)
    else:
        lines.append("- No extra operator notes were provided.")
    lines.extend(
        [
            "",
            "## Artifact References",
            f"- Bundle manifest: {manifest_path}",
            f"- Bundle summary: {summary_path}",
            f"- Artifact index: {artifact_index_path}",
        ]
    )
    for key, value in artifact_refs.items():
        lines.append(f"- {key}: {value or 'n/a'}")
    lines.extend(
        [
            "",
            "## Boundaries",
            "- This handoff bundle is export-only.",
            "- Control Plane v2 does not perform rollout, deployment, or rollback orchestration from this bundle.",
            "",
        ]
    )
    return "\n".join(lines)


def _ensure_tables(connection: sqlite3.Connection, database_path: Path, required_tables: tuple[str, ...]) -> None:
    try:
        _ensure_required_tables(connection, database_path, required_tables)
    except RunPersistenceError as exc:
        raise ReleaseHandoffError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _read_json_optional(path: Path) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _read_json_required(path: Path, database_path: Path, bundle_id: str) -> dict[str, object]:
    payload = _read_json_optional(path)
    if payload is None:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_STORAGE_ERROR,
            message="Failed to load release handoff manifest JSON",
            database_path=database_path,
            details=f"bundle_id={bundle_id} manifest_path={path}",
        )
    return payload


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def _sha256_for_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_text(field_name: str, value: object, database_path: Path) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise ReleaseHandoffError(
            code=RELEASE_HANDOFF_REQUEST_INVALID,
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


def _normalize_commit_sha(value: object) -> str | None:
    normalized = _optional_text(value)
    if normalized is None or _COMMIT_SHA_RE.fullmatch(normalized) is None:
        return None
    return normalized
