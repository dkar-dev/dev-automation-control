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
import subprocess
import time
from urllib import error as urllib_error
from urllib import request as urllib_request

from .id_generation import generate_opaque_id
from .project_package import load_project_package
from .project_package_validator import ProjectPackageValidationFailed, RUNTIME_FILE
from .runtime_secrets import ResolvedRuntimeValueBundle, RuntimeValueRedactor, RuntimeValueResolutionError, resolve_runtime_value_bundle
from .run_persistence import RunDetails, RunPersistenceError, _connect_run_db, _ensure_required_tables, _resolve_database_path, get_run
from .step_run_persistence import StepRunDetails, StepRunPersistenceError, get_step_run
from .task_intake import ARTIFACT_KIND_TASK_RUNTIME_CONTEXT_MANIFEST


CONTROL_DIR = Path(__file__).resolve().parents[1]

HOST_CHECKS_RUNTIME_BLOCK = "host_checks_v1"
HOST_CHECKS_CONFIG_BLOCK_PATH = f"{RUNTIME_FILE}.{HOST_CHECKS_RUNTIME_BLOCK}"
HOST_CHECK_KINDS = ("command_check", "http_check", "file_check", "process_check")
HOST_CHECK_SEVERITIES = ("required", "advisory")
HOST_CHECK_VERDICTS = ("green", "not_green", "blocked")

ARTIFACT_KIND_HOST_CHECK_MANIFEST = "host_check_manifest"

HOST_CHECKS_CONFIG_INVALID = "HOST_CHECKS_CONFIG_INVALID"
HOST_CHECKS_NOT_FOUND = "HOST_CHECKS_NOT_FOUND"
HOST_CHECKS_REQUEST_INVALID = "HOST_CHECKS_REQUEST_INVALID"
HOST_CHECKS_RUN_SCOPE_INVALID = "HOST_CHECKS_RUN_SCOPE_INVALID"
HOST_CHECKS_RUNTIME_VALUE_RESOLUTION_FAILED = "HOST_CHECKS_RUNTIME_VALUE_RESOLUTION_FAILED"
HOST_CHECKS_STORAGE_ERROR = "HOST_CHECKS_STORAGE_ERROR"

_PLACEHOLDER_RE = re.compile(r"\{\{\s*(?P<key>[a-zA-Z0-9_]+)\s*\}\}")


@dataclass(frozen=True)
class HostCheckDefinition:
    check_id: str
    kind: str
    enabled: bool
    severity: str
    timeout_seconds: int
    allowed_workflow_ids: tuple[str, ...]
    allowed_project_profiles: tuple[str, ...]
    command: str | tuple[str, ...] | None
    url: str | None
    headers: dict[str, str] | None
    path: str | None
    process_selector: str | None
    success: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        command: str | list[str] | None
        if isinstance(self.command, tuple):
            command = list(self.command)
        else:
            command = self.command
        return {
            "id": self.check_id,
            "kind": self.kind,
            "enabled": self.enabled,
            "severity": self.severity,
            "timeout_seconds": self.timeout_seconds,
            "allowed_workflow_ids": list(self.allowed_workflow_ids),
            "allowed_project_profiles": list(self.allowed_project_profiles),
            "command": command,
            "url": self.url,
            "headers": dict(self.headers) if self.headers is not None else None,
            "path": self.path,
            "process_selector": self.process_selector,
            "success": self.success,
        }


@dataclass(frozen=True)
class HostCheckResult:
    check_id: str
    kind: str
    severity: str
    status: str
    message: str
    timeout_seconds: int
    started_at: str
    finished_at: str
    duration_seconds: float
    definition: dict[str, object]
    success_criteria: dict[str, object]
    observed: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.check_id,
            "kind": self.kind,
            "severity": self.severity,
            "status": self.status,
            "message": self.message,
            "timeout_seconds": self.timeout_seconds,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": round(self.duration_seconds, 6),
            "definition": self.definition,
            "success_criteria": self.success_criteria,
            "observed": self.observed,
        }


@dataclass(frozen=True)
class HostCheckSummary:
    selected_total: int
    required_total: int
    required_passed: int
    required_failed: int
    advisory_total: int
    advisory_failed: int
    blocked_total: int

    def to_dict(self) -> dict[str, int]:
        return {
            "selected_total": self.selected_total,
            "required_total": self.required_total,
            "required_passed": self.required_passed,
            "required_failed": self.required_failed,
            "advisory_total": self.advisory_total,
            "advisory_failed": self.advisory_failed,
            "blocked_total": self.blocked_total,
        }


@dataclass(frozen=True)
class HostCheckArtifact:
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
class HostCheckRunResult:
    check_run_id: str
    created_at: str
    config_block: str
    project_key: str
    package_root: Path
    project_profile: str
    workflow_id: str
    run_id: str
    flow_id: str
    step_run_id: str | None
    verdict: str
    summary: HostCheckSummary
    manifest: dict[str, object]
    manifest_path: Path
    artifacts: tuple[HostCheckArtifact, ...]
    check_results: tuple[HostCheckResult, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "check_run_id": self.check_run_id,
            "created_at": self.created_at,
            "config_block": self.config_block,
            "project_key": self.project_key,
            "package_root": str(self.package_root),
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "step_run_id": self.step_run_id,
            "verdict": self.verdict,
            "summary": self.summary.to_dict(),
            "manifest": self.manifest,
            "manifest_path": str(self.manifest_path),
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "check_results": [result.to_dict() for result in self.check_results],
        }


@dataclass(frozen=True)
class HostCheckRunRecord:
    check_run_id: str
    verdict: str
    created_at: str
    project_key: str
    project_profile: str
    workflow_id: str
    run_id: str
    flow_id: str
    step_run_id: str | None
    summary: HostCheckSummary
    manifest_path: Path
    manifest: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "check_run_id": self.check_run_id,
            "verdict": self.verdict,
            "created_at": self.created_at,
            "project_key": self.project_key,
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "step_run_id": self.step_run_id,
            "summary": self.summary.to_dict(),
            "manifest_path": str(self.manifest_path),
            "manifest": self.manifest,
        }


@dataclass(frozen=True)
class HostCheckRunInspection:
    run: RunDetails
    latest_result: HostCheckRunRecord | None
    history: tuple[HostCheckRunRecord, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "run": self.run.to_dict(),
            "latest_result": self.latest_result.to_dict() if self.latest_result is not None else None,
            "history": [record.to_dict() for record in self.history],
        }


@dataclass(frozen=True)
class HostCheckSelection:
    project_key: str
    package_root: Path
    workflow_id: str
    project_profile: str
    run_id: str | None
    flow_id: str | None
    config_block_present: bool
    selected_checks: tuple[HostCheckDefinition, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "project_key": self.project_key,
            "package_root": str(self.package_root),
            "workflow_id": self.workflow_id,
            "project_profile": self.project_profile,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "config_block_present": self.config_block_present,
            "selected_checks": [item.to_dict() for item in self.selected_checks],
        }


class HostCheckError(Exception):
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


def run_host_checks(
    database_path: str | Path,
    request_payload: Mapping[str, object],
) -> HostCheckRunResult:
    resolved_db_path = _resolve_database_path(database_path)
    request = _normalize_run_request(request_payload, resolved_db_path)
    run_details = _load_run_details_or_raise(resolved_db_path, request["run_id"])
    step_details = _load_step_run_details_or_none(resolved_db_path, request["step_run_id"])
    _validate_run_scope(run_details, step_details, request["step_run_id"], resolved_db_path)

    created_at = _utc_now()
    check_run_id = generate_opaque_id()
    runtime_context = _build_runtime_context(
        resolved_db_path,
        run_details=run_details,
        step_details=step_details,
        explicit_runtime_context=request["runtime_context"],
        explicit_artifact_root=request["artifact_root"],
    )

    config_issues: list[str] = []
    definitions: list[HostCheckDefinition] = []
    try:
        project_package = load_project_package(run_details.run.package_root)
        definitions, config_issues = _load_host_checks_config(
            project_package=project_package,
            workflow_id=run_details.run.workflow_id,
            project_profile=run_details.run.project_profile,
            database_path=resolved_db_path,
            requested_check_ids=request["check_ids"],
        )
    except ProjectPackageValidationFailed as exc:
        config_issues.append(
            "registered project package is invalid: "
            + "; ".join(f"{error.code}:{error.message}" for error in exc.errors)
        )

    runtime_value_bundle = _resolve_host_check_runtime_value_bundle(
        database_path=resolved_db_path,
        project_package_root=run_details.run.package_root,
        runtime_root=request["runtime_root"],
        local_secrets_file=request["local_secrets_file"],
    )
    runtime_redactor = runtime_value_bundle.redactor()
    runtime_context_for_execution = dict(runtime_context)
    runtime_context_for_execution.update(runtime_value_bundle.host_check_context())

    check_results = tuple(
        _sanitize_host_check_result(
            _execute_check(definition, runtime_context_for_execution),
            redactor=runtime_redactor,
        )
        for definition in definitions
    )
    summary = _summarize_check_results(check_results)
    verdict = _determine_verdict(config_issues, check_results)
    manifest_path = _resolve_manifest_path(
        explicit_artifact_root=request["artifact_root"],
        database_path=resolved_db_path,
        run_details=run_details,
        check_run_id=check_run_id,
    )

    manifest = {
        "check_run_id": check_run_id,
        "created_at": created_at,
        "config_block": HOST_CHECKS_CONFIG_BLOCK_PATH,
        "project": {
            "project_key": run_details.run.project_key,
            "package_root": str(run_details.run.package_root),
            "project_profile": run_details.run.project_profile,
            "workflow_id": run_details.run.workflow_id,
        },
        "runtime": {
            "run_id": run_details.run.id,
            "flow_id": run_details.run.flow_id,
            "step_run_id": step_details.step_run.id if step_details is not None else None,
            "run_status": run_details.run.status,
            "milestone": run_details.run.milestone,
        },
        "selection": {
            "requested_check_ids": request["check_ids"],
            "selected_check_ids": [definition.check_id for definition in definitions],
            "config_issues": list(config_issues),
        },
        "runtime_values": runtime_value_bundle.to_dict(),
        "gate": {
            "verdict": verdict,
            "summary": summary.to_dict(),
            "decision_rule": {
                "green": "all required checks passed and nothing was blocked",
                "not_green": "at least one required check failed and nothing was blocked",
                "blocked": "invalid config, missing runtime prerequisites, or impossible execution prevented a clean gate result",
            },
            "reviewer_integration_note": (
                "v1 keeps reviewer approval separate. Treat deployable green as reviewer-approved path plus a green host-check result."
            ),
        },
        "runtime_context": runtime_redactor.sanitize_object(runtime_context_for_execution),
        "checks": [result.to_dict() for result in check_results],
    }

    _write_json(manifest_path, manifest)
    _insert_host_check_run_row(
        resolved_db_path,
        check_run_id=check_run_id,
        run_details=run_details,
        step_run_id=step_details.step_run.id if step_details is not None else None,
        verdict=verdict,
        summary=summary,
        manifest_path=manifest_path,
        created_at=created_at,
    )
    artifacts = _record_host_check_artifacts(
        resolved_db_path,
        run_details=run_details,
        step_run_id=step_details.step_run.id if step_details is not None else None,
        created_at=created_at,
        artifact_paths=((ARTIFACT_KIND_HOST_CHECK_MANIFEST, manifest_path),),
    )

    return HostCheckRunResult(
        check_run_id=check_run_id,
        created_at=created_at,
        config_block=HOST_CHECKS_CONFIG_BLOCK_PATH,
        project_key=run_details.run.project_key,
        package_root=run_details.run.package_root,
        project_profile=run_details.run.project_profile,
        workflow_id=run_details.run.workflow_id,
        run_id=run_details.run.id,
        flow_id=run_details.run.flow_id,
        step_run_id=step_details.step_run.id if step_details is not None else None,
        verdict=verdict,
        summary=summary,
        manifest=manifest,
        manifest_path=manifest_path,
        artifacts=artifacts,
        check_results=check_results,
    )


def show_host_check_results(
    database_path: str | Path,
    run_id: str,
    *,
    limit: int = 20,
) -> HostCheckRunInspection:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_run_id = _require_text("run_id", run_id, resolved_db_path)
    if limit <= 0:
        raise HostCheckError(
            code=HOST_CHECKS_REQUEST_INVALID,
            message="limit must be greater than zero",
            database_path=resolved_db_path,
        )
    run_details = _load_run_details_or_raise(resolved_db_path, normalized_run_id)

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_tables(connection, resolved_db_path, ("host_check_runs",))
        rows = connection.execute(
            """
            SELECT
              host_check_runs.id,
              host_check_runs.verdict,
              host_check_runs.created_at,
              host_check_runs.workflow_id,
              host_check_runs.project_profile,
              host_check_runs.run_id,
              host_check_runs.flow_id,
              host_check_runs.step_run_id,
              host_check_runs.selected_total,
              host_check_runs.required_total,
              host_check_runs.required_passed,
              host_check_runs.required_failed,
              host_check_runs.advisory_total,
              host_check_runs.advisory_failed,
              host_check_runs.blocked_total,
              host_check_runs.manifest_json_path,
              projects.project_key
            FROM host_check_runs
            JOIN projects ON projects.id = host_check_runs.project_id
            WHERE host_check_runs.run_id = ?
            ORDER BY host_check_runs.created_at DESC, host_check_runs.id DESC
            LIMIT ?
            """,
            (normalized_run_id, limit),
        ).fetchall()
    except sqlite3.Error as exc:
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message="Failed to load host check history",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()

    history = tuple(_row_to_host_check_record(row, resolved_db_path) for row in rows)
    return HostCheckRunInspection(
        run=run_details,
        latest_result=history[0] if history else None,
        history=history,
    )


def list_host_checks(
    database_path: str | Path | None = None,
    *,
    run_id: str | None = None,
    project_key: str | None = None,
    package_root: str | Path | None = None,
    workflow_id: str | None = None,
    project_profile: str | None = None,
) -> HostCheckSelection:
    if run_id is None and package_root is None and (database_path is None or project_key is None):
        raise ValueError("Provide run_id, package_root, or database_path + project_key.")

    run_details: RunDetails | None = None
    resolved_db_path = _resolve_database_path(database_path) if database_path is not None else Path("<package-only>")
    if run_id is not None:
        if database_path is None:
            raise ValueError("database_path is required with run_id.")
        run_details = _load_run_details_or_raise(resolved_db_path, run_id)
        package_root_path = run_details.run.package_root
        workflow = run_details.run.workflow_id
        profile = run_details.run.project_profile
        project = run_details.run.project_key
    elif package_root is not None:
        package_root_path = Path(package_root).expanduser().resolve()
        workflow = _require_inline_text("workflow_id", workflow_id)
        profile = _require_inline_text("project_profile", project_profile)
        project = package_root_path.name
    else:
        assert database_path is not None
        assert project_key is not None
        project_row = _load_registered_project_row(resolved_db_path, project_key)
        if project_row is None:
            raise HostCheckError(
                code=HOST_CHECKS_NOT_FOUND,
                message=f"Project is not registered in SQLite: {project_key}",
                database_path=resolved_db_path,
            )
        package_root_path = Path(str(project_row["package_root"])).expanduser().resolve()
        workflow = _require_inline_text("workflow_id", workflow_id)
        profile = _require_inline_text("project_profile", project_profile)
        project = project_key

    try:
        project_package = load_project_package(package_root_path)
    except ProjectPackageValidationFailed as exc:
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"Project package is invalid: {package_root_path}",
            database_path=resolved_db_path,
            details="; ".join(f"{error.code}:{error.message}" for error in exc.errors),
        ) from exc

    definitions, issues = _load_host_checks_config(
        project_package=project_package,
        workflow_id=workflow,
        project_profile=profile,
        database_path=resolved_db_path,
        requested_check_ids=None,
    )
    if issues and project_package.files[RUNTIME_FILE].data.get(HOST_CHECKS_RUNTIME_BLOCK) is None:
        return HostCheckSelection(
            project_key=project,
            package_root=package_root_path,
            workflow_id=workflow,
            project_profile=profile,
            run_id=run_details.run.id if run_details is not None else None,
            flow_id=run_details.run.flow_id if run_details is not None else None,
            config_block_present=False,
            selected_checks=tuple(),
        )
    if issues:
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message="host check configuration is invalid",
            database_path=resolved_db_path,
            details="; ".join(issues),
        )

    return HostCheckSelection(
        project_key=project,
        package_root=package_root_path,
        workflow_id=workflow,
        project_profile=profile,
        run_id=run_details.run.id if run_details is not None else None,
        flow_id=run_details.run.flow_id if run_details is not None else None,
        config_block_present=True,
        selected_checks=tuple(definitions),
    )


def _normalize_run_request(payload: Mapping[str, object], database_path: Path) -> dict[str, object]:
    request = dict(payload)
    run_id = _require_text("run_id", request.get("run_id"), database_path)
    step_run_id = _optional_text(request.get("step_run_id"))
    artifact_root = _optional_path(request.get("artifact_root"))
    runtime_root = _optional_path(request.get("runtime_root"))
    local_secrets_file = _optional_path(request.get("local_secrets_file"))
    runtime_context = request.get("runtime_context")
    if runtime_context is not None and not isinstance(runtime_context, Mapping):
        raise HostCheckError(
            code=HOST_CHECKS_REQUEST_INVALID,
            message="runtime_context must be a mapping/object",
            database_path=database_path,
        )

    check_ids: list[str] = []
    raw_check_ids = request.get("check_ids")
    if raw_check_ids is not None:
        if isinstance(raw_check_ids, str):
            normalized = raw_check_ids.strip()
            if normalized:
                check_ids.append(normalized)
        elif isinstance(raw_check_ids, Sequence) and not isinstance(raw_check_ids, (str, bytes, bytearray)):
            for item in raw_check_ids:
                normalized = _optional_text(item)
                if normalized is not None:
                    check_ids.append(normalized)
        else:
            raise HostCheckError(
                code=HOST_CHECKS_REQUEST_INVALID,
                message="check_ids must be a string or array of strings",
                database_path=database_path,
            )

    return {
        "run_id": run_id,
        "step_run_id": step_run_id,
        "artifact_root": artifact_root,
        "runtime_root": runtime_root,
        "local_secrets_file": local_secrets_file,
        "runtime_context": dict(runtime_context) if isinstance(runtime_context, Mapping) else {},
        "check_ids": check_ids,
    }


def _load_run_details_or_raise(database_path: Path, run_id: str) -> RunDetails:
    try:
        return get_run(database_path, run_id)
    except RunPersistenceError as exc:
        raise HostCheckError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _ensure_tables(connection: sqlite3.Connection, database_path: Path, required_tables: tuple[str, ...]) -> None:
    try:
        _ensure_required_tables(connection, database_path, required_tables)
    except RunPersistenceError as exc:
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _load_step_run_details_or_none(database_path: Path, step_run_id: str | None) -> StepRunDetails | None:
    if step_run_id is None:
        return None
    try:
        return get_step_run(database_path, step_run_id)
    except StepRunPersistenceError as exc:
        raise HostCheckError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _validate_run_scope(
    run_details: RunDetails,
    step_details: StepRunDetails | None,
    requested_step_run_id: str | None,
    database_path: Path,
) -> None:
    if requested_step_run_id is None:
        return
    assert step_details is not None
    if step_details.step_run.run_id != run_details.run.id:
        raise HostCheckError(
            code=HOST_CHECKS_RUN_SCOPE_INVALID,
            message="step_run_id does not belong to the supplied run_id",
            database_path=database_path,
            details=f"run_id={run_details.run.id} step_run_run_id={step_details.step_run.run_id}",
        )


def _build_runtime_context(
    database_path: Path,
    *,
    run_details: RunDetails,
    step_details: StepRunDetails | None,
    explicit_runtime_context: Mapping[str, object],
    explicit_artifact_root: Path | None,
) -> dict[str, object]:
    runtime_context: dict[str, object] = {}
    persisted_runtime_context = _load_persisted_runtime_context(database_path, run_details.run.id)
    if persisted_runtime_context is not None:
        runtime_context.update(persisted_runtime_context)
    runtime_context.update(
        {
            "project_key": run_details.run.project_key,
            "package_root": str(run_details.run.package_root),
            "project_profile": run_details.run.project_profile,
            "workflow_id": run_details.run.workflow_id,
            "milestone": run_details.run.milestone,
            "run_id": run_details.run.id,
            "flow_id": run_details.run.flow_id,
            "step_run_id": step_details.step_run.id if step_details is not None else None,
        }
    )
    runtime_context.update(dict(explicit_runtime_context))
    if explicit_artifact_root is not None:
        runtime_context["artifact_root"] = str(explicit_artifact_root)
    return runtime_context


def _load_persisted_runtime_context(database_path: Path, run_id: str) -> dict[str, object] | None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("artifact_refs",))
        row = connection.execute(
            """
            SELECT filesystem_path
            FROM artifact_refs
            WHERE run_id = ? AND artifact_kind = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (run_id, ARTIFACT_KIND_TASK_RUNTIME_CONTEXT_MANIFEST),
        ).fetchone()
    except sqlite3.Error as exc:
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message="Failed to load persisted runtime context for host checks",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()
    if row is None:
        return None
    payload = _read_json_optional(Path(str(row["filesystem_path"])).expanduser().resolve())
    runtime_context = payload.get("runtime_context")
    return dict(runtime_context) if isinstance(runtime_context, Mapping) else None


def _load_host_checks_config(
    *,
    project_package,
    workflow_id: str,
    project_profile: str,
    database_path: Path,
    requested_check_ids: Sequence[str] | None,
) -> tuple[list[HostCheckDefinition], list[str]]:
    runtime_doc = project_package.files[RUNTIME_FILE].data
    raw_block = runtime_doc.get(HOST_CHECKS_RUNTIME_BLOCK)
    if raw_block is None:
        return [], [f"missing required checks config block: {HOST_CHECKS_CONFIG_BLOCK_PATH}"]
    if not isinstance(raw_block, Mapping):
        return [], [f"{HOST_CHECKS_CONFIG_BLOCK_PATH} must be a mapping/object"]

    raw_checks = raw_block.get("checks")
    if not isinstance(raw_checks, list):
        return [], [f"{HOST_CHECKS_CONFIG_BLOCK_PATH}.checks must be a list"]

    issues: list[str] = []
    parsed: list[HostCheckDefinition] = []
    seen_ids: set[str] = set()
    for index, raw_check in enumerate(raw_checks):
        if not isinstance(raw_check, Mapping):
            issues.append(f"{HOST_CHECKS_CONFIG_BLOCK_PATH}.checks[{index}] must be a mapping/object")
            continue
        try:
            parsed_check = _parse_check_definition(raw_check, index=index, database_path=database_path)
        except HostCheckError as exc:
            issues.append(exc.message if exc.details is None else f"{exc.message} ({exc.details})")
            continue
        if parsed_check.check_id in seen_ids:
            issues.append(f"duplicate check id in {HOST_CHECKS_CONFIG_BLOCK_PATH}.checks: {parsed_check.check_id}")
            continue
        seen_ids.add(parsed_check.check_id)
        if not parsed_check.enabled:
            continue
        if parsed_check.allowed_workflow_ids and workflow_id not in parsed_check.allowed_workflow_ids:
            continue
        if parsed_check.allowed_project_profiles and project_profile not in parsed_check.allowed_project_profiles:
            continue
        parsed.append(parsed_check)

    if requested_check_ids:
        selected_lookup = {item.check_id: item for item in parsed}
        missing_requested = [item for item in requested_check_ids if item not in selected_lookup]
        if missing_requested:
            raise HostCheckError(
                code=HOST_CHECKS_REQUEST_INVALID,
                message="requested check_ids are not enabled/applicable for this run scope",
                database_path=database_path,
                details="missing_check_ids=" + ",".join(missing_requested),
            )
        parsed = [selected_lookup[item] for item in requested_check_ids]

    return parsed, issues


def _parse_check_definition(raw_check: Mapping[str, object], *, index: int, database_path: Path) -> HostCheckDefinition:
    prefix = f"{HOST_CHECKS_CONFIG_BLOCK_PATH}.checks[{index}]"
    check_id = _require_text(f"{prefix}.id", raw_check.get("id"), database_path)
    kind = _require_text(f"{prefix}.kind", raw_check.get("kind"), database_path)
    if kind not in HOST_CHECK_KINDS:
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.kind must be one of: {', '.join(HOST_CHECK_KINDS)}",
            database_path=database_path,
            details=f"actual={kind}",
        )
    enabled = raw_check.get("enabled")
    if not isinstance(enabled, bool):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.enabled must be a boolean",
            database_path=database_path,
        )
    severity = _require_text(f"{prefix}.severity", raw_check.get("severity"), database_path)
    if severity not in HOST_CHECK_SEVERITIES:
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.severity must be one of: {', '.join(HOST_CHECK_SEVERITIES)}",
            database_path=database_path,
            details=f"actual={severity}",
        )
    timeout_seconds = _require_positive_int(f"{prefix}.timeout_seconds", raw_check.get("timeout_seconds"), database_path)
    success = raw_check.get("success")
    if not isinstance(success, Mapping):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success must be a mapping/object",
            database_path=database_path,
        )

    command: str | tuple[str, ...] | None = None
    url: str | None = None
    headers: dict[str, str] | None = None
    path: str | None = None
    process_selector: str | None = None
    if kind == "command_check":
        command = _parse_command_value(raw_check.get("command"), prefix=prefix, database_path=database_path)
        _validate_command_success(success, prefix=prefix, database_path=database_path)
    elif kind == "http_check":
        url = _require_text(f"{prefix}.url", raw_check.get("url"), database_path)
        headers = _parse_headers_value(raw_check.get("headers"), prefix=prefix, database_path=database_path)
        _validate_http_success(success, prefix=prefix, database_path=database_path)
    elif kind == "file_check":
        path = _require_text(f"{prefix}.path", raw_check.get("path"), database_path)
        _validate_file_success(success, prefix=prefix, database_path=database_path)
    else:
        process_selector = _require_text(f"{prefix}.process_selector", raw_check.get("process_selector"), database_path)
        _validate_process_success(success, prefix=prefix, database_path=database_path)

    return HostCheckDefinition(
        check_id=check_id,
        kind=kind,
        enabled=enabled,
        severity=severity,
        timeout_seconds=timeout_seconds,
        allowed_workflow_ids=tuple(_string_list(raw_check.get("allowed_workflow_ids"), field_name=f"{prefix}.allowed_workflow_ids", database_path=database_path)),
        allowed_project_profiles=tuple(_string_list(raw_check.get("allowed_project_profiles"), field_name=f"{prefix}.allowed_project_profiles", database_path=database_path)),
        command=command,
        url=url,
        headers=headers,
        path=path,
        process_selector=process_selector,
        success={str(key): value for key, value in success.items()},
    )


def _parse_command_value(value: object, *, prefix: str, database_path: Path) -> str | tuple[str, ...]:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        parts: list[str] = []
        for index, item in enumerate(value):
            normalized = _optional_text(item)
            if normalized is None:
                raise HostCheckError(
                    code=HOST_CHECKS_CONFIG_INVALID,
                    message=f"{prefix}.command[{index}] must be a non-empty string",
                    database_path=database_path,
                )
            parts.append(normalized)
        if not parts:
            raise HostCheckError(
                code=HOST_CHECKS_CONFIG_INVALID,
                message=f"{prefix}.command must not be an empty array",
                database_path=database_path,
            )
        return tuple(parts)
    raise HostCheckError(
        code=HOST_CHECKS_CONFIG_INVALID,
        message=f"{prefix}.command must be a string or list of strings",
        database_path=database_path,
    )


def _parse_headers_value(value: object, *, prefix: str, database_path: Path) -> dict[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.headers must be a mapping/object",
            database_path=database_path,
        )
    headers: dict[str, str] = {}
    for raw_key, raw_item in value.items():
        header_name = _optional_text(raw_key)
        header_value = _optional_text(raw_item)
        if header_name is None or header_value is None:
            raise HostCheckError(
                code=HOST_CHECKS_CONFIG_INVALID,
                message=f"{prefix}.headers entries must use non-empty string keys and values",
                database_path=database_path,
            )
        headers[header_name] = header_value
    return headers


def _validate_command_success(success: Mapping[str, object], *, prefix: str, database_path: Path) -> None:
    if "exit_code" in success and not isinstance(success.get("exit_code"), int):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success.exit_code must be an integer",
            database_path=database_path,
        )
    for key in ("stdout_contains", "stderr_contains"):
        value = success.get(key)
        if value is not None and not isinstance(value, str):
            raise HostCheckError(
                code=HOST_CHECKS_CONFIG_INVALID,
                message=f"{prefix}.success.{key} must be a string",
                database_path=database_path,
            )


def _validate_http_success(success: Mapping[str, object], *, prefix: str, database_path: Path) -> None:
    if "status_code" in success and not isinstance(success.get("status_code"), int):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success.status_code must be an integer",
            database_path=database_path,
        )
    if "body_contains" in success and not isinstance(success.get("body_contains"), str):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success.body_contains must be a string",
            database_path=database_path,
        )


def _validate_file_success(success: Mapping[str, object], *, prefix: str, database_path: Path) -> None:
    exists = success.get("exists")
    if exists is not None and not isinstance(exists, bool):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success.exists must be a boolean",
            database_path=database_path,
        )
    file_type = success.get("file_type")
    if file_type is not None and file_type not in {"file", "directory", "any"}:
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success.file_type must be one of: file, directory, any",
            database_path=database_path,
        )
    contains_text = success.get("contains_text")
    if contains_text is not None and not isinstance(contains_text, str):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success.contains_text must be a string",
            database_path=database_path,
        )


def _validate_process_success(success: Mapping[str, object], *, prefix: str, database_path: Path) -> None:
    min_matches = success.get("min_matches")
    max_matches = success.get("max_matches")
    if min_matches is not None and (not isinstance(min_matches, int) or min_matches < 0):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success.min_matches must be an integer >= 0",
            database_path=database_path,
        )
    if max_matches is not None and (not isinstance(max_matches, int) or max_matches < 0):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{prefix}.success.max_matches must be an integer >= 0",
            database_path=database_path,
        )


def _execute_check(definition: HostCheckDefinition, runtime_context: Mapping[str, object]) -> HostCheckResult:
    if definition.kind == "command_check":
        return _run_command_check(definition, runtime_context)
    if definition.kind == "http_check":
        return _run_http_check(definition, runtime_context)
    if definition.kind == "file_check":
        return _run_file_check(definition, runtime_context)
    return _run_process_check(definition, runtime_context)


def _run_command_check(definition: HostCheckDefinition, runtime_context: Mapping[str, object]) -> HostCheckResult:
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    observed_command: str | list[str] | tuple[str, ...] | None = None
    cwd: Path | None = None
    try:
        cwd = _resolve_command_cwd(runtime_context)
        if isinstance(definition.command, tuple):
            command = tuple(_render_string(item, runtime_context) for item in definition.command)
            observed_command = list(command)
            completed = subprocess.run(
                list(command),
                capture_output=True,
                text=True,
                timeout=definition.timeout_seconds,
                cwd=str(cwd) if cwd is not None else None,
                check=False,
            )
        else:
            assert isinstance(definition.command, str)
            rendered_command = _render_string(definition.command, runtime_context)
            observed_command = rendered_command
            completed = subprocess.run(
                ["/bin/sh", "-lc", rendered_command],
                capture_output=True,
                text=True,
                timeout=definition.timeout_seconds,
                cwd=str(cwd) if cwd is not None else None,
                check=False,
            )
    except _RuntimePrerequisiteMissing as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=exc.message,
            observed={"error": exc.message},
        )
    except FileNotFoundError as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"command executable is missing: {exc.filename}",
            observed={"error": str(exc)},
        )
    except subprocess.TimeoutExpired as exc:
        stdout_text = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout.decode("utf-8", errors="replace") if exc.stdout else "")
        stderr_text = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr.decode("utf-8", errors="replace") if exc.stderr else "")
        return _failed_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"timed out after {definition.timeout_seconds} seconds",
            observed={
                "command": observed_command or definition.command,
                "cwd": str(cwd) if cwd is not None else None,
                "timed_out": True,
                "stdout": stdout_text,
                "stderr": stderr_text,
            },
        )
    except OSError as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"failed to execute command check: {exc}",
            observed={"error": str(exc)},
        )

    success = definition.success
    expected_exit_code = int(success.get("exit_code", 0))
    stdout_contains = success.get("stdout_contains")
    stderr_contains = success.get("stderr_contains")
    passes = completed.returncode == expected_exit_code
    if isinstance(stdout_contains, str):
        passes = passes and stdout_contains in completed.stdout
    if isinstance(stderr_contains, str):
        passes = passes and stderr_contains in completed.stderr

    message = "passed"
    if not passes:
        message = f"command exited with {completed.returncode}; expected {expected_exit_code}"
    return _terminal_result(
        definition,
        status="passed" if passes else "failed",
        started_at=started_at,
        started_monotonic=started_monotonic,
        message=message,
        observed={
            "command": observed_command,
            "cwd": str(cwd) if cwd is not None else None,
            "exit_code": completed.returncode,
            "expected_exit_code": expected_exit_code,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "timed_out": False,
        },
    )


def _run_http_check(definition: HostCheckDefinition, runtime_context: Mapping[str, object]) -> HostCheckResult:
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    rendered_url: str | None = None
    rendered_headers: dict[str, str] | None = None
    try:
        assert definition.url is not None
        rendered_url = _render_string(definition.url, runtime_context)
        rendered_headers = {
            header_name: _render_string(header_value, runtime_context)
            for header_name, header_value in (definition.headers or {}).items()
        }
        request = urllib_request.Request(rendered_url, headers=rendered_headers, method="GET")
        with urllib_request.urlopen(request, timeout=definition.timeout_seconds) as response:
            status_code = int(response.getcode())
            body = response.read().decode("utf-8", errors="replace")
            content_type = response.headers.get("Content-Type")
    except _RuntimePrerequisiteMissing as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=exc.message,
            observed={"error": exc.message},
        )
    except urllib_error.HTTPError as exc:
        status_code = int(exc.code)
        body = exc.read().decode("utf-8", errors="replace")
        content_type = exc.headers.get("Content-Type")
        rendered_url = rendered_url or definition.url
    except TimeoutError:
        return _failed_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"timed out after {definition.timeout_seconds} seconds",
            observed={"url": rendered_url or definition.url, "request_headers": rendered_headers, "timed_out": True},
        )
    except urllib_error.URLError as exc:
        return _failed_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"http request failed: {exc.reason}",
            observed={"url": rendered_url or definition.url, "request_headers": rendered_headers, "error": str(exc.reason), "timed_out": False},
        )
    except ValueError as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"invalid http_check url: {exc}",
            observed={"url": rendered_url or definition.url, "request_headers": rendered_headers, "error": str(exc)},
        )

    success = definition.success
    expected_status = int(success.get("status_code", 200))
    body_contains = success.get("body_contains")
    passes = status_code == expected_status
    if isinstance(body_contains, str):
        passes = passes and body_contains in body
    return _terminal_result(
        definition,
        status="passed" if passes else "failed",
        started_at=started_at,
        started_monotonic=started_monotonic,
        message="passed" if passes else f"http status/body did not satisfy success criteria (status={status_code})",
        observed={
            "url": rendered_url,
            "request_headers": rendered_headers,
            "status_code": status_code,
            "expected_status_code": expected_status,
            "body": body,
            "content_type": content_type,
            "timed_out": False,
        },
    )


def _run_file_check(definition: HostCheckDefinition, runtime_context: Mapping[str, object]) -> HostCheckResult:
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    try:
        assert definition.path is not None
        rendered_path = _render_string(definition.path, runtime_context)
        resolved_path = _resolve_check_path(rendered_path, runtime_context)
    except _RuntimePrerequisiteMissing as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=exc.message,
            observed={"error": exc.message},
        )

    success = definition.success
    expected_exists = bool(success.get("exists", True))
    exists = resolved_path.exists()
    file_type = "missing"
    contains_text = success.get("contains_text")
    content_value: str | None = None
    if exists:
        if resolved_path.is_file():
            file_type = "file"
        elif resolved_path.is_dir():
            file_type = "directory"
        else:
            file_type = "other"

    if exists and isinstance(contains_text, str):
        if not resolved_path.is_file():
            return _blocked_result(
                definition,
                started_at=started_at,
                started_monotonic=started_monotonic,
                message="contains_text requires a readable regular file",
                observed={"path": str(resolved_path), "exists": exists, "file_type": file_type},
            )
        try:
            content_value = resolved_path.read_text(encoding="utf-8")
        except OSError as exc:
            return _blocked_result(
                definition,
                started_at=started_at,
                started_monotonic=started_monotonic,
                message=f"failed to read file check target: {exc}",
                observed={"path": str(resolved_path), "error": str(exc)},
            )

    expected_file_type = str(success.get("file_type", "any"))
    passes = exists == expected_exists
    if expected_exists and expected_file_type != "any":
        if expected_file_type == "file":
            passes = passes and resolved_path.is_file()
        elif expected_file_type == "directory":
            passes = passes and resolved_path.is_dir()
    if expected_exists and isinstance(contains_text, str):
        passes = passes and content_value is not None and contains_text in content_value

    return _terminal_result(
        definition,
        status="passed" if passes else "failed",
        started_at=started_at,
        started_monotonic=started_monotonic,
        message="passed" if passes else "file state did not satisfy success criteria",
        observed={
            "path": str(resolved_path),
            "exists": exists,
            "expected_exists": expected_exists,
            "file_type": file_type,
            "expected_file_type": expected_file_type,
            "content": content_value,
        },
    )


def _run_process_check(definition: HostCheckDefinition, runtime_context: Mapping[str, object]) -> HostCheckResult:
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    try:
        assert definition.process_selector is not None
        rendered_selector = _render_string(definition.process_selector, runtime_context)
        completed = subprocess.run(
            ["ps", "-eo", "pid=,comm=,args="],
            capture_output=True,
            text=True,
            timeout=definition.timeout_seconds,
            check=False,
        )
    except _RuntimePrerequisiteMissing as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=exc.message,
            observed={"error": exc.message},
        )
    except FileNotFoundError as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"ps executable is missing: {exc.filename}",
            observed={"error": str(exc)},
        )
    except subprocess.TimeoutExpired:
        return _failed_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"timed out after {definition.timeout_seconds} seconds",
            observed={"process_selector": definition.process_selector, "timed_out": True},
        )
    except OSError as exc:
        return _blocked_result(
            definition,
            started_at=started_at,
            started_monotonic=started_monotonic,
            message=f"failed to inspect process list: {exc}",
            observed={"error": str(exc)},
        )

    lines = [line.rstrip() for line in completed.stdout.splitlines() if line.strip()]
    matches = [line for line in lines if rendered_selector in line]
    success = definition.success
    min_matches = int(success.get("min_matches", 1))
    max_matches_raw = success.get("max_matches")
    max_matches = int(max_matches_raw) if isinstance(max_matches_raw, int) else None
    passes = len(matches) >= min_matches
    if max_matches is not None:
        passes = passes and len(matches) <= max_matches
    return _terminal_result(
        definition,
        status="passed" if passes else "failed",
        started_at=started_at,
        started_monotonic=started_monotonic,
        message="passed" if passes else f"matched {len(matches)} processes; expected min={min_matches}" + (f" max={max_matches}" if max_matches is not None else ""),
        observed={
            "process_selector": rendered_selector,
            "min_matches": min_matches,
            "max_matches": max_matches,
            "match_count": len(matches),
            "matches": matches,
            "ps_exit_code": completed.returncode,
            "stderr": completed.stderr,
        },
    )


def _resolve_command_cwd(runtime_context: Mapping[str, object]) -> Path | None:
    raw_path = runtime_context.get("project_repo_path")
    normalized = _optional_text(raw_path)
    if normalized is None:
        return None
    candidate = Path(normalized).expanduser().resolve()
    if not candidate.exists() or not candidate.is_dir():
        raise _RuntimePrerequisiteMissing(f"project_repo_path is missing or not a directory: {candidate}")
    return candidate


def _resolve_check_path(path_value: str, runtime_context: Mapping[str, object]) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    raw_repo_path = _optional_text(runtime_context.get("project_repo_path"))
    if raw_repo_path is None:
        raise _RuntimePrerequisiteMissing("relative check path requires runtime_context.project_repo_path")
    repo_root = Path(raw_repo_path).expanduser().resolve()
    if not repo_root.exists() or not repo_root.is_dir():
        raise _RuntimePrerequisiteMissing(f"project_repo_path is missing or not a directory: {repo_root}")
    return (repo_root / candidate).resolve()


def _render_string(template: str, context: Mapping[str, object]) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group("key")
        value = context.get(key)
        normalized = _stringify_placeholder_value(value)
        if normalized is None:
            raise _RuntimePrerequisiteMissing(f"missing runtime_context value for placeholder: {key}")
        return normalized

    return _PLACEHOLDER_RE.sub(replace, template)


def _stringify_placeholder_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return json.dumps(value, ensure_ascii=False)


def _failed_result(
    definition: HostCheckDefinition,
    *,
    started_at: str,
    started_monotonic: float,
    message: str,
    observed: dict[str, object],
) -> HostCheckResult:
    return _terminal_result(
        definition,
        status="failed",
        started_at=started_at,
        started_monotonic=started_monotonic,
        message=message,
        observed=observed,
    )


def _blocked_result(
    definition: HostCheckDefinition,
    *,
    started_at: str,
    started_monotonic: float,
    message: str,
    observed: dict[str, object],
) -> HostCheckResult:
    return _terminal_result(
        definition,
        status="blocked",
        started_at=started_at,
        started_monotonic=started_monotonic,
        message=message,
        observed=observed,
    )


def _terminal_result(
    definition: HostCheckDefinition,
    *,
    status: str,
    started_at: str,
    started_monotonic: float,
    message: str,
    observed: dict[str, object],
) -> HostCheckResult:
    finished_at = _utc_now()
    return HostCheckResult(
        check_id=definition.check_id,
        kind=definition.kind,
        severity=definition.severity,
        status=status,
        message=message,
        timeout_seconds=definition.timeout_seconds,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=time.monotonic() - started_monotonic,
        definition=definition.to_dict(),
        success_criteria=dict(definition.success),
        observed=observed,
    )


def _resolve_host_check_runtime_value_bundle(
    *,
    database_path: Path,
    project_package_root: Path,
    runtime_root: Path | None,
    local_secrets_file: Path | None,
) -> ResolvedRuntimeValueBundle:
    try:
        return resolve_runtime_value_bundle(
            package_root=project_package_root,
            selection="host_checks",
            runtime_root=runtime_root,
            control_root=CONTROL_DIR,
            local_secrets_file=local_secrets_file,
        )
    except RuntimeValueResolutionError as exc:
        raise HostCheckError(
            code=HOST_CHECKS_RUNTIME_VALUE_RESOLUTION_FAILED,
            message=exc.message,
            database_path=database_path,
            details=exc.details,
        ) from exc


def _sanitize_host_check_result(
    result: HostCheckResult,
    *,
    redactor: RuntimeValueRedactor,
) -> HostCheckResult:
    sanitized_definition = redactor.sanitize_object(result.definition)
    sanitized_success = redactor.sanitize_object(result.success_criteria)
    sanitized_observed = redactor.sanitize_object(result.observed)
    assert isinstance(sanitized_definition, Mapping)
    assert isinstance(sanitized_success, Mapping)
    assert isinstance(sanitized_observed, Mapping)
    return HostCheckResult(
        check_id=result.check_id,
        kind=result.kind,
        severity=result.severity,
        status=result.status,
        message=redactor.sanitize_text(result.message) or result.message,
        timeout_seconds=result.timeout_seconds,
        started_at=result.started_at,
        finished_at=result.finished_at,
        duration_seconds=result.duration_seconds,
        definition=dict(sanitized_definition),
        success_criteria=dict(sanitized_success),
        observed=dict(sanitized_observed),
    )


def _summarize_check_results(check_results: Sequence[HostCheckResult]) -> HostCheckSummary:
    required_total = 0
    required_passed = 0
    required_failed = 0
    advisory_total = 0
    advisory_failed = 0
    blocked_total = 0
    for result in check_results:
        if result.status == "blocked":
            blocked_total += 1
        if result.severity == "required":
            required_total += 1
            if result.status == "passed":
                required_passed += 1
            elif result.status == "failed":
                required_failed += 1
        else:
            advisory_total += 1
            if result.status == "failed":
                advisory_failed += 1
    return HostCheckSummary(
        selected_total=len(check_results),
        required_total=required_total,
        required_passed=required_passed,
        required_failed=required_failed,
        advisory_total=advisory_total,
        advisory_failed=advisory_failed,
        blocked_total=blocked_total,
    )


def _determine_verdict(config_issues: Sequence[str], check_results: Sequence[HostCheckResult]) -> str:
    if config_issues:
        return "blocked"
    if any(result.status == "blocked" for result in check_results):
        return "blocked"
    if any(result.severity == "required" and result.status == "failed" for result in check_results):
        return "not_green"
    return "green"


def _resolve_manifest_path(
    *,
    explicit_artifact_root: Path | None,
    database_path: Path,
    run_details: RunDetails,
    check_run_id: str,
) -> Path:
    if explicit_artifact_root is not None:
        output_root = explicit_artifact_root / run_details.run.project_key / run_details.run.flow_id / run_details.run.id / "checks" / check_run_id
    else:
        inferred_run_directory = _infer_run_artifact_directory(database_path, run_details.run.id)
        if inferred_run_directory is not None:
            output_root = inferred_run_directory / "checks" / check_run_id
        else:
            output_root = CONTROL_DIR / ".logs" / "host-checks" / run_details.run.project_key / run_details.run.flow_id / run_details.run.id / check_run_id
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
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message="Failed to infer host-check artifact directory",
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


def _insert_host_check_run_row(
    database_path: Path,
    *,
    check_run_id: str,
    run_details: RunDetails,
    step_run_id: str | None,
    verdict: str,
    summary: HostCheckSummary,
    manifest_path: Path,
    created_at: str,
) -> None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("projects", "runs", "host_check_runs"))
        connection.execute("BEGIN")
        connection.execute(
            """
            INSERT INTO host_check_runs (
              id,
              project_id,
              flow_id,
              run_id,
              step_run_id,
              workflow_id,
              project_profile,
              verdict,
              selected_total,
              required_total,
              required_passed,
              required_failed,
              advisory_total,
              advisory_failed,
              blocked_total,
              manifest_json_path,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                check_run_id,
                run_details.run.project_id,
                run_details.run.flow_id,
                run_details.run.id,
                step_run_id,
                run_details.run.workflow_id,
                run_details.run.project_profile,
                verdict,
                summary.selected_total,
                summary.required_total,
                summary.required_passed,
                summary.required_failed,
                summary.advisory_total,
                summary.advisory_failed,
                summary.blocked_total,
                str(manifest_path),
                created_at,
            ),
        )
        connection.commit()
    except sqlite3.Error as exc:
        connection.rollback()
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message=f"Failed to persist host check run row: {check_run_id}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _record_host_check_artifacts(
    database_path: Path,
    *,
    run_details: RunDetails,
    step_run_id: str | None,
    created_at: str,
    artifact_paths: Sequence[tuple[str, Path]],
) -> tuple[HostCheckArtifact, ...]:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("artifact_refs",))
        connection.execute("BEGIN")
        artifacts: list[HostCheckArtifact] = []
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_ref_id,
                    run_details.run.project_id,
                    run_details.run.flow_id,
                    run_details.run.id,
                    step_run_id,
                    artifact_kind,
                    str(resolved_path),
                    media_type,
                    size_bytes,
                    checksum_sha256,
                    created_at,
                ),
            )
            artifacts.append(
                HostCheckArtifact(
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
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message="Failed to persist host check artifact refs",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _row_to_host_check_record(row: sqlite3.Row, database_path: Path) -> HostCheckRunRecord:
    manifest_path = Path(str(row["manifest_json_path"])).expanduser().resolve()
    manifest = _read_json_required(manifest_path, database_path, str(row["id"]))
    return HostCheckRunRecord(
        check_run_id=str(row["id"]),
        verdict=str(row["verdict"]),
        created_at=str(row["created_at"]),
        project_key=str(row["project_key"]),
        project_profile=str(row["project_profile"]),
        workflow_id=str(row["workflow_id"]),
        run_id=str(row["run_id"]),
        flow_id=str(row["flow_id"]),
        step_run_id=_optional_text(row["step_run_id"]),
        summary=HostCheckSummary(
            selected_total=int(row["selected_total"]),
            required_total=int(row["required_total"]),
            required_passed=int(row["required_passed"]),
            required_failed=int(row["required_failed"]),
            advisory_total=int(row["advisory_total"]),
            advisory_failed=int(row["advisory_failed"]),
            blocked_total=int(row["blocked_total"]),
        ),
        manifest_path=manifest_path,
        manifest=manifest,
    )


def _load_registered_project_row(database_path: Path, project_key: str) -> sqlite3.Row | None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_tables(connection, database_path, ("projects",))
        return connection.execute(
            """
            SELECT id, project_key, package_root
            FROM projects
            WHERE project_key = ?
            """,
            (project_key,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message="Failed to load registered project row",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _string_list(value: object, *, field_name: str, database_path: Path) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{field_name} must be a list of strings",
            database_path=database_path,
        )
    result: list[str] = []
    for item in value:
        normalized = _optional_text(item)
        if normalized is None:
            raise HostCheckError(
                code=HOST_CHECKS_CONFIG_INVALID,
                message=f"{field_name} must contain only non-empty strings",
                database_path=database_path,
            )
        result.append(normalized)
    return result


def _require_positive_int(field_name: str, value: object, database_path: Path) -> int:
    if not isinstance(value, int) or value <= 0:
        raise HostCheckError(
            code=HOST_CHECKS_CONFIG_INVALID,
            message=f"{field_name} must be an integer greater than zero",
            database_path=database_path,
        )
    return value


def _require_text(field_name: str, value: object, database_path: Path) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise HostCheckError(
            code=HOST_CHECKS_REQUEST_INVALID,
            message=f"{field_name} must be a non-empty string",
            database_path=database_path,
        )
    return normalized


def _require_inline_text(field_name: str, value: str | None) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise ValueError(f"{field_name} is required")
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


def _read_json_optional(path: Path) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _read_json_required(path: Path, database_path: Path, check_run_id: str) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message=f"Host check manifest is missing on disk: {check_run_id}",
            database_path=database_path,
            details=str(path),
        ) from exc
    except json.JSONDecodeError as exc:
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message=f"Host check manifest is invalid JSON: {check_run_id}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    if not isinstance(raw, Mapping):
        raise HostCheckError(
            code=HOST_CHECKS_STORAGE_ERROR,
            message=f"Host check manifest root must be an object: {check_run_id}",
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


class _RuntimePrerequisiteMissing(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)
