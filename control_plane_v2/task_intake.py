from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
from pathlib import Path
import sqlite3

from .dispatch_adapter import (
    ARTIFACT_KIND_TASK_RUNTIME_CONTEXT_MANIFEST,
    ARTIFACT_KIND_TASK_SUBMISSION_MANIFEST,
)
from .id_generation import generate_opaque_id
from .project_package import load_project_package
from .project_package_validator import ProjectPackageValidationFailed
from .project_package_validator import INSTRUCTIONS_FILE, RUNTIME_FILE
from .run_persistence import (
    PRIORITY_CLASSES,
    RootRunCreateRequest,
    RunDetails,
    RunPersistenceError,
    _connect_run_db,
    _ensure_required_tables,
    _resolve_database_path,
    create_root_run,
    get_run,
)


CONTROL_DIR = Path(__file__).resolve().parents[1]

RUNTIME_DEFAULTS_BLOCK = "bounded_task_runtime_v1"
INSTRUCTIONS_DEFAULTS_BLOCK = "bounded_task_intake_v1"

INTAKE_MANIFEST_NOT_FOUND = "INTAKE_MANIFEST_NOT_FOUND"
INTAKE_PROJECT_NOT_REGISTERED = "INTAKE_PROJECT_NOT_REGISTERED"
INTAKE_RUNTIME_CONFIG_INVALID = "INTAKE_RUNTIME_CONFIG_INVALID"
INTAKE_STORAGE_ERROR = "INTAKE_STORAGE_ERROR"
INTAKE_SUBMISSION_INVALID = "INTAKE_SUBMISSION_INVALID"


@dataclass(frozen=True)
class BoundedTaskSubmissionRequest:
    project_key: str
    task_text: str
    project_profile: str
    workflow_id: str
    milestone: str
    priority_class: str = "interactive"
    instruction_profile: str | None = None
    instruction_overlays: tuple[str, ...] | None = None
    source: str | None = None
    thread_label: str | None = None
    constraints: tuple[str, ...] = ()
    expected_output: tuple[str, ...] = ()
    artifact_root: Path | None = None
    workspace_root: Path | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "project_key": self.project_key,
            "task_text": self.task_text,
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "milestone": self.milestone,
            "priority_class": self.priority_class,
            "instruction_profile": self.instruction_profile,
            "instruction_overlays": list(self.instruction_overlays) if self.instruction_overlays is not None else None,
            "source": self.source,
            "thread_label": self.thread_label,
            "constraints": list(self.constraints),
            "expected_output": list(self.expected_output),
            "artifact_root": str(self.artifact_root) if self.artifact_root is not None else None,
            "workspace_root": str(self.workspace_root) if self.workspace_root is not None else None,
        }


@dataclass(frozen=True)
class SubmittedTaskArtifact:
    id: str
    run_id: str
    artifact_kind: str
    filesystem_path: Path
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "artifact_kind": self.artifact_kind,
            "filesystem_path": str(self.filesystem_path),
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class SubmittedTaskResult:
    submitted_at: str
    request: BoundedTaskSubmissionRequest
    run_details: RunDetails
    runtime_context: dict[str, object]
    submission_manifest: dict[str, object]
    runtime_context_manifest: dict[str, object]
    artifacts: tuple[SubmittedTaskArtifact, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "submitted_at": self.submitted_at,
            "request": self.request.to_dict(),
            "run_details": self.run_details.to_dict(),
            "runtime_context": self.runtime_context,
            "submission_manifest": self.submission_manifest,
            "runtime_context_manifest": self.runtime_context_manifest,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }


@dataclass(frozen=True)
class SubmittedTaskInspection:
    run_details: RunDetails
    submission_manifest: dict[str, object]
    runtime_context_manifest: dict[str, object]
    artifacts: tuple[SubmittedTaskArtifact, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_details": self.run_details.to_dict(),
            "submission_manifest": self.submission_manifest,
            "runtime_context_manifest": self.runtime_context_manifest,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }


@dataclass(frozen=True)
class SubmittedTaskSummary:
    run_id: str
    flow_id: str
    project_key: str
    project_profile: str
    workflow_id: str
    milestone: str
    run_status: str
    queue_status: str | None
    task_text: str
    source: str | None
    thread_label: str | None
    submitted_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "project_key": self.project_key,
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "milestone": self.milestone,
            "run_status": self.run_status,
            "queue_status": self.queue_status,
            "task_text": self.task_text,
            "source": self.source,
            "thread_label": self.thread_label,
            "submitted_at": self.submitted_at,
        }


class TaskIntakeError(Exception):
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


def submit_bounded_task(
    database_path: str | Path,
    submission_payload: Mapping[str, object],
) -> SubmittedTaskResult:
    resolved_db_path = _resolve_database_path(database_path)
    request = _normalize_submission_request(submission_payload, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("projects",))
        registered_project = _load_registered_project_row(connection, request.project_key)
    except sqlite3.Error as exc:
        connection.close()
        raise TaskIntakeError(
            code=INTAKE_STORAGE_ERROR,
            message="SQLite intake project lookup failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()

    if registered_project is None:
        raise TaskIntakeError(
            code=INTAKE_PROJECT_NOT_REGISTERED,
            message=f"Project is not registered in SQLite: {request.project_key}",
            database_path=resolved_db_path,
        )

    try:
        project_package = load_project_package(Path(str(registered_project["package_root"])).expanduser().resolve())
    except ProjectPackageValidationFailed as exc:
        raise TaskIntakeError(
            code=INTAKE_RUNTIME_CONFIG_INVALID,
            message=f"Registered project package is no longer valid: {request.project_key}",
            database_path=resolved_db_path,
            details="; ".join(f"{error.code}:{error.file_name}:{error.message}" for error in exc.errors),
        ) from exc
    submitted_at = _utc_now()
    runtime_context = _build_runtime_context(
        request,
        project_package=project_package,
        database_path=resolved_db_path,
    )

    try:
        run_details = create_root_run(
            resolved_db_path,
            RootRunCreateRequest(
                project_key=request.project_key,
                project_profile=request.project_profile,
                workflow_id=request.workflow_id,
                milestone=request.milestone,
                priority_class=request.priority_class,
                artifact_root=_optional_path(runtime_context.get("artifact_root")),
            ),
        )
    except RunPersistenceError as exc:
        raise TaskIntakeError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc

    submission_manifest = {
        "submitted_at": submitted_at,
        "project_package_root": str(project_package.package_root),
        "run": {
            "run_id": run_details.run.id,
            "flow_id": run_details.run.flow_id,
            "queue_item_id": run_details.run.queue_item.id if run_details.run.queue_item is not None else None,
            "queue_status": run_details.run.queue_item.status if run_details.run.queue_item is not None else None,
            "artifact_directory": str(run_details.artifact_directory) if run_details.artifact_directory is not None else None,
        },
        "submission": request.to_dict(),
    }
    runtime_context_manifest = {
        "submitted_at": submitted_at,
        "run_id": run_details.run.id,
        "flow_id": run_details.run.flow_id,
        "runtime_context": runtime_context,
    }

    artifact_directory = _resolve_submission_artifact_directory(run_details)
    submission_manifest_path = artifact_directory / "task-submission.json"
    runtime_context_path = artifact_directory / "runtime-context.json"
    _write_json(submission_manifest_path, submission_manifest)
    _write_json(runtime_context_path, runtime_context_manifest)

    try:
        artifacts = _record_submission_artifacts(
            resolved_db_path,
            run_details,
            artifact_paths=(
                (ARTIFACT_KIND_TASK_SUBMISSION_MANIFEST, submission_manifest_path),
                (ARTIFACT_KIND_TASK_RUNTIME_CONTEXT_MANIFEST, runtime_context_path),
            ),
        )
    except TaskIntakeError:
        raise

    return SubmittedTaskResult(
        submitted_at=submitted_at,
        request=request,
        run_details=run_details,
        runtime_context=runtime_context,
        submission_manifest=submission_manifest,
        runtime_context_manifest=runtime_context_manifest,
        artifacts=artifacts,
    )


def show_submitted_task(database_path: str | Path, run_id: str) -> SubmittedTaskInspection:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_run_id = _required_text("run_id", run_id, resolved_db_path)
    run_details = _load_run_details_or_raise(resolved_db_path, normalized_run_id)
    artifacts = _load_submission_artifacts(resolved_db_path, normalized_run_id)
    submission_manifest = _load_latest_manifest_or_raise(
        resolved_db_path,
        normalized_run_id,
        artifact_kind=ARTIFACT_KIND_TASK_SUBMISSION_MANIFEST,
    )
    runtime_context_manifest = _load_latest_manifest_or_raise(
        resolved_db_path,
        normalized_run_id,
        artifact_kind=ARTIFACT_KIND_TASK_RUNTIME_CONTEXT_MANIFEST,
    )
    return SubmittedTaskInspection(
        run_details=run_details,
        submission_manifest=submission_manifest,
        runtime_context_manifest=runtime_context_manifest,
        artifacts=artifacts,
    )


def list_submitted_tasks(
    database_path: str | Path,
    *,
    project_key: str | None = None,
    limit: int = 100,
) -> list[SubmittedTaskSummary]:
    resolved_db_path = _resolve_database_path(database_path)
    if limit <= 0:
        raise TaskIntakeError(
            code=INTAKE_SUBMISSION_INVALID,
            message="limit must be greater than zero",
            database_path=resolved_db_path,
        )

    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("projects", "runs", "queue_items", "artifact_refs"))
        filters = ["artifact_refs.artifact_kind = ?"]
        params: list[object] = [ARTIFACT_KIND_TASK_SUBMISSION_MANIFEST]
        if project_key is not None:
            filters.append("projects.project_key = ?")
            params.append(_required_text("project_key", project_key, resolved_db_path))
        rows = connection.execute(
            f"""
            SELECT
              runs.id AS run_id,
              runs.flow_id,
              projects.project_key,
              runs.project_profile,
              runs.workflow_id,
              runs.milestone,
              runs.status AS run_status,
              queue_items.status AS queue_status,
              artifact_refs.filesystem_path,
              artifact_refs.created_at
            FROM artifact_refs
            JOIN runs ON runs.id = artifact_refs.run_id
            JOIN projects ON projects.id = runs.project_id
            LEFT JOIN queue_items ON queue_items.run_id = runs.id
            WHERE {" AND ".join(filters)}
            ORDER BY artifact_refs.created_at DESC, artifact_refs.id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
    except sqlite3.Error as exc:
        raise TaskIntakeError(
            code=INTAKE_STORAGE_ERROR,
            message="SQLite submitted task listing failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()

    summaries: list[SubmittedTaskSummary] = []
    seen_runs: set[str] = set()
    for row in rows:
        run_id_value = str(row["run_id"])
        if run_id_value in seen_runs:
            continue
        seen_runs.add(run_id_value)
        submission_manifest = _read_json_optional(Path(str(row["filesystem_path"])).expanduser().resolve()) or {}
        submission = submission_manifest.get("submission")
        source = None
        thread_label = None
        task_text = ""
        if isinstance(submission, Mapping):
            source = _optional_text(submission.get("source"))
            thread_label = _optional_text(submission.get("thread_label"))
            task_text = _optional_text(submission.get("task_text")) or ""
        summaries.append(
            SubmittedTaskSummary(
                run_id=run_id_value,
                flow_id=str(row["flow_id"]),
                project_key=str(row["project_key"]),
                project_profile=str(row["project_profile"]),
                workflow_id=str(row["workflow_id"]),
                milestone=str(row["milestone"]),
                run_status=str(row["run_status"]),
                queue_status=_optional_text(row["queue_status"]),
                task_text=task_text,
                source=source,
                thread_label=thread_label,
                submitted_at=str(row["created_at"]),
            )
        )
    return summaries


def _normalize_submission_request(
    submission_payload: Mapping[str, object],
    database_path: Path,
) -> BoundedTaskSubmissionRequest:
    payload = dict(submission_payload)
    project_key = _required_text("project_key", payload.get("project_key"), database_path)
    task_text = _required_text("task_text", payload.get("task_text"), database_path)
    project_profile = _required_text("project_profile", payload.get("project_profile"), database_path)
    workflow_id = _required_text("workflow_id", payload.get("workflow_id"), database_path)
    milestone = _required_text("milestone", payload.get("milestone"), database_path)
    priority_class = _optional_text(payload.get("priority_class")) or "interactive"
    if priority_class not in PRIORITY_CLASSES:
        raise TaskIntakeError(
            code=INTAKE_SUBMISSION_INVALID,
            message=f"priority_class must be one of: {', '.join(PRIORITY_CLASSES)}",
            database_path=database_path,
            details=f"actual={priority_class}",
        )

    raw_instruction_overlays = payload.get("instruction_overlays")
    instruction_overlays = None
    if raw_instruction_overlays is not None:
        instruction_overlays = tuple(
            _coerce_string_list(
                raw_instruction_overlays,
                database_path=database_path,
                field_name="instruction_overlays",
            )
        )

    return BoundedTaskSubmissionRequest(
        project_key=project_key,
        task_text=task_text,
        project_profile=project_profile,
        workflow_id=workflow_id,
        milestone=milestone,
        priority_class=priority_class,
        instruction_profile=_optional_text(payload.get("instruction_profile")),
        instruction_overlays=instruction_overlays,
        source=_optional_text(payload.get("source")),
        thread_label=_optional_text(payload.get("thread_label")),
        constraints=tuple(
            _coerce_string_list(
                payload.get("constraints"),
                database_path=database_path,
                field_name="constraints",
            )
        ),
        expected_output=tuple(
            _coerce_string_list(
                payload.get("expected_output"),
                database_path=database_path,
                field_name="expected_output",
            )
        ),
        artifact_root=_optional_path(payload.get("artifact_root")),
        workspace_root=_optional_path(payload.get("workspace_root")),
    )


def _build_runtime_context(
    request: BoundedTaskSubmissionRequest,
    *,
    project_package,
    database_path: Path,
) -> dict[str, object]:
    runtime_file = project_package.files[RUNTIME_FILE].data
    instructions_file = project_package.files[INSTRUCTIONS_FILE].data
    runtime_defaults = _optional_mapping(
        runtime_file.get(RUNTIME_DEFAULTS_BLOCK),
        field_name=f"{RUNTIME_FILE}.{RUNTIME_DEFAULTS_BLOCK}",
        database_path=database_path,
    )
    instructions_defaults = _optional_mapping(
        instructions_file.get(INSTRUCTIONS_DEFAULTS_BLOCK),
        field_name=f"{INSTRUCTIONS_FILE}.{INSTRUCTIONS_DEFAULTS_BLOCK}",
        database_path=database_path,
    )

    workspace_root = request.workspace_root or _optional_path(runtime_defaults.get("workspace_root"))
    derived_paths = _derive_paths_from_workspace_root(workspace_root, request.project_key)

    project_repo_path = _optional_path(runtime_defaults.get("project_repo_path")) or derived_paths.get("project_repo_path")
    executor_worktree_path = _optional_path(runtime_defaults.get("executor_worktree_path")) or derived_paths.get("executor_worktree_path")
    reviewer_worktree_path = _optional_path(runtime_defaults.get("reviewer_worktree_path")) or derived_paths.get("reviewer_worktree_path")
    instructions_repo_path = _optional_path(runtime_defaults.get("instructions_repo_path")) or derived_paths.get("instructions_repo_path")
    artifact_root = request.artifact_root or _optional_path(runtime_defaults.get("artifact_root"))

    mode = _optional_text(runtime_defaults.get("mode")) or "executor+reviewer"
    if mode not in {"executor-only", "executor+reviewer"}:
        raise TaskIntakeError(
            code=INTAKE_RUNTIME_CONFIG_INVALID,
            message=f"runtime mode must be 'executor-only' or 'executor+reviewer': {mode}",
            database_path=database_path,
            details=f"field={RUNTIME_FILE}.{RUNTIME_DEFAULTS_BLOCK}.mode",
        )

    branch_base = _optional_text(runtime_defaults.get("branch_base")) or "main"
    auto_commit = _coerce_bool(runtime_defaults.get("auto_commit"), default=False, database_path=database_path, field_name=f"{RUNTIME_FILE}.{RUNTIME_DEFAULTS_BLOCK}.auto_commit")
    source = request.source or _optional_text(runtime_defaults.get("source")) or "task-intake-v1"
    thread_label = request.thread_label or _optional_text(runtime_defaults.get("thread_label")) or f"{request.project_key}-{request.workflow_id}-{request.milestone}"
    instruction_profile = request.instruction_profile or _optional_text(instructions_defaults.get("instruction_profile"))
    instruction_overlays = (
        list(request.instruction_overlays)
        if request.instruction_overlays is not None
        else _coerce_string_list(
            instructions_defaults.get("instruction_overlays"),
            database_path=database_path,
            field_name=f"{INSTRUCTIONS_FILE}.{INSTRUCTIONS_DEFAULTS_BLOCK}.instruction_overlays",
        )
    )

    missing_fields: list[str] = []
    if project_repo_path is None:
        missing_fields.append("project_repo_path")
    if executor_worktree_path is None:
        missing_fields.append("executor_worktree_path")
    if mode == "executor+reviewer" and reviewer_worktree_path is None:
        missing_fields.append("reviewer_worktree_path")
    if instructions_repo_path is None:
        missing_fields.append("instructions_repo_path")
    if instruction_profile is None:
        missing_fields.append("instruction_profile")
    if missing_fields:
        raise TaskIntakeError(
            code=INTAKE_RUNTIME_CONFIG_INVALID,
            message="project package and submission do not provide all required runtime defaults for bounded task intake",
            database_path=database_path,
            details="missing_fields=" + ",".join(missing_fields),
        )

    assert project_repo_path is not None
    assert executor_worktree_path is not None
    assert instructions_repo_path is not None
    runtime_context = {
        "project": request.project_key,
        "task_text": request.task_text,
        "mode": mode,
        "branch_base": branch_base,
        "auto_commit": auto_commit,
        "source": source,
        "thread_label": thread_label,
        "project_repo_path": str(project_repo_path),
        "executor_worktree_path": str(executor_worktree_path),
        "reviewer_worktree_path": str(reviewer_worktree_path) if reviewer_worktree_path is not None else None,
        "instruction_profile": instruction_profile,
        "instruction_overlays": instruction_overlays,
        "instructions_repo_path": str(instructions_repo_path),
        "constraints": list(request.constraints),
        "expected_output": list(request.expected_output),
        "artifact_root": str(artifact_root) if artifact_root is not None else None,
        "workspace_root": str(workspace_root) if workspace_root is not None else None,
    }
    return runtime_context


def _derive_paths_from_workspace_root(workspace_root: Path | None, project_key: str) -> dict[str, Path]:
    if workspace_root is None:
        return {}
    return {
        "project_repo_path": workspace_root / "projects" / project_key,
        "executor_worktree_path": workspace_root / "runtime" / "worktrees" / f"{project_key}-executor",
        "reviewer_worktree_path": workspace_root / "runtime" / "worktrees" / f"{project_key}-reviewer",
        "instructions_repo_path": workspace_root / "instructions",
    }


def _load_registered_project_row(connection: sqlite3.Connection, project_key: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id, project_key, package_root, created_at, updated_at
        FROM projects
        WHERE project_key = ?
        """,
        (project_key,),
    ).fetchone()


def _resolve_submission_artifact_directory(run_details: RunDetails) -> Path:
    if run_details.artifact_directory is not None:
        directory = run_details.artifact_directory / "intake"
    else:
        directory = CONTROL_DIR / ".logs" / "submitted-tasks" / run_details.run.project_key / run_details.run.flow_id / run_details.run.id / "intake"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _record_submission_artifacts(
    database_path: Path,
    run_details: RunDetails,
    *,
    artifact_paths: Sequence[tuple[str, Path]],
) -> tuple[SubmittedTaskArtifact, ...]:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("artifact_refs",))
        created_at = _utc_now()
        connection.execute("BEGIN")
        artifacts: list[SubmittedTaskArtifact] = []
        for artifact_kind, filesystem_path in artifact_paths:
            resolved_path = filesystem_path.expanduser().resolve()
            artifact_id = generate_opaque_id()
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
                    artifact_id,
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
                SubmittedTaskArtifact(
                    id=artifact_id,
                    run_id=run_details.run.id,
                    artifact_kind=artifact_kind,
                    filesystem_path=resolved_path,
                    created_at=created_at,
                )
            )
        connection.commit()
        return tuple(artifacts)
    except (sqlite3.Error, OSError) as exc:
        connection.rollback()
        raise TaskIntakeError(
            code=INTAKE_STORAGE_ERROR,
            message=f"Failed to persist submitted task manifests for run {run_details.run.id}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _load_submission_artifacts(database_path: Path, run_id: str) -> tuple[SubmittedTaskArtifact, ...]:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("artifact_refs",))
        rows = connection.execute(
            """
            SELECT id, run_id, artifact_kind, filesystem_path, created_at
            FROM artifact_refs
            WHERE run_id = ? AND artifact_kind IN (?, ?)
            ORDER BY created_at, id
            """,
            (run_id, ARTIFACT_KIND_TASK_SUBMISSION_MANIFEST, ARTIFACT_KIND_TASK_RUNTIME_CONTEXT_MANIFEST),
        ).fetchall()
        return tuple(
            SubmittedTaskArtifact(
                id=str(row["id"]),
                run_id=str(row["run_id"]),
                artifact_kind=str(row["artifact_kind"]),
                filesystem_path=Path(str(row["filesystem_path"])).expanduser().resolve(),
                created_at=str(row["created_at"]),
            )
            for row in rows
        )
    except sqlite3.Error as exc:
        raise TaskIntakeError(
            code=INTAKE_STORAGE_ERROR,
            message="SQLite submitted task artifact lookup failed",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _load_latest_manifest_or_raise(
    database_path: Path,
    run_id: str,
    *,
    artifact_kind: str,
) -> dict[str, object]:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("artifact_refs",))
        row = connection.execute(
            """
            SELECT filesystem_path
            FROM artifact_refs
            WHERE run_id = ? AND artifact_kind = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (run_id, artifact_kind),
        ).fetchone()
    except sqlite3.Error as exc:
        raise TaskIntakeError(
            code=INTAKE_STORAGE_ERROR,
            message="SQLite submitted task manifest lookup failed",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()

    if row is None:
        raise TaskIntakeError(
            code=INTAKE_MANIFEST_NOT_FOUND,
            message=f"Submitted task manifest is missing for run {run_id}: {artifact_kind}",
            database_path=database_path,
        )
    payload = _read_json_optional(Path(str(row["filesystem_path"])).expanduser().resolve())
    if payload is None:
        raise TaskIntakeError(
            code=INTAKE_MANIFEST_NOT_FOUND,
            message=f"Submitted task manifest is unreadable for run {run_id}: {artifact_kind}",
            database_path=database_path,
        )
    return payload


def _load_run_details_or_raise(database_path: Path, run_id: str) -> RunDetails:
    try:
        return get_run(database_path, run_id)
    except RunPersistenceError as exc:
        raise TaskIntakeError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _optional_mapping(value: object, *, field_name: str, database_path: Path) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TaskIntakeError(
            code=INTAKE_RUNTIME_CONFIG_INVALID,
            message=f"{field_name} must be a mapping/object",
            database_path=database_path,
        )
    return dict(value)


def _coerce_bool(value: object, *, default: bool, database_path: Path, field_name: str) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise TaskIntakeError(
        code=INTAKE_RUNTIME_CONFIG_INVALID,
        message=f"{field_name} must be a boolean",
        database_path=database_path,
        details=f"actual={value!r}",
    )


def _coerce_string_list(value: object, *, database_path: Path, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    raise TaskIntakeError(
        code=INTAKE_SUBMISSION_INVALID,
        message=f"{field_name} must be a string or list of strings",
        database_path=database_path,
        details=f"actual_type={type(value).__name__}",
    )


def _required_text(name: str, value: object, database_path: Path) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise TaskIntakeError(
            code=INTAKE_SUBMISSION_INVALID,
            message=f"{name} must be a non-empty string",
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
    return Path(normalized).expanduser().resolve() if normalized is not None else None


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json_optional(path: Path) -> dict[str, object] | None:
    try:
        raw = path.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _sha256_for_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
