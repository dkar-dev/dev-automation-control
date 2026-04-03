from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess

from .dispatch_adapter import ARTIFACT_KIND_DISPATCH_CONTEXT_MANIFEST
from .id_generation import generate_opaque_id
from .project_package import load_project_package
from .project_package_validator import POLICY_FILE, ProjectPackageValidationFailed
from .run_persistence import RunPersistenceError, _connect_run_db, _ensure_required_tables, _resolve_database_path


CLEANUP_SCOPES = ("artifacts", "worktrees", "branches")
RUNTIME_CLEANUP_RECORD_SCOPES = ("worktree", "branch")

DEFAULT_ARTIFACTS_TTL_SECONDS = 86400
DEFAULT_WORKTREE_TTL_SECONDS = 604800
DEFAULT_BRANCH_TTL_SECONDS = 604800

TERMINAL_RUN_STATUSES = {"completed", "failed", "stopped", "cancelled"}
PROTECTED_BRANCH_FALLBACKS = {"main", "master"}

CLEANUP_MANAGER_STORAGE_ERROR = "CLEANUP_MANAGER_STORAGE_ERROR"
CLEANUP_INVALID_POLICY = "CLEANUP_INVALID_POLICY"
CLEANUP_INVALID_SCOPE = "CLEANUP_INVALID_SCOPE"
CLEANUP_RUN_NOT_FOUND = "CLEANUP_RUN_NOT_FOUND"


@dataclass(frozen=True)
class CleanupPolicy:
    project_key: str
    package_root: Path
    artifacts_ttl_seconds: int
    worktree_ttl_seconds: int
    branch_ttl_seconds: int

    def to_dict(self) -> dict[str, object]:
        return {
            "project_key": self.project_key,
            "package_root": str(self.package_root),
            "artifacts_ttl_seconds": self.artifacts_ttl_seconds,
            "worktree_ttl_seconds": self.worktree_ttl_seconds,
            "branch_ttl_seconds": self.branch_ttl_seconds,
        }


@dataclass(frozen=True)
class CleanupCandidate:
    scope: str
    target_identity: str
    run_ids: tuple[str, ...]
    flow_ids: tuple[str, ...]
    project_keys: tuple[str, ...]
    ttl_seconds: int
    terminal_at: str
    expires_at: str
    filesystem_path: str | None = None
    git_repo_path: str | None = None
    branch_name: str | None = None
    role_hints: tuple[str, ...] = ()
    source: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "target_identity": self.target_identity,
            "run_ids": list(self.run_ids),
            "flow_ids": list(self.flow_ids),
            "project_keys": list(self.project_keys),
            "ttl_seconds": self.ttl_seconds,
            "terminal_at": self.terminal_at,
            "expires_at": self.expires_at,
            "filesystem_path": self.filesystem_path,
            "git_repo_path": self.git_repo_path,
            "branch_name": self.branch_name,
            "role_hints": list(self.role_hints),
            "source": self.source,
        }


@dataclass(frozen=True)
class CleanupCandidateReport:
    as_of: str
    scopes: tuple[str, ...]
    candidates: tuple[CleanupCandidate, ...]

    def to_dict(self) -> dict[str, object]:
        counts = {scope: 0 for scope in CLEANUP_SCOPES}
        for candidate in self.candidates:
            counts[candidate.scope] = counts.get(candidate.scope, 0) + 1
        return {
            "as_of": self.as_of,
            "scopes": list(self.scopes),
            "counts": counts,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


@dataclass(frozen=True)
class CleanupTargetResult:
    scope: str
    target_identity: str
    run_ids: tuple[str, ...]
    action: str
    status: str
    deleted: bool
    filesystem_path: str | None
    git_repo_path: str | None
    branch_name: str | None
    cleaned_at: str | None
    error: str | None = None
    details: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "target_identity": self.target_identity,
            "run_ids": list(self.run_ids),
            "action": self.action,
            "status": self.status,
            "deleted": self.deleted,
            "filesystem_path": self.filesystem_path,
            "git_repo_path": self.git_repo_path,
            "branch_name": self.branch_name,
            "cleaned_at": self.cleaned_at,
            "error": self.error,
            "details": self.details,
        }


@dataclass(frozen=True)
class CleanupPassResult:
    as_of: str
    dry_run: bool
    scopes: tuple[str, ...]
    candidate_report: CleanupCandidateReport
    results: tuple[CleanupTargetResult, ...]

    def to_dict(self) -> dict[str, object]:
        summary = {
            scope: {"processed": 0, "deleted": 0, "errors": 0, "missing": 0, "dry_run": 0}
            for scope in CLEANUP_SCOPES
        }
        for result in self.results:
            bucket = summary[result.scope]
            bucket["processed"] += 1
            if result.deleted:
                bucket["deleted"] += 1
            if result.status == "error":
                bucket["errors"] += 1
            if result.status == "missing":
                bucket["missing"] += 1
            if result.action == "dry_run":
                bucket["dry_run"] += 1
        return {
            "as_of": self.as_of,
            "dry_run": self.dry_run,
            "scopes": list(self.scopes),
            "summary": summary,
            "candidate_report": self.candidate_report.to_dict(),
            "results": [result.to_dict() for result in self.results],
        }


@dataclass(frozen=True)
class CleanupStatusEntry:
    scope: str
    run_id: str
    target_identity: str
    artifact_ref_id: str | None
    record_id: str | None
    artifact_kind: str | None
    filesystem_path: str | None
    git_repo_path: str | None
    branch_name: str | None
    cleaned_at: str | None
    cleanup_status: str | None
    cleanup_result_json: str | None
    last_cleanup_error: str | None
    created_at: str
    updated_at: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "run_id": self.run_id,
            "target_identity": self.target_identity,
            "artifact_ref_id": self.artifact_ref_id,
            "record_id": self.record_id,
            "artifact_kind": self.artifact_kind,
            "filesystem_path": self.filesystem_path,
            "git_repo_path": self.git_repo_path,
            "branch_name": self.branch_name,
            "cleaned_at": self.cleaned_at,
            "cleanup_status": self.cleanup_status,
            "cleanup_result_json": self.cleanup_result_json,
            "last_cleanup_error": self.last_cleanup_error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class CleanupStatusReport:
    as_of: str
    run_id: str | None
    entries: tuple[CleanupStatusEntry, ...]
    eligible_candidates: tuple[CleanupCandidate, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "as_of": self.as_of,
            "run_id": self.run_id,
            "entries": [entry.to_dict() for entry in self.entries],
            "eligible_candidates": [candidate.to_dict() for candidate in self.eligible_candidates],
        }


class CleanupManagerError(Exception):
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
class _RunContext:
    run_id: str
    flow_id: str
    project_id: str
    project_key: str
    package_root: Path
    run_status: str
    queue_status: str | None
    terminal_at: str | None

    @property
    def is_terminal(self) -> bool:
        return self.run_status in TERMINAL_RUN_STATUSES

    @property
    def is_paused(self) -> bool:
        return self.run_status == "paused" or self.queue_status == "paused"


@dataclass(frozen=True)
class _RuntimeTargetEntry:
    scope: str
    run_id: str
    flow_id: str
    project_key: str
    package_root: Path
    target_identity: str
    target_path: str | None
    git_repo_path: str | None
    branch_name: str | None
    branch_base: str | None
    role_hint: str | None
    cleaned_at: str | None
    cleanup_status: str | None
    cleanup_result_json: str | None
    last_cleanup_error: str | None
    source: str


def list_cleanup_candidates(
    database_path: str | Path,
    *,
    now: str | None = None,
    scopes: Iterable[str] | None = None,
) -> CleanupCandidateReport:
    resolved_db_path = _resolve_cleanup_database_path(database_path)
    normalized_scopes = _normalize_scopes(scopes, resolved_db_path)
    as_of_dt = _resolve_now(now, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_cleanup_tables(connection, resolved_db_path)
        run_contexts = _load_run_contexts(connection)
        policies = _load_cleanup_policies(run_contexts, resolved_db_path)
        flow_paused = _build_flow_paused_map(run_contexts)
        runtime_entries = _discover_runtime_targets(connection, resolved_db_path, run_contexts, persist_missing=False)
        candidates = _build_candidates(
            connection,
            resolved_db_path,
            run_contexts=run_contexts,
            policies=policies,
            flow_paused=flow_paused,
            runtime_entries=runtime_entries,
            scopes=normalized_scopes,
            as_of_dt=as_of_dt,
        )
        return CleanupCandidateReport(
            as_of=_format_timestamp(as_of_dt),
            scopes=normalized_scopes,
            candidates=tuple(candidates),
        )
    except CleanupManagerError:
        raise
    except sqlite3.Error as exc:
        raise CleanupManagerError(
            code=CLEANUP_MANAGER_STORAGE_ERROR,
            message="SQLite cleanup candidate listing failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def run_cleanup_once(
    database_path: str | Path,
    *,
    dry_run: bool = False,
    now: str | None = None,
    scopes: Iterable[str] | None = None,
) -> CleanupPassResult:
    resolved_db_path = _resolve_cleanup_database_path(database_path)
    normalized_scopes = _normalize_scopes(scopes, resolved_db_path)
    as_of_dt = _resolve_now(now, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_cleanup_tables(connection, resolved_db_path)
        run_contexts = _load_run_contexts(connection)
        policies = _load_cleanup_policies(run_contexts, resolved_db_path)
        flow_paused = _build_flow_paused_map(run_contexts)
        runtime_entries = _discover_runtime_targets(
            connection,
            resolved_db_path,
            run_contexts,
            persist_missing=not dry_run,
        )
        candidates = _build_candidates(
            connection,
            resolved_db_path,
            run_contexts=run_contexts,
            policies=policies,
            flow_paused=flow_paused,
            runtime_entries=runtime_entries,
            scopes=normalized_scopes,
            as_of_dt=as_of_dt,
        )
        candidate_report = CleanupCandidateReport(
            as_of=_format_timestamp(as_of_dt),
            scopes=normalized_scopes,
            candidates=tuple(candidates),
        )
        results: list[CleanupTargetResult] = []
        for scope in ("artifacts", "worktrees", "branches"):
            if scope not in normalized_scopes:
                continue
            for candidate in [item for item in candidates if item.scope == scope]:
                if scope == "artifacts":
                    result = _execute_artifact_cleanup(
                        connection,
                        resolved_db_path,
                        candidate,
                        dry_run=dry_run,
                        cleaned_at=_format_timestamp(as_of_dt),
                    )
                elif scope == "worktrees":
                    result = _execute_worktree_cleanup(
                        connection,
                        resolved_db_path,
                        candidate,
                        dry_run=dry_run,
                        cleaned_at=_format_timestamp(as_of_dt),
                    )
                else:
                    result = _execute_branch_cleanup(
                        connection,
                        resolved_db_path,
                        candidate,
                        dry_run=dry_run,
                        cleaned_at=_format_timestamp(as_of_dt),
                    )
                results.append(result)
        return CleanupPassResult(
            as_of=_format_timestamp(as_of_dt),
            dry_run=dry_run,
            scopes=normalized_scopes,
            candidate_report=candidate_report,
            results=tuple(results),
        )
    except CleanupManagerError:
        raise
    except sqlite3.Error as exc:
        raise CleanupManagerError(
            code=CLEANUP_MANAGER_STORAGE_ERROR,
            message="SQLite cleanup pass failed",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def show_cleanup_status(
    database_path: str | Path,
    *,
    run_id: str | None = None,
    limit: int = 200,
    now: str | None = None,
) -> CleanupStatusReport:
    resolved_db_path = _resolve_cleanup_database_path(database_path)
    normalized_run_id = _normalize_optional_text(run_id)
    as_of_dt = _resolve_now(now, resolved_db_path)
    connection = _connect_run_db(resolved_db_path)
    try:
        _ensure_cleanup_tables(connection, resolved_db_path)
        if normalized_run_id is not None and not _run_exists(connection, normalized_run_id):
            raise CleanupManagerError(
                code=CLEANUP_RUN_NOT_FOUND,
                message=f"run is not present in SQLite: {normalized_run_id}",
                database_path=resolved_db_path,
            )
        entries = _load_cleanup_status_entries(connection, run_id=normalized_run_id, limit=limit)
    finally:
        connection.close()
    candidate_report = list_cleanup_candidates(resolved_db_path, now=_format_timestamp(as_of_dt))
    eligible = tuple(
        candidate
        for candidate in candidate_report.candidates
        if normalized_run_id is None or normalized_run_id in candidate.run_ids
    )
    return CleanupStatusReport(
        as_of=_format_timestamp(as_of_dt),
        run_id=normalized_run_id,
        entries=tuple(entries),
        eligible_candidates=eligible,
    )


def _build_candidates(
    connection: sqlite3.Connection,
    database_path: Path,
    *,
    run_contexts: dict[str, _RunContext],
    policies: dict[str, CleanupPolicy],
    flow_paused: dict[str, bool],
    runtime_entries: list[_RuntimeTargetEntry],
    scopes: tuple[str, ...],
    as_of_dt: datetime,
) -> list[CleanupCandidate]:
    candidates: list[CleanupCandidate] = []
    if "artifacts" in scopes:
        candidates.extend(_load_artifact_candidates(connection, run_contexts, policies, flow_paused, as_of_dt))
    if "worktrees" in scopes:
        candidates.extend(
            _load_runtime_cleanup_candidates(
                scope="worktree",
                entries=runtime_entries,
                run_contexts=run_contexts,
                policies=policies,
                flow_paused=flow_paused,
                as_of_dt=as_of_dt,
            )
        )
    if "branches" in scopes:
        candidates.extend(
            _load_runtime_cleanup_candidates(
                scope="branch",
                entries=runtime_entries,
                run_contexts=run_contexts,
                policies=policies,
                flow_paused=flow_paused,
                as_of_dt=as_of_dt,
            )
        )
    candidates.sort(key=lambda candidate: (candidate.scope, candidate.expires_at, candidate.target_identity))
    return candidates


def _load_artifact_candidates(
    connection: sqlite3.Connection,
    run_contexts: dict[str, _RunContext],
    policies: dict[str, CleanupPolicy],
    flow_paused: dict[str, bool],
    as_of_dt: datetime,
) -> list[CleanupCandidate]:
    rows = connection.execute(
        """
        SELECT
          artifact_refs.id,
          artifact_refs.run_id,
          artifact_refs.artifact_kind,
          artifact_refs.filesystem_path,
          artifact_refs.cleaned_at
        FROM artifact_refs
        ORDER BY artifact_refs.created_at, artifact_refs.id
        """
    ).fetchall()
    candidates: list[CleanupCandidate] = []
    for row in rows:
        if row["cleaned_at"] is not None:
            continue
        run_context = run_contexts.get(str(row["run_id"]))
        if run_context is None or not run_context.is_terminal:
            continue
        if flow_paused.get(run_context.flow_id, False):
            continue
        terminal_at = _parse_timestamp(run_context.terminal_at)
        if terminal_at is None:
            continue
        policy = policies[run_context.project_key]
        expires_at = terminal_at + timedelta(seconds=policy.artifacts_ttl_seconds)
        if as_of_dt < expires_at:
            continue
        candidates.append(
            CleanupCandidate(
                scope="artifacts",
                target_identity=str(row["id"]),
                run_ids=(run_context.run_id,),
                flow_ids=(run_context.flow_id,),
                project_keys=(run_context.project_key,),
                ttl_seconds=policy.artifacts_ttl_seconds,
                terminal_at=_format_timestamp(terminal_at),
                expires_at=_format_timestamp(expires_at),
                filesystem_path=str(Path(str(row["filesystem_path"])).expanduser().resolve()),
                source=str(row["artifact_kind"]),
            )
        )
    return candidates


def _load_runtime_cleanup_candidates(
    *,
    scope: str,
    entries: list[_RuntimeTargetEntry],
    run_contexts: dict[str, _RunContext],
    policies: dict[str, CleanupPolicy],
    flow_paused: dict[str, bool],
    as_of_dt: datetime,
) -> list[CleanupCandidate]:
    grouped: dict[str, list[_RuntimeTargetEntry]] = defaultdict(list)
    for entry in entries:
        if entry.scope == scope:
            grouped[entry.target_identity].append(entry)

    candidates: list[CleanupCandidate] = []
    for target_identity, group in grouped.items():
        associated_runs = [run_contexts[entry.run_id] for entry in group if entry.run_id in run_contexts]
        if not associated_runs:
            continue
        if any(not run_context.is_terminal for run_context in associated_runs):
            continue
        if any(flow_paused.get(run_context.flow_id, False) for run_context in associated_runs):
            continue
        terminal_times = [_parse_timestamp(run_context.terminal_at) for run_context in associated_runs]
        if any(value is None for value in terminal_times):
            continue
        ttl_seconds = max(
            policies[run_context.project_key].worktree_ttl_seconds if scope == "worktree" else policies[run_context.project_key].branch_ttl_seconds
            for run_context in associated_runs
        )
        terminal_at = max(value for value in terminal_times if value is not None)
        expires_at = terminal_at + timedelta(seconds=ttl_seconds)
        if as_of_dt < expires_at:
            continue
        if all(entry.cleaned_at is not None for entry in group):
            continue
        first = group[0]
        branch_name = first.branch_name if scope == "branch" else None
        if scope == "branch":
            protected = set(PROTECTED_BRANCH_FALLBACKS)
            for entry in group:
                if entry.branch_base:
                    protected.add(entry.branch_base)
            if branch_name is None or branch_name in protected:
                continue
        candidates.append(
            CleanupCandidate(
                scope="worktrees" if scope == "worktree" else "branches",
                target_identity=target_identity,
                run_ids=tuple(sorted({run_context.run_id for run_context in associated_runs})),
                flow_ids=tuple(sorted({run_context.flow_id for run_context in associated_runs})),
                project_keys=tuple(sorted({run_context.project_key for run_context in associated_runs})),
                ttl_seconds=ttl_seconds,
                terminal_at=_format_timestamp(terminal_at),
                expires_at=_format_timestamp(expires_at),
                filesystem_path=first.target_path,
                git_repo_path=first.git_repo_path,
                branch_name=branch_name,
                role_hints=tuple(sorted({entry.role_hint for entry in group if entry.role_hint})),
                source="+".join(sorted({entry.source for entry in group})),
            )
        )
    return candidates


def _execute_artifact_cleanup(
    connection: sqlite3.Connection,
    database_path: Path,
    candidate: CleanupCandidate,
    *,
    dry_run: bool,
    cleaned_at: str,
) -> CleanupTargetResult:
    path = Path(candidate.filesystem_path or "").expanduser().resolve()
    if dry_run:
        path_exists = _path_exists(path)
        return CleanupTargetResult(
            scope="artifacts",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="dry_run",
            status="would_delete" if path_exists else "would_mark_missing",
            deleted=False,
            filesystem_path=str(path),
            git_repo_path=None,
            branch_name=None,
            cleaned_at=None,
            details={"path_exists": path_exists},
        )

    try:
        details = _delete_artifact_path(path)
        status = "deleted" if details["deleted"] else "missing"
        connection.execute(
            """
            UPDATE artifact_refs
            SET cleaned_at = ?, cleanup_status = ?, cleanup_result_json = ?, last_cleanup_error = NULL
            WHERE id = ?
            """,
            (cleaned_at, status, json.dumps(details, sort_keys=True), candidate.target_identity),
        )
        connection.commit()
        return CleanupTargetResult(
            scope="artifacts",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="cleanup",
            status=status,
            deleted=bool(details["deleted"]),
            filesystem_path=str(path),
            git_repo_path=None,
            branch_name=None,
            cleaned_at=cleaned_at,
            details=details,
        )
    except (OSError, ValueError, sqlite3.Error) as exc:
        connection.rollback()
        connection.execute(
            """
            UPDATE artifact_refs
            SET cleanup_status = ?, cleanup_result_json = ?, last_cleanup_error = ?
            WHERE id = ?
            """,
            ("error", None, str(exc), candidate.target_identity),
        )
        connection.commit()
        return CleanupTargetResult(
            scope="artifacts",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="cleanup",
            status="error",
            deleted=False,
            filesystem_path=str(path),
            git_repo_path=None,
            branch_name=None,
            cleaned_at=None,
            error=str(exc),
        )


def _execute_worktree_cleanup(
    connection: sqlite3.Connection,
    database_path: Path,
    candidate: CleanupCandidate,
    *,
    dry_run: bool,
    cleaned_at: str,
) -> CleanupTargetResult:
    worktree_path = Path(candidate.filesystem_path or "").expanduser().resolve()
    repo_path = Path(candidate.git_repo_path).expanduser().resolve() if candidate.git_repo_path else None
    if dry_run:
        path_exists = _path_exists(worktree_path)
        return CleanupTargetResult(
            scope="worktrees",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="dry_run",
            status="would_delete" if path_exists else "would_mark_missing",
            deleted=False,
            filesystem_path=str(worktree_path),
            git_repo_path=str(repo_path) if repo_path else None,
            branch_name=None,
            cleaned_at=None,
            details={"path_exists": path_exists},
        )

    try:
        details = _delete_worktree_path(worktree_path, repo_path=repo_path)
        _update_runtime_cleanup_records(
            connection,
            run_ids=candidate.run_ids,
            scope="worktree",
            target_identity=candidate.target_identity,
            cleaned_at=cleaned_at,
            cleanup_status="deleted" if details["deleted"] else "missing",
            cleanup_result_json=json.dumps(details, sort_keys=True),
            last_cleanup_error=None,
        )
        connection.commit()
        return CleanupTargetResult(
            scope="worktrees",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="cleanup",
            status="deleted" if details["deleted"] else "missing",
            deleted=bool(details["deleted"]),
            filesystem_path=str(worktree_path),
            git_repo_path=str(repo_path) if repo_path else None,
            branch_name=None,
            cleaned_at=cleaned_at,
            details=details,
        )
    except (OSError, ValueError, sqlite3.Error) as exc:
        connection.rollback()
        _update_runtime_cleanup_records(
            connection,
            run_ids=candidate.run_ids,
            scope="worktree",
            target_identity=candidate.target_identity,
            cleaned_at=None,
            cleanup_status="error",
            cleanup_result_json=None,
            last_cleanup_error=str(exc),
        )
        connection.commit()
        return CleanupTargetResult(
            scope="worktrees",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="cleanup",
            status="error",
            deleted=False,
            filesystem_path=str(worktree_path),
            git_repo_path=str(repo_path) if repo_path else None,
            branch_name=None,
            cleaned_at=None,
            error=str(exc),
        )


def _execute_branch_cleanup(
    connection: sqlite3.Connection,
    database_path: Path,
    candidate: CleanupCandidate,
    *,
    dry_run: bool,
    cleaned_at: str,
) -> CleanupTargetResult:
    repo_path = Path(candidate.git_repo_path or "").expanduser().resolve()
    branch_name = candidate.branch_name or ""
    if dry_run:
        branch_exists = _branch_exists(repo_path, branch_name)
        return CleanupTargetResult(
            scope="branches",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="dry_run",
            status="would_delete" if branch_exists else "would_mark_missing",
            deleted=False,
            filesystem_path=candidate.filesystem_path,
            git_repo_path=str(repo_path),
            branch_name=branch_name,
            cleaned_at=None,
            details={"branch_exists": branch_exists},
        )

    try:
        details = _delete_branch(repo_path, branch_name)
        _update_runtime_cleanup_records(
            connection,
            run_ids=candidate.run_ids,
            scope="branch",
            target_identity=candidate.target_identity,
            cleaned_at=cleaned_at,
            cleanup_status="deleted" if details["deleted"] else "missing",
            cleanup_result_json=json.dumps(details, sort_keys=True),
            last_cleanup_error=None,
        )
        connection.commit()
        return CleanupTargetResult(
            scope="branches",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="cleanup",
            status="deleted" if details["deleted"] else "missing",
            deleted=bool(details["deleted"]),
            filesystem_path=candidate.filesystem_path,
            git_repo_path=str(repo_path),
            branch_name=branch_name,
            cleaned_at=cleaned_at,
            details=details,
        )
    except (OSError, ValueError, sqlite3.Error) as exc:
        connection.rollback()
        _update_runtime_cleanup_records(
            connection,
            run_ids=candidate.run_ids,
            scope="branch",
            target_identity=candidate.target_identity,
            cleaned_at=None,
            cleanup_status="error",
            cleanup_result_json=None,
            last_cleanup_error=str(exc),
        )
        connection.commit()
        return CleanupTargetResult(
            scope="branches",
            target_identity=candidate.target_identity,
            run_ids=candidate.run_ids,
            action="cleanup",
            status="error",
            deleted=False,
            filesystem_path=candidate.filesystem_path,
            git_repo_path=str(repo_path),
            branch_name=branch_name,
            cleaned_at=None,
            error=str(exc),
        )


def _discover_runtime_targets(
    connection: sqlite3.Connection,
    database_path: Path,
    run_contexts: dict[str, _RunContext],
    *,
    persist_missing: bool,
) -> list[_RuntimeTargetEntry]:
    records = _load_runtime_cleanup_entries(connection)
    manifest_entries = _load_runtime_targets_from_manifests(connection, run_contexts)
    if persist_missing:
        _backfill_runtime_cleanup_records(connection, manifest_entries)
        connection.commit()
        records = _load_runtime_cleanup_entries(connection)

    existing_keys = {(entry.run_id, entry.scope, entry.target_identity) for entry in records}
    combined = list(records)
    for entry in manifest_entries:
        key = (entry.run_id, entry.scope, entry.target_identity)
        if key not in existing_keys:
            combined.append(entry)
    return combined


def _load_runtime_cleanup_entries(connection: sqlite3.Connection) -> list[_RuntimeTargetEntry]:
    rows = connection.execute(
        """
        SELECT
          runtime_cleanup_records.run_id,
          runtime_cleanup_records.flow_id,
          projects.project_key,
          projects.package_root,
          runtime_cleanup_records.cleanup_scope,
          runtime_cleanup_records.target_identity,
          runtime_cleanup_records.target_path,
          runtime_cleanup_records.git_repo_path,
          runtime_cleanup_records.role_hint,
          runtime_cleanup_records.cleaned_at,
          runtime_cleanup_records.cleanup_status,
          runtime_cleanup_records.cleanup_result_json,
          runtime_cleanup_records.last_cleanup_error
        FROM runtime_cleanup_records
        JOIN runs ON runs.id = runtime_cleanup_records.run_id
        JOIN projects ON projects.id = runs.project_id
        ORDER BY runtime_cleanup_records.created_at, runtime_cleanup_records.id
        """
    ).fetchall()
    entries: list[_RuntimeTargetEntry] = []
    for row in rows:
        scope = str(row["cleanup_scope"])
        target_identity = str(row["target_identity"])
        branch_name = None
        if scope == "branch":
            _, _, branch_name = target_identity.partition("::")
        entries.append(
            _RuntimeTargetEntry(
                scope=scope,
                run_id=str(row["run_id"]),
                flow_id=str(row["flow_id"]),
                project_key=str(row["project_key"]),
                package_root=Path(str(row["package_root"])).expanduser().resolve(),
                target_identity=target_identity,
                target_path=_normalize_optional_text(row["target_path"]),
                git_repo_path=_normalize_optional_text(row["git_repo_path"]),
                branch_name=_normalize_optional_text(branch_name),
                branch_base=None,
                role_hint=_normalize_optional_text(row["role_hint"]),
                cleaned_at=_normalize_optional_text(row["cleaned_at"]),
                cleanup_status=_normalize_optional_text(row["cleanup_status"]),
                cleanup_result_json=_normalize_optional_text(row["cleanup_result_json"]),
                last_cleanup_error=_normalize_optional_text(row["last_cleanup_error"]),
                source="cleanup_record",
            )
        )
    return entries


def _load_runtime_targets_from_manifests(
    connection: sqlite3.Connection,
    run_contexts: dict[str, _RunContext],
) -> list[_RuntimeTargetEntry]:
    rows = connection.execute(
        """
        SELECT
          artifact_refs.run_id,
          artifact_refs.flow_id,
          artifact_refs.filesystem_path,
          step_runs.step_key
        FROM artifact_refs
        LEFT JOIN step_runs ON step_runs.id = artifact_refs.step_run_id
        WHERE artifact_refs.artifact_kind = ?
        ORDER BY artifact_refs.created_at, artifact_refs.id
        """,
        (ARTIFACT_KIND_DISPATCH_CONTEXT_MANIFEST,),
    ).fetchall()
    entries: dict[tuple[str, str, str], _RuntimeTargetEntry] = {}
    for row in rows:
        run_id = str(row["run_id"])
        run_context = run_contexts.get(run_id)
        if run_context is None:
            continue
        payload = _read_json_optional(Path(str(row["filesystem_path"])).expanduser().resolve())
        if not isinstance(payload, dict):
            continue
        runtime_context = payload.get("runtime_context")
        if not isinstance(runtime_context, dict):
            continue
        role = _normalize_optional_text(payload.get("resolved_role")) or _normalize_optional_text(row["step_key"])
        if role not in {"executor", "reviewer"}:
            continue
        worktree_value = _normalize_optional_text(
            runtime_context.get("executor_worktree_path") if role == "executor" else runtime_context.get("reviewer_worktree_path")
        )
        project_repo_value = _normalize_optional_text(runtime_context.get("project_repo_path"))
        branch_base = _normalize_optional_text(runtime_context.get("branch_base"))
        if worktree_value is None:
            continue
        worktree_path = Path(worktree_value).expanduser().resolve()
        repo_path = Path(project_repo_value).expanduser().resolve() if project_repo_value is not None else None
        worktree_identity = str(worktree_path)
        entries[(run_id, "worktree", worktree_identity)] = _RuntimeTargetEntry(
            scope="worktree",
            run_id=run_id,
            flow_id=run_context.flow_id,
            project_key=run_context.project_key,
            package_root=run_context.package_root,
            target_identity=worktree_identity,
            target_path=str(worktree_path),
            git_repo_path=str(repo_path) if repo_path is not None else None,
            branch_name=None,
            branch_base=branch_base,
            role_hint=role,
            cleaned_at=None,
            cleanup_status=None,
            cleanup_result_json=None,
            last_cleanup_error=None,
            source="dispatch_manifest",
        )
        branch_name = _current_branch_name(worktree_path)
        if branch_name is None:
            continue
        protected = set(PROTECTED_BRANCH_FALLBACKS)
        if branch_base is not None:
            protected.add(branch_base)
        if branch_name in protected:
            continue
        branch_repo_path = repo_path if repo_path is not None else worktree_path
        branch_identity = f"{branch_repo_path}::{branch_name}"
        entries[(run_id, "branch", branch_identity)] = _RuntimeTargetEntry(
            scope="branch",
            run_id=run_id,
            flow_id=run_context.flow_id,
            project_key=run_context.project_key,
            package_root=run_context.package_root,
            target_identity=branch_identity,
            target_path=str(worktree_path),
            git_repo_path=str(branch_repo_path),
            branch_name=branch_name,
            branch_base=branch_base,
            role_hint=role,
            cleaned_at=None,
            cleanup_status=None,
            cleanup_result_json=None,
            last_cleanup_error=None,
            source="dispatch_manifest",
        )
    return list(entries.values())


def _backfill_runtime_cleanup_records(connection: sqlite3.Connection, entries: list[_RuntimeTargetEntry]) -> None:
    now = _utc_now()
    for entry in entries:
        connection.execute(
            """
            INSERT INTO runtime_cleanup_records (
              id,
              run_id,
              flow_id,
              cleanup_scope,
              target_identity,
              target_path,
              git_repo_path,
              role_hint,
              cleaned_at,
              cleanup_status,
              cleanup_result_json,
              last_cleanup_error,
              created_at,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?)
            ON CONFLICT(run_id, cleanup_scope, target_identity)
            DO UPDATE SET
              target_path = excluded.target_path,
              git_repo_path = excluded.git_repo_path,
              role_hint = excluded.role_hint,
              updated_at = excluded.updated_at
            """,
            (
                generate_opaque_id(),
                entry.run_id,
                entry.flow_id,
                entry.scope,
                entry.target_identity,
                entry.target_path,
                entry.git_repo_path,
                entry.role_hint,
                now,
                now,
            ),
        )


def _update_runtime_cleanup_records(
    connection: sqlite3.Connection,
    *,
    run_ids: Iterable[str],
    scope: str,
    target_identity: str,
    cleaned_at: str | None,
    cleanup_status: str,
    cleanup_result_json: str | None,
    last_cleanup_error: str | None,
) -> None:
    updated_at = _utc_now()
    for run_id in run_ids:
        connection.execute(
            """
            UPDATE runtime_cleanup_records
            SET cleaned_at = ?, cleanup_status = ?, cleanup_result_json = ?, last_cleanup_error = ?, updated_at = ?
            WHERE run_id = ? AND cleanup_scope = ? AND target_identity = ?
            """,
            (cleaned_at, cleanup_status, cleanup_result_json, last_cleanup_error, updated_at, run_id, scope, target_identity),
        )


def _load_cleanup_status_entries(
    connection: sqlite3.Connection,
    *,
    run_id: str | None,
    limit: int,
) -> list[CleanupStatusEntry]:
    normalized_limit = max(1, limit)
    artifact_rows = connection.execute(
        """
        SELECT
          artifact_refs.id,
          artifact_refs.run_id,
          artifact_refs.artifact_kind,
          artifact_refs.filesystem_path,
          artifact_refs.cleaned_at,
          artifact_refs.cleanup_status,
          artifact_refs.cleanup_result_json,
          artifact_refs.last_cleanup_error,
          artifact_refs.created_at
        FROM artifact_refs
        WHERE (? IS NULL OR artifact_refs.run_id = ?)
        ORDER BY artifact_refs.created_at DESC, artifact_refs.id DESC
        """,
        (run_id, run_id),
    ).fetchall()
    runtime_rows = connection.execute(
        """
        SELECT
          runtime_cleanup_records.id,
          runtime_cleanup_records.run_id,
          runtime_cleanup_records.cleanup_scope,
          runtime_cleanup_records.target_identity,
          runtime_cleanup_records.target_path,
          runtime_cleanup_records.git_repo_path,
          runtime_cleanup_records.role_hint,
          runtime_cleanup_records.cleaned_at,
          runtime_cleanup_records.cleanup_status,
          runtime_cleanup_records.cleanup_result_json,
          runtime_cleanup_records.last_cleanup_error,
          runtime_cleanup_records.created_at,
          runtime_cleanup_records.updated_at
        FROM runtime_cleanup_records
        WHERE (? IS NULL OR runtime_cleanup_records.run_id = ?)
        ORDER BY runtime_cleanup_records.updated_at DESC, runtime_cleanup_records.created_at DESC, runtime_cleanup_records.id DESC
        """,
        (run_id, run_id),
    ).fetchall()

    entries: list[CleanupStatusEntry] = []
    for row in artifact_rows:
        entries.append(
            CleanupStatusEntry(
                scope="artifacts",
                run_id=str(row["run_id"]),
                target_identity=str(row["id"]),
                artifact_ref_id=str(row["id"]),
                record_id=None,
                artifact_kind=str(row["artifact_kind"]),
                filesystem_path=_normalize_optional_text(row["filesystem_path"]),
                git_repo_path=None,
                branch_name=None,
                cleaned_at=_normalize_optional_text(row["cleaned_at"]),
                cleanup_status=_normalize_optional_text(row["cleanup_status"]),
                cleanup_result_json=_normalize_optional_text(row["cleanup_result_json"]),
                last_cleanup_error=_normalize_optional_text(row["last_cleanup_error"]),
                created_at=str(row["created_at"]),
                updated_at=_normalize_optional_text(row["cleaned_at"]),
            )
        )

    for row in runtime_rows:
        scope = str(row["cleanup_scope"])
        branch_name = None
        if scope == "branch":
            _, _, branch_name = str(row["target_identity"]).partition("::")
        entries.append(
            CleanupStatusEntry(
                scope="worktrees" if scope == "worktree" else "branches",
                run_id=str(row["run_id"]),
                target_identity=str(row["target_identity"]),
                artifact_ref_id=None,
                record_id=str(row["id"]),
                artifact_kind=None,
                filesystem_path=_normalize_optional_text(row["target_path"]),
                git_repo_path=_normalize_optional_text(row["git_repo_path"]),
                branch_name=_normalize_optional_text(branch_name),
                cleaned_at=_normalize_optional_text(row["cleaned_at"]),
                cleanup_status=_normalize_optional_text(row["cleanup_status"]),
                cleanup_result_json=_normalize_optional_text(row["cleanup_result_json"]),
                last_cleanup_error=_normalize_optional_text(row["last_cleanup_error"]),
                created_at=str(row["created_at"]),
                updated_at=_normalize_optional_text(row["updated_at"]),
            )
        )

    entries.sort(
        key=lambda entry: (
            entry.updated_at or entry.created_at,
            entry.scope,
            entry.target_identity,
        ),
        reverse=True,
    )
    return entries[:normalized_limit]


def _load_run_contexts(connection: sqlite3.Connection) -> dict[str, _RunContext]:
    rows = connection.execute(
        """
        SELECT
          runs.id,
          runs.flow_id,
          runs.project_id,
          projects.project_key,
          projects.package_root,
          runs.status,
          queue_items.status AS queue_status,
          runs.terminal_at
        FROM runs
        JOIN projects ON projects.id = runs.project_id
        LEFT JOIN queue_items ON queue_items.run_id = runs.id
        ORDER BY runs.created_at, runs.id
        """
    ).fetchall()
    contexts: dict[str, _RunContext] = {}
    for row in rows:
        contexts[str(row["id"])] = _RunContext(
            run_id=str(row["id"]),
            flow_id=str(row["flow_id"]),
            project_id=str(row["project_id"]),
            project_key=str(row["project_key"]),
            package_root=Path(str(row["package_root"])).expanduser().resolve(),
            run_status=str(row["status"]),
            queue_status=_normalize_optional_text(row["queue_status"]),
            terminal_at=_normalize_optional_text(row["terminal_at"]),
        )
    return contexts


def _load_cleanup_policies(
    run_contexts: dict[str, _RunContext],
    database_path: Path,
) -> dict[str, CleanupPolicy]:
    policies: dict[str, CleanupPolicy] = {}
    for run_context in run_contexts.values():
        if run_context.project_key in policies:
            continue
        policies[run_context.project_key] = _load_cleanup_policy_for_package(
            run_context.package_root,
            database_path=database_path,
        )
    return policies


def _load_cleanup_policy_for_package(package_root: Path, *, database_path: Path) -> CleanupPolicy:
    try:
        project_package = load_project_package(package_root)
    except ProjectPackageValidationFailed as exc:
        raise CleanupManagerError(
            code=CLEANUP_INVALID_POLICY,
            message=f"Project package is not valid for cleanup policy loading: {package_root}",
            database_path=database_path,
            details="; ".join(error.message for error in exc.errors),
        ) from exc

    raw_policy = project_package.files[POLICY_FILE].data
    cleanup_block = raw_policy.get("cleanup_v1", {})
    if cleanup_block is None:
        cleanup_block = {}
    if not isinstance(cleanup_block, dict):
        raise CleanupManagerError(
            code=CLEANUP_INVALID_POLICY,
            message=f"policy.yaml cleanup_v1 must be a mapping: {project_package.package_root}",
            database_path=database_path,
        )

    return CleanupPolicy(
        project_key=project_package.project_key,
        package_root=project_package.package_root,
        artifacts_ttl_seconds=_parse_ttl(
            cleanup_block.get("artifacts_ttl_seconds"),
            default_value=DEFAULT_ARTIFACTS_TTL_SECONDS,
            field_name="cleanup_v1.artifacts_ttl_seconds",
            package_root=project_package.package_root,
            database_path=database_path,
        ),
        worktree_ttl_seconds=_parse_ttl(
            cleanup_block.get("worktree_ttl_seconds"),
            default_value=DEFAULT_WORKTREE_TTL_SECONDS,
            field_name="cleanup_v1.worktree_ttl_seconds",
            package_root=project_package.package_root,
            database_path=database_path,
        ),
        branch_ttl_seconds=_parse_ttl(
            cleanup_block.get("branch_ttl_seconds"),
            default_value=DEFAULT_BRANCH_TTL_SECONDS,
            field_name="cleanup_v1.branch_ttl_seconds",
            package_root=project_package.package_root,
            database_path=database_path,
        ),
    )


def _build_flow_paused_map(run_contexts: dict[str, _RunContext]) -> dict[str, bool]:
    flow_paused: dict[str, bool] = defaultdict(bool)
    for run_context in run_contexts.values():
        if run_context.is_paused:
            flow_paused[run_context.flow_id] = True
        else:
            flow_paused.setdefault(run_context.flow_id, False)
    return dict(flow_paused)


def _delete_artifact_path(path: Path) -> dict[str, object]:
    resolved_path = _validate_deletable_path(path)
    if not _path_exists(resolved_path):
        return {
            "deleted": False,
            "path_exists": False,
            "path_type": "missing",
            "filesystem_path": str(resolved_path),
        }
    if resolved_path.is_dir() and not resolved_path.is_symlink():
        shutil.rmtree(resolved_path)
        path_type = "directory"
    else:
        resolved_path.unlink()
        path_type = "file"
    return {
        "deleted": True,
        "path_exists": False,
        "path_type": path_type,
        "filesystem_path": str(resolved_path),
    }


def _delete_worktree_path(worktree_path: Path, *, repo_path: Path | None) -> dict[str, object]:
    protected_paths = tuple(path for path in (repo_path,) if path is not None)
    resolved_worktree = _validate_deletable_path(worktree_path, protected_paths=protected_paths)
    resolved_repo = repo_path.expanduser().resolve() if repo_path is not None else None
    if not _path_exists(resolved_worktree):
        return {
            "deleted": False,
            "path_exists": False,
            "removal_mode": "missing",
            "filesystem_path": str(resolved_worktree),
            "git_repo_path": str(resolved_repo) if resolved_repo is not None else None,
        }

    details: dict[str, object] = {
        "deleted": False,
        "path_exists": True,
        "filesystem_path": str(resolved_worktree),
        "git_repo_path": str(resolved_repo) if resolved_repo is not None else None,
        "removal_mode": "filesystem",
    }

    if resolved_repo is not None and resolved_repo.exists():
        git_remove = subprocess.run(
            ["git", "-C", str(resolved_repo), "worktree", "remove", "--force", str(resolved_worktree)],
            capture_output=True,
            text=True,
            check=False,
        )
        details["git_worktree_remove_exit_code"] = git_remove.returncode
        if git_remove.stdout.strip():
            details["git_stdout"] = git_remove.stdout.strip()
        if git_remove.stderr.strip():
            details["git_stderr"] = git_remove.stderr.strip()
        if git_remove.returncode == 0:
            details["removal_mode"] = "git_worktree_remove"
            details["deleted"] = not _path_exists(resolved_worktree)
            return details

    if resolved_worktree.is_dir() and not resolved_worktree.is_symlink():
        shutil.rmtree(resolved_worktree)
    else:
        resolved_worktree.unlink()
    details["deleted"] = True
    return details


def _delete_branch(repo_path: Path, branch_name: str) -> dict[str, object]:
    resolved_repo = repo_path.expanduser().resolve()
    normalized_branch = branch_name.strip()
    if not normalized_branch:
        raise ValueError("branch_name is required for branch cleanup")
    if normalized_branch in PROTECTED_BRANCH_FALLBACKS:
        raise ValueError(f"branch cleanup refuses to delete protected branch: {normalized_branch}")
    if not resolved_repo.is_dir():
        raise ValueError(f"git_repo_path does not exist: {resolved_repo}")
    current_branch = _current_branch_name(resolved_repo)
    if current_branch == normalized_branch:
        raise ValueError(f"branch cleanup refuses to delete the current checked-out branch: {normalized_branch}")
    if not _branch_exists(resolved_repo, normalized_branch):
        return {
            "deleted": False,
            "branch_exists": False,
            "git_repo_path": str(resolved_repo),
            "branch_name": normalized_branch,
            "current_branch": current_branch,
        }
    delete_proc = subprocess.run(
        ["git", "-C", str(resolved_repo), "branch", "-D", normalized_branch],
        capture_output=True,
        text=True,
        check=False,
    )
    if delete_proc.returncode != 0:
        raise OSError(delete_proc.stderr.strip() or delete_proc.stdout.strip() or f"git branch -D failed for {normalized_branch}")
    return {
        "deleted": True,
        "branch_exists": False,
        "git_repo_path": str(resolved_repo),
        "branch_name": normalized_branch,
        "current_branch": current_branch,
    }


def _path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return False


def _branch_exists(repo_path: Path, branch_name: str) -> bool:
    if not repo_path.exists():
        return False
    proc = subprocess.run(
        ["git", "-C", str(repo_path), "show-ref", "--verify", "--quiet", f"refs/heads/{branch_name}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0


def _current_branch_name(worktree_path: Path) -> str | None:
    if not worktree_path.exists():
        return None
    proc = subprocess.run(
        ["git", "-C", str(worktree_path), "symbolic-ref", "--quiet", "--short", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return _normalize_optional_text(proc.stdout)


def _validate_deletable_path(path: Path, *, protected_paths: Iterable[Path] = ()) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_absolute():
        raise ValueError(f"cleanup target must be an absolute path: {path}")
    if resolved == Path(resolved.anchor):
        raise ValueError(f"cleanup target cannot be filesystem root: {resolved}")
    home_path = Path.home().resolve()
    if resolved == home_path or home_path.is_relative_to(resolved):
        raise ValueError(f"cleanup target is too broad and would include the home directory: {resolved}")
    for protected_path in protected_paths:
        protected = protected_path.expanduser().resolve()
        if resolved == protected or protected.is_relative_to(resolved):
            raise ValueError(f"cleanup target would delete a protected path: {resolved}")
    return resolved


def _cleanup_empty_parent_directories(path: Path, *, stop_at: Path | None = None) -> None:
    current = path.expanduser().resolve()
    boundary = stop_at.expanduser().resolve() if stop_at is not None else None
    while True:
        if boundary is not None and current == boundary:
            return
        if not current.exists() or not current.is_dir():
            return
        try:
            current.rmdir()
        except OSError:
            return
        parent = current.parent
        if parent == current:
            return
        current = parent


def _ensure_cleanup_tables(connection: sqlite3.Connection, database_path: Path) -> None:
    try:
        _ensure_required_tables(connection, database_path, ("projects", "runs", "queue_items", "artifact_refs"))
    except RunPersistenceError as exc:
        raise CleanupManagerError(
            code=CLEANUP_MANAGER_STORAGE_ERROR,
            message="SQLite cleanup manager requires the base Control Plane v2 schema",
            database_path=database_path,
            details=exc.message,
        ) from exc

    runtime_cleanup_exists = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'runtime_cleanup_records'
        """
    ).fetchone()
    if runtime_cleanup_exists is None:
        raise CleanupManagerError(
            code=CLEANUP_MANAGER_STORAGE_ERROR,
            message="SQLite cleanup audit schema is missing runtime_cleanup_records",
            database_path=database_path,
            details="Run migrate-sqlite-v1 before using cleanup manager.",
        )

    artifact_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info('artifact_refs')").fetchall()
    }
    required_columns = {"cleaned_at", "cleanup_status", "cleanup_result_json", "last_cleanup_error"}
    if not required_columns.issubset(artifact_columns):
        missing_columns = sorted(required_columns - artifact_columns)
        raise CleanupManagerError(
            code=CLEANUP_MANAGER_STORAGE_ERROR,
            message="SQLite cleanup audit columns are missing from artifact_refs",
            database_path=database_path,
            details=f"missing_columns={missing_columns}. Run migrate-sqlite-v1 before using cleanup manager.",
        )


def _run_exists(connection: sqlite3.Connection, run_id: str) -> bool:
    row = connection.execute("SELECT 1 FROM runs WHERE id = ?", (run_id,)).fetchone()
    return row is not None


def _normalize_scopes(scopes: Iterable[str] | None, database_path: Path) -> tuple[str, ...]:
    if scopes is None:
        return CLEANUP_SCOPES
    normalized: list[str] = []
    aliases = {
        "artifact": "artifacts",
        "artifacts": "artifacts",
        "worktree": "worktrees",
        "worktrees": "worktrees",
        "branch": "branches",
        "branches": "branches",
    }
    for scope in scopes:
        normalized_scope = aliases.get(str(scope).strip().lower())
        if normalized_scope is None:
            raise CleanupManagerError(
                code=CLEANUP_INVALID_SCOPE,
                message=f"Unsupported cleanup scope: {scope}",
                database_path=database_path,
                details=f"allowed_scopes={', '.join(CLEANUP_SCOPES)}",
            )
        if normalized_scope not in normalized:
            normalized.append(normalized_scope)
    return tuple(normalized) if normalized else CLEANUP_SCOPES


def _resolve_cleanup_database_path(database_path: str | Path) -> Path:
    return _resolve_database_path(database_path)


def _resolve_now(now: str | None, database_path: Path) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    parsed = _parse_timestamp(now)
    if parsed is None:
        raise CleanupManagerError(
            code=CLEANUP_MANAGER_STORAGE_ERROR,
            message=f"Invalid cleanup timestamp: {now}",
            database_path=database_path,
        )
    return parsed


def _parse_timestamp(value: str | None) -> datetime | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    candidate = normalized[:-1] + "+00:00" if normalized.endswith("Z") else normalized
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _utc_now() -> str:
    return _format_timestamp(datetime.now(timezone.utc))


def _parse_ttl(
    raw_value: object,
    *,
    default_value: int,
    field_name: str,
    package_root: Path,
    database_path: Path,
) -> int:
    if raw_value is None:
        return default_value
    if isinstance(raw_value, bool):
        parsed_value: int | None = None
    elif isinstance(raw_value, int):
        parsed_value = raw_value
    elif isinstance(raw_value, float) and raw_value.is_integer():
        parsed_value = int(raw_value)
    elif isinstance(raw_value, str) and raw_value.strip().isdigit():
        parsed_value = int(raw_value.strip())
    else:
        parsed_value = None
    if parsed_value is None or parsed_value < 0:
        raise CleanupManagerError(
            code=CLEANUP_INVALID_POLICY,
            message=f"Invalid cleanup TTL in policy.yaml: {field_name}",
            database_path=database_path,
            details=f"package_root={package_root} raw_value={raw_value!r}",
        )
    return parsed_value


def _normalize_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _read_json_optional(path: Path) -> dict[str, object] | None:
    try:
        raw = path.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None
