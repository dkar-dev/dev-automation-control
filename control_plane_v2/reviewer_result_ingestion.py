from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sqlite3

from .dispatch_adapter import (
    ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST,
    ARTIFACT_KIND_STEP_REPORT,
    ARTIFACT_KIND_STEP_RESULT_JSON,
    ARTIFACT_KIND_STEP_STATE_JSON,
)
from .reviewer_outcome_persistence import (
    REVIEWER_VERDICTS,
    ReviewerOutcomeResult,
    complete_reviewer_outcome,
)
from .run_persistence import _connect_run_db, _ensure_required_tables, _resolve_database_path
from .step_run_persistence import STEP_RUN_TERMINAL_STATUSES


REVIEWER_RESULT_TARGET_REQUIRED = "REVIEWER_RESULT_TARGET_REQUIRED"
REVIEWER_RESULT_MANIFEST_INVALID = "REVIEWER_RESULT_MANIFEST_INVALID"
REVIEWER_RESULT_REPORT_INVALID = "REVIEWER_RESULT_REPORT_INVALID"
REVIEWER_RESULT_SOURCE_NOT_FOUND = "REVIEWER_RESULT_SOURCE_NOT_FOUND"
REVIEWER_RESULT_STEP_NOT_FOUND = "REVIEWER_RESULT_STEP_NOT_FOUND"
REVIEWER_RESULT_STEP_NOT_REVIEWER = "REVIEWER_RESULT_STEP_NOT_REVIEWER"
REVIEWER_RESULT_STEP_NOT_TERMINAL = "REVIEWER_RESULT_STEP_NOT_TERMINAL"
REVIEWER_RESULT_STRUCTURED_SOURCE_INVALID = "REVIEWER_RESULT_STRUCTURED_SOURCE_INVALID"
REVIEWER_RESULT_VERDICT_AMBIGUOUS = "REVIEWER_RESULT_VERDICT_AMBIGUOUS"

SOURCE_KIND_STEP_RESULT_JSON = "step_result_json"
SOURCE_KIND_DISPATCH_STATE_RESULT = "dispatch_result_manifest.state_result"
SOURCE_KIND_STEP_STATE_JSON = "step_state_json.result"
SOURCE_KIND_REVIEWER_REPORT = "reviewer_report"
SOURCE_KIND_OVERRIDE = "override"

SOURCE_PRIORITY = (
    SOURCE_KIND_STEP_RESULT_JSON,
    SOURCE_KIND_DISPATCH_STATE_RESULT,
    SOURCE_KIND_STEP_STATE_JSON,
    SOURCE_KIND_REVIEWER_REPORT,
)

_COMMIT_SHA_PATTERN = re.compile(r"[0-9a-fA-F]{7,40}")


@dataclass(frozen=True)
class ReviewerResultCandidate:
    source_kind: str
    source_path: str | None
    verdict: str | None
    summary: str | None
    commit_sha: str | None
    rank: int
    structured: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "source_kind": self.source_kind,
            "source_path": self.source_path,
            "verdict": self.verdict,
            "summary": self.summary,
            "commit_sha": self.commit_sha,
            "rank": self.rank,
            "structured": self.structured,
        }


@dataclass(frozen=True)
class SelectedReviewerResult:
    verdict: str
    summary: str | None
    commit_sha: str | None
    verdict_source_kind: str
    verdict_source_path: str | None
    summary_source_kind: str | None
    summary_source_path: str | None
    commit_sha_source_kind: str | None
    commit_sha_source_path: str | None
    override_verdict: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "verdict": self.verdict,
            "summary": self.summary,
            "commit_sha": self.commit_sha,
            "verdict_source_kind": self.verdict_source_kind,
            "verdict_source_path": self.verdict_source_path,
            "summary_source_kind": self.summary_source_kind,
            "summary_source_path": self.summary_source_path,
            "commit_sha_source_kind": self.commit_sha_source_kind,
            "commit_sha_source_path": self.commit_sha_source_path,
            "override_verdict": self.override_verdict,
        }


@dataclass(frozen=True)
class ReviewerResultInspection:
    reviewer_step_run_id: str
    run_id: str
    dispatch_result_manifest_path: Path | None
    dispatch_result_manifest: dict[str, object] | None
    artifact_paths: dict[str, str | None]
    source_candidates: tuple[ReviewerResultCandidate, ...]
    selected_result: SelectedReviewerResult
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "reviewer_step_run_id": self.reviewer_step_run_id,
            "run_id": self.run_id,
            "dispatch_result_manifest_path": (
                str(self.dispatch_result_manifest_path) if self.dispatch_result_manifest_path is not None else None
            ),
            "dispatch_result_manifest": self.dispatch_result_manifest,
            "artifact_paths": dict(self.artifact_paths),
            "source_candidates": [candidate.to_dict() for candidate in self.source_candidates],
            "selected_result": self.selected_result.to_dict(),
            "warnings": list(self.warnings),
            "source_priority": list(SOURCE_PRIORITY),
        }


@dataclass(frozen=True)
class ReviewerResultIngestionResult:
    inspection: ReviewerResultInspection
    reviewer_outcome: ReviewerOutcomeResult

    def to_dict(self) -> dict[str, object]:
        return {
            "inspection": self.inspection.to_dict(),
            "reviewer_outcome": self.reviewer_outcome.to_dict(),
        }


class ReviewerResultIngestionError(Exception):
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


def inspect_reviewer_result(
    database_path: str | Path,
    *,
    reviewer_step_run_id: str | None = None,
    dispatch_result_manifest_path: str | Path | None = None,
    override_verdict: str | None = None,
) -> ReviewerResultInspection:
    resolved_db_path = _resolve_database_path(database_path)
    manifest_path = _resolve_optional_path(dispatch_result_manifest_path)
    override_value = _normalize_override_verdict(override_verdict, resolved_db_path)

    target = _resolve_target(
        resolved_db_path,
        reviewer_step_run_id=reviewer_step_run_id,
        dispatch_result_manifest_path=manifest_path,
    )
    source_candidates, warnings, selected_result = _collect_and_select_sources(
        resolved_db_path,
        target,
        override_verdict=override_value,
    )
    return ReviewerResultInspection(
        reviewer_step_run_id=target["step_run_id"],
        run_id=target["run_id"],
        dispatch_result_manifest_path=target["dispatch_result_manifest_path"],
        dispatch_result_manifest=target["dispatch_result_manifest"],
        artifact_paths=target["artifact_paths"],
        source_candidates=tuple(source_candidates),
        selected_result=selected_result,
        warnings=tuple(warnings),
    )


def ingest_reviewer_result(
    database_path: str | Path,
    *,
    reviewer_step_run_id: str | None = None,
    dispatch_result_manifest_path: str | Path | None = None,
    override_verdict: str | None = None,
) -> ReviewerResultIngestionResult:
    inspection = inspect_reviewer_result(
        database_path,
        reviewer_step_run_id=reviewer_step_run_id,
        dispatch_result_manifest_path=dispatch_result_manifest_path,
        override_verdict=override_verdict,
    )
    reviewer_outcome = complete_reviewer_outcome(
        database_path,
        inspection.reviewer_step_run_id,
        inspection.selected_result.verdict,
        summary_text=inspection.selected_result.summary,
    )
    return ReviewerResultIngestionResult(inspection=inspection, reviewer_outcome=reviewer_outcome)


def parse_reviewer_report_text(
    report_text: str,
    *,
    database_path: str | Path,
    source_path: str | Path | None = None,
) -> ReviewerResultCandidate:
    resolved_db_path = _resolve_database_path(database_path)
    raw_lines = report_text.lstrip("\ufeff").splitlines()
    if len(raw_lines) < 2:
        raise ReviewerResultIngestionError(
            code=REVIEWER_RESULT_REPORT_INVALID,
            message="Reviewer report must start with Verdict and Summary lines",
            database_path=resolved_db_path,
            details=f"source_path={source_path}" if source_path is not None else None,
        )

    verdict_match = re.fullmatch(r"Verdict:\s*(approved|changes_requested|blocked)\s*", raw_lines[0].strip())
    if verdict_match is None:
        raise ReviewerResultIngestionError(
            code=REVIEWER_RESULT_REPORT_INVALID,
            message="Reviewer report line 1 must be: Verdict: approved|changes_requested|blocked",
            database_path=resolved_db_path,
            details=f"source_path={source_path}" if source_path is not None else None,
        )
    summary_match = re.fullmatch(r"Summary:\s*(.+\S)\s*", raw_lines[1].strip())
    if summary_match is None:
        raise ReviewerResultIngestionError(
            code=REVIEWER_RESULT_REPORT_INVALID,
            message="Reviewer report line 2 must be: Summary: <non-empty summary>",
            database_path=resolved_db_path,
            details=f"source_path={source_path}" if source_path is not None else None,
        )

    commit_sha: str | None = None
    if len(raw_lines) >= 3 and raw_lines[2].strip():
        commit_match = re.fullmatch(r"Commit SHA:\s*(\S+)\s*", raw_lines[2].strip())
        if commit_match is None:
            raise ReviewerResultIngestionError(
                code=REVIEWER_RESULT_REPORT_INVALID,
                message="Reviewer report line 3 must be empty or: Commit SHA: <sha|none>",
                database_path=resolved_db_path,
                details=f"source_path={source_path}" if source_path is not None else None,
            )
        commit_sha = _normalize_commit_sha(
            commit_match.group(1),
            database_path=resolved_db_path,
            source_kind=SOURCE_KIND_REVIEWER_REPORT,
            source_path=source_path,
            error_code=REVIEWER_RESULT_REPORT_INVALID,
        )

    return ReviewerResultCandidate(
        source_kind=SOURCE_KIND_REVIEWER_REPORT,
        source_path=str(Path(source_path).expanduser().resolve()) if source_path is not None else None,
        verdict=verdict_match.group(1),
        summary=summary_match.group(1).strip(),
        commit_sha=commit_sha,
        rank=SOURCE_PRIORITY.index(SOURCE_KIND_REVIEWER_REPORT),
        structured=False,
    )


def _resolve_target(
    database_path: Path,
    *,
    reviewer_step_run_id: str | None,
    dispatch_result_manifest_path: Path | None,
) -> dict[str, object]:
    normalized_step_run_id = (reviewer_step_run_id or "").strip() or None
    if normalized_step_run_id is None and dispatch_result_manifest_path is None:
        raise ReviewerResultIngestionError(
            code=REVIEWER_RESULT_TARGET_REQUIRED,
            message="Provide reviewer_step_run_id or dispatch_result_manifest_path",
            database_path=database_path,
        )
    if normalized_step_run_id is not None and dispatch_result_manifest_path is not None:
        raise ReviewerResultIngestionError(
            code=REVIEWER_RESULT_TARGET_REQUIRED,
            message="Provide only one target: reviewer_step_run_id or dispatch_result_manifest_path",
            database_path=database_path,
        )

    manifest_payload: dict[str, object] | None = None
    if dispatch_result_manifest_path is not None:
        manifest_payload = _load_json_object(
            dispatch_result_manifest_path,
            database_path=database_path,
            error_code=REVIEWER_RESULT_MANIFEST_INVALID,
            error_message=f"Failed to load dispatch result manifest: {dispatch_result_manifest_path}",
        )
        normalized_step_run_id = _extract_step_run_id_from_manifest(manifest_payload, database_path)

    assert normalized_step_run_id is not None
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("step_runs", "artifact_refs"))
        step_row = connection.execute(
            """
            SELECT id, run_id, step_key, status
            FROM step_runs
            WHERE id = ?
            """,
            (normalized_step_run_id,),
        ).fetchone()
        if step_row is None:
            raise ReviewerResultIngestionError(
                code=REVIEWER_RESULT_STEP_NOT_FOUND,
                message=f"reviewer step_run is not present in SQLite: {normalized_step_run_id}",
                database_path=database_path,
            )
        if step_row["step_key"] != "reviewer":
            raise ReviewerResultIngestionError(
                code=REVIEWER_RESULT_STEP_NOT_REVIEWER,
                message=f"step_run is not a reviewer step: {normalized_step_run_id}",
                database_path=database_path,
                details=f"actual_step_key={step_row['step_key']}",
            )
        if step_row["status"] not in STEP_RUN_TERMINAL_STATUSES:
            raise ReviewerResultIngestionError(
                code=REVIEWER_RESULT_STEP_NOT_TERMINAL,
                message=f"reviewer step_run must be terminal before ingestion: {normalized_step_run_id}",
                database_path=database_path,
                details=f"actual_status={step_row['status']}",
            )

        artifact_paths = _load_artifact_paths(connection, normalized_step_run_id)
    finally:
        connection.close()

    resolved_manifest_path = dispatch_result_manifest_path or _path_or_none(
        artifact_paths.get(ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST),
    )
    if manifest_payload is None and resolved_manifest_path is not None and resolved_manifest_path.is_file():
        manifest_payload = _load_json_object(
            resolved_manifest_path,
            database_path=database_path,
            error_code=REVIEWER_RESULT_MANIFEST_INVALID,
            error_message=f"Failed to load dispatch result manifest: {resolved_manifest_path}",
        )

    return {
        "step_run_id": normalized_step_run_id,
        "run_id": step_row["run_id"],
        "dispatch_result_manifest_path": resolved_manifest_path,
        "dispatch_result_manifest": manifest_payload,
        "artifact_paths": {
            "dispatch_result_manifest": str(resolved_manifest_path) if resolved_manifest_path is not None else None,
            "step_result_json": artifact_paths.get(ARTIFACT_KIND_STEP_RESULT_JSON),
            "step_state_json": artifact_paths.get(ARTIFACT_KIND_STEP_STATE_JSON),
            "step_report": artifact_paths.get(ARTIFACT_KIND_STEP_REPORT),
        },
    }


def _load_artifact_paths(connection: sqlite3.Connection, reviewer_step_run_id: str) -> dict[str, str]:
    rows = connection.execute(
        """
        SELECT artifact_kind, filesystem_path
        FROM artifact_refs
        WHERE step_run_id = ?
          AND artifact_kind IN (?, ?, ?, ?)
        ORDER BY created_at DESC, id DESC
        """,
        (
            reviewer_step_run_id,
            ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST,
            ARTIFACT_KIND_STEP_RESULT_JSON,
            ARTIFACT_KIND_STEP_STATE_JSON,
            ARTIFACT_KIND_STEP_REPORT,
        ),
    ).fetchall()
    resolved: dict[str, str] = {}
    for row in rows:
        resolved.setdefault(row["artifact_kind"], row["filesystem_path"])
    return resolved


def _collect_and_select_sources(
    database_path: Path,
    target: dict[str, object],
    *,
    override_verdict: str | None,
) -> tuple[list[ReviewerResultCandidate], list[str], SelectedReviewerResult]:
    warnings: list[str] = []
    candidates: list[ReviewerResultCandidate] = []
    source_errors: list[str] = []
    artifact_paths = target["artifact_paths"]

    result_json_path = _path_or_none(artifact_paths.get("step_result_json"))
    if result_json_path is not None:
        _append_structured_candidate(
            candidates,
            warnings,
            source_errors,
            database_path,
            source_kind=SOURCE_KIND_STEP_RESULT_JSON,
            source_path=result_json_path,
        )

    manifest_payload = target["dispatch_result_manifest"]
    if manifest_payload is not None:
        try:
            candidate = _candidate_from_mapping(
                _extract_dispatch_state_result(manifest_payload),
                database_path=database_path,
                source_kind=SOURCE_KIND_DISPATCH_STATE_RESULT,
                source_path=target["dispatch_result_manifest_path"],
                structured=True,
            )
            if candidate is not None:
                candidates.append(candidate)
        except ReviewerResultIngestionError as exc:
            source_errors.append(f"{SOURCE_KIND_DISPATCH_STATE_RESULT}: {exc.message}")

    state_json_path = _path_or_none(artifact_paths.get("step_state_json"))
    if state_json_path is not None:
        _append_structured_candidate(
            candidates,
            warnings,
            source_errors,
            database_path,
            source_kind=SOURCE_KIND_STEP_STATE_JSON,
            source_path=state_json_path,
        )

    report_path = _path_or_none(artifact_paths.get("step_report"))
    if report_path is not None:
        try:
            report_text = report_path.read_text(encoding="utf-8")
            candidates.append(
                parse_reviewer_report_text(
                    report_text,
                    database_path=database_path,
                    source_path=report_path,
                )
            )
        except OSError as exc:
            source_errors.append(f"{SOURCE_KIND_REVIEWER_REPORT}: {exc}")
        except ReviewerResultIngestionError as exc:
            source_errors.append(f"{SOURCE_KIND_REVIEWER_REPORT}: {exc.message}")

    selected_result = _select_result(
        database_path,
        candidates=candidates,
        source_errors=source_errors,
        override_verdict=override_verdict,
    )
    warnings.extend(_metadata_warnings(candidates))
    return sorted(candidates, key=lambda item: item.rank), warnings, selected_result


def _append_structured_candidate(
    candidates: list[ReviewerResultCandidate],
    warnings: list[str],
    source_errors: list[str],
    database_path: Path,
    *,
    source_kind: str,
    source_path: Path,
) -> None:
    try:
        payload = _load_json_object(
            source_path,
            database_path=database_path,
            error_code=REVIEWER_RESULT_STRUCTURED_SOURCE_INVALID,
            error_message=f"Failed to load structured reviewer result source: {source_path}",
        )
        candidate = _candidate_from_mapping(
            payload,
            database_path=database_path,
            source_kind=source_kind,
            source_path=source_path,
            structured=True,
        )
        if candidate is not None:
            candidates.append(candidate)
    except ReviewerResultIngestionError as exc:
        source_errors.append(f"{source_kind}: {exc.message}")
        warnings.append(f"{source_kind} was ignored: {exc.message}")


def _select_result(
    database_path: Path,
    *,
    candidates: list[ReviewerResultCandidate],
    source_errors: list[str],
    override_verdict: str | None,
) -> SelectedReviewerResult:
    sorted_candidates = sorted(candidates, key=lambda item: item.rank)
    verdict_candidates = [candidate for candidate in sorted_candidates if candidate.verdict is not None]
    if override_verdict is None:
        if not verdict_candidates:
            details = "; ".join(source_errors) if source_errors else None
            raise ReviewerResultIngestionError(
                code=REVIEWER_RESULT_SOURCE_NOT_FOUND,
                message="No semantic reviewer verdict could be extracted from structured artifacts or reviewer report",
                database_path=database_path,
                details=details,
            )
        observed_verdicts = {candidate.verdict for candidate in verdict_candidates}
        if len(observed_verdicts) != 1:
            raise ReviewerResultIngestionError(
                code=REVIEWER_RESULT_VERDICT_AMBIGUOUS,
                message="Reviewer verdict is ambiguous across available sources",
                database_path=database_path,
                details=", ".join(
                    f"{candidate.source_kind}={candidate.verdict}" for candidate in verdict_candidates
                ),
            )
        verdict_source = verdict_candidates[0]
        verdict = verdict_source.verdict
        assert verdict is not None
        return SelectedReviewerResult(
            verdict=verdict,
            summary=_best_summary(sorted_candidates, preferred=verdict_source),
            commit_sha=_best_commit_sha(sorted_candidates, preferred=verdict_source),
            verdict_source_kind=verdict_source.source_kind,
            verdict_source_path=verdict_source.source_path,
            summary_source_kind=_summary_source_kind(sorted_candidates, preferred=verdict_source),
            summary_source_path=_summary_source_path(sorted_candidates, preferred=verdict_source),
            commit_sha_source_kind=_commit_source_kind(sorted_candidates, preferred=verdict_source),
            commit_sha_source_path=_commit_source_path(sorted_candidates, preferred=verdict_source),
            override_verdict=None,
        )

    preferred_source = verdict_candidates[0] if verdict_candidates else None
    return SelectedReviewerResult(
        verdict=override_verdict,
        summary=_best_summary(sorted_candidates, preferred=preferred_source),
        commit_sha=_best_commit_sha(sorted_candidates, preferred=preferred_source),
        verdict_source_kind=SOURCE_KIND_OVERRIDE,
        verdict_source_path=None,
        summary_source_kind=_summary_source_kind(sorted_candidates, preferred=preferred_source),
        summary_source_path=_summary_source_path(sorted_candidates, preferred=preferred_source),
        commit_sha_source_kind=_commit_source_kind(sorted_candidates, preferred=preferred_source),
        commit_sha_source_path=_commit_source_path(sorted_candidates, preferred=preferred_source),
        override_verdict=override_verdict,
    )


def _best_summary(
    candidates: list[ReviewerResultCandidate],
    *,
    preferred: ReviewerResultCandidate | None,
) -> str | None:
    if preferred is not None and preferred.summary is not None:
        return preferred.summary
    for candidate in candidates:
        if candidate.summary is not None:
            return candidate.summary
    return None


def _summary_source_kind(
    candidates: list[ReviewerResultCandidate],
    *,
    preferred: ReviewerResultCandidate | None,
) -> str | None:
    source = _best_summary_source(candidates, preferred=preferred)
    return source.source_kind if source is not None else None


def _summary_source_path(
    candidates: list[ReviewerResultCandidate],
    *,
    preferred: ReviewerResultCandidate | None,
) -> str | None:
    source = _best_summary_source(candidates, preferred=preferred)
    return source.source_path if source is not None else None


def _best_summary_source(
    candidates: list[ReviewerResultCandidate],
    *,
    preferred: ReviewerResultCandidate | None,
) -> ReviewerResultCandidate | None:
    if preferred is not None and preferred.summary is not None:
        return preferred
    for candidate in candidates:
        if candidate.summary is not None:
            return candidate
    return None


def _best_commit_sha(
    candidates: list[ReviewerResultCandidate],
    *,
    preferred: ReviewerResultCandidate | None,
) -> str | None:
    if preferred is not None and preferred.commit_sha is not None:
        return preferred.commit_sha
    for candidate in candidates:
        if candidate.commit_sha is not None:
            return candidate.commit_sha
    return None


def _commit_source_kind(
    candidates: list[ReviewerResultCandidate],
    *,
    preferred: ReviewerResultCandidate | None,
) -> str | None:
    source = _best_commit_source(candidates, preferred=preferred)
    return source.source_kind if source is not None else None


def _commit_source_path(
    candidates: list[ReviewerResultCandidate],
    *,
    preferred: ReviewerResultCandidate | None,
) -> str | None:
    source = _best_commit_source(candidates, preferred=preferred)
    return source.source_path if source is not None else None


def _best_commit_source(
    candidates: list[ReviewerResultCandidate],
    *,
    preferred: ReviewerResultCandidate | None,
) -> ReviewerResultCandidate | None:
    if preferred is not None and preferred.commit_sha is not None:
        return preferred
    for candidate in candidates:
        if candidate.commit_sha is not None:
            return candidate
    return None


def _metadata_warnings(candidates: list[ReviewerResultCandidate]) -> list[str]:
    warnings: list[str] = []
    summaries = {
        candidate.summary
        for candidate in candidates
        if candidate.summary is not None
    }
    commit_shas = {
        candidate.commit_sha
        for candidate in candidates
        if candidate.commit_sha is not None
    }
    if len(summaries) > 1:
        warnings.append("reviewer summary differs across extracted sources; highest-priority non-empty summary was used")
    if len(commit_shas) > 1:
        warnings.append("commit_sha differs across extracted sources; highest-priority non-empty commit_sha was used")
    return warnings


def _extract_dispatch_state_result(manifest_payload: Mapping[str, object]) -> Mapping[str, object] | None:
    dispatch_outcome = manifest_payload.get("dispatch_outcome")
    if not isinstance(dispatch_outcome, Mapping):
        return None
    state_result = dispatch_outcome.get("state_result")
    if not isinstance(state_result, Mapping):
        return None
    return state_result


def _candidate_from_mapping(
    payload: Mapping[str, object] | None,
    *,
    database_path: Path,
    source_kind: str,
    source_path: Path | None,
    structured: bool,
) -> ReviewerResultCandidate | None:
    if payload is None:
        return None
    extracted_payload = _semantic_mapping(payload)
    if extracted_payload is None:
        return None

    verdict = _normalize_optional_text(extracted_payload.get("verdict"))
    if verdict is not None and verdict not in REVIEWER_VERDICTS:
        raise ReviewerResultIngestionError(
            code=REVIEWER_RESULT_STRUCTURED_SOURCE_INVALID,
            message=f"{source_kind} contains unsupported verdict: {verdict}",
            database_path=database_path,
            details=str(source_path) if source_path is not None else None,
        )
    summary = _normalize_optional_text(extracted_payload.get("summary"))
    commit_sha = _normalize_commit_sha(
        extracted_payload.get("commit_sha"),
        database_path=database_path,
        source_kind=source_kind,
        source_path=source_path,
        error_code=REVIEWER_RESULT_STRUCTURED_SOURCE_INVALID,
    )
    if verdict is None and summary is None and commit_sha is None:
        return None
    return ReviewerResultCandidate(
        source_kind=source_kind,
        source_path=str(source_path.expanduser().resolve()) if source_path is not None else None,
        verdict=verdict,
        summary=summary,
        commit_sha=commit_sha,
        rank=SOURCE_PRIORITY.index(source_kind),
        structured=structured,
    )


def _semantic_mapping(payload: Mapping[str, object]) -> Mapping[str, object] | None:
    if not isinstance(payload, Mapping):
        return None
    nested_result = payload.get("result")
    if isinstance(nested_result, Mapping) and _contains_semantic_fields(nested_result):
        return nested_result
    if _contains_semantic_fields(payload):
        return payload
    return None


def _contains_semantic_fields(payload: Mapping[str, object]) -> bool:
    return any(key in payload for key in ("verdict", "summary", "commit_sha"))


def _extract_step_run_id_from_manifest(manifest_payload: Mapping[str, object], database_path: Path) -> str:
    step_run_id = _normalize_optional_text(manifest_payload.get("step_run_id"))
    if step_run_id is not None:
        return step_run_id
    step_run_payload = manifest_payload.get("step_run")
    if isinstance(step_run_payload, Mapping):
        nested_step_run = step_run_payload.get("step_run")
        if isinstance(nested_step_run, Mapping):
            nested_id = _normalize_optional_text(nested_step_run.get("id"))
            if nested_id is not None:
                return nested_id
    raise ReviewerResultIngestionError(
        code=REVIEWER_RESULT_MANIFEST_INVALID,
        message="Dispatch result manifest does not contain a reviewer step_run id",
        database_path=database_path,
    )


def _load_json_object(
    path: Path,
    *,
    database_path: Path,
    error_code: str,
    error_message: str,
) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReviewerResultIngestionError(
            code=error_code,
            message=error_message,
            database_path=database_path,
            details=str(exc),
        ) from exc
    if not isinstance(payload, dict):
        raise ReviewerResultIngestionError(
            code=error_code,
            message=f"JSON payload must be an object: {path}",
            database_path=database_path,
        )
    return payload


def _normalize_override_verdict(override_verdict: str | None, database_path: Path) -> str | None:
    verdict = _normalize_optional_text(override_verdict)
    if verdict is None:
        return None
    if verdict not in REVIEWER_VERDICTS:
        raise ReviewerResultIngestionError(
            code=REVIEWER_RESULT_MANIFEST_INVALID,
            message=f"override verdict must be one of: {', '.join(REVIEWER_VERDICTS)}",
            database_path=database_path,
            details=f"actual={override_verdict}",
        )
    return verdict


def _normalize_commit_sha(
    value: object,
    *,
    database_path: Path,
    source_kind: str,
    source_path: str | Path | None,
    error_code: str,
) -> str | None:
    commit_sha = _normalize_optional_text(value)
    if commit_sha is None or commit_sha.lower() == "none":
        return None
    if _COMMIT_SHA_PATTERN.fullmatch(commit_sha) is None:
        raise ReviewerResultIngestionError(
            code=error_code,
            message=f"{source_kind} contains an invalid commit_sha",
            database_path=database_path,
            details=f"source_path={source_path}, commit_sha={commit_sha}",
        )
    return commit_sha


def _normalize_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_optional_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    return Path(value).expanduser().resolve()


def _path_or_none(value: object) -> Path | None:
    normalized = _normalize_optional_text(value)
    return Path(normalized).expanduser().resolve() if normalized is not None else None
