from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Protocol

from .id_generation import generate_opaque_id


RUNTIME_EVENT_SEVERITIES = ("info", "warning", "error")
RUNTIME_EVENT_TYPES = (
    "task_submitted",
    "run_created",
    "run_claimed",
    "step_run_started",
    "step_run_finished",
    "reviewer_outcome_completed",
    "host_checks_completed",
    "deployable_green_decided",
    "release_handoff_created",
    "runtime_supervisor_started",
    "runtime_supervisor_stopped",
    "runtime_supervisor_degraded",
)

RUNTIME_EVENT_NOT_FOUND = "RUNTIME_EVENT_NOT_FOUND"
RUNTIME_EVENT_REQUEST_INVALID = "RUNTIME_EVENT_REQUEST_INVALID"
RUNTIME_EVENT_STORAGE_ERROR = "RUNTIME_EVENT_STORAGE_ERROR"
RUNTIME_EVENT_REQUIRED_TABLES_MISSING = "RUNTIME_EVENT_REQUIRED_TABLES_MISSING"

_SUSPECT_SENSITIVE_KEYS = {
    "authorization",
    "cookie",
    "credential",
    "credentials",
    "local_secrets_file",
    "password",
    "private_key",
    "secret",
    "secret_key",
    "secret_value",
    "token",
}


class RuntimeEventRedactor(Protocol):
    def sanitize_text(self, value: str | None) -> str | None: ...

    def sanitize_object(self, value: object) -> object: ...


@dataclass(frozen=True)
class RuntimeEventAppendRequest:
    event_type: str
    entity_type: str
    entity_id: str
    severity: str
    summary: str
    payload_redacted: object
    source_module: str
    project_key: str | None = None
    flow_id: str | None = None
    run_id: str | None = None
    step_run_id: str | None = None
    created_at: str | None = None
    event_id: str | None = None


@dataclass(frozen=True)
class RuntimeEventRecord:
    event_id: str
    created_at: str
    event_type: str
    entity_type: str
    entity_id: str
    project_key: str | None
    flow_id: str | None
    run_id: str | None
    step_run_id: str | None
    severity: str
    summary: str
    payload_redacted: object
    source_module: str

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "created_at": self.created_at,
            "event_type": self.event_type,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "project_key": self.project_key,
            "flow_id": self.flow_id,
            "run_id": self.run_id,
            "step_run_id": self.step_run_id,
            "severity": self.severity,
            "summary": self.summary,
            "payload_redacted": self.payload_redacted,
            "source_module": self.source_module,
        }


class RuntimeEventJournalError(Exception):
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


def append_runtime_event(
    database_path: str | Path,
    request: RuntimeEventAppendRequest,
    *,
    redactor: RuntimeEventRedactor | None = None,
) -> RuntimeEventRecord:
    resolved_db_path = _resolve_database_path(database_path)
    connection = _connect_event_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runtime_events",))
        connection.execute("BEGIN")
        record = insert_runtime_event_in_connection(
            connection,
            database_path=resolved_db_path,
            request=request,
            redactor=redactor,
        )
        connection.commit()
        return record
    except RuntimeEventJournalError:
        connection.rollback()
        raise
    except sqlite3.Error as exc:
        connection.rollback()
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_STORAGE_ERROR,
            message="Failed to append runtime event",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def insert_runtime_event_in_connection(
    connection: sqlite3.Connection,
    *,
    database_path: Path,
    request: RuntimeEventAppendRequest,
    redactor: RuntimeEventRedactor | None = None,
) -> RuntimeEventRecord:
    normalized = _normalize_append_request(request, database_path, redactor=redactor)
    try:
        connection.execute(
            """
            INSERT INTO runtime_events (
              event_id,
              created_at,
              event_type,
              entity_type,
              entity_id,
              project_key,
              flow_id,
              run_id,
              step_run_id,
              severity,
              summary,
              payload_redacted_json,
              source_module
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.event_id,
                normalized.created_at,
                normalized.event_type,
                normalized.entity_type,
                normalized.entity_id,
                normalized.project_key,
                normalized.flow_id,
                normalized.run_id,
                normalized.step_run_id,
                normalized.severity,
                normalized.summary,
                json.dumps(normalized.payload_redacted, ensure_ascii=False, sort_keys=True),
                normalized.source_module,
            ),
        )
    except sqlite3.Error as exc:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_STORAGE_ERROR,
            message="Failed to insert runtime event row",
            database_path=database_path,
            details=str(exc),
        ) from exc
    return normalized


def list_runtime_events(
    database_path: str | Path,
    *,
    limit: int = 100,
    project_key: str | None = None,
    flow_id: str | None = None,
    run_id: str | None = None,
    event_type: str | None = None,
    created_after: str | None = None,
) -> list[RuntimeEventRecord]:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_limit = _normalize_limit(limit, resolved_db_path)
    normalized_project_key = _normalize_optional_text(project_key)
    normalized_flow_id = _normalize_optional_text(flow_id)
    normalized_run_id = _normalize_optional_text(run_id)
    normalized_event_type = _normalize_optional_event_type(event_type, resolved_db_path)
    normalized_created_after = _normalize_optional_timestamp(created_after, resolved_db_path, field_name="created_after")

    connection = _connect_event_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runtime_events",))
        filters: list[str] = []
        params: list[object] = []
        if normalized_project_key is not None:
            filters.append("project_key = ?")
            params.append(normalized_project_key)
        if normalized_flow_id is not None:
            filters.append("flow_id = ?")
            params.append(normalized_flow_id)
        if normalized_run_id is not None:
            filters.append("run_id = ?")
            params.append(normalized_run_id)
        if normalized_event_type is not None:
            filters.append("event_type = ?")
            params.append(normalized_event_type)
        if normalized_created_after is not None:
            filters.append("created_at > ?")
            params.append(normalized_created_after)

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        rows = connection.execute(
            f"""
            SELECT
              event_id,
              created_at,
              event_type,
              entity_type,
              entity_id,
              project_key,
              flow_id,
              run_id,
              step_run_id,
              severity,
              summary,
              payload_redacted_json,
              source_module
            FROM runtime_events
            {where_sql}
            ORDER BY created_at DESC, event_id DESC
            LIMIT ?
            """,
            (*params, normalized_limit),
        ).fetchall()
    except sqlite3.Error as exc:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_STORAGE_ERROR,
            message="Failed to list runtime events",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()
    return [_row_to_runtime_event(row, resolved_db_path) for row in rows]


def get_runtime_event(database_path: str | Path, event_id: str) -> RuntimeEventRecord:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_event_id = _normalize_required_text("event_id", event_id, resolved_db_path)

    connection = _connect_event_db(resolved_db_path)
    try:
        _ensure_required_tables(connection, resolved_db_path, ("runtime_events",))
        row = connection.execute(
            """
            SELECT
              event_id,
              created_at,
              event_type,
              entity_type,
              entity_id,
              project_key,
              flow_id,
              run_id,
              step_run_id,
              severity,
              summary,
              payload_redacted_json,
              source_module
            FROM runtime_events
            WHERE event_id = ?
            """,
            (normalized_event_id,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_STORAGE_ERROR,
            message="Failed to load runtime event",
            database_path=resolved_db_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()

    if row is None:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_NOT_FOUND,
            message=f"Runtime event is not present in SQLite: {normalized_event_id}",
            database_path=resolved_db_path,
        )
    return _row_to_runtime_event(row, resolved_db_path)


def _normalize_append_request(
    request: RuntimeEventAppendRequest,
    database_path: Path,
    *,
    redactor: RuntimeEventRedactor | None,
) -> RuntimeEventRecord:
    normalized_event_type = _normalize_required_event_type(request.event_type, database_path)
    normalized_entity_type = _normalize_required_text("entity_type", request.entity_type, database_path)
    normalized_entity_id = _normalize_required_text("entity_id", request.entity_id, database_path)
    normalized_severity = _normalize_required_severity(request.severity, database_path)
    normalized_source_module = _normalize_required_text("source_module", request.source_module, database_path)
    created_at = _normalize_optional_timestamp(request.created_at, database_path, field_name="created_at") or _utc_now()
    event_id = _normalize_optional_text(request.event_id) or generate_opaque_id()
    sanitized_summary = _sanitize_summary(request.summary, redactor=redactor, database_path=database_path)
    sanitized_payload = _sanitize_payload(request.payload_redacted, redactor=redactor)

    return RuntimeEventRecord(
        event_id=event_id,
        created_at=created_at,
        event_type=normalized_event_type,
        entity_type=normalized_entity_type,
        entity_id=normalized_entity_id,
        project_key=_normalize_optional_text(request.project_key),
        flow_id=_normalize_optional_text(request.flow_id),
        run_id=_normalize_optional_text(request.run_id),
        step_run_id=_normalize_optional_text(request.step_run_id),
        severity=normalized_severity,
        summary=sanitized_summary,
        payload_redacted=sanitized_payload,
        source_module=normalized_source_module,
    )


def _sanitize_summary(summary: str, *, redactor: RuntimeEventRedactor | None, database_path: Path) -> str:
    normalized = _normalize_required_text("summary", summary, database_path)
    if redactor is not None:
        normalized = redactor.sanitize_text(normalized) or normalized
    return normalized


def _sanitize_payload(value: object, *, redactor: RuntimeEventRedactor | None) -> object:
    candidate = redactor.sanitize_object(value) if redactor is not None else value
    return _json_safe_value(candidate)


def _json_safe_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return str(value.expanduser().resolve())
    if isinstance(value, Mapping):
        payload: dict[str, object] = {}
        for key, item in value.items():
            normalized_key = str(key)
            lowered_key = normalized_key.strip().lower()
            if lowered_key in _SUSPECT_SENSITIVE_KEYS and item is not None:
                payload[normalized_key] = f"[redacted:suspected_sensitive_key:{normalized_key}]"
            else:
                payload[normalized_key] = _json_safe_value(item)
        return payload
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return [_json_safe_value(item) for item in value]
    if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
        return _json_safe_value(value.to_dict())
    return str(value)


def _row_to_runtime_event(row: sqlite3.Row, database_path: Path) -> RuntimeEventRecord:
    raw_payload = row["payload_redacted_json"]
    try:
        payload = json.loads(raw_payload) if raw_payload is not None else {}
    except json.JSONDecodeError as exc:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_STORAGE_ERROR,
            message=f"Runtime event payload is not valid JSON: {row['event_id']}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    return RuntimeEventRecord(
        event_id=str(row["event_id"]),
        created_at=str(row["created_at"]),
        event_type=str(row["event_type"]),
        entity_type=str(row["entity_type"]),
        entity_id=str(row["entity_id"]),
        project_key=_normalize_optional_text(row["project_key"]),
        flow_id=_normalize_optional_text(row["flow_id"]),
        run_id=_normalize_optional_text(row["run_id"]),
        step_run_id=_normalize_optional_text(row["step_run_id"]),
        severity=str(row["severity"]),
        summary=str(row["summary"]),
        payload_redacted=payload,
        source_module=str(row["source_module"]),
    )


def _normalize_limit(limit: int, database_path: Path) -> int:
    if limit <= 0:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_REQUEST_INVALID,
            message="limit must be greater than zero",
            database_path=database_path,
        )
    return limit


def _normalize_required_event_type(value: object, database_path: Path) -> str:
    normalized = _normalize_required_text("event_type", value, database_path)
    if normalized not in RUNTIME_EVENT_TYPES:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_REQUEST_INVALID,
            message=f"event_type must be one of: {', '.join(RUNTIME_EVENT_TYPES)}",
            database_path=database_path,
            details=f"actual={normalized}",
        )
    return normalized


def _normalize_optional_event_type(value: object, database_path: Path) -> str | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    return _normalize_required_event_type(normalized, database_path)


def _normalize_required_severity(value: object, database_path: Path) -> str:
    normalized = _normalize_required_text("severity", value, database_path)
    if normalized not in RUNTIME_EVENT_SEVERITIES:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_REQUEST_INVALID,
            message=f"severity must be one of: {', '.join(RUNTIME_EVENT_SEVERITIES)}",
            database_path=database_path,
            details=f"actual={normalized}",
        )
    return normalized


def _normalize_required_text(field_name: str, value: object, database_path: Path) -> str:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_REQUEST_INVALID,
            message=f"{field_name} must be a non-empty string",
            database_path=database_path,
        )
    return normalized


def _normalize_optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_optional_timestamp(
    value: object,
    database_path: Path,
    *,
    field_name: str,
) -> str | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_REQUEST_INVALID,
            message=f"{field_name} must be a valid ISO-8601 timestamp",
            database_path=database_path,
            details=f"actual={normalized}",
        ) from exc
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _resolve_database_path(database_path: str | Path) -> Path:
    resolved_db_path = Path(database_path).expanduser().resolve()
    if not resolved_db_path.exists():
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_STORAGE_ERROR,
            message=f"SQLite database does not exist: {resolved_db_path}",
            database_path=resolved_db_path,
            details="Run init-sqlite-v1 or migrate-sqlite-v1 before using runtime event journal utilities.",
        )
    if not resolved_db_path.is_file():
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_STORAGE_ERROR,
            message=f"SQLite database path is not a file: {resolved_db_path}",
            database_path=resolved_db_path,
        )
    return resolved_db_path


def _connect_event_db(database_path: Path) -> sqlite3.Connection:
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
    present_tables = {str(row["name"]) for row in rows}
    missing_tables = [table_name for table_name in required_tables if table_name not in present_tables]
    if missing_tables:
        raise RuntimeEventJournalError(
            code=RUNTIME_EVENT_REQUIRED_TABLES_MISSING,
            message=f"SQLite database is missing required tables: {', '.join(missing_tables)}",
            database_path=database_path,
            details="Run init-sqlite-v1 or migrate-sqlite-v1 before using runtime event journal utilities.",
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
