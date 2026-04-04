from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
from urllib.parse import parse_qs, unquote, urlparse

from .id_generation import generate_opaque_id
from .manual_control import ManualControlError, force_stop_run, pause_run, rerun_run_step, resume_run, show_run_control_state
from .run_persistence import RunPersistenceError
from .runtime_cleanup_manager import CLEANUP_SCOPES, CleanupManagerError, run_cleanup_once
from .step_run_persistence import StepRunPersistenceError, get_step_run
from .task_intake import TaskIntakeError, list_submitted_tasks, show_submitted_task, submit_bounded_task
from .worker_loop import WorkerLoopError, WorkerRuntimeConfig, run_worker_tick, run_worker_until_idle


API_DEFAULT_HOST = "127.0.0.1"
API_DEFAULT_PORT = 8788
API_ENV_HOST = "CONTROL_PLANE_API_HOST"
API_ENV_PORT = "CONTROL_PLANE_API_PORT"
API_ENV_SQLITE_DB = "CONTROL_PLANE_API_SQLITE_DB"
API_ENV_ARTIFACT_ROOT = "CONTROL_PLANE_API_ARTIFACT_ROOT"
API_ENV_WORKSPACE_ROOT = "CONTROL_PLANE_API_WORKSPACE_ROOT"
API_ENV_WORKER_LOG_ROOT = "CONTROL_PLANE_API_WORKER_LOG_ROOT"
API_SERVER_NAME = "control-plane-v2-api/0.1"
LOCAL_BIND_HOSTS = {"127.0.0.1", "localhost"}
JSON_CONTENT_TYPES = ("application/json",)

API_CONFIG_INVALID = "API_CONFIG_INVALID"
ENDPOINT_NOT_FOUND = "ENDPOINT_NOT_FOUND"
INVALID_JSON_BODY = "INVALID_JSON_BODY"
INVALID_QUERY_PARAMETER = "INVALID_QUERY_PARAMETER"
INVALID_REQUEST_PAYLOAD = "INVALID_REQUEST_PAYLOAD"
JSON_BODY_REQUIRED = "JSON_BODY_REQUIRED"
LOCALHOST_ONLY = "LOCALHOST_ONLY"
METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED"
RUN_STEP_MISMATCH = "RUN_STEP_MISMATCH"
UNHANDLED_SERVER_ERROR = "UNHANDLED_SERVER_ERROR"
UNSUPPORTED_MEDIA_TYPE = "UNSUPPORTED_MEDIA_TYPE"

_RUN_ACTION_PATH_RE = re.compile(
    r"^/v1/runs/(?P<run_id>[^/]+)/(?P<action>pause|resume|force-stop|rerun-step|control-state)$"
)
_TASK_DETAIL_PATH_RE = re.compile(r"^/v1/tasks/(?P<run_id>[^/]+)$")


@dataclass(frozen=True)
class ControlPlaneApiConfig:
    host: str
    port: int
    sqlite_db: Path
    default_artifact_root: Path | None = None
    default_workspace_root: Path | None = None
    default_worker_log_root: Path | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def to_dict(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "sqlite_db": str(self.sqlite_db),
            "default_artifact_root": str(self.default_artifact_root) if self.default_artifact_root is not None else None,
            "default_workspace_root": str(self.default_workspace_root) if self.default_workspace_root is not None else None,
            "default_worker_log_root": str(self.default_worker_log_root) if self.default_worker_log_root is not None else None,
            "base_url": self.base_url,
            "localhost_only": True,
        }


class ControlPlaneApiConfigError(Exception):
    def __init__(self, code: str, message: str, details: str | None = None) -> None:
        self.code = code
        self.message = message
        self.details = details
        super().__init__(message)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class ApiRequestError(Exception):
    def __init__(
        self,
        http_status: int,
        code: str,
        message: str,
        details: str | None = None,
        *,
        stage: str = "http_api",
        database_path: str | Path | None = None,
    ) -> None:
        self.http_status = http_status
        self.code = code
        self.message = message
        self.details = details
        self.stage = stage
        self.database_path = str(database_path) if database_path is not None else None
        super().__init__(message)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "stage": self.stage,
            "database_path": self.database_path,
        }


def create_control_plane_api_config(
    *,
    host: str | None = None,
    port: int | str | None = None,
    sqlite_db: str | Path | None = None,
    default_artifact_root: str | Path | None = None,
    default_workspace_root: str | Path | None = None,
    default_worker_log_root: str | Path | None = None,
) -> ControlPlaneApiConfig:
    resolved_host = (host or os.environ.get(API_ENV_HOST) or API_DEFAULT_HOST).strip()
    if resolved_host not in LOCAL_BIND_HOSTS:
        raise ControlPlaneApiConfigError(
            code=LOCALHOST_ONLY,
            message="v1 HTTP API is localhost-only and must bind to 127.0.0.1 or localhost",
            details=f"actual_host={resolved_host}",
        )

    raw_port = port if port is not None else os.environ.get(API_ENV_PORT)
    if raw_port is None:
        resolved_port = API_DEFAULT_PORT
    else:
        try:
            resolved_port = int(str(raw_port).strip())
        except ValueError as exc:
            raise ControlPlaneApiConfigError(
                code=API_CONFIG_INVALID,
                message="API port must be an integer",
                details=f"actual_port={raw_port!r}",
            ) from exc
        if resolved_port <= 0 or resolved_port > 65535:
            raise ControlPlaneApiConfigError(
                code=API_CONFIG_INVALID,
                message="API port must be in the range 1..65535",
                details=f"actual_port={resolved_port}",
            )

    raw_sqlite_db = sqlite_db if sqlite_db is not None else os.environ.get(API_ENV_SQLITE_DB)
    if raw_sqlite_db is None:
        raise ControlPlaneApiConfigError(
            code=API_CONFIG_INVALID,
            message="sqlite_db is required for the HTTP API",
            details=f"provide --sqlite-db or {API_ENV_SQLITE_DB}",
        )

    return ControlPlaneApiConfig(
        host=resolved_host,
        port=resolved_port,
        sqlite_db=_resolve_path(raw_sqlite_db),
        default_artifact_root=_resolve_optional_path(
            default_artifact_root if default_artifact_root is not None else os.environ.get(API_ENV_ARTIFACT_ROOT)
        ),
        default_workspace_root=_resolve_optional_path(
            default_workspace_root if default_workspace_root is not None else os.environ.get(API_ENV_WORKSPACE_ROOT)
        ),
        default_worker_log_root=_resolve_optional_path(
            default_worker_log_root if default_worker_log_root is not None else os.environ.get(API_ENV_WORKER_LOG_ROOT)
        ),
    )


class ControlPlaneApiApplication:
    def __init__(self, config: ControlPlaneApiConfig) -> None:
        self.config = config

    def health(self) -> dict[str, object]:
        return {
            "service": "control-plane-v2-api",
            "version": "v1",
            "sqlite_db": str(self.config.sqlite_db),
            "sqlite_db_exists": self.config.sqlite_db.exists(),
            "base_url": self.config.base_url,
            "localhost_only": True,
        }

    def submit_task(self, payload: Mapping[str, object]) -> dict[str, object]:
        submission_payload = dict(payload)
        if "artifact_root" not in submission_payload and self.config.default_artifact_root is not None:
            submission_payload["artifact_root"] = str(self.config.default_artifact_root)
        if "workspace_root" not in submission_payload and self.config.default_workspace_root is not None:
            submission_payload["workspace_root"] = str(self.config.default_workspace_root)
        result = submit_bounded_task(self.config.sqlite_db, submission_payload)
        return {"submitted_task": result.to_dict()}

    def get_task(self, run_id: str) -> dict[str, object]:
        result = show_submitted_task(self.config.sqlite_db, run_id)
        return {"submitted_task": result.to_dict()}

    def list_tasks(self, query: Mapping[str, Sequence[str]]) -> dict[str, object]:
        project_key = _query_single(query, "project_key")
        limit = _query_int(query, "limit", default=100)
        result = list_submitted_tasks(self.config.sqlite_db, project_key=project_key, limit=limit)
        return {"submitted_tasks": [item.to_dict() for item in result]}

    def worker_tick(self, payload: Mapping[str, object]) -> dict[str, object]:
        result = run_worker_tick(
            self.config.sqlite_db,
            runtime_config=self._build_worker_runtime_config(payload),
        )
        return {"worker_tick": result.to_dict()}

    def worker_run_until_idle(self, payload: Mapping[str, object]) -> dict[str, object]:
        max_ticks = _coerce_optional_int(payload.get("max_ticks"), field_name="max_ticks")
        max_claims = _coerce_optional_int(payload.get("max_claims"), field_name="max_claims")
        max_flows = _coerce_optional_int(payload.get("max_flows"), field_name="max_flows")
        max_wall_clock_seconds = _coerce_optional_float(
            payload.get("max_wall_clock_seconds"),
            field_name="max_wall_clock_seconds",
        )
        result = run_worker_until_idle(
            self.config.sqlite_db,
            runtime_config=self._build_worker_runtime_config(payload),
            max_ticks=max_ticks if max_ticks is not None else 100,
            max_claims=max_claims,
            max_flows=max_flows,
            max_wall_clock_seconds=max_wall_clock_seconds,
        )
        return {"worker_loop": result.to_dict()}

    def pause(self, run_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        result = pause_run(
            self.config.sqlite_db,
            run_id,
            note=_optional_text(payload.get("note")),
            operator=_optional_text(payload.get("operator")),
        )
        return {"manual_control": result.to_dict()}

    def resume(self, run_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        result = resume_run(
            self.config.sqlite_db,
            run_id,
            mode=_optional_text(payload.get("mode")) or "normal",
            note=_optional_text(payload.get("note")),
            operator=_optional_text(payload.get("operator")),
        )
        return {"manual_control": result.to_dict()}

    def force_stop(self, run_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        result = force_stop_run(
            self.config.sqlite_db,
            run_id,
            note=_optional_text(payload.get("note")),
            operator=_optional_text(payload.get("operator")),
        )
        return {"manual_control": result.to_dict()}

    def rerun_step(self, run_id: str, payload: Mapping[str, object]) -> dict[str, object]:
        normalized_run_id = _required_text(run_id, field_name="run_id")
        step_run_id = _optional_text(payload.get("step_run_id")) or _optional_text(payload.get("source_step_run_id"))
        if step_run_id is None:
            raise ApiRequestError(
                400,
                INVALID_REQUEST_PAYLOAD,
                "rerun-step requires step_run_id in the JSON body",
            )
        step_details = get_step_run(self.config.sqlite_db, step_run_id)
        if step_details.step_run.run_id != normalized_run_id:
            raise ApiRequestError(
                409,
                RUN_STEP_MISMATCH,
                "step_run_id does not belong to the run_id in the route",
                details=f"route_run_id={normalized_run_id} step_run_run_id={step_details.step_run.run_id}",
            )
        result = rerun_run_step(
            self.config.sqlite_db,
            step_run_id,
            note=_optional_text(payload.get("note")),
            operator=_optional_text(payload.get("operator")),
        )
        return {"manual_control": result.to_dict()}

    def control_state(self, run_id: str) -> dict[str, object]:
        result = show_run_control_state(self.config.sqlite_db, run_id)
        return {"control_state": result.to_dict()}

    def cleanup_run_once(self, payload: Mapping[str, object]) -> dict[str, object]:
        result = run_cleanup_once(
            self.config.sqlite_db,
            dry_run=_coerce_bool(payload.get("dry_run"), field_name="dry_run", default=False),
            now=_optional_text(payload.get("now")),
            scopes=_coerce_optional_string_list(payload.get("scopes"), field_name="scopes"),
        )
        return {"cleanup_pass": result.to_dict()}

    def _build_worker_runtime_config(self, payload: Mapping[str, object]) -> WorkerRuntimeConfig:
        runtime_context = payload.get("runtime_context")
        if runtime_context is not None and not isinstance(runtime_context, Mapping):
            raise ApiRequestError(
                400,
                INVALID_REQUEST_PAYLOAD,
                "runtime_context must be a JSON object",
            )
        mode = _optional_text(payload.get("mode"))
        if mode is not None and mode not in {"executor-only", "executor+reviewer"}:
            raise ApiRequestError(
                400,
                INVALID_REQUEST_PAYLOAD,
                "mode must be executor-only or executor+reviewer",
                details=f"actual={mode}",
            )
        return WorkerRuntimeConfig(
            runtime_context=dict(runtime_context) if isinstance(runtime_context, Mapping) else None,
            artifact_root=_path_from_payload_or_default(
                payload,
                "artifact_root",
                self.config.default_artifact_root,
            ),
            worker_log_root=_path_from_payload_or_default(
                payload,
                "worker_log_root",
                self.config.default_worker_log_root,
            ),
            workspace_root=_path_from_payload_or_default(
                payload,
                "workspace_root",
                self.config.default_workspace_root,
            ),
            project_repo_path=_resolve_optional_path(payload.get("project_repo_path")),
            executor_worktree_path=_resolve_optional_path(payload.get("executor_worktree_path")),
            reviewer_worktree_path=_resolve_optional_path(payload.get("reviewer_worktree_path")),
            instructions_repo_path=_resolve_optional_path(payload.get("instructions_repo_path")),
            branch_base=_optional_text(payload.get("branch_base")),
            instruction_profile=_optional_text(payload.get("instruction_profile")),
            instruction_overlays=_coerce_optional_string_tuple(payload.get("instruction_overlays"), field_name="instruction_overlays"),
            task_text=_optional_text(payload.get("task_text")),
            mode=mode,
            source=_optional_text(payload.get("source")),
            thread_label=_optional_text(payload.get("thread_label")),
            constraints=_coerce_optional_string_tuple(payload.get("constraints"), field_name="constraints"),
            expected_output=_coerce_optional_string_tuple(payload.get("expected_output"), field_name="expected_output"),
            legacy_control_dir=_resolve_optional_path(payload.get("legacy_control_dir")),
            executor_runner_path=_resolve_optional_path(payload.get("executor_runner_path")),
            reviewer_runner_path=_resolve_optional_path(payload.get("reviewer_runner_path")),
            claim_now=_optional_text(payload.get("claim_now")),
        )


class ControlPlaneApiServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, config: ControlPlaneApiConfig) -> None:
        self.config = config
        self.application = ControlPlaneApiApplication(config)
        super().__init__((config.host, config.port), ControlPlaneApiHandler)


class ControlPlaneApiHandler(BaseHTTPRequestHandler):
    server_version = API_SERVER_NAME

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def _handle(self) -> None:
        request_id = _request_id_from_headers(self.headers.get("X-Request-Id"))
        try:
            status, data = self._dispatch()
            self._send_envelope(status, request_id, data=data, error=None)
        except ApiRequestError as exc:
            self._send_envelope(exc.http_status, request_id, data=None, error=exc.to_dict())
        except (TaskIntakeError, WorkerLoopError, ManualControlError, CleanupManagerError, StepRunPersistenceError, RunPersistenceError) as exc:
            mapped = _map_domain_error(exc)
            self._send_envelope(mapped.http_status, request_id, data=None, error=mapped.to_dict())
        except Exception as exc:
            self._send_envelope(
                500,
                request_id,
                data=None,
                error=ApiRequestError(
                    500,
                    UNHANDLED_SERVER_ERROR,
                    "unhandled server error",
                    details=str(exc),
                ).to_dict(),
            )

    def _dispatch(self) -> tuple[int, dict[str, object]]:
        parsed = urlparse(self.path)
        path = _normalize_path(parsed.path)
        query = parse_qs(parsed.query, keep_blank_values=False)
        application = self.server.application  # type: ignore[attr-defined]

        if path == "/v1/health":
            if self.command != "GET":
                raise ApiRequestError(405, METHOD_NOT_ALLOWED, "method not allowed for /v1/health")
            return 200, application.health()

        if path == "/v1/tasks":
            if self.command == "GET":
                return 200, application.list_tasks(query)
            if self.command == "POST":
                raise ApiRequestError(405, METHOD_NOT_ALLOWED, "use POST /v1/tasks/submit to create a task")
            raise ApiRequestError(405, METHOD_NOT_ALLOWED, "method not allowed for /v1/tasks")

        if path == "/v1/tasks/submit":
            if self.command != "POST":
                raise ApiRequestError(405, METHOD_NOT_ALLOWED, "method not allowed for /v1/tasks/submit")
            return 200, application.submit_task(self._read_json_body())

        task_match = _TASK_DETAIL_PATH_RE.match(path)
        if task_match is not None:
            if self.command != "GET":
                raise ApiRequestError(405, METHOD_NOT_ALLOWED, "method not allowed for task detail endpoint")
            run_id = unquote(task_match.group("run_id"))
            return 200, application.get_task(run_id)

        if path == "/v1/worker/tick":
            if self.command != "POST":
                raise ApiRequestError(405, METHOD_NOT_ALLOWED, "method not allowed for /v1/worker/tick")
            return 200, application.worker_tick(self._read_json_body())

        if path == "/v1/worker/run-until-idle":
            if self.command != "POST":
                raise ApiRequestError(405, METHOD_NOT_ALLOWED, "method not allowed for /v1/worker/run-until-idle")
            return 200, application.worker_run_until_idle(self._read_json_body())

        run_match = _RUN_ACTION_PATH_RE.match(path)
        if run_match is not None:
            run_id = unquote(run_match.group("run_id"))
            action = run_match.group("action")
            if action == "control-state":
                if self.command != "GET":
                    raise ApiRequestError(405, METHOD_NOT_ALLOWED, "method not allowed for run control-state endpoint")
                return 200, application.control_state(run_id)
            if self.command != "POST":
                raise ApiRequestError(405, METHOD_NOT_ALLOWED, f"method not allowed for run {action} endpoint")
            payload = self._read_json_body()
            if action == "pause":
                return 200, application.pause(run_id, payload)
            if action == "resume":
                return 200, application.resume(run_id, payload)
            if action == "force-stop":
                return 200, application.force_stop(run_id, payload)
            return 200, application.rerun_step(run_id, payload)

        if path == "/v1/cleanup/run-once":
            if self.command != "POST":
                raise ApiRequestError(405, METHOD_NOT_ALLOWED, "method not allowed for /v1/cleanup/run-once")
            return 200, application.cleanup_run_once(self._read_json_body())

        raise ApiRequestError(404, ENDPOINT_NOT_FOUND, f"unknown endpoint: {path}")

    def _read_json_body(self) -> dict[str, object]:
        content_type = (self.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
        if content_type not in JSON_CONTENT_TYPES:
            raise ApiRequestError(
                415,
                UNSUPPORTED_MEDIA_TYPE,
                "JSON body is required with Content-Type: application/json",
                details=f"content_type={content_type or 'missing'}",
            )
        length_header = self.headers.get("Content-Length")
        length = int(length_header) if length_header is not None else 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            raise ApiRequestError(400, JSON_BODY_REQUIRED, "JSON body is required")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ApiRequestError(
                400,
                INVALID_JSON_BODY,
                "request body must be valid JSON",
                details=str(exc),
            ) from exc
        if not isinstance(payload, Mapping):
            raise ApiRequestError(400, INVALID_REQUEST_PAYLOAD, "JSON body must be an object")
        return dict(payload)

    def _send_envelope(
        self,
        status: int,
        request_id: str,
        *,
        data: dict[str, object] | None,
        error: dict[str, object] | None,
    ) -> None:
        body = json.dumps(
            {
                "ok": error is None,
                "data": data,
                "error": error,
                "request_id": request_id,
            },
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Request-Id", request_id)
        self.end_headers()
        self.wfile.write(body)


def serve_control_plane_api(config: ControlPlaneApiConfig) -> None:
    server = ControlPlaneApiServer(config)
    print(f"Control Plane v2 API listening on {config.base_url}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _map_domain_error(exc: Exception) -> ApiRequestError:
    code = getattr(exc, "code", UNHANDLED_SERVER_ERROR)
    message = getattr(exc, "message", str(exc))
    details = getattr(exc, "details", None)
    database_path = getattr(exc, "database_path", None)
    stage = _error_stage(exc)
    return ApiRequestError(
        _http_status_for_error_code(code),
        code,
        message,
        details=details,
        stage=stage,
        database_path=database_path,
    )


def _error_stage(exc: Exception) -> str:
    if isinstance(exc, TaskIntakeError):
        return "task_intake"
    if isinstance(exc, WorkerLoopError):
        return "worker_loop"
    if isinstance(exc, ManualControlError):
        return "manual_control"
    if isinstance(exc, CleanupManagerError):
        return "runtime_cleanup_manager"
    if isinstance(exc, StepRunPersistenceError):
        return "step_run_persistence"
    if isinstance(exc, RunPersistenceError):
        return "run_persistence"
    return "unknown"


def _http_status_for_error_code(code: str) -> int:
    if "NOT_FOUND" in code or code in {"PROJECT_NOT_REGISTERED", "INTAKE_PROJECT_NOT_REGISTERED"}:
        return 404
    if code in {
        "MANUAL_ACTIVE_STEP_NOT_SAFE",
        "MANUAL_RUN_NOT_PAUSABLE",
        "MANUAL_RUN_NOT_PAUSED",
        "MANUAL_RUN_NOT_FORCE_STOPPABLE",
        "MANUAL_STEP_NOT_RERUNNABLE",
        RUN_STEP_MISMATCH,
    }:
        return 409
    if "INVALID" in code or "UNSUPPORTED" in code:
        return 400
    return 500


def _query_single(query: Mapping[str, Sequence[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None
    return _optional_text(values[-1])


def _query_int(query: Mapping[str, Sequence[str]], key: str, *, default: int) -> int:
    raw = _query_single(query, key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ApiRequestError(
            400,
            INVALID_QUERY_PARAMETER,
            f"{key} must be an integer",
            details=f"actual={raw!r}",
        ) from exc
    if value <= 0:
        raise ApiRequestError(
            400,
            INVALID_QUERY_PARAMETER,
            f"{key} must be greater than zero",
            details=f"actual={value}",
        )
    return value


def _normalize_path(path: str) -> str:
    if path != "/" and path.endswith("/"):
        return path.rstrip("/")
    return path


def _request_id_from_headers(header_value: str | None) -> str:
    if header_value is not None:
        normalized = header_value.strip()
        if normalized:
            return normalized
    return generate_opaque_id()


def _resolve_path(value: str | Path) -> Path:
    return Path(str(value)).expanduser().resolve()


def _resolve_optional_path(value: object) -> Path | None:
    normalized = _optional_text(value)
    if normalized is None:
        return None
    return _resolve_path(normalized)


def _path_from_payload_or_default(payload: Mapping[str, object], key: str, default: Path | None) -> Path | None:
    if key in payload:
        return _resolve_optional_path(payload.get(key))
    return default


def _required_text(value: object, *, field_name: str) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise ApiRequestError(400, INVALID_REQUEST_PAYLOAD, f"{field_name} must be a non-empty string")
    return normalized


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _coerce_bool(value: object, *, field_name: str, default: bool) -> bool:
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
    raise ApiRequestError(
        400,
        INVALID_REQUEST_PAYLOAD,
        f"{field_name} must be a boolean",
        details=f"actual={value!r}",
    )


def _coerce_optional_int(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    try:
        normalized = int(str(value).strip())
    except ValueError as exc:
        raise ApiRequestError(
            400,
            INVALID_REQUEST_PAYLOAD,
            f"{field_name} must be an integer",
            details=f"actual={value!r}",
        ) from exc
    if normalized <= 0:
        raise ApiRequestError(
            400,
            INVALID_REQUEST_PAYLOAD,
            f"{field_name} must be greater than zero",
            details=f"actual={normalized}",
        )
    return normalized


def _coerce_optional_float(value: object, *, field_name: str) -> float | None:
    if value is None:
        return None
    try:
        normalized = float(str(value).strip())
    except ValueError as exc:
        raise ApiRequestError(
            400,
            INVALID_REQUEST_PAYLOAD,
            f"{field_name} must be a number",
            details=f"actual={value!r}",
        ) from exc
    if normalized <= 0:
        raise ApiRequestError(
            400,
            INVALID_REQUEST_PAYLOAD,
            f"{field_name} must be greater than zero",
            details=f"actual={normalized}",
        )
    return normalized


def _coerce_optional_string_list(value: object, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            normalized = _optional_text(item)
            if normalized is not None:
                result.append(normalized)
        return result
    raise ApiRequestError(
        400,
        INVALID_REQUEST_PAYLOAD,
        f"{field_name} must be a string or list of strings",
        details=f"actual_type={type(value).__name__}",
    )


def _coerce_optional_string_tuple(value: object, *, field_name: str) -> tuple[str, ...] | None:
    if value is None:
        return None
    return tuple(_coerce_optional_string_list(value, field_name=field_name))
