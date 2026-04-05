from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - Linux-only by contract.
    fcntl = None

from .http_api import (
    API_DEFAULT_HOST,
    API_DEFAULT_PORT,
    ControlPlaneApiConfig,
    ControlPlaneApiConfigError,
    ControlPlaneApiServer,
    create_control_plane_api_config,
)
from .worker_loop import WorkerLoopError, WorkerRuntimeConfig, run_worker_until_idle


CONTROL_DIR = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_ROOT = CONTROL_DIR.parent / "runtime" / "control-plane-v2"
DEFAULT_RUNTIME_STATE_DIRNAME = "state"
DEFAULT_RUNTIME_PID_DIRNAME = "pid"
DEFAULT_RUNTIME_LOG_DIRNAME = "logs"
DEFAULT_RUNTIME_ARTIFACT_DIRNAME = "artifacts"
DEFAULT_RUNTIME_WORKER_LOG_DIRNAME = "worker-logs"
DEFAULT_WORKER_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_TICKS_PER_CYCLE = 100
DEFAULT_MAX_CLAIMS_PER_CYCLE = 1
DEFAULT_STARTUP_TIMEOUT_SECONDS = 15.0
DEFAULT_STOP_TIMEOUT_SECONDS = 30.0
EVENT_LOG_ROTATE_BYTES = 2 * 1024 * 1024

RUNTIME_SUPERVISOR_ALREADY_RUNNING = "RUNTIME_SUPERVISOR_ALREADY_RUNNING"
RUNTIME_SUPERVISOR_CONFIG_INVALID = "RUNTIME_SUPERVISOR_CONFIG_INVALID"
RUNTIME_SUPERVISOR_CONFIG_MISSING = "RUNTIME_SUPERVISOR_CONFIG_MISSING"
RUNTIME_SUPERVISOR_LINUX_ONLY = "RUNTIME_SUPERVISOR_LINUX_ONLY"
RUNTIME_SUPERVISOR_NOT_RUNNING = "RUNTIME_SUPERVISOR_NOT_RUNNING"
RUNTIME_SUPERVISOR_START_FAILED = "RUNTIME_SUPERVISOR_START_FAILED"
RUNTIME_SUPERVISOR_START_TIMEOUT = "RUNTIME_SUPERVISOR_START_TIMEOUT"
RUNTIME_SUPERVISOR_STATE_INVALID = "RUNTIME_SUPERVISOR_STATE_INVALID"
RUNTIME_SUPERVISOR_STOP_TIMEOUT = "RUNTIME_SUPERVISOR_STOP_TIMEOUT"


@dataclass(frozen=True)
class RuntimeLogPaths:
    event_log_path: Path
    console_log_path: Path
    worker_log_root: Path

    def to_dict(self) -> dict[str, str]:
        return {
            "event_log_path": str(self.event_log_path),
            "console_log_path": str(self.console_log_path),
            "worker_log_root": str(self.worker_log_root),
        }


@dataclass(frozen=True)
class ControlPlaneRuntimePaths:
    runtime_root: Path
    state_dir: Path
    pid_dir: Path
    log_dir: Path
    lock_path: Path
    pid_path: Path
    state_path: Path
    config_path: Path
    log_paths: RuntimeLogPaths

    def to_dict(self) -> dict[str, object]:
        return {
            "runtime_root": str(self.runtime_root),
            "state_dir": str(self.state_dir),
            "pid_dir": str(self.pid_dir),
            "log_dir": str(self.log_dir),
            "lock_path": str(self.lock_path),
            "pid_path": str(self.pid_path),
            "state_path": str(self.state_path),
            "config_path": str(self.config_path),
            "log_paths": self.log_paths.to_dict(),
        }


@dataclass(frozen=True)
class ControlPlaneRuntimeConfig:
    api_config: ControlPlaneApiConfig
    worker_runtime_config: WorkerRuntimeConfig
    worker_poll_interval_seconds: float
    max_ticks_per_cycle: int
    max_claims_per_cycle: int | None
    max_flows_per_cycle: int | None
    max_wall_clock_seconds_per_cycle: float | None
    paths: ControlPlaneRuntimePaths

    @property
    def sqlite_db(self) -> Path:
        return self.api_config.sqlite_db

    @property
    def artifact_root(self) -> Path:
        assert self.api_config.default_artifact_root is not None
        return self.api_config.default_artifact_root

    @property
    def workspace_root(self) -> Path:
        assert self.api_config.default_workspace_root is not None
        return self.api_config.default_workspace_root

    @property
    def worker_log_root(self) -> Path:
        assert self.api_config.default_worker_log_root is not None
        return self.api_config.default_worker_log_root

    @property
    def worker_mode(self) -> str:
        return self.worker_runtime_config.effective_mode()

    def to_dict(self) -> dict[str, object]:
        return {
            "sqlite_db": str(self.sqlite_db),
            "api_host": self.api_config.host,
            "api_port": self.api_config.port,
            "artifact_root": str(self.artifact_root),
            "workspace_root": str(self.workspace_root),
            "worker_log_root": str(self.worker_log_root),
            "worker_poll_interval_seconds": self.worker_poll_interval_seconds,
            "max_ticks_per_cycle": self.max_ticks_per_cycle,
            "max_claims_per_cycle": self.max_claims_per_cycle,
            "max_flows_per_cycle": self.max_flows_per_cycle,
            "max_wall_clock_seconds_per_cycle": self.max_wall_clock_seconds_per_cycle,
            "worker_mode": self.worker_mode,
            "paths": self.paths.to_dict(),
            "worker_runtime_config": _serialize_worker_runtime_config(self.worker_runtime_config),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ControlPlaneRuntimeConfig:
        paths_payload = payload.get("paths")
        if not isinstance(paths_payload, Mapping):
            raise RuntimeSupervisorError(
                RUNTIME_SUPERVISOR_STATE_INVALID,
                "runtime config payload is missing the paths block",
            )

        return create_control_plane_runtime_config(
            sqlite_db=_required_string(payload, "sqlite_db"),
            api_host=_optional_string(payload.get("api_host")),
            api_port=payload.get("api_port"),
            artifact_root=_optional_string(payload.get("artifact_root")),
            workspace_root=_optional_string(payload.get("workspace_root")),
            worker_log_root=_optional_string(payload.get("worker_log_root")),
            worker_poll_interval_seconds=payload.get("worker_poll_interval_seconds"),
            max_ticks_per_cycle=payload.get("max_ticks_per_cycle"),
            max_claims_per_cycle=payload.get("max_claims_per_cycle"),
            max_flows_per_cycle=payload.get("max_flows_per_cycle"),
            max_wall_clock_seconds_per_cycle=payload.get("max_wall_clock_seconds_per_cycle"),
            runtime_root=_required_string(paths_payload, "runtime_root"),
            runtime_state_dir=_required_string(paths_payload, "state_dir"),
            runtime_pid_dir=_required_string(paths_payload, "pid_dir"),
            runtime_log_dir=_required_string(paths_payload, "log_dir"),
            worker_runtime_config=_deserialize_worker_runtime_config(payload.get("worker_runtime_config")),
        )


@dataclass(frozen=True)
class RuntimeSupervisorStatus:
    supervisor_running: bool
    supervisor_state: str
    pid: int | None
    started_at: str | None
    stopped_at: str | None
    sqlite_db: Path | None
    api_base_url: str | None
    api_status: str | None
    worker_mode: str | None
    worker_status: str | None
    last_worker_cycle: Mapping[str, object] | None
    log_paths: RuntimeLogPaths
    lock_path: Path
    pid_path: Path
    state_path: Path
    config_path: Path
    stale_pid: bool
    lock_held: bool
    degraded_reasons: tuple[str, ...]
    last_error: Mapping[str, object] | None
    launch_mode: str | None
    runtime_root: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "supervisor_running": self.supervisor_running,
            "supervisor_state": self.supervisor_state,
            "pid": self.pid,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "sqlite_db": str(self.sqlite_db) if self.sqlite_db is not None else None,
            "api_base_url": self.api_base_url,
            "api_status": self.api_status,
            "worker_mode": self.worker_mode,
            "worker_status": self.worker_status,
            "last_worker_cycle": dict(self.last_worker_cycle) if self.last_worker_cycle is not None else None,
            "log_paths": self.log_paths.to_dict(),
            "lock_path": str(self.lock_path),
            "pid_path": str(self.pid_path),
            "state_path": str(self.state_path),
            "config_path": str(self.config_path),
            "stale_pid": self.stale_pid,
            "lock_held": self.lock_held,
            "degraded_reasons": list(self.degraded_reasons),
            "last_error": dict(self.last_error) if self.last_error is not None else None,
            "launch_mode": self.launch_mode,
            "runtime_root": str(self.runtime_root),
        }


@dataclass(frozen=True)
class RuntimeSupervisorRestartResult:
    before: RuntimeSupervisorStatus
    after: RuntimeSupervisorStatus

    def to_dict(self) -> dict[str, object]:
        return {
            "before": self.before.to_dict(),
            "after": self.after.to_dict(),
        }


class RuntimeSupervisorError(Exception):
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


class ControlPlaneRuntimeSupervisor:
    def __init__(self, config: ControlPlaneRuntimeConfig, *, launch_mode: str) -> None:
        _require_linux_runtime_supervisor()
        self.config = config
        self.launch_mode = launch_mode
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._log_lock = threading.Lock()
        self._lock_fd: int | None = None
        self._api_server: ControlPlaneApiServer | None = None
        self._api_thread: threading.Thread | None = None
        self._worker_thread: threading.Thread | None = None
        self._signal_count = 0
        self._state = self._build_initial_state()

    def run(self) -> int:
        self._ensure_runtime_dirs()
        self._acquire_lock()
        self._persist_config()
        self._write_pid_file()
        self._write_state()
        self._append_event("info", "supervisor_starting", launch_mode=self.launch_mode)
        self._install_signal_handlers()

        try:
            self._api_server = ControlPlaneApiServer(self.config.api_config)
        except OSError as exc:
            self._record_api_failure(exc)
            self._append_event("error", "api_start_failed", error=str(exc))
            self._release_lock()
            return 1

        self._start_api_thread()
        self._start_worker_thread()
        self._set_supervisor_state("running")
        self._append_event("info", "supervisor_running", pid=os.getpid(), api_base_url=self.config.api_config.base_url)

        try:
            while not self._stop_event.is_set():
                self._synchronize_thread_state()
                time.sleep(0.5)
        except KeyboardInterrupt:
            self._request_stop("keyboard_interrupt")

        self._begin_shutdown()
        return 0

    def _build_initial_state(self) -> dict[str, Any]:
        now = _utc_now()
        return {
            "version": 1,
            "service": "control-plane-v2-runtime-supervisor",
            "updated_at": now,
            "config": self.config.to_dict(),
            "supervisor": {
                "state": "starting",
                "pid": os.getpid(),
                "started_at": now,
                "stopped_at": None,
                "launch_mode": self.launch_mode,
            },
            "api": {
                "status": "starting",
                "started_at": None,
                "failed_at": None,
                "last_error": None,
            },
            "worker": {
                "status": "starting",
                "started_at": None,
                "failed_at": None,
                "last_error": None,
                "consecutive_failures": 0,
                "next_cycle_not_before": None,
                "last_cycle": None,
            },
            "degraded_reasons": [],
            "last_error": None,
            "paths": self.config.paths.to_dict(),
        }

    def _ensure_runtime_dirs(self) -> None:
        self.config.paths.runtime_root.mkdir(parents=True, exist_ok=True)
        self.config.paths.state_dir.mkdir(parents=True, exist_ok=True)
        self.config.paths.pid_dir.mkdir(parents=True, exist_ok=True)
        self.config.paths.log_dir.mkdir(parents=True, exist_ok=True)
        self.config.worker_log_root.mkdir(parents=True, exist_ok=True)

    def _acquire_lock(self) -> None:
        lock_path = self.config.paths.lock_path
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        assert fcntl is not None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise RuntimeSupervisorError(
                RUNTIME_SUPERVISOR_ALREADY_RUNNING,
                "control plane runtime supervisor is already active for this runtime root",
                details=f"lock_path={lock_path}",
            ) from exc

        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode("utf-8"))
        os.fsync(fd)
        self._lock_fd = fd

    def _release_lock(self) -> None:
        if self._lock_fd is None:
            return
        assert fcntl is not None
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(self._lock_fd)
            self._lock_fd = None

    def _persist_config(self) -> None:
        _write_json_file(self.config.paths.config_path, self.config.to_dict())

    def _write_pid_file(self) -> None:
        self.config.paths.pid_path.write_text(f"{os.getpid()}\n", encoding="utf-8")

    def _remove_pid_file(self) -> None:
        try:
            self.config.paths.pid_path.unlink()
        except FileNotFoundError:
            pass

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        self._signal_count += 1
        self._request_stop(signal.Signals(signum).name.lower())

    def _request_stop(self, reason: str) -> None:
        self._stop_event.set()
        self._append_event("info", "supervisor_stop_requested", reason=reason, signal_count=self._signal_count)
        with self._state_lock:
            self._state["supervisor"]["state"] = "stopping"
            self._state["updated_at"] = _utc_now()
            self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
            self._write_state_locked()

    def _start_api_thread(self) -> None:
        self._api_thread = threading.Thread(target=self._run_api_thread, name="control-plane-runtime-api", daemon=True)
        self._api_thread.start()

    def _start_worker_thread(self) -> None:
        self._worker_thread = threading.Thread(target=self._run_worker_thread, name="control-plane-runtime-worker", daemon=True)
        self._worker_thread.start()
        with self._state_lock:
            self._state["worker"]["status"] = "running"
            self._state["worker"]["started_at"] = _utc_now()
            self._state["updated_at"] = _utc_now()
            self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
            self._write_state_locked()

    def _run_api_thread(self) -> None:
        with self._state_lock:
            self._state["api"]["status"] = "running"
            self._state["api"]["started_at"] = _utc_now()
            self._state["updated_at"] = _utc_now()
            self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
            self._write_state_locked()
        self._append_event("info", "api_started", base_url=self.config.api_config.base_url)
        assert self._api_server is not None

        try:
            self._api_server.serve_forever(poll_interval=0.5)
        except Exception as exc:  # pragma: no cover
            self._record_api_failure(exc)
            self._append_event("error", "api_failed", error=str(exc))
        finally:
            self._api_server.server_close()
            with self._state_lock:
                if self._state["api"]["status"] == "running":
                    self._state["api"]["status"] = "stopped" if self._stop_event.is_set() else "failed"
                self._state["updated_at"] = _utc_now()
                self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
                self._write_state_locked()

    def _run_worker_thread(self) -> None:
        try:
            while not self._stop_event.is_set():
                cycle_started_at = _utc_now()
                cycle_started_monotonic = time.monotonic()
                with self._state_lock:
                    self._state["worker"]["status"] = "running"
                    self._state["worker"]["next_cycle_not_before"] = None
                    self._state["updated_at"] = _utc_now()
                    self._write_state_locked()

                self._append_event(
                    "info",
                    "worker_cycle_started",
                    worker_mode=self.config.worker_mode,
                    max_ticks_per_cycle=self.config.max_ticks_per_cycle,
                    max_claims_per_cycle=self.config.max_claims_per_cycle,
                    max_flows_per_cycle=self.config.max_flows_per_cycle,
                    max_wall_clock_seconds_per_cycle=self.config.max_wall_clock_seconds_per_cycle,
                )

                try:
                    result = run_worker_until_idle(
                        self.config.sqlite_db,
                        runtime_config=self.config.worker_runtime_config,
                        max_ticks=self.config.max_ticks_per_cycle,
                        max_claims=self.config.max_claims_per_cycle,
                        max_flows=self.config.max_flows_per_cycle,
                        max_wall_clock_seconds=self.config.max_wall_clock_seconds_per_cycle,
                    )
                    cycle_summary = _build_cycle_summary(
                        result=result,
                        cycle_started_at=cycle_started_at,
                        cycle_started_monotonic=cycle_started_monotonic,
                    )
                    self._record_worker_cycle(cycle_summary)
                    self._append_event("info", "worker_cycle_completed", **cycle_summary)
                    sleep_seconds = 0.0 if result.claims_processed > 0 and result.ended_reason not in {"dispatch_failed", "ingestion_failed"} else self.config.worker_poll_interval_seconds
                except WorkerLoopError as exc:
                    cycle_summary = _build_worker_exception_summary(
                        exc=exc,
                        cycle_started_at=cycle_started_at,
                        cycle_started_monotonic=cycle_started_monotonic,
                    )
                    self._record_worker_cycle(cycle_summary)
                    self._append_event("error", "worker_cycle_failed", **cycle_summary)
                    sleep_seconds = self.config.worker_poll_interval_seconds

                if self._stop_event.is_set():
                    break
                if sleep_seconds > 0:
                    with self._state_lock:
                        self._state["worker"]["next_cycle_not_before"] = _utc_after_seconds(sleep_seconds)
                        self._state["updated_at"] = _utc_now()
                        self._write_state_locked()
                    if self._stop_event.wait(sleep_seconds):
                        break
        except Exception as exc:  # pragma: no cover
            self._record_worker_thread_failure(exc)
            self._append_event("error", "worker_thread_failed", error=str(exc))
        finally:
            with self._state_lock:
                if self._state["worker"]["status"] != "failed":
                    self._state["worker"]["status"] = "stopped" if self._stop_event.is_set() else self._state["worker"]["status"]
                self._state["worker"]["next_cycle_not_before"] = None
                self._state["updated_at"] = _utc_now()
                self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
                self._write_state_locked()

    def _record_api_failure(self, exc: Exception) -> None:
        error_payload = _exception_payload(exc)
        with self._state_lock:
            self._state["api"]["status"] = "failed"
            self._state["api"]["failed_at"] = _utc_now()
            self._state["api"]["last_error"] = error_payload
            self._state["last_error"] = error_payload
            self._state["updated_at"] = _utc_now()
            self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
            self._write_state_locked()

    def _record_worker_thread_failure(self, exc: Exception) -> None:
        error_payload = _exception_payload(exc)
        with self._state_lock:
            self._state["worker"]["status"] = "failed"
            self._state["worker"]["failed_at"] = _utc_now()
            self._state["worker"]["last_error"] = error_payload
            self._state["last_error"] = error_payload
            self._state["updated_at"] = _utc_now()
            self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
            self._write_state_locked()

    def _record_worker_cycle(self, cycle_summary: Mapping[str, object]) -> None:
        cycle_state = _optional_string(cycle_summary.get("cycle_state")) or "error"
        last_error = cycle_summary.get("last_error") if isinstance(cycle_summary.get("last_error"), Mapping) else None

        with self._state_lock:
            self._state["worker"]["last_cycle"] = dict(cycle_summary)
            self._state["worker"]["next_cycle_not_before"] = None
            if cycle_state == "ok":
                self._state["worker"]["consecutive_failures"] = 0
                self._state["worker"]["last_error"] = None
                self._state["last_error"] = None if self._state["api"]["status"] == "running" else self._state["api"]["last_error"]
            else:
                self._state["worker"]["consecutive_failures"] = int(self._state["worker"]["consecutive_failures"]) + 1
                self._state["worker"]["failed_at"] = _utc_now()
                self._state["worker"]["last_error"] = dict(last_error) if last_error is not None else {
                    "code": "WORKER_CYCLE_FAILED",
                    "message": "worker cycle ended with an error status",
                    "details": _optional_string(cycle_summary.get("ended_reason")),
                }
                self._state["last_error"] = self._state["worker"]["last_error"]
            self._state["updated_at"] = _utc_now()
            self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
            self._write_state_locked()

    def _synchronize_thread_state(self) -> None:
        with self._state_lock:
            degraded_reasons = self._compute_degraded_reasons_locked()
            current_state = self._state["supervisor"]["state"]
            if current_state not in {"stopping", "stopped"}:
                self._state["supervisor"]["state"] = "degraded" if degraded_reasons else "running"
            self._state["degraded_reasons"] = degraded_reasons
            self._state["updated_at"] = _utc_now()
            self._write_state_locked()

    def _compute_degraded_reasons_locked(self) -> list[str]:
        reasons: list[str] = []
        if self._state["api"]["status"] not in {"running", "stopped"}:
            reasons.append(f"api:{self._state['api']['status']}")
        if self._state["worker"]["status"] == "failed":
            reasons.append("worker:thread_failed")
        if int(self._state["worker"]["consecutive_failures"]) > 0:
            reasons.append("worker:last_cycle_failed")
        if self._api_thread is not None and not self._api_thread.is_alive() and not self._stop_event.is_set():
            reasons.append("api:thread_not_alive")
        if self._worker_thread is not None and not self._worker_thread.is_alive() and not self._stop_event.is_set():
            reasons.append("worker:thread_not_alive")
        return reasons

    def _set_supervisor_state(self, state: str) -> None:
        with self._state_lock:
            self._state["supervisor"]["state"] = state
            self._state["updated_at"] = _utc_now()
            self._state["degraded_reasons"] = self._compute_degraded_reasons_locked()
            self._write_state_locked()

    def _begin_shutdown(self) -> None:
        self._set_supervisor_state("stopping")
        self._append_event("info", "supervisor_stopping")
        self._stop_event.set()

        if self._api_server is not None:
            self._api_server.shutdown()
        if self._api_thread is not None:
            self._api_thread.join()
        if self._worker_thread is not None:
            self._worker_thread.join()

        stopped_at = _utc_now()
        with self._state_lock:
            self._state["supervisor"]["state"] = "stopped"
            self._state["supervisor"]["stopped_at"] = stopped_at
            self._state["worker"]["status"] = "stopped"
            if self._state["api"]["status"] == "running":
                self._state["api"]["status"] = "stopped"
            self._state["updated_at"] = stopped_at
            self._state["degraded_reasons"] = []
            self._write_state_locked()

        self._remove_pid_file()
        self._append_event("info", "supervisor_stopped", stopped_at=stopped_at)
        self._release_lock()

    def _append_event(self, level: str, event: str, **fields: object) -> None:
        payload = {
            "timestamp": _utc_now(),
            "level": level,
            "event": event,
            "pid": os.getpid(),
            "sqlite_db": str(self.config.sqlite_db),
            "api_base_url": self.config.api_config.base_url,
            **fields,
        }
        with self._log_lock:
            _rotate_log_if_needed(self.config.paths.log_paths.event_log_path)
            with self.config.paths.log_paths.event_log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")

    def _write_state(self) -> None:
        with self._state_lock:
            self._write_state_locked()

    def _write_state_locked(self) -> None:
        _write_json_file(self.config.paths.state_path, self._state)


def create_control_plane_runtime_config(
    *,
    sqlite_db: str | Path,
    api_host: str | None = None,
    api_port: int | str | None = None,
    artifact_root: str | Path | None = None,
    workspace_root: str | Path | None = None,
    worker_log_root: str | Path | None = None,
    worker_poll_interval_seconds: int | float | str | None = None,
    max_ticks_per_cycle: int | str | None = None,
    max_claims_per_cycle: int | str | None = None,
    max_flows_per_cycle: int | str | None = None,
    max_wall_clock_seconds_per_cycle: int | float | str | None = None,
    runtime_root: str | Path | None = None,
    runtime_state_dir: str | Path | None = None,
    runtime_pid_dir: str | Path | None = None,
    runtime_log_dir: str | Path | None = None,
    worker_runtime_config: WorkerRuntimeConfig | None = None,
) -> ControlPlaneRuntimeConfig:
    _require_linux_runtime_supervisor()
    base_paths = resolve_runtime_paths(
        runtime_root=runtime_root,
        runtime_state_dir=runtime_state_dir,
        runtime_pid_dir=runtime_pid_dir,
        runtime_log_dir=runtime_log_dir,
    )
    resolved_artifact_root = _resolve_path(artifact_root) if artifact_root is not None else base_paths.runtime_root / DEFAULT_RUNTIME_ARTIFACT_DIRNAME
    resolved_workspace_root = _resolve_path(workspace_root) if workspace_root is not None else CONTROL_DIR.parent
    resolved_worker_log_root = _resolve_path(worker_log_root) if worker_log_root is not None else base_paths.runtime_root / DEFAULT_RUNTIME_WORKER_LOG_DIRNAME

    paths = ControlPlaneRuntimePaths(
        runtime_root=base_paths.runtime_root,
        state_dir=base_paths.state_dir,
        pid_dir=base_paths.pid_dir,
        log_dir=base_paths.log_dir,
        lock_path=base_paths.lock_path,
        pid_path=base_paths.pid_path,
        state_path=base_paths.state_path,
        config_path=base_paths.config_path,
        log_paths=RuntimeLogPaths(
            event_log_path=base_paths.log_paths.event_log_path,
            console_log_path=base_paths.log_paths.console_log_path,
            worker_log_root=resolved_worker_log_root,
        ),
    )

    try:
        api_config = create_control_plane_api_config(
            host=api_host,
            port=api_port,
            sqlite_db=sqlite_db,
            default_artifact_root=resolved_artifact_root,
            default_workspace_root=resolved_workspace_root,
            default_worker_log_root=resolved_worker_log_root,
        )
    except ControlPlaneApiConfigError as exc:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_CONFIG_INVALID,
            exc.message,
            details=exc.details,
        ) from exc

    poll_interval = _coerce_positive_float(
        worker_poll_interval_seconds,
        field_name="worker_poll_interval_seconds",
        default=DEFAULT_WORKER_POLL_INTERVAL_SECONDS,
    )
    ticks_per_cycle = _coerce_positive_int(
        max_ticks_per_cycle,
        field_name="max_ticks_per_cycle",
        default=DEFAULT_MAX_TICKS_PER_CYCLE,
    )
    claims_per_cycle = _coerce_optional_positive_int(max_claims_per_cycle, field_name="max_claims_per_cycle", default=DEFAULT_MAX_CLAIMS_PER_CYCLE)
    flows_per_cycle = _coerce_optional_positive_int(max_flows_per_cycle, field_name="max_flows_per_cycle", default=None)
    wall_clock_seconds = _coerce_optional_positive_float(
        max_wall_clock_seconds_per_cycle,
        field_name="max_wall_clock_seconds_per_cycle",
        default=None,
    )

    base_worker_runtime_config = worker_runtime_config or WorkerRuntimeConfig()
    resolved_worker_runtime_config = WorkerRuntimeConfig(
        runtime_context=dict(base_worker_runtime_config.runtime_context) if base_worker_runtime_config.runtime_context is not None else None,
        artifact_root=base_worker_runtime_config.artifact_root or resolved_artifact_root,
        worker_log_root=base_worker_runtime_config.worker_log_root or resolved_worker_log_root,
        workspace_root=base_worker_runtime_config.workspace_root or resolved_workspace_root,
        project_repo_path=base_worker_runtime_config.project_repo_path,
        executor_worktree_path=base_worker_runtime_config.executor_worktree_path,
        reviewer_worktree_path=base_worker_runtime_config.reviewer_worktree_path,
        instructions_repo_path=base_worker_runtime_config.instructions_repo_path,
        branch_base=base_worker_runtime_config.branch_base,
        instruction_profile=base_worker_runtime_config.instruction_profile,
        instruction_overlays=tuple(base_worker_runtime_config.instruction_overlays) if base_worker_runtime_config.instruction_overlays is not None else None,
        task_text=base_worker_runtime_config.task_text,
        mode=base_worker_runtime_config.mode,
        source=base_worker_runtime_config.source,
        thread_label=base_worker_runtime_config.thread_label,
        constraints=tuple(base_worker_runtime_config.constraints) if base_worker_runtime_config.constraints is not None else None,
        expected_output=tuple(base_worker_runtime_config.expected_output) if base_worker_runtime_config.expected_output is not None else None,
        legacy_control_dir=base_worker_runtime_config.legacy_control_dir,
        executor_runner_path=base_worker_runtime_config.executor_runner_path,
        reviewer_runner_path=base_worker_runtime_config.reviewer_runner_path,
        claim_now=base_worker_runtime_config.claim_now,
    )

    return ControlPlaneRuntimeConfig(
        api_config=api_config,
        worker_runtime_config=resolved_worker_runtime_config,
        worker_poll_interval_seconds=poll_interval,
        max_ticks_per_cycle=ticks_per_cycle,
        max_claims_per_cycle=claims_per_cycle,
        max_flows_per_cycle=flows_per_cycle,
        max_wall_clock_seconds_per_cycle=wall_clock_seconds,
        paths=paths,
    )


def resolve_runtime_paths(
    *,
    runtime_root: str | Path | None = None,
    runtime_state_dir: str | Path | None = None,
    runtime_pid_dir: str | Path | None = None,
    runtime_log_dir: str | Path | None = None,
) -> ControlPlaneRuntimePaths:
    resolved_runtime_root = _resolve_path(runtime_root) if runtime_root is not None else DEFAULT_RUNTIME_ROOT
    resolved_state_dir = _resolve_path(runtime_state_dir) if runtime_state_dir is not None else resolved_runtime_root / DEFAULT_RUNTIME_STATE_DIRNAME
    resolved_pid_dir = _resolve_path(runtime_pid_dir) if runtime_pid_dir is not None else resolved_runtime_root / DEFAULT_RUNTIME_PID_DIRNAME
    resolved_log_dir = _resolve_path(runtime_log_dir) if runtime_log_dir is not None else resolved_runtime_root / DEFAULT_RUNTIME_LOG_DIRNAME
    return ControlPlaneRuntimePaths(
        runtime_root=resolved_runtime_root,
        state_dir=resolved_state_dir,
        pid_dir=resolved_pid_dir,
        log_dir=resolved_log_dir,
        lock_path=resolved_state_dir / "supervisor.lock",
        pid_path=resolved_pid_dir / "supervisor.pid",
        state_path=resolved_state_dir / "runtime-state.json",
        config_path=resolved_state_dir / "runtime-config.json",
        log_paths=RuntimeLogPaths(
            event_log_path=resolved_log_dir / "runtime-events.jsonl",
            console_log_path=resolved_log_dir / "runtime-console.log",
            worker_log_root=resolved_runtime_root / DEFAULT_RUNTIME_WORKER_LOG_DIRNAME,
        ),
    )


def load_control_plane_runtime_config(
    *,
    runtime_root: str | Path | None = None,
    runtime_state_dir: str | Path | None = None,
    runtime_pid_dir: str | Path | None = None,
    runtime_log_dir: str | Path | None = None,
    config_json_path: str | Path | None = None,
) -> ControlPlaneRuntimeConfig:
    paths = resolve_runtime_paths(
        runtime_root=runtime_root,
        runtime_state_dir=runtime_state_dir,
        runtime_pid_dir=runtime_pid_dir,
        runtime_log_dir=runtime_log_dir,
    )
    target_path = _resolve_path(config_json_path) if config_json_path is not None else paths.config_path
    if not target_path.exists():
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_CONFIG_MISSING,
            "runtime supervisor config is missing",
            details=f"config_path={target_path}",
        )
    payload = _load_json_file(target_path)
    if not isinstance(payload, Mapping):
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_STATE_INVALID,
            "runtime supervisor config is not a JSON object",
            details=f"config_path={target_path}",
        )
    return ControlPlaneRuntimeConfig.from_dict(payload)


def run_control_plane_runtime_foreground(config: ControlPlaneRuntimeConfig, *, launch_mode: str = "foreground") -> int:
    supervisor = ControlPlaneRuntimeSupervisor(config, launch_mode=launch_mode)
    return supervisor.run()


def start_control_plane_runtime(
    config: ControlPlaneRuntimeConfig,
    *,
    startup_timeout_seconds: int | float = DEFAULT_STARTUP_TIMEOUT_SECONDS,
) -> RuntimeSupervisorStatus:
    _require_linux_runtime_supervisor()
    status = get_control_plane_runtime_status(config=config)
    if status.supervisor_running:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_ALREADY_RUNNING,
            "control plane runtime supervisor is already active for this runtime root",
            details=f"pid={status.pid} lock_path={config.paths.lock_path}",
        )

    config.paths.state_dir.mkdir(parents=True, exist_ok=True)
    config.paths.pid_dir.mkdir(parents=True, exist_ok=True)
    config.paths.log_dir.mkdir(parents=True, exist_ok=True)
    _write_json_file(config.paths.config_path, config.to_dict())

    _rotate_log_if_needed(config.paths.log_paths.console_log_path)
    console_handle = config.paths.log_paths.console_log_path.open("a", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            str(CONTROL_DIR / "scripts" / "run-control-plane-runtime-foreground"),
            "--runtime-config-json",
            str(config.paths.config_path),
            "--launch-mode",
            "background",
        ],
        cwd=CONTROL_DIR,
        stdin=subprocess.DEVNULL,
        stdout=console_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    console_handle.close()

    deadline = time.monotonic() + float(startup_timeout_seconds)
    while time.monotonic() < deadline:
        status = get_control_plane_runtime_status(config=config)
        if status.supervisor_running:
            return status
        if process.poll() is not None:
            raise RuntimeSupervisorError(
                RUNTIME_SUPERVISOR_START_FAILED,
                "runtime supervisor exited before becoming ready",
                details=f"console_log_path={config.paths.log_paths.console_log_path}",
            )
        time.sleep(0.2)

    raise RuntimeSupervisorError(
        RUNTIME_SUPERVISOR_START_TIMEOUT,
        "timed out waiting for the runtime supervisor to report running state",
        details=f"console_log_path={config.paths.log_paths.console_log_path}",
    )


def stop_control_plane_runtime(
    *,
    config: ControlPlaneRuntimeConfig | None = None,
    runtime_root: str | Path | None = None,
    runtime_state_dir: str | Path | None = None,
    runtime_pid_dir: str | Path | None = None,
    runtime_log_dir: str | Path | None = None,
    timeout_seconds: int | float = DEFAULT_STOP_TIMEOUT_SECONDS,
) -> RuntimeSupervisorStatus:
    status = get_control_plane_runtime_status(
        config=config,
        runtime_root=runtime_root,
        runtime_state_dir=runtime_state_dir,
        runtime_pid_dir=runtime_pid_dir,
        runtime_log_dir=runtime_log_dir,
    )
    if not status.supervisor_running or status.pid is None:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_NOT_RUNNING,
            "control plane runtime supervisor is not running",
            details=f"lock_path={status.lock_path}",
        )

    os.kill(status.pid, signal.SIGTERM)
    deadline = time.monotonic() + float(timeout_seconds)
    while time.monotonic() < deadline:
        current = get_control_plane_runtime_status(
            config=config,
            runtime_root=runtime_root,
            runtime_state_dir=runtime_state_dir,
            runtime_pid_dir=runtime_pid_dir,
            runtime_log_dir=runtime_log_dir,
        )
        if not current.supervisor_running:
            return current
        time.sleep(0.2)

    raise RuntimeSupervisorError(
        RUNTIME_SUPERVISOR_STOP_TIMEOUT,
        "timed out waiting for the runtime supervisor to stop",
        details=f"pid={status.pid} state_path={status.state_path}",
    )


def restart_control_plane_runtime(
    *,
    config: ControlPlaneRuntimeConfig | None = None,
    runtime_root: str | Path | None = None,
    runtime_state_dir: str | Path | None = None,
    runtime_pid_dir: str | Path | None = None,
    runtime_log_dir: str | Path | None = None,
    startup_timeout_seconds: int | float = DEFAULT_STARTUP_TIMEOUT_SECONDS,
    stop_timeout_seconds: int | float = DEFAULT_STOP_TIMEOUT_SECONDS,
) -> RuntimeSupervisorRestartResult:
    effective_config = config or load_control_plane_runtime_config(
        runtime_root=runtime_root,
        runtime_state_dir=runtime_state_dir,
        runtime_pid_dir=runtime_pid_dir,
        runtime_log_dir=runtime_log_dir,
    )
    before = get_control_plane_runtime_status(config=effective_config)
    if before.supervisor_running:
        stop_control_plane_runtime(
            config=effective_config,
            timeout_seconds=stop_timeout_seconds,
        )
    after = start_control_plane_runtime(effective_config, startup_timeout_seconds=startup_timeout_seconds)
    return RuntimeSupervisorRestartResult(before=before, after=after)


def get_control_plane_runtime_status(
    *,
    config: ControlPlaneRuntimeConfig | None = None,
    runtime_root: str | Path | None = None,
    runtime_state_dir: str | Path | None = None,
    runtime_pid_dir: str | Path | None = None,
    runtime_log_dir: str | Path | None = None,
) -> RuntimeSupervisorStatus:
    paths = config.paths if config is not None else resolve_runtime_paths(
        runtime_root=runtime_root,
        runtime_state_dir=runtime_state_dir,
        runtime_pid_dir=runtime_pid_dir,
        runtime_log_dir=runtime_log_dir,
    )
    persisted_config = config
    if persisted_config is None and paths.config_path.exists():
        try:
            persisted_config = load_control_plane_runtime_config(
                runtime_root=paths.runtime_root,
                runtime_state_dir=paths.state_dir,
                runtime_pid_dir=paths.pid_dir,
                runtime_log_dir=paths.log_dir,
            )
        except RuntimeSupervisorError:
            persisted_config = None

    state_payload = _load_json_file(paths.state_path) if paths.state_path.exists() else {}
    supervisor_payload = state_payload.get("supervisor") if isinstance(state_payload.get("supervisor"), Mapping) else {}
    api_payload = state_payload.get("api") if isinstance(state_payload.get("api"), Mapping) else {}
    worker_payload = state_payload.get("worker") if isinstance(state_payload.get("worker"), Mapping) else {}

    pid = _read_pid_file(paths.pid_path) or _coerce_optional_int(supervisor_payload.get("pid"))
    pid_running = _pid_is_running(pid)
    lock_held = _lock_is_held(paths.lock_path)
    supervisor_running = bool(pid is not None and pid_running and lock_held)

    supervisor_state = _optional_string(supervisor_payload.get("state")) or ("running" if supervisor_running else "stopped")
    if not supervisor_running and supervisor_state in {"starting", "running", "degraded", "stopping"}:
        supervisor_state = "stopped"

    sqlite_db = persisted_config.sqlite_db if persisted_config is not None else _resolve_optional_path_from_config(state_payload, "sqlite_db")
    api_base_url = persisted_config.api_config.base_url if persisted_config is not None else None
    if api_base_url is None:
        api_host = _optional_string(_nested_get(state_payload, "config", "api_host")) or API_DEFAULT_HOST
        api_port = _coerce_optional_int(_nested_get(state_payload, "config", "api_port")) or API_DEFAULT_PORT
        api_base_url = f"http://{api_host}:{api_port}"

    worker_mode = persisted_config.worker_mode if persisted_config is not None else _optional_string(_nested_get(state_payload, "config", "worker_mode"))
    log_paths = persisted_config.paths.log_paths if persisted_config is not None else RuntimeLogPaths(
        event_log_path=paths.log_paths.event_log_path,
        console_log_path=paths.log_paths.console_log_path,
        worker_log_root=_resolve_optional_path_from_config(state_payload, "worker_log_root") or paths.log_paths.worker_log_root,
    )

    raw_degraded_reasons = state_payload.get("degraded_reasons") if isinstance(state_payload, Mapping) else []
    degraded_reasons = tuple(
        str(item)
        for item in (raw_degraded_reasons if isinstance(raw_degraded_reasons, list) else [])
        if isinstance(item, str)
    )
    return RuntimeSupervisorStatus(
        supervisor_running=supervisor_running,
        supervisor_state=supervisor_state,
        pid=pid,
        started_at=_optional_string(supervisor_payload.get("started_at")),
        stopped_at=_optional_string(supervisor_payload.get("stopped_at")),
        sqlite_db=sqlite_db,
        api_base_url=api_base_url,
        api_status=_optional_string(api_payload.get("status")),
        worker_mode=worker_mode,
        worker_status=_optional_string(worker_payload.get("status")),
        last_worker_cycle=worker_payload.get("last_cycle") if isinstance(worker_payload.get("last_cycle"), Mapping) else None,
        log_paths=log_paths,
        lock_path=paths.lock_path,
        pid_path=paths.pid_path,
        state_path=paths.state_path,
        config_path=paths.config_path,
        stale_pid=bool(pid is not None and not pid_running),
        lock_held=lock_held,
        degraded_reasons=degraded_reasons,
        last_error=state_payload.get("last_error") if isinstance(state_payload.get("last_error"), Mapping) else None,
        launch_mode=_optional_string(supervisor_payload.get("launch_mode")),
        runtime_root=paths.runtime_root,
    )


def _serialize_worker_runtime_config(config: WorkerRuntimeConfig) -> dict[str, object]:
    return {
        "runtime_context": dict(config.runtime_context) if config.runtime_context is not None else None,
        "artifact_root": str(config.artifact_root) if config.artifact_root is not None else None,
        "worker_log_root": str(config.worker_log_root) if config.worker_log_root is not None else None,
        "workspace_root": str(config.workspace_root) if config.workspace_root is not None else None,
        "project_repo_path": str(config.project_repo_path) if config.project_repo_path is not None else None,
        "executor_worktree_path": str(config.executor_worktree_path) if config.executor_worktree_path is not None else None,
        "reviewer_worktree_path": str(config.reviewer_worktree_path) if config.reviewer_worktree_path is not None else None,
        "instructions_repo_path": str(config.instructions_repo_path) if config.instructions_repo_path is not None else None,
        "branch_base": config.branch_base,
        "instruction_profile": config.instruction_profile,
        "instruction_overlays": list(config.instruction_overlays) if config.instruction_overlays is not None else None,
        "task_text": config.task_text,
        "mode": config.mode,
        "source": config.source,
        "thread_label": config.thread_label,
        "constraints": list(config.constraints) if config.constraints is not None else None,
        "expected_output": list(config.expected_output) if config.expected_output is not None else None,
        "legacy_control_dir": str(config.legacy_control_dir) if config.legacy_control_dir is not None else None,
        "executor_runner_path": str(config.executor_runner_path) if config.executor_runner_path is not None else None,
        "reviewer_runner_path": str(config.reviewer_runner_path) if config.reviewer_runner_path is not None else None,
        "claim_now": config.claim_now,
    }


def _deserialize_worker_runtime_config(payload: object) -> WorkerRuntimeConfig | None:
    if payload is None:
        return None
    if not isinstance(payload, Mapping):
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_STATE_INVALID,
            "worker_runtime_config payload must be a JSON object",
        )
    return WorkerRuntimeConfig(
        runtime_context=dict(payload["runtime_context"]) if isinstance(payload.get("runtime_context"), Mapping) else None,
        artifact_root=_resolve_optional_path(payload.get("artifact_root")),
        worker_log_root=_resolve_optional_path(payload.get("worker_log_root")),
        workspace_root=_resolve_optional_path(payload.get("workspace_root")),
        project_repo_path=_resolve_optional_path(payload.get("project_repo_path")),
        executor_worktree_path=_resolve_optional_path(payload.get("executor_worktree_path")),
        reviewer_worktree_path=_resolve_optional_path(payload.get("reviewer_worktree_path")),
        instructions_repo_path=_resolve_optional_path(payload.get("instructions_repo_path")),
        branch_base=_optional_string(payload.get("branch_base")),
        instruction_profile=_optional_string(payload.get("instruction_profile")),
        instruction_overlays=_optional_tuple(payload.get("instruction_overlays")),
        task_text=_optional_string(payload.get("task_text")),
        mode=_optional_string(payload.get("mode")),
        source=_optional_string(payload.get("source")),
        thread_label=_optional_string(payload.get("thread_label")),
        constraints=_optional_tuple(payload.get("constraints")),
        expected_output=_optional_tuple(payload.get("expected_output")),
        legacy_control_dir=_resolve_optional_path(payload.get("legacy_control_dir")),
        executor_runner_path=_resolve_optional_path(payload.get("executor_runner_path")),
        reviewer_runner_path=_resolve_optional_path(payload.get("reviewer_runner_path")),
        claim_now=_optional_string(payload.get("claim_now")),
    )


def _build_cycle_summary(
    *,
    result: Any,
    cycle_started_at: str,
    cycle_started_monotonic: float,
) -> dict[str, object]:
    return {
        "cycle_state": "error" if result.ended_reason in {"dispatch_failed", "ingestion_failed"} else "ok",
        "started_at": cycle_started_at,
        "completed_at": _utc_now(),
        "duration_seconds": round(time.monotonic() - cycle_started_monotonic, 3),
        "ended_reason": result.ended_reason,
        "ticks_executed": result.ticks_executed,
        "claims_processed": result.claims_processed,
        "unique_flows_processed": result.unique_flows_processed,
        "runs_progressed": result.runs_progressed,
        "runs_failed_technically": result.runs_failed_technically,
        "ingestion_failures": result.ingestion_failures,
        "runs_stopped": result.runs_stopped,
        "follow_ups_created": result.follow_ups_created,
        "summary_paths": result.summary_paths.to_dict(),
        "last_error": None
        if result.ended_reason not in {"dispatch_failed", "ingestion_failed"}
        else {
            "code": "WORKER_CYCLE_FAILED",
            "message": "worker cycle ended with a failing terminal reason",
            "details": result.ended_reason,
        },
    }


def _build_worker_exception_summary(
    *,
    exc: WorkerLoopError,
    cycle_started_at: str,
    cycle_started_monotonic: float,
) -> dict[str, object]:
    return {
        "cycle_state": "error",
        "started_at": cycle_started_at,
        "completed_at": _utc_now(),
        "duration_seconds": round(time.monotonic() - cycle_started_monotonic, 3),
        "ended_reason": "exception",
        "ticks_executed": 0,
        "claims_processed": 0,
        "unique_flows_processed": 0,
        "runs_progressed": 0,
        "runs_failed_technically": 0,
        "ingestion_failures": 0,
        "runs_stopped": 0,
        "follow_ups_created": 0,
        "summary_paths": {
            "root_directory": None,
            "json_path": None,
            "markdown_path": None,
        },
        "last_error": exc.to_dict(),
    }


def _load_json_file(path: Path) -> Mapping[str, object] | dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_STATE_INVALID,
            "failed to read runtime supervisor JSON state",
            details=f"path={path} error={exc}",
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_STATE_INVALID,
            "runtime supervisor JSON state must be an object",
            details=f"path={path}",
        )
    return payload


def _write_json_file(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def _read_pid_file(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _pid_is_running(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _lock_is_held(path: Path) -> bool:
    _require_linux_runtime_supervisor()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    assert fcntl is not None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return True
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        return False


def _rotate_log_if_needed(path: Path) -> None:
    if not path.exists():
        return
    try:
        if path.stat().st_size <= EVENT_LOG_ROTATE_BYTES:
            return
    except OSError:
        return
    backup_path = path.with_suffix(path.suffix + ".1")
    if backup_path.exists():
        backup_path.unlink()
    path.replace(backup_path)


def _exception_payload(exc: Exception) -> dict[str, str | None]:
    code = getattr(exc, "code", exc.__class__.__name__)
    message = getattr(exc, "message", str(exc))
    details = getattr(exc, "details", None)
    return {
        "code": str(code),
        "message": str(message),
        "details": str(details) if details is not None else None,
    }


def _resolve_path(value: str | Path) -> Path:
    return Path(str(value)).expanduser().resolve()


def _resolve_optional_path(value: object) -> Path | None:
    normalized = _optional_string(value)
    if normalized is None:
        return None
    return _resolve_path(normalized)


def _resolve_optional_path_from_config(payload: Mapping[str, object], key: str) -> Path | None:
    return _resolve_optional_path(_nested_get(payload, "config", key))


def _nested_get(payload: Mapping[str, object], *keys: str) -> object:
    current: object = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _required_string(payload: Mapping[str, object], key: str) -> str:
    value = _optional_string(payload.get(key))
    if value is None:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_STATE_INVALID,
            f"runtime supervisor config is missing required field: {key}",
        )
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_tuple(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_STATE_INVALID,
            "runtime supervisor config expected a JSON array",
            details=f"actual_type={type(value).__name__}",
        )
    normalized = tuple(str(item) for item in value)
    return normalized or None


def _coerce_positive_float(value: object, *, field_name: str, default: float) -> float:
    if value is None:
        return default
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_CONFIG_INVALID,
            f"{field_name} must be a positive number",
            details=f"actual={value!r}",
        ) from exc
    if resolved <= 0:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_CONFIG_INVALID,
            f"{field_name} must be greater than zero",
            details=f"actual={resolved}",
        )
    return resolved


def _coerce_optional_positive_float(value: object, *, field_name: str, default: float | None) -> float | None:
    if value is None:
        return default
    return _coerce_positive_float(value, field_name=field_name, default=1.0)


def _coerce_positive_int(value: object, *, field_name: str, default: int) -> int:
    if value is None:
        return default
    try:
        resolved = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_CONFIG_INVALID,
            f"{field_name} must be a positive integer",
            details=f"actual={value!r}",
        ) from exc
    if resolved <= 0:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_CONFIG_INVALID,
            f"{field_name} must be greater than zero",
            details=f"actual={resolved}",
        )
    return resolved


def _coerce_optional_positive_int(value: object, *, field_name: str, default: int | None) -> int | None:
    if value is None:
        return default
    return _coerce_positive_int(value, field_name=field_name, default=1)


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after_seconds(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_linux_runtime_supervisor() -> None:
    if fcntl is None:
        raise RuntimeSupervisorError(
            RUNTIME_SUPERVISOR_LINUX_ONLY,
            "runtime supervisor v1 is supported only on Linux hosts",
        )
