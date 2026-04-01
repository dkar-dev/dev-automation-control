#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


CONTROL_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = CONTROL_DIR / "scripts"
OUTBOX_DIR = CONTROL_DIR / "outbox"


class BridgeError(Exception):
    def __init__(self, status: int, message: str, details: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.details = details


def format_process_details(proc: subprocess.CompletedProcess[str]) -> str | None:
    parts: list[str] = []

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()

    if stdout:
        parts.append(f"stdout:\n{stdout}")
    if stderr:
        parts.append(f"stderr:\n{stderr}")

    return "\n\n".join(parts) or None


def run_script(*args: str) -> str:
    command = [str(a) for a in args]
    script_path = Path(command[0])

    if not script_path.exists():
        raise BridgeError(500, f"Missing script: {script_path.name}")

    try:
        proc = subprocess.run(
            command,
            cwd=str(CONTROL_DIR),
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        raise BridgeError(500, f"Failed to execute script: {script_path.name}", str(exc)) from exc

    if proc.returncode != 0:
        raise BridgeError(
            500,
            f"Script execution failed: {script_path.name} exited with code {proc.returncode}",
            format_process_details(proc),
        )
    return proc.stdout.strip()


def export_current_run() -> dict:
    script = SCRIPTS_DIR / "export-current-run.sh"
    if not script.exists():
        raise BridgeError(500, "Missing export-current-run.sh")
    try:
        raw = run_script(script)
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BridgeError(500, f"Invalid JSON from export-current-run.sh: {exc}") from exc


def write_outbox_file(filename: str, content: str) -> None:
    OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
    (OUTBOX_DIR / filename).write_text(content, encoding="utf-8")


class Handler(BaseHTTPRequestHandler):
    server_version = "control-bridge/0.1"

    def log_message(self, format: str, *args) -> None:
        return

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise BridgeError(400, f"Invalid JSON body: {exc}") from exc
        if not isinstance(data, dict):
            raise BridgeError(400, "JSON body must be an object")
        return data

    def _handle(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if self.command == "GET" and path == "/healthz":
            self._send_json(200, {"ok": True})
            return

        if self.command == "GET" and path == "/current-run":
            try:
                payload = export_current_run()
            except BridgeError as exc:
                self._send_json(exc.status, {"ok": False, "error": exc.message, "details": exc.details})
                return
            self._send_json(200, {"ok": True, "data": payload})
            return

        if self.command != "POST":
            self._send_json(405, {"ok": False, "error": "Method not allowed"})
            return

        try:
            body = self._read_json()

            if path == "/prepare-run":
                with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
                    json.dump(body, tmp, ensure_ascii=False, indent=2)
                    tmp_path = Path(tmp.name)

                try:
                    run_script(SCRIPTS_DIR / "prepare-run.sh", str(tmp_path))
                finally:
                    tmp_path.unlink(missing_ok=True)

                self._send_json(200, {"ok": True, "data": export_current_run()})
                return

            if path == "/mark-running":
                role = body.get("role")
                if role not in {"executor", "reviewer"}:
                    raise BridgeError(400, "role must be executor or reviewer")

                run_script(SCRIPTS_DIR / "mark-running.sh", role)
                self._send_json(200, {"ok": True, "data": export_current_run()})
                return

            if path == "/run-executor":
                run_script(SCRIPTS_DIR / "run-executor.sh")
                self._send_json(200, {"ok": True, "data": export_current_run()})
                return

            if path == "/run-reviewer":
                run_script(SCRIPTS_DIR / "run-reviewer.sh")
                self._send_json(200, {"ok": True, "data": export_current_run()})
                return

            if path == "/outbox/update":
                mapping = {
                    "executor_last_message": "executor-last-message.md",
                    "executor_report": "executor-report.md",
                    "reviewer_report": "reviewer-report.md",
                }

                touched = False
                for key, filename in mapping.items():
                    if key in body:
                        value = body[key]
                        if not isinstance(value, str):
                            raise BridgeError(400, f"{key} must be a string")
                        write_outbox_file(filename, value)
                        touched = True

                if not touched:
                    raise BridgeError(400, "No outbox fields provided")

                run_script(SCRIPTS_DIR / "sync-outbox.sh")
                self._send_json(200, {"ok": True, "data": export_current_run()})
                return

            if path == "/sync-outbox":
                run_script(SCRIPTS_DIR / "sync-outbox.sh")
                self._send_json(200, {"ok": True, "data": export_current_run()})
                return

            if path == "/set-commit-sha":
                commit_sha = body.get("commit_sha")
                if not isinstance(commit_sha, str) or not commit_sha.strip():
                    raise BridgeError(400, "commit_sha must be a non-empty string")

                run_script(SCRIPTS_DIR / "set-commit-sha.sh", commit_sha.strip())
                self._send_json(200, {"ok": True, "data": export_current_run()})
                return

            if path == "/finalize-run":
                status = body.get("status", "completed")
                verdict = body.get("verdict")
                summary = body.get("summary", "")
                error = body.get("error")

                if not isinstance(status, str) or not status.strip():
                    raise BridgeError(400, "status must be a non-empty string")
                if not isinstance(verdict, str) or not verdict.strip():
                    raise BridgeError(400, "verdict must be a non-empty string")
                if not isinstance(summary, str):
                    raise BridgeError(400, "summary must be a string")
                if error is not None and not isinstance(error, str):
                    raise BridgeError(400, "error must be a string or null")

                args = [
                    str(SCRIPTS_DIR / "finalize-run.sh"),
                    status.strip(),
                    verdict.strip(),
                    summary,
                ]
                if error is not None:
                    args.append(error)

                run_script(*args)
                self._send_json(200, {"ok": True, "data": export_current_run()})
                return

            self._send_json(404, {"ok": False, "error": f"Unknown endpoint: {path}"})

        except BridgeError as exc:
            self._send_json(exc.status, {"ok": False, "error": exc.message, "details": exc.details})
        except Exception as exc:
            self._send_json(500, {"ok": False, "error": f"Unhandled error: {exc}"})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Bridge listening on http://{args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
