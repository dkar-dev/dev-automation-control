#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <payload.json>" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$CONTROL_DIR/.." && pwd)"
PROJECTS_DIR="$WORKSPACE_DIR/projects"
RUNTIME_DIR="$WORKSPACE_DIR/runtime"
STATE_FILE="$CONTROL_DIR/state/current.json"
TASK_FILE="$CONTROL_DIR/inbox/current-task.md"
OUTBOX_DIR="$CONTROL_DIR/outbox"
PAYLOAD_PATH="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1")"

export CONTROL_DIR WORKSPACE_DIR PROJECTS_DIR RUNTIME_DIR STATE_FILE TASK_FILE OUTBOX_DIR PAYLOAD_PATH

RUN_ID="$(
python3 <<'PY'
import json
import os
from pathlib import Path
from datetime import datetime, timezone

payload_path = Path(os.environ["PAYLOAD_PATH"])
control_dir = Path(os.environ["CONTROL_DIR"])
workspace_dir = Path(os.environ["WORKSPACE_DIR"])
projects_dir = Path(os.environ["PROJECTS_DIR"])
runtime_dir = Path(os.environ["RUNTIME_DIR"])
state_file = Path(os.environ["STATE_FILE"])
task_file = Path(os.environ["TASK_FILE"])
outbox_dir = Path(os.environ["OUTBOX_DIR"])

if not payload_path.exists():
    raise SystemExit(f"Payload file not found: {payload_path}")

payload = json.loads(payload_path.read_text(encoding="utf-8"))

required = [
    "project",
    "task_text",
    "mode",
    "branch_base",
    "auto_commit",
    "source",
    "thread_label",
]
missing = [k for k in required if k not in payload]
if missing:
    raise SystemExit(f"Missing required keys: {', '.join(missing)}")

project = str(payload["project"]).strip()
task_text = str(payload["task_text"]).rstrip()
mode = str(payload["mode"]).strip()
branch_base = str(payload["branch_base"]).strip()
auto_commit = bool(payload["auto_commit"])
source = str(payload["source"]).strip()
thread_label = str(payload["thread_label"]).strip()

if mode not in {"executor-only", "executor+reviewer"}:
    raise SystemExit("mode must be 'executor-only' or 'executor+reviewer'")

project_dir = projects_dir / project
executor_worktree = runtime_dir / "worktrees" / f"{project}-executor"
reviewer_worktree = runtime_dir / "worktrees" / f"{project}-reviewer"

errors = []
if not project_dir.exists():
    errors.append(f"Project directory not found: {project_dir}")
if not executor_worktree.exists():
    errors.append(f"Executor worktree not found: {executor_worktree}")
if mode == "executor+reviewer" and not reviewer_worktree.exists():
    errors.append(f"Reviewer worktree not found: {reviewer_worktree}")
if errors:
    raise SystemExit("\n".join(errors))

now = datetime.now(timezone.utc)
now_iso = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
run_id = f"{now.strftime('%Y-%m-%dT%H-%M-%SZ')}_{project}"

run_dir = runtime_dir / "runs" / run_id
run_outbox_dir = run_dir / "outbox"
run_outbox_dir.mkdir(parents=True, exist_ok=False)

(run_dir / "input.json").write_text(
    json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)

state = {
    "run_id": run_id,
    "project": project,
    "mode": mode,
    "status": "queued",
    "branch_base": branch_base,
    "auto_commit": auto_commit,
    "source": source,
    "thread_label": thread_label,
    "paths": {
        "task_file": "inbox/current-task.md",
        "active_outbox_dir": "outbox",
        "run_dir": str(run_dir),
        "executor_worktree": str(executor_worktree),
        "reviewer_worktree": str(reviewer_worktree),
        "project_dir": str(project_dir),
    },
    "timestamps": {
        "created_at": now_iso,
        "updated_at": now_iso,
        "executor_started_at": None,
        "executor_finished_at": None,
        "reviewer_started_at": None,
        "reviewer_finished_at": None,
    },
    "result": {
        "verdict": None,
        "commit_sha": None,
        "summary": None,
        "error": None,
    },
}

constraints = payload.get("constraints") or [
    "Work only inside the requested project and automation workspace.",
    "Do not change unrelated files.",
]
expected_output = payload.get("expected_output") or [
    "Changed files in the target project or control repo.",
    "Short executor and reviewer reports in control/outbox.",
]

def to_md_list(value):
    if not isinstance(value, list):
        return "- " + str(value)
    return "\n".join(f"- {item}" for item in value)

task_md = f"""---
run_id: {run_id}
project: {project}
mode: {mode}
branch_base: {branch_base}
auto_commit: {"true" if auto_commit else "false"}
source: {source}
thread_label: {thread_label}
project_repo_path: {project_dir}
executor_worktree_path: {executor_worktree}
reviewer_worktree_path: {reviewer_worktree}
---

# Task

{task_text}

# Constraints

{to_md_list(constraints)}

# Expected output

{to_md_list(expected_output)}
"""

state_file.write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
(run_dir / "state.json").write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
task_file.write_text(task_md, encoding="utf-8")

outbox_dir.mkdir(parents=True, exist_ok=True)
for name in ["executor-last-message.md", "executor-report.md", "reviewer-report.md"]:
    p = outbox_dir / name
    if p.exists():
        p.unlink()
    p.write_text("", encoding="utf-8")

print(run_id)
PY
)"

echo "Prepared run: $RUN_ID"
