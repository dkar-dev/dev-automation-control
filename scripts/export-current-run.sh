#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STATE_FILE="$CONTROL_DIR/state/current.json"

if [[ ! -f "$STATE_FILE" ]]; then
  echo "State file not found: $STATE_FILE" >&2
  exit 1
fi

export STATE_FILE

python3 <<'PY'
import json
import os
from pathlib import Path

state_file = Path(os.environ["STATE_FILE"])
state = json.loads(state_file.read_text(encoding="utf-8"))
run_dir = Path(state["paths"]["run_dir"])
run_outbox_dir = run_dir / "outbox"

def read_text(path: Path):
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    return text if text else None

payload = {
    "run_id": state["run_id"],
    "project": state["project"],
    "mode": state["mode"],
    "status": state["status"],
    "branch_base": state["branch_base"],
    "auto_commit": state["auto_commit"],
    "source": state["source"],
    "thread_label": state["thread_label"],
    "instruction_profile": state.get("instruction_profile"),
    "instruction_overlays": state.get("instruction_overlays") or [],
    "instructions_repo_path": state.get("instructions_repo_path"),
    "instructions_revision": state.get("instructions_revision"),
    "resolved_instruction_files": state.get("resolved_instruction_files") or [],
    "timestamps": state["timestamps"],
    "result": state["result"],
    "paths": state["paths"],
    "outbox": {
        "executor_last_message": read_text(run_outbox_dir / "executor-last-message.md"),
        "executor_report": read_text(run_outbox_dir / "executor-report.md"),
        "reviewer_report": read_text(run_outbox_dir / "reviewer-report.md"),
    },
}

print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
