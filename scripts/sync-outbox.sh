#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STATE_FILE="$CONTROL_DIR/state/current.json"
OUTBOX_DIR="$CONTROL_DIR/outbox"

if [[ ! -f "$STATE_FILE" ]]; then
  echo "State file not found: $STATE_FILE" >&2
  exit 1
fi

export STATE_FILE OUTBOX_DIR

RUN_ID="$(
python3 <<'PY'
import json
import os
import shutil
from pathlib import Path

state_file = Path(os.environ["STATE_FILE"])
outbox_dir = Path(os.environ["OUTBOX_DIR"])

state = json.loads(state_file.read_text(encoding="utf-8"))
run_id = state["run_id"]
run_dir = Path(state["paths"]["run_dir"])
run_outbox_dir = run_dir / "outbox"
run_outbox_dir.mkdir(parents=True, exist_ok=True)

for name in ["executor-last-message.md", "executor-report.md", "reviewer-report.md"]:
    src = outbox_dir / name
    dst = run_outbox_dir / name
    if src.exists():
        shutil.copy2(src, dst)

(run_dir / "state.json").write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)

print(run_id)
PY
)"

echo "Synced outbox for run: $RUN_ID"
