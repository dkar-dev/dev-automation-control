#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <executor|reviewer>" >&2
  exit 1
fi

ROLE="$1"
case "$ROLE" in
  executor|reviewer) ;;
  *)
    echo "Role must be executor or reviewer" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STATE_FILE="$CONTROL_DIR/state/current.json"

if [[ ! -f "$STATE_FILE" ]]; then
  echo "State file not found: $STATE_FILE" >&2
  exit 1
fi

export STATE_FILE ROLE

RUN_ID="$(
python3 <<'PY'
import json
import os
from pathlib import Path
from datetime import datetime, timezone

state_file = Path(os.environ["STATE_FILE"])
role = os.environ["ROLE"]

state = json.loads(state_file.read_text(encoding="utf-8"))
status = state.get("status")

allowed = False
if role == "executor":
    allowed = status == "queued"
elif role == "reviewer":
    allowed = status == "executor_done"

if not allowed:
    raise SystemExit(f"Cannot mark {role} running from status: {status}")

now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
state["status"] = f"{role}_running"
state["timestamps"]["updated_at"] = now_iso

if role == "executor":
    if state["timestamps"]["executor_started_at"] is None:
        state["timestamps"]["executor_started_at"] = now_iso
elif role == "reviewer":
    if state["timestamps"]["reviewer_started_at"] is None:
        state["timestamps"]["reviewer_started_at"] = now_iso

run_dir = Path(state["paths"]["run_dir"])
state_file.write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
(run_dir / "state.json").write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)

print(state["run_id"])
PY
)"

echo "Marked running: $RUN_ID ($ROLE)"
