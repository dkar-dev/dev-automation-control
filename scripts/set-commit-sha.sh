#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <commit_sha>" >&2
  exit 1
fi

COMMIT_SHA="$1"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STATE_FILE="$CONTROL_DIR/state/current.json"

if [[ ! -f "$STATE_FILE" ]]; then
  echo "State file not found: $STATE_FILE" >&2
  exit 1
fi

export STATE_FILE COMMIT_SHA

RUN_ID="$(
python3 <<'PY'
import json
import os
from pathlib import Path
from datetime import datetime, timezone

state_file = Path(os.environ["STATE_FILE"])
commit_sha = os.environ["COMMIT_SHA"].strip()

if not commit_sha:
    raise SystemExit("commit_sha must not be empty")

state = json.loads(state_file.read_text(encoding="utf-8"))
state["result"]["commit_sha"] = commit_sha
state["timestamps"]["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

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

echo "Saved commit sha for run: $RUN_ID"
