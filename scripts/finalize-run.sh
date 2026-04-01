#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <completed|failed> <approved|changes_requested|blocked|none> [summary...]" >&2
  exit 1
fi

FINAL_STATUS="$1"
VERDICT_ARG="$2"
shift 2
SUMMARY="${*:-}"

case "$FINAL_STATUS" in
  completed|failed) ;;
  *)
    echo "Invalid final status: $FINAL_STATUS" >&2
    exit 1
    ;;
esac

case "$VERDICT_ARG" in
  approved|changes_requested|blocked|none) ;;
  *)
    echo "Invalid verdict: $VERDICT_ARG" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STATE_FILE="$CONTROL_DIR/state/current.json"
OUTBOX_DIR="$CONTROL_DIR/outbox"

if [[ ! -f "$STATE_FILE" ]]; then
  echo "State file not found: $STATE_FILE" >&2
  exit 1
fi

"$SCRIPT_DIR/sync-outbox.sh" >/dev/null

export STATE_FILE OUTBOX_DIR FINAL_STATUS VERDICT_ARG SUMMARY

RUN_ID="$(
python3 <<'PY'
import json
import os
from pathlib import Path
from datetime import datetime, timezone

state_file = Path(os.environ["STATE_FILE"])
outbox_dir = Path(os.environ["OUTBOX_DIR"])
final_status = os.environ["FINAL_STATUS"]
verdict_arg = os.environ["VERDICT_ARG"]
summary = os.environ["SUMMARY"].strip()

state = json.loads(state_file.read_text(encoding="utf-8"))
run_id = state["run_id"]
run_dir = Path(state["paths"]["run_dir"])
now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

verdict = None if verdict_arg == "none" else verdict_arg
if not summary:
    summary = f"Run {final_status}" if verdict is None else f"Run {final_status}, verdict: {verdict}"

state["status"] = final_status
state["timestamps"]["updated_at"] = now_iso

if state["timestamps"]["executor_finished_at"] is None:
    state["timestamps"]["executor_finished_at"] = now_iso

if state["mode"] == "executor+reviewer" and verdict is not None and state["timestamps"]["reviewer_finished_at"] is None:
    state["timestamps"]["reviewer_finished_at"] = now_iso

state["result"]["verdict"] = verdict
state["result"]["summary"] = summary
state["result"]["error"] = None if final_status == "completed" else summary

result = {
    "run_id": run_id,
    "status": final_status,
    "verdict": verdict,
    "commit_sha": state["result"]["commit_sha"],
    "summary": summary,
    "artifacts": {
        "executor_last_message": str(run_dir / "outbox" / "executor-last-message.md"),
        "executor_report": str(run_dir / "outbox" / "executor-report.md"),
        "reviewer_report": str(run_dir / "outbox" / "reviewer-report.md"),
    },
}

state_file.write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
(run_dir / "state.json").write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
(run_dir / "result.json").write_text(
    json.dumps(result, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)

print(run_id)
PY
)"

echo "Finalized run: $RUN_ID"
