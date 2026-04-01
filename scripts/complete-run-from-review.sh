#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STATE_FILE="$CONTROL_DIR/state/current.json"
REPORT_FILE="$CONTROL_DIR/outbox/reviewer-report.md"

if [[ ! -f "$STATE_FILE" ]]; then
  echo "State file not found: $STATE_FILE" >&2
  exit 1
fi

if [[ ! -f "$REPORT_FILE" ]]; then
  echo "Reviewer report not found: $REPORT_FILE" >&2
  exit 1
fi

mapfile -t REVIEW_META < <(
  python3 - "$STATE_FILE" "$REPORT_FILE" <<'PY'
import json
import re
import sys
from pathlib import Path

state_path = Path(sys.argv[1])
report_path = Path(sys.argv[2])

state = json.loads(state_path.read_text(encoding="utf-8"))
text = report_path.read_text(encoding="utf-8").lstrip("\ufeff")
lines = text.splitlines()

if len(lines) < 3:
    raise SystemExit(
        "Reviewer report must start with Verdict, Summary, and Commit SHA lines"
    )

def parse_line(pattern: str, value: str, error: str) -> re.Match[str]:
    match = re.fullmatch(pattern, value.strip())
    if not match:
        raise SystemExit(error)
    return match

verdict = parse_line(
    r"Verdict:\s*(approved|changes_requested|blocked)\s*",
    lines[0],
    "Missing required reviewer field on line 1: Verdict: approved|changes_requested|blocked",
).group(1).lower()

summary = parse_line(
    r"Summary:\s*(.+\S)\s*",
    lines[1],
    "Missing required reviewer field on line 2: Summary: <one-line summary>",
).group(1).strip()

commit_sha = parse_line(
    r"Commit SHA:\s*(\S+)\s*",
    lines[2],
    "Missing required reviewer field on line 3: Commit SHA: <sha or none>",
).group(1).strip()

if commit_sha.lower() != "none" and not re.fullmatch(r"[0-9a-fA-F]{7,40}", commit_sha):
    raise SystemExit("Commit SHA must be a git sha or 'none'")

print(state["run_id"])
print(verdict)
print(summary)
print(commit_sha.lower() if commit_sha.lower() == "none" else commit_sha)
PY
)

if [[ "${#REVIEW_META[@]}" -ne 4 ]]; then
  echo "Failed to parse reviewer completion metadata" >&2
  exit 1
fi

RUN_ID="${REVIEW_META[0]}"
VERDICT="${REVIEW_META[1]}"
SUMMARY="${REVIEW_META[2]}"
COMMIT_SHA="${REVIEW_META[3]}"

if [[ "$COMMIT_SHA" != "none" ]]; then
  "$SCRIPT_DIR/set-commit-sha.sh" "$COMMIT_SHA" >/dev/null
fi

case "$VERDICT" in
  approved)
    "$SCRIPT_DIR/finalize-run.sh" completed approved "$SUMMARY" >/dev/null
    ;;
  changes_requested)
    "$SCRIPT_DIR/finalize-run.sh" failed changes_requested "$SUMMARY" >/dev/null
    ;;
  blocked)
    "$SCRIPT_DIR/finalize-run.sh" failed blocked "$SUMMARY" >/dev/null
    ;;
  *)
    echo "Unsupported reviewer verdict: $VERDICT" >&2
    exit 1
    ;;
esac

echo "Completed run from reviewer report: $RUN_ID ($VERDICT)"
