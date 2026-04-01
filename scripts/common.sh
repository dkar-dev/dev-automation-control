#!/usr/bin/env bash
set -euo pipefail

CONTROL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TASK_FILE="$CONTROL_DIR/inbox/current-task.md"
STATE_FILE="$CONTROL_DIR/state/current.json"
OUTBOX_DIR="$CONTROL_DIR/outbox"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

task_get() {
  local key="$1"
  awk -v key="$key" '
    BEGIN { in_fm = 0 }
    /^---[[:space:]]*$/ {
      if (in_fm == 0) { in_fm = 1; next }
      else { exit }
    }
    in_fm == 1 {
      line = $0
      sub(/\r$/, "", line)
      split(line, a, ":")
      k = a[1]
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", k)
      if (k == key) {
        sub(/^[^:]*:[[:space:]]*/, "", line)
        gsub(/^"|"$/, "", line)
        print line
        exit
      }
    }
  ' "$TASK_FILE"
}

state_set() {
  local status="$1"
  local last_error="${2:-}"
  local next_action="${3:-}"

  python3 - "$STATE_FILE" "$status" "$last_error" "$next_action" <<'PY'
import json, sys, datetime
path, status, last_error, next_action = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
data["status"] = status
data["last_error"] = last_error
data["next_action"] = next_action
data["updated_at"] = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
    f.write("\n")
PY
}

prepare_run_dir() {
  local worktree="$1"
  local run_dir="$worktree/.codex-run"
  mkdir -p "$run_dir"
  printf '%s\n' "$run_dir"
}

ensure_file() {
  local path="$1"
  [ -f "$path" ] || {
    echo "missing file: $path" >&2
    exit 1
  }
}

ensure_dir() {
  local path="$1"
  [ -d "$path" ] || {
    echo "missing directory: $path" >&2
    exit 1
  }
}
