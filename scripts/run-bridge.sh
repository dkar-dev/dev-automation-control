#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

HOST="${BRIDGE_HOST:-127.0.0.1}"
PORT="${BRIDGE_PORT:-8787}"

exec python3 "$CONTROL_DIR/bridge/http_bridge.py" --host "$HOST" --port "$PORT"
