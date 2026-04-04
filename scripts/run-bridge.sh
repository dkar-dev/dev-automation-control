#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

HOST="${BRIDGE_HOST:-127.0.0.1}"
PORT="${BRIDGE_PORT:-8787}"

if [[ "${CONTROL_BRIDGE_SUPPRESS_DEPRECATION_WARNING:-0}" != "1" ]]; then
  cat >&2 <<EOF
warning: legacy orchestration bridge transport on ${HOST}:${PORT} is deprecated.
use ./scripts/run-control-plane-api on 127.0.0.1:8788 for submit, contract generation, worker, manual control, and cleanup flows.
the legacy bridge remains only for compatibility/debugging and for legacy backend/runtime support.
EOF
fi

exec python3 "$CONTROL_DIR/bridge/http_bridge.py" --host "$HOST" --port "$PORT"
