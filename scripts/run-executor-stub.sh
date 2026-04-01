#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CONTROL_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
OUTBOX_DIR="$CONTROL_DIR/outbox"

mkdir -p "$OUTBOX_DIR"

cat > "$OUTBOX_DIR/executor-last-message.md" <<'EOF'
Executor finished local stub pass.
EOF

cat > "$OUTBOX_DIR/executor-report.md" <<'EOF'
# Executor Report

## Status
success

## Summary
Local executor script ran from n8n Execute Command node.

## Changed files
- none
EOF