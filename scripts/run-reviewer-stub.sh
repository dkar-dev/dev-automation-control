#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CONTROL_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
OUTBOX_DIR="$CONTROL_DIR/outbox"

mkdir -p "$OUTBOX_DIR"

cat > "$OUTBOX_DIR/reviewer-report.md" <<'EOF'
# Reviewer Report

## Verdict
approved

## Summary
Local reviewer script ran from n8n Execute Command node.
EOF