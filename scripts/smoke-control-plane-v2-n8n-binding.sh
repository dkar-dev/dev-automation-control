#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

"$CONTROL_DIR/scripts/smoke-control-plane-v2-api.sh"

cat <<'EOF'
{
  "binding": "n8n-http-api-v1",
  "submit_fields": [
    "data.submitted_task.run_details.run.id",
    "data.submitted_task.run_details.run.flow_id",
    "data.submitted_task.run_details.run.queue_item.status"
  ],
  "worker_fields": [
    "data.worker_loop.ended_reason",
    "data.worker_loop.ticks_executed",
    "data.worker_loop.claims_processed",
    "data.worker_loop.tick_results[*].claimed_run_id"
  ],
  "manual_control_fields": [
    "data.control_state.run_status",
    "data.control_state.queue_status",
    "data.control_state.paused",
    "data.manual_control.operation"
  ],
  "boundary": [
    "n8n talks only to the localhost HTTP API",
    "n8n does not open SQLite directly",
    "n8n does not call the legacy 8787 bridge"
  ]
}
EOF
