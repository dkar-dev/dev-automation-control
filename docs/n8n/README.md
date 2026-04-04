# n8n HTTP Binding v1

This is the v1 n8n integration package for Control Plane v2.

Purpose:
- let `n8n` act as an orchestration client over the localhost HTTP API
- keep task intake, worker execution, manual control, and cleanup inside the control plane
- avoid the legacy bridge on `127.0.0.1:8787`

## Package contents

Workflow templates:
- [`automation/n8n/workflows/control-plane-v2-submit-bounded-task-v1.json`](/home/dkar/workspace/control/automation/n8n/workflows/control-plane-v2-submit-bounded-task-v1.json)
- [`automation/n8n/workflows/control-plane-v2-run-worker-until-idle-v1.json`](/home/dkar/workspace/control/automation/n8n/workflows/control-plane-v2-run-worker-until-idle-v1.json)
- [`automation/n8n/workflows/control-plane-v2-manual-control-v1.json`](/home/dkar/workspace/control/automation/n8n/workflows/control-plane-v2-manual-control-v1.json)

Supporting docs:
- [`docs/n8n/configuration.md`](/home/dkar/workspace/control/docs/n8n/configuration.md)
- [`docs/n8n/example-payloads.md`](/home/dkar/workspace/control/docs/n8n/example-payloads.md)
- [`docs/n8n/smoke.md`](/home/dkar/workspace/control/docs/n8n/smoke.md)

## Workflows included

`Control Plane v2 - Submit Bounded Task v1`
- Uses `Manual Trigger`, `Edit Fields`, `Code`, and `HTTP Request`.
- Calls `POST /v1/tasks/submit`.
- Returns flattened `run_id`, `flow_id`, `queue_status`, `run_status`, and `request_id`.

`Control Plane v2 - Run Worker Until Idle v1`
- Uses `Manual Trigger`, `Edit Fields`, `Code`, and `HTTP Request`.
- Calls `POST /v1/worker/run-until-idle`.
- Supports `max_ticks`, `max_claims`, `max_flows`, and `max_wall_clock_seconds`.
- Returns a machine-friendly worker summary plus the raw API envelope.

`Control Plane v2 - Manual Control v1`
- Uses `Manual Trigger`, `Edit Fields`, `Code`, `If`, and separate `HTTP Request` nodes.
- Supports `show_control_state`, `pause`, `resume`, and `force_stop`.
- Calls distinct HTTP paths for each action.

## Import into n8n

1. Start the Control Plane API.
2. Open n8n.
3. Import one or more workflow JSON files from [`automation/n8n/workflows/`](/home/dkar/workspace/control/automation/n8n/workflows).
4. In each imported workflow, adjust the `Edit Fields` defaults if your base URL or operator label differs.
5. Execute the workflow manually or replace `Manual Trigger` with your own trigger node.

## Boundary

These templates intentionally keep `n8n` thin:
- `n8n` does not open SQLite.
- `n8n` does not call `scripts/*` directly.
- `n8n` does not call the legacy bridge on `127.0.0.1:8787`.
- `Code` nodes only shape request bodies and flatten API responses.
- Run lifecycle decisions remain in the Control Plane HTTP API and worker loop.
- There is no bidirectional sync between n8n execution state and Control Plane state.

## Compatibility note

The existing legacy workflow export at [`n8n/workflows/control-bridge-run-v1.json`](/home/dkar/workspace/control/n8n/workflows/control-bridge-run-v1.json) is left in place for compatibility notes only.

This v1 package is the forward path for Control Plane v2 over the localhost HTTP API.

## Out of scope

- production-grade authentication
- direct database access from `n8n`
- legacy bridge replacement work on port `8787`
- orchestration logic duplicated inside `Code` nodes
- synchronization of Control Plane state into native n8n state machines
