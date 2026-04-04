# Smoke Guide

This integration does not require a dedicated n8n test harness.

The authoritative API-boundary smoke is:
- [`scripts/smoke-control-plane-v2-api.sh`](/home/dkar/workspace/control/scripts/smoke-control-plane-v2-api.sh)

The n8n-focused wrapper is:
- [`scripts/smoke-control-plane-v2-n8n-binding.sh`](/home/dkar/workspace/control/scripts/smoke-control-plane-v2-n8n-binding.sh)

## What the smoke validates

- API starts on localhost
- `POST /v1/tasks/submit` accepts bounded-task input
- `POST /v1/worker/run-until-idle` completes through the worker loop
- `GET /v1/runs/{run_id}/control-state` returns inspectable manual-control state
- `POST /v1/runs/{run_id}/pause`
- `POST /v1/runs/{run_id}/resume`
- `POST /v1/runs/{run_id}/force-stop`

That is the same surface consumed by the n8n v1 workflow package.

## Run the smoke

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-n8n-binding.sh
```

The wrapper delegates the heavy lifting to the existing API smoke and then prints the canonical n8n field mappings.

## Manual curl smoke

Start the API first:

```bash
cd /home/dkar/workspace/control
./scripts/run-control-plane-api \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --host 127.0.0.1 \
  --port 8788
```

Submit:

```bash
curl -s http://127.0.0.1:8788/v1/tasks/submit \
  -H 'Content-Type: application/json' \
  -d '{
    "project_key": "sample-project",
    "task_text": "Drive the run through the localhost API boundary.",
    "project_profile": "default",
    "workflow_id": "build",
    "milestone": "n8n-smoke"
  }'
```

Run worker until idle:

```bash
curl -s http://127.0.0.1:8788/v1/worker/run-until-idle \
  -H 'Content-Type: application/json' \
  -d '{
    "max_ticks": 10,
    "max_wall_clock_seconds": 60
  }'
```

Inspect control state:

```bash
curl -s http://127.0.0.1:8788/v1/runs/<run-id>/control-state
```

## Expected n8n-relevant fields

Submit:
- `data.submitted_task.run_details.run.id`
- `data.submitted_task.run_details.run.flow_id`
- `data.submitted_task.run_details.run.queue_item.status`

Worker:
- `data.worker_loop.ended_reason`
- `data.worker_loop.ticks_executed`
- `data.worker_loop.claims_processed`
- `data.worker_loop.tick_results[*].claimed_run_id`

Manual control:
- `data.control_state.run_status`
- `data.control_state.queue_status`
- `data.control_state.paused`
- `data.manual_control.operation`

## Boundary reminder

For this smoke, and for the n8n package itself:
- use only the HTTP API
- do not access SQLite directly
- do not call the legacy bridge
- do not reimplement worker or run-state logic in `Code` nodes
- treat the legacy bridge workflow export as compatibility-only
