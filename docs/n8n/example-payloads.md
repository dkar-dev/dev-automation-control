# Example Payloads

These are the HTTP payloads the n8n templates send to the Control Plane API.

## Submit body

Endpoint:
- `POST /v1/tasks/submit`

Example:

```json
{
  "project_key": "sample-project",
  "task_text": "Implement the requested bounded task through the control plane HTTP API.",
  "project_profile": "default",
  "workflow_id": "build",
  "milestone": "n8n-binding-v1",
  "priority_class": "interactive",
  "instruction_profile": "default",
  "instruction_overlays": [
    "strict-review"
  ],
  "source": "n8n-control-plane-v2",
  "thread_label": "sample-project-n8n",
  "constraints": [
    "Only use the control plane HTTP API boundary."
  ],
  "expected_output": [
    "Return run_id, flow_id, and queue status."
  ]
}
```

Expected response fields most useful in n8n:

```json
{
  "ok": true,
  "request_id": "req_...",
  "data": {
    "submitted_task": {
      "submitted_at": "2026-04-04T10:00:00Z",
      "run_details": {
        "run": {
          "id": "run_...",
          "flow_id": "flow_...",
          "status": "queued",
          "queue_item": {
            "id": "queue_...",
            "status": "queued"
          }
        }
      }
    }
  }
}
```

Flattened output from the provided workflow:

```json
{
  "run_id": "run_...",
  "flow_id": "flow_...",
  "queue_status": "queued",
  "run_status": "queued",
  "request_id": "req_..."
}
```

## Worker body

Endpoint:
- `POST /v1/worker/run-until-idle`

Example:

```json
{
  "max_ticks": 25,
  "max_claims": 5,
  "max_flows": 5,
  "max_wall_clock_seconds": 60
}
```

Minimal example:

```json
{
  "max_ticks": 25
}
```

Expected response fields most useful in n8n:

```json
{
  "ok": true,
  "request_id": "req_...",
  "data": {
    "worker_loop": {
      "ticks_executed": 2,
      "claims_processed": 1,
      "unique_flows_processed": 1,
      "runs_progressed": 1,
      "runs_failed_technically": 0,
      "ingestion_failures": 0,
      "runs_stopped": 0,
      "follow_ups_created": 0,
      "ended_reason": "idle"
    }
  }
}
```

Flattened output from the provided workflow:

```json
{
  "ended_reason": "idle",
  "ticks_executed": 2,
  "claims_processed": 1,
  "processed_run_ids": [
    "run_..."
  ],
  "last_tick_status": "progressed",
  "last_tick_final_run_status": "completed",
  "request_id": "req_..."
}
```

## Manual control bodies

`GET /v1/runs/{run_id}/control-state`
- no JSON body

`POST /v1/runs/{run_id}/pause`

```json
{
  "note": "Pause from n8n",
  "operator": "n8n"
}
```

`POST /v1/runs/{run_id}/resume`

```json
{
  "mode": "normal",
  "note": "Resume from n8n",
  "operator": "n8n"
}
```

Resume with stabilization mode:

```json
{
  "mode": "stabilize_to_green",
  "note": "Recover to green",
  "operator": "n8n"
}
```

`POST /v1/runs/{run_id}/force-stop`

```json
{
  "note": "Force-stop from n8n",
  "operator": "n8n"
}
```

Expected `control-state` response fragment:

```json
{
  "ok": true,
  "request_id": "req_...",
  "data": {
    "control_state": {
      "run_id": "run_...",
      "run_status": "paused",
      "queue_status": "paused",
      "paused": true,
      "terminal": false,
      "latest_manual_transition_type": "manual_queue_paused",
      "latest_resume_mode": null
    }
  }
}
```

Expected pause/resume/force-stop response fragment:

```json
{
  "ok": true,
  "request_id": "req_...",
  "data": {
    "manual_control": {
      "operation": "pause",
      "run": {
        "run": {
          "id": "run_...",
          "status": "paused"
        }
      },
      "control_state": {
        "run_id": "run_...",
        "queue_status": "paused",
        "paused": true,
        "terminal": false
      }
    }
  }
}
```

## Boundary reminder

These payloads go only to the Control Plane HTTP API.

They do not:
- target SQLite directly
- call legacy bridge endpoints on `8787`
- invoke host scripts from inside n8n

For legacy bridge command mapping, see [`docs/control-plane-v2/orchestration-cutover.md`](/home/dkar/workspace/control/docs/control-plane-v2/orchestration-cutover.md).
