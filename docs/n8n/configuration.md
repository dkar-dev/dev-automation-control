# n8n Configuration

## Control Plane API env vars

The API server is configured on the control-plane side, not in `n8n`.

Required:
- `CONTROL_PLANE_API_SQLITE_DB`

Optional:
- `CONTROL_PLANE_API_HOST`
- `CONTROL_PLANE_API_PORT`
- `CONTROL_PLANE_API_ARTIFACT_ROOT`
- `CONTROL_PLANE_API_WORKSPACE_ROOT`
- `CONTROL_PLANE_API_WORKER_LOG_ROOT`

Equivalent CLI flags exist on [`scripts/run-control-plane-api`](/home/dkar/workspace/control/scripts/run-control-plane-api).

## n8n-side env vars

No special credentials are required in v1.

Recommended optional env vars for workflow defaults:
- `N8N_CONTROL_PLANE_BASE_URL`
- `N8N_CONTROL_PLANE_OPERATOR`

The provided workflow templates already default `base_url` and `operator` from those env vars when available.

## Base URL selection

If `n8n` runs in Docker:
- use `http://host.docker.internal:8788`

If `n8n` runs directly on the same host:
- use `http://127.0.0.1:8788`

If you override the API port, update the base URL in the workflow `Edit Fields` node or via `N8N_CONTROL_PLANE_BASE_URL`.

## Docker vs host execution

Docker container to host API:
- `n8n` container reaches the host-side API through `host.docker.internal`
- example: `http://host.docker.internal:8788`

Host process to host API:
- use loopback directly
- example: `http://127.0.0.1:8788`

The API itself still binds only to `127.0.0.1` or `localhost`.

## Credentials

v1 credentials requirement:
- none

Use the `HTTP Request` node with:
- authentication: `none`
- `Content-Type: application/json`
- JSON body enabled for `POST` endpoints

Do not add:
- database credentials
- SSH credentials for host scripts
- bridge credentials for `8787`

## Workflow-specific defaults

`Submit Bounded Task`
- set `project_key`, `task_text`, `workflow_id`, `milestone`, and optional overlays in `Edit Fields`

`Run Worker Until Idle`
- set `max_ticks`, `max_claims`, `max_flows`, and `max_wall_clock_seconds` in `Edit Fields`

`Manual Control`
- set `run_id`
- set `action` to one of:
  - `show_control_state`
  - `pause`
  - `resume`
  - `force_stop`
- set `resume_mode` only when `action=resume`
