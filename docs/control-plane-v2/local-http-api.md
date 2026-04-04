# Control Plane v2 Local HTTP API

## Scope
- This is the thin v1 transport boundary for the existing Control Plane v2 primitives.
- It is intended for localhost-only use from `n8n`, local automations, and other single-machine triggers.
- It does not replace the existing CLI utilities; it routes requests into the same intake, worker, manual-control, and cleanup modules.

## Chosen server stack
- Python stdlib `http.server.ThreadingHTTPServer`
- No heavy framework
- JSON-only request and response contract

## Localhost-only assumption
- v1 binds only to `127.0.0.1` or `localhost`.
- No auth is implemented in this step.
- Do not expose this server on a public or shared network interface.
- Default bind is `127.0.0.1:8788`.

## Run the API

```bash
cd /home/dkar/workspace/control
./scripts/run-control-plane-api \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --host 127.0.0.1 \
  --port 8788
```

Optional default roots can be injected once at server startup:

```bash
cd /home/dkar/workspace/control
./scripts/run-control-plane-api \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --artifact-root /home/dkar/workspace/control-artifacts \
  --workspace-root /home/dkar/workspace \
  --worker-log-root /home/dkar/workspace/control-worker-logs
```

Inspect the effective config:

```bash
cd /home/dkar/workspace/control
./scripts/show-control-plane-config \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --json
```

CLI/env config sources:
- `--host` or `CONTROL_PLANE_API_HOST`
- `--port` or `CONTROL_PLANE_API_PORT`
- `--sqlite-db` or `CONTROL_PLANE_API_SQLITE_DB`
- `--artifact-root` or `CONTROL_PLANE_API_ARTIFACT_ROOT`
- `--workspace-root` or `CONTROL_PLANE_API_WORKSPACE_ROOT`
- `--worker-log-root` or `CONTROL_PLANE_API_WORKER_LOG_ROOT`

## Response envelope

Every response is JSON and uses this stable envelope:

```json
{
  "ok": true,
  "data": {},
  "error": null,
  "request_id": "..."
}
```

Error responses keep the same envelope and include structured provenance:

```json
{
  "ok": false,
  "data": null,
  "error": {
    "code": "INTAKE_RUNTIME_CONFIG_INVALID",
    "message": "...",
    "details": "...",
    "stage": "task_intake",
    "database_path": "/tmp/control-plane-v2.sqlite"
  },
  "request_id": "..."
}
```

Notes:
- existing domain error codes are preserved where possible
- malformed or non-JSON request bodies fail explicitly
- worker and cleanup endpoints return explicit result objects even when the result contains `dispatch_failed`, `ingestion_failed`, or per-target cleanup errors

## Endpoint list

`GET /v1/health`
- Liveness plus basic local config summary.

`POST /v1/tasks/submit`
- Submit one bounded task through the intake layer.
- Uses server default `artifact_root` and `workspace_root` when the request omits them.

`GET /v1/tasks`
- List submitted bounded tasks.
- Query params:
  - `project_key`
  - `limit`

`GET /v1/tasks/{run_id}`
- Show the persisted submission/runtime-context manifests for one submitted run.

`POST /v1/worker/tick`
- Run one worker tick.
- Request body may be `{}`.
- Optional runtime overrides mirror the existing worker runtime config fields.

`POST /v1/worker/run-until-idle`
- Run the bounded loop until idle or until one limit is hit.
- Optional fields:
  - `max_ticks`
  - `max_claims`
  - `max_flows`
  - `max_wall_clock_seconds`
  - worker runtime override fields

`POST /v1/runs/{run_id}/pause`
- Pause a queued or claimed-not-started run.
- Optional fields:
  - `note`
  - `operator`

`POST /v1/runs/{run_id}/resume`
- Resume a paused run.
- Optional fields:
  - `mode` = `normal|stabilize_to_green`
  - `note`
  - `operator`

`POST /v1/runs/{run_id}/force-stop`
- Force-stop a queued, claimed, paused, or active run without cleanup side effects.
- Optional fields:
  - `note`
  - `operator`

`POST /v1/runs/{run_id}/rerun-step`
- Narrow rerun request for a specific terminal `step_run`.
- Required body field:
  - `step_run_id`
- Optional fields:
  - `note`
  - `operator`
- The API rejects a `step_run_id` that does not belong to the `run_id` in the route.

`GET /v1/runs/{run_id}/control-state`
- Show the manual-control state for a run.

`POST /v1/cleanup/run-once`
- Run one cleanup pass.
- Optional fields:
  - `dry_run`
  - `now`
  - `scopes`

## Example payloads

Submit a bounded task:

```bash
curl -s http://127.0.0.1:8788/v1/tasks/submit \
  -H 'Content-Type: application/json' \
  -d '{
    "project_key": "sample-project",
    "task_text": "Implement the requested bounded task.",
    "project_profile": "default",
    "workflow_id": "build",
    "milestone": "http-api-v1",
    "instruction_overlays": ["strict-review"],
    "source": "n8n-http",
    "thread_label": "sample-project-http"
  }'
```

Run one worker tick:

```bash
curl -s http://127.0.0.1:8788/v1/worker/tick \
  -H 'Content-Type: application/json' \
  -d '{}'
```

Resume in stabilize mode:

```bash
curl -s http://127.0.0.1:8788/v1/runs/<run-id>/resume \
  -H 'Content-Type: application/json' \
  -d '{
    "mode": "stabilize_to_green",
    "note": "recover to green",
    "operator": "n8n"
  }'
```

Dry-run cleanup:

```bash
curl -s http://127.0.0.1:8788/v1/cleanup/run-once \
  -H 'Content-Type: application/json' \
  -d '{
    "dry_run": true
  }'
```

## n8n HTTP Request node guidance
- Method: `POST` or `GET` matching the endpoint
- URL: `http://127.0.0.1:8788/v1/...`
- Authentication: none in v1
- Send body as JSON
- Set `Content-Type: application/json`
- Treat `request_id` as the correlation key in logs and failure handling

Importable n8n templates for this API now live under:
- [`automation/n8n/workflows/`](/home/dkar/workspace/control/automation/n8n/workflows)

Supporting n8n docs now live under:
- [`docs/n8n/README.md`](/home/dkar/workspace/control/docs/n8n/README.md)
- [`docs/n8n/configuration.md`](/home/dkar/workspace/control/docs/n8n/configuration.md)
- [`docs/n8n/example-payloads.md`](/home/dkar/workspace/control/docs/n8n/example-payloads.md)
- [`docs/n8n/smoke.md`](/home/dkar/workspace/control/docs/n8n/smoke.md)

Boundary for the n8n package:
- `n8n` calls only the HTTP API on `8788`
- `n8n` does not open SQLite directly
- `n8n` does not call the legacy bridge on `8787`
- `Code` nodes are limited to request/response shaping

Recommended first v1 flow for `n8n`:
1. `POST /v1/tasks/submit`
2. `POST /v1/worker/tick` or `POST /v1/worker/run-until-idle`
3. `GET /v1/tasks/{run_id}` or `GET /v1/runs/{run_id}/control-state` for operator inspection

## Smoke check

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-api.sh
```

Focused n8n-binding wrapper:

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-n8n-binding.sh
```

This smoke verifies:
- local server startup
- `GET /v1/health`
- malformed JSON failure
- task submit/list/show through HTTP
- worker tick and run-until-idle through HTTP
- pause/resume/force-stop through HTTP
- cleanup dry-run through HTTP

## Out of scope in v1
- public deployment hardening
- auth, tenancy, or roles
- websocket or streaming transport
- distributed coordination
- remote worker execution protocol
- replacing the legacy bridge on `8787`
- direct SQLite access from `n8n`
- calling host-side legacy scripts from inside `n8n`
