# Control Plane v1 Orchestration Cutover

## Goal
- Move the preferred orchestration boundary from the legacy local bridge on `127.0.0.1:8787` to the Control Plane HTTP API on `127.0.0.1:8788`.
- Keep legacy executor/reviewer runner scripts only as backend execution implementations behind the dispatch adapter and worker loop.
- Avoid a destructive rewrite or a second full compatibility bridge.

## Preferred Path Now
- `POST /v1/tasks/submit` or `./scripts/submit-bounded-task`
- `POST /v1/contracts/generate` or `./scripts/generate-bounded-contract`
- `POST /v1/worker/tick` or `POST /v1/worker/run-until-idle`
- `GET /v1/runs/{run_id}/control-state` plus `POST /v1/runs/{run_id}/{pause|resume|force-stop|rerun-step}`
- `POST /v1/cleanup/run-once`

Run the local API with:

```bash
cd /home/dkar/workspace/control
./scripts/run-control-plane-api \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --host 127.0.0.1 \
  --port 8788
```

## Boundary Split

Allowed legacy backend usage:
- `scripts/run-executor.sh`
- `scripts/run-reviewer.sh`
- prompt assembly and instruction resolution helpers
- commit handoff logic
- reviewer ingestion persistence path

Deprecated legacy transport/control-plane usage:
- `scripts/run-bridge.sh`
- `bridge/http_bridge.py` as the preferred orchestration entry path
- `n8n` workflows that call `/prepare-run`, `/run-executor`, `/run-reviewer`, or `/finalize-run` on `8787`

## Old To New Mapping

| Legacy bridge usage | Preferred replacement | Notes |
| --- | --- | --- |
| `POST /prepare-run` | `POST /v1/tasks/submit` or `./scripts/submit-bounded-task` | New path persists normalized submission and runtime-context manifests. |
| `POST /run-executor` + `POST /run-reviewer` | `POST /v1/worker/tick`, `POST /v1/worker/run-until-idle`, or manual `dispatch-*` CLI | Worker/dispatch still reuse legacy backend runner scripts underneath. |
| `GET /current-run` | `GET /v1/tasks/{run_id}`, `GET /v1/runs/{run_id}/control-state`, `./scripts/show-run`, `./scripts/show-submitted-task` | There is no single preferred mutable bridge snapshot anymore. |
| `POST /finalize-run` | reviewer ingestion via `./scripts/ingest-reviewer-result` or the worker path | Reviewer semantic completion is explicit in v2. |
| legacy bridge-driven `n8n` workflow | `automation/n8n/workflows/control-plane-v2-*.json` | Base URL must point to `8788`, not `8787`. |
| bridge-only orchestration state checks | HTTP API health/manual-control/contracts endpoints | Use the endpoint that matches the operator task instead of a monolithic bridge state view. |

## What Stays The Same
- Real executor/reviewer work still runs on the host side.
- The dispatch adapter still shells out to the legacy runner scripts.
- Existing CLI flows remain available.
- Artifact, worktree, and prompt-building behavior stays in the same backend layer.
- Cleanup, manual control, and worker semantics stay inside the control plane.

## What Changes
- `n8n` and local automation should target `127.0.0.1:8788`.
- Operator docs should use submit, contract generation, worker, manual control, and cleanup entry points from the HTTP API or matching CLI commands.
- The legacy bridge is no longer the primary transport/control-plane surface.
- Bounded contract generation is part of the preferred orchestration path instead of an optional side flow.

## Intentionally Not Migrated
- No new compatibility bridge on a second port.
- No 1:1 HTTP alias layer for every legacy bridge endpoint.
- No direct `n8n` access to SQLite or host scripts.
- No public deployment/auth work.
- No dispatch backend rewrite.

## Cutover Checklist
- Start `./scripts/run-control-plane-api`
- Submit work through `/v1/tasks/submit`
- Generate a bounded contract through `/v1/contracts/generate`
- Progress execution through `/v1/worker/tick` or `/v1/worker/run-until-idle`
- Use manual-control endpoints when operator intervention is needed
- Run cleanup through `/v1/cleanup/run-once`
- Treat `8787` as compatibility-only
