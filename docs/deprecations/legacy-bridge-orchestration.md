# Deprecated: Legacy Bridge Orchestration Transport

## Status
- The legacy control bridge on `127.0.0.1:8787` is deprecated as the preferred orchestration/control-plane boundary.
- The preferred local orchestration path is the Control Plane HTTP API v1 on `127.0.0.1:8788`.
- New operator docs, `n8n` workflows, and local automation entry paths should use the HTTP API, not the legacy bridge.

## What Is Not Deprecated
- `scripts/run-executor.sh`
- `scripts/run-reviewer.sh`
- prompt building and instruction resolution scripts
- the v2 dispatch adapter and worker loop that reuse those backend scripts
- reviewer result ingestion through `scripts/ingest-reviewer-result`

These pieces remain the backend execution implementation. They are allowed to stay legacy as long as orchestration enters through the HTTP API or the existing v2 CLI utilities.

## What Remains Only For Compatibility
- `scripts/run-bridge.sh`
- `bridge/http_bridge.py`
- `n8n/workflows/control-bridge-run-v1.json`
- legacy bridge endpoints such as `/prepare-run`, `/run-executor`, `/run-reviewer`, `/current-run`, and `/finalize-run`

These surfaces remain available only for compatibility/debugging. They are not the forward orchestration path.

## Operator Guidance
- Submit bounded work with `POST /v1/tasks/submit` or `./scripts/submit-bounded-task`
- Generate bounded contracts with `POST /v1/contracts/generate` or `./scripts/generate-bounded-contract`
- Progress work with `POST /v1/worker/tick`, `POST /v1/worker/run-until-idle`, `./scripts/run-worker-tick`, or `./scripts/run-worker-until-idle`
- Use manual control through `GET /v1/runs/{run_id}/control-state` and `POST /v1/runs/{run_id}/{pause|resume|force-stop|rerun-step}`
- Run cleanup through `POST /v1/cleanup/run-once` or `./scripts/run-cleanup-once`

## Compatibility Notes
- This cutover does not introduce a second compatibility bridge.
- This cutover does not remove the legacy backend runner layer.
- This cutover does not change public deployment or auth posture.
- This cutover does not rewrite the dispatch backend.

For old-to-new command mapping and behavior details, see [`docs/control-plane-v2/orchestration-cutover.md`](/home/dkar/workspace/control/docs/control-plane-v2/orchestration-cutover.md).
