# Control Plane v2 Release Handoff Bundle v1

## Scope
- This is the explicit export and packaging layer after the formal deployable-green decision.
- It creates one release-ready handoff bundle for an operator or an external deployment system.
- It packages persisted final context for one run:
  - run and flow scope
  - handoff commit snapshot
  - reviewer verdict source
  - host-check source
  - deployable-green decision source
  - next operator actions
- It does not deploy anything.
- It does not run rollout scripts.
- It does not add rollback orchestration.
- It does not mutate reviewer, host-check, or deployable-green decision state.

## Eligibility
- `run_id` is required.
- `flow_id` is optional and is used only for scope validation.
- Bundle creation is allowed only when the latest persisted deployable-green decision for the run is `deployable_green`.
- If the latest decision is `not_green` or `blocked`, bundle creation fails explicitly and no bundle row/artifacts are written.

## Bundle Contents
Every manifest includes at least:
- `bundle_id`
- `created_at`
- `run_id`
- `flow_id`
- `project_key`
- `workflow_id`
- `project_profile`
- `milestone`
- `decision_status`
- `summary`
- `rationale`
- `commit_sha`
- `commit_source`
- `reviewer_verdict_source`
- `host_checks_source`
- `deployable_green_decision_source`
- `next_action_instructions`
- `operator_notes`
- `artifact_refs`

Generated bundle files:
- machine-readable manifest JSON
- human-readable markdown summary
- artifact index JSON

## Commit Source Rule
Chosen v1 rule:
- bundle creation fails explicitly when no persisted `commit_sha` can be resolved
- v1 does not create a partial or blocked handoff export

Resolution order:
1. latest executor `dispatch_result_manifest` for the run
2. latest executor `dispatch_result_manifest` for the flow
3. latest reviewer `dispatch_result_manifest` or reviewer-derived persisted commit source for the run
4. latest reviewer `dispatch_result_manifest` or reviewer-derived persisted commit source for the flow

Interpretation notes:
- the preferred source of truth is the persisted runtime artifact set already recorded by dispatch/reviewer flows
- if no persisted `commit_sha` is found in those sources, export fails with `RELEASE_HANDOFF_COMMIT_MISSING`

## Persistence
- Each bundle creates a new append-only row in SQLite table `release_handoffs`.
- Each bundle records three artifact refs in `artifact_refs`:
  - `release_handoff_manifest`
  - `release_handoff_summary_markdown`
  - `release_handoff_artifact_index`
- Output path:
  - run artifact tree when available: `<artifact_root>/<project>/<flow>/<run>/release-handoffs/<bundle_id>/...`
  - fallback: `control/.logs/release-handoffs/...`
- Prior bundles are never silently overwritten.

## CLI
- [`scripts/create-release-handoff`](/home/dkar/workspace/control/scripts/create-release-handoff)
- [`scripts/show-release-handoff`](/home/dkar/workspace/control/scripts/show-release-handoff)
- [`scripts/list-release-handoffs`](/home/dkar/workspace/control/scripts/list-release-handoffs)

Examples:

```bash
cd /home/dkar/workspace/control
./scripts/create-release-handoff \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --json
```

```bash
cd /home/dkar/workspace/control
./scripts/show-release-handoff \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <run-id> \
  --json
```

## HTTP API
- `POST /v1/release-handoff/create`
- `GET /v1/release-handoff/{run_id}`

Example:

```bash
curl -s http://127.0.0.1:8788/v1/release-handoff/create \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "<run-id>",
    "operator_notes": ["external deploy system picks up this bundle"]
  }'
```

## Operator / External Deploy Consumption
- Treat the bundle as the explicit approval/export packet for one release snapshot.
- Use `commit_sha` as the handoff snapshot to deploy.
- Use the referenced deployable-green, reviewer, and host-check sources as the supporting evidence for promotion.
- Execute rollout or deployment outside Control Plane v2.
- Control Plane v2 still does not perform rollout/deployment orchestration in v1.

## Smoke Coverage
- [`scripts/smoke-control-plane-v2-release-handoff.sh`](/home/dkar/workspace/control/scripts/smoke-control-plane-v2-release-handoff.sh) verifies:
  - deployable-green + persisted commit => bundle created
  - deployable-green + missing commit => explicit export failure
  - `not_green` decision => export rejected
  - persisted bundle visibility through CLI
  - markdown summary and JSON manifest consistency
- [`scripts/smoke-control-plane-v2-api.sh`](/home/dkar/workspace/control/scripts/smoke-control-plane-v2-api.sh) also verifies:
  - release-handoff create/show through HTTP
