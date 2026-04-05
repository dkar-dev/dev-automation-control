# Control Plane v2 Deployable-Green Decision Gate v1

## Scope
- This is the formal final quality/promotion decision layer for Control Plane v2.
- It reads persisted reviewer outcome state plus the latest persisted host-side checks result for one run.
- It returns one final decision:
  - `deployable_green`
  - `not_green`
  - `blocked`
- It is explicitly host-side only.
- It does not perform rollout orchestration, Kubernetes control, cluster deployment management, or remote execution.

## Required Inputs
- `run_id` is required.
- `flow_id` is optional and is used only for scope validation.

Source of truth in v1:
- reviewer verdict source:
  - latest persisted reviewer outcome for the run
  - primary source: `run_snapshots.snapshot_json.kind = reviewer_outcome`
  - fallback source: reviewer terminal `state_transitions` metadata when needed
- host checks verdict source:
  - latest row in `host_check_runs` for the run

## Exact Decision Table

| Reviewer verdict source | Latest host checks verdict source | Final decision | Notes |
| --- | --- | --- | --- |
| missing or invalid | any | `blocked` | reviewer outcome must exist before the formal gate can decide |
| `changes_requested` | any | `not_green` | reviewer did not approve the run |
| `blocked` | any | `blocked` | reviewer explicitly blocked the run |
| `approved` | missing or invalid | `blocked` | latest host-side checks result must exist before the formal gate can decide |
| `approved` | `not_green` | `not_green` | latest host-side checks contain required failures |
| `approved` | `blocked` | `blocked` | latest host-side checks are blocked by invalid config or impossible execution |
| `approved` | `green` | `deployable_green` | reviewer approved and latest host-side checks are green |

Interpretation notes:
- `not_green` is for explicit non-pass quality outcomes.
- `blocked` is reserved for missing prerequisites, invalid state, or an explicit reviewer block.
- v1 uses the latest host-check result for the run as persisted truth. It does not add extra rollout freshness semantics beyond that.

## Decision Metadata
Each decision manifest includes:
- `decision_id`
- `created_at`
- `decision_status`
- `reviewer_verdict_source`
- `host_checks_verdict_source`
- `summary`
- `rationale`
- `next_action_hint`

The source objects record:
- verdict
- source kind
- source reference id
- source timestamp
- source-specific metadata such as reviewer step id or host-check run id

## Persistence
- Each decision creates a new append-only row in SQLite table `green_decisions`.
- Each decision writes a manifest JSON artifact and records it in `artifact_refs` as `deployable_green_decision_manifest`.
- Output path:
  - run artifact tree when available: `<artifact_root>/<project>/<flow>/<run>/green-decisions/<decision_id>/manifest.json`
  - fallback: `control/.logs/deployable-green/...`
- Prior decisions are never silently overwritten.

## CLI
- [`scripts/decide-deployable-green`](/home/dkar/workspace/control/scripts/decide-deployable-green)
- [`scripts/show-deployable-green-decision`](/home/dkar/workspace/control/scripts/show-deployable-green-decision)

Examples:

```bash
cd /home/dkar/workspace/control
./scripts/decide-deployable-green \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --json
```

```bash
cd /home/dkar/workspace/control
./scripts/show-deployable-green-decision \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <run-id> \
  --json
```

Exit behavior:
- `decide-deployable-green` exits `0` only for `deployable_green`
- `not_green` and `blocked` exit non-zero but still return structured JSON when `--json` is used

## HTTP API
- `POST /v1/green/decide`
- `GET /v1/green/{run_id}`

Example:

```bash
curl -s http://127.0.0.1:8788/v1/green/decide \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "<run-id>"
  }'
```

## Relation Between Reviewer, Host Checks, and Final Green Decision
- Reviewer outcome remains its own bounded layer.
- Host-side checks remain their own bounded layer.
- The formal v1 path is explicit:
  1. complete reviewer outcome persistence
  2. run host-side checks
  3. decide deployable green

The final decision layer does not hide reviewer logic inside host checks and does not hide host-check logic inside reviewer persistence.

## Worker / Flow Integration
- v1 does not auto-insert this decision inside every worker path.
- Operators and automations should call it explicitly after reviewer approval and host checks.
- That keeps the promotion decision auditable and separate from execution semantics.

## Out Of Scope For v1
- deployment controller behavior
- Kubernetes rollout orchestration
- cluster state reconciliation
- remote fleet promotion
- secret distribution
- automatic deploy execution

## Smoke Coverage
- [`scripts/smoke-control-plane-v2-deployable-green.sh`](/home/dkar/workspace/control/scripts/smoke-control-plane-v2-deployable-green.sh) verifies:
  - reviewer approved + host checks green => `deployable_green`
  - reviewer approved + host checks not_green => `not_green`
  - reviewer approved + missing host checks => `blocked`
  - reviewer blocked => `blocked`
  - persisted decision visibility through CLI and HTTP
