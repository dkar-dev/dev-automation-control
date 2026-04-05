# Control Plane v2 Runtime Event Journal v1

## Scope
- `v1` adds one append-only runtime event journal in SQLite.
- It is the bounded operator-facing feed for run, flow, and runtime lifecycle milestones.
- It is safe for CLI inspection and localhost HTTP polling.
- It is intentionally polling-friendly storage, not a message bus, not webhook delivery, and not a streaming transport.

## Non-goals
- no Kafka, NATS, Redis streams, or distributed fan-out
- no websocket or SSE transport
- no webhook delivery
- no exactly-once delivery semantics across nodes
- no attempt to replace the existing bounded persistence tables

## Schema
Every event stores only the bounded v1 fields below:
- `event_id`
- `created_at`
- `event_type`
- `entity_type`
- `entity_id`
- `project_key`
- `flow_id`
- `run_id`
- `step_run_id`
- `severity`
- `summary`
- `payload_redacted`
- `source_module`

Notes:
- `step_run_id` is optional.
- `project_key`, `flow_id`, and `run_id` may be `null` for runtime-level supervisor events that are not attached to one project/run.
- rows are append-only; existing events are never overwritten in place.

## Taxonomy
Implemented v1 event types:
- `task_submitted`
- `run_created`
- `run_claimed`
- `step_run_started`
- `step_run_finished`
- `reviewer_outcome_completed`
- `host_checks_completed`
- `deployable_green_decided`
- `release_handoff_created`
- `runtime_supervisor_started`
- `runtime_supervisor_stopped`
- `runtime_supervisor_degraded`

`run_created` is used both for root runs and reviewer-created follow-up runs. The payload distinguishes the origin type.

## Redaction Policy
- Event payloads are stored only as `payload_redacted`.
- Raw secret and `sensitive_config` values must never be written into journal rows.
- Where runtime secret/config resolution is involved, the journal uses the same runtime redaction policy as `runtime_secrets.py`.
- `plain_config` may remain visible only when it is already explicitly allowed by the existing runtime-value policy.
- Event summaries and payloads are kept intentionally small; they point at lifecycle outcomes and identifiers instead of copying whole manifests.

Practical implications:
- task submission events do not copy full task text into the journal
- reviewer outcome events do not copy reviewer free-form summary text into the journal
- host-check events record gate result metadata, not raw command output, headers, or resolved secret values

## Query Model
Supported filters:
- `limit`
- `project_key`
- `flow_id`
- `run_id`
- `event_type`
- `created_after`

Stable query behavior:
- results are newest-first
- filtering happens in application logic and is reused by CLI and HTTP handlers
- `created_after` is exclusive
- `limit` must be greater than zero

## CLI
List events:

```bash
cd /home/dkar/workspace/control
./scripts/list-runtime-events \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id run_123 \
  --limit 50
```

Show one event:

```bash
cd /home/dkar/workspace/control
./scripts/show-runtime-event \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  evt_123
```

Use `--json` for machine-readable output.

## HTTP API
List events:

```bash
curl 'http://127.0.0.1:8788/v1/events?run_id=run_123&limit=50'
```

Show one event:

```bash
curl 'http://127.0.0.1:8788/v1/events/evt_123'
```

Both routes reuse the same journal list/get functions as the CLI.

## Polling Contract For Operators And n8n
Recommended polling pattern:
1. Persist the last seen `created_at`.
2. Poll `GET /v1/events?created_after=<last_seen>&limit=<N>`.
3. Process events in response order.
4. Save the newest `created_at` you handled.

Common polling examples:
- one project: `GET /v1/events?project_key=demo&created_after=...`
- one flow: `GET /v1/events?flow_id=flow_123&created_after=...`
- one run: `GET /v1/events?run_id=run_123&created_after=...`
- one gate milestone: `GET /v1/events?event_type=deployable_green_decided&created_after=...`

This is the intended v1 integration shape for:
- local operators
- `n8n`
- external local pollers

## Relationship To Existing Persistence
- The journal does not replace `runs`, `step_runs`, `host_check_runs`, `green_decisions`, or `release_handoffs`.
- Those tables remain the source of detailed state and history.
- The journal is the bounded cross-cutting feed that tells a poller which lifecycle milestone happened and where to drill down next.
