# Dev Automation Control Plane v2 Storage and Queue Contract

## Status
- Accepted contract for v1 SQLite-backed storage and queue model.
- Scheduler, worker loop, and manual operations API remain out of scope in this step.

## Scope
- Fix the storage model for:
  - `project`
  - `run`
  - `step_run`
  - `queue_item`
  - `artifact_ref`
  - `state_transition`
  - `run_snapshot`
- Preserve a future-ready abstraction so the same contract can later move from SQLite to Postgres.
- Keep legacy runtime behavior unchanged in this step.

## Non-goals in this step
- No scheduler implementation.
- No worker loop.
- No runtime execution code.
- No SQL migrations framework.
- No changes to legacy single-run scripts as executable behavior.
- No manual operations API implementation.

## Storage split

### SQLite is the source of truth for
- Current active state for `runs`, `step_runs`, and `queue_items`.
- Immutable run identity and linkage metadata.
- Append-only transition history.
- Append-only snapshot metadata and payloads.
- Artifact metadata and filesystem references.

### Filesystem artifacts are the source of truth for
- Prompts.
- Transcripts.
- Logs.
- Patches.
- Executor/reviewer outputs.
- Any large or binary payloads.

### Separation rule
- SQLite stores metadata, pointers, timestamps, state, and compact JSON/text payloads.
- Filesystem stores heavy artifacts addressed by stable paths recorded in `artifact_refs`.

## Entity model

Field lists below describe the schema contract, split into mandatory and nullable fields where that matters.

### `projects`
Represents a project package defined in the control repo under `projects/<project-key>/`.

Mandatory fields:
- `id`
- `project_key`
- `package_root`
- `created_at`
- `updated_at`

Notes:
- `project_key` is the stable logical identifier used by runs and artifact layout.
- Project config remains sourced from the control repo package, not from SQLite rows.

### `runs`
Represents one strictly scoped run.

Run scope is immutable and always includes exactly:
- one `project`
- one `project_profile`
- one `workflow_id`
- one `milestone`

Mandatory fields:
- `id`
- `project_id`
- `project_profile`
- `workflow_id`
- `milestone`
- `flow_id`
- `origin_type`
- `status`
- `created_at`
- `updated_at`

Nullable fields:
- `parent_run_id`
- `origin_run_id`
- `origin_step_run_id`
- `queued_at`
- `started_at`
- `terminal_at`

Notes:
- Root runs start a new `flow_id`.
- Follow-up runs reuse the same `flow_id`.
- Root runs keep `parent_run_id`, `origin_run_id`, and `origin_step_run_id` null while still carrying a non-null `origin_type`.
- `project_profile` and `workflow_id` are immutable after run creation.
- `milestone` is also treated as immutable run scope.
- In the current executable scaffold step, the only implemented root-run `origin_type` is the provisional value `root_manual`.

### `step_runs`
Represents one actual launch of a logical step inside a run.

Contract:
- Every factual executor/reviewer launch creates a new `step_run`.
- Every retry creates a new `step_run`.
- Retry never mutates or reuses a previous `step_run`.

Mandatory fields:
- `id`
- `run_id`
- `step_key`
- `attempt_no`
- `status`
- `created_at`
- `started_at`

Nullable fields:
- `previous_step_run_id`
- `terminal_at`

Notes:
- `step_key` identifies the logical step inside the run.
- In v1, expected logical steps include executor/reviewer style stages, but the full step taxonomy is not frozen here.
- `attempt_no` starts at `1`.
- First attempts keep `previous_step_run_id` null.
- A retry must point to the immediately previous `step_run` through `previous_step_run_id`.

### `queue_items`
Represents a runnable queue entry for a run.

Mandatory fields:
- `id`
- `run_id`
- `priority_class`
- `status`
- `enqueued_at`
- `available_at`

Nullable fields:
- `claimed_at`
- `terminal_at`

Notes:
- v1 stores one active queue record per run.
- Queue state is mutable, but queue history is append-only in `state_transitions`.
- `claimed_at` and `terminal_at` stay null until the queue item is claimed or reaches a terminal queue state.
- `priority_class` values for v1 are:
  - `system`
  - `interactive`
  - `background`

### `artifact_refs`
Represents metadata for filesystem-backed artifacts.

Mandatory fields:
- `id`
- `project_id`
- `flow_id`
- `run_id`
- `artifact_kind`
- `filesystem_path`
- `created_at`

Nullable fields:
- `step_run_id`
- `media_type`
- `size_bytes`
- `checksum_sha256`

Notes:
- `step_run_id` may be null for run-level artifacts.
- `run_id` must always be present for v1 artifact refs.
- Artifact rows are append-only.

### `state_transitions`
Append-only history of state changes.

Mandatory fields:
- `id`
- `entity_type`
- `to_state`
- `transition_type`
- `created_at`

Conditional target field:
- exactly one of `run_id`, `step_run_id`, `queue_item_id`

Nullable fields:
- `from_state`
- `reason_code`
- `metadata_json`

Notes:
- Exactly one target entity reference is populated per row.
- `entity_type` is one of:
  - `run`
  - `step_run`
  - `queue_item`
- This table is the append-only audit trail for mutable active-state rows.

### `run_snapshots`
Append-only snapshots captured only at key transitions.

Mandatory fields:
- `id`
- `snapshot_scope`
- `project_id`
- `flow_id`
- `state_transition_id`
- `snapshot_json`
- `created_at`

Conditional target field:
- `run_id` is mandatory for `snapshot_scope = run` and null for `snapshot_scope = flow`

Notes:
- `snapshot_scope` is:
  - `run`
  - `flow`
- Flow snapshots do not require a dedicated `flows` table in v1.
- `run_id` is required for run snapshots and null for flow snapshots.

## Relationships

### Relational links
- One `project` has many `runs`.
- One `run` has many `step_runs`.
- One `run` has one active `queue_item` in v1.
- One `run` has many `artifact_refs`.
- One `step_run` can have many `artifact_refs`.
- One `run` and one `step_run` can each have many `state_transitions`.
- One `queue_item` can have many `state_transitions`.
- One `run` and one `flow_id` can each have many `run_snapshots`.

### Flow linkage source of truth
Flow lineage is stored on the `runs` row, not in a separate active `flows` table.

Required linkage fields on `runs`:
- `flow_id`
- `parent_run_id`
- `origin_type`
- `origin_run_id`
- `origin_step_run_id`

Interpretation:
- `flow_id` groups all runs in one atomic flow.
- `parent_run_id` links a follow-up run to the immediately preceding run in the chain.
- `origin_type` captures why the run exists, for example root creation, reviewer follow-up, manual intervention, or system-generated continuation.
- `origin_run_id` points to the run that caused this run to exist.
- `origin_step_run_id` points to the concrete step execution that caused this run to exist.

## Immutable fields

### Immutable on `runs`
- `id`
- `project_id`
- `project_profile`
- `workflow_id`
- `milestone`
- `flow_id`
- `parent_run_id`
- `origin_type`
- `origin_run_id`
- `origin_step_run_id`
- `created_at`

### Mutable on `runs`
- `status`
- `updated_at`
- `queued_at`
- `started_at`
- `terminal_at`

### Immutable on `step_runs`
- `id`
- `run_id`
- `step_key`
- `attempt_no`
- `previous_step_run_id`
- `created_at`

### Mutable on `step_runs`
- `status`
- `started_at`
- `terminal_at`

### Immutable on `queue_items`
- `id`
- `run_id`
- `priority_class`
- `enqueued_at`
- `available_at`

### Mutable on `queue_items`
- `status`
- `claimed_at`
- `terminal_at`

### Append-only by definition
- `artifact_refs`
- `state_transitions`
- `run_snapshots`

## Append-only history vs active state

### Active state
Mutable current-state fields live on:
- `runs.status`
- `step_runs.status`
- `queue_items.status`

### Append-only history
- `state_transitions` records every accepted state change.
- `run_snapshots` records only key run/flow state snapshots.
- `artifact_refs` records immutable metadata about filesystem outputs.

### Run history rule
- Run history is append-only even though `runs.status` is mutable.
- The mutable row is only the latest materialized state.
- The authoritative history is the ordered append-only `state_transitions` stream plus `run_snapshots`.

## Filesystem artifact layout

### Required path shape
Artifacts live under a project / flow / run / step_run hierarchy:

```text
<artifact-root>/<project-key>/<flow_id>/<run_id>/<step_run_id>/
```

For run-level artifacts without a concrete step directory, the path is:

```text
<artifact-root>/<project-key>/<flow_id>/<run_id>/
```

### SQLite responsibility
SQLite stores:
- artifact metadata
- owning `project_id`
- owning `flow_id`
- owning `run_id`
- optional owning `step_run_id`
- stable filesystem path pointer

### Filesystem responsibility
Filesystem stores:
- actual content bytes
- large JSON payloads
- markdown reports
- raw logs
- transcripts
- diffs
- binaries

## Run state machine

### States
- `queued`
- `running`
- `completed`
- `failed`
- `stopped`
- `cancelled`

### Transition rules
- New runs are inserted as `queued`.
- `queued -> running` when the run is claimed for execution.
- `running -> completed` when the run finishes normally, including the case where it produces a follow-up run.
- `running -> failed` when the run finishes unsuccessfully and no follow-up run is created.
- `queued -> stopped` or `running -> stopped` when hard-stop or guardrail rules block further execution.
- `queued -> cancelled` or `running -> cancelled` for explicit cancellation/manual stop paths once those APIs exist.

### Current executable boundary
- The current executable scaffold implements only root run insertion in `queued`.
- It also inserts one linked `queue_item` in `queued` and append-only initial state transition rows.
- It can also start and finish `step_runs`, create retry `step_runs`, move a queued run to `running`, and move a queued queue item to `claimed`.
- It can also complete reviewer outcomes, stop/complete the current run, complete/cancel the current queue item, create queued reviewer follow-up runs, and write key `run_snapshots`.
- Queue selection and real execution transitions are not implemented yet.

### Interpretation rule
- `runs.status` models execution lifecycle only.
- Semantic outcome such as reviewer approval, reviewer follow-up request, or block reason is recorded in snapshots, transition metadata, and artifacts, not by expanding the run status taxonomy in this contract.

## Step run state machine

### States
- `running`
- `succeeded`
- `failed`
- `timed_out`
- `cancelled`

### Transition rules
- In v1 a `step_run` row is created when the actual launch starts, so the initial persisted state is `running`.
- `running -> succeeded` when the concrete launch succeeds.
- `running -> failed` when the concrete launch fails.
- `running -> timed_out` when wall-clock or runtime timeout handling terminates the launch.
- `running -> cancelled` for explicit cancellation/manual stop paths once those APIs exist.

### Current executable boundary
- The current executable scaffold supports `executor` and `reviewer` step keys only.
- Retry is implemented as a new `step_run` row with the same `run_id` and `step_key`, `attempt_no + 1`, and `previous_step_run_id`.
- A terminal reviewer `step_run` can now be resolved into a semantic reviewer outcome in a separate persistence step.

### Retry rule
- Retry is never a state transition on the same `step_run`.
- Retry always inserts a new `step_run`.
- The new row uses:
  - same `run_id`
  - same `step_key`
  - `attempt_no = previous attempt_no + 1`
  - `previous_step_run_id = <previous step_run id>`

## Queue ordering rules

### Eligibility
A queue item is eligible only when:
- `status = queued`
- `available_at <= now`

### Class ordering
Queue classes are strictly ordered:
1. `system`
2. `interactive`
3. `background`

### Default follow-up priority
- A follow-up run created from reviewer output defaults to `interactive`.

### Stable ordering within a class
Within the same class, ordering is by:
1. effective age inside the class
2. `enqueued_at`
3. queue item identifier as a stable tie-breaker

### Cross-class rule
- Aging is not allowed to move an item across classes.
- A more aged `background` item still stays behind eligible `interactive` and `system` items.
- A more aged `interactive` item still stays behind eligible `system` items.

## Starvation protection

### v1 guarantee
- Starvation protection exists only inside a single class.
- The implementation may age older items within that class so they eventually outrank newer items of the same class.

### Explicit non-goal for v1
- There is no cross-class fairness guarantee.
- Persistent `system` load may starve `interactive` and `background`.
- Persistent `interactive` load may starve `background`.

### Scheduler contract boundary
- The exact aging formula is intentionally left out of this storage contract.
- What is fixed here is the boundary:
  - aging is allowed only inside a class
  - class precedence is never violated

## Hard stop and guardrail rules for an atomic flow

### Atomic flow identity
- An atomic flow is identified by `flow_id`.
- There is no separate active `flow` row in v1.

### Hard stop
- `max_cycles = 3`
- Root run counts as cycle `1`.
- Each follow-up run in the same `flow_id` increments the conceptual cycle by `1`.
- No new follow-up run may be created once the next cycle would exceed `3`.
- When the hard stop blocks continuation, the blocking fact must be recorded in `state_transitions`, and the affected run ends in `stopped`.

### Guardrails
Guardrails must be checked before:
- creating/enqueueing a follow-up run
- starting a new `step_run`

Guardrails for v1:
- `max_run_attempts`
- `max_wall_clock_time`

### Guardrail outcome
- If a guardrail trips, the current run must not continue to a new execution attempt.
- The run ends in `stopped`.
- The reason is recorded append-only in `state_transitions`.
- A key run or flow snapshot may also be written to `run_snapshots`.

### Storage support for guardrails
The contract stores enough information to evaluate:
- follow-up chain depth by `flow_id` plus `parent_run_id`
- elapsed wall-clock time by `created_at` and terminal timestamps
- retry attempts by counting `step_runs` per `run_id` and `step_key`

### Current executable boundary
- The current executable scaffold enforces `max_cycles = 3` when reviewer-driven follow-up creation is requested.
- The current executable scaffold also evaluates provisional in-code values for `max_run_attempts` and `max_wall_clock_time`.
- Current provisional counting rule for `max_run_attempts`: count persisted `runs` in the same `flow_id`, with the root run counted as attempt `1`.
- Current provisional counting rule for `max_wall_clock_time`: measure elapsed seconds from the first run `created_at` in the flow to the reviewer outcome decision timestamp.

## Future-ready for Postgres
- All identifiers are stored as opaque text ids rather than SQLite-only integer row ids.
- JSON-like payloads are stored as text so they can later move to `JSONB`.
- Relationships are explicit and normalized rather than hidden inside blobs.
- Filesystem artifact storage is decoupled from the relational backend.

## OPEN_ISSUE / TODO
- TODO(OPEN_ISSUE): Approve canonical opaque id format for all entities (`UUIDv7`, `ULID`, or equivalent).
- TODO(OPEN_ISSUE): Approve the exact allowed `origin_type` taxonomy.
- TODO(OPEN_ISSUE): Approve the exact `step_key` taxonomy for v1 beyond executor/reviewer-style stages.
- TODO(OPEN_ISSUE): Approve the exact aging formula used inside one queue class.
- TODO(OPEN_ISSUE): Freeze the precise policy counting semantics for `max_run_attempts` if it must differ from step-run retry counting.
- TODO(OPEN_ISSUE): Freeze the artifact root location and retention policy outside the relative path contract defined here.
