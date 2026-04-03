# Control Plane v2 Bootstrap, Validation, SQLite Migrations, Registry, Intake, Run, Step, Dispatch, Worker, Manual Control, and Cleanup Utilities

## Scope
- This step adds the first executable infrastructure layer for the v2 scaffold only.
- It provides strict project package validation, SQLite schema bootstrap/init, SQLite migration management, project registry/import, bounded task intake/run submission, root run creation/inspection, step_run lifecycle utilities, reviewer outcome/follow-up persistence, provisional scheduler claim/release primitives, a bounded manual dispatch adapter for claimed runs, a bounded single-worker loop v1, a bounded manual control/recovery layer v1, and a bounded runtime cleanup manager v1.
- It still does not implement a daemon/service runtime, multi-worker protocol, or auto-continue policy engine.

## Project package validation

Use the validator against a project key under `projects/`:

```bash
cd /home/dkar/workspace/control
./scripts/validate-project-package sample-project
```

Use an explicit package path and JSON output:

```bash
cd /home/dkar/workspace/control
./scripts/validate-project-package ./projects/sample-project --json
```

Current hard validation covers only the approved contract:
- package directory must exist
- all mandatory YAML files must exist
- every YAML root document must be a mapping
- `project.yaml.schema_version` must exist and be a string
- `capabilities.yaml.sections` must exist and be a mapping

Current validation error codes:
- `PACKAGE_DIRECTORY_MISSING`
- `FILE_MISSING`
- `INVALID_YAML`
- `WRONG_ROOT_TYPE`
- `MISSING_REQUIRED_KEY`
- `WRONG_KEY_TYPE`

## SQLite bootstrap/init

Create or initialize a SQLite database from `schemas/sqlite-v1.sql`:

```bash
cd /home/dkar/workspace/control
./scripts/init-sqlite-v1 /tmp/control-plane-v2.sqlite
```

Machine-readable output:

```bash
cd /home/dkar/workspace/control
./scripts/init-sqlite-v1 /tmp/control-plane-v2.sqlite --json
```

Notes:
- fresh DB policy is: bootstrap the latest schema snapshot from `schemas/sqlite-v1.sql`, then mark the known migration chain as applied in `schema_migrations`
- the migration chain remains the source of truth for upgrade history under `schemas/migrations/`
- `init-sqlite-v1` is still safe on an empty database, but it now records schema version metadata instead of leaving the DB untracked

## SQLite migrations

Show the current SQLite schema version:

```bash
cd /home/dkar/workspace/control
./scripts/show-sqlite-schema-version /tmp/control-plane-v2.sqlite --json
```

List the discovered migration chain:

```bash
cd /home/dkar/workspace/control
./scripts/list-sqlite-migrations --json
```

Migrate an existing SQLite database in place:

```bash
cd /home/dkar/workspace/control
./scripts/migrate-sqlite-v1 /tmp/control-plane-v2.sqlite --json
```

SQLite migration behavior in this step:
- migration metadata is stored in `schema_migrations(version, name, applied_at)`
- the engine discovers ordered SQL files from `schemas/migrations/`
- migrations are required to form a contiguous non-branching chain such as `0001`, `0002`, `0003`
- migrate is idempotent; already-current DBs do not reapply migrations
- for recognized legacy untracked DBs, the engine backfills migration metadata first and then applies only the missing ordered SQL migrations
- current legacy detection distinguishes:
  - empty DB
  - legacy untracked v1 baseline schema
  - legacy untracked v2 schema that already includes manual-control `paused` state but lacks `schema_migrations`
  - legacy untracked v3 schema that already includes cleanup audit columns/tables but lacks `schema_migrations`
- invalid or partial migration history fails closed with an explicit error instead of silently guessing

Current migration chain:
- `0001_baseline.sql`: original pre-manual-control schema
- `0002_manual_control_paused.sql`: upgrades run and queue status constraints for paused-state support
- `0003_runtime_cleanup_audit.sql`: adds cleanup audit columns on `artifact_refs` plus `runtime_cleanup_records`

What is not supported in this step:
- downgrades
- branching migration histories
- non-SQLite backends

## Project registry/import

Register a validated package by project key:

```bash
cd /home/dkar/workspace/control
./scripts/register-project-package sample-project --sqlite-db /tmp/control-plane-v2.sqlite
```

Register a validated package by explicit path:

```bash
cd /home/dkar/workspace/control
./scripts/register-project-package ./projects/sample-project --sqlite-db /tmp/control-plane-v2.sqlite --json
```

List the current registry contents:

```bash
cd /home/dkar/workspace/control
./scripts/list-registered-projects --sqlite-db /tmp/control-plane-v2.sqlite
```

Registry behavior in this step:
- registration accepts only already validated project packages
- SQLite stores only registry metadata in `projects`
- idempotent upsert is keyed by `project_key`
- re-registering an existing project updates `package_root` and `updated_at`
- project YAML/config content remains sourced only from the control repo package under `projects/<project-key>/`
- project config is not copied into SQLite

## Bounded task intake / run submission v1

Submit one bounded task through the unified intake path:

```bash
cd /home/dkar/workspace/control
./scripts/submit-bounded-task \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --project-key sample-project \
  --task-text "Implement the bounded task intake bridge." \
  --project-profile default \
  --workflow-id build \
  --milestone intake-v1 \
  --workspace-root /home/dkar/workspace \
  --json
```

Or submit from a JSON payload:

```bash
cd /home/dkar/workspace/control
./scripts/submit-bounded-task \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --submission-json /tmp/submission.json \
  --json
```

Inspect a submitted task:

```bash
cd /home/dkar/workspace/control
./scripts/show-submitted-task --sqlite-db /tmp/control-plane-v2.sqlite <run-id> --json
```

List recent submitted tasks:

```bash
cd /home/dkar/workspace/control
./scripts/list-submitted-tasks --sqlite-db /tmp/control-plane-v2.sqlite --project-key sample-project --json
```

Required submission fields in this step:
- `project_key`
- `task_text`
- `project_profile`
- `workflow_id`
- `milestone`

Optional submission fields in this step:
- `priority_class`
- `instruction_profile`
- `instruction_overlays`
- `source`
- `thread_label`
- `constraints`
- `expected_output`
- `artifact_root`
- `workspace_root`

Runtime context assembly behavior:
- intake validates that `project_key` is already registered in SQLite
- project package files remain the source of truth; the intake layer does not persist full config copies into SQLite
- runtime defaults come from `runtime.yaml.bounded_task_runtime_v1`
- instruction defaults come from `instructions.yaml.bounded_task_intake_v1`
- submission may override only the bounded-task fields listed above
- `workspace_root` may derive:
  - `project_repo_path = <workspace_root>/projects/<project_key>`
  - `executor_worktree_path = <workspace_root>/runtime/worktrees/<project_key>-executor`
  - `reviewer_worktree_path = <workspace_root>/runtime/worktrees/<project_key>-reviewer`
  - `instructions_repo_path = <workspace_root>/instructions`

Persistence behavior:
- intake creates the root run through the existing `create-root-run` path
- it stores `task-submission.json` and `runtime-context.json` under the run artifact tree when available
- both manifests are recorded in `artifact_refs` as:
  - `task_submission_manifest`
  - `task_runtime_context_manifest`

Worker integration in this step:
- a successfully submitted run is immediately queue-eligible
- `run-worker-tick` and `run-worker-until-idle` can pick it up without a separate `context.json`
- the dispatch adapter reuses the persisted runtime-context manifest as the worker-side runtime input source

## Root run creation and inspection

Create a root run for an already registered project:

```bash
cd /home/dkar/workspace/control
./scripts/create-root-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --project-key sample-project \
  --project-profile default \
  --workflow-id build \
  --milestone initial
```

Create a root run and also build the run-level artifact directory skeleton:

```bash
cd /home/dkar/workspace/control
./scripts/create-root-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --project-key sample-project \
  --project-profile default \
  --workflow-id build \
  --milestone initial \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --json
```

List runs:

```bash
cd /home/dkar/workspace/control
./scripts/list-runs --sqlite-db /tmp/control-plane-v2.sqlite --project-key sample-project
```

Show one run:

```bash
cd /home/dkar/workspace/control
./scripts/show-run --sqlite-db /tmp/control-plane-v2.sqlite <run-id>
```

Root run behavior in this step:
- the project must already be registered in SQLite
- only root run creation is implemented
- one root run creates one `runs` row and one linked `queue_items` row
- initial append-only transition history is written for the run and queue item
- manual root creation defaults to `priority_class = interactive`
- a new `flow_id` is created for each root run
- run scope is immutable at creation time: `project`, `project_profile`, `workflow_id`, `milestone`
- provisional `origin_type` is currently fixed to `root_manual` for this path only
- full worker/runtime execution is not implemented

## Step run lifecycle

Start a step run:

```bash
cd /home/dkar/workspace/control
./scripts/start-step-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --step-key executor
```

Finish a running step run:

```bash
cd /home/dkar/workspace/control
./scripts/finish-step-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <step-run-id> \
  --status succeeded
```

Create a retry from a terminal step run:

```bash
cd /home/dkar/workspace/control
./scripts/retry-step-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <step-run-id>
```

List step runs:

```bash
cd /home/dkar/workspace/control
./scripts/list-step-runs --sqlite-db /tmp/control-plane-v2.sqlite --run-id <run-id>
```

Show one step run:

```bash
cd /home/dkar/workspace/control
./scripts/show-step-run --sqlite-db /tmp/control-plane-v2.sqlite <step-run-id>
```

Step run behavior in this step:
- allowed `step_key` values are currently limited to `executor` and `reviewer`
- `start-step-run` creates a new `step_runs` row in `running`
- the first started step on a queued run moves the run to `running`
- the first started step on a queued queue item moves the queue item to `claimed`
- `finish-step-run` moves only the target `step_run` to a terminal status
- `retry-step-run` creates a new `step_run` row with `attempt_no + 1` and `previous_step_run_id`
- retry is allowed only from a terminal predecessor
- retry from a non-terminal predecessor fails closed
- reviewer outcome handling is a separate step after the reviewer `step_run` becomes terminal

## Reviewer outcomes and follow-up runs

Complete a terminal reviewer step run:

```bash
cd /home/dkar/workspace/control
./scripts/complete-reviewer-outcome \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <reviewer-step-run-id> \
  --verdict approved \
  --summary "ready to merge"
```

List the flow chain:

```bash
cd /home/dkar/workspace/control
./scripts/list-flow-runs --sqlite-db /tmp/control-plane-v2.sqlite <flow-id>
```

Reviewer outcome behavior in this step:
- `approved` completes the current run and completes its queue item
- `blocked` stops the current run and cancels its queue item
- `changes_requested` completes the current run and creates a queued follow-up run only if continuation is still allowed
- a created follow-up run reuses the same `project`, `project_profile`, `workflow_id`, `milestone`, and `flow_id`
- a created follow-up run stores `parent_run_id`, `origin_type = reviewer_followup`, `origin_run_id`, and `origin_step_run_id`
- reviewer-created follow-up runs default to `priority_class = interactive`
- key reviewer semantic outcomes now write `run_snapshots` with both `run` and `flow` scope

Cycle and guardrail behavior in this step:
- hard stop is enforced as `max_cycles = 3`
- root run counts as cycle `1`
- each follow-up run in the same `flow_id` increments the cycle by `1`
- when `changes_requested` would require cycle `4`, the current run is stopped and no new follow-up run is created
- provisional `max_run_attempts` is currently counted as total persisted `runs` in the same `flow_id`, so the next follow-up attempt is blocked once it would exceed `3`
- provisional `max_wall_clock_time` is currently measured from the first run `created_at` in the flow to the reviewer outcome decision time
- provisional `max_wall_clock_time` is currently fixed in code to `86400` seconds until policy/config semantics are frozen

## Reviewer result ingestion bridge

Ingest a terminal reviewer dispatch result into the existing reviewer outcome layer:

```bash
cd /home/dkar/workspace/control
./scripts/ingest-reviewer-result \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --step-run-id <reviewer-step-run-id> \
  --json
```

Or target the persisted dispatch result manifest directly:

```bash
cd /home/dkar/workspace/control
./scripts/ingest-reviewer-result \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --dispatch-result-manifest /tmp/control-plane-v2-artifacts/<project>/<flow>/<run>/reviewer/<step-run-id>/dispatch-result.json \
  --json
```

Optional inspection-only path:

```bash
cd /home/dkar/workspace/control
./scripts/show-dispatch-result \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --step-run-id <reviewer-step-run-id> \
  --json
```

Reviewer ingestion behavior in this step:
- the bridge extracts semantic reviewer data from persisted artifacts, then calls the existing `complete-reviewer-outcome` persistence layer
- it does not duplicate follow-up creation, guardrails, or terminal run/queue semantics
- source-of-truth priority for verdict extraction is:
  - `step_result_json`
  - `dispatch_result_manifest.dispatch_outcome.state_result`
  - `step_state_json.result`
  - strict `reviewer-report.md` parsing as fallback
- the reviewer report parser is strict:
  - line 1 must be `Verdict: approved|changes_requested|blocked`
  - line 2 must be `Summary: <non-empty summary>`
  - line 3 may be empty or `Commit SHA: <sha|none>`
- if no unambiguous verdict can be extracted, ingestion fails closed and leaves the flow unchanged
- `--verdict` exists only for manual recovery/debug mode and overrides only the semantic verdict
- when summary or `commit_sha` are available in readable artifacts, the ingestion result preserves their provenance and reuses them where possible

## Scheduler claim, release, and dispatch-failed primitives

Claim the next runnable queued run:

```bash
cd /home/dkar/workspace/control
./scripts/claim-next-run --sqlite-db /tmp/control-plane-v2.sqlite --json
```

Release a claimed queue item back to `queued`:

```bash
cd /home/dkar/workspace/control
./scripts/release-claimed-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --available-at 2026-01-01T00:00:00Z \
  --json
```

Record dispatch failure or abandoned claim and requeue the item:

```bash
cd /home/dkar/workspace/control
./scripts/mark-claimed-run-dispatch-failed \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --queue-item-id <queue-item-id> \
  --reason-code dispatch_failed \
  --json
```

Scheduler primitive behavior in this step:
- eligible queue item means `queue_items.status = queued` and `queue_items.available_at <= now`
- eligible run status is now `queued` or `running`, so a requeued in-progress run can be claimed again after a technical dispatch failure
- claim ordering is `system > interactive > background`
- inside a class, the provisional v1 aging formula is `effective_age_seconds = max(0, now_utc - available_at_utc)`
- after effective age, ties are broken by `enqueued_at`, then `queue_item.id`
- claim is atomic inside one SQLite transaction using `BEGIN IMMEDIATE`
- claim changes only the `queue_items` row from `queued -> claimed` and appends a `state_transitions` row
- claim does not move `runs.status` to `running`; that still happens later when a real `step_run` starts
- claim returns machine-readable payload for future dispatch containing:
  - run
  - queue item
  - project registry info
  - project package root
  - minimal flow context
- release changes the queue item from `claimed -> queued`, clears `claimed_at`, optionally updates `available_at`, and appends a transition
- dispatch-failed uses the same requeue shape as release, but writes a dedicated transition type plus required `reason_code`
- dispatch-failed does not move the run to a terminal state by itself

## Manual dispatch adapter

Dispatch the executor for a claimed run:

```bash
cd /home/dkar/workspace/control
./scripts/dispatch-executor-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --claim-json /tmp/claimed-run.json \
  --context-json /tmp/context.json \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --json
```

Dispatch the reviewer separately:

```bash
cd /home/dkar/workspace/control
./scripts/dispatch-reviewer-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --json
```

Or auto-detect the next role:

```bash
cd /home/dkar/workspace/control
./scripts/dispatch-next-for-claimed-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --json
```

Manual dispatch behavior in this step:
- the run must already be claimed
- role resolution is intentionally limited to `executor` and `reviewer`
- the adapter reuses `run-executor.sh` and `run-reviewer.sh` as the backend runtime
- reviewer dispatch explicitly disables legacy auto-completion so reviewer semantic outcome remains a separate ingestion step
- the adapter records minimal useful artifacts through `artifact_refs`: dispatch context/result manifests, resolved instruction manifests, stdout/stderr logs, prompt copy, and report file refs when present
- a backend launch failure before execution requeues the claimed item through `mark-claimed-run-dispatch-failed`
- a backend process that starts and then exits non-zero produces a terminal failed `step_run`; it is not treated as a dispatch-failed requeue

Current safety boundary:
- claim safety is provisional and assumes the accepted v1 single-machine shape
- the implementation serializes claims with SQLite write locking, but there is still no lease heartbeat, ownership token, or finalized multi-worker protocol
- the worker loop in this step is intentionally single-process and bounded; it is not a daemon, service manager, or multi-worker protocol

## Single-worker loop v1

Run one worker tick:

```bash
cd /home/dkar/workspace/control
./scripts/run-worker-tick \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --context-json /tmp/context.json \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --worker-log-root /tmp/control-plane-v2-worker-logs \
  --json
```

Run the bounded loop until idle:

```bash
cd /home/dkar/workspace/control
./scripts/run-worker-until-idle \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --context-json /tmp/context.json \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --worker-log-root /tmp/control-plane-v2-worker-logs \
  --max-ticks 100 \
  --json
```

Worker tick behavior in this step:
- claim the next runnable run through existing scheduler primitives
- if nothing is claimable, return `idle`
- determine the next dispatchable role through the existing dispatch adapter
- dispatch `executor` or `reviewer` through the existing dispatch adapter
- after successful `executor`, immediately attempt `reviewer` in the same claimed run unless mode is explicitly `executor-only`
- after successful `reviewer`, immediately run `ingest-reviewer-result`
- if reviewer outcome creates a follow-up run, the current run becomes terminal and the follow-up remains queued for a future tick
- if reviewer dispatch succeeds but ingestion fails, the worker returns explicit `ingestion_failed` without silently mutating the flow further

Tick result statuses:
- `idle`: nothing was claimable
- `progressed`: the run advanced successfully, including approved and changes-requested outcomes
- `stopped`: reviewer outcome stopped the run, for example `blocked`
- `dispatch_failed`: dispatch adapter returned technical failure
- `ingestion_failed`: reviewer dispatch succeeded but semantic ingestion failed

Loop stop conditions implemented in this step:
- `idle`
- `dispatch_failed`
- `ingestion_failed`
- `max_ticks_reached`
- `max_claims_reached`
- `max_flows_reached`
- `max_wall_clock_time_reached`

Single-worker guarantees and limits in this step:
- intended for one Linux machine and one worker process
- relies on existing SQLite single-writer assumptions only
- no heartbeat, lease renewal, fencing token, or daemon/service manager
- no multi-worker safety protocol
- no deploy matrix in this step

## Manual control v1

Show the current run control state:

```bash
cd /home/dkar/workspace/control
./scripts/show-run-control-state \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <run-id> \
  --json
```

Pause a run:

```bash
cd /home/dkar/workspace/control
./scripts/pause-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <run-id> \
  --note "operator pause" \
  --operator alice \
  --json
```

Resume a paused run:

```bash
cd /home/dkar/workspace/control
./scripts/resume-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <run-id> \
  --mode normal \
  --json
```

Resume a paused run in stabilization mode:

```bash
cd /home/dkar/workspace/control
./scripts/resume-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <run-id> \
  --mode stabilize_to_green \
  --note "recover toward green" \
  --json
```

Force-stop a run:

```bash
cd /home/dkar/workspace/control
./scripts/force-stop-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <run-id> \
  --note "operator stop" \
  --json
```

Request a narrow rerun from a terminal step:

```bash
cd /home/dkar/workspace/control
./scripts/rerun-run-step \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <step-run-id> \
  --note "rerun failed step" \
  --json
```

Manual control behavior in this step:
- paused run means both `runs.status` and `queue_items.status` are set to `paused`
- paused runs are not worker-claimable because scheduler eligibility still requires `queue_items.status = queued`
- pause is allowed only when the run is queued or claimed and there is no active `step_run`
- pausing an active backend step is explicitly rejected in v1 as `not safe`, because there is no true interrupt, heartbeat, or fencing protocol yet
- `resume --mode normal` returns the run to `queued`
- `resume --mode stabilize_to_green` also returns the run to `queued`, but appends explicit recovery-intent metadata to state/history so later inspection can see why the run was resumed
- `force-stop` moves the run to terminal `stopped` and the queue item to terminal `cancelled`
- `force-stop` does not perform cleanup side effects and does not attempt to interrupt an already-running backend process
- rerun is intentionally narrow and append-only:
  - executor rerun is allowed only from a terminal failed/stopped executor path before reviewer history exists
  - reviewer rerun is allowed only from a terminal failed/stopped reviewer path when the run is not already completed
- rerun does not reset the whole flow; it records a rerun intent and lets the worker/dispatch path consume it through the existing retry primitive
- `show-run-control-state` exposes the latest manual transition, latest resume mode, and any pending rerun intent for recovery/debug use

## Runtime cleanup manager v1

List eligible cleanup candidates:

```bash
cd /home/dkar/workspace/control
./scripts/list-cleanup-candidates \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --json
```

Run one dry-run cleanup pass:

```bash
cd /home/dkar/workspace/control
./scripts/run-cleanup-once \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --dry-run \
  --json
```

Run one real cleanup pass for worktrees only:

```bash
cd /home/dkar/workspace/control
./scripts/run-cleanup-once \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --scope worktrees \
  --json
```

Show persisted cleanup audit state:

```bash
cd /home/dkar/workspace/control
./scripts/show-cleanup-status \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --json
```

Cleanup behavior in this step:
- cleanup is terminal-only: queued, claimed, running, and paused runs are never auto-cleaned
- `force-stop` does not trigger immediate cleanup; stopped runs become eligible only after TTL expiry
- artifacts, worktrees, and local runtime branches have separate TTL categories
- TTL policy comes from `policy.yaml.cleanup_v1` when present:
  - `artifacts_ttl_seconds`
  - `worktree_ttl_seconds`
  - `branch_ttl_seconds`
- if `cleanup_v1` is missing, runtime defaults are used:
  - artifacts: `86400`
  - worktrees: `604800`
  - branches: `604800`
- artifact cleanup deletes the referenced filesystem path, but keeps the `artifact_refs` row and writes `cleaned_at`, `cleanup_status`, `cleanup_result_json`, and `last_cleanup_error`
- worktree and branch cleanup keep append-only audit rows in `runtime_cleanup_records`
- branch cleanup is limited to local runtime branches discovered from persisted dispatch context and will not delete `main`, `master`, or the persisted `branch_base`
- cleanup errors are recorded per target and do not crash the whole pass

What is not supported in this step:
- remote branch deletion
- cleanup of active or paused flows
- distributed garbage collection
- daemon/service scheduling for cleanup

## Smoke checks

Run the isolated smoke coverage for validator and SQLite bootstrap:

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2.sh
```

The smoke script verifies:
- successful validation of `projects/sample-project`
- failure on missing required file
- failure on non-mapping YAML root
- failure on invalid YAML
- failure on missing required key
- failure on wrong key type
- successful SQLite schema bootstrap
- successful project registration for `sample-project`
- idempotent second register without duplicate row
- `package_root` update on re-register
- registry listing
- clean register failure when package validation fails
- clean root run create failure when project is not registered
- successful root run creation for a registered project
- `runs`, `queue_items`, and initial `state_transitions` rows exist
- `list-runs` returns the created run
- `show-run` returns the detailed payload
- `start-step-run` starts executor/reviewer step runs
- `finish-step-run` persists terminal step statuses

Run the focused manual-dispatch smoke:

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-dispatch.sh
```

The dispatch smoke verifies:
- claim a queued run
- executor dispatch through the v2 adapter and real legacy executor backend
- terminal executor `step_run` persistence
- reviewer dispatch through the v2 adapter and real legacy reviewer backend
- artifact refs, logs, prompt copy, and manifests are persisted
- reviewer ingestion can complete `approved`
- reviewer ingestion can stop `blocked`
- reviewer ingestion can create a `changes_requested` follow-up run
- malformed reviewer verdict extraction fails explicitly without silently mutating the flow
- manual override recovery can still close a malformed reviewer result
- a broken backend launch requeues the queue item cleanly through the dispatch-failed path

Run the focused single-worker smoke:

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-worker.sh
```

The worker smoke verifies:
- queue with one root run -> executor -> reviewer -> approved -> completed
- queue with `changes_requested` -> follow-up created -> next tick claims the follow-up run
- queue with `blocked` -> terminal stop
- broken dispatch path -> explicit `dispatch_failed` worker summary plus queue requeue
- malformed reviewer result -> explicit `ingestion_failed` worker summary
- `run-worker-until-idle` processes a bounded chain and stops at `idle`
- `run-worker-until-idle --max-ticks 1` stops at `max_ticks_reached`
- worker tick and loop summaries are written as minimal JSON/Markdown artifacts

Run the focused manual-control smoke:

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-manual-control.sh
```

The manual-control smoke verifies:
- pause queued run -> worker skips it
- resume paused run -> worker claims it again
- `resume --mode stabilize_to_green` preserves recovery metadata in run control state/history
- force-stop queued run -> run `stopped`, queue item `cancelled`, no cleanup side effects
- force-stop claimed-not-started run -> run `stopped`, queue item `cancelled`, no cleanup side effects
- pause active step path -> explicit `not safe` failure
- narrow rerun of a failed executor path -> pending rerun intent is visible and the next worker tick consumes it through the retry primitive
- `retry-step-run` builds a retry chain with `attempt_no + 1` and `previous_step_run_id`
- invalid retry from a non-terminal step fails cleanly
- `list-step-runs` and `show-step-run` return the expected chain
- reviewer `approved` completes the run without creating a follow-up
- reviewer `blocked` stops the run without creating a follow-up
- reviewer `changes_requested` creates follow-up runs until cycle `3`
- reviewer `changes_requested` past cycle `3` stops the current run cleanly
- `list-flow-runs` returns the ordered flow chain
- scheduler claim returns the highest-priority eligible run

Run the focused SQLite migration smoke:

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-sqlite-migrations.sh
```

The migration smoke verifies:
- fresh DB init -> schema version metadata is recorded correctly
- old DB at earlier schema -> migrate upgrades successfully without re-init
- second migrate run is idempotent
- migrated DB supports manual-control `paused` state
- migrated DB also includes cleanup-audit schema version `0003`
- migrated DB still passes a bounded worker executor -> reviewer -> approved path
- invalid/partial migration metadata state fails explicitly
- same-class scheduler ordering is stable and deterministic

Run the focused cleanup smoke:

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-cleanup.sh
```

The cleanup smoke verifies:
- terminal run becomes an artifact/worktree/branch cleanup candidate after TTL expiry
- paused run is skipped
- dry-run does not delete files, worktrees, or branches
- full cleanup deletes eligible artifacts, worktree paths, and local runtime branches
- cleanup metadata remains queryable through `show-cleanup-status`
- repeated cleanup pass is idempotent
- claimed runs can be released back to `queued`
- dispatch-failed requeues the claimed run and writes append-only transitions with reason metadata
- one claimed queue item is not claimed twice while still claimed

Run the focused intake smoke:

```bash
cd /home/dkar/workspace/control
./scripts/smoke-control-plane-v2-intake.sh
```

The intake smoke verifies:
- explicit failure when required runtime defaults cannot be assembled
- root run creation plus intake manifest persistence
- submission overrides for overlays, source, and thread label
- worker pickup without a separate dispatch `context.json`

## Boundaries of this step
- No daemonized worker/service manager.
- No multi-worker protocol.
- No finalized multi-worker scheduler protocol.
- No project import of YAML/config payload into SQLite beyond registry metadata.
- No legacy pipeline behavior changes.

## OPEN_ISSUE / TODO
- TODO(OPEN_ISSUE): freeze canonical `schema_version` format/policy beyond string type.
- TODO(OPEN_ISSUE): freeze mandatory semantic keys for non-`project.yaml` files.
- TODO(OPEN_ISSUE): freeze canonical capabilities section taxonomy.
- TODO(OPEN_ISSUE): freeze canonical opaque id format; current id generation wrapper uses UUIDv4 text behind abstraction.
- TODO(OPEN_ISSUE): freeze root/manual `origin_type` taxonomy beyond the provisional `root_manual` value used in this step.
- TODO(OPEN_ISSUE): freeze reviewer follow-up `origin_type` taxonomy beyond the provisional `reviewer_followup` value used in this step.
- TODO(OPEN_ISSUE): freeze whether retry is allowed from every terminal step status or only from a subset.
- TODO(OPEN_ISSUE): freeze step transition taxonomy beyond the provisional start/finish/retry labels used in this step.
- TODO(OPEN_ISSUE): decide whether initial root run creation should also emit `run_snapshots` once snapshot policy is approved.
- TODO(OPEN_ISSUE): decide when run/queue should move to terminal states after the last step completes.
- TODO(OPEN_ISSUE): freeze the exact numeric/config source for `max_run_attempts` and `max_wall_clock_time`; current implementation uses provisional in-code values and counting rules.
- TODO(OPEN_ISSUE): decide long-term home for v2 executable code if a larger Python package layout is introduced later.
