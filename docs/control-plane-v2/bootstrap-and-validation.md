# Control Plane v2 Bootstrap, Validation, Registry, Run, and Step Utilities

## Scope
- This step adds the first executable infrastructure layer for the v2 scaffold only.
- It provides strict project package validation, SQLite schema bootstrap/init, project registry/import, root run creation/inspection, step_run lifecycle utilities, and reviewer outcome/follow-up persistence.
- It does not implement scheduler behavior or worker/runtime execution.

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
- this utility applies the accepted schema SQL as-is
- it is not a migrations framework
- the current requirement is only schema bootstrap on an empty SQLite database

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
- scheduler claims and runtime execution are not implemented

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
- `retry-step-run` builds a retry chain with `attempt_no + 1` and `previous_step_run_id`
- invalid retry from a non-terminal step fails cleanly
- `list-step-runs` and `show-step-run` return the expected chain
- reviewer `approved` completes the run without creating a follow-up
- reviewer `blocked` stops the run without creating a follow-up
- reviewer `changes_requested` creates follow-up runs until cycle `3`
- reviewer `changes_requested` past cycle `3` stops the current run cleanly
- `list-flow-runs` returns the ordered flow chain

## Boundaries of this step
- No scheduler.
- No worker loop.
- No runtime execution.
- No queue execution.
- No project import of YAML/config payload into SQLite beyond registry metadata.
- No automatic claim/dispatch.
- No claim/worker execution.
- No real Codex launch.
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
