# SQLite v1 Schema Notes

## Why SQLite is chosen for v1
- v1 runtime is a single scheduler/worker process on one Linux machine.
- SQLite matches that operational model with minimal moving parts.
- It keeps the first storage-backed control plane simple to inspect, back up, and recover.
- It aligns with the existing v1 storage backend direction already approved for this control plane.
- It lets the storage contract be fixed now without introducing a separate database service before runtime behavior is stabilized.

## What lives where
- SQLite stores active state, append-only history, snapshots, and artifact metadata.
- Filesystem stores heavy artifacts under the project / flow / run / step_run path layout.
- The relational schema is the source of truth for linkage and queue state; the filesystem is the source of truth for large content blobs.

## Why the schema is future-ready for Postgres
- Entity ids are opaque text ids instead of SQLite-specific integer row ids.
- Cross-entity relationships are explicit through foreign keys and normalized tables.
- JSON-like payloads are stored as text and can later move to `JSONB`.
- Queue ordering relies on portable columns (`priority_class`, timestamps, status) rather than SQLite-only behavior.
- Artifact storage is externalized to filesystem references, so relational backend migration does not need to move large blobs into the database.

## Deliberately not implemented now
- No scheduler.
- No worker loop.
- No runtime execution code.
- No SQL migrations framework.
- No manual operations API.
- No separate active `flows` table.
- No exact in-schema aging formula for queue fairness inside a class.
- No frozen taxonomy yet for `origin_type` or full `step_key` values beyond the current executor/reviewer-oriented use case.

## Notes on the current schema skeleton
- `projects` is a registry/import table for validated control-repo packages; it stores only `project_key`, `package_root`, and timestamps.
- The project YAML package remains the primary source of truth outside SQLite; the registry row is linkage metadata only.
- The current executable scaffold also writes root run rows into `runs`, one linked queue row into `queue_items`, and initial append-only rows into `state_transitions`.
- The current executable scaffold also writes `step_runs` rows plus append-only `state_transitions` for step start, finish, and retry events.
- `runs`, `step_runs`, and `queue_items` hold current mutable active state.
- `state_transitions`, `run_snapshots`, and `artifact_refs` are append-only records.
- Flow linkage lives on `runs` via `flow_id`, `parent_run_id`, `origin_type`, `origin_run_id`, and `origin_step_run_id`.
- Retry lineage lives on `step_runs` via `attempt_no` and `previous_step_run_id`.
- Guardrails and hard stops are not implemented in SQL; the schema only stores the data needed to enforce them later.

## OPEN_ISSUE / TODO
- TODO(OPEN_ISSUE): Freeze the canonical id format before first runtime implementation.
- TODO(OPEN_ISSUE): Freeze the exact counting semantics for `max_run_attempts`.
- TODO(OPEN_ISSUE): Freeze the queue aging formula used inside one class.
