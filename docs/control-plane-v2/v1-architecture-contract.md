# Dev Automation Control Plane v1 Architecture Contract

## Status
- Accepted baseline for Control Plane v2 scaffolding work.

## Intent
- Capture already approved v1 boundaries so new v2 artifacts can be prepared without changing legacy runtime behavior.

## Approved v1 boundaries
- `v1` is a multiproject orchestrator contract.
- Source of truth for project config is this dedicated control repo.
- Project config format is YAML.
- Each project is represented as a strict directory-based package.
- Mandatory files in every project package:
  - `project.yaml`
  - `workflow.yaml`
  - `policy.yaml`
  - `runtime.yaml`
  - `instructions.yaml`
  - `capabilities.yaml`
- Each project package must contain `schema_version`.
- `capabilities.yaml` is mandatory even when capability sections are empty.

## Explicitly out of scope in this step
- No SQLite runtime implementation.
- No scheduler implementation.
- No queue implementation.
- No rewrite of legacy control scripts.
- No functional changes to:
  - `n8n/`
  - `state/current.json`
  - legacy single-run pipeline behavior

## Legacy and v2 separation rule
- Legacy pipeline remains the active executable path.
- v2 work in this step is scaffold-only and lives separately from legacy runtime logic.
- Separation is documentary and structural only; migration is not executed.

## Future-ready direction (not implemented here)
- Runtime v1 is expected to become SQLite-backed in a future step.
- The accepted storage and queue contract for that direction is defined in `docs/control-plane-v2/storage-and-queue.md`.
- The matching SQLite schema skeleton is defined in `schemas/sqlite-v1.sql`.
- Machine validation for project packages is expected to be added incrementally.
- Migration plan from legacy pipeline to v2 control plane is expected in a separate ADR/task.

## OPEN_ISSUE / TODO
- TODO(OPEN_ISSUE): Define canonical `schema_version` format (`string` pattern and versioning policy).
- TODO(OPEN_ISSUE): Define mandatory semantic keys for each YAML file beyond file presence.
- TODO(OPEN_ISSUE): Define canonical capabilities section taxonomy (section names and semantics).
