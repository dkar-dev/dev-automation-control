# Control Plane v2 Bootstrap, Validation, and Project Registry Utilities

## Scope
- This step adds the first executable infrastructure layer for the v2 scaffold only.
- It provides strict project package validation, SQLite schema bootstrap/init, and project registry/import utilities.
- It does not implement scheduler behavior, worker/runtime execution, or run creation.

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

## Boundaries of this step
- No scheduler.
- No worker loop.
- No runtime execution.
- No queue execution.
- No project import of YAML/config payload into SQLite beyond registry metadata.
- No runtime run creation.
- No legacy pipeline behavior changes.

## OPEN_ISSUE / TODO
- TODO(OPEN_ISSUE): freeze canonical `schema_version` format/policy beyond string type.
- TODO(OPEN_ISSUE): freeze mandatory semantic keys for non-`project.yaml` files.
- TODO(OPEN_ISSUE): freeze canonical capabilities section taxonomy.
- TODO(OPEN_ISSUE): freeze canonical project id format; current registry insert uses generated UUIDv4 text.
- TODO(OPEN_ISSUE): decide long-term home for v2 executable code if a larger Python package layout is introduced later.
