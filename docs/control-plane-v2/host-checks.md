# Control Plane v2 Host Checks Matrix v1

## Scope
- This is the bounded host-side verification layer for Control Plane v2.
- It selects checks by project package, workflow, and project profile.
- It runs checks on one Linux host, persists append-only result manifests, and returns one verdict:
  - `green`
  - `not_green`
  - `blocked`
- It is explicitly not Kubernetes deployment orchestration, a cluster controller, remote fleet execution, or a generic shell automation platform.

## Chosen Config Storage Block
- Checks definitions live in the project package runtime config block:
  - `runtime.yaml.host_checks_v1`
- That keeps host verification with the project runtime package instead of adding a second control-plane policy source.
- If `runtime.yaml.host_checks_v1` is missing and a run requests host checks, the result is `blocked`.

## Supported Taxonomy
- `command_check`
- `http_check`
- `file_check`
- `process_check`

## Config Shape

Example:

```yaml
host_checks_v1:
  checks:
    - id: api-health
      kind: http_check
      enabled: true
      severity: required
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      url: "http://127.0.0.1:{{smoke_http_port}}/health"
      timeout_seconds: 5
      success:
        status_code: 200
        body_contains: ok

    - id: repo-readme
      kind: file_check
      enabled: true
      severity: advisory
      allowed_workflow_ids:
        - build
      allowed_project_profiles:
        - default
      path: "{{project_repo_path}}/README.md"
      timeout_seconds: 5
      success:
        exists: true
        file_type: file
```

Required per check:
- `id`
- `kind`
- `enabled`
- `severity`
- `timeout_seconds`
- `success`
- one selector field by kind:
  - `command`
  - `url`
  - `path`
  - `process_selector`

Optional selection fields:
- `allowed_workflow_ids`
- `allowed_project_profiles`

Runtime placeholders:
- String fields support `{{name}}` substitution from persisted runtime context plus run scope fields such as:
  - `project_key`
  - `project_profile`
  - `workflow_id`
  - `run_id`
  - `flow_id`
  - `step_run_id`
  - `project_repo_path`
  - explicit `runtime_context` overrides passed to the CLI or HTTP API
- `http_check.headers` also supports the same placeholder rendering.
- Secret-backed placeholders should come from `runtime.yaml.runtime_value_refs_v1` through `host_check_context_key`, not from raw secret literals in the project package.

Execution notes:
- `command_check` runs with `project_repo_path` as the working directory when that runtime field exists.
- Relative `file_check.path` values resolve against `project_repo_path`.
- `http_check` is `GET`-only in v1.
- `http_check` may send static or rendered request headers.
- `process_check` matches `process_selector` as a substring against `ps -eo pid=,comm=,args=`.

## Success Criteria
- `command_check`
  - `exit_code`
  - `stdout_contains`
  - `stderr_contains`
- `http_check`
  - `status_code`
  - `body_contains`
- `file_check`
  - `exists`
  - `file_type` = `file|directory|any`
  - `contains_text`
- `process_check`
  - `min_matches`
  - `max_matches`

## Selection Rules
- A check is applicable only when:
  - `enabled: true`
  - `allowed_workflow_ids` is empty or includes the run workflow
  - `allowed_project_profiles` is empty or includes the run project profile
- The CLI and HTTP entrypoints may optionally narrow execution with explicit `check_ids`.

## Verdict Rules
- `green`
  - all required checks passed
  - no check was blocked
- `not_green`
  - at least one required check failed
  - no check was blocked
- `blocked`
  - invalid config
  - unresolved runtime placeholders
  - impossible execution such as unreadable files or missing required local prerequisites

Advisory behavior:
- advisory failures are recorded in the manifest
- advisory failures do not prevent `green`

Timeout behavior:
- timeouts are recorded explicitly in per-check observed results
- a timed-out required check yields `not_green`

## Persistence
- Each execution creates a new append-only row in SQLite table `host_check_runs`.
- Each execution writes a manifest JSON artifact and records it in `artifact_refs` as `host_check_manifest`.
- Output path:
  - run artifact tree when available: `<artifact_root>/<project>/<flow>/<run>/checks/<check_run_id>/manifest.json`
  - fallback: `control/.logs/host-checks/...`
- Prior results are never silently overwritten.

## CLI
- [`scripts/list-host-checks`](/home/dkar/workspace/control/scripts/list-host-checks)
- [`scripts/run-host-checks`](/home/dkar/workspace/control/scripts/run-host-checks)
- [`scripts/show-host-check-results`](/home/dkar/workspace/control/scripts/show-host-check-results)

Examples:

```bash
cd /home/dkar/workspace/control
./scripts/run-host-checks \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --runtime-context-json /tmp/runtime-context.json \
  --json
```

```bash
cd /home/dkar/workspace/control
./scripts/show-host-check-results \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  <run-id> \
  --json
```

## HTTP API
- `POST /v1/checks/run`
- `GET /v1/checks/{run_id}`

Example:

```bash
curl -s http://127.0.0.1:8788/v1/checks/run \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "<run-id>",
    "runtime_context": {
      "project_repo_path": "/home/dkar/workspace/projects/demo"
    }
  }'
```

## Reviewer / Worker Integration
- Reviewer approval stays separate.
- v1 does not merge host checks into reviewer semantics.
- The explicit v1 path is:
  1. reviewer-approved path completes
  2. host checks run on the host
  3. run the formal deployable-green decision gate
- If that requires a manual or provisional gate in the surrounding automation, keep it explicit. Do not hide it inside reviewer outcome persistence.

## Out Of Scope For v1
- Kubernetes deployment control
- real cluster orchestration
- remote fleet execution
- complex secret distribution
- turning checks into arbitrary workflow automation

## Smoke Coverage
- [`scripts/smoke-control-plane-v2-host-checks.sh`](/home/dkar/workspace/control/scripts/smoke-control-plane-v2-host-checks.sh) verifies:
  - required command check pass
  - required command check fail
  - advisory failure does not block green
  - explicit timeout handling
  - CLI and HTTP visibility over persisted results
