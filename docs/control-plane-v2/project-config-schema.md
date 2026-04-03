# Project Package Config Schema (v2 scaffold)

## Scope
- This document defines the current minimal contract for a project package in the Control Plane v2 scaffold.
- It intentionally avoids runtime-specific behavior and fields not approved yet.

## Package location and structure
- Each package lives under: `projects/<project-name>/`
- Required files in every package:
  - `project.yaml`
  - `workflow.yaml`
  - `policy.yaml`
  - `runtime.yaml`
  - `instructions.yaml`
  - `capabilities.yaml`

## Required YAML files and current contract

| File | Purpose | Required fields (current) | Empty sections allowed |
|---|---|---|---|
| `project.yaml` | Package-level metadata and contract version | `schema_version` | Not applicable |
| `workflow.yaml` | Workflow contract placeholder | none yet | Yes |
| `policy.yaml` | Policy contract placeholder plus optional cleanup retention policy | none yet | Yes |
| `runtime.yaml` | Runtime contract placeholder plus optional bounded-task intake defaults | none yet | Yes |
| `instructions.yaml` | Instruction contract placeholder plus optional bounded-task instruction defaults | none yet | Yes |
| `capabilities.yaml` | Capability declarations | `sections` (mapping) | Yes, `sections: {}` is valid |

## `schema_version` location
- `schema_version` must be present at the root of `project.yaml`.
- In this scaffold step, `schema_version` is package-level and is not duplicated across every YAML file.
- Version format policy is not finalized yet (see `OPEN_ISSUE`).

## What can stay empty in this step
- `workflow.yaml`, `runtime.yaml`, and `instructions.yaml` may be empty mappings (`{}`).
- `policy.yaml` may still be an empty mapping (`{}`), but it now also supports an optional `cleanup_v1` block.
- `capabilities.yaml` may contain empty capability sections through `sections: {}`.
- Empty files are not allowed; files must still be valid YAML documents.

## Optional `policy.yaml.cleanup_v1`

Current optional v1 retention block:

```yaml
cleanup_v1:
  artifacts_ttl_seconds: 86400
  worktree_ttl_seconds: 604800
  branch_ttl_seconds: 604800
```

- All fields are optional.
- If `cleanup_v1` is omitted, runtime defaults apply.
- TTL values are currently expressed in seconds.
- This block is consumed only by the bounded cleanup manager v1; it is not yet a broader policy engine contract.

## Optional `runtime.yaml.bounded_task_runtime_v1`

Current optional bounded task intake runtime block:

```yaml
bounded_task_runtime_v1:
  workspace_root: /home/example/workspace
  project_repo_path: /home/example/workspace/projects/sample-project
  executor_worktree_path: /home/example/workspace/runtime/worktrees/sample-project-executor
  reviewer_worktree_path: /home/example/workspace/runtime/worktrees/sample-project-reviewer
  instructions_repo_path: /home/example/workspace/instructions
  artifact_root: /home/example/workspace/control-artifacts
  branch_base: main
  mode: executor+reviewer
  auto_commit: false
  source: intake-default
  thread_label: sample-project-default
```

- All fields are optional.
- `workspace_root` can be used instead of explicit path fields; the intake layer derives:
  - `projects/<project_key>`
  - `runtime/worktrees/<project_key>-executor`
  - `runtime/worktrees/<project_key>-reviewer`
  - `instructions`
- If neither explicit paths nor `workspace_root` are available, bounded-task submission fails explicitly.

## Optional `instructions.yaml.bounded_task_intake_v1`

Current optional bounded task instruction-default block:

```yaml
bounded_task_intake_v1:
  instruction_profile: default
  instruction_overlays:
    - docs-only
```

- `instruction_profile` becomes the default profile for `submit-bounded-task`.
- `instruction_overlays` becomes the default overlay list unless submission overrides it.

## Hard validation errors (current)
- Project package directory missing under `projects/`.
- Any required YAML file is missing.
- Any required YAML file is not valid YAML.
- `project.yaml` does not contain root-level `schema_version`.
- `capabilities.yaml` does not contain root-level `sections`.
- `sections` in `capabilities.yaml` is not a mapping/object.
- Any required YAML root document is not a mapping/object.

## Future-ready (non-blocking in this step)
- Additional mandatory fields per file.
- Cross-file validation rules.
- Strict semantic validation of capabilities taxonomy.

## OPEN_ISSUE / TODO
- TODO(OPEN_ISSUE): Approve `schema_version` format and compatibility policy.
- TODO(OPEN_ISSUE): Approve minimal mandatory keys for `workflow.yaml`.
- TODO(OPEN_ISSUE): Decide whether `cleanup_v1` remains optional or becomes part of a broader required policy contract.
- TODO(OPEN_ISSUE): Decide whether bounded-task intake config blocks stay optional or graduate into a stricter project runtime contract.
- TODO(OPEN_ISSUE): Approve minimal mandatory keys for `runtime.yaml`.
- TODO(OPEN_ISSUE): Approve minimal mandatory keys for `instructions.yaml`.
- TODO(OPEN_ISSUE): Approve canonical capabilities sections list.
