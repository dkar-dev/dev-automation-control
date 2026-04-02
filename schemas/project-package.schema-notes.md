# Project Package Schema Notes (skeleton)

## Purpose
- Machine-readable schema targets planning notes for `projects/<project-name>/`.
- This file is a scaffold for future validators and does not define a full validator implementation.

## Validation target: package-level
- Required directory shape: `projects/<project-name>/`.
- Required files:
  - `project.yaml`
  - `workflow.yaml`
  - `policy.yaml`
  - `runtime.yaml`
  - `instructions.yaml`
  - `capabilities.yaml`
- All required files must parse as YAML mapping documents.

## Validation target: `project.yaml`
- Required key: `schema_version`.
- Target type: scalar string.
- Future target: version pattern and compatibility matrix.

## Validation target: `workflow.yaml`
- Current target: file exists and root is mapping.
- Future target: required workflow contract keys.

## Validation target: `policy.yaml`
- Current target: file exists and root is mapping.
- Future target: required policy contract keys.

## Validation target: `runtime.yaml`
- Current target: file exists and root is mapping.
- Future target: runtime mode/profile keys (without implementation coupling).

## Validation target: `instructions.yaml`
- Current target: file exists and root is mapping.
- Future target: instruction source/selection contract keys.

## Validation target: `capabilities.yaml`
- Required key: `sections`.
- `sections` type target: mapping.
- Empty mapping allowed: `sections: {}`.
- Future target: canonical section names and section-level schema.

## Cross-file validation targets (future)
- Consistent schema compatibility by `schema_version`.
- Cross-file reference integrity (if references are introduced).
- Forward/backward compatibility policy checks.

## OPEN_ISSUE / TODO
- TODO(OPEN_ISSUE): Confirm whether additional global metadata file is needed.
- TODO(OPEN_ISSUE): Define canonical error codes/messages for validator output.
- TODO(OPEN_ISSUE): Decide schema publication format (JSON Schema, CUE, or custom).
