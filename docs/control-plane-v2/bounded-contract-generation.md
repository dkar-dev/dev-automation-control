# Control Plane v2 Bounded Contract Generation

## Scope
- This is the v1 bounded-contract generation engine for Control Plane v2.
- Its job is to turn approved project policy/templates plus runtime context into machine-readable bounded contracts for the next step.
- It is intentionally not an autonomous planner.

## What The Engine May Generate
- Only approved template-based bounded contracts.
- Only these taxonomy values:
  - `implementation_step`
  - `inspection_step`
  - `recovery_step`
  - `manual_followup_step`
- Only contracts that stay inside the approved workflow, policy, and capability boundaries for the project.
- Only contracts that have the runtime context required by the selected template.

## What The Engine Must Not Generate
- It must not change the architecture contract.
- It must not change policy or workflow semantics.
- It must not invent arbitrary free-form tasks without a matching approved template.
- It must not request or rely on new unapproved capability sections.
- It must not run an LLM-in-the-loop generation step inside the engine.
- Architectural replanning remains human-driven.

## Chosen Template/Policy Storage Model
- Storage model: `project_package_policy_v1`
- Approved templates live in the project package:
  - [`policy.yaml`](/home/dkar/workspace/control/projects/sample-project/policy.yaml)
  - block: `bounded_contract_generation_v1`
- Approved capability boundaries come from:
  - [`capabilities.yaml`](/home/dkar/workspace/control/projects/sample-project/capabilities.yaml)
- This keeps the bounded-contract policy with the registered project package instead of introducing a second control-repo policy source.

## Inputs
- Registered project metadata from SQLite project registry.
- `workflow_id`
- `project_profile`
- Current run / flow / control state when runtime IDs are provided.
- Runtime context from request payload and persisted runtime-context artifacts.
- Optional operator request:
  - requested contract type
  - requested capability sections
  - requested actions

## Template Selection
- If `template_key` is provided, the engine uses it only if it matches the requested `contract_type`.
- Otherwise the engine resolves the default template for the chosen taxonomy value from `policy.yaml.bounded_contract_generation_v1.defaults`.
- `contract_type` may be passed explicitly.
- If it is omitted, the engine infers a safe default from current state tags:
  - recovery tags -> `recovery_step`
  - executor succeeded without reviewer started -> `inspection_step`
  - terminal run -> `manual_followup_step`
  - otherwise -> `implementation_step`

## Validation Boundaries
- Generation fails closed when:
  - the request scope conflicts with `run_id` / `step_run_id`
  - the template does not allow the current workflow or project profile
  - the current run/queue state is outside template boundaries
  - required state tags are missing
  - forbidden state tags are present
  - required runtime context fields are missing
  - the project does not approve the capability sections required by the template
  - the operator asks for capability sections or blocked actions outside the template boundary
  - the rendered contract tries to allow blocked actions such as architecture/policy/workflow changes

## Outputs
- Machine-readable normalized JSON contract artifact.
- Human-readable prompt text artifact for Codex.
- Manifest artifact linking the generated contract to the selected project/run/flow context.
- Append-only SQLite history in `contract_manifests`.
- Runtime-linked artifact refs in `artifact_refs` when generation happens for a concrete run/flow.

## CLI
- [`scripts/list-contract-templates`](/home/dkar/workspace/control/scripts/list-contract-templates)
- [`scripts/generate-bounded-contract`](/home/dkar/workspace/control/scripts/generate-bounded-contract)
- [`scripts/show-bounded-contract`](/home/dkar/workspace/control/scripts/show-bounded-contract)

Example:

```bash
cd /home/dkar/workspace/control
./scripts/generate-bounded-contract \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --contract-type implementation_step \
  --runtime-context-json /tmp/runtime-context.json \
  --json
```

## HTTP API
- `POST /v1/contracts/generate`
- `GET /v1/contracts/{id}`
- The HTTP handlers are thin wrappers over the same engine module used by the CLI.

Example:

```bash
curl -s http://127.0.0.1:8788/v1/contracts/generate \
  -H 'Content-Type: application/json' \
  -d '{
    "run_id": "run_123",
    "contract_type": "implementation_step",
    "runtime_context": {
      "task_text": "Implement the approved bounded task.",
      "project_repo_path": "/repo",
      "executor_worktree_path": "/worktrees/executor",
      "reviewer_worktree_path": "/worktrees/reviewer",
      "instructions_repo_path": "/instructions",
      "instruction_profile": "default"
    }
  }'
```

## Fit With ChatGPT / Codex / n8n
- An operator, ChatGPT flow, or `n8n` can request bounded contract generation through CLI or HTTP.
- The engine returns:
  - normalized JSON for machine consumption
  - prompt text for Codex
  - persisted manifest/artifacts for auditability
- Codex executes only the bounded prompt it receives.
- `n8n` stays a thin orchestration client over the HTTP API.
- Human operators remain responsible for approving or changing architecture, policy, and workflow contracts.

## Smoke Coverage
- [`scripts/smoke-control-plane-v2-contracts.sh`](/home/dkar/workspace/control/scripts/smoke-control-plane-v2-contracts.sh) verifies:
  - implementation-step generation from approved template
  - recovery-step generation from paused context
  - rejection outside template/policy boundaries
  - prompt and JSON artifact consistency
  - CLI/HTTP parity for normalized contracts
  - append-only manifest history and runtime artifact linking
