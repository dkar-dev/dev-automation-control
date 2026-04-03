# Dev Automation Control Repo

This repo is the control plane for orchestration between ChatGPT Web, n8n, Playwright bridge, and Codex executor/reviewer runs.

## Legacy pipeline vs Control Plane v2 scaffold
- The current executable pipeline in this repository is still the legacy single-task flow (`n8n`, `state/current.json`, bridge scripts, and outbox lifecycle below).
- A separate Control Plane v2 scaffold now exists under:
  - `docs/control-plane-v2/`
  - `projects/`
  - `schemas/`
  - `examples/`
- The v2 scaffold now includes storage/persistence utilities and scheduler claim primitives, but full runtime implementation is still intentionally not included.
- Migration from legacy pipeline to v2 is not completed yet.
- The first executable v2 utilities now live in:
  - `scripts/validate-project-package`
  - `scripts/init-sqlite-v1`
  - `scripts/register-project-package`
  - `scripts/list-registered-projects`
  - `scripts/create-root-run`
  - `scripts/list-runs`
  - `scripts/show-run`
  - `scripts/start-step-run`
  - `scripts/finish-step-run`
  - `scripts/retry-step-run`
  - `scripts/list-step-runs`
  - `scripts/show-step-run`
  - `scripts/complete-reviewer-outcome`
  - `scripts/list-flow-runs`
  - `scripts/claim-next-run`
  - `scripts/release-claimed-run`
  - `scripts/mark-claimed-run-dispatch-failed`
  - `scripts/smoke-control-plane-v2.sh`
- Operator/dev usage notes for those utilities are in [`docs/control-plane-v2/bootstrap-and-validation.md`](/home/dkar/workspace/control/docs/control-plane-v2/bootstrap-and-validation.md).

## Single-task v1 contract
- One active task at a time
- Inbox: `inbox/current-task.md`
- State: `state/current.json`
- Executor report: `outbox/executor-report.md`
- Reviewer report: `outbox/reviewer-report.md`

## Bridge contract for real runs
- Real executor/reviewer runs are started by the host-side HTTP bridge, not by `Execute Command` inside the n8n Docker container.
- n8n should call `http://host.docker.internal:8787` with `HTTP Request` nodes.
- The bridge runs `control/scripts/run-executor.sh` and `control/scripts/run-reviewer.sh` on the host/WSL side, where Codex, worktrees, runtime, and project paths actually exist.
- n8n should send symbolic instruction selectors only: `instruction_profile`, `instruction_overlays`, and `instructions_repo_path`.
- The control layer resolves instruction files on the host, records the repo revision and exact files used, then builds the final executor/reviewer prompts locally.
- `GET /current-run` now exposes `instruction_profile`, `instruction_overlays`, `instructions_repo_path`, `instructions_revision`, and `resolved_instruction_files`.
- Existing stub scripts `control/scripts/run-executor-stub.sh` and `control/scripts/run-reviewer-stub.sh` remain available only for local smoke tests that simulate old `Execute Command` behavior.

## Instructions Repo Contract
Supported structure:
```text
profiles/<profile>/shared.md
profiles/<profile>/executor.md
profiles/<profile>/reviewer.md
overlays/<name>.md
overlays/<name>/shared.md
overlays/<name>/executor.md
overlays/<name>/reviewer.md
```
- Required profile files:
  - `profiles/<profile>/executor.md`
  - `profiles/<profile>/reviewer.md`
- Optional profile file:
  - `profiles/<profile>/shared.md`
- Overlay forms:
  - flat overlay file: `overlays/<name>.md`
  - directory overlay files: `overlays/<name>/shared.md`, `overlays/<name>/executor.md`, `overlays/<name>/reviewer.md`
- Requested overlays are applied strictly in the order provided by `instruction_overlays`.
- Resolution order for a role is:
  - `profiles/<profile>/shared.md`
  - `profiles/<profile>/<role>.md`
  - for each overlay in request order:
    - `overlays/<name>.md`
    - `overlays/<name>/shared.md`
    - `overlays/<name>/<role>.md`
- For overlays, at least one supported file must exist. Missing overlay content is a validation error.
- What n8n sends:
  - `instruction_profile`
  - `instruction_overlays`
  - `instructions_repo_path`
- What control resolves locally:
  - prompt assembly for executor and reviewer
  - `instructions_revision`
  - `resolved_instruction_files`
- What is persisted into run state and `GET /current-run`:
  - selectors from input: `instruction_profile`, `instruction_overlays`, `instructions_repo_path`
  - resolved traceability: `instructions_revision`, `resolved_instruction_files`
- `resolved_instruction_files` in state/current-run is the de-duplicated union of files resolved so far for the current run.
- `runtime/runs/<run_id>/resolved-executor-instructions.json` and `runtime/runs/<run_id>/resolved-reviewer-instructions.json` keep the role-specific manifests used to build each prompt.

## Relevant bridge endpoints
- `GET /healthz`
- `GET /current-run`
- `POST /prepare-run`
- `POST /mark-running`
- `POST /run-executor`
- `POST /run-reviewer`
- `POST /outbox/update`
- `POST /sync-outbox`
- `POST /set-commit-sha`
- `POST /finalize-run`

## Prompt path contract v2
- The runner resolves instruction selectors on the host, then embeds the active task, current run state, executor report, and resolved instruction content directly into the Codex prompt.
- `project_repo_path` identifies the canonical project repository for context only.
- `executor_worktree_path` and `reviewer_worktree_path` are the only writable workspaces for the real runs.
- Executor artifacts stay under `.codex-run/`, including `.codex-run/executor-report.md` and `.codex-run/executor-last-message.md`.
- Reviewer writes to `.codex-run/reviewer-report.md`.
- Host-side runner scripts copy `.codex-run/*.md` into `control/outbox` and immediately sync them into `runtime/runs/<run_id>/outbox`.
- `resolve-instructions.sh` records `instructions_revision` and `resolved_instruction_files` in run state for traceability, and `build-executor-prompt.sh` / `build-reviewer-prompt.sh` assemble the final prompts on the host.
- After a successful executor run, `run-executor.sh` stages project changes in the executor worktree, creates a handoff commit, and saves that commit to `result.commit_sha`.
- Before reviewer Codex starts, `run-reviewer.sh` requires `result.commit_sha`, hard-resets the reviewer worktree to that commit, and cleans untracked files so review always starts from the executor snapshot.

## Reviewer completion contract
- Reviewer report must begin with these exact machine-readable lines:
  - `Verdict: approved|changes_requested|blocked`
  - `Summary: <one-line summary>`
  - `Commit SHA: <sha or none>`
- After copying `reviewer-report.md` into `control/outbox`, the host-side pipeline runs [`complete-run-from-review.sh`](/home/dkar/workspace/control/scripts/complete-run-from-review.sh).
- `complete-run-from-review.sh` parses the reviewer report, optionally saves `Commit SHA`, and finalizes the run automatically.
- External orchestration no longer needs a separate finalize step after a successful reviewer stage.

## Bridge lifecycle
- Normal control-side lifecycle is now:
  - `POST /prepare-run`
  - `POST /run-executor`
  - `POST /run-reviewer`
  - `GET /current-run`
- Status transitions for executor+reviewer runs are `queued -> executor_running -> executor_done -> reviewer_running -> completed|failed`.
- `POST /finalize-run` is still available for compatibility and manual intervention, but host-side reviewer completion no longer depends on it.
- Existing n8n workflow export in the repo is intentionally not changed in this task.

## Local startup path
1. Start the host-side bridge:
   ```bash
   cd /home/dkar/workspace/control
   ./scripts/run-bridge.sh
   ```
2. Verify the bridge:
   ```bash
   curl -s http://127.0.0.1:8787/healthz
   ```
3. Start local n8n:
   ```bash
   cd /home/dkar/workspace/runtime/services/n8n
   docker compose up -d
   ```
4. Open `http://127.0.0.1:5678`, import [`n8n/workflows/control-bridge-run-v1.json`](/home/dkar/workspace/control/n8n/workflows/control-bridge-run-v1.json), then activate the workflow.
5. Trigger the workflow locally:
   ```bash
   curl -s -X POST http://127.0.0.1:5678/webhook/control-bridge-run-v1 \
     -H 'Content-Type: application/json' \
     -d '{
       "project": "mcp_clickup",
       "task_text": "Run the local control pipeline through n8n.",
       "mode": "executor+reviewer",
       "branch_base": "main",
       "auto_commit": false,
       "source": "n8n",
       "thread_label": "mcp-clickup-dev",
       "instruction_profile": "default",
       "instruction_overlays": ["docs-only", "strict-review"],
       "instructions_repo_path": "/home/dkar/workspace/instructions"
     }'
   ```

## Smoke test
- Run the isolated e2e smoke script:
  ```bash
  cd /home/dkar/workspace/control
  ./scripts/smoke-bridge-e2e.sh
  ```
- The smoke script does not use the real Codex binary. It runs against a temporary copy of `control`, starts a temporary bridge, injects a fake `codex`, and validates both success and HTTP 500 failure paths.
- It specifically verifies the commit-based handoff: executor changes are committed, `result.commit_sha` is persisted, reviewer resets to that commit, and reviewer completion still auto-finalizes the run.

## Instruction Smoke Test
- Validate a concrete instructions repo shape directly:
  ```bash
  cd /home/dkar/workspace/control
  ./scripts/validate-instructions-repo.sh ./fixtures/instructions-repo default docs-only strict-review
  ```
- Run the dedicated instruction-resolution smoke test:
  ```bash
  cd /home/dkar/workspace/control
  ./scripts/smoke-instructions-pipeline.sh
  ```
- The dedicated smoke test uses the deterministic fixture repo under [`fixtures/instructions-repo`](/home/dkar/workspace/control/fixtures/instructions-repo/profiles/default/shared.md), initializes it as a temporary git repo, prepares a run with selectors only, resolves instructions for both roles, builds both prompt files, and verifies the exported instruction metadata.

## Manual smoke test
1. Start the bridge:
   ```bash
   cd /home/dkar/workspace/control
   ./scripts/run-bridge.sh
   ```
2. Prepare a run:
   ```bash
   curl -s -X POST http://127.0.0.1:8787/prepare-run \
     -H 'Content-Type: application/json' \
     -d '{
       "project": "mcp_clickup",
       "task_text": "Manual complete-run-from-review smoke test.",
       "mode": "executor+reviewer",
       "branch_base": "main",
       "auto_commit": false,
       "source": "manual-smoke",
       "thread_label": "control-v2-smoke"
     }'
   ```
3. Place fake executor and reviewer artifacts in `control/outbox`:
   ```bash
   cat > /home/dkar/workspace/control/outbox/executor-report.md <<'EOF'
   # Executor Report

   ## Summary
   Synthetic executor artifact
   EOF

   cat > /home/dkar/workspace/control/outbox/reviewer-report.md <<'EOF'
   Verdict: approved
   Summary: reviewer approved the manual smoke run
   Commit SHA: deadbeef

   ## Defects found
   - none

   ## Verification performed
   - manual smoke

   ## Risk assessment
   - low

   ## Required fixes
   - none

   ## Recommended next action
   - done
   EOF
   ```
4. Complete the run from the reviewer report:
   ```bash
   /home/dkar/workspace/control/scripts/complete-run-from-review.sh
   ```
5. Inspect the final state:
   ```bash
   curl -s http://127.0.0.1:8787/current-run | python3 -m json.tool
   ```
6. Verify these fields in the response:
   - `data.status = completed`
   - `data.result.verdict = approved`
   - `data.result.summary = reviewer approved the manual smoke run`
   - `data.result.commit_sha = deadbeef`
