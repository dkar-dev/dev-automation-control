# Dev Automation Control Repo

This repo is the control plane for orchestration between ChatGPT Web, n8n, Playwright bridge, and Codex executor/reviewer runs.

## Legacy pipeline vs Control Plane v2 scaffold
- The current executable pipeline in this repository is still the legacy single-task flow (`n8n`, `state/current.json`, bridge scripts, and outbox lifecycle below).
- A separate Control Plane v2 scaffold now exists under:
  - `docs/control-plane-v2/`
  - `projects/`
  - `schemas/`
  - `examples/`
- The v2 scaffold now includes storage/persistence utilities, scheduler claim primitives, a bounded manual dispatch adapter, and a bounded single-worker loop v1 for one Linux host process.
- The v2 scaffold now also includes a lightweight SQLite migration path, so schema evolution no longer requires re-initializing the database for every accepted change.
- The v2 scaffold now also includes a bounded runtime cleanup manager v1 for terminal-only retention of artifacts, runtime worktrees, and local runtime branches.
- The v2 scaffold now also includes a bounded task intake / run submission layer v1, so bounded tasks can be submitted as one normalized entrypoint instead of hand-assembling root runs plus runtime context.
- The v2 scaffold now also includes a thin localhost-only HTTP API v1, so `n8n` and other local automations can call intake, worker, manual-control, and cleanup primitives over stable JSON endpoints.
- Migration from legacy pipeline to v2 is not completed yet.
- The first executable v2 utilities now live in:
  - `scripts/validate-project-package`
  - `scripts/init-sqlite-v1`
  - `scripts/migrate-sqlite-v1`
  - `scripts/show-sqlite-schema-version`
  - `scripts/list-sqlite-migrations`
  - `scripts/register-project-package`
  - `scripts/list-registered-projects`
  - `scripts/submit-bounded-task`
  - `scripts/show-submitted-task`
  - `scripts/list-submitted-tasks`
  - `scripts/create-root-run`
  - `scripts/list-runs`
  - `scripts/show-run`
  - `scripts/start-step-run`
  - `scripts/finish-step-run`
  - `scripts/retry-step-run`
  - `scripts/list-step-runs`
  - `scripts/show-step-run`
  - `scripts/complete-reviewer-outcome`
  - `scripts/ingest-reviewer-result`
  - `scripts/list-flow-runs`
  - `scripts/claim-next-run`
  - `scripts/release-claimed-run`
  - `scripts/mark-claimed-run-dispatch-failed`
  - `scripts/dispatch-executor-run`
  - `scripts/dispatch-reviewer-run`
  - `scripts/dispatch-next-for-claimed-run`
  - `scripts/show-dispatch-result`
  - `scripts/run-worker-tick`
  - `scripts/run-worker-until-idle`
  - `scripts/pause-run`
  - `scripts/resume-run`
  - `scripts/force-stop-run`
  - `scripts/rerun-run-step`
  - `scripts/show-run-control-state`
  - `scripts/list-cleanup-candidates`
  - `scripts/run-cleanup-once`
  - `scripts/show-cleanup-status`
  - `scripts/run-control-plane-api`
  - `scripts/show-control-plane-config`
  - `scripts/smoke-control-plane-v2.sh`
  - `scripts/smoke-control-plane-v2-sqlite-migrations.sh`
  - `scripts/smoke-control-plane-v2-dispatch.sh`
  - `scripts/smoke-control-plane-v2-worker.sh`
  - `scripts/smoke-control-plane-v2-manual-control.sh`
  - `scripts/smoke-control-plane-v2-cleanup.sh`
  - `scripts/smoke-control-plane-v2-intake.sh`
  - `scripts/smoke-control-plane-v2-api.sh`
- Operator/dev usage notes for those utilities are in:
  - [`docs/control-plane-v2/bootstrap-and-validation.md`](/home/dkar/workspace/control/docs/control-plane-v2/bootstrap-and-validation.md)
  - [`docs/control-plane-v2/manual-dispatch.md`](/home/dkar/workspace/control/docs/control-plane-v2/manual-dispatch.md)
  - [`docs/control-plane-v2/local-http-api.md`](/home/dkar/workspace/control/docs/control-plane-v2/local-http-api.md)

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
- The v2 manual dispatch adapter reuses those same host-side scripts, but runs them inside an isolated per-dispatch sandbox and disables reviewer semantic auto-completion for the v2 reviewer path.
- After a v2 reviewer dispatch finishes, `scripts/ingest-reviewer-result` can extract the semantic verdict from stored reviewer artifacts and close the v2 outcome chain.
- `scripts/run-worker-tick` and `scripts/run-worker-until-idle` now chain claim -> dispatch -> ingestion on a single host process, but they do not add daemonization or multi-worker fencing.
- `scripts/submit-bounded-task` creates a root run plus persisted submission/runtime-context manifests, so `scripts/run-worker-tick` can pick up the queued run later without a separate `context.json`.
- `scripts/pause-run`, `scripts/resume-run`, `scripts/force-stop-run`, and `scripts/rerun-run-step` provide the bounded v1 manual recovery layer over the same run/queue/step primitives.
- `scripts/list-cleanup-candidates`, `scripts/run-cleanup-once`, and `scripts/show-cleanup-status` provide terminal-only TTL cleanup for runtime artifacts, worktrees, and local runtime branches, while preserving cleanup audit metadata in SQLite.
- `scripts/run-control-plane-api` exposes those same v2 primitives over localhost-only JSON endpoints at `127.0.0.1:8788` by default; it is separate from the legacy bridge on `127.0.0.1:8787`.
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

## Legacy Reviewer Completion Contract
- Reviewer report must begin with these exact machine-readable lines:
  - `Verdict: approved|changes_requested|blocked`
  - `Summary: <one-line summary>`
  - `Commit SHA: <sha or none>`
- After copying `reviewer-report.md` into `control/outbox`, the host-side pipeline runs [`complete-run-from-review.sh`](/home/dkar/workspace/control/scripts/complete-run-from-review.sh).
- `complete-run-from-review.sh` parses the reviewer report, optionally saves `Commit SHA`, and finalizes the run automatically.
- External orchestration no longer needs a separate finalize step after a successful reviewer stage.

## V2 Reviewer Ingestion Bridge
- After a v2 reviewer dispatch completes, run `./scripts/ingest-reviewer-result` against the terminal reviewer `step_run` or the stored `dispatch_result_manifest`.
- The bridge does not reimplement reviewer outcome semantics. It extracts verdict metadata, then delegates terminal state changes and follow-up creation to [`complete_reviewer_outcome`](/home/dkar/workspace/control/control_plane_v2/reviewer_outcome_persistence.py).
- Verdict extraction source priority is:
  - `step_result_json` artifact when present
  - `dispatch_result_manifest.dispatch_outcome.state_result`
  - `step_state_json.result`
  - strict parsing of `step_report` / `reviewer-report.md`
- The v2 report parser is strict:
  - line 1 must be `Verdict: approved|changes_requested|blocked`
  - line 2 must be `Summary: <non-empty summary>`
  - line 3 may be empty or `Commit SHA: <sha|none>`
- If no unambiguous verdict can be extracted, ingestion fails closed with an explicit error and does not mutate the flow.
- `--verdict` is available only as a manual recovery/debug override. It forces the semantic verdict, but still preserves summary and `commit_sha` from the highest-priority readable artifacts when available.

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

## Dispatch Adapter Smoke Test
- Run the focused v2 dispatch smoke:
  ```bash
  cd /home/dkar/workspace/control
  ./scripts/smoke-control-plane-v2-dispatch.sh
  ```
- This smoke uses the real v2 dispatch adapter plus the real legacy executor/reviewer backend scripts, injects a fake `codex`, verifies executor/reviewer `step_run` lifecycle persistence, artifact refs/logs/manifests, approved/blocked/changes_requested ingestion outcomes, malformed reviewer verdict failure, manual override recovery, and the dispatch-failed requeue path for a broken backend launch.

## SQLite Migration Smoke Test
- Run the focused SQLite migration smoke:
  ```bash
  cd /home/dkar/workspace/control
  ./scripts/smoke-control-plane-v2-sqlite-migrations.sh
  ```
- This smoke verifies:
  - fresh `init-sqlite-v1` bootstraps the latest schema snapshot and records the current migration chain
  - an old untracked v1 database upgrades in place through `migrate-sqlite-v1`
  - a second migrate run is idempotent
  - manual-control `paused` state works after migrate
  - the single-worker path still completes on the migrated database
  - invalid migration metadata state fails explicitly

## SQLite Migration Policy
- Fresh DB policy: `init-sqlite-v1` bootstraps the latest snapshot in [`schemas/sqlite-v1.sql`](/home/dkar/workspace/control/schemas/sqlite-v1.sql), then marks all known migrations as applied in `schema_migrations`.
- Existing DB policy: `migrate-sqlite-v1` inspects the current SQLite layout, backfills missing migration metadata for recognized legacy schemas, and then applies only the remaining ordered SQL migrations under [`schemas/migrations/`](/home/dkar/workspace/control/schemas/migrations).
- Current drift coverage includes the `paused` run/queue state introduced by manual control.
- Unsupported in v1:
  - downgrades
  - branching migration histories
  - non-SQLite backends

## Worker Loop Smoke Test
- Run the focused v2 single-worker smoke:
  ```bash
  cd /home/dkar/workspace/control
  ./scripts/smoke-control-plane-v2-worker.sh
  ```
- This smoke uses the same real v2 claim / dispatch / ingestion primitives plus the legacy executor/reviewer backend scripts, injects a fake `codex`, and verifies:
  - one-tick executor -> reviewer -> approved -> completed
  - `changes_requested` follow-up creation and next-tick pickup
  - `blocked` terminal stop
  - explicit `dispatch_failed` and `ingestion_failed` worker summaries
  - bounded `run-worker-until-idle` behavior and worker summary artifacts

## V2 Manual Control v1
- `./scripts/show-run-control-state` shows the current run status, queue status, active step, pause/terminal flags, latest manual transition, latest resume mode, and any pending narrow rerun intent.
- `./scripts/pause-run` is allowed only for:
  - queued runs
  - claimed runs with no active `step_run` yet
- Paused runs are reflected in both `runs.status` and `queue_items.status` as `paused`, and the worker skips them because only `queue_items.status = queued` is claimable.
- `./scripts/resume-run` supports:
  - `--mode normal`: move the paused run back to `queued`
  - `--mode stabilize_to_green`: move the paused run back to `queued` and persist explicit recovery intent metadata for later inspection/history
- `./scripts/force-stop-run` moves the run to terminal `stopped` and the queue item to terminal `cancelled` for queued, claimed, or active runs. It does not perform cleanup side effects and does not attempt a true interrupt of an already running backend process.
- `./scripts/rerun-run-step` is intentionally narrow. It appends rerun intent history for:
  - failed/stopped executor paths before reviewer history exists
  - failed/stopped reviewer paths when the run has not already been completed
- The rerun path does not reset the whole flow. The next worker tick notices the pending rerun intent and uses the existing retry primitive for the matching `step_run`.
- Pausing a run with an active `step_run` is explicitly rejected in v1 as not safe, because there is no backend interrupt/lease protocol yet.

## Manual Control Smoke Test
- Run the focused manual-control smoke:
  ```bash
  cd /home/dkar/workspace/control
  ./scripts/smoke-control-plane-v2-manual-control.sh
  ```
- This smoke verifies:
  - pause queued run -> worker skips it
  - resume paused run -> worker picks it again
  - `resume --mode stabilize_to_green` preserves recovery metadata
  - force-stop queued run -> terminal stop with no cleanup side effects
  - force-stop claimed-not-started run -> terminal stop with no cleanup side effects
  - pause active step path -> explicit `not safe` failure
  - narrow rerun of a failed executor step -> run becomes schedulable again through the existing retry path

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
