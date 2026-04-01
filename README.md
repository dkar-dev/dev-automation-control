# Dev Automation Control Repo

This repo is the control plane for orchestration between ChatGPT Web, n8n, Playwright bridge, and Codex executor/reviewer runs.

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
- Existing stub scripts `control/scripts/run-executor-stub.sh` and `control/scripts/run-reviewer-stub.sh` remain available only for local smoke tests that simulate old `Execute Command` behavior.

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
- The runner embeds the active task, current run state, and executor report directly into the Codex prompt.
- `project_repo_path` identifies the canonical project repository for context only.
- `executor_worktree_path` and `reviewer_worktree_path` are the only writable workspaces for the real runs.
- Executor artifacts stay under `.codex-run/`, including `.codex-run/executor-report.md` and `.codex-run/executor-last-message.md`.
- Reviewer writes to `.codex-run/reviewer-report.md`.
- Host-side runner scripts copy `.codex-run/*.md` into `control/outbox` and immediately sync them into `runtime/runs/<run_id>/outbox`.

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
       "thread_label": "mcp-clickup-dev"
     }'
   ```

## Smoke test
- Run the isolated e2e smoke script:
  ```bash
  cd /home/dkar/workspace/control
  ./scripts/smoke-bridge-e2e.sh
  ```
- The smoke script does not use the real Codex binary. It runs against a temporary copy of `control`, starts a temporary bridge, injects a fake `codex`, and validates both success and HTTP 500 failure paths.
- It specifically verifies that `run-reviewer` now auto-finalizes the run and persists `Commit SHA`.

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
