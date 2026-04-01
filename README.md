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

## Prompt path contract v1
- The runner embeds the active task, current run state, and executor report directly into the Codex prompt.
- `project_repo_path` identifies the canonical project repository for context only.
- `executor_worktree_path` and `reviewer_worktree_path` are the only writable workspaces for the real runs.
- Executor writes to `.codex-run/executor-report.md`; reviewer writes to `.codex-run/reviewer-report.md`.
- Host-side runner scripts copy `.codex-run/*.md` into `control/outbox` and immediately sync them into `runtime/runs/<run_id>/outbox`.

## Production n8n workflow export
- Import [`n8n/workflows/control-bridge-run-v1.json`](/home/dkar/workspace/control/n8n/workflows/control-bridge-run-v1.json) into the local n8n instance.
- The workflow is bridge-driven and calls:
  - `POST /prepare-run`
  - `POST /run-executor`
  - conditional `POST /run-reviewer`
  - `POST /finalize-run`
- The workflow expects bridge access at `http://host.docker.internal:8787`.
- Trigger path after activation: `POST /webhook/control-bridge-run-v1`

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
