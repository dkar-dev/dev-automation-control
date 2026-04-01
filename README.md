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
