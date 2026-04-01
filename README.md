# Dev Automation Control Repo

This repo is the control plane for orchestration between ChatGPT Web, n8n, Playwright bridge, and Codex executor/reviewer runs.

## Single-task v1 contract
- One active task at a time
- Inbox: `inbox/current-task.md`
- State: `state/current.json`
- Executor report: `outbox/executor-report.md`
- Reviewer report: `outbox/reviewer-report.md`
