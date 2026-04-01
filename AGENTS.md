# Control repo rules

## Purpose
This repository is the control plane for automated Codex runs.
It does not contain product code.
It contains task instructions, orchestration state, and execution reports.

## Rules
- Treat `inbox/current-task.md` as the single source of truth for the active task.
- Treat `state/current.json` as the machine-readable state.
- Write executor output only to `outbox/executor-report.md`.
- Write reviewer output only to `outbox/reviewer-report.md`.
- Never modify files outside this repository unless the active task explicitly points to a project repository path.
- If task fields are missing or inconsistent, fail closed and write the reason to the corresponding report file.
