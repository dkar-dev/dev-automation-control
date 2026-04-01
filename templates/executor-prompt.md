The active task and current run state are embedded in this prompt by the host-side runner.

Treat `project_repo_path` as the canonical project identity.
Run commands and make edits only inside `executor_worktree_path`.
Do not try to read or write `control/inbox`, `control/state`, or `control/outbox` directly from the worktree.
Keep all executor artifacts inside `.codex-run/` only.
- Write the execution report to `.codex-run/executor-report.md`.
- Your final assistant message will be captured by the runner in `.codex-run/executor-last-message.md`.
- Do not write executor artifacts anywhere else.

Perform the implementation task exactly as written.
Run the required verification.

The report must contain:
1. Summary
2. Files changed
3. Commands run
4. Verification results
5. Open issues
6. Recommended next action
