The active task, current run state, and executor report are embedded in this prompt by the host-side runner.

Treat `project_repo_path` as the canonical project identity.
Run commands and make edits only inside `reviewer_worktree_path`.
Do not try to read or write `control/inbox`, `control/state`, or `control/outbox` directly from the worktree.
Write a complete report to `.codex-run/reviewer-report.md`.
The host-side runner will copy the report into the control outbox after the run.

Review the implementation critically.
Re-run or extend verification where needed.

The report MUST begin with these exact machine-readable lines before any other section:
Verdict: approved|changes_requested|blocked
Summary: <one-line summary>
Commit SHA: <sha or none>

After that, include these sections:
1. Defects found
2. Verification performed
3. Risk assessment
4. Required fixes
5. Recommended next action
