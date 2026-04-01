#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_cmd codex
require_cmd python3
require_cmd awk

PROJECT_REPO="$(task_get project_repo_path)"
WORKTREE="$(task_get reviewer_worktree_path)"
RUN_DIR="$(prepare_run_dir "$WORKTREE")"

ensure_file "$TASK_FILE"
ensure_file "$OUTBOX_DIR/executor-report.md"
ensure_dir "$PROJECT_REPO"
ensure_dir "$WORKTREE"

PROMPT_FILE="$RUN_DIR/reviewer-prompt.md"
LOCAL_INPUT_EXECUTOR_REPORT="$RUN_DIR/executor-report.input.md"
LOCAL_REPORT="$RUN_DIR/reviewer-report.md"
LOCAL_LAST_MESSAGE="$RUN_DIR/reviewer-last-message.md"

cp "$OUTBOX_DIR/executor-report.md" "$LOCAL_INPUT_EXECUTOR_REPORT"
rm -f "$LOCAL_REPORT" "$LOCAL_LAST_MESSAGE"

cat > "$PROMPT_FILE" <<PROMPT
You are the reviewer in an automated local pipeline.

Workspace root: $WORKTREE
Project repo root: $PROJECT_REPO

Rules:
- Work only inside the current workspace.
- Review critically. Do not trust the executor report.
- Re-run or extend verification where needed.
- Write the review report to: .codex-run/reviewer-report.md
- Your final assistant message must be short and include the report path.

Reviewer instructions template:
$(cat "$CONTROL_DIR/templates/reviewer-prompt.md")

Executor report:
$(cat "$LOCAL_INPUT_EXECUTOR_REPORT")

Active task:
$(cat "$TASK_FILE")
PROMPT

"$SCRIPT_DIR/mark-running.sh" reviewer >/dev/null
state_set "reviewer_running" "" "await_reviewer_result"

set +e
codex exec \
  -C "$WORKTREE" \
  -s workspace-write \
  -c 'approval_policy="never"' \
  --output-last-message "$LOCAL_LAST_MESSAGE" \
  - < "$PROMPT_FILE"
rc=$?
set -e

if [ "$rc" -ne 0 ]; then
  state_set "failed" "reviewer failed with exit code $rc" "investigate_reviewer"
  "$SCRIPT_DIR/sync-outbox.sh" >/dev/null
  exit $rc
fi

if [ ! -f "$LOCAL_REPORT" ]; then
  {
    echo "# Reviewer report missing"
    echo
    echo "Codex finished without producing .codex-run/reviewer-report.md"
    echo
    if [ -f "$LOCAL_LAST_MESSAGE" ]; then
      echo "## Final message"
      echo
      cat "$LOCAL_LAST_MESSAGE"
    fi
  } > "$OUTBOX_DIR/reviewer-report.md"
  state_set "failed" "reviewer report was not produced" "fix_reviewer_prompt_or_runner"
  "$SCRIPT_DIR/sync-outbox.sh" >/dev/null
  exit 1
fi

cp "$LOCAL_REPORT" "$OUTBOX_DIR/reviewer-report.md"
[ -f "$LOCAL_LAST_MESSAGE" ] && cp "$LOCAL_LAST_MESSAGE" "$OUTBOX_DIR/reviewer-last-message.md" || true

state_set "completed" "" "await_next_task"
"$SCRIPT_DIR/sync-outbox.sh" >/dev/null
