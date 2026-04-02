#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_cmd codex
require_cmd git
require_cmd python3
require_cmd awk

RUN_ID="$(task_get run_id)"
BRANCH_BASE="$(task_get branch_base)"
PROJECT_REPO="$(task_get project_repo_path)"
WORKTREE="$(task_get executor_worktree_path)"

ensure_file "$TASK_FILE"
ensure_dir "$PROJECT_REPO"
ensure_dir "$WORKTREE"

executor_fail() {
  local exit_code="$1"
  local message="$2"
  local next_action="${3:-investigate_executor}"

  state_set "failed" "$message" "$next_action"
  "$SCRIPT_DIR/sync-outbox.sh" >/dev/null || true
  echo "$message" >&2
  exit "$exit_code"
}

"$SCRIPT_DIR/mark-running.sh" executor >/dev/null
state_set "executor_running" "" "await_executor_result"

if ! git -C "$WORKTREE" reset --hard "$BRANCH_BASE" >/dev/null; then
  executor_fail 1 "failed to reset executor worktree to branch_base $BRANCH_BASE" "fix_executor_worktree_sync"
fi

if ! git -C "$WORKTREE" clean -fdx >/dev/null; then
  executor_fail 1 "failed to clean executor worktree before execution" "fix_executor_worktree_sync"
fi

RUN_DIR="$(prepare_run_dir "$WORKTREE")"
PROMPT_FILE="$RUN_DIR/executor-prompt.md"
LOCAL_REPORT="$RUN_DIR/executor-report.md"
LOCAL_LAST_MESSAGE="$RUN_DIR/executor-last-message.md"

rm -f "$LOCAL_REPORT" "$LOCAL_LAST_MESSAGE"

{
  cat <<EOF
You are the executor in an automated local pipeline.

Workspace root: $WORKTREE
Project repo root: $PROJECT_REPO

Path contract:
- project_repo_path is the canonical project path for repo identity and context.
- executor_worktree_path is the only workspace where you may run commands and make edits.
- Control repo files are not available for direct access from this workspace. The task and state snapshot are embedded below.

Rules:
- Work only inside the executor worktree.
- Do not change scope.
- Implement only what the active task requires.
- Run the minimum relevant verification.
- Write the execution report to: .codex-run/executor-report.md
- Your final assistant message must be short and include the report path.

Executor instructions template:
EOF
  cat "$CONTROL_DIR/templates/executor-prompt.md"
  cat <<'EOF'

Current run state snapshot:
EOF
  cat "$STATE_FILE"
  cat <<'EOF'

Active task:
EOF
  cat "$TASK_FILE"
} > "$PROMPT_FILE"

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
  executor_fail "$rc" "executor failed with exit code $rc" "investigate_executor"
fi

if [ ! -f "$LOCAL_REPORT" ]; then
  {
    echo "# Executor report missing"
    echo
    echo "Codex finished without producing .codex-run/executor-report.md"
    echo
    if [ -f "$LOCAL_LAST_MESSAGE" ]; then
      echo "## Final message"
      echo
      cat "$LOCAL_LAST_MESSAGE"
    fi
  } > "$OUTBOX_DIR/executor-report.md"
  executor_fail 1 "executor report was not produced" "fix_executor_prompt_or_runner"
fi

cp "$LOCAL_REPORT" "$OUTBOX_DIR/executor-report.md"
[ -f "$LOCAL_LAST_MESSAGE" ] && cp "$LOCAL_LAST_MESSAGE" "$OUTBOX_DIR/executor-last-message.md" || true

mapfile -d '' -t CHANGED_FILES < <(git -C "$WORKTREE" ls-files -z -m -o --exclude-standard)

if [[ "${#CHANGED_FILES[@]}" -eq 0 ]]; then
  executor_fail 1 "executor finished successfully but produced no task-scoped project changes to commit" "fix_executor_scope_or_task"
fi

if ! git -C "$WORKTREE" add -- "${CHANGED_FILES[@]}"; then
  executor_fail 1 "failed to stage executor project changes for handoff commit" "fix_executor_handoff_git"
fi

if git -C "$WORKTREE" diff --cached --quiet --exit-code; then
  executor_fail 1 "executor finished successfully but staged no task-scoped project changes to commit" "fix_executor_scope_or_task"
fi

if ! git -C "$WORKTREE" commit -m "Executor handoff: $RUN_ID" >/dev/null; then
  executor_fail 1 "failed to create executor handoff commit" "fix_executor_handoff_git"
fi

if ! COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"; then
  executor_fail 1 "failed to read executor handoff commit sha" "fix_executor_handoff_git"
fi

if ! "$SCRIPT_DIR/set-commit-sha.sh" "$COMMIT_SHA" >/dev/null; then
  executor_fail 1 "failed to persist executor handoff commit sha" "fix_executor_handoff_git"
fi

state_set "executor_done" "" "run_reviewer"
"$SCRIPT_DIR/sync-outbox.sh" >/dev/null
