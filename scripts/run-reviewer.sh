#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_cmd codex
require_cmd git
require_cmd python3
require_cmd awk

PROJECT_REPO="$(task_get project_repo_path)"
WORKTREE="$(task_get reviewer_worktree_path)"

ensure_file "$TASK_FILE"
ensure_file "$OUTBOX_DIR/executor-report.md"
ensure_dir "$PROJECT_REPO"
ensure_dir "$WORKTREE"

mapfile -t REVIEW_STATE < <(
  python3 - "$STATE_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    state = json.load(f)

print(state.get("status", ""))
print(((state.get("result") or {}).get("commit_sha") or "").strip())
PY
)

CURRENT_STATUS="${REVIEW_STATE[0]:-}"
COMMIT_SHA="${REVIEW_STATE[1]:-}"

reviewer_prep_fail() {
  local message="$1"
  local next_action="${2:-fix_reviewer_handoff}"

  state_set "failed" "$message" "$next_action"
  "$SCRIPT_DIR/sync-outbox.sh" >/dev/null || true
  echo "$message" >&2
  exit 1
}

if [[ "$CURRENT_STATUS" != "executor_done" ]]; then
  echo "reviewer can only start from executor_done; current status: ${CURRENT_STATUS:-missing}" >&2
  exit 1
fi

if [[ -z "$COMMIT_SHA" ]]; then
  reviewer_prep_fail "result.commit_sha is missing; executor must create a handoff commit before reviewer starts" "fix_executor_handoff_git"
fi

if ! git -C "$WORKTREE" rev-parse --verify "${COMMIT_SHA}^{commit}" >/dev/null 2>&1; then
  reviewer_prep_fail "result.commit_sha does not resolve in reviewer worktree: $COMMIT_SHA" "fix_executor_handoff_git"
fi

if ! git -C "$WORKTREE" reset --hard "$COMMIT_SHA" >/dev/null; then
  reviewer_prep_fail "failed to reset reviewer worktree to commit $COMMIT_SHA" "fix_reviewer_worktree_sync"
fi

if ! git -C "$WORKTREE" clean -fdx >/dev/null; then
  reviewer_prep_fail "failed to clean reviewer worktree before review" "fix_reviewer_worktree_sync"
fi

RUN_DIR="$(prepare_run_dir "$WORKTREE")"
PROMPT_FILE="$RUN_DIR/reviewer-prompt.md"
LOCAL_INPUT_EXECUTOR_REPORT="$RUN_DIR/executor-report.input.md"
LOCAL_REPORT="$RUN_DIR/reviewer-report.md"
LOCAL_LAST_MESSAGE="$RUN_DIR/reviewer-last-message.md"

cp "$OUTBOX_DIR/executor-report.md" "$LOCAL_INPUT_EXECUTOR_REPORT"
rm -f "$LOCAL_REPORT" "$LOCAL_LAST_MESSAGE"

if ! "$SCRIPT_DIR/resolve-instructions.sh" reviewer >/dev/null; then
  reviewer_prep_fail "failed to resolve reviewer instructions" "fix_instruction_resolution"
fi

if ! "$SCRIPT_DIR/build-reviewer-prompt.sh" "$PROMPT_FILE" "$LOCAL_INPUT_EXECUTOR_REPORT"; then
  reviewer_prep_fail "failed to build reviewer prompt from resolved instructions" "fix_instruction_resolution"
fi

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

if [[ "${CONTROL_REVIEWER_SKIP_COMPLETION:-0}" == "1" ]]; then
  state_set "reviewer_done" "" "await_reviewer_outcome"
  "$SCRIPT_DIR/sync-outbox.sh" >/dev/null
  exit 0
fi

set +e
"$SCRIPT_DIR/complete-run-from-review.sh"
complete_rc=$?
set -e

if [ "$complete_rc" -ne 0 ]; then
  state_set "failed" "reviewer completion failed with exit code $complete_rc" "fix_reviewer_report_contract"
  "$SCRIPT_DIR/sync-outbox.sh" >/dev/null
  exit "$complete_rc"
fi
