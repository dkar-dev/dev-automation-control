#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <output-path> <executor-report-path>" >&2
  exit 1
fi

OUTPUT_PATH="$1"
EXECUTOR_REPORT_PATH="$2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_cmd python3

ensure_file "$STATE_FILE"
ensure_file "$TASK_FILE"
ensure_file "$EXECUTOR_REPORT_PATH"

export STATE_FILE TASK_FILE CONTROL_DIR OUTPUT_PATH EXECUTOR_REPORT_PATH

python3 <<'PY'
import json
import os
from pathlib import Path

state_file = Path(os.environ["STATE_FILE"])
task_file = Path(os.environ["TASK_FILE"])
control_dir = Path(os.environ["CONTROL_DIR"])
output_path = Path(os.environ["OUTPUT_PATH"])
executor_report_path = Path(os.environ["EXECUTOR_REPORT_PATH"])

state = json.loads(state_file.read_text(encoding="utf-8"))
run_dir = Path(state["paths"]["run_dir"])
manifest_path = run_dir / "resolved-reviewer-instructions.json"
if not manifest_path.is_file():
    raise SystemExit(f"Resolved reviewer instructions not found: {manifest_path}")

manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
template = (control_dir / "templates" / "reviewer-prompt.md").read_text(encoding="utf-8").rstrip()
task_text = task_file.read_text(encoding="utf-8").rstrip()
executor_report = executor_report_path.read_text(encoding="utf-8").rstrip()
state_snapshot = json.dumps(state, ensure_ascii=False, indent=2)
instructions_repo = Path(manifest["instructions_repo_path"])
overlays = manifest["instruction_overlays"]

def rel_label(path_str: str) -> str:
    path = Path(path_str)
    try:
        return str(path.relative_to(instructions_repo))
    except ValueError:
        return str(path)

resolved_files = manifest["resolved_instruction_files"]
resolved_list = "\n".join(f"- {rel_label(path)}" for path in resolved_files) or "- none"
instruction_blocks = []
for path_str in resolved_files:
    path = Path(path_str)
    instruction_blocks.append(
        f"### {rel_label(path_str)}\n\n{path.read_text(encoding='utf-8').rstrip()}"
    )

prompt = f"""You are the reviewer in an automated local pipeline.

Workspace root: {state["paths"]["reviewer_worktree"]}
Project repo root: {state["paths"]["project_dir"]}

Path contract:
- project_repo_path is the canonical project path for repo identity and context.
- reviewer_worktree_path is the only workspace where you may run commands and make edits.
- Control repo files are not available for direct access from this workspace. The task, state snapshot, executor report, and resolved instructions are embedded below.

Rules:
- Work only inside the reviewer worktree.
- Review critically. Do not trust the executor report.
- Re-run or extend verification where needed.
- Write the review report to: .codex-run/reviewer-report.md
- Your final assistant message must be short and include the report path.

Base reviewer instructions:
{template}

Resolved instruction selectors:
- instruction_profile: {manifest["instruction_profile"]}
- instruction_overlays: {", ".join(overlays) if overlays else "(none)"}
- instructions_repo_path: {manifest["instructions_repo_path"]}
- instructions_revision: {manifest["instructions_revision"]}
- resolved_instruction_files:
{resolved_list}

Resolved instructions:
{"\n\n".join(instruction_blocks)}

Current run state snapshot:
{state_snapshot}

Executor report:
{executor_report}

Active task:
{task_text}
"""

output_path.write_text(prompt.rstrip() + "\n", encoding="utf-8")
PY
