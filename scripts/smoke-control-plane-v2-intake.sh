#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

require_cmd git
require_cmd mktemp
require_cmd python3

TMP_ROOT="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

mkdir -p \
  "$TMP_ROOT/projects/demo" \
  "$TMP_ROOT/runtime/worktrees" \
  "$TMP_ROOT/instructions" \
  "$TMP_ROOT/packages/demo" \
  "$TMP_ROOT/fakebin" \
  "$TMP_ROOT/artifacts"

cp -a "$CONTROL_DIR/projects/sample-project/." "$TMP_ROOT/packages/demo/"
cat > "$TMP_ROOT/packages/demo/runtime.yaml" <<EOF
bounded_task_runtime_v1:
  branch_base: main
  mode: executor+reviewer
  auto_commit: false
  source: config-default-source
  thread_label: config-default-thread
  artifact_root: $TMP_ROOT/artifacts
EOF
cat > "$TMP_ROOT/packages/demo/instructions.yaml" <<'EOF'
bounded_task_intake_v1:
  instruction_profile: default
  instruction_overlays:
    - docs-only
EOF

git -C "$TMP_ROOT/projects/demo" init -b main >/dev/null
git -C "$TMP_ROOT/projects/demo" config user.name "Smoke Test"
git -C "$TMP_ROOT/projects/demo" config user.email "smoke@example.com"
mkdir -p "$TMP_ROOT/projects/demo/docs"
cat > "$TMP_ROOT/projects/demo/.gitignore" <<'EOF'
.codex/
.codex-run/
EOF
cat > "$TMP_ROOT/projects/demo/README.md" <<'EOF'
# Demo Project
EOF
cat > "$TMP_ROOT/projects/demo/docs/control-intake-smoke.md" <<'EOF'
# Control Intake Smoke

Initial content.
EOF
git -C "$TMP_ROOT/projects/demo" add .gitignore README.md docs/control-intake-smoke.md
git -C "$TMP_ROOT/projects/demo" commit -m "Initial intake smoke fixture" >/dev/null
git -C "$TMP_ROOT/projects/demo" worktree add --detach "$TMP_ROOT/runtime/worktrees/demo-executor" HEAD >/dev/null
git -C "$TMP_ROOT/projects/demo" worktree add --detach "$TMP_ROOT/runtime/worktrees/demo-reviewer" HEAD >/dev/null

printf 'stale executor scratch\n' > "$TMP_ROOT/runtime/worktrees/demo-executor/stale-untracked.txt"
mkdir -p "$TMP_ROOT/runtime/worktrees/demo-executor/.codex-run"
printf 'stale executor artifact\n' > "$TMP_ROOT/runtime/worktrees/demo-executor/.codex-run/stale-before-run.md"
printf 'stale reviewer scratch\n' > "$TMP_ROOT/runtime/worktrees/demo-reviewer/stale-untracked.txt"
mkdir -p "$TMP_ROOT/runtime/worktrees/demo-reviewer/.codex-run"
printf 'stale reviewer artifact\n' > "$TMP_ROOT/runtime/worktrees/demo-reviewer/.codex-run/stale-before-run.md"

git -C "$TMP_ROOT/instructions" init -b main >/dev/null
git -C "$TMP_ROOT/instructions" config user.name "Smoke Test"
git -C "$TMP_ROOT/instructions" config user.email "smoke@example.com"
mkdir -p "$TMP_ROOT/instructions/profiles/default" "$TMP_ROOT/instructions/overlays/strict-review"
cat > "$TMP_ROOT/instructions/profiles/default/shared.md" <<'EOF'
Shared profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/profiles/default/executor.md" <<'EOF'
Executor profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/profiles/default/reviewer.md" <<'EOF'
Reviewer profile instruction marker.
EOF
cat > "$TMP_ROOT/instructions/overlays/docs-only.md" <<'EOF'
Docs-only overlay marker.
EOF
cat > "$TMP_ROOT/instructions/overlays/strict-review/shared.md" <<'EOF'
Strict shared overlay marker.
EOF
git -C "$TMP_ROOT/instructions" add .
git -C "$TMP_ROOT/instructions" commit -m "Initial instructions fixture" >/dev/null

cat > "$TMP_ROOT/fakebin/codex" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

[ "${1:-}" = "exec" ] || exit 64
shift

WORKTREE=""
LAST_MESSAGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -C) WORKTREE="$2"; shift 2 ;;
    --output-last-message) LAST_MESSAGE="$2"; shift 2 ;;
    -s|-c) shift 2 ;;
    -) shift ;;
    *) shift ;;
  esac
done

PROMPT="$(cat)"
mkdir -p "$WORKTREE/.codex-run"

if printf '%s' "$PROMPT" | grep -q 'You are the executor'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 25
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 25
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Executor profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Strict shared overlay marker\.' <<<"$PROMPT" || exit 26
  ! grep -q 'Docs-only overlay marker\.' <<<"$PROMPT" || exit 26
  printf '\nExecutor intake smoke validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor intake smoke validated.\n' >> "$WORKTREE/docs/control-intake-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor completed intake smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/control-intake-smoke.md.
REPORT
  exit 0
fi

if printf '%s' "$PROMPT" | grep -q 'You are the reviewer'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 27
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 27
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Reviewer profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Strict shared overlay marker\.' <<<"$PROMPT" || exit 28
  ! grep -q 'Docs-only overlay marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Executor intake smoke validated\.' "$WORKTREE/README.md" || exit 29
  grep -q 'Executor intake smoke validated\.' "$WORKTREE/docs/control-intake-smoke.md" || exit 29
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Reviewer completed intake smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/reviewer-report.md" <<REPORT
Verdict: approved
Summary: synthetic intake reviewer summary for approved
Commit SHA: __COMMIT_SHA__
REPORT
  sed -i "s/__COMMIT_SHA__/$COMMIT_SHA/" "$WORKTREE/.codex-run/reviewer-report.md"
  exit 0
fi

exit 35
EOF
chmod +x "$TMP_ROOT/fakebin/codex"

export PATH="$TMP_ROOT/fakebin:$PATH"

python3 - "$CONTROL_DIR" "$TMP_ROOT" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import subprocess
import sys


control_dir = Path(sys.argv[1]).resolve()
tmp_root = Path(sys.argv[2]).resolve()
db_path = tmp_root / "control-plane-v2.sqlite"
artifact_root = tmp_root / "artifacts"
submission_json = tmp_root / "submission.json"

scripts = {
    "init": control_dir / "scripts" / "init-sqlite-v1",
    "register": control_dir / "scripts" / "register-project-package",
    "submit": control_dir / "scripts" / "submit-bounded-task",
    "show_submitted": control_dir / "scripts" / "show-submitted-task",
    "list_submitted": control_dir / "scripts" / "list-submitted-tasks",
    "tick": control_dir / "scripts" / "run-worker-tick",
    "show_run": control_dir / "scripts" / "show-run",
}


def run_command(*args: object, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(arg) for arg in args],
        cwd=control_dir,
        text=True,
        capture_output=True,
    )
    if expect_success and proc.returncode != 0:
        raise SystemExit(
            f"Command failed unexpectedly: {' '.join(str(arg) for arg in args)}\nstdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        )
    if not expect_success and proc.returncode == 0:
        raise SystemExit(
            f"Command was expected to fail but succeeded: {' '.join(str(arg) for arg in args)}\nstdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        )
    return proc


def load_json(proc: subprocess.CompletedProcess[str]) -> dict:
    payload_text = proc.stdout.strip() or proc.stderr.strip()
    if not payload_text:
        raise SystemExit("Expected JSON payload but command returned no output")
    return json.loads(payload_text)


def run_json(*args: object, expect_success: bool = True) -> dict:
    return load_json(run_command(*args, expect_success=expect_success))


run_json(scripts["init"], db_path, "--json")
run_json(scripts["register"], tmp_root / "packages" / "demo", "--sqlite-db", db_path, "--json")

bad_submit = run_json(
    scripts["submit"],
    "--sqlite-db",
    db_path,
    "--project-key",
    "demo",
    "--task-text",
    "This should fail because workspace_root is missing.",
    "--project-profile",
    "default",
    "--workflow-id",
    "build",
    "--milestone",
    "intake-missing-runtime",
    "--json",
    expect_success=False,
)
assert bad_submit["error"]["code"] == "INTAKE_RUNTIME_CONFIG_INVALID", bad_submit

submission_payload = {
    "project_key": "demo",
    "task_text": "Run the unified intake path and let the worker pick the submitted task without manual context assembly.",
    "project_profile": "default",
    "workflow_id": "build",
    "milestone": "intake-success",
    "priority_class": "interactive",
    "instruction_overlays": ["strict-review"],
    "source": "submit-json",
    "thread_label": "submit-intake",
    "constraints": ["Only modify README.md and docs/control-intake-smoke.md"],
    "expected_output": ["The worker should complete the submitted run without context-json"],
    "workspace_root": str(tmp_root),
}
submission_json.write_text(json.dumps(submission_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

submit_payload = run_json(
    scripts["submit"],
    "--sqlite-db",
    db_path,
    "--submission-json",
    submission_json,
    "--json",
)
submitted = submit_payload["submitted_task"]
run_id = submitted["run_details"]["run"]["id"]
flow_id = submitted["run_details"]["run"]["flow_id"]
assert submitted["run_details"]["run"]["status"] == "queued", submitted
assert submitted["run_details"]["run"]["queue_item"]["status"] == "queued", submitted
assert submitted["runtime_context"]["instruction_profile"] == "default", submitted
assert submitted["runtime_context"]["instruction_overlays"] == ["strict-review"], submitted
assert submitted["runtime_context"]["source"] == "submit-json", submitted
assert submitted["runtime_context"]["thread_label"] == "submit-intake", submitted
assert submitted["runtime_context"]["artifact_root"] == str(artifact_root), submitted
assert submitted["runtime_context"]["workspace_root"] == str(tmp_root), submitted

show_submitted = run_json(scripts["show_submitted"], "--sqlite-db", db_path, run_id, "--json")
assert show_submitted["submitted_task"]["submission_manifest"]["submission"]["task_text"] == submission_payload["task_text"], show_submitted
assert show_submitted["submitted_task"]["runtime_context_manifest"]["runtime_context"]["executor_worktree_path"] == str(
    tmp_root / "runtime" / "worktrees" / "demo-executor"
), show_submitted
assert len(show_submitted["submitted_task"]["artifacts"]) == 2, show_submitted

list_submitted = run_json(scripts["list_submitted"], "--sqlite-db", db_path, "--json")
assert any(item["run_id"] == run_id for item in list_submitted["submitted_tasks"]), list_submitted

tick_payload = run_json(scripts["tick"], "--sqlite-db", db_path, "--json")
worker_tick = tick_payload["worker_tick"]
assert worker_tick["claimed_run_id"] == run_id, worker_tick
assert worker_tick["status"] == "progressed", worker_tick
assert worker_tick["final_run_status"] == "completed", worker_tick

show_run = run_json(scripts["show_run"], "--sqlite-db", db_path, run_id, "--json")
assert show_run["run_details"]["run"]["status"] == "completed", show_run

connection = sqlite3.connect(db_path)
connection.row_factory = sqlite3.Row
try:
    artifact_rows = connection.execute(
        """
        SELECT artifact_kind, filesystem_path
        FROM artifact_refs
        WHERE run_id = ?
        ORDER BY created_at, id
        """,
        (run_id,),
    ).fetchall()
finally:
    connection.close()

artifact_kinds = [str(row["artifact_kind"]) for row in artifact_rows]
assert "task_submission_manifest" in artifact_kinds, artifact_kinds
assert "task_runtime_context_manifest" in artifact_kinds, artifact_kinds
dispatch_context_paths = [
    Path(str(row["filesystem_path"]))
    for row in artifact_rows
    if str(row["artifact_kind"]) == "dispatch_context_manifest"
]
assert dispatch_context_paths, artifact_rows
dispatch_context = json.loads(dispatch_context_paths[-1].read_text(encoding="utf-8"))
assert dispatch_context["runtime_context"]["source"] == "submit-json", dispatch_context
assert dispatch_context["runtime_context"]["thread_label"] == "submit-intake", dispatch_context
assert dispatch_context["runtime_context"]["instruction_overlays"] == ["strict-review"], dispatch_context
assert str(artifact_root) in str(dispatch_context_paths[-1]), dispatch_context_paths[-1]

print(
    json.dumps(
        {
            "run_id": run_id,
            "flow_id": flow_id,
            "artifact_kinds": artifact_kinds,
            "dispatch_context_path": str(dispatch_context_paths[-1]),
            "bad_submit_error_code": bad_submit["error"]["code"],
        },
        ensure_ascii=False,
        indent=2,
    )
)
PY
