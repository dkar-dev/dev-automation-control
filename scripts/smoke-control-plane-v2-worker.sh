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
  "$TMP_ROOT/artifacts" \
  "$TMP_ROOT/worker-logs"

cp -a "$CONTROL_DIR/projects/sample-project/." "$TMP_ROOT/packages/demo/"

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
cat > "$TMP_ROOT/projects/demo/docs/control-worker-smoke.md" <<'EOF'
# Control Worker Smoke

Initial content.
EOF
git -C "$TMP_ROOT/projects/demo" add .gitignore README.md docs/control-worker-smoke.md
git -C "$TMP_ROOT/projects/demo" commit -m "Initial worker smoke fixture" >/dev/null
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
Docs overlay instruction marker.
EOF
cat > "$TMP_ROOT/instructions/overlays/strict-review/reviewer.md" <<'EOF'
Strict review overlay instruction marker.
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

next_sequence_value() {
  local raw="$1"
  local default_value="$2"
  local counter_name="$3"
  if [[ -z "$raw" ]]; then
    printf '%s' "$default_value"
    return
  fi
  local state_dir="${SMOKE_SEQUENCE_STATE_DIR:-}"
  if [[ -z "$state_dir" ]]; then
    printf '%s' "${raw%%,*}"
    return
  fi
  mkdir -p "$state_dir"
  local counter_file="$state_dir/$counter_name"
  local index=0
  if [[ -f "$counter_file" ]]; then
    index="$(cat "$counter_file")"
  fi
  IFS=',' read -r -a values <<< "$raw"
  if (( index >= ${#values[@]} )); then
    index=$((${#values[@]} - 1))
  fi
  printf '%s' "${values[$index]}"
  printf '%s' "$((index + 1))" > "$counter_file"
}

PROMPT="$(cat)"
mkdir -p "$WORKTREE/.codex-run"

if printf '%s' "$PROMPT" | grep -q 'You are the executor'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 25
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 25
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Executor profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Docs overlay instruction marker\.' <<<"$PROMPT" || exit 26
  printf '\nExecutor worker smoke validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor worker smoke validated.\n' >> "$WORKTREE/docs/control-worker-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor completed worker smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/control-worker-smoke.md.
REPORT
  exit 0
fi

if printf '%s' "$PROMPT" | grep -q 'You are the reviewer'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 27
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 27
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Reviewer profile instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Docs overlay instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Strict review overlay instruction marker\.' <<<"$PROMPT" || exit 28
  grep -q 'Executor worker smoke validated\.' "$WORKTREE/README.md" || exit 29
  grep -q 'Executor worker smoke validated\.' "$WORKTREE/docs/control-worker-smoke.md" || exit 29
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  REVIEWER_MODE="$(next_sequence_value "${SMOKE_REVIEWER_VERDICT_SEQUENCE:-${SMOKE_REVIEWER_VERDICT_MODE:-}}" "approved" reviewer_mode)"
  REVIEWER_REPORT_MODE="$(next_sequence_value "${SMOKE_REVIEWER_REPORT_SEQUENCE:-${SMOKE_REVIEWER_REPORT_MODE:-}}" "valid" reviewer_report)"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Reviewer completed worker smoke successfully.
MESSAGE
  if [[ "$REVIEWER_REPORT_MODE" == "missing_verdict" ]]; then
    cat > "$WORKTREE/.codex-run/reviewer-report.md" <<'REPORT'
Decision: malformed reviewer report
Summary: malformed synthetic reviewer report for worker smoke
Commit SHA: __COMMIT_SHA__
REPORT
  else
    cat > "$WORKTREE/.codex-run/reviewer-report.md" <<REPORT
Verdict: $REVIEWER_MODE
Summary: synthetic worker reviewer summary for $REVIEWER_MODE
Commit SHA: __COMMIT_SHA__
REPORT
  fi
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
import os
from pathlib import Path
import subprocess
import sys


control_dir = Path(sys.argv[1]).resolve()
tmp_root = Path(sys.argv[2]).resolve()
db_path = tmp_root / "control-plane-v2.sqlite"
artifact_root = tmp_root / "artifacts"
worker_log_root = tmp_root / "worker-logs"
context_path = tmp_root / "worker-context.json"

scripts = {
    "init": control_dir / "scripts" / "init-sqlite-v1",
    "register": control_dir / "scripts" / "register-project-package",
    "create": control_dir / "scripts" / "create-root-run",
    "show_run": control_dir / "scripts" / "show-run",
    "tick": control_dir / "scripts" / "run-worker-tick",
    "until_idle": control_dir / "scripts" / "run-worker-until-idle",
}

base_env = os.environ.copy()


def run_command(*args: object, expect_success: bool = True, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    command_env = base_env.copy()
    if env:
        command_env.update(env)
    proc = subprocess.run(
        [str(arg) for arg in args],
        cwd=control_dir,
        text=True,
        capture_output=True,
        env=command_env,
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


def run_json(*args: object, expect_success: bool = True, env: dict[str, str] | None = None) -> dict:
    return load_json(run_command(*args, expect_success=expect_success, env=env))


def create_run(milestone: str, *, priority_class: str = "interactive") -> str:
    payload = run_json(
        scripts["create"],
        "--sqlite-db",
        db_path,
        "--project-key",
        "demo",
        "--project-profile",
        "default",
        "--workflow-id",
        "build",
        "--milestone",
        milestone,
        "--priority-class",
        priority_class,
        "--artifact-root",
        artifact_root,
        "--json",
    )
    return payload["run_details"]["run"]["id"]


def worker_tick(*, env: dict[str, str] | None = None, expect_success: bool = True, extra_args: list[object] | None = None) -> dict:
    args = [
        scripts["tick"],
        "--sqlite-db",
        db_path,
        "--context-json",
        context_path,
        "--artifact-root",
        artifact_root,
        "--worker-log-root",
        worker_log_root,
        "--json",
    ]
    if extra_args:
        args.extend(extra_args)
    return run_json(*args, expect_success=expect_success, env=env)["worker_tick"]


def worker_until_idle(*, env: dict[str, str] | None = None, extra_args: list[object] | None = None) -> dict:
    args = [
        scripts["until_idle"],
        "--sqlite-db",
        db_path,
        "--context-json",
        context_path,
        "--artifact-root",
        artifact_root,
        "--worker-log-root",
        worker_log_root,
        "--json",
    ]
    if extra_args:
        args.extend(extra_args)
    return run_json(*args, env=env)["worker_loop"]


def show_run(run_id: str) -> dict:
    return run_json(scripts["show_run"], "--sqlite-db", db_path, run_id, "--json")["run_details"]


def assert_summary_paths(summary_paths: dict) -> None:
    assert summary_paths["json_path"], summary_paths
    assert summary_paths["markdown_path"], summary_paths
    assert Path(summary_paths["json_path"]).exists(), summary_paths
    assert Path(summary_paths["markdown_path"]).exists(), summary_paths


context_payload = {
    "project": "demo",
    "task_text": "Smoke-test the v2 single-worker loop using existing primitives.",
    "mode": "executor+reviewer",
    "branch_base": "main",
    "auto_commit": False,
    "source": "worker-smoke",
    "thread_label": "worker-smoke",
    "constraints": ["Only modify README.md and docs/control-worker-smoke.md"],
    "expected_output": ["Reviewer semantic outcome closes through the worker loop"],
    "project_repo_path": str(tmp_root / "projects" / "demo"),
    "executor_worktree_path": str(tmp_root / "runtime" / "worktrees" / "demo-executor"),
    "reviewer_worktree_path": str(tmp_root / "runtime" / "worktrees" / "demo-reviewer"),
    "instruction_profile": "default",
    "instruction_overlays": ["docs-only", "strict-review"],
    "instructions_repo_path": str(tmp_root / "instructions"),
}
context_path.write_text(json.dumps(context_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

run_command(scripts["init"], db_path, "--json")
run_command(scripts["register"], tmp_root / "packages" / "demo", "--sqlite-db", db_path, "--json")

approved_run = create_run("worker-approved")
approved_tick = worker_tick(env={"SMOKE_REVIEWER_VERDICT_MODE": "approved"})
assert approved_tick["status"] == "progressed", approved_tick
assert approved_tick["claimed_run_id"] == approved_run, approved_tick
assert approved_tick["roles_dispatched"] == ["executor", "reviewer"], approved_tick
assert approved_tick["reviewer_ingestion_happened"] is True, approved_tick
assert approved_tick["final_run_status"] == "completed", approved_tick
assert_summary_paths(approved_tick["summary_paths"])
assert show_run(approved_run)["run"]["status"] == "completed"
assert worker_tick()["status"] == "idle"

changes_run = create_run("worker-changes")
changes_tick = worker_tick(env={"SMOKE_REVIEWER_VERDICT_MODE": "changes_requested"})
follow_up_run_id = changes_tick["follow_up_run_id"]
assert changes_tick["status"] == "progressed", changes_tick
assert changes_tick["follow_up_run_created"] is True, changes_tick
assert follow_up_run_id, changes_tick
assert show_run(changes_run)["run"]["status"] == "completed"
assert show_run(follow_up_run_id)["run"]["status"] == "queued"
follow_up_tick = worker_tick(env={"SMOKE_REVIEWER_VERDICT_MODE": "approved"})
assert follow_up_tick["claimed_run_id"] == follow_up_run_id, follow_up_tick
assert follow_up_tick["final_run_status"] == "completed", follow_up_tick

blocked_run = create_run("worker-blocked")
blocked_tick = worker_tick(env={"SMOKE_REVIEWER_VERDICT_MODE": "blocked"})
assert blocked_tick["status"] == "stopped", blocked_tick
assert blocked_tick["final_run_status"] == "stopped", blocked_tick
assert show_run(blocked_run)["run"]["status"] == "stopped"

malformed_run = create_run("worker-malformed")
malformed_tick = worker_tick(
    env={"SMOKE_REVIEWER_REPORT_MODE": "missing_verdict"},
    expect_success=False,
)
assert malformed_tick["status"] == "ingestion_failed", malformed_tick
assert malformed_tick["error"]["code"] == "REVIEWER_RESULT_SOURCE_NOT_FOUND", malformed_tick
assert malformed_tick["final_run_status"] == "running", malformed_tick
assert show_run(malformed_run)["run"]["status"] == "running"

until_idle_run = create_run("worker-until-idle")
sequence_dir = tmp_root / "sequence-state"
until_idle = worker_until_idle(
    env={
        "SMOKE_REVIEWER_VERDICT_SEQUENCE": "changes_requested,approved",
        "SMOKE_SEQUENCE_STATE_DIR": str(sequence_dir),
    },
    extra_args=["--max-ticks", "10"],
)
assert until_idle["ended_reason"] == "idle", until_idle
assert until_idle["ticks_executed"] == 3, until_idle
assert until_idle["claims_processed"] == 2, until_idle
assert until_idle["follow_ups_created"] == 1, until_idle
assert until_idle["tick_results"][0]["claimed_run_id"] == until_idle_run, until_idle
assert until_idle["tick_results"][0]["follow_up_run_created"] is True, until_idle
assert until_idle["tick_results"][1]["claimed_run_id"] == until_idle["tick_results"][0]["follow_up_run_id"], until_idle
assert until_idle["tick_results"][2]["status"] == "idle", until_idle
assert_summary_paths(until_idle["summary_paths"])

bounded_run = create_run("worker-max-ticks")
bounded = worker_until_idle(
    env={
        "SMOKE_REVIEWER_VERDICT_SEQUENCE": "changes_requested,approved",
        "SMOKE_SEQUENCE_STATE_DIR": str(tmp_root / "sequence-state-bounded"),
    },
    extra_args=["--max-ticks", "1"],
)
assert bounded["ended_reason"] == "max_ticks_reached", bounded
assert bounded["claims_processed"] == 1, bounded
assert bounded["follow_ups_created"] == 1, bounded
assert show_run(bounded["tick_results"][0]["follow_up_run_id"])["run"]["status"] == "queued"

broken_run = create_run("worker-broken", priority_class="background")
broken_tick = worker_tick(
    expect_success=False,
    extra_args=["--executor-runner", tmp_root / "missing-runner.sh"],
)
assert broken_tick["status"] == "dispatch_failed", broken_tick
assert broken_tick["queue_requeued"] is True, broken_tick
assert broken_tick["final_run_status"] == "queued", broken_tick
assert show_run(broken_run)["run"]["status"] == "queued"

print(json.dumps({
    "approved_run": approved_run,
    "changes_root_run": changes_run,
    "changes_follow_up_run": follow_up_run_id,
    "blocked_run": blocked_run,
    "broken_run": broken_run,
    "malformed_run": malformed_run,
    "until_idle_root_run": until_idle_run,
    "bounded_root_run": bounded_run,
    "worker_log_root": str(worker_log_root),
}, ensure_ascii=False, indent=2))
PY
