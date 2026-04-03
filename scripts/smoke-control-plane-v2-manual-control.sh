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
cat > "$TMP_ROOT/projects/demo/docs/control-manual-smoke.md" <<'EOF'
# Control Manual Smoke

Initial content.
EOF
git -C "$TMP_ROOT/projects/demo" add .gitignore README.md docs/control-manual-smoke.md
git -C "$TMP_ROOT/projects/demo" commit -m "Initial manual-control smoke fixture" >/dev/null
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
  EXECUTOR_MODE="$(next_sequence_value "${SMOKE_EXECUTOR_RESULT_SEQUENCE:-${SMOKE_EXECUTOR_RESULT_MODE:-}}" "success" executor_result)"
  if [[ "$EXECUTOR_MODE" == "fail" ]]; then
    cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor failed manual-control smoke intentionally.
MESSAGE
    cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Intentional executor failure for manual-control rerun coverage.
REPORT
    exit 41
  fi
  printf '\nExecutor manual smoke validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor manual smoke validated.\n' >> "$WORKTREE/docs/control-manual-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor completed manual-control smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/control-manual-smoke.md.
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
  grep -q 'Executor manual smoke validated\.' "$WORKTREE/README.md" || exit 29
  grep -q 'Executor manual smoke validated\.' "$WORKTREE/docs/control-manual-smoke.md" || exit 29
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  REVIEWER_MODE="$(next_sequence_value "${SMOKE_REVIEWER_VERDICT_SEQUENCE:-${SMOKE_REVIEWER_VERDICT_MODE:-}}" "approved" reviewer_mode)"
  REVIEWER_REPORT_MODE="$(next_sequence_value "${SMOKE_REVIEWER_REPORT_SEQUENCE:-${SMOKE_REVIEWER_REPORT_MODE:-}}" "valid" reviewer_report)"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Reviewer completed manual-control smoke successfully.
MESSAGE
  if [[ "$REVIEWER_REPORT_MODE" == "missing_verdict" ]]; then
    cat > "$WORKTREE/.codex-run/reviewer-report.md" <<'REPORT'
Decision: malformed reviewer report
Summary: malformed reviewer report for manual-control smoke
Commit SHA: __COMMIT_SHA__
REPORT
  else
    cat > "$WORKTREE/.codex-run/reviewer-report.md" <<REPORT
Verdict: $REVIEWER_MODE
Summary: synthetic reviewer summary for $REVIEWER_MODE
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
    "claim": control_dir / "scripts" / "claim-next-run",
    "start_step": control_dir / "scripts" / "start-step-run",
    "show_run": control_dir / "scripts" / "show-run",
    "show_control": control_dir / "scripts" / "show-run-control-state",
    "pause": control_dir / "scripts" / "pause-run",
    "resume": control_dir / "scripts" / "resume-run",
    "force_stop": control_dir / "scripts" / "force-stop-run",
    "rerun_step": control_dir / "scripts" / "rerun-run-step",
    "tick": control_dir / "scripts" / "run-worker-tick",
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


def worker_tick(*, env: dict[str, str] | None = None, expect_success: bool = True) -> dict:
    payload = run_json(
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
        expect_success=expect_success,
        env=env,
    )
    return payload["worker_tick"]


def show_run(run_id: str) -> dict:
    return run_json(scripts["show_run"], "--sqlite-db", db_path, run_id, "--json")["run_details"]


def show_control(run_id: str) -> dict:
    return run_json(scripts["show_control"], "--sqlite-db", db_path, run_id, "--json")["run_control_state"]


context_payload = {
    "project": "demo",
    "task_text": "Smoke-test the v2 manual control layer using existing worker and dispatch primitives.",
    "mode": "executor+reviewer",
    "branch_base": "main",
    "auto_commit": False,
    "source": "manual-control-smoke",
    "thread_label": "manual-control-smoke",
    "constraints": ["Only modify README.md and docs/control-manual-smoke.md"],
    "expected_output": ["Manual control operations stay append-only and worker-safe"],
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

paused_run = create_run("manual-pause")
paused_result = run_json(scripts["pause"], "--sqlite-db", db_path, paused_run, "--json")["manual_control"]
assert paused_result["run"]["run"]["status"] == "paused", paused_result
assert paused_result["control_state"]["queue_status"] == "paused", paused_result
assert worker_tick()["status"] == "idle"
resumed_result = run_json(scripts["resume"], "--sqlite-db", db_path, paused_run, "--json")["manual_control"]
assert resumed_result["run"]["run"]["status"] == "queued", resumed_result
assert resumed_result["control_state"]["latest_resume_mode"] == "normal", resumed_result
resumed_tick = worker_tick(env={"SMOKE_REVIEWER_VERDICT_MODE": "approved"})
assert resumed_tick["claimed_run_id"] == paused_run, resumed_tick
assert resumed_tick["final_run_status"] == "completed", resumed_tick

stabilize_run = create_run("manual-stabilize")
run_json(scripts["pause"], "--sqlite-db", db_path, stabilize_run, "--json")
run_json(
    scripts["resume"],
    "--sqlite-db",
    db_path,
    stabilize_run,
    "--mode",
    "stabilize_to_green",
    "--note",
    "recover to green",
    "--json",
)
stabilize_control = show_control(stabilize_run)
assert stabilize_control["latest_resume_mode"] == "stabilize_to_green", stabilize_control
stabilize_tick = worker_tick(env={"SMOKE_REVIEWER_VERDICT_MODE": "approved"})
assert stabilize_tick["claimed_run_id"] == stabilize_run, stabilize_tick
assert stabilize_tick["final_run_status"] == "completed", stabilize_tick

force_stop_queued_run = create_run("manual-force-stop-queued")
force_stop_queued = run_json(scripts["force_stop"], "--sqlite-db", db_path, force_stop_queued_run, "--json")["manual_control"]
assert force_stop_queued["run"]["run"]["status"] == "stopped", force_stop_queued
assert force_stop_queued["control_state"]["queue_status"] == "cancelled", force_stop_queued
assert show_run(force_stop_queued_run)["run"]["status"] == "stopped"

force_stop_claimed_run = create_run("manual-force-stop-claimed")
claim_payload = run_json(scripts["claim"], "--sqlite-db", db_path, "--json")
assert claim_payload["claim"]["dispatch_run"]["run"]["id"] == force_stop_claimed_run, claim_payload
force_stop_claimed = run_json(scripts["force_stop"], "--sqlite-db", db_path, force_stop_claimed_run, "--json")["manual_control"]
assert force_stop_claimed["run"]["run"]["status"] == "stopped", force_stop_claimed
assert force_stop_claimed["control_state"]["queue_status"] == "cancelled", force_stop_claimed

active_pause_run = create_run("manual-pause-active")
claim_payload = run_json(scripts["claim"], "--sqlite-db", db_path, "--json")
assert claim_payload["claim"]["dispatch_run"]["run"]["id"] == active_pause_run, claim_payload
start_payload = run_json(
    scripts["start_step"],
    "--sqlite-db",
    db_path,
    "--run-id",
    active_pause_run,
    "--step-key",
    "executor",
    "--json",
)
assert start_payload["step_run_details"]["step_run"]["status"] == "running", start_payload
pause_active = run_json(scripts["pause"], "--sqlite-db", db_path, active_pause_run, "--json", expect_success=False)
assert pause_active["error"]["code"] == "MANUAL_ACTIVE_STEP_NOT_SAFE", pause_active
force_stop_active = run_json(scripts["force_stop"], "--sqlite-db", db_path, active_pause_run, "--json")["manual_control"]
assert force_stop_active["run"]["run"]["status"] == "stopped", force_stop_active

rerun_run = create_run("manual-rerun-executor")
failed_tick = worker_tick(
    env={"SMOKE_EXECUTOR_RESULT_MODE": "fail"},
    expect_success=False,
)
assert failed_tick["claimed_run_id"] == rerun_run, failed_tick
assert failed_tick["status"] == "dispatch_failed", failed_tick
assert failed_tick["roles_dispatched"] == ["executor"], failed_tick
executor_step_run_id = failed_tick["dispatch_results"][0]["step_run"]["step_run"]["id"]
rerun_result = run_json(
    scripts["rerun_step"],
    "--sqlite-db",
    db_path,
    executor_step_run_id,
    "--note",
    "retry executor path",
    "--json",
)["manual_control"]
assert rerun_result["control_state"]["pending_rerun"]["step_key"] == "executor", rerun_result
assert rerun_result["run"]["run"]["status"] == "queued", rerun_result
rerun_control = show_control(rerun_run)
assert rerun_control["pending_rerun"]["source_step_run_id"] == executor_step_run_id, rerun_control
rerun_tick = worker_tick(
    env={
        "SMOKE_EXECUTOR_RESULT_MODE": "success",
        "SMOKE_REVIEWER_VERDICT_MODE": "approved",
    }
)
assert rerun_tick["claimed_run_id"] == rerun_run, rerun_tick
assert rerun_tick["roles_dispatched"] == ["executor", "reviewer"], rerun_tick
assert rerun_tick["final_run_status"] == "completed", rerun_tick
assert show_control(rerun_run)["pending_rerun"] is None

print(json.dumps({
    "paused_run": paused_run,
    "stabilize_run": stabilize_run,
    "force_stop_queued_run": force_stop_queued_run,
    "force_stop_claimed_run": force_stop_claimed_run,
    "active_pause_run": active_pause_run,
    "rerun_run": rerun_run,
    "rerun_source_step_run_id": executor_step_run_id,
    "worker_log_root": str(worker_log_root),
}, ensure_ascii=False, indent=2))
PY
