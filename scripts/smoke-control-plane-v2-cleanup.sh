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
cat > "$TMP_ROOT/packages/demo/policy.yaml" <<'EOF'
cleanup_v1:
  artifacts_ttl_seconds: 0
  worktree_ttl_seconds: 0
  branch_ttl_seconds: 0
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
cat > "$TMP_ROOT/projects/demo/docs/control-cleanup-smoke.md" <<'EOF'
# Control Cleanup Smoke

Initial content.
EOF
git -C "$TMP_ROOT/projects/demo" add .gitignore README.md docs/control-cleanup-smoke.md
git -C "$TMP_ROOT/projects/demo" commit -m "Initial cleanup smoke fixture" >/dev/null
git -C "$TMP_ROOT/projects/demo" worktree add -b cleanup-executor-runtime "$TMP_ROOT/runtime/worktrees/demo-executor" HEAD >/dev/null
git -C "$TMP_ROOT/projects/demo" worktree add -b cleanup-reviewer-runtime "$TMP_ROOT/runtime/worktrees/demo-reviewer" HEAD >/dev/null

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

PROMPT="$(cat)"
mkdir -p "$WORKTREE/.codex-run"

if printf '%s' "$PROMPT" | grep -q 'You are the executor'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 25
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 25
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Executor profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Docs overlay instruction marker\.' <<<"$PROMPT" || exit 26
  printf '\nExecutor cleanup smoke validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor cleanup smoke validated.\n' >> "$WORKTREE/docs/control-cleanup-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor completed cleanup smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/control-cleanup-smoke.md.
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
  grep -q 'Executor cleanup smoke validated\.' "$WORKTREE/README.md" || exit 29
  grep -q 'Executor cleanup smoke validated\.' "$WORKTREE/docs/control-cleanup-smoke.md" || exit 29
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Reviewer completed cleanup smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/reviewer-report.md" <<REPORT
Verdict: approved
Summary: synthetic cleanup reviewer summary for approved
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
package_root = tmp_root / "packages" / "demo"
project_repo = tmp_root / "projects" / "demo"
executor_worktree = tmp_root / "runtime" / "worktrees" / "demo-executor"
reviewer_worktree = tmp_root / "runtime" / "worktrees" / "demo-reviewer"

scripts = {
    "init": control_dir / "scripts" / "init-sqlite-v1",
    "show_version": control_dir / "scripts" / "show-sqlite-schema-version",
    "register": control_dir / "scripts" / "register-project-package",
    "create": control_dir / "scripts" / "create-root-run",
    "pause": control_dir / "scripts" / "pause-run",
    "show_run": control_dir / "scripts" / "show-run",
    "until_idle": control_dir / "scripts" / "run-worker-until-idle",
    "list_cleanup": control_dir / "scripts" / "list-cleanup-candidates",
    "run_cleanup": control_dir / "scripts" / "run-cleanup-once",
    "show_cleanup": control_dir / "scripts" / "show-cleanup-status",
}

base_env = os.environ.copy()


def run_command(*args: object, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [str(arg) for arg in args],
        cwd=control_dir,
        text=True,
        capture_output=True,
        env=base_env,
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


def create_run(milestone: str) -> str:
    payload = run_json(
        scripts["create"],
        "--sqlite-db",
        db_path,
        "--project-key",
        "demo",
        "--project-profile",
        "default",
        "--workflow-id",
        "cleanup",
        "--milestone",
        milestone,
        "--artifact-root",
        artifact_root,
        "--json",
    )
    return payload["run_details"]["run"]["id"]


context_payload = {
    "project": "demo",
    "task_text": "Smoke-test runtime cleanup manager on one approved terminal flow.",
    "mode": "executor+reviewer",
    "branch_base": "main",
    "auto_commit": False,
    "source": "cleanup-smoke",
    "thread_label": "cleanup-smoke",
    "constraints": ["Only modify README.md and docs/control-cleanup-smoke.md"],
    "expected_output": ["Cleanup candidates become eligible immediately after terminal state"],
    "project_repo_path": str(project_repo),
    "executor_worktree_path": str(executor_worktree),
    "reviewer_worktree_path": str(reviewer_worktree),
    "instruction_profile": "default",
    "instruction_overlays": ["docs-only", "strict-review"],
    "instructions_repo_path": str(tmp_root / "instructions"),
}
context_path.write_text(json.dumps(context_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

init_payload = run_json(scripts["init"], db_path, "--json")
assert init_payload["database"]["current_version"] == 3, init_payload
version_payload = run_json(scripts["show_version"], db_path, "--json")
assert version_payload["schema_version"]["current_version"] == 3, version_payload

register_payload = run_json(scripts["register"], package_root, "--sqlite-db", db_path, "--json")
assert register_payload["registration"]["project"]["project_key"] == "demo", register_payload

approved_run_id = create_run("cleanup-approved")
paused_run_id = create_run("cleanup-paused")
pause_payload = run_json(scripts["pause"], "--sqlite-db", db_path, paused_run_id, "--json")
assert pause_payload["manual_control"]["control_state"]["paused"] is True, pause_payload

loop_payload = run_json(
    scripts["until_idle"],
    "--sqlite-db",
    db_path,
    "--context-json",
    context_path,
    "--artifact-root",
    artifact_root,
    "--worker-log-root",
    worker_log_root,
    "--max-ticks",
    "10",
    "--json",
)
assert loop_payload["worker_loop"]["ended_reason"] == "idle", loop_payload
assert loop_payload["worker_loop"]["runs_progressed"] >= 1, loop_payload

approved_run = run_json(scripts["show_run"], "--sqlite-db", db_path, approved_run_id, "--json")["run_details"]["run"]
paused_run = run_json(scripts["show_run"], "--sqlite-db", db_path, paused_run_id, "--json")["run_details"]["run"]
assert approved_run["status"] == "completed", approved_run
assert paused_run["status"] == "paused", paused_run
assert paused_run["queue_item"]["status"] == "paused", paused_run

candidate_payload = run_json(scripts["list_cleanup"], "--sqlite-db", db_path, "--json")
counts = candidate_payload["cleanup_candidates"]["counts"]
assert counts["artifacts"] > 0, candidate_payload
assert counts["worktrees"] == 2, candidate_payload
assert counts["branches"] == 2, candidate_payload

candidates = candidate_payload["cleanup_candidates"]["candidates"]
artifact_candidates = [item for item in candidates if item["scope"] == "artifacts"]
worktree_candidates = [item for item in candidates if item["scope"] == "worktrees"]
branch_candidates = [item for item in candidates if item["scope"] == "branches"]
assert artifact_candidates, candidate_payload
assert all(approved_run_id in item["run_ids"] for item in candidates), candidate_payload
assert all(paused_run_id not in item["run_ids"] for item in candidates), candidate_payload

artifact_path = Path(artifact_candidates[0]["filesystem_path"])
assert artifact_path.exists(), artifact_path
assert executor_worktree.exists(), executor_worktree
assert reviewer_worktree.exists(), reviewer_worktree

executor_branch_before = run_command("git", "-C", project_repo, "branch", "--list", "cleanup-executor-runtime")
reviewer_branch_before = run_command("git", "-C", project_repo, "branch", "--list", "cleanup-reviewer-runtime")
assert "cleanup-executor-runtime" in executor_branch_before.stdout, executor_branch_before.stdout
assert "cleanup-reviewer-runtime" in reviewer_branch_before.stdout, reviewer_branch_before.stdout

dry_run_payload = run_json(scripts["run_cleanup"], "--sqlite-db", db_path, "--dry-run", "--json")
assert dry_run_payload["cleanup_pass"]["dry_run"] is True, dry_run_payload
assert all(result["action"] == "dry_run" for result in dry_run_payload["cleanup_pass"]["results"]), dry_run_payload
assert artifact_path.exists(), artifact_path
assert executor_worktree.exists(), executor_worktree
assert reviewer_worktree.exists(), reviewer_worktree
assert "cleanup-executor-runtime" in run_command("git", "-C", project_repo, "branch", "--list", "cleanup-executor-runtime").stdout
assert "cleanup-reviewer-runtime" in run_command("git", "-C", project_repo, "branch", "--list", "cleanup-reviewer-runtime").stdout

cleanup_payload = run_json(scripts["run_cleanup"], "--sqlite-db", db_path, "--json")
summary = cleanup_payload["cleanup_pass"]["summary"]
assert summary["artifacts"]["processed"] > 0, cleanup_payload
assert summary["worktrees"]["deleted"] == 2, cleanup_payload
assert summary["branches"]["deleted"] == 2, cleanup_payload
assert any(result["scope"] == "artifacts" and result["deleted"] for result in cleanup_payload["cleanup_pass"]["results"]), cleanup_payload
assert not artifact_path.exists(), artifact_path
assert not executor_worktree.exists(), executor_worktree
assert not reviewer_worktree.exists(), reviewer_worktree

executor_branch_after = run_command("git", "-C", project_repo, "branch", "--list", "cleanup-executor-runtime")
reviewer_branch_after = run_command("git", "-C", project_repo, "branch", "--list", "cleanup-reviewer-runtime")
main_branch_after = run_command("git", "-C", project_repo, "branch", "--list", "main")
assert "cleanup-executor-runtime" not in executor_branch_after.stdout, executor_branch_after.stdout
assert "cleanup-reviewer-runtime" not in reviewer_branch_after.stdout, reviewer_branch_after.stdout
assert "main" in main_branch_after.stdout, main_branch_after.stdout

status_payload = run_json(scripts["show_cleanup"], "--sqlite-db", db_path, "--run-id", approved_run_id, "--json")
entries = status_payload["cleanup_status"]["entries"]
assert entries, status_payload
assert any(entry["scope"] == "artifacts" and entry["cleaned_at"] is not None for entry in entries), status_payload
assert any(entry["scope"] == "worktrees" and entry["cleanup_status"] == "deleted" for entry in entries), status_payload
assert any(entry["scope"] == "branches" and entry["cleanup_status"] == "deleted" for entry in entries), status_payload
assert status_payload["cleanup_status"]["eligible_candidates"] == [], status_payload

repeat_cleanup_payload = run_json(scripts["run_cleanup"], "--sqlite-db", db_path, "--json")
assert repeat_cleanup_payload["cleanup_pass"]["results"] == [], repeat_cleanup_payload
repeat_counts = repeat_cleanup_payload["cleanup_pass"]["candidate_report"]["counts"]
assert repeat_counts["artifacts"] == 0, repeat_cleanup_payload
assert repeat_counts["worktrees"] == 0, repeat_cleanup_payload
assert repeat_counts["branches"] == 0, repeat_cleanup_payload

print(
    json.dumps(
        {
            "approved_run_id": approved_run_id,
            "paused_run_id": paused_run_id,
            "candidate_counts_before_cleanup": counts,
            "dry_run_result_count": len(dry_run_payload["cleanup_pass"]["results"]),
            "cleanup_summary": summary,
            "status_entry_count": len(entries),
        },
        ensure_ascii=False,
        indent=2,
    )
)
PY
