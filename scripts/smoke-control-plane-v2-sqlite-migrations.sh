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
cat > "$TMP_ROOT/projects/demo/docs/control-migration-smoke.md" <<'EOF'
# Control Migration Smoke

Initial content.
EOF
git -C "$TMP_ROOT/projects/demo" add .gitignore README.md docs/control-migration-smoke.md
git -C "$TMP_ROOT/projects/demo" commit -m "Initial sqlite migration smoke fixture" >/dev/null
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

PROMPT="$(cat)"
mkdir -p "$WORKTREE/.codex-run"

if printf '%s' "$PROMPT" | grep -q 'You are the executor'; then
  [[ ! -e "$WORKTREE/stale-untracked.txt" ]] || exit 25
  [[ ! -e "$WORKTREE/.codex-run/stale-before-run.md" ]] || exit 25
  grep -q 'Shared profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Executor profile instruction marker\.' <<<"$PROMPT" || exit 26
  grep -q 'Docs overlay instruction marker\.' <<<"$PROMPT" || exit 26
  printf '\nExecutor migration smoke validated.\n' >> "$WORKTREE/README.md"
  printf '\nExecutor migration smoke validated.\n' >> "$WORKTREE/docs/control-migration-smoke.md"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Executor completed sqlite migration smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/executor-report.md" <<'REPORT'
# Executor Report

## Summary
Updated README.md and docs/control-migration-smoke.md.
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
  grep -q 'Executor migration smoke validated\.' "$WORKTREE/README.md" || exit 29
  grep -q 'Executor migration smoke validated\.' "$WORKTREE/docs/control-migration-smoke.md" || exit 29
  COMMIT_SHA="$(git -C "$WORKTREE" rev-parse HEAD)"
  cat > "$LAST_MESSAGE" <<'MESSAGE'
Reviewer completed sqlite migration smoke successfully.
MESSAGE
  cat > "$WORKTREE/.codex-run/reviewer-report.md" <<REPORT
Verdict: approved
Summary: synthetic migration reviewer summary for approved
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
import sqlite3
import subprocess
import sys


control_dir = Path(sys.argv[1]).resolve()
tmp_root = Path(sys.argv[2]).resolve()
artifact_root = tmp_root / "artifacts"
worker_log_root = tmp_root / "worker-logs"
context_path = tmp_root / "worker-context.json"

scripts = {
    "init": control_dir / "scripts" / "init-sqlite-v1",
    "migrate": control_dir / "scripts" / "migrate-sqlite-v1",
    "show_version": control_dir / "scripts" / "show-sqlite-schema-version",
    "register": control_dir / "scripts" / "register-project-package",
    "create": control_dir / "scripts" / "create-root-run",
    "pause": control_dir / "scripts" / "pause-run",
    "resume": control_dir / "scripts" / "resume-run",
    "tick": control_dir / "scripts" / "run-worker-tick",
    "show_run": control_dir / "scripts" / "show-run",
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


context_payload = {
    "project": "demo",
    "task_text": "Smoke-test SQLite migrations plus one worker/manual-control pass on the migrated DB.",
    "mode": "executor+reviewer",
    "branch_base": "main",
    "auto_commit": False,
    "source": "sqlite-migration-smoke",
    "thread_label": "sqlite-migration-smoke",
    "constraints": ["Only modify README.md and docs/control-migration-smoke.md"],
    "expected_output": ["Migrated DB remains usable by manual-control and worker loop"],
    "project_repo_path": str(tmp_root / "projects" / "demo"),
    "executor_worktree_path": str(tmp_root / "runtime" / "worktrees" / "demo-executor"),
    "reviewer_worktree_path": str(tmp_root / "runtime" / "worktrees" / "demo-reviewer"),
    "instruction_profile": "default",
    "instruction_overlays": ["docs-only", "strict-review"],
    "instructions_repo_path": str(tmp_root / "instructions"),
}
context_path.write_text(json.dumps(context_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

fresh_db = tmp_root / "fresh.sqlite"
fresh_init = run_json(scripts["init"], fresh_db, "--json")
fresh_show = run_json(scripts["show_version"], fresh_db, "--json")
assert fresh_init["database"]["current_version"] == 2, fresh_init
assert fresh_show["schema_version"]["tracked"] is True, fresh_show
assert fresh_show["schema_version"]["current_version"] == 2, fresh_show

legacy_db = tmp_root / "legacy.sqlite"
baseline_sql = (control_dir / "schemas" / "migrations" / "0001_baseline.sql").read_text(encoding="utf-8")
connection = sqlite3.connect(legacy_db)
try:
    connection.executescript(baseline_sql)
    connection.commit()
finally:
    connection.close()

legacy_before = run_json(scripts["show_version"], legacy_db, "--json")
assert legacy_before["schema_version"]["tracked"] is False, legacy_before
assert legacy_before["schema_version"]["current_version"] == 1, legacy_before

legacy_migrate_1 = run_json(scripts["migrate"], legacy_db, "--json")
assert legacy_migrate_1["migration"]["version_before"] == 1, legacy_migrate_1
assert legacy_migrate_1["migration"]["version_after"] == 2, legacy_migrate_1
assert [migration["version"] for migration in legacy_migrate_1["migration"]["executed_migrations"]] == [2], legacy_migrate_1

legacy_migrate_2 = run_json(scripts["migrate"], legacy_db, "--json")
assert legacy_migrate_2["migration"]["operation"] == "already_current", legacy_migrate_2

legacy_after = run_json(scripts["show_version"], legacy_db, "--json")
assert legacy_after["schema_version"]["tracked"] is True, legacy_after
assert legacy_after["schema_version"]["current_version"] == 2, legacy_after

registration = run_json(
    scripts["register"],
    tmp_root / "packages" / "demo",
    "--sqlite-db",
    legacy_db,
    "--json",
)
assert registration["registration"]["project"]["project_key"] == "demo", registration

run_payload = run_json(
    scripts["create"],
    "--sqlite-db",
    legacy_db,
    "--project-key",
    "demo",
    "--project-profile",
    "default",
    "--workflow-id",
    "build",
    "--milestone",
    "migrated-db-flow",
    "--artifact-root",
    artifact_root,
    "--json",
)["run_details"]["run"]
paused = run_json(scripts["pause"], "--sqlite-db", legacy_db, run_payload["id"], "--json")
assert paused["manual_control"]["run"]["run"]["status"] == "paused", paused
resumed = run_json(scripts["resume"], "--sqlite-db", legacy_db, run_payload["id"], "--json")
assert resumed["manual_control"]["run"]["run"]["status"] == "queued", resumed

worker_tick = run_json(
    scripts["tick"],
    "--sqlite-db",
    legacy_db,
    "--context-json",
    context_path,
    "--artifact-root",
    artifact_root,
    "--worker-log-root",
    worker_log_root,
    "--json",
)["worker_tick"]
assert worker_tick["status"] == "progressed", worker_tick
assert worker_tick["final_run_status"] == "completed", worker_tick

show_run = run_json(scripts["show_run"], "--sqlite-db", legacy_db, run_payload["id"], "--json")
assert show_run["run_details"]["run"]["status"] == "completed", show_run

invalid_db = tmp_root / "invalid.sqlite"
connection = sqlite3.connect(invalid_db)
try:
    connection.executescript(baseline_sql)
    connection.execute(
        """
        CREATE TABLE schema_migrations (
          version INTEGER PRIMARY KEY CHECK (version >= 1),
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
        (2, "manual_control_paused", "2026-01-01T00:00:00Z"),
    )
    connection.commit()
finally:
    connection.close()
invalid = run_json(scripts["migrate"], invalid_db, "--json", expect_success=False)
assert invalid["error"]["code"] == "SQLITE_MIGRATION_INVALID_STATE", invalid

print(json.dumps({
    "fresh_operation": fresh_init["database"]["operation"],
    "legacy_before_state": legacy_before["schema_version"]["detected_state"],
    "legacy_first_migrate": legacy_migrate_1["migration"]["operation"],
    "legacy_second_migrate": legacy_migrate_2["migration"]["operation"],
    "migrated_run_id": run_payload["id"],
    "invalid_error_code": invalid["error"]["code"],
}, ensure_ascii=False, indent=2))
PY
