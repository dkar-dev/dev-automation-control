#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <executor|reviewer>" >&2
  exit 1
fi

ROLE="$1"
case "$ROLE" in
  executor|reviewer) ;;
  *)
    echo "Role must be executor or reviewer" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_cmd git
require_cmd python3

ensure_file "$STATE_FILE"

export STATE_FILE ROLE

MANIFEST_PATH="$(
python3 <<'PY'
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

state_file = Path(os.environ["STATE_FILE"])
role = os.environ["ROLE"]

state = json.loads(state_file.read_text(encoding="utf-8"))

instruction_profile = (state.get("instruction_profile") or "").strip()
instruction_overlays = state.get("instruction_overlays") or []
instructions_repo_path = (state.get("instructions_repo_path") or "").strip()

if not instruction_profile:
    raise SystemExit("instruction_profile is missing from the current run")
if not isinstance(instruction_overlays, list):
    raise SystemExit("instruction_overlays must be an array")
if any(not isinstance(item, str) or not item.strip() for item in instruction_overlays):
    raise SystemExit("instruction_overlays must contain only non-empty strings")
if not instructions_repo_path:
    raise SystemExit("instructions_repo_path is missing from the current run")

instructions_repo = Path(instructions_repo_path).expanduser().resolve()
if not instructions_repo.is_dir():
    raise SystemExit(f"Instructions repo not found: {instructions_repo}")

profile_dir = instructions_repo / "profiles" / instruction_profile
if not profile_dir.is_dir():
    raise SystemExit(f"Instruction profile not found: {profile_dir}")

role_file = profile_dir / f"{role}.md"
if not role_file.is_file():
    raise SystemExit(f"Missing required instruction file: {role_file}")

resolved_files: list[Path] = []

def add_if_file(path: Path) -> None:
    if path.is_file():
        resolved_files.append(path.resolve())

add_if_file(profile_dir / "shared.md")
add_if_file(role_file)

for overlay_name in instruction_overlays:
    overlay_name = overlay_name.strip()
    overlay_file = instructions_repo / "overlays" / f"{overlay_name}.md"
    overlay_dir = instructions_repo / "overlays" / overlay_name
    overlay_any = any(
        candidate.is_file()
        for candidate in [
            overlay_file,
            overlay_dir / "shared.md",
            overlay_dir / "executor.md",
            overlay_dir / "reviewer.md",
        ]
    )
    if not overlay_any:
        raise SystemExit(f"Instruction overlay not found: {overlay_name}")

    add_if_file(overlay_file)
    add_if_file(overlay_dir / "shared.md")
    add_if_file(overlay_dir / f"{role}.md")

try:
    instructions_revision = subprocess.check_output(
        ["git", "-C", str(instructions_repo), "rev-parse", "HEAD"],
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip()
except subprocess.CalledProcessError:
    instructions_revision = "unversioned"

resolved_files_str = [str(path) for path in resolved_files]
resolved_union = []
for path in list(state.get("resolved_instruction_files") or []) + resolved_files_str:
    if path not in resolved_union:
        resolved_union.append(path)

now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
state["instructions_revision"] = instructions_revision
state["resolved_instruction_files"] = resolved_union
state["timestamps"]["updated_at"] = now_iso

run_dir = Path(state["paths"]["run_dir"])
run_dir.mkdir(parents=True, exist_ok=True)

manifest = {
    "role": role,
    "instruction_profile": instruction_profile,
    "instruction_overlays": instruction_overlays,
    "instructions_repo_path": str(instructions_repo),
    "instructions_revision": instructions_revision,
    "resolved_instruction_files": resolved_files_str,
}

manifest_path = run_dir / f"resolved-{role}-instructions.json"
manifest_path.write_text(
    json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)

state_file.write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
(run_dir / "state.json").write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)

print(manifest_path)
PY
)"

echo "$MANIFEST_PATH"
