#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <instructions-repo-path> <instruction-profile> [overlay...]" >&2
  exit 1
fi

INSTRUCTIONS_REPO_PATH="$1"
INSTRUCTION_PROFILE="$2"
shift 2

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

require_cmd python3

python3 - "$INSTRUCTIONS_REPO_PATH" "$INSTRUCTION_PROFILE" "$@" <<'PY'
import sys
from pathlib import Path

instructions_repo = Path(sys.argv[1]).expanduser().resolve()
instruction_profile = sys.argv[2].strip()
overlays = [value.strip() for value in sys.argv[3:]]

errors: list[str] = []

if not instructions_repo.exists():
    errors.append(f"Instructions repo path does not exist: {instructions_repo}")
elif not instructions_repo.is_dir():
    errors.append(f"Instructions repo path is not a directory: {instructions_repo}")

if not instruction_profile:
    errors.append("instruction_profile must not be empty")

if instructions_repo.is_dir() and instruction_profile:
    profile_dir = instructions_repo / "profiles" / instruction_profile
    if not profile_dir.is_dir():
        errors.append(f"Missing profile directory: {profile_dir}")
    else:
        for role in ("executor", "reviewer"):
            required_file = profile_dir / f"{role}.md"
            if not required_file.is_file():
                errors.append(f"Missing required profile file: {required_file}")

for overlay_name in overlays:
    if not overlay_name:
        errors.append("overlay names must not be empty")
        continue

    overlay_file = instructions_repo / "overlays" / f"{overlay_name}.md"
    overlay_dir = instructions_repo / "overlays" / overlay_name
    overlay_candidates = [
        overlay_file,
        overlay_dir / "shared.md",
        overlay_dir / "executor.md",
        overlay_dir / "reviewer.md",
    ]

    if not any(candidate.is_file() for candidate in overlay_candidates):
        errors.append(
            "Overlay not found or empty: "
            f"{overlay_name} (expected one of: {overlay_file}, "
            f"{overlay_dir / 'shared.md'}, {overlay_dir / 'executor.md'}, "
            f"{overlay_dir / 'reviewer.md'})"
        )

if errors:
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    raise SystemExit(1)

print(f"Instructions repo is valid: {instructions_repo}")
print(f"Profile: {instruction_profile}")
print("Overlays: " + (", ".join(overlays) if overlays else "(none)"))
PY
