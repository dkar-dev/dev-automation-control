from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any

from .project_package import load_project_package
from .project_package_validator import ProjectPackageValidationFailed, RUNTIME_FILE
from .run_persistence import RunPersistenceError, _connect_run_db, _ensure_required_tables, _resolve_database_path, get_run


CONTROL_DIR = Path(__file__).resolve().parents[1]

RUNTIME_VALUE_REFS_BLOCK = "runtime_value_refs_v1"
RUNTIME_VALUE_REFS_BLOCK_PATH = f"{RUNTIME_FILE}.{RUNTIME_VALUE_REFS_BLOCK}"
RUNTIME_VALUE_CLASSIFICATIONS = ("secret", "sensitive_config", "plain_config")
RUNTIME_VALUE_SELECTIONS = ("all", "dispatch_env", "host_checks")
RUNTIME_VALUE_SOURCE_ENV = "env"
RUNTIME_VALUE_SOURCE_FILE = "file"
RUNTIME_VALUE_SOURCE_INLINE = "inline"
RUNTIME_VALUE_SENSITIVE_CLASSIFICATIONS = ("secret", "sensitive_config")

DEFAULT_RUNTIME_SECRETS_FILE_FROM_RUNTIME_ROOT = Path("secrets") / "runtime-secrets.json"
DEFAULT_RUNTIME_SECRETS_FILE_FROM_CONTROL_ROOT = Path(".control-plane-runtime-secrets.json")

RUNTIME_VALUE_BUNDLE_INVALID = "RUNTIME_VALUE_BUNDLE_INVALID"
RUNTIME_VALUE_CONFIG_INVALID = "RUNTIME_VALUE_CONFIG_INVALID"
RUNTIME_VALUE_LOCAL_FILE_INVALID = "RUNTIME_VALUE_LOCAL_FILE_INVALID"
RUNTIME_VALUE_PACKAGE_NOT_FOUND = "RUNTIME_VALUE_PACKAGE_NOT_FOUND"
RUNTIME_VALUE_REQUIRED_MISSING = "RUNTIME_VALUE_REQUIRED_MISSING"
RUNTIME_VALUE_REQUEST_INVALID = "RUNTIME_VALUE_REQUEST_INVALID"
RUNTIME_VALUE_STORAGE_ERROR = "RUNTIME_VALUE_STORAGE_ERROR"

_ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_CONTEXT_KEY_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SUPPORTED_SCALAR_TYPES = (str, int, float, bool)


@dataclass(frozen=True)
class RuntimeValueSpec:
    key: str
    classification: str
    required: bool
    refs: tuple[str, ...]
    inline_value: str | int | float | bool | None
    description: str | None
    dispatch_env: str | None
    host_check_context_key: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "classification": self.classification,
            "required": self.required,
            "refs": list(self.refs),
            "inline_value": self.inline_value if self.classification == "plain_config" else None,
            "description": self.description,
            "dispatch_env": self.dispatch_env,
            "host_check_context_key": self.host_check_context_key,
        }


@dataclass(frozen=True)
class RuntimeValueResolution:
    key: str
    classification: str
    required: bool
    resolved: bool
    source_type: str | None
    source_ref: str | None
    description: str
    dispatch_env: str | None
    host_check_context_key: str | None
    value: str | int | float | bool | None = None

    def redacted_value(self) -> str | int | float | bool | None:
        if not self.resolved:
            return None
        if self.classification == "plain_config":
            return self.value
        return redacted_runtime_value(self.key, self.classification)

    def to_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "classification": self.classification,
            "required": self.required,
            "resolved": self.resolved,
            "source_type": self.source_type,
            "source_ref": self.source_ref,
            "description": self.description,
            "dispatch_env": self.dispatch_env,
            "host_check_context_key": self.host_check_context_key,
            "value": self.redacted_value(),
        }


@dataclass(frozen=True)
class RuntimeValueInspection:
    package_root: Path
    config_block: str
    selection: str
    runtime_root: Path | None
    control_root: Path
    local_secrets_file: Path | None
    all_required_resolved: bool
    resolved_total: int
    selected_total: int
    missing_required_keys: tuple[str, ...]
    values: tuple[RuntimeValueResolution, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "package_root": str(self.package_root),
            "config_block": self.config_block,
            "selection": self.selection,
            "runtime_root": str(self.runtime_root) if self.runtime_root is not None else None,
            "control_root": str(self.control_root),
            "local_secrets_file": str(self.local_secrets_file) if self.local_secrets_file is not None else None,
            "all_required_resolved": self.all_required_resolved,
            "resolved_total": self.resolved_total,
            "selected_total": self.selected_total,
            "missing_required_keys": list(self.missing_required_keys),
            "values": [item.to_dict() for item in self.values],
        }


@dataclass(frozen=True)
class ResolvedRuntimeValueBundle:
    inspection: RuntimeValueInspection
    values: tuple[RuntimeValueResolution, ...]

    def to_dict(self) -> dict[str, object]:
        return self.inspection.to_dict()

    def runtime_values(self) -> dict[str, str | int | float | bool]:
        return {
            item.key: item.value
            for item in self.values
            if item.resolved and item.value is not None
        }

    def dispatch_env(self) -> dict[str, str]:
        env_payload: dict[str, str] = {}
        for item in self.values:
            if not item.resolved or item.dispatch_env is None or item.value is None:
                continue
            env_payload[item.dispatch_env] = stringify_runtime_value(item.value)
        return env_payload

    def host_check_context(self) -> dict[str, str | int | float | bool]:
        context_payload: dict[str, str | int | float | bool] = {}
        for item in self.values:
            if not item.resolved or item.host_check_context_key is None or item.value is None:
                continue
            context_payload[item.host_check_context_key] = item.value
        return context_payload

    def redactor(self) -> "RuntimeValueRedactor":
        return RuntimeValueRedactor(self.values)


class RuntimeValueRedactor:
    def __init__(self, values: Sequence[RuntimeValueResolution]) -> None:
        sensitive_pairs: list[tuple[str, str]] = []
        for item in values:
            if not item.resolved or item.value is None or item.classification not in RUNTIME_VALUE_SENSITIVE_CLASSIFICATIONS:
                continue
            rendered = stringify_runtime_value(item.value)
            if rendered:
                sensitive_pairs.append((rendered, redacted_runtime_value(item.key, item.classification)))
        sensitive_pairs.sort(key=lambda pair: len(pair[0]), reverse=True)
        self._sensitive_pairs = tuple(sensitive_pairs)

    def sanitize_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        sanitized = value
        for raw_value, replacement in self._sensitive_pairs:
            sanitized = sanitized.replace(raw_value, replacement)
        return sanitized

    def sanitize_object(self, value: object) -> object:
        if isinstance(value, str):
            return self.sanitize_text(value)
        if isinstance(value, Mapping):
            return {
                str(key): self.sanitize_object(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self.sanitize_object(item) for item in value]
        if isinstance(value, tuple):
            return [self.sanitize_object(item) for item in value]
        return value

    def sanitize_file(self, path: Path) -> None:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return
        sanitized = self.sanitize_text(raw)
        if sanitized != raw:
            path.write_text(sanitized or "", encoding="utf-8")


class RuntimeValueResolutionError(Exception):
    def __init__(self, code: str, message: str, details: str | None = None) -> None:
        self.code = code
        self.message = message
        self.details = details
        super().__init__(message)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


def redacted_runtime_value(key: str, classification: str) -> str:
    return f"[redacted:{classification}:{key}]"


def stringify_runtime_value(value: str | int | float | bool) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def resolve_runtime_value(
    package_root: str | Path,
    key: str,
    *,
    runtime_root: str | Path | None = None,
    control_root: str | Path | None = None,
    local_secrets_file: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> RuntimeValueResolution:
    bundle = resolve_runtime_value_bundle(
        package_root=package_root,
        selection="all",
        runtime_root=runtime_root,
        control_root=control_root,
        local_secrets_file=local_secrets_file,
        env=env,
        require_all_required=False,
    )
    for item in bundle.values:
        if item.key == key:
            return item
    raise RuntimeValueResolutionError(
        RUNTIME_VALUE_REQUEST_INVALID,
        f"runtime value key is not declared in {RUNTIME_VALUE_REFS_BLOCK_PATH}: {key}",
    )


def resolve_runtime_value_bundle(
    *,
    package_root: str | Path,
    selection: str = "all",
    runtime_root: str | Path | None = None,
    control_root: str | Path | None = None,
    local_secrets_file: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    require_all_required: bool = True,
) -> ResolvedRuntimeValueBundle:
    specs = load_runtime_value_specs(package_root)
    selected_specs = _select_runtime_value_specs(specs, selection=selection)
    resolved_runtime_root = _resolve_optional_path(runtime_root)
    resolved_control_root = _resolve_optional_path(control_root) or CONTROL_DIR
    local_values, resolved_local_secrets_file = _load_local_secret_values(
        runtime_root=resolved_runtime_root,
        control_root=resolved_control_root,
        local_secrets_file=local_secrets_file,
    )
    environment = env if env is not None else os.environ

    resolutions = tuple(
        _resolve_runtime_value_spec(
            spec,
            env=environment,
            local_values=local_values,
            local_secrets_file=resolved_local_secrets_file,
        )
        for spec in selected_specs
    )
    missing_required = tuple(
        item.key
        for item in resolutions
        if item.required and not item.resolved
    )
    inspection = RuntimeValueInspection(
        package_root=Path(package_root).expanduser().resolve(),
        config_block=RUNTIME_VALUE_REFS_BLOCK_PATH,
        selection=selection,
        runtime_root=resolved_runtime_root,
        control_root=resolved_control_root,
        local_secrets_file=resolved_local_secrets_file,
        all_required_resolved=not missing_required,
        resolved_total=sum(1 for item in resolutions if item.resolved),
        selected_total=len(resolutions),
        missing_required_keys=missing_required,
        values=resolutions,
    )
    bundle = ResolvedRuntimeValueBundle(inspection=inspection, values=resolutions)
    if require_all_required and missing_required:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_REQUIRED_MISSING,
            "required runtime values could not be resolved",
            details="missing_required_keys=" + ",".join(missing_required),
        )
    return bundle


def load_runtime_value_specs(package_root: str | Path) -> tuple[RuntimeValueSpec, ...]:
    resolved_package_root = Path(package_root).expanduser().resolve()
    try:
        project_package = load_project_package(resolved_package_root)
    except ProjectPackageValidationFailed as exc:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_PACKAGE_NOT_FOUND,
            f"project package is invalid: {resolved_package_root}",
            details="; ".join(f"{error.code}:{error.message}" for error in exc.errors),
        ) from exc

    runtime_doc = project_package.files[RUNTIME_FILE].data
    raw_block = runtime_doc.get(RUNTIME_VALUE_REFS_BLOCK)
    if raw_block is None:
        return tuple()
    if not isinstance(raw_block, Mapping):
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH} must be a mapping/object",
        )
    raw_values = raw_block.get("values")
    if not isinstance(raw_values, Mapping):
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values must be a mapping/object",
        )

    specs: list[RuntimeValueSpec] = []
    seen_keys: set[str] = set()
    for key, raw_value in raw_values.items():
        normalized_key = str(key).strip()
        if not normalized_key:
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_CONFIG_INVALID,
                f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values contains an empty key",
            )
        if normalized_key in seen_keys:
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_CONFIG_INVALID,
                f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values contains a duplicate key: {normalized_key}",
            )
        seen_keys.add(normalized_key)
        specs.append(_parse_runtime_value_spec(normalized_key, raw_value))
    return tuple(specs)


def resolve_runtime_value_bundle_for_selector(
    *,
    database_path: str | Path | None = None,
    run_id: str | None = None,
    project_key: str | None = None,
    package_root: str | Path | None = None,
    selection: str = "all",
    runtime_root: str | Path | None = None,
    control_root: str | Path | None = None,
    local_secrets_file: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    require_all_required: bool = True,
) -> ResolvedRuntimeValueBundle:
    resolved_package_root = resolve_runtime_value_package_root(
        database_path=database_path,
        run_id=run_id,
        project_key=project_key,
        package_root=package_root,
    )
    return resolve_runtime_value_bundle(
        package_root=resolved_package_root,
        selection=selection,
        runtime_root=runtime_root,
        control_root=control_root,
        local_secrets_file=local_secrets_file,
        env=env,
        require_all_required=require_all_required,
    )


def resolve_runtime_value_package_root(
    *,
    database_path: str | Path | None = None,
    run_id: str | None = None,
    project_key: str | None = None,
    package_root: str | Path | None = None,
) -> Path:
    if package_root is not None:
        return Path(package_root).expanduser().resolve()
    if run_id is not None:
        if database_path is None:
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_REQUEST_INVALID,
                "database_path is required with run_id",
            )
        try:
            run_details = get_run(database_path, run_id)
        except RunPersistenceError as exc:
            raise RuntimeValueResolutionError(exc.code, exc.message, exc.details) from exc
        return run_details.run.package_root
    if project_key is not None:
        if database_path is None:
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_REQUEST_INVALID,
                "database_path is required with project_key",
            )
        return _load_project_package_root(_resolve_database_path(database_path), project_key)
    raise RuntimeValueResolutionError(
        RUNTIME_VALUE_REQUEST_INVALID,
        "provide package_root, run_id, or database_path + project_key",
    )


def classification_policy() -> dict[str, dict[str, object]]:
    return {
        "secret": {
            "allowed_sources": [RUNTIME_VALUE_SOURCE_ENV, RUNTIME_VALUE_SOURCE_FILE],
            "allowed_inline_value": False,
            "logging": "redacted_only",
            "persistence": "no_raw_persistence",
        },
        "sensitive_config": {
            "allowed_sources": [RUNTIME_VALUE_SOURCE_ENV, RUNTIME_VALUE_SOURCE_FILE],
            "allowed_inline_value": False,
            "logging": "redacted_only",
            "persistence": "no_raw_persistence",
        },
        "plain_config": {
            "allowed_sources": [RUNTIME_VALUE_SOURCE_ENV, RUNTIME_VALUE_SOURCE_FILE, RUNTIME_VALUE_SOURCE_INLINE],
            "allowed_inline_value": True,
            "logging": "plain_allowed",
            "persistence": "plain_allowed",
        },
    }


def _select_runtime_value_specs(
    specs: Sequence[RuntimeValueSpec],
    *,
    selection: str,
) -> tuple[RuntimeValueSpec, ...]:
    if selection not in RUNTIME_VALUE_SELECTIONS:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_REQUEST_INVALID,
            f"selection must be one of: {', '.join(RUNTIME_VALUE_SELECTIONS)}",
            details=f"actual={selection}",
        )
    if selection == "all":
        return tuple(specs)
    if selection == "dispatch_env":
        return tuple(spec for spec in specs if spec.dispatch_env is not None)
    return tuple(spec for spec in specs if spec.host_check_context_key is not None)


def _parse_runtime_value_spec(key: str, raw_value: object) -> RuntimeValueSpec:
    if not isinstance(raw_value, Mapping):
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key} must be a mapping/object",
        )
    classification = _required_text(raw_value.get("classification"), f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.classification")
    if classification not in RUNTIME_VALUE_CLASSIFICATIONS:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.classification must be one of: {', '.join(RUNTIME_VALUE_CLASSIFICATIONS)}",
            details=f"actual={classification}",
        )

    required = raw_value.get("required", False)
    if not isinstance(required, bool):
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.required must be a boolean",
        )

    refs = _normalize_runtime_value_refs(raw_value.get("refs"), raw_value.get("ref"), key=key)
    inline_value = raw_value.get("inline_value")
    if inline_value is not None and not isinstance(inline_value, _SUPPORTED_SCALAR_TYPES):
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.inline_value must be a scalar string/number/boolean",
            details=f"actual_type={type(inline_value).__name__}",
        )
    if classification in RUNTIME_VALUE_SENSITIVE_CLASSIFICATIONS and inline_value is not None:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.inline_value is not allowed for classification {classification}",
        )
    if classification == "plain_config" and not refs and inline_value is None:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key} must declare refs and/or inline_value",
        )
    if classification in RUNTIME_VALUE_SENSITIVE_CLASSIFICATIONS and not refs:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key} must declare at least one ref",
        )

    dispatch_env = _optional_text(raw_value.get("dispatch_env"))
    if dispatch_env is not None and not _ENV_NAME_RE.match(dispatch_env):
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.dispatch_env must be an upper-case env var name",
            details=f"actual={dispatch_env}",
        )
    host_check_context_key = _optional_text(raw_value.get("host_check_context_key"))
    if host_check_context_key is not None and not _CONTEXT_KEY_RE.match(host_check_context_key):
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.host_check_context_key must be an identifier-like key",
            details=f"actual={host_check_context_key}",
        )

    return RuntimeValueSpec(
        key=key,
        classification=classification,
        required=required,
        refs=refs,
        inline_value=inline_value if isinstance(inline_value, _SUPPORTED_SCALAR_TYPES) else None,
        description=_optional_text(raw_value.get("description")),
        dispatch_env=dispatch_env,
        host_check_context_key=host_check_context_key,
    )


def _normalize_runtime_value_refs(raw_refs: object, raw_ref: object, *, key: str) -> tuple[str, ...]:
    refs: list[str] = []
    if raw_ref is not None:
        refs.append(_required_text(raw_ref, f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.ref"))
    if raw_refs is not None:
        if isinstance(raw_refs, str):
            normalized = raw_refs.strip()
            if normalized:
                refs.append(normalized)
        elif isinstance(raw_refs, Sequence) and not isinstance(raw_refs, (str, bytes, bytearray)):
            for index, item in enumerate(raw_refs):
                refs.append(_required_text(item, f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.refs[{index}]"))
        else:
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_CONFIG_INVALID,
                f"{RUNTIME_VALUE_REFS_BLOCK_PATH}.values.{key}.refs must be a string or list of strings",
            )
    normalized_refs: list[str] = []
    for ref in refs:
        source_type, source_key = _parse_source_ref(ref)
        if source_type == RUNTIME_VALUE_SOURCE_ENV and not _ENV_NAME_RE.match(source_key):
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_CONFIG_INVALID,
                f"env ref must use an upper-case env var name: {ref}",
            )
        normalized_refs.append(f"{source_type}:{source_key}")
    return tuple(normalized_refs)


def _parse_source_ref(ref: str) -> tuple[str, str]:
    normalized = ref.strip()
    if ":" not in normalized:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"runtime value refs must use env:<NAME> or file:<key>: {ref}",
        )
    source_type, source_key = normalized.split(":", 1)
    source_type = source_type.strip().lower()
    source_key = source_key.strip()
    if source_type not in {RUNTIME_VALUE_SOURCE_ENV, RUNTIME_VALUE_SOURCE_FILE}:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"unsupported runtime value source ref: {ref}",
        )
    if not source_key:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"runtime value source key must not be empty: {ref}",
        )
    return source_type, source_key


def _load_local_secret_values(
    *,
    runtime_root: Path | None,
    control_root: Path,
    local_secrets_file: str | Path | None,
) -> tuple[dict[str, str], Path | None]:
    candidate_paths: list[tuple[Path, bool]] = []
    if local_secrets_file is not None:
        candidate_paths.append((Path(local_secrets_file).expanduser().resolve(), True))
    if runtime_root is not None:
        candidate_paths.append(((runtime_root / DEFAULT_RUNTIME_SECRETS_FILE_FROM_RUNTIME_ROOT).resolve(), False))
    candidate_paths.append(((control_root / DEFAULT_RUNTIME_SECRETS_FILE_FROM_CONTROL_ROOT).resolve(), False))

    for candidate_path, explicit in candidate_paths:
        if not candidate_path.exists():
            if explicit:
                raise RuntimeValueResolutionError(
                    RUNTIME_VALUE_LOCAL_FILE_INVALID,
                    f"local secrets file does not exist: {candidate_path}",
                )
            continue
        if not candidate_path.is_file():
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_LOCAL_FILE_INVALID,
                f"local secrets file path is not a file: {candidate_path}",
            )
        try:
            payload = json.loads(candidate_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_LOCAL_FILE_INVALID,
                f"failed to load local secrets file: {candidate_path}",
                details=str(exc),
            ) from exc
        if not isinstance(payload, Mapping):
            raise RuntimeValueResolutionError(
                RUNTIME_VALUE_LOCAL_FILE_INVALID,
                f"local secrets file must contain a JSON object: {candidate_path}",
            )
        values: dict[str, str] = {}
        for key, value in payload.items():
            normalized_key = str(key).strip()
            if not normalized_key:
                continue
            if isinstance(value, _SUPPORTED_SCALAR_TYPES):
                values[normalized_key] = stringify_runtime_value(value)
        return values, candidate_path
    return {}, None


def _resolve_runtime_value_spec(
    spec: RuntimeValueSpec,
    *,
    env: Mapping[str, str],
    local_values: Mapping[str, str],
    local_secrets_file: Path | None,
) -> RuntimeValueResolution:
    for ref in spec.refs:
        source_type, source_key = _parse_source_ref(ref)
        if source_type == RUNTIME_VALUE_SOURCE_ENV:
            raw_value = env.get(source_key)
            normalized_value = _optional_text(raw_value)
            if normalized_value is not None:
                return RuntimeValueResolution(
                    key=spec.key,
                    classification=spec.classification,
                    required=spec.required,
                    resolved=True,
                    source_type=RUNTIME_VALUE_SOURCE_ENV,
                    source_ref=f"env:{source_key}",
                    description=f"resolved from env ref env:{source_key}",
                    dispatch_env=spec.dispatch_env,
                    host_check_context_key=spec.host_check_context_key,
                    value=normalized_value,
                )
            continue
        file_value = _optional_text(local_values.get(source_key))
        if file_value is not None:
            source_description = f"resolved from file ref file:{source_key}"
            if local_secrets_file is not None:
                source_description += f" via {local_secrets_file}"
            return RuntimeValueResolution(
                key=spec.key,
                classification=spec.classification,
                required=spec.required,
                resolved=True,
                source_type=RUNTIME_VALUE_SOURCE_FILE,
                source_ref=f"file:{source_key}",
                description=source_description,
                dispatch_env=spec.dispatch_env,
                host_check_context_key=spec.host_check_context_key,
                value=file_value,
            )

    if spec.inline_value is not None:
        return RuntimeValueResolution(
            key=spec.key,
            classification=spec.classification,
            required=spec.required,
            resolved=True,
            source_type=RUNTIME_VALUE_SOURCE_INLINE,
            source_ref="inline_value",
            description="resolved from inline non-secret config value",
            dispatch_env=spec.dispatch_env,
            host_check_context_key=spec.host_check_context_key,
            value=spec.inline_value,
        )

    return RuntimeValueResolution(
        key=spec.key,
        classification=spec.classification,
        required=spec.required,
        resolved=False,
        source_type=None,
        source_ref=None,
        description=(
            "runtime value is not resolved; checked refs: "
            + ", ".join(spec.refs)
            if spec.refs
            else "runtime value is not resolved"
        ),
        dispatch_env=spec.dispatch_env,
        host_check_context_key=spec.host_check_context_key,
        value=None,
    )


def _load_project_package_root(database_path: Path, project_key: str) -> Path:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("projects",))
        row = connection.execute(
            """
            SELECT package_root
            FROM projects
            WHERE project_key = ?
            LIMIT 1
            """,
            (project_key,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_STORAGE_ERROR,
            "failed to load registered project package root",
            details=str(exc),
        ) from exc
    finally:
        connection.close()
    if row is None:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_PACKAGE_NOT_FOUND,
            f"project is not registered in SQLite: {project_key}",
        )
    return Path(str(row["package_root"])).expanduser().resolve()


def _required_text(value: object, field_name: str) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise RuntimeValueResolutionError(
            RUNTIME_VALUE_CONFIG_INVALID,
            f"{field_name} must be a non-empty string",
        )
    return normalized


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _resolve_optional_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    return Path(value).expanduser().resolve()
