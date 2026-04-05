from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


PROJECT_FILE = "project.yaml"
WORKFLOW_FILE = "workflow.yaml"
POLICY_FILE = "policy.yaml"
RUNTIME_FILE = "runtime.yaml"
INSTRUCTIONS_FILE = "instructions.yaml"
CAPABILITIES_FILE = "capabilities.yaml"
BOUNDED_CONTRACT_POLICY_BLOCK = "bounded_contract_generation_v1"
BOUNDED_CONTRACT_STORAGE_MODEL = "project_package_policy_v1"
HOST_CHECKS_RUNTIME_BLOCK = "host_checks_v1"
RUNTIME_VALUE_REFS_BLOCK = "runtime_value_refs_v1"
HOST_CHECK_KINDS = (
    "command_check",
    "http_check",
    "file_check",
    "process_check",
)
HOST_CHECK_SEVERITIES = ("required", "advisory")
RUNTIME_VALUE_CLASSIFICATIONS = ("secret", "sensitive_config", "plain_config")
BOUNDED_CONTRACT_TAXONOMY = (
    "implementation_step",
    "inspection_step",
    "recovery_step",
    "manual_followup_step",
)
BOUNDED_CONTRACT_TARGET_ROLES = ("executor", "reviewer", "manual")

REQUIRED_YAML_FILES = (
    PROJECT_FILE,
    WORKFLOW_FILE,
    POLICY_FILE,
    RUNTIME_FILE,
    INSTRUCTIONS_FILE,
    CAPABILITIES_FILE,
)

PACKAGE_DIRECTORY_MISSING = "PACKAGE_DIRECTORY_MISSING"
FILE_MISSING = "FILE_MISSING"
INVALID_YAML = "INVALID_YAML"
WRONG_ROOT_TYPE = "WRONG_ROOT_TYPE"
MISSING_REQUIRED_KEY = "MISSING_REQUIRED_KEY"
WRONG_KEY_TYPE = "WRONG_KEY_TYPE"


@dataclass(frozen=True)
class ValidationError:
    code: str
    message: str
    package_root: Path
    file_path: Path | None = None
    key_path: str | None = None
    details: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "code": self.code,
            "message": self.message,
            "package_root": str(self.package_root),
            "file_path": str(self.file_path) if self.file_path else None,
            "key_path": self.key_path,
            "details": self.details,
        }
        return payload


class ProjectPackageValidationFailed(Exception):
    def __init__(self, package_root: Path, errors: list[ValidationError]) -> None:
        self.package_root = package_root
        self.errors = tuple(errors)
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        return "; ".join(f"{error.code}: {error.message}" for error in self.errors)


@dataclass(frozen=True)
class ValidatedYamlDocument:
    filename: str
    path: Path
    data: dict[str, Any]


@dataclass(frozen=True)
class ValidatedProjectPackage:
    project_key: str
    package_root: Path
    files: dict[str, ValidatedYamlDocument]


def validate_project_package(package_root: str | Path) -> ValidatedProjectPackage:
    resolved_root = Path(package_root).expanduser().resolve()
    errors: list[ValidationError] = []

    if not resolved_root.exists():
        errors.append(
            ValidationError(
                code=PACKAGE_DIRECTORY_MISSING,
                message=f"Project package directory does not exist: {resolved_root}",
                package_root=resolved_root,
            )
        )
        raise ProjectPackageValidationFailed(resolved_root, errors)

    if not resolved_root.is_dir():
        errors.append(
            ValidationError(
                code=PACKAGE_DIRECTORY_MISSING,
                message=f"Project package path is not a directory: {resolved_root}",
                package_root=resolved_root,
            )
        )
        raise ProjectPackageValidationFailed(resolved_root, errors)

    files: dict[str, ValidatedYamlDocument] = {}
    for filename in REQUIRED_YAML_FILES:
        file_path = resolved_root / filename
        if not file_path.is_file():
            errors.append(
                ValidationError(
                    code=FILE_MISSING,
                    message=f"Missing required YAML file: {filename}",
                    package_root=resolved_root,
                    file_path=file_path,
                )
            )
            continue

        try:
            loaded = yaml.safe_load(file_path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            errors.append(
                ValidationError(
                    code=INVALID_YAML,
                    message=f"Invalid YAML in {filename}",
                    package_root=resolved_root,
                    file_path=file_path,
                    details=str(exc),
                )
            )
            continue

        if not isinstance(loaded, dict):
            errors.append(
                ValidationError(
                    code=WRONG_ROOT_TYPE,
                    message=f"YAML root in {filename} must be a mapping, got {_yaml_type_name(loaded)}",
                    package_root=resolved_root,
                    file_path=file_path,
                    details=f"root_type={_yaml_type_name(loaded)}",
                )
            )
            continue

        files[filename] = ValidatedYamlDocument(filename=filename, path=file_path, data=loaded)

    _validate_required_keys(package_root=resolved_root, files=files, errors=errors)

    if errors:
        raise ProjectPackageValidationFailed(resolved_root, errors)

    return ValidatedProjectPackage(
        project_key=resolved_root.name,
        package_root=resolved_root,
        files=files,
    )


def _validate_required_keys(
    package_root: Path,
    files: dict[str, ValidatedYamlDocument],
    errors: list[ValidationError],
) -> None:
    project_doc = files.get(PROJECT_FILE)
    if project_doc is not None:
        if "schema_version" not in project_doc.data:
            errors.append(
                ValidationError(
                    code=MISSING_REQUIRED_KEY,
                    message="Missing required key: schema_version",
                    package_root=package_root,
                    file_path=project_doc.path,
                    key_path="schema_version",
                )
            )
        elif not isinstance(project_doc.data["schema_version"], str):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message="project.yaml.schema_version must be a string",
                    package_root=package_root,
                    file_path=project_doc.path,
                    key_path="schema_version",
                    details=f"actual_type={_yaml_type_name(project_doc.data['schema_version'])}",
                )
            )

    capabilities_doc = files.get(CAPABILITIES_FILE)
    if capabilities_doc is not None:
        if "sections" not in capabilities_doc.data:
            errors.append(
                ValidationError(
                    code=MISSING_REQUIRED_KEY,
                    message="Missing required key: sections",
                    package_root=package_root,
                    file_path=capabilities_doc.path,
                    key_path="sections",
                )
            )
        elif not isinstance(capabilities_doc.data["sections"], dict):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message="capabilities.yaml.sections must be a mapping",
                    package_root=package_root,
                    file_path=capabilities_doc.path,
                    key_path="sections",
                    details=f"actual_type={_yaml_type_name(capabilities_doc.data['sections'])}",
                )
            )

    policy_doc = files.get(POLICY_FILE)
    if policy_doc is not None:
        _validate_bounded_contract_policy_block(package_root, policy_doc, errors)

    runtime_doc = files.get(RUNTIME_FILE)
    if runtime_doc is not None:
        _validate_host_checks_runtime_block(package_root, runtime_doc, errors)
        _validate_runtime_value_refs_block(package_root, runtime_doc, errors)


def _validate_bounded_contract_policy_block(
    package_root: Path,
    policy_doc: ValidatedYamlDocument,
    errors: list[ValidationError],
) -> None:
    raw_block = policy_doc.data.get(BOUNDED_CONTRACT_POLICY_BLOCK)
    if raw_block is None:
        return
    if not isinstance(raw_block, dict):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"policy.yaml.{BOUNDED_CONTRACT_POLICY_BLOCK} must be a mapping",
                package_root=package_root,
                file_path=policy_doc.path,
                key_path=BOUNDED_CONTRACT_POLICY_BLOCK,
                details=f"actual_type={_yaml_type_name(raw_block)}",
            )
        )
        return

    storage_model = raw_block.get("storage_model")
    if storage_model is not None and not isinstance(storage_model, str):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"policy.yaml.{BOUNDED_CONTRACT_POLICY_BLOCK}.storage_model must be a string",
                package_root=package_root,
                file_path=policy_doc.path,
                key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.storage_model",
                details=f"actual_type={_yaml_type_name(storage_model)}",
            )
        )
    elif isinstance(storage_model, str) and storage_model != BOUNDED_CONTRACT_STORAGE_MODEL:
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"policy.yaml.{BOUNDED_CONTRACT_POLICY_BLOCK}.storage_model must be {BOUNDED_CONTRACT_STORAGE_MODEL}",
                package_root=package_root,
                file_path=policy_doc.path,
                key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.storage_model",
                details=f"actual={storage_model}",
            )
        )

    defaults = raw_block.get("defaults")
    templates = raw_block.get("templates")
    if defaults is None:
        errors.append(
            ValidationError(
                code=MISSING_REQUIRED_KEY,
                message=f"Missing required key: {BOUNDED_CONTRACT_POLICY_BLOCK}.defaults",
                package_root=package_root,
                file_path=policy_doc.path,
                key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.defaults",
            )
        )
    elif not isinstance(defaults, dict):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"policy.yaml.{BOUNDED_CONTRACT_POLICY_BLOCK}.defaults must be a mapping",
                package_root=package_root,
                file_path=policy_doc.path,
                key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.defaults",
                details=f"actual_type={_yaml_type_name(defaults)}",
            )
        )
    if templates is None:
        errors.append(
            ValidationError(
                code=MISSING_REQUIRED_KEY,
                message=f"Missing required key: {BOUNDED_CONTRACT_POLICY_BLOCK}.templates",
                package_root=package_root,
                file_path=policy_doc.path,
                key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.templates",
            )
        )
        return
    if not isinstance(templates, dict):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"policy.yaml.{BOUNDED_CONTRACT_POLICY_BLOCK}.templates must be a mapping",
                package_root=package_root,
                file_path=policy_doc.path,
                key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.templates",
                details=f"actual_type={_yaml_type_name(templates)}",
            )
        )
        return

    if isinstance(defaults, dict):
        for contract_type, template_key in defaults.items():
            if contract_type not in BOUNDED_CONTRACT_TAXONOMY:
                errors.append(
                    ValidationError(
                        code=WRONG_KEY_TYPE,
                        message=f"Unknown bounded contract taxonomy key: {contract_type}",
                        package_root=package_root,
                        file_path=policy_doc.path,
                        key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.defaults.{contract_type}",
                    )
                )
            if not isinstance(template_key, str):
                errors.append(
                    ValidationError(
                        code=WRONG_KEY_TYPE,
                        message="Default bounded contract template keys must be strings",
                        package_root=package_root,
                        file_path=policy_doc.path,
                        key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.defaults.{contract_type}",
                        details=f"actual_type={_yaml_type_name(template_key)}",
                    )
                )
            elif template_key not in templates:
                errors.append(
                    ValidationError(
                        code=MISSING_REQUIRED_KEY,
                        message=f"Default bounded contract template is missing from templates: {template_key}",
                        package_root=package_root,
                        file_path=policy_doc.path,
                        key_path=f"{BOUNDED_CONTRACT_POLICY_BLOCK}.defaults.{contract_type}",
                    )
                )

    for template_name, template_value in templates.items():
        template_key_path = f"{BOUNDED_CONTRACT_POLICY_BLOCK}.templates.{template_name}"
        if not isinstance(template_value, dict):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"{template_key_path} must be a mapping",
                    package_root=package_root,
                    file_path=policy_doc.path,
                    key_path=template_key_path,
                    details=f"actual_type={_yaml_type_name(template_value)}",
                )
            )
            continue
        _validate_template_mapping(package_root, policy_doc.path, template_key_path, template_value, errors)


def _validate_template_mapping(
    package_root: Path,
    policy_path: Path,
    key_path: str,
    template_value: dict[str, Any],
    errors: list[ValidationError],
) -> None:
    _require_string_key(package_root, policy_path, key_path, template_value, "contract_type", errors)
    _require_string_key(package_root, policy_path, key_path, template_value, "target_role", errors)
    contract_type = template_value.get("contract_type")
    if isinstance(contract_type, str) and contract_type not in BOUNDED_CONTRACT_TAXONOMY:
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_path}.contract_type must be one of: {', '.join(BOUNDED_CONTRACT_TAXONOMY)}",
                package_root=package_root,
                file_path=policy_path,
                key_path=f"{key_path}.contract_type",
                details=f"actual={contract_type}",
            )
        )
    target_role = template_value.get("target_role")
    if isinstance(target_role, str) and target_role not in BOUNDED_CONTRACT_TARGET_ROLES:
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_path}.target_role must be one of: {', '.join(BOUNDED_CONTRACT_TARGET_ROLES)}",
                package_root=package_root,
                file_path=policy_path,
                key_path=f"{key_path}.target_role",
                details=f"actual={target_role}",
            )
        )
    _require_mapping_key(package_root, policy_path, key_path, template_value, "contract", errors)
    for list_key in (
        "allowed_workflow_ids",
        "allowed_project_profiles",
        "allowed_run_statuses",
        "allowed_queue_statuses",
        "allowed_origin_types",
        "required_runtime_fields",
        "allowed_capability_sections",
        "required_state_tags",
        "required_any_state_tags",
        "forbidden_state_tags",
    ):
        _validate_optional_string_list(package_root, policy_path, f"{key_path}.{list_key}", template_value.get(list_key), errors)
    contract = template_value.get("contract")
    if isinstance(contract, dict):
        _require_string_key(package_root, policy_path, f"{key_path}.contract", contract, "summary", errors)
        _require_string_key(package_root, policy_path, f"{key_path}.contract", contract, "objective", errors)
        for list_key in ("deliverables", "allowed_actions", "forbidden_actions", "output_requirements", "boundary_notes"):
            _validate_optional_string_list(package_root, policy_path, f"{key_path}.contract.{list_key}", contract.get(list_key), errors, required=True)


def _validate_host_checks_runtime_block(
    package_root: Path,
    runtime_doc: ValidatedYamlDocument,
    errors: list[ValidationError],
) -> None:
    raw_block = runtime_doc.data.get(HOST_CHECKS_RUNTIME_BLOCK)
    if raw_block is None:
        return
    if not isinstance(raw_block, dict):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"runtime.yaml.{HOST_CHECKS_RUNTIME_BLOCK} must be a mapping",
                package_root=package_root,
                file_path=runtime_doc.path,
                key_path=HOST_CHECKS_RUNTIME_BLOCK,
                details=f"actual_type={_yaml_type_name(raw_block)}",
            )
        )
        return

    checks = raw_block.get("checks")
    if checks is None:
        errors.append(
            ValidationError(
                code=MISSING_REQUIRED_KEY,
                message=f"Missing required key: {HOST_CHECKS_RUNTIME_BLOCK}.checks",
                package_root=package_root,
                file_path=runtime_doc.path,
                key_path=f"{HOST_CHECKS_RUNTIME_BLOCK}.checks",
            )
        )
        return
    if not isinstance(checks, list):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"runtime.yaml.{HOST_CHECKS_RUNTIME_BLOCK}.checks must be a list",
                package_root=package_root,
                file_path=runtime_doc.path,
                key_path=f"{HOST_CHECKS_RUNTIME_BLOCK}.checks",
                details=f"actual_type={_yaml_type_name(checks)}",
            )
        )
        return

    seen_ids: set[str] = set()
    for index, raw_check in enumerate(checks):
        key_path = f"{HOST_CHECKS_RUNTIME_BLOCK}.checks[{index}]"
        if not isinstance(raw_check, dict):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path} must be a mapping",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=key_path,
                    details=f"actual_type={_yaml_type_name(raw_check)}",
                )
            )
            continue
        _require_string_key(package_root, runtime_doc.path, key_path, raw_check, "id", errors)
        _require_string_key(package_root, runtime_doc.path, key_path, raw_check, "kind", errors)
        _require_boolean_key(package_root, runtime_doc.path, key_path, raw_check, "enabled", errors)
        _require_string_key(package_root, runtime_doc.path, key_path, raw_check, "severity", errors)
        _require_positive_integer_key(package_root, runtime_doc.path, key_path, raw_check, "timeout_seconds", errors)
        _require_mapping_key(package_root, runtime_doc.path, key_path, raw_check, "success", errors)
        _validate_optional_string_list(package_root, runtime_doc.path, f"{key_path}.allowed_workflow_ids", raw_check.get("allowed_workflow_ids"), errors)
        _validate_optional_string_list(package_root, runtime_doc.path, f"{key_path}.allowed_project_profiles", raw_check.get("allowed_project_profiles"), errors)

        check_id = raw_check.get("id")
        if isinstance(check_id, str):
            if check_id in seen_ids:
                errors.append(
                    ValidationError(
                        code=WRONG_KEY_TYPE,
                        message=f"Duplicate host check id: {check_id}",
                        package_root=package_root,
                        file_path=runtime_doc.path,
                        key_path=f"{key_path}.id",
                    )
                )
            seen_ids.add(check_id)

        kind = raw_check.get("kind")
        if isinstance(kind, str) and kind not in HOST_CHECK_KINDS:
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"{key_path}.kind must be one of: {', '.join(HOST_CHECK_KINDS)}",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.kind",
                    details=f"actual={kind}",
                )
            )

        severity = raw_check.get("severity")
        if isinstance(severity, str) and severity not in HOST_CHECK_SEVERITIES:
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"{key_path}.severity must be one of: {', '.join(HOST_CHECK_SEVERITIES)}",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.severity",
                    details=f"actual={severity}",
                )
            )

        success = raw_check.get("success")
        if not isinstance(success, dict):
            continue
        if kind == "command_check":
            command = raw_check.get("command")
            if not (isinstance(command, str) and command.strip()) and not _is_string_list(command):
                errors.append(
                    ValidationError(
                        code=WRONG_KEY_TYPE,
                        message=f"{key_path}.command must be a string or list of strings",
                        package_root=package_root,
                        file_path=runtime_doc.path,
                        key_path=f"{key_path}.command",
                        details=f"actual_type={_yaml_type_name(command)}",
                    )
                )
            _validate_optional_integer(package_root, runtime_doc.path, f"{key_path}.success.exit_code", success.get("exit_code"), errors)
            _validate_optional_string(package_root, runtime_doc.path, f"{key_path}.success.stdout_contains", success.get("stdout_contains"), errors)
            _validate_optional_string(package_root, runtime_doc.path, f"{key_path}.success.stderr_contains", success.get("stderr_contains"), errors)
        elif kind == "http_check":
            _require_string_key(package_root, runtime_doc.path, key_path, raw_check, "url", errors)
            headers = raw_check.get("headers")
            if headers is not None and not isinstance(headers, dict):
                errors.append(
                    ValidationError(
                        code=WRONG_KEY_TYPE,
                        message=f"{key_path}.headers must be a mapping",
                        package_root=package_root,
                        file_path=runtime_doc.path,
                        key_path=f"{key_path}.headers",
                        details=f"actual_type={_yaml_type_name(headers)}",
                    )
                )
            elif isinstance(headers, dict):
                for header_name, header_value in headers.items():
                    if not isinstance(header_name, str) or not isinstance(header_value, str):
                        errors.append(
                            ValidationError(
                                code=WRONG_KEY_TYPE,
                                message=f"{key_path}.headers entries must use string keys and values",
                                package_root=package_root,
                                file_path=runtime_doc.path,
                                key_path=f"{key_path}.headers",
                            )
                        )
                        break
            _validate_optional_integer(package_root, runtime_doc.path, f"{key_path}.success.status_code", success.get("status_code"), errors)
            _validate_optional_string(package_root, runtime_doc.path, f"{key_path}.success.body_contains", success.get("body_contains"), errors)
        elif kind == "file_check":
            _require_string_key(package_root, runtime_doc.path, key_path, raw_check, "path", errors)
            _validate_optional_boolean(package_root, runtime_doc.path, f"{key_path}.success.exists", success.get("exists"), errors)
            _validate_optional_string(package_root, runtime_doc.path, f"{key_path}.success.contains_text", success.get("contains_text"), errors)
            file_type = success.get("file_type")
            if file_type is not None and file_type not in {"file", "directory", "any"}:
                errors.append(
                    ValidationError(
                        code=WRONG_KEY_TYPE,
                        message=f"{key_path}.success.file_type must be one of: file, directory, any",
                        package_root=package_root,
                        file_path=runtime_doc.path,
                        key_path=f"{key_path}.success.file_type",
                        details=f"actual={file_type}",
                    )
                )
        elif kind == "process_check":
            _require_string_key(package_root, runtime_doc.path, key_path, raw_check, "process_selector", errors)
            _validate_optional_non_negative_integer(package_root, runtime_doc.path, f"{key_path}.success.min_matches", success.get("min_matches"), errors)
            _validate_optional_non_negative_integer(package_root, runtime_doc.path, f"{key_path}.success.max_matches", success.get("max_matches"), errors)


def _validate_runtime_value_refs_block(
    package_root: Path,
    runtime_doc: ValidatedYamlDocument,
    errors: list[ValidationError],
) -> None:
    raw_block = runtime_doc.data.get(RUNTIME_VALUE_REFS_BLOCK)
    if raw_block is None:
        return
    if not isinstance(raw_block, dict):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"runtime.yaml.{RUNTIME_VALUE_REFS_BLOCK} must be a mapping",
                package_root=package_root,
                file_path=runtime_doc.path,
                key_path=RUNTIME_VALUE_REFS_BLOCK,
                details=f"actual_type={_yaml_type_name(raw_block)}",
            )
        )
        return

    values = raw_block.get("values")
    if values is None:
        errors.append(
            ValidationError(
                code=MISSING_REQUIRED_KEY,
                message=f"Missing required key: {RUNTIME_VALUE_REFS_BLOCK}.values",
                package_root=package_root,
                file_path=runtime_doc.path,
                key_path=f"{RUNTIME_VALUE_REFS_BLOCK}.values",
            )
        )
        return
    if not isinstance(values, dict):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"runtime.yaml.{RUNTIME_VALUE_REFS_BLOCK}.values must be a mapping",
                package_root=package_root,
                file_path=runtime_doc.path,
                key_path=f"{RUNTIME_VALUE_REFS_BLOCK}.values",
                details=f"actual_type={_yaml_type_name(values)}",
            )
        )
        return

    for raw_key, raw_value in values.items():
        key_name = str(raw_key).strip()
        key_path = f"{RUNTIME_VALUE_REFS_BLOCK}.values.{key_name or '<empty>'}"
        if not key_name:
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{RUNTIME_VALUE_REFS_BLOCK}.values keys must be non-empty strings",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{RUNTIME_VALUE_REFS_BLOCK}.values",
                )
            )
            continue
        if not isinstance(raw_value, dict):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path} must be a mapping",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=key_path,
                    details=f"actual_type={_yaml_type_name(raw_value)}",
                )
            )
            continue

        classification = raw_value.get("classification")
        if classification is None:
            errors.append(
                ValidationError(
                    code=MISSING_REQUIRED_KEY,
                    message=f"Missing required key: {key_path}.classification",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.classification",
                )
            )
        elif not isinstance(classification, str):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path}.classification must be a string",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.classification",
                    details=f"actual_type={_yaml_type_name(classification)}",
                )
            )
        elif classification not in RUNTIME_VALUE_CLASSIFICATIONS:
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path}.classification must be one of: {', '.join(RUNTIME_VALUE_CLASSIFICATIONS)}",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.classification",
                    details=f"actual={classification}",
                )
            )

        required = raw_value.get("required")
        if required is not None and not isinstance(required, bool):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path}.required must be a boolean",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.required",
                    details=f"actual_type={_yaml_type_name(required)}",
                )
            )

        ref = raw_value.get("ref")
        refs = raw_value.get("refs")
        if ref is not None and not isinstance(ref, str):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path}.ref must be a string",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.ref",
                    details=f"actual_type={_yaml_type_name(ref)}",
                )
            )
        if refs is not None and not isinstance(refs, (str, list)):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path}.refs must be a string or list of strings",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.refs",
                    details=f"actual_type={_yaml_type_name(refs)}",
                )
            )
        elif isinstance(refs, list):
            for index, item in enumerate(refs):
                if not isinstance(item, str):
                    errors.append(
                        ValidationError(
                            code=WRONG_KEY_TYPE,
                            message=f"runtime.yaml.{key_path}.refs[{index}] must be a string",
                            package_root=package_root,
                            file_path=runtime_doc.path,
                            key_path=f"{key_path}.refs[{index}]",
                            details=f"actual_type={_yaml_type_name(item)}",
                        )
                    )

        inline_value = raw_value.get("inline_value")
        if inline_value is not None and not isinstance(inline_value, (str, int, float, bool)):
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path}.inline_value must be a scalar string/number/boolean",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.inline_value",
                    details=f"actual_type={_yaml_type_name(inline_value)}",
                )
            )
        if isinstance(classification, str) and classification in {"secret", "sensitive_config"} and inline_value is not None:
            errors.append(
                ValidationError(
                    code=WRONG_KEY_TYPE,
                    message=f"runtime.yaml.{key_path}.inline_value is not allowed for classification {classification}",
                    package_root=package_root,
                    file_path=runtime_doc.path,
                    key_path=f"{key_path}.inline_value",
                )
            )

        for field_name in ("description", "dispatch_env", "host_check_context_key"):
            field_value = raw_value.get(field_name)
            if field_value is not None and not isinstance(field_value, str):
                errors.append(
                    ValidationError(
                        code=WRONG_KEY_TYPE,
                        message=f"runtime.yaml.{key_path}.{field_name} must be a string",
                        package_root=package_root,
                        file_path=runtime_doc.path,
                        key_path=f"{key_path}.{field_name}",
                        details=f"actual_type={_yaml_type_name(field_value)}",
                    )
                )


def _require_string_key(
    package_root: Path,
    file_path: Path,
    key_prefix: str,
    mapping: dict[str, Any],
    key: str,
    errors: list[ValidationError],
) -> None:
    if key not in mapping:
        errors.append(
            ValidationError(
                code=MISSING_REQUIRED_KEY,
                message=f"Missing required key: {key_prefix}.{key}",
                package_root=package_root,
                file_path=file_path,
                key_path=f"{key_prefix}.{key}",
            )
        )
        return
    if not isinstance(mapping[key], str):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_prefix}.{key} must be a string",
                package_root=package_root,
                file_path=file_path,
                key_path=f"{key_prefix}.{key}",
                details=f"actual_type={_yaml_type_name(mapping[key])}",
            )
        )


def _require_mapping_key(
    package_root: Path,
    file_path: Path,
    key_prefix: str,
    mapping: dict[str, Any],
    key: str,
    errors: list[ValidationError],
) -> None:
    if key not in mapping:
        errors.append(
            ValidationError(
                code=MISSING_REQUIRED_KEY,
                message=f"Missing required key: {key_prefix}.{key}",
                package_root=package_root,
                file_path=file_path,
                key_path=f"{key_prefix}.{key}",
            )
        )
        return
    if not isinstance(mapping[key], dict):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_prefix}.{key} must be a mapping",
                package_root=package_root,
                file_path=file_path,
                key_path=f"{key_prefix}.{key}",
                details=f"actual_type={_yaml_type_name(mapping[key])}",
            )
        )


def _validate_optional_string_list(
    package_root: Path,
    file_path: Path,
    key_path: str,
    value: Any,
    errors: list[ValidationError],
    *,
    required: bool = False,
) -> None:
    if value is None:
        if required:
            errors.append(
                ValidationError(
                    code=MISSING_REQUIRED_KEY,
                    message=f"Missing required key: {key_path}",
                    package_root=package_root,
                    file_path=file_path,
                    key_path=key_path,
                )
            )
        return
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_path} must be a list of strings",
                package_root=package_root,
                file_path=file_path,
                key_path=key_path,
                details=f"actual_type={_yaml_type_name(value)}",
            )
        )


def _require_boolean_key(
    package_root: Path,
    file_path: Path,
    key_prefix: str,
    mapping: dict[str, Any],
    key: str,
    errors: list[ValidationError],
) -> None:
    if key not in mapping:
        errors.append(
            ValidationError(
                code=MISSING_REQUIRED_KEY,
                message=f"Missing required key: {key_prefix}.{key}",
                package_root=package_root,
                file_path=file_path,
                key_path=f"{key_prefix}.{key}",
            )
        )
        return
    if not isinstance(mapping[key], bool):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_prefix}.{key} must be a boolean",
                package_root=package_root,
                file_path=file_path,
                key_path=f"{key_prefix}.{key}",
                details=f"actual_type={_yaml_type_name(mapping[key])}",
            )
        )


def _require_positive_integer_key(
    package_root: Path,
    file_path: Path,
    key_prefix: str,
    mapping: dict[str, Any],
    key: str,
    errors: list[ValidationError],
) -> None:
    if key not in mapping:
        errors.append(
            ValidationError(
                code=MISSING_REQUIRED_KEY,
                message=f"Missing required key: {key_prefix}.{key}",
                package_root=package_root,
                file_path=file_path,
                key_path=f"{key_prefix}.{key}",
            )
        )
        return
    value = mapping[key]
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_prefix}.{key} must be a positive integer",
                package_root=package_root,
                file_path=file_path,
                key_path=f"{key_prefix}.{key}",
                details=f"actual_type={_yaml_type_name(value)}",
            )
        )


def _validate_optional_string(
    package_root: Path,
    file_path: Path,
    key_path: str,
    value: Any,
    errors: list[ValidationError],
) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_path} must be a string",
                package_root=package_root,
                file_path=file_path,
                key_path=key_path,
                details=f"actual_type={_yaml_type_name(value)}",
            )
        )


def _validate_optional_boolean(
    package_root: Path,
    file_path: Path,
    key_path: str,
    value: Any,
    errors: list[ValidationError],
) -> None:
    if value is None:
        return
    if not isinstance(value, bool):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_path} must be a boolean",
                package_root=package_root,
                file_path=file_path,
                key_path=key_path,
                details=f"actual_type={_yaml_type_name(value)}",
            )
        )


def _validate_optional_integer(
    package_root: Path,
    file_path: Path,
    key_path: str,
    value: Any,
    errors: list[ValidationError],
) -> None:
    if value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool):
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_path} must be an integer",
                package_root=package_root,
                file_path=file_path,
                key_path=key_path,
                details=f"actual_type={_yaml_type_name(value)}",
            )
        )


def _validate_optional_non_negative_integer(
    package_root: Path,
    file_path: Path,
    key_path: str,
    value: Any,
    errors: list[ValidationError],
) -> None:
    if value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        errors.append(
            ValidationError(
                code=WRONG_KEY_TYPE,
                message=f"{key_path} must be an integer >= 0",
                package_root=package_root,
                file_path=file_path,
                key_path=key_path,
                details=f"actual_type={_yaml_type_name(value)}",
            )
        )


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _yaml_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, dict):
        return "mapping"
    if isinstance(value, list):
        return "sequence"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    return type(value).__name__
