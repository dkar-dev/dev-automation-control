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
