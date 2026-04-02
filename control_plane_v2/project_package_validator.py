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
