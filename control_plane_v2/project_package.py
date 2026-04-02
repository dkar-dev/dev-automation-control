from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .project_package_validator import CAPABILITIES_FILE, PROJECT_FILE, validate_project_package


@dataclass(frozen=True)
class ProjectPackageDocument:
    filename: str
    path: Path
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "filename": self.filename,
            "path": str(self.path),
            "data": self.data,
        }


@dataclass(frozen=True)
class ProjectPackage:
    project_key: str
    package_root: Path
    schema_version: str
    capabilities_sections: dict[str, Any]
    files: dict[str, ProjectPackageDocument]

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_key": self.project_key,
            "package_root": str(self.package_root),
            "schema_version": self.schema_version,
            "capabilities_sections": self.capabilities_sections,
            "files": {
                filename: document.to_dict()
                for filename, document in sorted(self.files.items())
            },
        }


def resolve_project_package_root(package_ref: str | Path, projects_root: str | Path) -> Path:
    candidate = Path(package_ref).expanduser()
    if candidate.is_absolute() or len(candidate.parts) > 1 or str(package_ref).startswith("."):
        return candidate.resolve()
    return (Path(projects_root).expanduser().resolve() / str(package_ref)).resolve()


def load_project_package(package_root: str | Path) -> ProjectPackage:
    validated = validate_project_package(package_root)
    files = {
        filename: ProjectPackageDocument(
            filename=document.filename,
            path=document.path,
            data=document.data,
        )
        for filename, document in validated.files.items()
    }

    return ProjectPackage(
        project_key=validated.project_key,
        package_root=validated.package_root,
        schema_version=files[PROJECT_FILE].data["schema_version"],
        capabilities_sections=files[CAPABILITIES_FILE].data["sections"],
        files=files,
    )
