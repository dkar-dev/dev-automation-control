from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import mimetypes
from pathlib import Path
import re
import sqlite3

from .dispatch_adapter import (
    ARTIFACT_KIND_DISPATCH_CONTEXT_MANIFEST,
    ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST,
    ARTIFACT_KIND_TASK_RUNTIME_CONTEXT_MANIFEST,
)
from .id_generation import generate_opaque_id
from .manual_control import ManualControlError, show_run_control_state
from .project_package import load_project_package
from .project_package_validator import POLICY_FILE, ProjectPackageValidationFailed
from .reviewer_outcome_persistence import ReviewerOutcomeError, list_flow_runs
from .run_persistence import RunDetails, RunPersistenceError, _connect_run_db, _ensure_required_tables, _resolve_database_path
from .step_run_persistence import StepRunDetails, StepRunPersistenceError, get_step_run, list_step_runs


CONTROL_DIR = Path(__file__).resolve().parents[1]

CONTRACT_TAXONOMY = (
    "implementation_step",
    "inspection_step",
    "recovery_step",
    "manual_followup_step",
)
CONTRACT_POLICY_BLOCK = "bounded_contract_generation_v1"
CONTRACT_STORAGE_MODEL = "project_package_policy_v1"
CONTRACT_TARGET_ROLES = ("executor", "reviewer", "manual")

ARTIFACT_KIND_BOUNDED_CONTRACT_JSON = "bounded_contract_json"
ARTIFACT_KIND_BOUNDED_CONTRACT_PROMPT = "bounded_contract_prompt"
ARTIFACT_KIND_BOUNDED_CONTRACT_MANIFEST = "bounded_contract_manifest"

CONTRACT_BOUNDARY_VIOLATION = "CONTRACT_BOUNDARY_VIOLATION"
CONTRACT_CAPABILITY_NOT_APPROVED = "CONTRACT_CAPABILITY_NOT_APPROVED"
CONTRACT_GENERATION_INVALID = "CONTRACT_GENERATION_INVALID"
CONTRACT_NOT_FOUND = "CONTRACT_NOT_FOUND"
CONTRACT_POLICY_INVALID = "CONTRACT_POLICY_INVALID"
CONTRACT_POLICY_MISSING = "CONTRACT_POLICY_MISSING"
CONTRACT_PROJECT_NOT_REGISTERED = "CONTRACT_PROJECT_NOT_REGISTERED"
CONTRACT_RUNTIME_CONTEXT_MISSING = "CONTRACT_RUNTIME_CONTEXT_MISSING"
CONTRACT_STATE_NOT_ALLOWED = "CONTRACT_STATE_NOT_ALLOWED"
CONTRACT_STORAGE_ERROR = "CONTRACT_STORAGE_ERROR"
CONTRACT_TEMPLATE_INVALID = "CONTRACT_TEMPLATE_INVALID"
CONTRACT_TEMPLATE_NOT_FOUND = "CONTRACT_TEMPLATE_NOT_FOUND"
CONTRACT_TYPE_INVALID = "CONTRACT_TYPE_INVALID"

_BLOCKED_ALLOWED_ACTIONS = {
    "change_architecture_contract",
    "modify_architecture_contract",
    "modify_policy_model",
    "change_workflow_semantics",
    "request_unapproved_capabilities",
    "autonomous_architecture_planning",
}
_RUNTIME_CONTEXT_ARTIFACT_KINDS = (
    ARTIFACT_KIND_TASK_RUNTIME_CONTEXT_MANIFEST,
    ARTIFACT_KIND_DISPATCH_CONTEXT_MANIFEST,
    ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST,
)
_PLACEHOLDER_RE = re.compile(r"\{\{\s*(?P<key>[a-zA-Z0-9_]+)\s*\}\}")


@dataclass(frozen=True)
class BoundedContractTemplate:
    key: str
    contract_type: str
    target_role: str
    description: str | None
    allowed_workflow_ids: tuple[str, ...]
    allowed_project_profiles: tuple[str, ...]
    allowed_run_statuses: tuple[str, ...]
    allowed_queue_statuses: tuple[str, ...]
    allowed_origin_types: tuple[str, ...]
    required_runtime_fields: tuple[str, ...]
    allowed_capability_sections: tuple[str, ...]
    required_state_tags: tuple[str, ...]
    required_any_state_tags: tuple[str, ...]
    forbidden_state_tags: tuple[str, ...]
    contract: dict[str, object]

    def to_summary_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "contract_type": self.contract_type,
            "target_role": self.target_role,
            "description": self.description,
            "allowed_workflow_ids": list(self.allowed_workflow_ids),
            "allowed_project_profiles": list(self.allowed_project_profiles),
            "allowed_run_statuses": list(self.allowed_run_statuses),
            "allowed_queue_statuses": list(self.allowed_queue_statuses),
            "allowed_origin_types": list(self.allowed_origin_types),
            "required_runtime_fields": list(self.required_runtime_fields),
            "allowed_capability_sections": list(self.allowed_capability_sections),
            "required_state_tags": list(self.required_state_tags),
            "required_any_state_tags": list(self.required_any_state_tags),
            "forbidden_state_tags": list(self.forbidden_state_tags),
            "contract": self.contract,
        }


@dataclass(frozen=True)
class BoundedContractPolicy:
    project_key: str
    package_root: Path
    policy_path: Path
    storage_model: str
    defaults: dict[str, str]
    templates: dict[str, BoundedContractTemplate]

    def to_dict(self) -> dict[str, object]:
        return {
            "project_key": self.project_key,
            "package_root": str(self.package_root),
            "policy_path": str(self.policy_path),
            "storage_model": self.storage_model,
            "defaults": dict(self.defaults),
            "templates": {
                key: template.to_summary_dict()
                for key, template in sorted(self.templates.items())
            },
        }


@dataclass(frozen=True)
class BoundedContractArtifact:
    artifact_kind: str
    filesystem_path: Path
    created_at: str
    artifact_ref_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_kind": self.artifact_kind,
            "filesystem_path": str(self.filesystem_path),
            "created_at": self.created_at,
            "artifact_ref_id": self.artifact_ref_id,
        }


@dataclass(frozen=True)
class GeneratedBoundedContract:
    contract_id: str
    created_at: str
    storage_model: str
    project_key: str
    project_profile: str
    workflow_id: str
    contract_type: str
    template_key: str
    target_role: str
    run_id: str | None
    flow_id: str | None
    step_run_id: str | None
    package_root: Path
    normalized_contract: dict[str, object]
    prompt_text: str
    manifest: dict[str, object]
    artifacts: tuple[BoundedContractArtifact, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "created_at": self.created_at,
            "storage_model": self.storage_model,
            "project_key": self.project_key,
            "project_profile": self.project_profile,
            "workflow_id": self.workflow_id,
            "contract_type": self.contract_type,
            "template_key": self.template_key,
            "target_role": self.target_role,
            "run_id": self.run_id,
            "flow_id": self.flow_id,
            "step_run_id": self.step_run_id,
            "package_root": str(self.package_root),
            "normalized_contract": self.normalized_contract,
            "prompt_text": self.prompt_text,
            "manifest": self.manifest,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }


class BoundedContractError(Exception):
    def __init__(self, code: str, message: str, database_path: Path, details: str | None = None) -> None:
        self.code = code
        self.message = message
        self.database_path = database_path
        self.details = details
        super().__init__(message)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "message": self.message,
            "database_path": str(self.database_path),
            "details": self.details,
        }


def generate_bounded_contract(
    database_path: str | Path,
    request_payload: Mapping[str, object],
) -> GeneratedBoundedContract:
    resolved_db_path = _resolve_database_path(database_path)
    request = _normalize_generation_request(request_payload, resolved_db_path)

    run_details: RunDetails | None = None
    if request["run_id"] is not None:
        run_details = _load_run_details_or_raise(resolved_db_path, request["run_id"])

    step_details: StepRunDetails | None = None
    if request["step_run_id"] is not None:
        step_details = _load_step_run_details_or_raise(resolved_db_path, request["step_run_id"])

    _validate_request_scope(
        request=request,
        run_details=run_details,
        step_details=step_details,
        database_path=resolved_db_path,
    )

    project_key = _resolve_project_key(request, run_details, step_details, resolved_db_path)
    workflow_id = _resolve_workflow_id(request, run_details, step_details, resolved_db_path)
    project_profile = _resolve_project_profile(request, run_details, step_details, resolved_db_path)

    project_row = _load_registered_project_row(resolved_db_path, project_key)
    if project_row is None:
        raise BoundedContractError(
            code=CONTRACT_PROJECT_NOT_REGISTERED,
            message=f"Project is not registered in SQLite: {project_key}",
            database_path=resolved_db_path,
        )

    try:
        project_package = load_project_package(Path(str(project_row["package_root"])).expanduser().resolve())
    except ProjectPackageValidationFailed as exc:
        raise BoundedContractError(
            code=CONTRACT_POLICY_INVALID,
            message=f"Registered project package is invalid: {project_key}",
            database_path=resolved_db_path,
            details="; ".join(f"{error.code}:{error.message}" for error in exc.errors),
        ) from exc

    policy = _load_contract_policy(project_package, resolved_db_path)
    control_state = _load_control_state_or_none(resolved_db_path, run_details.run.id if run_details is not None else None)
    flow_runs = _load_flow_runs_or_none(resolved_db_path, run_details.run.flow_id if run_details is not None else None)
    step_runs = _load_step_runs_or_none(resolved_db_path, run_details.run.id if run_details is not None else None)
    flow_summary = _build_flow_summary(flow_runs, run_details.run.id if run_details is not None else None)
    current_state = _build_current_state_payload(
        explicit_state=request["current_state"],
        run_details=run_details,
        step_details=step_details,
        control_state=control_state,
        step_runs=step_runs,
        flow_summary=flow_summary,
    )
    runtime_context = _build_runtime_context_payload(
        resolved_db_path,
        request=request,
        run_details=run_details,
        flow_id=flow_summary.get("flow_id") if isinstance(flow_summary, Mapping) else None,
    )
    contract_type = _resolve_contract_type(
        request_contract_type=request["contract_type"],
        current_state=current_state,
        database_path=resolved_db_path,
    )
    template = _resolve_template(policy, contract_type=contract_type, template_key=request["template_key"], database_path=resolved_db_path)
    _validate_generation_request(
        database_path=resolved_db_path,
        policy=policy,
        template=template,
        workflow_id=workflow_id,
        project_profile=project_profile,
        run_details=run_details,
        current_state=current_state,
        runtime_context=runtime_context,
        operator_request=request["operator_request"],
        project_capability_sections=tuple(project_package.capabilities_sections.keys()),
    )

    contract_id = generate_opaque_id()
    created_at = _utc_now()
    render_context = _build_render_context(
        contract_id=contract_id,
        created_at=created_at,
        project_key=project_key,
        project_profile=project_profile,
        workflow_id=workflow_id,
        contract_type=contract_type,
        template_key=template.key,
        target_role=template.target_role,
        run_details=run_details,
        step_details=step_details,
        current_state=current_state,
        runtime_context=runtime_context,
        operator_request=request["operator_request"],
    )
    rendered_contract = _render_value(template.contract, render_context, resolved_db_path)
    if not isinstance(rendered_contract, Mapping):
        raise BoundedContractError(
            code=CONTRACT_TEMPLATE_INVALID,
            message=f"Rendered contract for template {template.key} must remain a mapping/object",
            database_path=resolved_db_path,
        )
    normalized_contract = _build_normalized_contract(
        contract_id=contract_id,
        created_at=created_at,
        policy=policy,
        template=template,
        project_package_root=project_package.package_root,
        project_key=project_key,
        project_profile=project_profile,
        workflow_id=workflow_id,
        run_details=run_details,
        step_details=step_details,
        current_state=current_state,
        runtime_context=runtime_context,
        operator_request=request["operator_request"],
        rendered_contract=rendered_contract,
        project_capability_sections=tuple(sorted(project_package.capabilities_sections.keys())),
        database_path=resolved_db_path,
    )
    prompt_text = render_bounded_contract_prompt(normalized_contract)

    output_root = _resolve_contract_output_root(
        request=request,
        run_details=run_details,
        flow_summary=flow_summary,
        runtime_context=runtime_context,
        project_key=project_key,
        workflow_id=workflow_id,
        contract_id=contract_id,
    )
    contract_path = output_root / "contract.json"
    prompt_path = output_root / "prompt.md"
    manifest_path = output_root / "manifest.json"

    manifest = {
        "contract_id": contract_id,
        "created_at": created_at,
        "storage_model": policy.storage_model,
        "project_key": project_key,
        "project_profile": project_profile,
        "workflow_id": workflow_id,
        "contract_type": contract_type,
        "template_key": template.key,
        "target_role": template.target_role,
        "run_id": run_details.run.id if run_details is not None else None,
        "flow_id": flow_summary.get("flow_id") if isinstance(flow_summary, Mapping) else None,
        "step_run_id": step_details.step_run.id if step_details is not None else None,
        "package_root": str(project_package.package_root),
        "policy_path": str(policy.policy_path),
        "artifacts": {
            "contract_json_path": str(contract_path),
            "prompt_text_path": str(prompt_path),
            "manifest_json_path": str(manifest_path),
        },
    }

    _write_json(contract_path, normalized_contract)
    _write_text(prompt_path, prompt_text)
    _write_json(manifest_path, manifest)

    _insert_contract_manifest_row(
        resolved_db_path,
        contract_id=contract_id,
        project_id=str(project_row["id"]),
        flow_id=_normalize_optional_text(flow_summary.get("flow_id") if isinstance(flow_summary, Mapping) else None),
        run_id=run_details.run.id if run_details is not None else None,
        step_run_id=step_details.step_run.id if step_details is not None else None,
        workflow_id=workflow_id,
        project_profile=project_profile,
        contract_type=contract_type,
        template_key=template.key,
        contract_json_path=contract_path,
        prompt_text_path=prompt_path,
        manifest_json_path=manifest_path,
        created_at=created_at,
    )
    artifacts = _record_contract_artifacts(
        resolved_db_path,
        project_id=str(project_row["id"]),
        flow_id=_normalize_optional_text(flow_summary.get("flow_id") if isinstance(flow_summary, Mapping) else None),
        run_id=run_details.run.id if run_details is not None else None,
        step_run_id=step_details.step_run.id if step_details is not None else None,
        created_at=created_at,
        artifact_paths=(
            (ARTIFACT_KIND_BOUNDED_CONTRACT_JSON, contract_path),
            (ARTIFACT_KIND_BOUNDED_CONTRACT_PROMPT, prompt_path),
            (ARTIFACT_KIND_BOUNDED_CONTRACT_MANIFEST, manifest_path),
        ),
    )

    return GeneratedBoundedContract(
        contract_id=contract_id,
        created_at=created_at,
        storage_model=policy.storage_model,
        project_key=project_key,
        project_profile=project_profile,
        workflow_id=workflow_id,
        contract_type=contract_type,
        template_key=template.key,
        target_role=template.target_role,
        run_id=run_details.run.id if run_details is not None else None,
        flow_id=_normalize_optional_text(flow_summary.get("flow_id") if isinstance(flow_summary, Mapping) else None),
        step_run_id=step_details.step_run.id if step_details is not None else None,
        package_root=project_package.package_root,
        normalized_contract=normalized_contract,
        prompt_text=prompt_text,
        manifest=manifest,
        artifacts=artifacts,
    )


def show_bounded_contract(database_path: str | Path, contract_id: str) -> GeneratedBoundedContract:
    resolved_db_path = _resolve_database_path(database_path)
    normalized_contract_id = _require_text("contract_id", contract_id, resolved_db_path)
    row = _load_contract_manifest_row(resolved_db_path, normalized_contract_id)
    if row is None:
        raise BoundedContractError(
            code=CONTRACT_NOT_FOUND,
            message=f"Bounded contract is not present in SQLite: {normalized_contract_id}",
            database_path=resolved_db_path,
        )

    contract_path = Path(str(row["contract_json_path"])).expanduser().resolve()
    prompt_path = Path(str(row["prompt_text_path"])).expanduser().resolve()
    manifest_path = Path(str(row["manifest_json_path"])).expanduser().resolve()
    normalized_contract = _read_json_required(contract_path, resolved_db_path, normalized_contract_id)
    prompt_text = _read_text_required(prompt_path, resolved_db_path, normalized_contract_id)
    manifest = _read_json_required(manifest_path, resolved_db_path, normalized_contract_id)
    artifacts = _load_contract_artifacts(resolved_db_path, normalized_contract_id)

    execution = normalized_contract.get("execution")
    execution_mapping = execution if isinstance(execution, Mapping) else {}
    return GeneratedBoundedContract(
        contract_id=normalized_contract_id,
        created_at=str(row["created_at"]),
        storage_model=manifest.get("storage_model", CONTRACT_STORAGE_MODEL),
        project_key=str(row["project_key"]),
        project_profile=str(row["project_profile"]),
        workflow_id=str(row["workflow_id"]),
        contract_type=str(row["contract_type"]),
        template_key=str(row["template_key"]),
        target_role=_normalize_optional_text(execution_mapping.get("target_role")) or "manual",
        run_id=_normalize_optional_text(row["run_id"]),
        flow_id=_normalize_optional_text(row["flow_id"]),
        step_run_id=_normalize_optional_text(row["step_run_id"]),
        package_root=Path(str(row["package_root"])).expanduser().resolve(),
        normalized_contract=normalized_contract,
        prompt_text=prompt_text,
        manifest=manifest,
        artifacts=artifacts,
    )


def list_contract_templates(
    database_path: str | Path | None = None,
    *,
    project_key: str | None = None,
    package_root: str | Path | None = None,
) -> dict[str, object]:
    if package_root is None and (database_path is None or project_key is None):
        raise ValueError("Provide package_root or database_path + project_key")

    if package_root is not None:
        project_package = load_project_package(Path(package_root).expanduser().resolve())
        database_hint = Path(str(database_path)).expanduser().resolve() if database_path is not None else Path("<package-only>")
    else:
        assert database_path is not None
        assert project_key is not None
        database_hint = _resolve_database_path(database_path)
        project_row = _load_registered_project_row(database_hint, project_key)
        if project_row is None:
            raise BoundedContractError(
                code=CONTRACT_PROJECT_NOT_REGISTERED,
                message=f"Project is not registered in SQLite: {project_key}",
                database_path=database_hint,
            )
        project_package = load_project_package(Path(str(project_row["package_root"])).expanduser().resolve())

    policy = _load_contract_policy(project_package, database_hint)
    return policy.to_dict()


def render_bounded_contract_prompt(normalized_contract: Mapping[str, object]) -> str:
    contract_type = _normalize_optional_text(normalized_contract.get("contract_type")) or "bounded_contract"
    execution = normalized_contract.get("execution")
    execution_mapping = execution if isinstance(execution, Mapping) else {}
    project = normalized_contract.get("project")
    project_mapping = project if isinstance(project, Mapping) else {}
    runtime = normalized_contract.get("runtime")
    runtime_mapping = runtime if isinstance(runtime, Mapping) else {}
    task = normalized_contract.get("task")
    task_mapping = task if isinstance(task, Mapping) else {}
    boundaries = normalized_contract.get("boundaries")
    boundaries_mapping = boundaries if isinstance(boundaries, Mapping) else {}

    lines = [
        f"# Bounded Contract: {contract_type}",
        "",
        f"Contract ID: {_normalize_optional_text(normalized_contract.get('contract_id')) or 'unknown'}",
        f"Target role: {_normalize_optional_text(execution_mapping.get('target_role')) or 'manual'}",
        f"Project: {_normalize_optional_text(project_mapping.get('project_key')) or 'unknown'}",
        f"Project profile: {_normalize_optional_text(project_mapping.get('project_profile')) or 'unknown'}",
        f"Workflow: {_normalize_optional_text(normalized_contract.get('workflow_id')) or 'unknown'}",
        f"Run ID: {_normalize_optional_text(runtime_mapping.get('run_id')) or 'none'}",
        f"Flow ID: {_normalize_optional_text(runtime_mapping.get('flow_id')) or 'none'}",
        "",
        "## Objective",
        _normalize_optional_text(task_mapping.get("objective")) or "No objective supplied.",
        "",
        "## Summary",
        _normalize_optional_text(task_mapping.get("summary")) or "No summary supplied.",
        "",
        "## Deliverables",
    ]
    lines.extend(_render_bullet_section(task_mapping.get("deliverables")))
    lines.extend(["", "## Output Requirements"])
    lines.extend(_render_bullet_section(task_mapping.get("output_requirements")))
    lines.extend(["", "## Allowed Actions"])
    lines.extend(_render_bullet_section(boundaries_mapping.get("allowed_actions")))
    lines.extend(["", "## Forbidden Actions"])
    lines.extend(_render_bullet_section(boundaries_mapping.get("forbidden_actions")))
    lines.extend(["", "## Runtime Context"])
    for key in (
        "task_text",
        "source",
        "thread_label",
        "project_repo_path",
        "executor_worktree_path",
        "reviewer_worktree_path",
        "instructions_repo_path",
        "instruction_profile",
        "handoff_commit_sha",
    ):
        value = _normalize_optional_text(_mapping_get(normalized_contract.get("context"), key))
        if value is not None:
            lines.append(f"- {key}: {value}")
    lines.extend(["", "## Boundary Notes"])
    lines.extend(_render_bullet_section(boundaries_mapping.get("boundary_notes")))
    lines.extend(
        [
            "",
            "## Hard Stops",
            "- Do not change the architecture contract.",
            "- Do not change policy or workflow semantics.",
            "- Do not request or rely on unapproved capabilities.",
            "- If the bounded task cannot be completed within these constraints, stop and report the blocker.",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _normalize_generation_request(payload: Mapping[str, object], database_path: Path) -> dict[str, object]:
    request = dict(payload)
    contract_type = _normalize_optional_text(request.get("contract_type"))
    if contract_type is not None and contract_type not in CONTRACT_TAXONOMY:
        raise BoundedContractError(
            code=CONTRACT_TYPE_INVALID,
            message=f"contract_type must be one of: {', '.join(CONTRACT_TAXONOMY)}",
            database_path=database_path,
            details=f"actual={contract_type}",
        )

    return {
        "project_key": _normalize_optional_text(request.get("project_key")),
        "workflow_id": _normalize_optional_text(request.get("workflow_id")),
        "project_profile": _normalize_optional_text(request.get("project_profile")),
        "run_id": _normalize_optional_text(request.get("run_id")),
        "step_run_id": _normalize_optional_text(request.get("step_run_id")),
        "contract_type": contract_type,
        "template_key": _normalize_optional_text(request.get("template_key")),
        "current_state": _normalize_optional_mapping(request.get("current_state"), field_name="current_state", database_path=database_path),
        "runtime_context": _normalize_optional_mapping(request.get("runtime_context"), field_name="runtime_context", database_path=database_path),
        "operator_request": _normalize_optional_mapping(request.get("operator_request"), field_name="operator_request", database_path=database_path),
        "artifact_root": _normalize_optional_path(request.get("artifact_root")),
    }


def _load_contract_policy(project_package, database_path: Path) -> BoundedContractPolicy:
    policy_doc = project_package.files[POLICY_FILE]
    raw_policy = policy_doc.data.get(CONTRACT_POLICY_BLOCK)
    if raw_policy is None:
        raise BoundedContractError(
            code=CONTRACT_POLICY_MISSING,
            message=f"{POLICY_FILE}.{CONTRACT_POLICY_BLOCK} is required to generate bounded contracts",
            database_path=database_path,
            details=f"package_root={project_package.package_root}",
        )
    if not isinstance(raw_policy, Mapping):
        raise BoundedContractError(
            code=CONTRACT_POLICY_INVALID,
            message=f"{POLICY_FILE}.{CONTRACT_POLICY_BLOCK} must be a mapping/object",
            database_path=database_path,
            details=f"actual_type={type(raw_policy).__name__}",
        )

    defaults_raw = raw_policy.get("defaults")
    templates_raw = raw_policy.get("templates")
    if not isinstance(defaults_raw, Mapping) or not isinstance(templates_raw, Mapping):
        raise BoundedContractError(
            code=CONTRACT_POLICY_INVALID,
            message=f"{POLICY_FILE}.{CONTRACT_POLICY_BLOCK} must define defaults and templates mappings",
            database_path=database_path,
        )

    defaults: dict[str, str] = {}
    for contract_type in CONTRACT_TAXONOMY:
        template_key = _normalize_optional_text(defaults_raw.get(contract_type))
        if template_key is not None:
            defaults[contract_type] = template_key

    templates: dict[str, BoundedContractTemplate] = {}
    for key, raw_template in templates_raw.items():
        template_key = _normalize_optional_text(key)
        if template_key is None:
            raise BoundedContractError(
                code=CONTRACT_POLICY_INVALID,
                message="Template keys in policy.yaml must be non-empty strings",
                database_path=database_path,
            )
        templates[template_key] = _parse_template(template_key, raw_template, database_path)

    for contract_type, template_key in defaults.items():
        if template_key not in templates:
            raise BoundedContractError(
                code=CONTRACT_POLICY_INVALID,
                message=f"Default template does not exist for {contract_type}: {template_key}",
                database_path=database_path,
            )
        if templates[template_key].contract_type != contract_type:
            raise BoundedContractError(
                code=CONTRACT_POLICY_INVALID,
                message=f"Default template {template_key} does not match contract_type {contract_type}",
                database_path=database_path,
                details=f"actual_contract_type={templates[template_key].contract_type}",
            )

    storage_model = _normalize_optional_text(raw_policy.get("storage_model")) or CONTRACT_STORAGE_MODEL
    if storage_model != CONTRACT_STORAGE_MODEL:
        raise BoundedContractError(
            code=CONTRACT_POLICY_INVALID,
            message=f"{POLICY_FILE}.{CONTRACT_POLICY_BLOCK}.storage_model must be {CONTRACT_STORAGE_MODEL}",
            database_path=database_path,
            details=f"actual={storage_model}",
        )
    return BoundedContractPolicy(
        project_key=project_package.project_key,
        package_root=project_package.package_root,
        policy_path=policy_doc.path,
        storage_model=storage_model,
        defaults=defaults,
        templates=templates,
    )


def _parse_template(template_key: str, raw_template: object, database_path: Path) -> BoundedContractTemplate:
    if not isinstance(raw_template, Mapping):
        raise BoundedContractError(
            code=CONTRACT_TEMPLATE_INVALID,
            message=f"Template {template_key} must be a mapping/object",
            database_path=database_path,
            details=f"actual_type={type(raw_template).__name__}",
        )

    contract_type = _require_text("contract_type", raw_template.get("contract_type"), database_path)
    if contract_type not in CONTRACT_TAXONOMY:
        raise BoundedContractError(
            code=CONTRACT_TEMPLATE_INVALID,
            message=f"Template {template_key} has an invalid contract_type",
            database_path=database_path,
            details=f"actual={contract_type}",
        )
    target_role = _require_text("target_role", raw_template.get("target_role"), database_path)
    if target_role not in CONTRACT_TARGET_ROLES:
        raise BoundedContractError(
            code=CONTRACT_TEMPLATE_INVALID,
            message=f"Template {template_key} has an invalid target_role",
            database_path=database_path,
            details=f"actual={target_role}",
        )
    contract = _normalize_optional_mapping(raw_template.get("contract"), field_name=f"template[{template_key}].contract", database_path=database_path)
    if not contract:
        raise BoundedContractError(
            code=CONTRACT_TEMPLATE_INVALID,
            message=f"Template {template_key} must define a non-empty contract mapping",
            database_path=database_path,
        )

    return BoundedContractTemplate(
        key=template_key,
        contract_type=contract_type,
        target_role=target_role,
        description=_normalize_optional_text(raw_template.get("description")),
        allowed_workflow_ids=tuple(_coerce_string_list(raw_template.get("allowed_workflow_ids"), database_path=database_path, field_name="allowed_workflow_ids")),
        allowed_project_profiles=tuple(_coerce_string_list(raw_template.get("allowed_project_profiles"), database_path=database_path, field_name="allowed_project_profiles")),
        allowed_run_statuses=tuple(_coerce_string_list(raw_template.get("allowed_run_statuses"), database_path=database_path, field_name="allowed_run_statuses")),
        allowed_queue_statuses=tuple(_coerce_string_list(raw_template.get("allowed_queue_statuses"), database_path=database_path, field_name="allowed_queue_statuses")),
        allowed_origin_types=tuple(_coerce_string_list(raw_template.get("allowed_origin_types"), database_path=database_path, field_name="allowed_origin_types")),
        required_runtime_fields=tuple(_coerce_string_list(raw_template.get("required_runtime_fields"), database_path=database_path, field_name="required_runtime_fields")),
        allowed_capability_sections=tuple(_coerce_string_list(raw_template.get("allowed_capability_sections"), database_path=database_path, field_name="allowed_capability_sections")),
        required_state_tags=tuple(_coerce_string_list(raw_template.get("required_state_tags"), database_path=database_path, field_name="required_state_tags")),
        required_any_state_tags=tuple(_coerce_string_list(raw_template.get("required_any_state_tags"), database_path=database_path, field_name="required_any_state_tags")),
        forbidden_state_tags=tuple(_coerce_string_list(raw_template.get("forbidden_state_tags"), database_path=database_path, field_name="forbidden_state_tags")),
        contract=dict(contract),
    )


def _resolve_contract_type(
    *,
    request_contract_type: str | None,
    current_state: Mapping[str, object],
    database_path: Path,
) -> str:
    if request_contract_type is not None:
        return request_contract_type

    state_tags = set(_coerce_string_list(current_state.get("state_tags"), database_path=database_path, field_name="state_tags"))
    recovery_tags = {"manual_paused", "pending_rerun", "executor_failed", "reviewer_failed"}
    if state_tags & recovery_tags:
        return "recovery_step"
    if "executor_succeeded" in state_tags and "reviewer_started" not in state_tags:
        return "inspection_step"
    if "run_terminal" in state_tags:
        return "manual_followup_step"
    if not state_tags:
        raise BoundedContractError(
            code=CONTRACT_TYPE_INVALID,
            message="contract_type is required when current_state does not allow safe inference",
            database_path=database_path,
        )
    return "implementation_step"


def _resolve_template(
    policy: BoundedContractPolicy,
    *,
    contract_type: str,
    template_key: str | None,
    database_path: Path,
) -> BoundedContractTemplate:
    if template_key is not None:
        template = policy.templates.get(template_key)
        if template is None:
            raise BoundedContractError(
                code=CONTRACT_TEMPLATE_NOT_FOUND,
                message=f"Template does not exist: {template_key}",
                database_path=database_path,
            )
        if template.contract_type != contract_type:
            raise BoundedContractError(
                code=CONTRACT_TEMPLATE_INVALID,
                message=f"Template {template_key} does not match requested contract_type {contract_type}",
                database_path=database_path,
                details=f"actual_contract_type={template.contract_type}",
            )
        return template

    default_template_key = policy.defaults.get(contract_type)
    if default_template_key is None:
        raise BoundedContractError(
            code=CONTRACT_TEMPLATE_NOT_FOUND,
            message=f"No default template is configured for contract_type {contract_type}",
            database_path=database_path,
        )
    return policy.templates[default_template_key]


def _validate_generation_request(
    *,
    database_path: Path,
    policy: BoundedContractPolicy,
    template: BoundedContractTemplate,
    workflow_id: str,
    project_profile: str,
    run_details: RunDetails | None,
    current_state: Mapping[str, object],
    runtime_context: Mapping[str, object],
    operator_request: Mapping[str, object],
    project_capability_sections: tuple[str, ...],
) -> None:
    del policy
    if template.allowed_workflow_ids and workflow_id not in template.allowed_workflow_ids:
        raise BoundedContractError(
            code=CONTRACT_STATE_NOT_ALLOWED,
            message=f"Template {template.key} does not allow workflow_id {workflow_id}",
            database_path=database_path,
            details=f"allowed={list(template.allowed_workflow_ids)}",
        )
    if template.allowed_project_profiles and project_profile not in template.allowed_project_profiles:
        raise BoundedContractError(
            code=CONTRACT_STATE_NOT_ALLOWED,
            message=f"Template {template.key} does not allow project_profile {project_profile}",
            database_path=database_path,
            details=f"allowed={list(template.allowed_project_profiles)}",
        )

    if run_details is not None:
        run = run_details.run
        queue_status = run.queue_item.status if run.queue_item is not None else None
        if template.allowed_run_statuses and run.status not in template.allowed_run_statuses:
            raise BoundedContractError(
                code=CONTRACT_STATE_NOT_ALLOWED,
                message=f"Template {template.key} does not allow run status {run.status}",
                database_path=database_path,
                details=f"allowed={list(template.allowed_run_statuses)}",
            )
        if template.allowed_queue_statuses and queue_status not in template.allowed_queue_statuses:
            raise BoundedContractError(
                code=CONTRACT_STATE_NOT_ALLOWED,
                message=f"Template {template.key} does not allow queue status {queue_status}",
                database_path=database_path,
                details=f"allowed={list(template.allowed_queue_statuses)}",
            )
        if template.allowed_origin_types and run.origin_type not in template.allowed_origin_types:
            raise BoundedContractError(
                code=CONTRACT_STATE_NOT_ALLOWED,
                message=f"Template {template.key} does not allow origin_type {run.origin_type}",
                database_path=database_path,
                details=f"allowed={list(template.allowed_origin_types)}",
            )

    state_tags = set(_coerce_string_list(current_state.get("state_tags"), database_path=database_path, field_name="state_tags"))
    missing_required = [tag for tag in template.required_state_tags if tag not in state_tags]
    if missing_required:
        raise BoundedContractError(
            code=CONTRACT_STATE_NOT_ALLOWED,
            message=f"Template {template.key} is missing required state tags",
            database_path=database_path,
            details="missing=" + ",".join(missing_required),
        )
    if template.required_any_state_tags and not state_tags.intersection(template.required_any_state_tags):
        raise BoundedContractError(
            code=CONTRACT_STATE_NOT_ALLOWED,
            message=f"Template {template.key} requires at least one matching state tag",
            database_path=database_path,
            details="required_any=" + ",".join(template.required_any_state_tags),
        )
    forbidden_present = [tag for tag in template.forbidden_state_tags if tag in state_tags]
    if forbidden_present:
        raise BoundedContractError(
            code=CONTRACT_STATE_NOT_ALLOWED,
            message=f"Template {template.key} is blocked by the current state",
            database_path=database_path,
            details="forbidden=" + ",".join(forbidden_present),
        )

    missing_runtime_fields = [
        field_name
        for field_name in template.required_runtime_fields
        if _normalize_optional_text(runtime_context.get(field_name)) is None
    ]
    if missing_runtime_fields:
        raise BoundedContractError(
            code=CONTRACT_RUNTIME_CONTEXT_MISSING,
            message=f"Template {template.key} requires runtime context fields that are not available",
            database_path=database_path,
            details="missing_fields=" + ",".join(missing_runtime_fields),
        )

    approved_capabilities = set(project_capability_sections)
    missing_capabilities = [
        section
        for section in template.allowed_capability_sections
        if section not in approved_capabilities
    ]
    if missing_capabilities:
        raise BoundedContractError(
            code=CONTRACT_CAPABILITY_NOT_APPROVED,
            message=f"Template {template.key} requires capability sections that are not approved for the project",
            database_path=database_path,
            details="missing_sections=" + ",".join(missing_capabilities),
        )

    requested_capability_sections = _coerce_string_list(
        operator_request.get("requested_capability_sections"),
        database_path=database_path,
        field_name="operator_request.requested_capability_sections",
    )
    disallowed_requested_sections = [
        section
        for section in requested_capability_sections
        if section not in template.allowed_capability_sections
    ]
    if disallowed_requested_sections:
        raise BoundedContractError(
            code=CONTRACT_CAPABILITY_NOT_APPROVED,
            message="operator_request asks for capability sections outside the approved template boundary",
            database_path=database_path,
            details="disallowed_sections=" + ",".join(disallowed_requested_sections),
        )

    requested_actions = _coerce_string_list(
        operator_request.get("requested_actions"),
        database_path=database_path,
        field_name="operator_request.requested_actions",
    )
    blocked_requested_actions = [
        action
        for action in requested_actions
        if action in _BLOCKED_ALLOWED_ACTIONS
    ]
    if blocked_requested_actions:
        raise BoundedContractError(
            code=CONTRACT_BOUNDARY_VIOLATION,
            message="operator_request asks for blocked actions outside bounded contract policy",
            database_path=database_path,
            details="blocked_actions=" + ",".join(blocked_requested_actions),
        )


def _build_normalized_contract(
    *,
    contract_id: str,
    created_at: str,
    policy: BoundedContractPolicy,
    template: BoundedContractTemplate,
    project_package_root: Path,
    project_key: str,
    project_profile: str,
    workflow_id: str,
    run_details: RunDetails | None,
    step_details: StepRunDetails | None,
    current_state: Mapping[str, object],
    runtime_context: Mapping[str, object],
    operator_request: Mapping[str, object],
    rendered_contract: Mapping[str, object],
    project_capability_sections: tuple[str, ...],
    database_path: Path,
) -> dict[str, object]:
    allowed_actions = _coerce_string_list(
        rendered_contract.get("allowed_actions"),
        database_path=database_path,
        field_name="contract.allowed_actions",
    )
    blocked_actions = [action for action in allowed_actions if action in _BLOCKED_ALLOWED_ACTIONS]
    if blocked_actions:
        raise BoundedContractError(
            code=CONTRACT_BOUNDARY_VIOLATION,
            message=f"Generated contract includes blocked allowed_actions: {', '.join(blocked_actions)}",
            database_path=database_path,
        )

    forbidden_actions = _coerce_string_list(
        rendered_contract.get("forbidden_actions"),
        database_path=database_path,
        field_name="contract.forbidden_actions",
    )
    for blocked_action in sorted(_BLOCKED_ALLOWED_ACTIONS):
        if blocked_action not in forbidden_actions:
            forbidden_actions.append(blocked_action)

    summary = _normalize_optional_text(rendered_contract.get("summary"))
    objective = _normalize_optional_text(rendered_contract.get("objective"))
    if summary is None or objective is None:
        missing_fields: list[str] = []
        if summary is None:
            missing_fields.append("summary")
        if objective is None:
            missing_fields.append("objective")
        raise BoundedContractError(
            code=CONTRACT_TEMPLATE_INVALID,
            message="Rendered contract must include non-empty summary and objective",
            database_path=database_path,
            details="missing_fields=" + ",".join(missing_fields),
        )

    return {
        "contract_id": contract_id,
        "schema_version": "bounded_contract_v1",
        "generated_at": created_at,
        "storage_model": policy.storage_model,
        "contract_type": template.contract_type,
        "workflow_id": workflow_id,
        "template": {
            "key": template.key,
            "description": template.description,
            "policy_path": str(policy.policy_path),
            "package_root": str(project_package_root),
        },
        "project": {
            "project_key": project_key,
            "project_profile": project_profile,
            "package_root": str(project_package_root),
        },
        "runtime": {
            "run_id": run_details.run.id if run_details is not None else None,
            "flow_id": run_details.run.flow_id if run_details is not None else _normalize_optional_text(current_state.get("flow_id")),
            "step_run_id": step_details.step_run.id if step_details is not None else None,
            "run_status": run_details.run.status if run_details is not None else _normalize_optional_text(current_state.get("run_status")),
            "queue_status": run_details.run.queue_item.status if run_details is not None and run_details.run.queue_item is not None else _normalize_optional_text(current_state.get("queue_status")),
            "state_tags": _coerce_string_list(current_state.get("state_tags"), database_path=database_path, field_name="state_tags"),
            "flow_summary": dict(_normalize_optional_mapping(current_state.get("flow_summary"), field_name="flow_summary", database_path=database_path)),
        },
        "execution": {
            "target_role": template.target_role,
            "dispatch_step_key": "executor" if template.target_role == "executor" else ("reviewer" if template.target_role == "reviewer" else None),
        },
        "capabilities": {
            "project_capability_sections": list(project_capability_sections),
            "template_allowed_capability_sections": list(template.allowed_capability_sections),
            "requested_capability_sections": _coerce_string_list(
                operator_request.get("requested_capability_sections"),
                database_path=database_path,
                field_name="operator_request.requested_capability_sections",
            ),
        },
        "context": dict(runtime_context),
        "current_state": dict(current_state),
        "operator_request": dict(operator_request),
        "task": {
            "summary": summary,
            "objective": objective,
            "deliverables": _coerce_string_list(rendered_contract.get("deliverables"), database_path=database_path, field_name="contract.deliverables"),
            "output_requirements": _coerce_string_list(rendered_contract.get("output_requirements"), database_path=database_path, field_name="contract.output_requirements"),
        },
        "boundaries": {
            "architecture_change_allowed": False,
            "policy_change_allowed": False,
            "workflow_semantics_change_allowed": False,
            "requires_approved_template": True,
            "llm_in_the_loop_generation": False,
            "allowed_actions": allowed_actions,
            "forbidden_actions": forbidden_actions,
            "boundary_notes": _coerce_string_list(rendered_contract.get("boundary_notes"), database_path=database_path, field_name="contract.boundary_notes"),
        },
    }


def _build_render_context(
    *,
    contract_id: str,
    created_at: str,
    project_key: str,
    project_profile: str,
    workflow_id: str,
    contract_type: str,
    template_key: str,
    target_role: str,
    run_details: RunDetails | None,
    step_details: StepRunDetails | None,
    current_state: Mapping[str, object],
    runtime_context: Mapping[str, object],
    operator_request: Mapping[str, object],
) -> dict[str, str]:
    render_context: dict[str, str] = {
        "contract_id": contract_id,
        "generated_at": created_at,
        "project_key": project_key,
        "project_profile": project_profile,
        "workflow_id": workflow_id,
        "contract_type": contract_type,
        "template_key": template_key,
        "target_role": target_role,
    }
    if run_details is not None:
        render_context["run_id"] = run_details.run.id
        render_context["flow_id"] = run_details.run.flow_id
        render_context["milestone"] = run_details.run.milestone
        render_context["origin_type"] = run_details.run.origin_type
    if step_details is not None:
        render_context["step_run_id"] = step_details.step_run.id
        render_context["step_key"] = step_details.step_run.step_key
    for source in (runtime_context, operator_request, current_state):
        for key, value in source.items():
            normalized = _normalize_scalar_for_render(value)
            if normalized is not None:
                render_context[key] = normalized
    flow_summary = _normalize_optional_mapping(current_state.get("flow_summary"), field_name="flow_summary", database_path=Path("<render-context>"))
    for key, value in flow_summary.items():
        normalized = _normalize_scalar_for_render(value)
        if normalized is not None:
            render_context[key] = normalized
    if "state_tags" in current_state:
        render_context["state_tags_csv"] = ", ".join(
            _coerce_string_list(current_state.get("state_tags"), database_path=Path("<render-context>"), field_name="state_tags")
        )
    return render_context


def _build_current_state_payload(
    *,
    explicit_state: Mapping[str, object],
    run_details: RunDetails | None,
    step_details: StepRunDetails | None,
    control_state,
    step_runs: Sequence | None,
    flow_summary: Mapping[str, object],
) -> dict[str, object]:
    state: dict[str, object] = dict(explicit_state)
    latest_steps_by_key: dict[str, dict[str, object]] = {}
    if step_runs is not None:
        for step_run in step_runs:
            latest_steps_by_key[step_run.step_key] = step_run.to_dict()

    state_tags: set[str] = set(_coerce_string_list(state.get("state_tags"), database_path=Path("<current-state>"), field_name="state_tags"))
    if run_details is not None:
        run = run_details.run
        state["run"] = run.to_dict()
        state["run_status"] = run.status
        state["project_key"] = run.project_key
        state["project_profile"] = run.project_profile
        state["workflow_id"] = run.workflow_id
        state["flow_id"] = run.flow_id
        state["queue_status"] = run.queue_item.status if run.queue_item is not None else None
        state_tags.add(f"run_{run.status}")
        if run.queue_item is not None:
            state_tags.add(f"queue_{run.queue_item.status}")
        if run.parent_run_id is None:
            state_tags.add("flow_root")
        else:
            state_tags.add("flow_followup")
        if run.status in {"completed", "failed", "stopped", "cancelled"}:
            state_tags.add("run_terminal")

    if control_state is not None:
        state["control_state"] = control_state.to_dict()
        state["active_step_key"] = control_state.active_step_key
        state["active_step_run_id"] = control_state.active_step_run_id
        if control_state.paused:
            state_tags.add("manual_paused")
        if control_state.pending_rerun is not None:
            state["pending_rerun"] = control_state.pending_rerun.to_dict()
            state_tags.add("pending_rerun")

    if step_details is not None:
        state["step_run"] = step_details.to_dict()

    state["flow_summary"] = dict(flow_summary)
    if step_runs is not None:
        state["latest_step_by_key"] = latest_steps_by_key
        state["latest_steps"] = [step_run.to_dict() for step_run in step_runs]
        for step_run in step_runs:
            state_tags.add(f"{step_run.step_key}_started")
            if step_run.status == "running":
                state_tags.add(f"{step_run.step_key}_running")
            elif step_run.status == "succeeded":
                state_tags.add(f"{step_run.step_key}_succeeded")
            elif step_run.status in {"failed", "timed_out", "cancelled"}:
                state_tags.add(f"{step_run.step_key}_failed")

    if _normalize_optional_text(state.get("active_step_key")) == "executor":
        state_tags.add("executor_running")
    if _normalize_optional_text(state.get("active_step_key")) == "reviewer":
        state_tags.add("reviewer_running")

    state["state_tags"] = sorted(state_tags)
    return state


def _build_flow_summary(flow_runs: Sequence | None, run_id: str | None) -> dict[str, object]:
    if not flow_runs or run_id is None:
        return {}
    root_run_id = flow_runs[0].run.id
    current_cycle = None
    for flow_run in flow_runs:
        if flow_run.run.id == run_id:
            current_cycle = flow_run.cycle_no
            break
    return {
        "flow_id": flow_runs[0].run.flow_id,
        "root_run_id": root_run_id,
        "current_run_id": run_id,
        "current_cycle": current_cycle,
        "total_runs": len(flow_runs),
    }


def _build_runtime_context_payload(
    database_path: Path,
    *,
    request: Mapping[str, object],
    run_details: RunDetails | None,
    flow_id: str | None,
) -> dict[str, object]:
    resolved: dict[str, object] = {}
    if run_details is not None:
        resolved.update(_load_runtime_context_from_artifacts(database_path, run_id=run_details.run.id, flow_id=flow_id))
        handoff_commit_sha = _load_latest_handoff_commit(database_path, run_id=run_details.run.id, flow_id=flow_id)
        if handoff_commit_sha is not None:
            resolved["handoff_commit_sha"] = handoff_commit_sha
    request_runtime_context = request.get("runtime_context")
    if isinstance(request_runtime_context, Mapping):
        resolved.update(dict(request_runtime_context))
    artifact_root = request.get("artifact_root")
    if isinstance(artifact_root, Path):
        resolved["artifact_root"] = str(artifact_root)
    return resolved


def _load_runtime_context_from_artifacts(database_path: Path, *, run_id: str, flow_id: str | None) -> dict[str, object]:
    resolved: dict[str, object] = {}
    for artifact_kind in _RUNTIME_CONTEXT_ARTIFACT_KINDS:
        flow_manifest = _load_latest_artifact_manifest(database_path, flow_id=flow_id, artifact_kind=artifact_kind)
        flow_context = _extract_runtime_context_from_manifest(flow_manifest)
        if flow_context is not None:
            resolved.update(dict(flow_context))
        run_manifest = _load_latest_artifact_manifest(database_path, run_id=run_id, artifact_kind=artifact_kind)
        run_context = _extract_runtime_context_from_manifest(run_manifest)
        if run_context is not None:
            resolved.update(dict(run_context))
    return resolved


def _load_latest_handoff_commit(database_path: Path, *, run_id: str, flow_id: str | None) -> str | None:
    for manifest in (
        _load_latest_artifact_manifest(database_path, run_id=run_id, artifact_kind=ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST, step_key="executor"),
        _load_latest_artifact_manifest(database_path, flow_id=flow_id, artifact_kind=ARTIFACT_KIND_DISPATCH_RESULT_MANIFEST, step_key="executor"),
    ):
        if not isinstance(manifest, Mapping):
            continue
        dispatch_outcome = manifest.get("dispatch_outcome")
        if isinstance(dispatch_outcome, Mapping):
            commit_sha = _normalize_optional_text(dispatch_outcome.get("commit_sha"))
            if commit_sha is not None:
                return commit_sha
    return None


def _resolve_project_key(
    request: Mapping[str, object],
    run_details: RunDetails | None,
    step_details: StepRunDetails | None,
    database_path: Path,
) -> str:
    candidates = (
        _normalize_optional_text(request.get("project_key")),
        step_details.step_run.project_key if step_details is not None else None,
        run_details.run.project_key if run_details is not None else None,
    )
    for candidate in candidates:
        if candidate is not None:
            return candidate
    raise BoundedContractError(
        code=CONTRACT_GENERATION_INVALID,
        message="project_key is required when it cannot be inferred from run_id or step_run_id",
        database_path=database_path,
    )


def _validate_request_scope(
    *,
    request: Mapping[str, object],
    run_details: RunDetails | None,
    step_details: StepRunDetails | None,
    database_path: Path,
) -> None:
    if run_details is not None and step_details is not None and step_details.step_run.run_id != run_details.run.id:
        raise BoundedContractError(
            code=CONTRACT_GENERATION_INVALID,
            message="step_run_id does not belong to the provided run_id",
            database_path=database_path,
            details=f"run_id={run_details.run.id} step_run.run_id={step_details.step_run.run_id}",
        )

    _validate_request_field_match(
        field_name="project_key",
        requested_value=_normalize_optional_text(request.get("project_key")),
        resolved_values=(
            step_details.step_run.project_key if step_details is not None else None,
            run_details.run.project_key if run_details is not None else None,
        ),
        database_path=database_path,
    )
    _validate_request_field_match(
        field_name="workflow_id",
        requested_value=_normalize_optional_text(request.get("workflow_id")),
        resolved_values=(
            step_details.step_run.workflow_id if step_details is not None else None,
            run_details.run.workflow_id if run_details is not None else None,
        ),
        database_path=database_path,
    )
    _validate_request_field_match(
        field_name="project_profile",
        requested_value=_normalize_optional_text(request.get("project_profile")),
        resolved_values=(
            step_details.step_run.project_profile if step_details is not None else None,
            run_details.run.project_profile if run_details is not None else None,
        ),
        database_path=database_path,
    )


def _validate_request_field_match(
    *,
    field_name: str,
    requested_value: str | None,
    resolved_values: Sequence[str | None],
    database_path: Path,
) -> None:
    if requested_value is None:
        return
    expected_values = {value for value in resolved_values if value is not None}
    if not expected_values or requested_value in expected_values:
        return
    raise BoundedContractError(
        code=CONTRACT_GENERATION_INVALID,
        message=f"{field_name} conflicts with the runtime scope selected by run_id/step_run_id",
        database_path=database_path,
        details=f"requested={requested_value} expected={sorted(expected_values)}",
    )


def _resolve_workflow_id(
    request: Mapping[str, object],
    run_details: RunDetails | None,
    step_details: StepRunDetails | None,
    database_path: Path,
) -> str:
    candidates = (
        _normalize_optional_text(request.get("workflow_id")),
        step_details.step_run.workflow_id if step_details is not None else None,
        run_details.run.workflow_id if run_details is not None else None,
    )
    for candidate in candidates:
        if candidate is not None:
            return candidate
    raise BoundedContractError(
        code=CONTRACT_GENERATION_INVALID,
        message="workflow_id is required when it cannot be inferred from run_id or step_run_id",
        database_path=database_path,
    )


def _resolve_project_profile(
    request: Mapping[str, object],
    run_details: RunDetails | None,
    step_details: StepRunDetails | None,
    database_path: Path,
) -> str:
    candidates = (
        _normalize_optional_text(request.get("project_profile")),
        step_details.step_run.project_profile if step_details is not None else None,
        run_details.run.project_profile if run_details is not None else None,
    )
    for candidate in candidates:
        if candidate is not None:
            return candidate
    raise BoundedContractError(
        code=CONTRACT_GENERATION_INVALID,
        message="project_profile is required when it cannot be inferred from run_id or step_run_id",
        database_path=database_path,
    )


def _resolve_contract_output_root(
    *,
    request: Mapping[str, object],
    run_details: RunDetails | None,
    flow_summary: Mapping[str, object],
    runtime_context: Mapping[str, object],
    project_key: str,
    workflow_id: str,
    contract_id: str,
) -> Path:
    explicit_root = request.get("artifact_root")
    artifact_root = explicit_root if isinstance(explicit_root, Path) else _normalize_optional_path(runtime_context.get("artifact_root"))
    if artifact_root is not None and run_details is not None:
        flow_id = _normalize_optional_text(flow_summary.get("flow_id")) or run_details.run.flow_id
        output_root = artifact_root / project_key / flow_id / run_details.run.id / "contracts" / contract_id
    else:
        output_root = CONTROL_DIR / ".logs" / "bounded-contracts" / project_key / workflow_id / contract_id
    output_root.mkdir(parents=True, exist_ok=True)
    return output_root


def _insert_contract_manifest_row(
    database_path: Path,
    *,
    contract_id: str,
    project_id: str,
    flow_id: str | None,
    run_id: str | None,
    step_run_id: str | None,
    workflow_id: str,
    project_profile: str,
    contract_type: str,
    template_key: str,
    contract_json_path: Path,
    prompt_text_path: Path,
    manifest_json_path: Path,
    created_at: str,
) -> None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("projects", "contract_manifests"))
        connection.execute("BEGIN")
        connection.execute(
            """
            INSERT INTO contract_manifests (
              id,
              project_id,
              flow_id,
              run_id,
              step_run_id,
              workflow_id,
              project_profile,
              contract_type,
              template_key,
              contract_json_path,
              prompt_text_path,
              manifest_json_path,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                contract_id,
                project_id,
                flow_id,
                run_id,
                step_run_id,
                workflow_id,
                project_profile,
                contract_type,
                template_key,
                str(contract_json_path),
                str(prompt_text_path),
                str(manifest_json_path),
                created_at,
            ),
        )
        connection.commit()
    except sqlite3.Error as exc:
        connection.rollback()
        raise BoundedContractError(
            code=CONTRACT_STORAGE_ERROR,
            message=f"Failed to persist contract manifest row: {contract_id}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _record_contract_artifacts(
    database_path: Path,
    *,
    project_id: str,
    flow_id: str | None,
    run_id: str | None,
    step_run_id: str | None,
    created_at: str,
    artifact_paths: Sequence[tuple[str, Path]],
) -> tuple[BoundedContractArtifact, ...]:
    if run_id is None or flow_id is None:
        return tuple(
            BoundedContractArtifact(
                artifact_kind=artifact_kind,
                filesystem_path=filesystem_path.expanduser().resolve(),
                created_at=created_at,
                artifact_ref_id=None,
            )
            for artifact_kind, filesystem_path in artifact_paths
        )

    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("artifact_refs",))
        connection.execute("BEGIN")
        artifacts: list[BoundedContractArtifact] = []
        for artifact_kind, filesystem_path in artifact_paths:
            resolved_path = filesystem_path.expanduser().resolve()
            artifact_ref_id = generate_opaque_id()
            media_type = mimetypes.guess_type(str(resolved_path))[0]
            size_bytes = resolved_path.stat().st_size
            checksum_sha256 = _sha256_for_path(resolved_path)
            connection.execute(
                """
                INSERT INTO artifact_refs (
                  id,
                  project_id,
                  flow_id,
                  run_id,
                  step_run_id,
                  artifact_kind,
                  filesystem_path,
                  media_type,
                  size_bytes,
                  checksum_sha256,
                  created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_ref_id,
                    project_id,
                    flow_id,
                    run_id,
                    step_run_id,
                    artifact_kind,
                    str(resolved_path),
                    media_type,
                    size_bytes,
                    checksum_sha256,
                    created_at,
                ),
            )
            artifacts.append(
                BoundedContractArtifact(
                    artifact_kind=artifact_kind,
                    filesystem_path=resolved_path,
                    created_at=created_at,
                    artifact_ref_id=artifact_ref_id,
                )
            )
        connection.commit()
        return tuple(artifacts)
    except (sqlite3.Error, OSError) as exc:
        connection.rollback()
        raise BoundedContractError(
            code=CONTRACT_STORAGE_ERROR,
            message="Failed to persist contract artifact refs",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _load_contract_manifest_row(database_path: Path, contract_id: str) -> sqlite3.Row | None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("projects", "contract_manifests"))
        return connection.execute(
            """
            SELECT
              contract_manifests.id,
              contract_manifests.flow_id,
              contract_manifests.run_id,
              contract_manifests.step_run_id,
              contract_manifests.workflow_id,
              contract_manifests.project_profile,
              contract_manifests.contract_type,
              contract_manifests.template_key,
              contract_manifests.contract_json_path,
              contract_manifests.prompt_text_path,
              contract_manifests.manifest_json_path,
              contract_manifests.created_at,
              projects.project_key,
              projects.package_root
            FROM contract_manifests
            JOIN projects ON projects.id = contract_manifests.project_id
            WHERE contract_manifests.id = ?
            """,
            (contract_id,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise BoundedContractError(
            code=CONTRACT_STORAGE_ERROR,
            message="Failed to load contract manifest row",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _load_contract_artifacts(database_path: Path, contract_id: str) -> tuple[BoundedContractArtifact, ...]:
    row = _load_contract_manifest_row(database_path, contract_id)
    if row is None:
        return ()
    path_map = {
        ARTIFACT_KIND_BOUNDED_CONTRACT_JSON: Path(str(row["contract_json_path"])).expanduser().resolve(),
        ARTIFACT_KIND_BOUNDED_CONTRACT_PROMPT: Path(str(row["prompt_text_path"])).expanduser().resolve(),
        ARTIFACT_KIND_BOUNDED_CONTRACT_MANIFEST: Path(str(row["manifest_json_path"])).expanduser().resolve(),
    }
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("artifact_refs",))
        records: list[BoundedContractArtifact] = []
        for artifact_kind, path in path_map.items():
            artifact_row = connection.execute(
                """
                SELECT id, created_at
                FROM artifact_refs
                WHERE filesystem_path = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (str(path),),
            ).fetchone()
            records.append(
                BoundedContractArtifact(
                    artifact_kind=artifact_kind,
                    filesystem_path=path,
                    created_at=str(artifact_row["created_at"]) if artifact_row is not None else str(row["created_at"]),
                    artifact_ref_id=str(artifact_row["id"]) if artifact_row is not None else None,
                )
            )
        return tuple(records)
    except sqlite3.Error as exc:
        raise BoundedContractError(
            code=CONTRACT_STORAGE_ERROR,
            message="Failed to load contract artifacts",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _load_registered_project_row(database_path: Path, project_key: str) -> sqlite3.Row | None:
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("projects",))
        return connection.execute(
            """
            SELECT id, project_key, package_root
            FROM projects
            WHERE project_key = ?
            """,
            (project_key,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise BoundedContractError(
            code=CONTRACT_STORAGE_ERROR,
            message="Failed to look up registered project",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()


def _load_latest_artifact_manifest(
    database_path: Path,
    *,
    run_id: str | None = None,
    flow_id: str | None = None,
    artifact_kind: str,
    step_key: str | None = None,
) -> Mapping[str, object] | None:
    if run_id is None and flow_id is None:
        return None
    connection = _connect_run_db(database_path)
    try:
        _ensure_required_tables(connection, database_path, ("artifact_refs", "step_runs"))
        filters = ["artifact_refs.artifact_kind = ?"]
        params: list[object] = [artifact_kind]
        if run_id is not None:
            filters.append("artifact_refs.run_id = ?")
            params.append(run_id)
        elif flow_id is not None:
            filters.append("artifact_refs.flow_id = ?")
            params.append(flow_id)
        if step_key is not None:
            filters.append("step_runs.step_key = ?")
            params.append(step_key)
        row = connection.execute(
            f"""
            SELECT artifact_refs.filesystem_path
            FROM artifact_refs
            LEFT JOIN step_runs ON step_runs.id = artifact_refs.step_run_id
            WHERE {' AND '.join(filters)}
            ORDER BY artifact_refs.created_at DESC, artifact_refs.id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    except sqlite3.Error as exc:
        raise BoundedContractError(
            code=CONTRACT_STORAGE_ERROR,
            message="Failed to resolve artifact manifest for bounded contract generation",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.close()
    if row is None:
        return None
    return _read_json_optional(Path(str(row["filesystem_path"])).expanduser().resolve())


def _extract_runtime_context_from_manifest(manifest: Mapping[str, object] | None) -> Mapping[str, object] | None:
    if manifest is None:
        return None
    runtime_context = manifest.get("runtime_context")
    if isinstance(runtime_context, Mapping):
        return runtime_context
    if any(key in manifest for key in ("task_text", "project_repo_path", "executor_worktree_path", "instructions_repo_path")):
        return manifest
    return None


def _load_run_details_or_raise(database_path: Path, run_id: str) -> RunDetails:
    try:
        from .run_persistence import get_run

        return get_run(database_path, run_id)
    except RunPersistenceError as exc:
        raise BoundedContractError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _load_step_run_details_or_raise(database_path: Path, step_run_id: str) -> StepRunDetails:
    try:
        return get_step_run(database_path, step_run_id)
    except StepRunPersistenceError as exc:
        raise BoundedContractError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _load_control_state_or_none(database_path: Path, run_id: str | None):
    if run_id is None:
        return None
    try:
        return show_run_control_state(database_path, run_id)
    except ManualControlError as exc:
        raise BoundedContractError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _load_flow_runs_or_none(database_path: Path, flow_id: str | None):
    if flow_id is None:
        return None
    try:
        return list_flow_runs(database_path, flow_id, limit=100)
    except ReviewerOutcomeError as exc:
        raise BoundedContractError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _load_step_runs_or_none(database_path: Path, run_id: str | None):
    if run_id is None:
        return None
    try:
        return list_step_runs(database_path, run_id=run_id, limit=100)
    except StepRunPersistenceError as exc:
        raise BoundedContractError(
            code=exc.code,
            message=exc.message,
            database_path=exc.database_path,
            details=exc.details,
        ) from exc


def _render_value(value: object, render_context: Mapping[str, str], database_path: Path) -> object:
    if isinstance(value, str):
        return _render_string(value, render_context, database_path)
    if isinstance(value, list):
        return [_render_value(item, render_context, database_path) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _render_value(item, render_context, database_path) for key, item in value.items()}
    return value


def _render_string(template: str, render_context: Mapping[str, str], database_path: Path) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group("key")
        if key not in render_context:
            raise BoundedContractError(
                code=CONTRACT_TEMPLATE_INVALID,
                message=f"Template placeholder is not available in render context: {key}",
                database_path=database_path,
            )
        return render_context[key]

    return _PLACEHOLDER_RE.sub(replace, template)


def _read_json_optional(path: Path) -> dict[str, object] | None:
    try:
        raw = path.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _read_json_required(path: Path, database_path: Path, contract_id: str) -> dict[str, object]:
    payload = _read_json_optional(path)
    if payload is None:
        raise BoundedContractError(
            code=CONTRACT_STORAGE_ERROR,
            message=f"Stored contract JSON is unreadable for {contract_id}",
            database_path=database_path,
            details=f"path={path}",
        )
    return payload


def _read_text_required(path: Path, database_path: Path, contract_id: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise BoundedContractError(
            code=CONTRACT_STORAGE_ERROR,
            message=f"Stored contract prompt is unreadable for {contract_id}",
            database_path=database_path,
            details=str(exc),
        ) from exc


def _render_bullet_section(value: object) -> list[str]:
    items = _string_list_from_value(value)
    if not items:
        return ["- none"]
    return [f"- {item}" for item in items]


def _string_list_from_value(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    normalized = _normalize_optional_text(value)
    return [normalized] if normalized is not None else []


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _sha256_for_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _coerce_string_list(value: object, *, database_path: Path, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    raise BoundedContractError(
        code=CONTRACT_GENERATION_INVALID,
        message=f"{field_name} must be a string or list of strings",
        database_path=database_path,
        details=f"actual_type={type(value).__name__}",
    )


def _normalize_optional_mapping(value: object, *, field_name: str, database_path: Path) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise BoundedContractError(
            code=CONTRACT_GENERATION_INVALID,
            message=f"{field_name} must be a mapping/object",
            database_path=database_path,
            details=f"actual_type={type(value).__name__}",
        )
    return dict(value)


def _normalize_optional_path(value: object) -> Path | None:
    normalized = _normalize_optional_text(value)
    return Path(normalized).expanduser().resolve() if normalized is not None else None


def _normalize_optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_scalar_for_render(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return str(value)
    return None


def _mapping_get(value: object, key: str) -> object:
    if isinstance(value, Mapping):
        return value.get(key)
    return None


def _require_text(name: str, value: object, database_path: Path) -> str:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        raise BoundedContractError(
            code=CONTRACT_GENERATION_INVALID,
            message=f"{name} must be a non-empty string",
            database_path=database_path,
        )
    return normalized


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
