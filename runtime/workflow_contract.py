"""Explicit deployment contracts, not inferred equivalence of Python functions."""
from dataclasses import asdict, dataclass, fields
import hashlib
import json
import math


class WorkflowContractError(ValueError):
    """Workflow identity is absent, invalid or incompatible."""


@dataclass(frozen=True)
class WorkflowVersion:
    workflow_id: str
    revision: str


@dataclass(frozen=True)
class StepVersions:
    step_id: str
    arguments: str
    executor: str
    provider: str
    provider_scope: str
    compensation: str | None = None
    reconciliation: str | None = None
    validator: str | None = None


def canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise WorkflowContractError("workflow contract must be finite JSON") from exc


def _identity(value):
    if type(value) is not str or not value or value.strip() != value or len(value) > 256:
        raise WorkflowContractError("workflow identities/revisions must be nonempty strings of at most 256 characters")


def validate_record(record):
    if (type(record) is not dict or set(record) != {"schema", "workflow_id", "revision", "steps", "digest"} or
            type(record["schema"]) is not int or record["schema"] != 1 or type(record["steps"]) is not list):
        raise WorkflowContractError("Missing or unsupported workflow contract")
    _identity(record["workflow_id"])
    _identity(record["revision"])
    body = {key: value for key, value in record.items() if key != "digest"}
    if record["digest"] != hashlib.sha256(canonical(body).encode()).hexdigest():
        raise WorkflowContractError("workflow contract digest mismatch")
    for step in record["steps"]:
        if (type(step) is not dict or type(step.get("tool_name")) is not str or
                type(step.get("kind")) is not str or step["kind"] not in {"read", "write"}):
            raise WorkflowContractError("Invalid workflow step definitions")
    return record


def capture(workflow, steps, registry):
    if type(workflow) is not WorkflowVersion or type(steps) is not list:
        raise WorkflowContractError("Explicit workflow version and step definitions required")
    _identity(workflow.workflow_id)
    _identity(workflow.revision)
    definitions, seen = [], set()
    for step in steps:
        versions = step.versions
        if type(versions) is not StepVersions:
            raise WorkflowContractError("Explicit workflow step revisions required")
        revisions = asdict(versions)
        for key, value in revisions.items():
            if value is not None or key in {"step_id", "arguments", "executor", "provider", "provider_scope"}:
                _identity(value)
        if versions.step_id in seen:
            raise WorkflowContractError("Duplicate workflow step identity")
        seen.add(versions.step_id)
        _identity(step.tool_name)
        if type(step.kind) is not str or step.kind not in {"read", "write"}:
            raise WorkflowContractError("Invalid workflow step kind")
        if type(step.required_fields) is not list:
            raise WorkflowContractError("workflow required fields must be a list")
        for name in step.required_fields:
            _identity(name)
        if len(set(step.required_fields)) != len(step.required_fields):
            raise WorkflowContractError("Duplicate workflow required field")
        manifest = registry.get(step.tool_name) if registry is not None else None
        if manifest is None or manifest.kind != step.kind:
            raise WorkflowContractError("Registered matching workflow tool manifest required")
        for callback, revision in ((step.compensate, versions.compensation),
                                   (step.status_check, versions.reconciliation),
                                   (manifest.validate_args, versions.validator)):
            if (callback is None) != (revision is None) or (callback is not None and not callable(callback)):
                raise WorkflowContractError("workflow handler presence must match its explicit revision")
        if step.build_args is not None and not callable(step.build_args):
            raise WorkflowContractError("workflow argument builder must be callable")
        callbacks = {name: getattr(manifest, name) is not None
                     for name in ("compensate", "reconciliation", "validate_args")}
        metadata = {f.name: getattr(manifest, f.name) for f in fields(manifest) if f.name not in callbacks}
        for name in ("idempotent", "cancellable", "requires_authoritative_commit", "requires_confirmation"):
            if type(metadata[name]) is not bool:
                raise WorkflowContractError("workflow manifest flags must be boolean")
        if (type(manifest.timeout) not in {int, float} or not math.isfinite(manifest.timeout) or manifest.timeout <= 0):
            raise WorkflowContractError("workflow timeout must be finite and positive")
        metadata["timeout"] = float(manifest.timeout)
        definitions.append({"tool_name": step.tool_name, "kind": step.kind,
                            "required_fields": list(step.required_fields), "versions": revisions,
                            "has_argument_builder": step.build_args is not None,
                            "manifest": metadata, "manifest_handlers": callbacks})
    body = {"schema": 1, **asdict(workflow), "steps": definitions}
    return canonical({**body, "digest": hashlib.sha256(canonical(body).encode()).hexdigest()})


def binding_refs(steps, registry):
    """In-process replacement detection; intentionally not serialized code hashes."""
    refs = []
    for step in steps:
        refs.extend((step.build_args, step.compensate, step.status_check))
        manifest = registry.get(step.tool_name)
        refs.extend((manifest.compensate, manifest.reconciliation, manifest.validate_args))
    return tuple(refs)
