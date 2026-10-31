"""Versioned, immutable review provenance. Evidence hashes are not identity proof."""
import hashlib
import json
import math
import re

from runtime.workflow_contract import canonical, validate_record


class ProposalAdmissionError(ValueError):
    """A proposal cannot cross the durable admission boundary."""


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def bounded_arguments(value):
    """Copy exact JSON values; reject coercion, excessive depth/size and nonfinite data."""
    remaining = 4096
    characters = 65536

    def text(item):
        nonlocal characters
        characters -= len(item)
        if characters < 0:
            raise ProposalAdmissionError("PROPOSAL_ARGUMENT_CAPACITY")

    def visit(item, depth):
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > 32:
            raise ProposalAdmissionError("PROPOSAL_ARGUMENT_CAPACITY")
        if type(item) is dict:
            if any(type(key) is not str for key in item):
                raise ProposalAdmissionError("PROPOSAL_ARGUMENT_KEYS")
            for key in item:
                text(key)
            for child in item.values():
                visit(child, depth + 1)
        elif type(item) is list:
            for child in item:
                visit(child, depth + 1)
        elif type(item) not in (str, int, float, bool, type(None)):
            raise ProposalAdmissionError("PROPOSAL_ARGUMENT_TYPE")
        elif type(item) is float and not math.isfinite(item):
            raise ProposalAdmissionError("PROPOSAL_ARGUMENT_NONFINITE")
        elif type(item) is str:
            text(item)
        elif type(item) is int and item.bit_length() > 4096:
            raise ProposalAdmissionError("PROPOSAL_ARGUMENT_INTEGER_CAPACITY")

    if type(value) is not list or not 1 <= len(value) <= 32 or any(type(v) is not dict for v in value):
        raise ProposalAdmissionError("PROPOSAL_ARGUMENT_STEPS")
    visit(value, 0)
    try:
        encoded = canonical(value)
        if len(encoded.encode("utf-8")) > 65536:
            raise ProposalAdmissionError("PROPOSAL_ARGUMENT_CAPACITY")
        return json.loads(encoded)
    except (UnicodeError, RecursionError) as exc:
        raise ProposalAdmissionError("PROPOSAL_ARGUMENT_ENCODING") from exc


def identity(value):
    if type(value) is not str or not value or value.strip() != value or len(value) > 256:
        raise ProposalAdmissionError("PROPOSAL_IDENTITY_INVALID")
    return value


def required_permissions(contract):
    permissions = {"ccs:task:create"}
    for step in contract["steps"]:
        declared = step["manifest"]["required_permissions"]
        if type(declared) is not list:
            raise ProposalAdmissionError("PROPOSAL_PERMISSIONS_INVALID")
        permissions.update(identity(permission) for permission in declared)
    return sorted(permissions)


def validate_binding(binding, checkpoint):
    """Validate provenance against the task, not merely its self-declared digest."""
    keys = {"schema", "review_id", "task_id", "task_name", "reviewer", "source", "workflow_digest",
            "arguments", "required_permissions", "reviewed_ms", "expires_ms", "digest"}
    if type(binding) is not dict or set(binding) != keys or type(binding["schema"]) is not int or binding["schema"] != 1:
        raise ProposalAdmissionError("PROPOSAL_BINDING_SCHEMA")
    for key in ("review_id", "task_id", "task_name"):
        identity(binding[key])
    reviewer, source = binding["reviewer"], binding["source"]
    if (type(reviewer) is not dict or set(reviewer) != {"principal_id", "version", "credential_id"} or
            type(reviewer["version"]) is not int or reviewer["version"] < 1):
        raise ProposalAdmissionError("PROPOSAL_REVIEWER_INVALID")
    identity(reviewer["principal_id"])
    identity(reviewer["credential_id"])
    if (type(source) is not dict or set(source) != {"source_id", "stream", "turn", "revision", "evidence_digest"} or
            any(type(source[k]) is not int or source[k] < 1 for k in ("stream", "turn", "revision"))):
        raise ProposalAdmissionError("PROPOSAL_SOURCE_INVALID")
    identity(source["source_id"])
    if type(source["evidence_digest"]) is not str or re.fullmatch("[0-9a-f]{64}", source["evidence_digest"]) is None:
        raise ProposalAdmissionError("PROPOSAL_EVIDENCE_DIGEST_INVALID")
    start, end = binding["reviewed_ms"], binding["expires_ms"]
    if type(start) is not int or type(end) is not int or start < 0 or not 1 <= end - start <= 300000:
        raise ProposalAdmissionError("PROPOSAL_REVIEW_LIFETIME_INVALID")
    contract = checkpoint.get("workflow_contract")
    validate_record(contract)
    if (binding["task_id"] != checkpoint.get("task_id") or binding["task_name"] != checkpoint.get("name") or
            binding["workflow_digest"] != contract["digest"] or
            binding["required_permissions"] != required_permissions(contract) or
            len(bounded_arguments(binding["arguments"])) != len(contract["steps"]) or
            any(s["has_argument_builder"] or s["required_fields"] for s in contract["steps"])):
        raise ProposalAdmissionError("PROPOSAL_TASK_BINDING_MISMATCH")
    if binding["digest"] != digest({k: v for k, v in binding.items() if k != "digest"}):
        raise ProposalAdmissionError("PROPOSAL_BINDING_DIGEST_MISMATCH")
