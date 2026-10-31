"""Explicit local-credential review -> durable task admission, never dispatch.

This is a trusted host API, not a model tool, voice participant authenticator,
HTTP endpoint or hostile-Python boundary. Pending reviews are process-local.
"""
from dataclasses import asdict, dataclass, field
import json
import uuid

from runtime.policy_engine import PermissionDeniedError
from runtime.proposal_contract import (
    ProposalAdmissionError, bounded_arguments, digest, identity, required_permissions, validate_binding,
)
from runtime.security_authority import SecurityAuthority
from runtime.voice_ingress import VoiceIngress, VoiceProposal
from runtime.workflow_contract import WorkflowVersion, binding_refs, canonical


@dataclass(frozen=True)
class AdmissionTemplate:
    """Deployment allowlist: static reviewed JSON only, no lazy argument builders."""
    name: str
    workflow: WorkflowVersion
    steps: tuple


@dataclass(frozen=True)
class ProposalReview:
    review_id: str
    task_id: str
    binding_digest: str
    _details: str = field(repr=False)

    def details(self):
        """Protected review presentation; mutations of this copy confer no authority."""
        return json.loads(self._details)


@dataclass(frozen=True)
class AdmissionReceipt:
    task_id: str
    binding_digest: str


@dataclass(frozen=True)
class _Pending:
    checkpoint: str = field(repr=False)
    proposal: VoiceProposal = field(repr=False)
    refs: tuple = field(repr=False)
    workflow_id: str
    expires_ms: int


class ProposalAdmissionService:
    def __init__(self, manager, ingress, templates, *, source_id, max_pending=128):
        if type(ingress) is not VoiceIngress or manager._saga.journal is None:
            raise ProposalAdmissionError("PROPOSAL_DURABLE_HOST_REQUIRED")
        if type(max_pending) is not int or not 1 <= max_pending <= 128:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_CAPACITY_INVALID")
        self.manager, self.ingress = manager, ingress
        self.journal = manager._saga.journal
        self.authority = SecurityAuthority(self.journal)
        self.source_id = identity(source_id)  # Trusted adapter label, NOT verified speaker identity.
        self._templates, self._pending = {}, {}
        self.max_pending = max_pending
        for template in templates:
            if type(template) is not AdmissionTemplate or type(template.steps) is not tuple or not 1 <= len(template.steps) <= 32:
                raise ProposalAdmissionError("PROPOSAL_TEMPLATE_INVALID")
            identity(template.name)
            manager._capture_workflow(template.workflow, list(template.steps))
            if any(step.build_args is not None or step.required_fields for step in template.steps):
                raise ProposalAdmissionError("PROPOSAL_STATIC_ARGUMENTS_REQUIRED")
            if template.workflow.workflow_id in self._templates:
                raise ProposalAdmissionError("PROPOSAL_DUPLICATE_WORKFLOW")
            self._templates[template.workflow.workflow_id] = template

    def _current(self, proposal):
        self.ingress._owned()
        if type(proposal) is not VoiceProposal or proposal is not self.ingress.latest or not proposal.grounded:
            raise ProposalAdmissionError("PROPOSAL_CURRENT_GROUNDED_EVIDENCE_REQUIRED")

    def review(self, proposal, workflow_id, arguments, *, token, ttl_ms=60000):
        """Prepare exact structured arguments for explicit authenticated acceptance.

        No inference is made that argument values follow from transcript words.
        The reviewer must inspect that mapping. Grounding is an acoustic gate.
        """
        self._current(proposal)
        if type(ttl_ms) is not int or not 1 <= ttl_ms <= 300000:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_LIFETIME_INVALID")
        identity(workflow_id)
        template = self._templates.get(workflow_id)
        if template is None:
            raise ProposalAdmissionError("PROPOSAL_WORKFLOW_NOT_ALLOWLISTED")
        arguments = bounded_arguments(arguments)
        task_id, review_id = "task-" + uuid.uuid4().hex, uuid.uuid4().hex
        cp = self.manager.prepare_checkpoint(task_id, template.name, list(template.steps), workflow=template.workflow)
        permissions = required_permissions(cp["workflow_contract"])
        with self.journal._transaction() as db:
            now = self.authority._now(db)
            actor = self.authority._authenticate(db, token, now)
            if not set(permissions) <= actor.permissions:
                raise PermissionDeniedError("Proposal review permissions required")
        self._pending = {key: value for key, value in self._pending.items() if now < value.expires_ms}
        if len(self._pending) >= self.max_pending:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_CAPACITY")
        binding = {"schema": 1, "review_id": review_id, "task_id": task_id, "task_name": template.name,
                   "reviewer": {"principal_id": actor.principal_id, "version": actor.version,
                                "credential_id": actor.credential_id},
                   "source": {"source_id": self.source_id, "stream": proposal.handle.stream,
                              "turn": proposal.handle.turn, "revision": proposal.revision,
                              "evidence_digest": digest(asdict(proposal))},
                   "workflow_digest": cp["workflow_contract"]["digest"], "arguments": arguments,
                   "required_permissions": permissions, "reviewed_ms": now,
                   "expires_ms": min(now + ttl_ms, actor.expires_ms)}
        binding["digest"] = digest(binding)
        cp["proposal_binding"] = binding
        validate_binding(binding, cp)
        self._pending[review_id] = _Pending(canonical(cp), proposal,
                                          binding_refs(list(template.steps), self.manager._saga.registry),
                                          workflow_id, binding["expires_ms"])
        return ProposalReview(review_id, task_id, binding["digest"], canonical(binding))

    def admit(self, review_id, *, token):
        """Explicit accept; retries reuse identity and never execute a mutation.

        On ambiguous storage failure inspect task_id. Never manufacture a new
        review/task as an automatic retry. Credentials are rechecked in SQLite.
        """
        self.ingress._owned()
        identity(review_id)
        pending = self._pending.get(review_id)
        if pending is None:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_UNAVAILABLE")
        self._current(pending.proposal)
        cp = json.loads(pending.checkpoint)
        template = self._templates[pending.workflow_id]
        current = self.manager._capture_workflow(template.workflow, list(template.steps))
        refs = binding_refs(list(template.steps), self.manager._saga.registry)
        if (current != canonical(cp["workflow_contract"]) or len(refs) != len(pending.refs) or
                any(a is not b for a, b in zip(refs, pending.refs))):
            raise ProposalAdmissionError("PROPOSAL_DEPLOYMENT_CHANGED")
        self.journal.admit_task(cp, token)
        return AdmissionReceipt(cp["task_id"], cp["proposal_binding"]["digest"])
