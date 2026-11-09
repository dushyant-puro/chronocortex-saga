"""Explicit local-credential review -> durable task admission, never dispatch.

This is a trusted host API, not a model tool, voice participant authenticator,
HTTP endpoint or hostile-Python boundary. Pending reviews are process-local.
"""
from dataclasses import asdict, dataclass, field, replace
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
    session_scope: str | None = None


class ProposalAdmissionService:
    def __init__(self, manager, ingress, templates, *, source_id, max_pending=128, retain_reviews=False):
        if type(ingress) is not VoiceIngress or manager._saga.journal is None:
            raise ProposalAdmissionError("PROPOSAL_DURABLE_HOST_REQUIRED")
        if type(max_pending) is not int or not 1 <= max_pending <= 128:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_CAPACITY_INVALID")
        if type(retain_reviews) is not bool:
            raise ProposalAdmissionError("PROPOSAL_RETENTION_OPT_IN_REQUIRED")
        self.manager, self.ingress = manager, ingress
        self.journal = manager._saga.journal
        self.authority = SecurityAuthority(self.journal)
        self.source_id = identity(source_id)  # Trusted adapter label, NOT verified speaker identity.
        self._templates, self._pending = {}, {}
        self.max_pending = max_pending
        self.retain_reviews = retain_reviews
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
        return self._review(proposal, workflow_id, arguments,
                            lambda db, now: self.authority._authenticate(db, token, now), ttl_ms)

    def _review(self, proposal, workflow_id, arguments, authenticate, ttl_ms, session_scope=None, supersedes=None, task_id=None, replaces_task=None):
        self._current(proposal)
        if type(ttl_ms) is not int or not 1 <= ttl_ms <= 300000:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_LIFETIME_INVALID")
        identity(workflow_id)
        template = self._templates.get(workflow_id)
        if template is None:
            raise ProposalAdmissionError("PROPOSAL_WORKFLOW_NOT_ALLOWLISTED")
        arguments = bounded_arguments(arguments)
        task_id, review_id = task_id or "task-" + uuid.uuid4().hex, uuid.uuid4().hex
        cp = self.manager.prepare_checkpoint(task_id, template.name, list(template.steps), workflow=template.workflow)
        permissions = required_permissions(cp["workflow_contract"])
        with self.journal._transaction() as db:
            now = self.authority._now(db)
            actor = authenticate(db, now)
            if not set(permissions) <= actor.permissions:
                raise PermissionDeniedError("Proposal review permissions required")
            if replaces_task is not None:
                from runtime.task_replacement import source
                source(db,actor,replaces_task['task_id'],replaces_task)
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
        if self.retain_reviews:
            binding.update(schema=2, supersedes=supersedes)
        if replaces_task is not None:
            binding.update(schema=3,replaces_task=replaces_task)
        binding["digest"] = digest(binding)
        cp["proposal_binding"] = binding
        validate_binding(binding, cp)
        if self.retain_reviews:
            from runtime.proposal_delivery import publish, DeliveryLimitError
            try:
                with self.journal._transaction() as db:
                    published_ms = self.authority._now(db)
                    current_actor = authenticate(db, published_ms)
                    if (current_actor.principal_id, current_actor.version, current_actor.credential_id) != (actor.principal_id, actor.version, actor.credential_id):
                        raise PermissionDeniedError("Proposal delivery authority changed")
                    if replaces_task is not None:
                        from runtime.task_replacement import source
                        source(db,current_actor,replaces_task['task_id'],replaces_task)
                    prior = publish(db, cp, proposal, published_ms, current_actor)
                    if prior is not None:
                        self.authority._event(db, 'PROPOSAL_REAUTHORIZED', published_ms,
                            task_id=task_id, principal_id=current_actor.principal_id,
                            principal_version=current_actor.version, credential_id=current_actor.credential_id,
                            previous_reviewer=prior['reviewer'],
                            previous_review_id=prior['review_id'], previous_binding_digest=prior['digest'],
                            review_id=review_id, binding_digest=binding['digest'])
            except DeliveryLimitError:
                raise ProposalAdmissionError("PROPOSAL_DELIVERY_UNAVAILABLE") from None
        self._pending[review_id] = _Pending(canonical(cp), proposal,
                                          binding_refs(list(template.steps), self.manager._saga.registry),
                                          workflow_id, binding["expires_ms"], session_scope)
        return ProposalReview(review_id, task_id, binding["digest"], canonical(binding))

    def admit(self, review_id, *, token):
        """Explicit accept; retries reuse identity and never execute a mutation.

        On ambiguous storage failure inspect task_id. Never manufacture a new
        review/task as an automatic retry. Credentials are rechecked in SQLite.
        """
        return self._admit(review_id, lambda cp: self.journal.admit_task(cp, token))

    def _admit(self, review_id, commit, session_scope=None, expected_digest=None):
        self.ingress._owned()
        identity(review_id)
        pending = self._pending.get(review_id)
        if pending is None or pending.session_scope != session_scope:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_UNAVAILABLE")
        self._current(pending.proposal)
        cp = json.loads(pending.checkpoint)
        if expected_digest is not None and expected_digest != cp["proposal_binding"]["digest"]:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_DIGEST_CHANGED")
        template = self._templates[pending.workflow_id]
        current = self.manager._capture_workflow(template.workflow, list(template.steps))
        refs = binding_refs(list(template.steps), self.manager._saga.registry)
        if (current != canonical(cp["workflow_contract"]) or len(refs) != len(pending.refs) or
                any(a is not b for a, b in zip(refs, pending.refs))):
            raise ProposalAdmissionError("PROPOSAL_DEPLOYMENT_CHANGED")
        commit(cp)
        return AdmissionReceipt(cp["task_id"], cp["proposal_binding"]["digest"])

    def _session_authenticator(self, session, csrf):
        # No bearer token is recovered, retained or minted for the browser.
        from runtime.control_store import ControlStore
        if type(csrf) is not str or not csrf:
            raise PermissionDeniedError("Proposal session CSRF required")
        store = ControlStore(self.journal)
        def authenticate(db, now):
            actor, _, identity = store._session(db, session, csrf)
            return replace(actor, expires_ms=min(actor.expires_ms, identity["expires_ms"]))
        return authenticate

    def review_session(self, proposal, workflow_id, arguments, *, session, csrf, ttl_ms=60000):
        from runtime.control_store import digest as session_digest
        authenticate = self._session_authenticator(session, csrf)
        # Reject anonymous sessions before revealing current evidence status.
        with self.journal._transaction() as db:
            authenticate(db, self.authority._now(db))
        return self._review(proposal, workflow_id, arguments, authenticate,
                            ttl_ms, session_digest(session))

    def admit_session(self, review_id, binding_digest, *, session, csrf):
        from runtime.control_store import digest as session_digest
        if type(binding_digest) is not str or len(binding_digest) != 64:
            raise ProposalAdmissionError("PROPOSAL_REVIEW_DIGEST_INVALID")
        authenticate = self._session_authenticator(session, csrf)
        return self._admit(review_id, lambda cp: self.journal._admit_task_authenticated(cp, authenticate),
                           session_digest(session), binding_digest)

    def reauthorize_session(self, review_id, binding_digest, workflow_id, arguments, *, session, csrf, ttl_ms=60000):
        """Fresh evidence/review, SAME unadmitted task. Never restores old authority."""
        from runtime.control_store import digest as session_digest
        from runtime.review_fence import source
        self.ingress._owned()
        if not self.retain_reviews:
            raise ProposalAdmissionError('PROPOSAL_RETENTION_OPT_IN_REQUIRED')
        identity(review_id)
        if type(binding_digest) is not str or len(binding_digest) != 64:
            raise ProposalAdmissionError('PROPOSAL_REVIEW_DIGEST_INVALID')
        authenticate = self._session_authenticator(session, csrf)
        with self.journal._transaction() as db:
            now = self.authority._now(db)
            actor = authenticate(db, now)
            previous = source(db, actor, review_id, binding_digest, now)
        return self._review(self.ingress.latest, workflow_id, arguments, authenticate, ttl_ms,
                           session_digest(session), {'review_id': review_id, 'binding_digest': binding_digest}, previous['task_id'],previous.get('replaces_task'))

    def replacement_session(self, task_id, checkpoint_digest, workflow_id, arguments, *, session, csrf, ttl_ms=60000):
        """Fresh replacement review only; claim/admission happens on explicit accept."""
        from runtime.control_store import digest as session_digest
        from runtime.task_replacement import source,ReplacementDenied
        self.ingress._owned()
        if not self.retain_reviews:
            raise ProposalAdmissionError('PROPOSAL_RETENTION_OPT_IN_REQUIRED')
        authenticate=self._session_authenticator(session,csrf)
        with self.journal._transaction() as db:
            actor=authenticate(db,self.authority._now(db))
            original,_=source(db,actor,task_id)
            if original['digest'] != checkpoint_digest:
                raise ReplacementDenied('EVIDENCE_CHANGED')
        return self._review(self.ingress.latest,workflow_id,arguments,authenticate,ttl_ms,session_digest(session),replaces_task=original)

    def view_session(self, review_id, *, session, csrf):
        """Protected process-local review/readback; unavailable after host restart."""
        from runtime.control_store import digest as session_digest
        self.ingress._owned()
        authenticate = self._session_authenticator(session, csrf)
        identity(review_id)
        pending = self._pending.get(review_id)
        if pending is None or pending.session_scope != session_digest(session):
            raise ProposalAdmissionError("PROPOSAL_REVIEW_UNAVAILABLE")
        with self.journal._transaction() as db:
            now = self.authority._now(db)
            actor = authenticate(db, now)
            binding = json.loads(pending.checkpoint)["proposal_binding"]
            if (not set(binding["required_permissions"]) <= actor.permissions or
                    binding["reviewer"] != {"principal_id": actor.principal_id, "version": actor.version,
                                            "credential_id": actor.credential_id}):
                raise PermissionDeniedError("Proposal review authority changed")
            row = db.execute("SELECT checkpoint FROM task_checkpoints WHERE task_id=?", (binding["task_id"],)).fetchone()
            admitted = row is not None and json.loads(row[0]).get("proposal_binding") == binding
            from runtime.review_fence import matches
            current_review = matches(db, binding)
        return {"binding": binding, "transcript": pending.proposal.text,
                "current": current_review and pending.proposal is self.ingress.latest and pending.proposal.grounded,
                "expired": now >= pending.expires_ms, "admitted": admitted,
                "execution_authorized": False}
