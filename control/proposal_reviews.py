"""Opt-in owning-loop HTTP bridge; proposals/arguments come from a trusted host.

Browser text, confidence scores, workflow definitions and credentials cannot
construct trusted voice evidence. Admission never restores or dispatches work.
"""
from runtime.proposal_admission import ProposalAdmissionService
from runtime.proposal_contract import bounded_arguments
from runtime.proposal_contract import ProposalAdmissionError


class ProposalReviewController:
    def __init__(self, service, workflow_id, arguments):
        if type(service) is not ProposalAdmissionService:
            raise TypeError("A trusted proposal admission service is required")
        from runtime.workflow_contract import canonical
        self.service = service
        self.workflow_id = workflow_id
        self._arguments = canonical(bounded_arguments(arguments))

    def review(self, session, csrf):
        import json
        service = self.service
        review = service.review_session(service.ingress.latest, self.workflow_id, json.loads(self._arguments),
                                        session=session, csrf=csrf)
        return service.view_session(review.review_id, session=session, csrf=csrf)

    def admit(self, session, csrf, review_id, binding_digest):
        receipt = self.service.admit_session(review_id, binding_digest, session=session, csrf=csrf)
        return {"task_id": receipt.task_id, "binding_digest": receipt.binding_digest,
                "state": "ADMITTED", "execution_authorized": False}

    def reauthorize(self, session, csrf, review_id, binding_digest):
        import json
        review = self.service.reauthorize_session(review_id, binding_digest, self.workflow_id,
                    json.loads(self._arguments), session=session, csrf=csrf)
        return self.service.view_session(review.review_id, session=session, csrf=csrf)

    def status(self, session, csrf, review_id, binding_digest):
        view = self.service.view_session(review_id, session=session, csrf=csrf)
        if view['binding']['digest'] != binding_digest:
            raise ProposalAdmissionError('PROPOSAL_REVIEW_DIGEST_CHANGED')
        return view
