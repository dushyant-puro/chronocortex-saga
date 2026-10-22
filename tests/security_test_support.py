"""Explicit local credentials/approvals for SIMULATED legacy boundary fixtures.

No runtime authorization is bypassed. Automatic repair approval here is a test
operator convenience for fake effects, never a production approval issuer.
"""
from dataclasses import replace
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import SpeculativeSagaManager


GRANTS = {"test:write", "fleet:write", "ccs:repair", "ccs:reconcile"}


def drop_security_tables(db):
    """Build an explicitly simulated older-schema fixture, never a downgrade API."""
    for table in ("security_events", "approvals", "credentials", "principals", "security_clock"):
        db.execute(f"DROP TABLE {table}")


def test_context(journal):
    for name, permissions in (("SIMULATED-worker", GRANTS), ("SIMULATED-approver", GRANTS | {"ccs:approve"})):
        with journal._transaction() as db:
            exists = db.execute("SELECT 1 FROM principals WHERE id=?", (name,)).fetchone()
        if not exists:
            journal.security.provision_principal(name, permissions)
    worker = journal.security.issue_credential("SIMULATED-worker")
    approver = journal.security.issue_credential("SIMULATED-approver")
    return AuthorizationContext(credential=worker.token), approver


class AuthenticatedTestSaga(SpeculativeSagaManager):
    def __init__(self, *args, **kwargs):
        journal = kwargs.get("journal")
        if journal is not None:
            kwargs["auth_context"], self.test_approver = test_context(journal)
        super().__init__(*args, **kwargs)

    def _persist_action(self, action, event, **kwargs):
        if self.journal is not None and event == "COMPENSATION_INTENT":
            approval_id = self.journal.security.issue_approval(
                self.test_approver.token, "SIMULATED-worker", action.action_id, direction="COMPENSATION")
            kwargs["authorization"] = replace(kwargs["authorization"], approval_id=approval_id)
        return super()._persist_action(action, event, **kwargs)
