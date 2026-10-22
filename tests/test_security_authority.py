"""Local credential/admin boundary; no network login or real identity proofing.

These tests deliberately provision through the trusted offline authority API.
Saga dispatch, durable approval consumption and races are tested separately.
"""
from contextlib import closing
from dataclasses import asdict, replace
import json
import sqlite3

import pytest

from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import PermissionDeniedError
from runtime.security_authority import SecurityAuthority


class Clock:
    def __init__(self):
        self.now = 1000

    def __call__(self):
        return self.now


def authority_at(tmp_path):
    clock = Clock()
    journal = SQLiteActionJournal(tmp_path / "security.db", clock_ms=clock)
    authority = SecurityAuthority(journal)
    authority.provision_principal("worker", {"test:write", "ccs:repair"})
    authority.provision_principal("approver", {"ccs:approve"})
    return authority, journal, clock


def test_authentication_returns_current_server_grants_and_no_self_asserted_permissions(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=100)
    original = authority.authenticate(credential.token)
    assert original.principal_id == "worker"
    assert original.granted_permissions == frozenset({"test:write", "ccs:repair"})
    assert original.version == 1
    assert original.credential_id == credential.id
    assert original.expires_ms == clock.now + 100

    # A frozen presentation snapshot is not an authenticator. A caller can
    # construct a new object, but cannot make its own permissions authoritative.
    forged = replace(original, granted_permissions=frozenset({"admin"}))
    with pytest.raises(PermissionDeniedError):
        authority.authenticate(forged)
    assert authority.authenticate(credential.token).granted_permissions == original.granted_permissions


def test_permission_update_refreshes_grants_without_trusting_a_stale_snapshot(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=100)
    old = authority.authenticate(credential.token)
    authority.update_principal("worker", permissions={"ccs:repair"})
    current = authority.authenticate(credential.token)
    assert current.version == old.version + 1
    assert current.granted_permissions == frozenset({"ccs:repair"})
    assert "test:write" in old.granted_permissions
    assert current.credential_id == old.credential_id
    assert current.expires_ms == old.expires_ms


def test_disabled_principal_denies_all_existing_credentials(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    first = authority.issue_credential("worker", ttl_ms=100)
    second = authority.issue_credential("worker", ttl_ms=100)
    authority.update_principal("worker", active=False)
    for credential in (first, second):
        with pytest.raises(PermissionDeniedError):
            authority.authenticate(credential.token)
    authority.update_principal("worker", active=True)
    for credential in (first, second):
        with pytest.raises(PermissionDeniedError):
            authority.authenticate(credential.token)
    replacement = authority.issue_credential("worker", ttl_ms=100)
    current = authority.authenticate(replacement.token)
    assert current.principal_id == "worker" and current.version == 3


def test_credential_revocation_does_not_revoke_another_credential(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    first = authority.issue_credential("worker", ttl_ms=100)
    second = authority.issue_credential("worker", ttl_ms=100)
    assert first.id != second.id and first.token != second.token
    authority.revoke_credential(first.id)
    with pytest.raises(PermissionDeniedError):
        authority.authenticate(first.token)
    assert authority.authenticate(second.token).principal_id == "worker"


def test_credential_expiry_uses_server_clock_and_rejects_exact_deadline(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=10)
    clock.now += 9
    assert authority.authenticate(credential.token).principal_id == "worker"
    clock.now += 1
    with pytest.raises(PermissionDeniedError):
        authority.authenticate(credential.token)


@pytest.mark.parametrize("ttl", [True, False, 0, -1, 1.5, "100", None, 86_400_001])
def test_credential_ttl_is_strict_and_bounded(tmp_path, ttl):
    authority, journal, clock = authority_at(tmp_path)
    with pytest.raises(PermissionDeniedError):
        authority.issue_credential("worker", ttl_ms=ttl)


@pytest.mark.parametrize("ttl", [1, 86_400_000])
def test_valid_credential_ttl_boundary_is_recorded_server_side(tmp_path, ttl):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=ttl)
    assert authority.authenticate(credential.token).expires_ms == clock.now + ttl


def test_duplicate_provisioning_cannot_reset_principal_security_version(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=100)
    authority.update_principal("worker", permissions={"test:write"})
    before = authority.authenticate(credential.token)
    with pytest.raises(PermissionDeniedError):
        authority.provision_principal("worker", {"admin"})
    assert authority.authenticate(credential.token) == before


@pytest.mark.parametrize("active", ["true", "false", 1, 0, [], {}])
def test_principal_active_flag_is_exact_bool(tmp_path, active):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=100)
    before = authority.authenticate(credential.token)
    with pytest.raises(PermissionDeniedError):
        authority.update_principal("worker", active=active)
    assert authority.authenticate(credential.token) == before


@pytest.mark.parametrize("token", [None, "", b"token", True, 1, "worker", "malformed.token.parts", "x" * 10000])
def test_bad_tokens_fail_generically_without_echoing_supplied_secret(tmp_path, token):
    authority, journal, clock = authority_at(tmp_path)
    with pytest.raises(PermissionDeniedError) as captured:
        authority.authenticate(token)
    if type(token) is str and len(token) > 16:
        assert token not in str(captured.value)


def test_existing_wrong_secret_unknown_credential_expiry_and_revocation_do_not_enumerate(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=10)
    wrong_secret = credential.token[:-1] + ("A" if credential.token[-1] != "A" else "B")
    messages = []
    for token in (wrong_secret, "unknown.credential"):
        with pytest.raises(PermissionDeniedError) as captured:
            authority.authenticate(token)
        messages.append(str(captured.value))
    clock.now += 10
    with pytest.raises(PermissionDeniedError) as expired:
        authority.authenticate(credential.token)
    messages.append(str(expired.value))
    assert len(set(messages)) == 1
    assert credential.token not in messages[0]


def test_credential_secret_never_appears_in_representations_or_sqlite_records(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=100)
    principal = authority.authenticate(credential.token)
    serialized_principal = json.dumps(asdict(principal), default=str)
    assert credential.token not in repr(credential)
    assert credential.token not in repr(principal)
    assert credential.token not in serialized_principal
    with closing(sqlite3.connect(journal.path)) as db:
        serialized_store = "\n".join(db.iterdump())
    assert credential.token not in serialized_store
    # Tokens may include a public lookup ID. Protect the verifier portion too,
    # so storing only the public prefix cannot make this assertion pass.
    secret = credential.token.rsplit(".", 1)[-1]
    assert secret not in serialized_store


def test_clock_rollback_denies_authentication_without_relying_on_worker_lease(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=100)
    clock.now += 10
    authority.authenticate(credential.token)
    clock.now -= 1
    with pytest.raises(PermissionDeniedError):
        authority.authenticate(credential.token)
    clock.now += 1
    assert authority.authenticate(credential.token).principal_id == "worker"


def test_observed_expiry_cannot_be_resurrected_by_clock_rollback_or_restart(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=10)
    clock.now += 10
    with pytest.raises(PermissionDeniedError):
        authority.authenticate(credential.token)
    clock.now -= 1
    with pytest.raises(PermissionDeniedError):
        authority.authenticate(credential.token)
    replacement = SecurityAuthority(SQLiteActionJournal(journal.path, clock_ms=clock))
    with pytest.raises(PermissionDeniedError):
        replacement.authenticate(credential.token)


def test_independent_connection_sees_revocation_and_current_grants(tmp_path):
    authority, journal, clock = authority_at(tmp_path)
    credential = authority.issue_credential("worker", ttl_ms=100)
    replacement = SecurityAuthority(SQLiteActionJournal(journal.path, clock_ms=clock))
    assert replacement.authenticate(credential.token).version == 1
    authority.update_principal("worker", permissions={"test:write"})
    assert replacement.authenticate(credential.token).granted_permissions == frozenset({"test:write"})
    authority.revoke_credential(credential.id)
    with pytest.raises(PermissionDeniedError):
        replacement.authenticate(credential.token)
