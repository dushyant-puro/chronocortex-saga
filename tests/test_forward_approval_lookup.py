"""Real console/SQLite authority; SIMULATED provider and explicit race barriers."""
from contextlib import closing
import json
import sqlite3
from types import SimpleNamespace
import uuid

import pytest

from runtime.action_journal import SQLiteActionJournal
from runtime.control_store import ControlStore
from runtime.policy_engine import AuthorizationContext, PermissionDeniedError, ConfirmationRequiredError
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock, ActionState
from runtime.tool_contract import ToolRegistry
from test_stripe_sandbox import adapter, INTENT, PERMISSION
from test_security_authority import Clock


def environment(tmp_path, provider):
    clock = Clock()
    journal = SQLiteActionJournal(tmp_path / "approval.db", clock_ms=clock)
    journal.acquire_lease("SIMULATED-worker", ttl_ms=120_000)
    authority = journal.security
    grants = {"ccs:work", "ccs:reconcile", PERMISSION}
    authority.provision_principal("worker", grants)
    authority.provision_principal("other-worker", grants)
    authority.provision_principal("operator", {"ccs:observe", "ccs:approve", PERMISSION})
    worker = authority.issue_credential("worker")
    other = authority.issue_credential("other-worker")
    issuer = authority.issue_credential("operator")
    registry = ToolRegistry()
    registry.register(provider.manifest())
    context = AuthorizationContext(credential=worker.token)
    saga = SpeculativeSagaManager(TurnEpochClock(), registry=registry, journal=journal, auth_context=context)
    action = provider.stage(saga, INTENT)
    store = ControlStore(journal)
    session, identity = store.login(issuer.token)
    return SimpleNamespace(journal=journal, authority=authority, saga=saga, action=action, context=context,
                           worker=worker, other=other, issuer=issuer, clock=clock, store=store,
                           session=session, csrf=identity["csrf_token"])


def approve(env, *, executor="worker", ttl_ms=300_000):
    review = env.store.detail(env.session, "actions", env.action.action_id)
    result = env.store.command(env.session, env.csrf, "APPROVE", env.action.action_id,
                               {"operation_id": str(uuid.uuid4()), "review_digest": review["review_digest"],
                                "direction": "FORWARD", "executor": executor, "ttl_ms": ttl_ms})
    return result["approval_id"]


def lookup(env):
    return env.authority.find_forward_approval(env.worker.token, env.action.action_id)


@pytest.mark.asyncio
async def test_lookup_requires_console_decision_and_does_not_consume_or_add_audit_events(tmp_path):
    async with adapter(lambda _: pytest.fail("approval lookup performed HTTP")) as provider:
        env = environment(tmp_path, provider)
        assert lookup(env) is None
        env.authority.issue_approval(env.issuer.token, "worker", env.action.action_id)
        assert lookup(env) is None  # Offline approval APIs remain explicit, not automatically discovered.
        selected = approve(env)
        with closing(sqlite3.connect(env.journal.path)) as db:
            before = db.execute("SELECT count(*) FROM security_events").fetchone()[0]
        assert lookup(env) == selected and lookup(env) == selected
        with closing(sqlite3.connect(env.journal.path)) as db:
            assert db.execute("SELECT consumed FROM approvals WHERE id=?", (selected,)).fetchone()[0] == 0
            assert db.execute("SELECT count(*) FROM security_events").fetchone()[0] == before
        assert env.action.state == ActionState.PENDING


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["ccs:work", PERMISSION])
async def test_lookup_requires_current_worker_and_operation_grants(tmp_path, missing):
    async with adapter(lambda _: pytest.fail("unauthorized lookup performed HTTP")) as provider:
        env = environment(tmp_path, provider)
        approve(env)
        env.authority.update_principal("worker", permissions={"ccs:work", "ccs:reconcile", PERMISSION} - {missing})
        with pytest.raises(PermissionDeniedError):
            lookup(env)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["revoked", "expired", "consumed", "issuer-revoked", "issuer-changed",
                                    "executor-version", "executor-mismatch", "binding"])
async def test_ineligible_approval_never_becomes_a_candidate(tmp_path, invalid):
    async with adapter(lambda _: pytest.fail("invalid approval performed HTTP")) as provider:
        env = environment(tmp_path, provider)
        selected = approve(env, executor="other-worker" if invalid == "executor-mismatch" else "worker")
        if invalid == "revoked":
            env.authority.revoke_approval(env.issuer.token, selected)
        elif invalid == "expired":
            env.clock.now += 300_000
        elif invalid == "issuer-revoked":
            env.authority.revoke_credential(env.issuer.id)
        elif invalid == "issuer-changed":
            env.authority.update_principal("operator", permissions={"ccs:observe", PERMISSION})
        elif invalid == "executor-version":
            env.authority.update_principal("worker", permissions={"ccs:work", "ccs:reconcile", PERMISSION})
        elif invalid in {"binding", "consumed"}:
            with env.journal._transaction() as db:
                if invalid == "consumed":
                    db.execute("UPDATE approvals SET consumed=1 WHERE id=?", (selected,))
                else:
                    record = json.loads(db.execute("SELECT record FROM approvals WHERE id=?", (selected,)).fetchone()[0])
                    record["binding"]["capture_epoch"] += 1
                    db.execute("UPDATE approvals SET record=? WHERE id=?", (json.dumps(record), selected))
        assert lookup(env) is None


@pytest.mark.asyncio
async def test_latest_same_clock_decision_never_falls_back_to_older_authority(tmp_path):
    async with adapter(lambda _: pytest.fail("approval selection performed HTTP")) as provider:
        env = environment(tmp_path, provider)
        first = approve(env)
        assert lookup(env) == first
        second = approve(env)
        assert lookup(env) == second
        env.authority.revoke_approval(env.issuer.token, second)
        assert lookup(env) is None
        with closing(sqlite3.connect(env.journal.path)) as db:
            assert db.execute("SELECT revoked FROM approvals WHERE id=?", (first,)).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_rewritten_command_rowids_cannot_resurrect_an_older_approval(tmp_path):
    async with adapter(lambda _: pytest.fail("approval ordering performed HTTP")) as provider:
        env = environment(tmp_path, provider)
        first = approve(env)
        latest = approve(env)
        assert lookup(env) == latest
        env.authority.revoke_approval(env.issuer.token, latest)
        with env.journal._transaction() as db:
            # Simulate maintenance copying an older command after a newer row.
            # Durable control-event sequence still defines the console chronology.
            row = next(row for row in db.execute("SELECT * FROM control_commands")
                       if json.loads(row[5])["approval_id"] == first)
            db.execute("DELETE FROM control_commands WHERE principal_id=? AND operation_id=?", row[:2])
            db.execute("INSERT INTO control_commands VALUES (?,?,?,?,?,?,?)", row)
        assert lookup(env) is None
        with closing(sqlite3.connect(env.journal.path)) as db:
            db.execute("VACUUM")
        assert lookup(env) is None


@pytest.mark.asyncio
async def test_revocation_between_selection_and_dispatch_is_rechecked_atomically(tmp_path):
    async with adapter(lambda _: pytest.fail("revoked approval reached provider")) as provider:
        env = environment(tmp_path, provider)
        selected = approve(env)
        env.context.approval_id = lookup(env)
        env.authority.revoke_approval(env.issuer.token, selected)
        with pytest.raises(ConfirmationRequiredError):
            await env.saga.commit_write(env.action.action_id, provider.execute)
        assert env.action.state == ActionState.PENDING
        assert [e["kind"] for e in env.journal.events(env.action.action_id)] == ["STAGED"]


@pytest.mark.asyncio
async def test_closed_chain_blocks_selection_and_does_not_consume_approval(tmp_path):
    async with adapter(lambda _: pytest.fail("closed chain performed HTTP")) as provider:
        env = environment(tmp_path, provider)
        selected = approve(env)
        env.journal.close_chain(env.action.chain_id)
        with pytest.raises(PermissionDeniedError):
            lookup(env)
        with closing(sqlite3.connect(env.journal.path)) as db:
            assert db.execute("SELECT consumed FROM approvals WHERE id=?", (selected,)).fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["missing-action", "'; DROP TABLE approvals; --", None])
async def test_unknown_or_invalid_action_never_exposes_an_approval(tmp_path, value):
    async with adapter(lambda _: pytest.fail("invalid action performed HTTP")) as provider:
        env = environment(tmp_path, provider)
        approve(env)
        with pytest.raises(PermissionDeniedError):
            env.authority.find_forward_approval(env.worker.token, value)
        assert lookup(env) is not None


@pytest.mark.asyncio
async def test_malformed_latest_decision_fails_closed(tmp_path):
    async with adapter(lambda _: pytest.fail("malformed decision performed HTTP")) as provider:
        env = environment(tmp_path, provider)
        approve(env)
        with env.journal._transaction() as db:
            db.execute("UPDATE control_commands SET result=?", ('{"state":"RECORDED"}',))
        with pytest.raises(PermissionDeniedError):
            lookup(env)
