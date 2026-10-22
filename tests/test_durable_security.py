"""Real journal authorization, separate from simulated legacy policy fixtures."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from runtime.action_journal import SQLiteActionJournal, JournalError, JournalConflict
from runtime.policy_engine import AuthorizationContext, PermissionDeniedError, ConfirmationRequiredError, MutationApproval, PolicyError
from runtime.security_authority import AuthRequest
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock, ActionState, CompensationState, MutationNotAppliedError
from runtime.tool_contract import ToolManifest, ToolRegistry


@pytest.mark.asyncio
async def test_self_asserted_grants_cannot_authenticate_durable_mutation(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    journal.acquire_lease("worker")
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write", required_permissions=["admin"]))
    manager = SpeculativeSagaManager(TurnEpochClock(), journal=journal, registry=registry,
                                     auth_context=AuthorizationContext(["admin"]))
    action = manager.stage_write("change", {})
    calls = []

    async def execute(args):
        calls.append("unauthenticated mutation")

    with pytest.raises(PermissionDeniedError):
        await manager.commit_write(action.action_id, execute)
    assert calls == []


class Clock:
    def __init__(self):
        self.now = 1000

    def __call__(self):
        return self.now


def runtime_at(tmp_path, *, confirmation=True, validator=None):
    clock = Clock()
    journal = SQLiteActionJournal(tmp_path / "security.db", clock_ms=clock)
    lease = journal.acquire_lease("worker", ttl_ms=1_000_000)
    permissions = {"test:write", "ccs:repair", "ccs:reconcile"}
    journal.security.provision_principal("executor", permissions)
    journal.security.provision_principal("approver", permissions | {"ccs:approve"})
    worker = journal.security.issue_credential("executor", ttl_ms=100_000)
    approver = journal.security.issue_credential("approver", ttl_ms=100_000)
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write", required_permissions=["test:write"],
                                   requires_confirmation=confirmation, validate_args=validator))
    context = AuthorizationContext(credential=worker.token)
    manager = SpeculativeSagaManager(TurnEpochClock(), journal=journal, registry=registry, auth_context=context)
    return SimpleNamespace(clock=clock, journal=journal, lease=lease, worker=worker, approver=approver,
                           registry=registry, context=context, manager=manager)


def approve(env, action, *, direction="FORWARD", ttl_ms=1000):
    approval_id = env.journal.security.issue_approval(env.approver.token, "executor", action.action_id,
                                                     direction=direction, ttl_ms=ttl_ms)
    if direction == "FORWARD":
        env.context.approval_id = approval_id
    else:
        env.context.repair_approval_id = approval_id
    return approval_id


def consumed(env, approval_id):
    with closing(sqlite3.connect(env.journal.path)) as db:
        return db.execute("SELECT consumed FROM approvals WHERE id=?", (approval_id,)).fetchone()[0]


@pytest.mark.asyncio
async def test_server_grants_and_atomic_approval_are_visible_before_executor(tmp_path):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {"value": "approved"})
    approval_id = approve(env, action)
    # No caller-supplied grants; authority comes only from the provisioned server row.
    assert env.context.granted_permissions == set()

    async def execute(args):
        assert consumed(env, approval_id) == 1
        assert env.journal.events(action.action_id)[-1]["kind"] == "DISPATCH_INTENT"
        with closing(sqlite3.connect(env.journal.path)) as db:
            records = [json.loads(row[0]) for row in db.execute("SELECT record FROM security_events WHERE kind='INTENT_AUTHORIZED'")]
        assert records[-1]["principal_id"] == "executor"
        assert records[-1]["approval_id"] == approval_id
        return {"receipt": "SIMULATED:applied"}

    await env.manager.commit_write(action.action_id, execute)
    assert action.state == ActionState.COMMITTED


@pytest.mark.asyncio
async def test_in_process_mutation_approval_is_insufficient_for_durable_dispatch(tmp_path):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {})
    env.context.confirmed = True
    env.context.approval = MutationApproval.for_action(action)
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.commit_write(action.action_id, lambda _: pytest.fail("legacy approval dispatched"))
    assert action.state == ActionState.PENDING


@pytest.mark.parametrize("revocation", ["credential", "approval", "principal", "issuer_credential", "issuer_version", "executor_version"])
@pytest.mark.asyncio
async def test_revocation_or_version_change_before_dispatch_denies_without_consumption(tmp_path, revocation):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {})
    approval_id = approve(env, action)
    if revocation == "credential":
        env.journal.security.revoke_credential(env.worker.id)
    elif revocation == "approval":
        env.journal.security.revoke_approval(env.approver.token, approval_id)
    elif revocation == "principal":
        env.journal.security.update_principal("executor", active=False)
    elif revocation == "issuer_credential":
        env.journal.security.revoke_credential(env.approver.id)
    else:
        env.journal.security.update_principal("approver" if revocation == "issuer_version" else "executor",
                                               permissions={"test:write", "ccs:approve", "ccs:repair", "ccs:reconcile"})
    with pytest.raises(PolicyError):
        await env.manager.commit_write(action.action_id, lambda _: pytest.fail("revoked dispatch"))
    assert action.state == ActionState.PENDING
    assert env.journal.records()[0][1]["state"] == "PENDING"
    assert consumed(env, approval_id) == 0


@pytest.mark.parametrize("revocation", ["credential", "approval", "grants"])
@pytest.mark.asyncio
async def test_final_transaction_rechecks_authority_changed_by_preflight_validator(tmp_path, revocation):
    env_ref = []

    def validate(args):
        env = env_ref[0]
        if revocation == "credential":
            env.journal.security.revoke_credential(env.worker.id)
        elif revocation == "approval":
            env.journal.security.revoke_approval(env.approver.token, env.context.approval_id)
        else:
            env.journal.security.update_principal("executor", permissions=set())

    env = runtime_at(tmp_path, validator=validate)
    env_ref.append(env)
    action = env.manager.stage_write("change", {})
    approval_id = approve(env, action)
    with pytest.raises(PolicyError):
        await env.manager.commit_write(action.action_id, lambda _: pytest.fail("preflight race dispatched"))
    assert action.state == ActionState.PENDING and consumed(env, approval_id) == 0


@pytest.mark.parametrize("outcome", ["success", "unknown", "not_applied"])
@pytest.mark.asyncio
async def test_consumed_approval_never_authorizes_another_attempt(tmp_path, outcome):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {"attempt": 1})
    approval_id = approve(env, action)

    async def execute(args):
        if outcome == "unknown":
            raise ConnectionError("SIMULATED response lost")
        if outcome == "not_applied":
            raise MutationNotAppliedError("SIMULATED rejected", evidence="SIMULATED:atomic-rejection")
        return {"receipt": "SIMULATED:applied"}

    if outcome == "success":
        await env.manager.commit_write(action.action_id, execute)
    else:
        with pytest.raises((ConnectionError, MutationNotAppliedError)):
            await env.manager.commit_write(action.action_id, execute)
    assert consumed(env, approval_id) == 1
    another = env.manager.stage_write("change", {"attempt": 2})
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.commit_write(another.action_id, lambda _: pytest.fail("approval replayed"))
    assert another.state == ActionState.PENDING


@pytest.mark.parametrize("revocation", ["credential", "approval", "issuer", "expiry"])
@pytest.mark.asyncio
async def test_post_intent_authority_loss_does_not_discard_returned_effect(tmp_path, revocation):
    env = runtime_at(tmp_path)
    undos = []

    async def undo(result):
        undos.append("forbidden undo")

    action = env.manager.stage_write("change", {}, compensate=undo)
    approval_id = approve(env, action)

    async def execute(args):
        if revocation == "credential":
            env.journal.security.revoke_credential(env.worker.id)
        elif revocation == "approval":
            env.journal.security.revoke_approval(env.approver.token, approval_id)
        elif revocation == "issuer":
            env.journal.security.revoke_credential(env.approver.id)
        else:
            env.clock.now += 100_000
        return {"receipt": "SIMULATED:already-applied"}

    await env.manager.commit_write(action.action_id, execute)
    assert action.state == ActionState.COMMITTED and action.result == {"receipt": "SIMULATED:already-applied"}
    assert env.journal.records()[0][1]["state"] == "COMMITTED"
    assert consumed(env, approval_id) == 1 and undos == []
    with pytest.raises(PolicyError):
        await env.manager.compensate_action(action.action_id)
    assert undos == [] and action.compensation_state == CompensationState.NOT_STARTED


@pytest.mark.asyncio
async def test_compensation_requires_separate_direction_bound_approval(tmp_path):
    env = runtime_at(tmp_path)
    calls = []

    async def undo(result):
        calls.append("SIMULATED undo")

    action = env.manager.stage_write("change", {}, compensate=undo)
    forward = approve(env, action)
    await env.manager.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"receipt": "SIMULATED:one"}))
    env.context.repair_approval_id = forward
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.compensate_action(action.action_id)
    assert action.compensation_state == CompensationState.NOT_STARTED and calls == []
    repair = approve(env, action, direction="COMPENSATION")
    await env.manager.compensate_action(action.action_id)
    await env.manager.compensate_action(action.action_id)
    assert calls == ["SIMULATED undo"]
    assert action.state == ActionState.COMPENSATED and consumed(env, repair) == 1


@pytest.mark.asyncio
async def test_compensation_approval_is_bound_to_original_receipt(tmp_path):
    env = runtime_at(tmp_path, confirmation=False)
    action = env.manager.stage_write("change", {}, compensate=lambda _: pytest.fail("changed receipt repaired"))
    await env.manager.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"receipt": "SIMULATED:one"}))
    repair = approve(env, action, direction="COMPENSATION")
    action.result = {"receipt": "SIMULATED:other"}
    env.manager._persist_action(action, "SIMULATED_RECEIPT_CHANGE")
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.compensate_action(action.action_id)
    assert consumed(env, repair) == 0


@pytest.mark.asyncio
async def test_in_memory_receipt_swap_cannot_retarget_an_approved_compensation(tmp_path):
    env = runtime_at(tmp_path, confirmation=False)
    action = env.manager.stage_write("change", {}, compensate=lambda _: pytest.fail("wrong resource undone"))
    await env.manager.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"id": "SIMULATED:approved-resource"}))
    repair = approve(env, action, direction="COMPENSATION")
    action.result = {"id": "SIMULATED:foreign-resource"}
    with pytest.raises(PermissionDeniedError, match="receipt"):
        await env.manager.compensate_action(action.action_id)
    assert consumed(env, repair) == 0 and action.compensation_state == CompensationState.NOT_STARTED


@pytest.mark.asyncio
async def test_compensation_handler_receives_detached_approved_receipt(tmp_path):
    env = runtime_at(tmp_path, confirmation=False)
    started, release = asyncio.Event(), asyncio.Event()
    received = []

    async def undo(receipt):
        started.set()
        await release.wait()
        received.append(receipt["id"])

    action = env.manager.stage_write("change", {}, compensate=undo)
    await env.manager.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"id": "SIMULATED:approved-resource"}))
    approve(env, action, direction="COMPENSATION")
    repairing = asyncio.create_task(env.manager.compensate_action(action.action_id))
    await asyncio.wait_for(started.wait(), 1)
    action.result["id"] = "SIMULATED:foreign-resource"
    release.set()
    await repairing
    assert received == ["SIMULATED:approved-resource"]
    assert action.result == {"id": "SIMULATED:approved-resource"}
    assert env.journal.records()[0][1]["result"] == action.result


@pytest.mark.parametrize("permissions", [{"ccs:approve"}, {"test:write"}])
def test_approval_issuer_and_revoker_need_operation_specific_grants(tmp_path, permissions):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {})
    approval_id = approve(env, action)
    env.journal.security.provision_principal("unprivileged", permissions)
    credential = env.journal.security.issue_credential("unprivileged")
    with pytest.raises(PermissionDeniedError):
        env.journal.security.issue_approval(credential.token, "executor", action.action_id)
    with pytest.raises(PermissionDeniedError):
        env.journal.security.revoke_approval(credential.token, approval_id)
    assert consumed(env, approval_id) == 0


@pytest.mark.asyncio
async def test_different_authenticated_principal_cannot_redeem_executor_approval(tmp_path):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {})
    approval_id = approve(env, action)
    env.journal.security.provision_principal("other", {"test:write"})
    other = env.journal.security.issue_credential("other")
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.commit_write(action.action_id, lambda _: pytest.fail("another principal redeemed approval"),
                                        auth_context=AuthorizationContext(credential=other.token, approval_id=approval_id))
    assert consumed(env, approval_id) == 0


@pytest.mark.asyncio
async def test_approval_and_dispatch_intent_rollback_atomically_on_event_failure(tmp_path):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {})
    approval_id = approve(env, action)
    with closing(sqlite3.connect(env.journal.path)) as db:
        db.execute("CREATE TRIGGER reject_intent BEFORE INSERT ON events WHEN NEW.kind='DISPATCH_INTENT' BEGIN SELECT RAISE(ABORT,'injected'); END")
        db.commit()
    with pytest.raises(JournalError):
        await env.manager.commit_write(action.action_id, lambda _: pytest.fail("failed intent dispatched"))
    assert consumed(env, approval_id) == 0
    assert env.journal.records()[0][1]["state"] == "PENDING"
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM security_events WHERE kind IN ('APPROVAL_CONSUMED','INTENT_AUTHORIZED')").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_approval_expiry_cannot_resurrect_after_clock_rollback(tmp_path):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {})
    approval_id = approve(env, action, ttl_ms=10)
    env.clock.now += 10
    with pytest.raises(ConfirmationRequiredError):
        await env.manager.commit_write(action.action_id, lambda _: pytest.fail("expired approval"))
    env.clock.now -= 1
    with pytest.raises(PolicyError):
        await env.manager.commit_write(action.action_id, lambda _: pytest.fail("clock rollback approval"))
    assert consumed(env, approval_id) == 0


@pytest.mark.asyncio
async def test_reconciliation_requires_authentication_and_repair_rechecks_after_query(tmp_path):
    env = runtime_at(tmp_path, confirmation=False)
    calls = []

    async def undo(result):
        calls.append("forbidden repair")

    async def unknown(args):
        raise ConnectionError("SIMULATED reply lost")

    action = env.manager.stage_write("change", {}, compensate=undo)
    with pytest.raises(ConnectionError):
        await env.manager.commit_write(action.action_id, unknown)
    with pytest.raises(PermissionDeniedError):
        await env.manager.reconcile_write(action.action_id, lambda _: pytest.fail("unauthenticated query"),
                                          auth_context=AuthorizationContext(["ccs:reconcile"]))
    await env.manager.epoch_clock.advance()

    async def status(action):
        calls.append("SIMULATED status query")
        env.journal.security.revoke_credential(env.worker.id)
        return True

    with pytest.raises(PermissionDeniedError):
        await env.manager.reconcile_write(action.action_id, status)
    assert calls == ["SIMULATED status query"]
    assert env.journal.records()[0][1]["state"] == "COMMITTED_STALE"
    assert action.compensation_state == CompensationState.NOT_STARTED


def test_two_connections_cannot_consume_one_approval_twice(tmp_path):
    env = runtime_at(tmp_path)
    action = env.manager.stage_write("change", {})
    approval_id = approve(env, action)
    second = SQLiteActionJournal(env.journal.path, clock_ms=env.clock)
    second.bind_lease(env.lease)
    version, record = env.journal.records()[0]
    record = {**record, "state": "IN_FLIGHT"}
    request = AuthRequest(env.worker.token, approval_id, ("test:write",), True, "FORWARD")
    barrier = threading.Barrier(2, timeout=5)

    def dispatch(journal):
        barrier.wait()
        try:
            journal.transition(record, version, "DISPATCH_INTENT", authorization=request)
            return True
        except JournalConflict:
            return False

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(dispatch, [env.journal, second]))
    assert results.count(True) == 1 and consumed(env, approval_id) == 1
    assert [e["kind"] for e in env.journal.events(action.action_id)] == ["STAGED", "DISPATCH_INTENT"]


@pytest.mark.parametrize("kind", ["DISPATCH_INTENT", "UNNAMED_ATTEMPT"])
def test_raw_transition_cannot_bypass_authenticated_intent_by_renaming_event(tmp_path, kind):
    env = runtime_at(tmp_path, confirmation=False)
    action = env.manager.stage_write("change", {})
    version, record = env.journal.records()[0]
    with pytest.raises(PermissionDeniedError):
        env.journal.transition({**record, "state": "IN_FLIGHT"}, version, kind)
    assert env.journal.records()[0][1]["state"] == "PENDING"


@pytest.mark.asyncio
async def test_revocation_after_repair_intent_preserves_compensation_outcome(tmp_path):
    env = runtime_at(tmp_path, confirmation=False)

    async def undo(receipt):
        env.journal.security.revoke_credential(env.worker.id)

    action = env.manager.stage_write("change", {}, compensate=undo)
    await env.manager.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"id": "SIMULATED:one"}))
    repair = approve(env, action, direction="COMPENSATION")
    await env.manager.compensate_action(action.action_id)
    assert action.state == ActionState.COMPENSATED and consumed(env, repair) == 1
    assert env.journal.records()[0][1]["compensation_state"] == "SUCCEEDED"


@pytest.mark.asyncio
async def test_repair_intent_failure_does_not_consume_approval_or_execute_undo(tmp_path):
    env = runtime_at(tmp_path, confirmation=False)
    action = env.manager.stage_write("change", {}, compensate=lambda _: pytest.fail("unrecorded repair"))
    await env.manager.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"id": "SIMULATED:one"}))
    repair = approve(env, action, direction="COMPENSATION")
    with closing(sqlite3.connect(env.journal.path)) as db:
        db.execute("CREATE TRIGGER reject_repair BEFORE INSERT ON events WHEN NEW.kind='COMPENSATION_INTENT' BEGIN SELECT RAISE(ABORT,'injected'); END")
        db.commit()
    with pytest.raises(JournalError):
        await env.manager.compensate_action(action.action_id)
    assert consumed(env, repair) == 0
    assert env.journal.records()[0][1]["state"] == "COMMITTED"
    assert env.journal.records()[0][1]["compensation_state"] == "NOT_STARTED"


@pytest.mark.parametrize("direction", ["FORWARD", "COMPENSATION"])
@pytest.mark.asyncio
async def test_unknown_outcome_cannot_be_reapproved_for_execution_or_repair(tmp_path, direction):
    env = runtime_at(tmp_path, confirmation=False)
    action = env.manager.stage_write("change", {})

    async def uncertain(args):
        raise ConnectionError("SIMULATED response lost")

    with pytest.raises(ConnectionError):
        await env.manager.commit_write(action.action_id, uncertain)
    with pytest.raises(PermissionDeniedError):
        approve(env, action, direction=direction)
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN


@pytest.mark.asyncio
async def test_schema4_migration_preserves_legacy_evidence_without_granting_authority(tmp_path):
    from tests.security_test_support import drop_security_tables
    env = runtime_at(tmp_path, confirmation=False)
    action = env.manager.stage_write("change", {})
    env.journal.release_lease()
    with closing(sqlite3.connect(env.journal.path)) as db:
        record = json.loads(db.execute("SELECT record FROM actions").fetchone()[0])
        record.pop("authorization_policy")
        db.execute("UPDATE actions SET record=?", (json.dumps(record),))
        drop_security_tables(db)
        db.execute("PRAGMA user_version=4")
        db.commit()
    with pytest.raises(JournalError, match="migration"):
        SQLiteActionJournal(env.journal.path, clock_ms=env.clock)
    migrated = SQLiteActionJournal(env.journal.path, clock_ms=env.clock, migrate=True)
    assert migrated.records() == [(0, record)]
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 6
        assert db.execute("SELECT count(*) FROM principals").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM approvals").fetchone()[0] == 0
        assert db.execute("SELECT action_id FROM key_claims").fetchall() == [(action.action_id,)]
    migrated.security.provision_principal("operator", {"ccs:reconcile", "test:write"})
    credential = migrated.security.issue_credential("operator")
    recovered = SpeculativeSagaManager(TurnEpochClock(), journal=migrated, registry=env.registry,
                                       auth_context=AuthorizationContext(credential=credential.token))
    with pytest.raises(PermissionDeniedError):
        await recovered.reconcile_write(action.action_id, lambda _: pytest.fail("legacy query requires explicit authority"))


@pytest.mark.parametrize("window,effect_count", [("intent", 0), ("applied", 1)])
@pytest.mark.asyncio
async def test_crash_preserves_consumed_approval_and_never_replays_unknown_mutation(tmp_path, window, effect_count):
    path, ledger = tmp_path / "security.db", tmp_path / "effects.jsonl"
    worker = Path(__file__).with_name("security_crash_worker.py")
    result = subprocess.run([sys.executable, str(worker), str(path), str(ledger), window],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 23, result.stderr
    journal = SQLiteActionJournal(path)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT consumed FROM approvals").fetchall() == [(1,)]
    credential = journal.security.issue_credential("executor")
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write", requires_confirmation=True))
    recovered = SpeculativeSagaManager(TurnEpochClock(), journal=journal, registry=registry,
                                       auth_context=AuthorizationContext(credential=credential.token))
    action = next(iter(recovered._actions.values()))
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN
    with pytest.raises(JournalError, match="Recovered"):
        await recovered.commit_write(action.action_id, lambda _: pytest.fail("crashed approval replayed"))
    assert len(ledger.read_text().splitlines() if ledger.exists() else []) == effect_count
