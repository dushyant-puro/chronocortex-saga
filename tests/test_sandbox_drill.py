"""Supervised console-to-provider drill; HTTP and all effects are SIMULATED."""
import asyncio
from contextlib import closing
import json
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace
import uuid

import httpx2 as httpx
import pytest

from providers import sandbox_drill as command
from providers.stripe_sandbox import StripeSandboxCancellation, PERMISSION
from providers.reconcile_stripe import run as reconcile
from runtime.action_journal import SQLiteActionJournal, JournalError
from runtime.control_store import ControlStore
from runtime.policy_engine import PermissionDeniedError
from runtime.speculative_saga import TurnEpochClock, DuplicateOperationError
from runtime.worker_supervisor import WorkerSupervisor
from test_stripe_reconciler import secret
from test_stripe_sandbox import ACCOUNT, INTENT, KEY, account, intent, response


def setup(tmp_path, *, permissions=None):
    journal = SQLiteActionJournal(tmp_path / "drill.db")
    grants = {"ccs:work", "ccs:reconcile", PERMISSION} if permissions is None else permissions
    journal.security.provision_principal("worker", grants)
    journal.security.provision_principal("operator", {"ccs:observe", "ccs:approve", PERMISSION})
    worker = journal.security.issue_credential("worker")
    operator = journal.security.issue_credential("operator")
    store = ControlStore(journal)
    token, identity = store.login(operator.token)
    args = SimpleNamespace(journal=str(journal.path), account_id=ACCOUNT, payment_intent_id=INTENT,
                           credential_file=secret(tmp_path, "worker.key", worker.token),
                           stripe_key_file=secret(tmp_path, "stripe.key", KEY), approval_wait_seconds=120,
                           allow_sandbox_cancellation=True)
    return SimpleNamespace(journal=journal, worker=worker, operator=operator, store=store,
                           token=token, csrf=identity["csrf_token"], args=args, events=[], calls=[], applied=False)


def approve(env, action_id):
    review = env.store.detail(env.token, "actions", action_id)
    return env.store.command(env.token, env.csrf, "APPROVE", action_id,
                             {"operation_id": str(uuid.uuid4()), "review_digest": review["review_digest"],
                              "direction": "FORWARD", "executor": "worker", "ttl_ms": 300_000})["approval_id"]


def transport(env, *, lost=False, unresolved=False, readback_open=False):
    def handler(request):
        env.calls.append((request.method, request.url.path))
        if request.url.path == "/v1/account":
            return account()
        if request.method == "POST":
            env.applied = not unresolved
            if lost:
                raise httpx.ReadError("PRIVATE:" + KEY, request=request)
            return response(intent())
        return response(intent() if env.applied and not readback_open else intent(status="requires_payment_method", canceled_at=None))
    return handler


async def execute(env, *, handler=None, on_stage=None, clock=None):
    def emit(event):
        env.events.append(event)
        if event["event"] == "STRIPE_SANDBOX_APPROVAL_REQUIRED":
            if on_stage is None:
                approve(env, event["action_id"])
            else:
                on_stage(event)
    make = lambda config: StripeSandboxCancellation(config, transport=httpx.MockTransport(handler or transport(env)))
    return await command.run(env.args, provider_factory=make, event_sink=emit, clock=clock)


def finished(env):
    return next(event for event in env.events if event["event"] == "STRIPE_SANDBOX_DRILL_FINISHED")


def capture_host(monkeypatch):
    captured = []
    def make(*args, **kwargs):
        host = WorkerSupervisor(*args, **kwargs)
        captured.append(host)
        return host
    monkeypatch.setattr(command, "WorkerSupervisor", make)
    return captured


@pytest.mark.asyncio
async def test_falsey_factory_stays_simulated_and_never_chooses_real_transport(tmp_path, monkeypatch):
    env = setup(tmp_path)
    class Factory:
        def __bool__(self):
            return False
        def __call__(self, config):
            return StripeSandboxCancellation(config, transport=httpx.MockTransport(transport(env)))
    def emit(event):
        env.events.append(event)
        if event["event"] == "STRIPE_SANDBOX_APPROVAL_REQUIRED":
            approve(env, event["action_id"])
    monkeypatch.setattr(command, "StripeSandboxCancellation", lambda _: pytest.fail("Unexpected real transport selection"))
    assert await command.run(env.args, provider_factory=Factory(), event_sink=emit) == 0
    assert finished(env)["source"] == "SIMULATED"


@pytest.mark.asyncio
async def test_one_exact_approved_post_has_durable_intent_redacted_receipt_and_readback(tmp_path):
    env = setup(tmp_path)
    assert await execute(env) == 0
    assert env.calls == [("GET", "/v1/account"), ("GET", "/v1/payment_intents/" + INTENT),
                         ("GET", "/v1/account"), ("POST", "/v1/payment_intents/" + INTENT + "/cancel"),
                         ("GET", "/v1/account"), ("GET", "/v1/payment_intents/" + INTENT)]
    record = env.journal.records()[0][1]
    assert record["state"] == "COMMITTED" and record["result"]["observed_via"] == "POST"
    result = finished(env)
    assert result["source"] == "SIMULATED" and result["readback_verified"] is True
    assert result["dispatch_intent_recorded"] is True and result["automatic_replay"] is False
    plan = next(event for event in env.events if event["event"] == "STRIPE_SANDBOX_APPROVAL_REQUIRED")
    assert plan["preflight_status"] == "requires_payment_method" and plan["remote_state_locked"] is False
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT consumed FROM approvals").fetchall() == [(1,)]
        assert db.execute("SELECT status FROM chains").fetchall() == [("CLOSED",)]
        assert db.execute("SELECT count(*) FROM key_claims").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM task_checkpoints").fetchone()[0] == 0
    facts = json.dumps(env.events) + json.dumps(env.journal.records()) + json.dumps(env.journal.worker_events())
    assert KEY not in facts and "PRIVATE:" not in facts and env.worker.token not in facts
    assert env.journal.worker_events()[-1]["kind"] == "RELEASED"
    assert any(row["kind"] == "PROVIDER_READBACK" for row in env.journal.worker_events())


@pytest.mark.asyncio
async def test_no_approval_expires_without_dispatch_and_records_known_local_abort(tmp_path):
    env = setup(tmp_path)
    clock = SimpleNamespace(now=0)
    def stage(event):
        clock.now = 121
    assert await execute(env, on_stage=stage, clock=lambda: clock.now) == 3
    assert all(method == "GET" for method, _ in env.calls)
    assert finished(env)["state"] == "ABORTED" and finished(env)["dispatch_intent_recorded"] is False
    assert env.journal.events(finished(env)["action_id"])[-1]["kind"] == "ABORTED_BEFORE_DISPATCH"


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["ccs:work", "ccs:reconcile", PERMISSION])
async def test_missing_current_grants_prevent_even_the_initial_provider_read(tmp_path, missing):
    env = setup(tmp_path, permissions={"ccs:work", "ccs:reconcile", PERMISSION} - {missing})
    with pytest.raises(PermissionDeniedError):
        await execute(env)
    assert env.calls == [] and env.journal.records() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["canceled", "requires_capture", "requires_action", "processing", "succeeded"])
async def test_first_drill_rejects_other_provider_states_without_staging(tmp_path, status):
    env = setup(tmp_path)
    def handler(request):
        env.calls.append(request.method)
        return account() if request.url.path == "/v1/account" else response(intent(status=status))
    assert await execute(env, handler=handler) == 3
    assert env.calls == ["GET", "GET"] and env.journal.records() == []


@pytest.mark.asyncio
async def test_lost_ack_reconciles_with_get_and_does_not_repeat_the_post(tmp_path):
    env = setup(tmp_path)
    assert await execute(env, handler=transport(env, lost=True)) == 0
    assert sum(method == "POST" for method, _ in env.calls) == 1
    record = env.journal.records()[0][1]
    assert record["state"] == "COMMITTED" and record["result"]["observed_via"] == "GET"


@pytest.mark.asyncio
async def test_unknown_survives_fresh_host_and_recovery_is_get_only(tmp_path):
    env = setup(tmp_path)
    assert await execute(env, handler=transport(env, lost=True, unresolved=True)) == 2
    action_id = finished(env)["action_id"]
    assert finished(env)["state"] == "IN_FLIGHT_UNKNOWN"
    env.applied = True  # Independent SIMULATED delayed provider application.
    env.args.action_id = action_id
    assert await reconcile(env.args, provider_factory=lambda config: StripeSandboxCancellation(
        config, transport=httpx.MockTransport(transport(env)))) == 0
    assert env.journal.records()[0][1]["state"] == "COMMITTED_STALE"
    assert sum(method == "POST" for method, _ in env.calls) == 1
    assert await execute(env) == 3  # The now-canceled resource is not eligible for a new drill.
    assert sum(method == "POST" for method, _ in env.calls) == 1


@pytest.mark.asyncio
async def test_readback_contradiction_retains_receipt_and_fails_acceptance(tmp_path):
    env = setup(tmp_path)
    assert await execute(env, handler=transport(env, readback_open=True)) == 3
    assert finished(env)["state"] == "COMMITTED" and finished(env)["receipt_present"] is True
    assert finished(env)["readback_verified"] is False
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT status FROM chains").fetchall() == [("CLOSED",)]
    assert sum(method == "POST" for method, _ in env.calls) == 1


@pytest.mark.asyncio
async def test_display_payload_is_detached_from_approved_action(tmp_path):
    env = setup(tmp_path)
    def stage(event):
        event["args"]["payment_intent_id"] = "pi_Altered12345"
        approve(env, event["action_id"])
    assert await execute(env, on_stage=stage) == 0
    assert ("POST", "/v1/payment_intents/" + INTENT + "/cancel") in env.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["approval", "worker", "permission"])
async def test_authority_change_after_lookup_prevents_dispatch(tmp_path, monkeypatch, change):
    env = setup(tmp_path)
    original = type(env.journal.security).find_forward_approval
    def changed(authority, credential, action_id):
        selected = original(authority, credential, action_id)
        if selected:
            if change == "approval":
                env.journal.security.revoke_approval(env.operator.token, selected)
            elif change == "worker":
                env.journal.security.revoke_credential(env.worker.id)
            else:
                env.journal.security.update_principal("worker", permissions={"ccs:work", "ccs:reconcile"})
        return selected
    monkeypatch.setattr(type(env.journal.security), "find_forward_approval", changed)
    assert await execute(env) == (1 if change == "worker" else 3)
    assert all(method == "GET" for method, _ in env.calls)
    assert finished(env)["state"] == "ABORTED"


@pytest.mark.asyncio
async def test_exception_after_staging_cannot_rebind_pending_action_in_a_fresh_host(tmp_path):
    env = setup(tmp_path)
    def interrupted(event):
        raise RuntimeError("SIMULATED host interrupted before approval")
    with pytest.raises(RuntimeError):
        await execute(env, on_stage=interrupted)
    assert env.journal.records()[0][1]["state"] == "PENDING"
    with pytest.raises(DuplicateOperationError):
        await execute(env)
    assert all(method == "GET" for method, _ in env.calls)


@pytest.mark.asyncio
async def test_shutdown_during_approval_io_persists_abort_before_lease_release(tmp_path, monkeypatch):
    env = setup(tmp_path)
    hosts = capture_host(monkeypatch)
    loop = asyncio.get_running_loop()
    entered, draining = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    original_observe = SQLiteActionJournal.observe_worker
    def observe(journal, kind, details):
        result = original_observe(journal, kind, details)
        if kind == "HOST_DRAINING":
            loop.call_soon_threadsafe(draining.set)
        return result
    def blocked(authority, credential, action_id):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return None
    monkeypatch.setattr(SQLiteActionJournal, "observe_worker", observe)
    monkeypatch.setattr(type(env.journal.security), "find_forward_approval", blocked)
    running = asyncio.create_task(execute(env, on_stage=lambda _: None))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        hosts[0].request_stop()
        await asyncio.wait_for(draining.wait(), 3)
    finally:
        release.set()
    assert await asyncio.wait_for(running, 5) == 3
    assert finished(env)["state"] == "ABORTED" and all(method == "GET" for method, _ in env.calls)
    kinds = [event["kind"] for event in env.journal.worker_events()]
    assert kinds.index("ABORTED_BEFORE_DISPATCH") < kinds.index("RELEASED")


@pytest.mark.asyncio
async def test_shutdown_during_post_drains_receipt_without_claiming_rollback(tmp_path, monkeypatch):
    env = setup(tmp_path)
    hosts = capture_host(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    async def handler(request):
        env.calls.append((request.method, request.url.path))
        if request.method == "POST":
            env.applied = True
            entered.set()
            await release.wait()
            return response(intent())
        return account() if request.url.path == "/v1/account" else response(intent(status="requires_payment_method", canceled_at=None))
    running = asyncio.create_task(execute(env, handler=handler))
    await asyncio.wait_for(entered.wait(), 3)
    hosts[0].request_stop()
    release.set()
    assert await asyncio.wait_for(running, 5) == 3  # No new readback after stop admission closed.
    assert env.applied and finished(env)["state"] == "COMMITTED" and finished(env)["receipt_present"]
    assert sum(method == "POST" for method, _ in env.calls) == 1
    assert env.journal.worker_events()[-1]["kind"] == "RELEASED"


@pytest.mark.asyncio
async def test_epoch_change_during_post_is_stale_not_accepted(tmp_path, monkeypatch):
    env = setup(tmp_path)
    epoch = TurnEpochClock()
    monkeypatch.setattr(command, "TurnEpochClock", lambda: epoch)
    base = transport(env)
    async def handler(request):
        result = base(request)
        if request.method == "POST":
            await epoch.advance("SIMULATED correction after apply")
        return result
    assert await execute(env, handler=handler) == 3
    assert finished(env)["state"] == "COMMITTED_STALE" and finished(env)["readback_verified"] is True


@pytest.mark.asyncio
async def test_lease_takeover_after_apply_fences_old_host_and_preserves_get_only_recovery(tmp_path):
    env = setup(tmp_path)
    replacement = []
    base = transport(env)
    def handler(request):
        result = base(request)
        if request.method == "POST":
            with env.journal._transaction() as db:
                db.execute("UPDATE worker_lease SET expires_ms=0")
            other = SQLiteActionJournal(env.journal.path)
            other.acquire_lease("SIMULATED-replacement")
            replacement.append(other)
        return result
    with pytest.raises(JournalError):
        await execute(env, handler=handler)
    record = env.journal.records()[0][1]
    assert record["state"] == "IN_FLIGHT" and record["result"] is None
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT owner FROM worker_lease").fetchone()[0] == "SIMULATED-replacement"
    replacement[0].release_lease()
    env.args.action_id = record["action_id"]
    assert await reconcile(env.args, provider_factory=lambda config: StripeSandboxCancellation(
        config, transport=httpx.MockTransport(transport(env)))) == 0
    assert env.journal.records()[0][1]["state"] == "COMMITTED_STALE"
    assert sum(method == "POST" for method, _ in env.calls) == 1


@pytest.mark.asyncio
async def test_approval_returned_after_wait_deadline_is_not_consumed(tmp_path, monkeypatch):
    env = setup(tmp_path)
    clock = SimpleNamespace(now=0)
    original = type(env.journal.security).find_forward_approval
    def delayed(authority, credential, action_id):
        selected = original(authority, credential, action_id)
        clock.now = 121
        return selected
    monkeypatch.setattr(type(env.journal.security), "find_forward_approval", delayed)
    assert await execute(env, clock=lambda: clock.now) == 3
    assert finished(env)["dispatch_intent_recorded"] is False
    with closing(sqlite3.connect(env.journal.path)) as db:
        assert db.execute("SELECT consumed FROM approvals").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_readback_persistence_failure_cannot_pass_acceptance(tmp_path, monkeypatch):
    env = setup(tmp_path)
    original = SQLiteActionJournal.observe_worker
    def fail(journal, kind, details):
        if kind == "PROVIDER_READBACK":
            raise JournalError("SIMULATED readback observation unavailable")
        return original(journal, kind, details)
    monkeypatch.setattr(SQLiteActionJournal, "observe_worker", fail)
    assert await execute(env) == 3
    assert finished(env)["receipt_present"] is True and finished(env)["readback_verified"] is False
    assert env.journal.records()[0][1]["result"]["observed_via"] == "POST"


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("allow_sandbox_cancellation", False), ("allow_sandbox_cancellation", 1),
                                        ("approval_wait_seconds", True), ("approval_wait_seconds", 0),
                                        ("approval_wait_seconds", 301), ("approval_wait_seconds", "120")])
async def test_unbounded_or_implicit_enablement_is_rejected_before_any_provider_read(tmp_path, field, value):
    env = setup(tmp_path)
    setattr(env.args, field, value)
    with pytest.raises(ValueError):
        await execute(env)
    assert env.calls == [] and env.journal.records() == []


@pytest.mark.parametrize("extra", [["--api-key", KEY], ["--base-url", "https://attacker.invalid/" + KEY],
                                  ["--action-id", "old-action"], ["--auto-approve"], []])
def test_cli_requires_explicit_scope_without_echoing_raw_secret_or_enabling_replay(extra):
    result = subprocess.run([sys.executable, "-m", "providers.sandbox_drill", *extra],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 2 and KEY not in result.stdout + result.stderr
    assert "Invalid sandbox drill arguments" in result.stderr


def test_cli_failure_does_not_print_exception_or_claim_remote_absence(monkeypatch, capsys):
    async def fail(args):
        raise RuntimeError("PRIVATE:" + KEY)
    monkeypatch.setattr(command, "run", fail)
    monkeypatch.setattr(sys, "argv", ["drill", "--journal", "unused", "--credential-file", "unused",
                                      "--stripe-key-file", "unused", "--account-id", ACCOUNT,
                                      "--payment-intent-id", INTENT, "--allow-sandbox-cancellation"])
    with pytest.raises(SystemExit) as stopped:
        command.main()
    assert stopped.value.code == 1
    output = capsys.readouterr()
    assert KEY not in output.out + output.err and "PRIVATE:" not in output.out + output.err
    assert "Preserve the journal" in output.err and "GET-only reconciliation" in output.err
