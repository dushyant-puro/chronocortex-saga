"""Real SQLite ownership and deterministic barriers; all effects SIMULATED."""
import asyncio
from contextlib import closing, contextmanager
from dataclasses import replace
import json
import sqlite3
import threading
from types import SimpleNamespace

import pytest
import pytest_asyncio

from runtime.action_journal import SQLiteActionJournal, JournalError, JournalLeaseError
from runtime.policy_engine import PermissionDeniedError
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock, ActionState
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.worker_supervisor import WorkerSettings, WorkerSupervisor, WorkerUnavailable
from runtime import worker_supervisor as supervision


class Clocks:
    wall = 1000
    mono = 0.0

    def advance(self, milliseconds):
        self.wall += milliseconds
        self.mono += milliseconds / 1000


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["AUTHORIZE", "RENEW_LEASE", "PERSIST_HEARTBEAT"])
async def test_native_contention_diagnostics_do_not_reopen_admission_or_retry(env, monkeypatch, caplog, phase):
    await env.host.start(factory)
    env.journal.timeout = .01  # Test-only bound; production timeout is unchanged.
    target = {"AUTHORIZE": (env.host, "_authorize_host"),
              "RENEW_LEASE": (env.journal, "renew_lease"),
              "PERSIST_HEARTBEAT": (env.journal, "observe_worker")}[phase]
    original = getattr(*target)
    attempts = []
    def contended(*args, **kwargs):
        if phase == "PERSIST_HEARTBEAT" and args[0] != "HOST_HEARTBEAT":
            return original(*args, **kwargs)
        attempts.append(phase)
        with closing(sqlite3.connect(env.path, isolation_level=None)) as holder:
            holder.execute("BEGIN EXCLUSIVE")
            try:
                return original(*args, **kwargs)
            finally:
                holder.rollback()
    monkeypatch.setattr(*target, contended)
    with pytest.raises(JournalError):
        await env.host.pulse()
    with pytest.raises(WorkerUnavailable):
        env.host.submit(lambda _: pytest.fail("Failed renewal admitted work"))
    report = await env.host.stop()
    assert report.failure_code == "RENEWAL_FAILED" and attempts == [phase]
    assert f"phase={phase} category=SQLITE_BUSY" in caplog.text
    assert env.journal.records() == []


@pytest.mark.asyncio
async def test_failed_diagnostic_sink_preserves_original_renewal_error_and_fence(env, monkeypatch):
    await env.host.start(factory)
    failure = JournalError("PRIVATE CREDENTIAL PAYLOAD")
    def fail(**_): raise failure
    def broken_log(*_, **__): raise RuntimeError("PRIVATE LOGGER PAYLOAD")
    monkeypatch.setattr(env.journal, "renew_lease", fail)
    monkeypatch.setattr(supervision.logger, "error", broken_log)
    with pytest.raises(JournalError) as observed:
        await env.host.pulse()
    assert observed.value is failure
    with pytest.raises(WorkerUnavailable):
        env.host.submit(lambda _: pytest.fail("Logging failure reopened admission"))
    assert (await env.host.stop()).failure_code == "RENEWAL_FAILED"


def factory(journal, auth):
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write", required_permissions=["test:write"]))
    return SpeculativeSagaManager(TurnEpochClock(), registry=registry, journal=journal, auth_context=auth)


@pytest_asyncio.fixture
async def env(tmp_path):
    clock = Clocks()
    path = tmp_path / "journal.db"
    journal = SQLiteActionJournal(path, clock_ms=lambda: clock.wall)
    journal.security.provision_principal("worker", {"ccs:work", "test:write", "ccs:repair", "ccs:reconcile"})
    journal.security.provision_principal("approver", {"test:write", "ccs:repair", "ccs:approve"})
    credential = journal.security.issue_credential("worker")
    approver = journal.security.issue_credential("approver")
    host = WorkerSupervisor(journal, credential.token, monotonic=lambda: clock.mono,
                            settings=WorkerSettings(drain_timeout_ms=1000))
    yield SimpleNamespace(host=host, journal=journal, path=path, clock=clock, credential=credential, approver=approver)
    if host._loop is not None:
        await host.stop()
    # Test-only teardown for failed assertions with fake blocked executors.
    # Production stop NEVER cancels these operations.
    pending = host._active_tasks()
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.asyncio
async def test_acquire_before_hydration_and_release_after_idle_drain(env):
    order = []
    def build(journal, auth):
        order.append(journal.worker_events()[-1]["kind"])
        assert auth.credential == env.credential.token
        return factory(journal, auth)
    await env.host.start(build)
    assert order == ["HOST_STARTING"] and env.host.state == "READY"
    report = await env.host.stop()
    assert report.drained and report.lease_released and report.state == "STOPPED"
    kinds = [event["kind"] for event in env.journal.worker_events()]
    assert kinds == ["ACQUIRED", "HOST_STARTING", "HOST_READY", "HOST_DRAINING", "HOST_STOPPED", "RELEASED"]
    assert (await env.host.stop()) is report
    with pytest.raises(WorkerUnavailable):
        await env.host.start(build)


@pytest.mark.asyncio
async def test_missing_worker_permission_never_acquires_or_constructs(env):
    env.journal.security.update_principal("worker", permissions={"test:write"})
    with pytest.raises(PermissionDeniedError):
        await env.host.start(lambda *_: pytest.fail("unauthorized hydration"))
    assert env.journal.worker_events() == [] and env.host.state == "STOPPED"


@pytest.mark.asyncio
async def test_second_host_cannot_take_over_current_lease(env):
    await env.host.start(factory)
    other = WorkerSupervisor(SQLiteActionJournal(env.path, clock_ms=lambda: env.clock.wall), env.credential.token)
    with pytest.raises(JournalLeaseError, match="held"):
        await other.start(lambda *_: pytest.fail("second hydration"))
    assert other.state == "STOPPED" and env.host.state == "READY"


@pytest.mark.asyncio
async def test_factory_failure_releases_owned_lease_without_enabling_dispatch(env):
    def fail(journal, auth):
        with pytest.raises(WorkerUnavailable):
            factory(journal, auth).stage_write("change", {})
        raise RuntimeError("SIMULATED factory failure")
    with pytest.raises(RuntimeError):
        await env.host.start(fail)
    assert env.journal.records() == []
    assert env.host.report.drained and env.host.report.lease_released
    assert env.host.report.failure_code == "STARTUP_FAILED"


@pytest.mark.asyncio
async def test_async_factory_is_rejected_and_never_runs(env):
    async def build(*_):
        pytest.fail("asynchronous factory ran")
    with pytest.raises(WorkerUnavailable, match="synchronously"):
        await env.host.start(build)
    assert env.host.report.lease_released


@pytest.mark.asyncio
async def test_consumer_cancellation_cannot_cancel_owned_forward_call(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    started, finish = asyncio.Event(), asyncio.Event()
    async def execute(_):
        started.set()
        await finish.wait()
        return {"provider": "SIMULATED", "receipt": "applied"}
    handle = env.host.submit(lambda runtime: runtime.commit_write(action.action_id, execute))
    waiter = asyncio.create_task(handle.wait())
    await started.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not handle.done() and action.state == ActionState.IN_FLIGHT
    finish.set()
    assert (await handle.wait())["receipt"] == "applied"
    assert env.journal.records()[0][1]["state"] == "COMMITTED"


@pytest.mark.asyncio
async def test_drain_fences_new_work_but_keeps_lease_and_retains_forward_receipt(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    started, finish = asyncio.Event(), asyncio.Event()
    async def execute(_):
        started.set()
        await finish.wait()
        return {"provider": "SIMULATED", "receipt": "retained during drain"}
    handle = env.host.submit(lambda runtime: runtime.commit_write(action.action_id, execute))
    await started.wait()
    stopping = asyncio.create_task(env.host.stop())
    await env.host.stopping.wait()
    assert not stopping.done()
    with pytest.raises(WorkerUnavailable):
        env.host.submit(lambda _: asyncio.sleep(0))
    with pytest.raises(JournalLeaseError):
        SQLiteActionJournal(env.path, clock_ms=lambda: env.clock.wall).acquire_lease("replacement")
    finish.set()
    await handle.wait()
    report = await stopping
    assert report.drained and report.lease_released
    assert env.journal.records()[0][1]["result"]["receipt"] == "retained during drain"


@pytest.mark.asyncio
async def test_direct_saga_calls_are_tracked_even_without_submit(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    started, finish = asyncio.Event(), asyncio.Event()
    async def execute(_):
        started.set()
        await finish.wait()
    call = asyncio.create_task(saga.commit_write(action.action_id, execute))
    await started.wait()
    stopping = asyncio.create_task(env.host.stop())
    await env.host.stopping.wait()
    assert not stopping.done() and call in env.host._active_tasks()
    finish.set()
    await call
    assert (await stopping).drained


@pytest.mark.asyncio
async def test_timeout_never_cancels_a_call_or_releases_its_lease(env):
    env.host.settings = replace(env.host.settings, drain_timeout_ms=1)
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    started, finish = asyncio.Event(), asyncio.Event()
    async def execute(_):
        started.set()
        await finish.wait()
        return {"provider": "SIMULATED", "receipt": "late"}
    handle = env.host.submit(lambda runtime: runtime.commit_write(action.action_id, execute))
    await started.wait()
    report = await env.host.stop()
    assert not report.drained and not report.lease_released and report.pending_operations == 1
    assert report.state == "RECOVERY_REQUIRED" and not handle.done()
    assert env.journal.records()[0][1]["state"] == "IN_FLIGHT"
    with pytest.raises(JournalLeaseError):
        SQLiteActionJournal(env.path, clock_ms=lambda: env.clock.wall).acquire_lease("replacement")
    finish.set()
    await handle.wait()
    assert env.journal.records()[0][1]["result"]["receipt"] == "late"
    assert env.host.state == "RECOVERY_REQUIRED"


@pytest.mark.asyncio
async def test_takeover_after_timeout_never_replays_or_accepts_old_late_success(env):
    env.host.settings = replace(env.host.settings, drain_timeout_ms=1)
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    started, finish = asyncio.Event(), asyncio.Event()
    effects = []
    async def execute(_):
        effects.append("SIMULATED applied")
        started.set()
        await finish.wait()
        return {"provider": "SIMULATED", "receipt": "late after takeover"}
    handle = env.host.submit(lambda runtime: runtime.commit_write(action.action_id, execute))
    await started.wait()
    await env.host.stop()
    env.clock.advance(30_000)
    replacement_journal = SQLiteActionJournal(env.path, clock_ms=lambda: env.clock.wall)
    replacement = WorkerSupervisor(replacement_journal, env.credential.token, monotonic=lambda: env.clock.mono)
    recovered = await replacement.start(factory)
    assert recovered._actions[action.action_id].state == ActionState.IN_FLIGHT_UNKNOWN
    finish.set()
    with pytest.raises(JournalLeaseError):
        await handle.wait()
    assert saga._actions[action.action_id].result["receipt"] == "late after takeover"
    assert effects == ["SIMULATED applied"] and env.journal.records()[0][1]["state"] == "IN_FLIGHT"
    with pytest.raises(JournalError, match="Recovered"):
        await recovered.commit_write(action.action_id, execute)
    assert (await replacement.stop()).lease_released


@pytest.mark.asyncio
async def test_bound_connections_inherit_the_same_closed_admission_gate(env):
    saga = await env.host.start(factory)
    other = SQLiteActionJournal(env.path, clock_ms=lambda: env.clock.wall)
    other.bind_lease(env.host._lease)
    bound = factory(other, saga.auth_context)
    env.host.request_stop()
    with pytest.raises(WorkerUnavailable):
        bound.stage_write("change", {})
    assert env.journal.records() == []


@pytest.mark.asyncio
async def test_monotonic_stall_fences_dispatch_even_with_unchanged_wall_time(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    env.clock.mono = 30.0
    with pytest.raises(WorkerUnavailable):
        await saga.commit_write(action.action_id, lambda _: pytest.fail("dispatch after loop stall"))
    assert env.journal.records()[0][1]["state"] == "PENDING"
    assert (await env.host.stop()).failure_code == "MONOTONIC_DEADLINE"


@pytest.mark.asyncio
async def test_renewal_uses_request_start_not_response_time(env, monkeypatch):
    await env.host.start(factory)
    original = env.journal.renew_lease
    def delayed(**kwargs):
        env.clock.mono = 30.0
        original(**kwargs)
    monkeypatch.setattr(env.journal, "renew_lease", delayed)
    with pytest.raises(WorkerUnavailable):
        await env.host.pulse()
    report = await env.host.stop()
    assert report.failure_code == "RENEWAL_FAILED"
    with pytest.raises(WorkerUnavailable):
        env.host.submit(lambda _: asyncio.sleep(0))


@pytest.mark.asyncio
async def test_host_role_revocation_wins_in_dispatch_transaction_before_heartbeat(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    env.journal.security.update_principal("worker", permissions={"test:write"})
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: pytest.fail("revoked host dispatched"))
    assert env.journal.records()[0][1]["state"] == "PENDING"
    assert (await env.host.stop()).failure_code == "AUTHORITY_LOST"


@pytest.mark.asyncio
async def test_a_failed_heartbeat_is_not_retried(env, monkeypatch):
    await env.host.start(factory)
    calls = []
    def failed(**kwargs):
        calls.append("renew")
        raise JournalError("SIMULATED storage failure")
    monkeypatch.setattr(env.journal, "renew_lease", failed)
    with pytest.raises(JournalError):
        await env.host.pulse()
    await env.host.stop()
    assert calls == ["renew"]


@pytest.mark.asyncio
async def test_cancelled_shutdown_waiter_cannot_cancel_drain(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    started, finish = asyncio.Event(), asyncio.Event()
    async def execute(_):
        started.set()
        await finish.wait()
    handle = env.host.submit(lambda runtime: runtime.commit_write(action.action_id, execute))
    await started.wait()
    waiter = asyncio.create_task(env.host.stop())
    await env.host.stopping.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not env.host._stop_task.cancelled()
    finish.set()
    await handle.wait()
    assert (await env.host.stop()).lease_released


@pytest.mark.asyncio
async def test_capacity_rejects_extra_admission_before_creating_a_coroutine(env):
    env.host.settings = replace(env.host.settings, max_operations=1)
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    started, finish = asyncio.Event(), asyncio.Event()
    async def execute(_):
        started.set()
        await finish.wait()
    first = env.host.submit(lambda runtime: runtime.commit_write(action.action_id, execute))
    await started.wait()
    with pytest.raises(WorkerUnavailable, match="capacity"):
        env.host.submit(lambda _: pytest.fail("extra operation factory called"))
    finish.set()
    await first.wait()


@pytest.mark.asyncio
async def test_running_compensation_is_drained_without_a_second_mutation(env):
    saga = await env.host.start(factory)
    started, finish = asyncio.Event(), asyncio.Event()
    undos = []
    async def undo(receipt):
        undos.append(receipt)
        started.set()
        await finish.wait()
    action = saga.stage_write("change", {}, compensate=undo)
    await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"provider": "SIMULATED", "receipt": "original"}))
    saga.auth_context.repair_approval_id = env.journal.security.issue_approval(env.approver.token, "worker", action.action_id, direction="COMPENSATION")
    handle = env.host.submit(lambda runtime: runtime.compensate_action(action.action_id))
    await started.wait()
    stopping = asyncio.create_task(env.host.stop())
    await env.host.stopping.wait()
    assert not stopping.done()
    finish.set()
    await handle.wait()
    assert (await stopping).drained
    assert len(undos) == 1 and env.journal.records()[0][1]["state"] == "COMPENSATED"


@pytest.mark.asyncio
async def test_status_result_during_drain_persists_without_starting_compensation(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {}, compensate=lambda _: pytest.fail("new repair during drain"))
    async def uncertain(_):
        raise ConnectionError("SIMULATED lost reply")
    with pytest.raises(ConnectionError):
        await saga.commit_write(action.action_id, uncertain)
    started, finish = asyncio.Event(), asyncio.Event()
    async def status(_):
        started.set()
        await finish.wait()
        return True
    handle = env.host.submit(lambda runtime: runtime.reconcile_write(action.action_id, status))
    await started.wait()
    stopping = asyncio.create_task(env.host.stop())
    await env.host.stopping.wait()
    finish.set()
    await handle.wait()
    assert (await stopping).drained
    assert env.journal.records()[0][1]["state"] == "COMMITTED"
    assert env.journal.records()[0][1]["compensation_state"] == "NOT_STARTED"


@pytest.mark.asyncio
async def test_current_host_role_is_required_for_new_status_queries(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    async def uncertain(_):
        raise ConnectionError("SIMULATED lost reply")
    with pytest.raises(ConnectionError):
        await saga.commit_write(action.action_id, uncertain)
    env.journal.security.update_principal("worker", permissions={"test:write", "ccs:reconcile"})
    with pytest.raises(PermissionDeniedError):
        await saga.reconcile_write(action.action_id, lambda _: pytest.fail("revoked host queried provider"))
    assert env.journal.records()[0][1]["state"] == "IN_FLIGHT_UNKNOWN"


@pytest.mark.asyncio
async def test_background_heartbeat_renews_and_stops_on_first_failure(env, monkeypatch):
    env.host.settings = replace(env.host.settings, renew_interval_ms=10)
    calls = []
    original = env.journal.renew_lease
    def renew(**kwargs):
        calls.append("renew")
        if len(calls) == 2:
            raise JournalError("SIMULATED second heartbeat fault")
        original(**kwargs)
    monkeypatch.setattr(env.journal, "renew_lease", renew)
    await env.host.start(factory)
    await asyncio.wait_for(env.host.stopped.wait(), 10)
    assert calls == ["renew", "renew"]
    assert env.host.report.failure_code == "RENEWAL_FAILED"
    assert any(event["kind"] == "HOST_HEARTBEAT" for event in env.journal.worker_events())


@pytest.mark.asyncio
async def test_audit_failure_never_reports_successful_release(env, monkeypatch):
    await env.host.start(factory)
    original = env.journal.observe_worker
    def observe(kind, details):
        if kind == "HOST_STOPPED":
            raise JournalError("SIMULATED audit failure")
        original(kind, details)
    monkeypatch.setattr(env.journal, "observe_worker", observe)
    report = await env.host.stop()
    assert report.drained and not report.lease_released
    assert report.state == "RECOVERY_REQUIRED" and report.failure_code == "STOP_PERSISTENCE_FAILED"
    assert not any(event["kind"] == "RELEASED" for event in env.journal.worker_events())


@pytest.mark.asyncio
async def test_cancelled_startup_waiter_cannot_abandon_acquisition_or_hydrate_after_stop(env, monkeypatch):
    started, finish = threading.Event(), threading.Event()
    original = env.journal.acquire_lease
    hydrated = []
    def acquire(*args, **kwargs):
        started.set()
        assert finish.wait(10)
        return original(*args, **kwargs)
    def build(*args):
        hydrated.append(True)
        return factory(*args)
    monkeypatch.setattr(env.journal, "acquire_lease", acquire)
    waiter = asyncio.create_task(env.host.start(build))
    assert await asyncio.to_thread(started.wait, 10)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not env.host._start_task.cancelled()
    finish.set()
    await asyncio.wait_for(env.host.stopped.wait(), 10)
    assert hydrated == [] and env.host.report.lease_released


@pytest.mark.asyncio
async def test_cancelled_renewal_waiter_retains_owned_io_until_shutdown(env, monkeypatch):
    await env.host.start(factory)
    started, finish = threading.Event(), threading.Event()
    original = env.journal.renew_lease
    def renew(**kwargs):
        started.set()
        assert finish.wait(10)
        original(**kwargs)
    monkeypatch.setattr(env.journal, "renew_lease", renew)
    waiter = asyncio.create_task(env.host.pulse())
    assert await asyncio.to_thread(started.wait, 10)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await env.host.stopping.wait()
    assert not env.host._pulse_task.cancelled() and not env.host._stop_task.done()
    assert not any(event["kind"] == "RELEASED" for event in env.journal.worker_events())
    finish.set()
    report = await env.host.stop()
    assert report.lease_released and report.failure_code == "RENEWAL_WAITER_CANCELLED"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["host_role", "stop"])
async def test_intent_rechecks_host_gate_after_preflight_validator(env, change):
    def validate(_):
        if change == "host_role":
            env.journal.security.update_principal("worker", permissions={"test:write"})
        else:
            env.host.request_stop()
    def build(journal, auth):
        registry = ToolRegistry()
        registry.register(ToolManifest("change", "write", required_permissions=["test:write"], validate_args=validate))
        return SpeculativeSagaManager(TurnEpochClock(), registry=registry, journal=journal, auth_context=auth)
    saga = await env.host.start(build)
    action = saga.stage_write("change", {})
    with pytest.raises((PermissionDeniedError, WorkerUnavailable)):
        await saga.commit_write(action.action_id, lambda _: pytest.fail("preflight race dispatched"))
    assert env.journal.records()[0][1]["state"] == "PENDING"
    assert not any(event["kind"] == "DISPATCH_INTENT" for event in env.journal.worker_events())


@pytest.mark.asyncio
async def test_failed_heartbeat_drains_issued_forward_without_erasing_its_receipt(env):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {})
    started, finish = asyncio.Event(), asyncio.Event()
    async def execute(_):
        started.set()
        await finish.wait()
        return {"provider": "SIMULATED", "receipt": "survived host authority loss"}
    handle = env.host.submit(lambda runtime: runtime.commit_write(action.action_id, execute))
    await started.wait()
    env.journal.security.revoke_credential(env.credential.id)
    with pytest.raises(PermissionDeniedError):
        await env.host.pulse()
    await env.host.stopping.wait()
    assert not handle.done()
    finish.set()
    await handle.wait()
    report = await env.host.stop()
    assert report.drained and report.lease_released and report.failure_code == "RENEWAL_FAILED"
    assert env.journal.records()[0][1]["result"]["receipt"] == "survived host authority loss"


@pytest.mark.asyncio
async def test_activity_registration_does_not_hold_gate_lock_while_waiting_for_sqlite(env, monkeypatch):
    saga = await env.host.start(factory)
    action = saga.stage_write("change", {"main": 1})
    other = SQLiteActionJournal(env.path, clock_ms=lambda: env.clock.wall)
    other.bind_lease(env.host._lease)
    bound = factory(other, saga.auth_context)
    entering, finished = threading.Event(), threading.Event()
    failures = []
    def writer():
        try:
            assert entering.wait(10)
            bound.stage_write("change", {"bound": 1})
        except BaseException as exc:
            failures.append(type(exc).__name__)
        finally:
            finished.set()
    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    original = env.journal._transaction
    first = [True]
    @contextmanager
    def interleave(**kwargs):
        if first[0]:
            first[0] = False
            entering.set()
            assert finished.wait(10), "Lifecycle lock inverted with a bound SQLite writer"
        with original(**kwargs) as db:
            yield db
    monkeypatch.setattr(env.journal, "_transaction", interleave)
    try:
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0))
    finally:
        thread.join(timeout=10)
    assert not failures and not thread.is_alive()
    assert len(env.journal.records()) == 2


@pytest.mark.parametrize("overrides", [{"lease_ttl_ms": True}, {"lease_ttl_ms": 999}, {"renew_interval_ms": 15_000}, {"safety_margin_ms": 30_000}, {"drain_timeout_ms": 0}, {"max_operations": 0}])
def test_invalid_settings_are_rejected(overrides):
    with pytest.raises(ValueError):
        WorkerSettings(**overrides)


@pytest.mark.asyncio
async def test_host_events_never_include_bearer_secrets(env):
    await env.host.start(factory)
    env.clock.advance(5000)
    await env.host.pulse()
    await env.host.stop()
    with closing(sqlite3.connect(env.path)) as db:
        dump = "\n".join(db.iterdump())
    assert env.credential.token not in dump and env.credential.token.split(".")[1] not in dump
    observations = [json.loads(event["subject"]) for event in env.journal.worker_events() if event["kind"].startswith("HOST_")]
    assert all(set(observation) == {"phase", "active_operations", "failure_code", "heartbeat_ttl_ms"} for observation in observations)
