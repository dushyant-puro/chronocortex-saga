"""Local execution authority only: fake provider effects are never rolled back by a lease."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading

import pytest

from runtime.action_journal import JournalError, JournalLeaseError, SQLiteActionJournal, WorkerLease
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import ActionState, CompensationState, SpeculativeSagaManager, TurnEpochClock
from runtime.tool_contract import ToolManifest, ToolRegistry
from runtime.task_engine import TaskManager, TaskStep, TaskState
from tests.workflow_test_support import VersionedTestTasks


class Clock:
    def __init__(self):
        self.now = 1000

    def __call__(self):
        return self.now


def saga(journal):
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write"))
    return SpeculativeSagaManager(TurnEpochClock(), registry=registry,
                                  auth_context=AuthorizationContext(), journal=journal)


def test_unowned_journal_cannot_stage(tmp_path):
    manager = saga(SQLiteActionJournal(tmp_path / "journal.db"))
    with pytest.raises(JournalError, match="lease"):
        manager.stage_write("change", {})
    assert manager.journal.records() == []


@pytest.mark.parametrize("operation", ["checkpoint", "close", "renew", "release"])
def test_unbound_connections_have_no_durable_mutation_authority(tmp_path, operation):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    with pytest.raises(JournalLeaseError):
        if operation == "checkpoint":
            journal.save_task("T", {"task_id": "T"}, None)
        elif operation == "close":
            journal.close_chain("batch")
        elif operation == "renew":
            journal.renew_lease()
        else:
            journal.release_lease()
    assert journal.closed_chains() == set() and journal.load_task("T") is None


@pytest.mark.asyncio
async def test_task_parks_when_lease_expires_after_effect(tmp_path):
    clock = Clock()
    journal = SQLiteActionJournal(tmp_path / "journal.db", clock_ms=clock)
    journal.acquire_lease("A", ttl_ms=10)
    manager = saga(journal)
    effects = []

    async def execute(args):
        effects.append("SIMULATED applied")
        clock.now += 10
        return {"id": "SIMULATED:applied"}

    class Tasks(VersionedTestTasks, TaskManager):
        def _get_executor(self, step):
            return execute

    tasks = Tasks(manager, lambda _: None)
    task = tasks.create_task("durable task", [TaskStep("change")])
    with pytest.raises(JournalError):
        await tasks.run_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert journal.load_task(task.task_id)[1]["state"] != "COMPLETED"
    assert effects == ["SIMULATED applied"] and manager._chain == []


@pytest.mark.asyncio
async def test_expiry_during_status_query_cannot_authorize_automatic_repair(tmp_path):
    clock = Clock()
    journal = SQLiteActionJournal(tmp_path / "journal.db", clock_ms=clock)
    journal.acquire_lease("A", ttl_ms=10)
    manager = saga(journal)

    async def unknown(args):
        raise ConnectionError("SIMULATED reply lost")

    action = manager.stage_write("change", {}, compensate=lambda _: pytest.fail("expired repair"))
    with pytest.raises(ConnectionError):
        await manager.commit_write(action.action_id, unknown)

    async def status(action):
        clock.now += 10
        return True

    with pytest.raises(JournalLeaseError):
        await manager.reconcile_write(action.action_id, status)
    assert journal.records()[0][1]["state"] == "IN_FLIGHT_UNKNOWN"
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN


@pytest.mark.asyncio
async def test_expiry_before_dispatch_calls_no_executor(tmp_path):
    clock = Clock()
    journal = SQLiteActionJournal(tmp_path / "journal.db", clock_ms=clock)
    journal.acquire_lease("worker-A", ttl_ms=10)
    manager = saga(journal)
    action = manager.stage_write("change", {})
    clock.now += 10
    with pytest.raises(JournalError, match="lease"):
        await manager.commit_write(action.action_id, lambda _: pytest.fail("expired dispatch"))
    assert journal.records()[0][1]["state"] == "PENDING"


@pytest.mark.asyncio
async def test_takeover_during_await_never_commits_or_automatically_undoes(tmp_path):
    clock = Clock()
    path = tmp_path / "journal.db"
    old = SQLiteActionJournal(path, clock_ms=clock)
    old.acquire_lease("worker-A", ttl_ms=10)
    manager = saga(old)
    calls = []
    started, release = asyncio.Event(), asyncio.Event()

    async def execute(args):
        calls.append("SIMULATED effect applied")
        started.set()
        await release.wait()
        return {"receipt": "SIMULATED:already-applied"}

    async def undo(result):
        calls.append("forbidden undo")

    action = manager.stage_write("change", {}, compensate=undo)
    committing = asyncio.create_task(manager.commit_write(action.action_id, execute))
    await asyncio.wait_for(started.wait(), 1)
    clock.now += 10
    new = SQLiteActionJournal(path, clock_ms=clock)
    new.acquire_lease("worker-B", ttl_ms=100)
    recovered = saga(new)
    release.set()
    with pytest.raises(JournalError, match="lease"):
        await committing
    assert calls == ["SIMULATED effect applied"]
    assert action.result == {"receipt": "SIMULATED:already-applied"}
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN and manager._chain == []
    assert old.records()[0][1]["state"] == "IN_FLIGHT"
    assert recovered._actions[action.action_id].state == ActionState.IN_FLIGHT_UNKNOWN
    with pytest.raises(JournalError, match="Recovered"):
        await recovered.commit_write(action.action_id, execute)


def test_stale_worker_cannot_publish_task_checkpoint(tmp_path):
    clock = Clock()
    path = tmp_path / "journal.db"
    old = SQLiteActionJournal(path, clock_ms=clock)
    old.acquire_lease("worker-A", ttl_ms=10)
    cp = {"task_id": "T", "state": "RUNNING"}
    old.save_task("T", cp, None)
    clock.now += 10
    new = SQLiteActionJournal(path, clock_ms=clock)
    new.acquire_lease("worker-B", ttl_ms=100)
    with pytest.raises(JournalError, match="lease"):
        old.save_task("T", {**cp, "state": "COMPLETED"}, 0)
    assert new.load_task("T") == (0, cp)


@pytest.mark.parametrize("operation", ["renew", "release", "close", "transition", "bind"])
def test_old_generation_cannot_mutate_after_same_owner_name_reacquires(tmp_path, operation):
    clock = Clock()
    path = tmp_path / "journal.db"
    old = SQLiteActionJournal(path, clock_ms=clock)
    token = old.acquire_lease("same-name", ttl_ms=10)
    manager = saga(old)
    action = manager.stage_write("change", {})
    version, record = old.records()[0]
    old.release_lease()
    new = SQLiteActionJournal(path, clock_ms=clock)
    fresh = new.acquire_lease("same-name", ttl_ms=100)
    assert fresh.generation == token.generation + 1
    before = new.worker_events()
    with pytest.raises(JournalLeaseError):
        if operation == "renew":
            old.renew_lease()
        elif operation == "release":
            old.release_lease()
        elif operation == "close":
            old.close_chain(action.chain_id)
        elif operation == "transition":
            old.transition({**record, "state": "ABORTED"}, version, "STALE_RELEASE")
        else:
            SQLiteActionJournal(path, clock_ms=clock).bind_lease(token)
    assert new.worker_events() == before
    assert new.records() == [(version, record)] and new.closed_chains() == set()
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT count(*) FROM key_claims").fetchone()[0] == 1


def test_renewal_before_expiry_preserves_generation_and_extends_authority(tmp_path):
    clock = Clock()
    path = tmp_path / "journal.db"
    journal = SQLiteActionJournal(path, clock_ms=clock)
    token = journal.acquire_lease("A", ttl_ms=10)
    clock.now += 9
    journal.renew_lease(ttl_ms=20)
    clock.now += 1
    with pytest.raises(JournalLeaseError, match="held"):
        SQLiteActionJournal(path, clock_ms=clock).acquire_lease("B")
    saga(journal).stage_write("change", {})
    assert journal.worker_events()[-1]["generation"] == token.generation
    clock.now += 20
    with pytest.raises(JournalLeaseError):
        journal.renew_lease()
    with pytest.raises(JournalLeaseError, match="unbound"):
        journal.acquire_lease("A")


@pytest.mark.parametrize("ttl", [True, False, 0, -1, 1.5, "10", None, 86_400_001])
def test_invalid_ttl_never_acquires(tmp_path, ttl):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    with pytest.raises(JournalLeaseError):
        journal.acquire_lease("A", ttl_ms=ttl)
    assert journal.worker_events() == []


@pytest.mark.parametrize("bad_time", [True, -1, 1.5, None, 9_223_372_036_854_775_807])
def test_invalid_clock_never_acquires(tmp_path, bad_time):
    journal = SQLiteActionJournal(tmp_path / "journal.db", clock_ms=lambda: bad_time)
    with pytest.raises(JournalLeaseError):
        journal.acquire_lease("A")
    assert journal.worker_events() == []


def test_clock_rollback_denies_existing_worker_and_takeover(tmp_path):
    clock = Clock()
    path = tmp_path / "journal.db"
    journal = SQLiteActionJournal(path, clock_ms=clock)
    journal.acquire_lease("A", ttl_ms=10)
    clock.now += 5
    journal.renew_lease(ttl_ms=10)
    clock.now -= 1
    for writer in (journal, SQLiteActionJournal(path, clock_ms=clock)):
        with pytest.raises(JournalLeaseError, match="backward"):
            if writer is journal:
                writer.renew_lease()
            else:
                writer.acquire_lease("B")
    # Read-only investigation is still possible without a working clock.
    assert journal.records() == []
    clock.now += 1
    journal.renew_lease()


def test_observed_expiry_cannot_resurrect_after_clock_rollback(tmp_path):
    clock = Clock()
    path = tmp_path / "journal.db"
    journal = SQLiteActionJournal(path, clock_ms=clock)
    token = journal.acquire_lease("A", ttl_ms=10)
    clock.now += 10
    with pytest.raises(JournalLeaseError, match="expired"):
        journal.renew_lease()
    clock.now -= 1
    with pytest.raises(JournalLeaseError, match="backward"):
        journal.renew_lease()
    with pytest.raises(JournalLeaseError, match="backward"):
        SQLiteActionJournal(path, clock_ms=clock).bind_lease(token)
    with pytest.raises(JournalLeaseError, match="backward"):
        SQLiteActionJournal(path, clock_ms=clock).acquire_lease("B")
    assert [e["kind"] for e in journal.worker_events()] == ["ACQUIRED"]


def test_independent_connections_racing_for_lease_have_one_winner(tmp_path):
    clock = Clock()
    journals = [SQLiteActionJournal(tmp_path / "journal.db", clock_ms=clock) for _ in range(2)]
    barrier = threading.Barrier(2, timeout=5)

    def acquire(item):
        idx, journal = item
        barrier.wait()
        try:
            return journal.acquire_lease(str(idx))
        except JournalLeaseError:
            return None

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(acquire, enumerate(journals)))
    assert sum(result is not None for result in results) == 1
    assert len(journals[0].worker_events()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("takeover", [False, True])
async def test_compensation_await_loses_authority_without_retry(tmp_path, takeover):
    clock = Clock()
    path = tmp_path / "journal.db"
    journal = SQLiteActionJournal(path, clock_ms=clock)
    journal.acquire_lease("A", ttl_ms=10)
    manager = saga(journal)
    calls = []

    async def undo(result):
        calls.append("SIMULATED undo applied")
        clock.now += 10
        if takeover:
            SQLiteActionJournal(path, clock_ms=clock).acquire_lease("B")

    action = manager.stage_write("change", {}, compensate=undo)
    await manager.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={}))
    with pytest.raises(JournalLeaseError):
        await manager.compensate_action(action.action_id)
    assert calls == ["SIMULATED undo applied"]
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN
    assert action.compensation_state == CompensationState.UNKNOWN
    assert journal.records()[0][1]["compensation_state"] == "IN_FLIGHT"
    await manager.compensate_action(action.action_id)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_expired_compensation_intent_never_calls_handler(tmp_path):
    clock = Clock()
    journal = SQLiteActionJournal(tmp_path / "journal.db", clock_ms=clock)
    journal.acquire_lease("A", ttl_ms=10)
    manager = saga(journal)
    action = manager.stage_write("change", {}, compensate=lambda _: pytest.fail("expired undo"))
    await manager.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={}))
    clock.now += 10
    with pytest.raises(JournalLeaseError):
        await manager.compensate_action(action.action_id)
    assert journal.records()[0][1]["compensation_state"] == "NOT_STARTED"


def test_worker_audit_and_action_write_rollback_together(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    journal.acquire_lease("A")
    manager = saga(journal)
    with closing(sqlite3.connect(journal.path)) as db:
        db.execute("CREATE TRIGGER reject_worker_event BEFORE INSERT ON worker_events BEGIN SELECT RAISE(ABORT,'injected audit fault'); END")
        db.commit()
    with pytest.raises(JournalError):
        manager.stage_write("change", {})
    assert journal.records() == [] and len(journal.worker_events()) == 1
    with closing(sqlite3.connect(journal.path)) as db:
        assert db.execute("SELECT count(*) FROM key_claims").fetchone()[0] == 0


def schema2_fixture(path):
    """Simulated schema-2 fixture retains real prior-format records and claims."""
    journal = SQLiteActionJournal(path)
    journal.acquire_lease("fixture")
    manager = saga(journal)
    manager.stage_write("change", {})
    before = journal.records()
    with closing(sqlite3.connect(path)) as db:
        db.execute("DROP TABLE worker_events")
        db.execute("DROP TABLE worker_lease")
        db.execute("PRAGMA user_version=2")
        db.commit()
    return before


def test_schema2_migration_is_explicit_and_preserves_unresolved_claims(tmp_path):
    path = tmp_path / "journal.db"
    before = schema2_fixture(path)
    with pytest.raises(JournalError, match="migration"):
        SQLiteActionJournal(path)
    journal = SQLiteActionJournal(path, migrate=True)
    assert journal.records() == before and journal.worker_events() == []
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT count(*) FROM key_claims").fetchone()[0] == 1
        assert db.execute("PRAGMA user_version").fetchone()[0] == 4
    with pytest.raises(JournalLeaseError):
        journal.close_chain(before[0][1]["chain_id"])
    journal.acquire_lease("new")
    assert saga(journal)._actions[before[0][1]["action_id"]].state == ActionState.IN_FLIGHT_UNKNOWN


def test_schema2_migration_failure_rolls_back_all_schema_changes(tmp_path):
    path = tmp_path / "journal.db"
    schema2_fixture(path)

    class Broken(SQLiteActionJournal):
        @staticmethod
        def _create_lease_tables(db):
            SQLiteActionJournal._create_lease_tables(db)
            raise JournalError("injected migration failure")

    with pytest.raises(JournalError, match="injected"):
        Broken(path, migrate=True)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='worker_lease'").fetchall()
    assert SQLiteActionJournal(path, migrate=True).records()


@pytest.mark.parametrize("mode", ["crash", "late"])
def test_subprocess_effect_survives_crash_or_takeover_without_replay(tmp_path, mode):
    path, ledger = tmp_path / "journal.db", tmp_path / "effects.jsonl"
    worker = Path(__file__).with_name("lease_crash_worker.py")
    process = subprocess.Popen([sys.executable, str(worker), str(path), str(ledger), mode],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        # communicate has a timeout; no unbounded readline if the child fails to start.
        if mode == "crash":
            output, error = process.communicate(timeout=15)
            assert process.returncode == 23, error
        else:
            # The fsynced ledger is the independent effect oracle; its sentinel
            # is polled with a bounded deadline, not a performance measurement.
            import time
            deadline = time.monotonic() + 10
            while not ledger.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            assert ledger.exists(), "worker did not reach simulated effect"
        clock = Clock()
        clock.now += 10
        replacement = SQLiteActionJournal(path, clock_ms=clock)
        token = replacement.acquire_lease("replacement", ttl_ms=100)
        assert token.generation == 2
        if mode == "late":
            output, error = process.communicate("return\n", timeout=15)
            assert process.returncode == 0, error
            observation = json.loads(output)
            assert observation["state"] == "IN_FLIGHT_UNKNOWN"
            assert observation["receipt"] == {"id": "SIMULATED:applied"}
        assert ledger.read_text().splitlines() == ['"SIMULATED forward"']
        action = next(iter(saga(replacement)._actions.values()))
        assert action.state == ActionState.IN_FLIGHT_UNKNOWN and action.compensate is None
        assert replacement.records()[0][1]["state"] == "IN_FLIGHT"
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
