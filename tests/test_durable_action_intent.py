"""Durable boundary tests use the real manager and a file-backed database."""
import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading

import pytest

from runtime.policy_engine import AuthorizationContext
from runtime.action_journal import JournalError, JournalConflict, SQLiteActionJournal
from runtime.speculative_saga import (
    ActionState, CompensationState, DuplicateOperationError, MutationNotAppliedError,
    SpeculativeSagaManager, TurnEpochClock,
)
from runtime.task_engine import TaskManager, TaskState, TaskStep, StepState
from runtime.persistence import SQLiteCheckpointStore
from runtime.tool_contract import ToolManifest, ToolRegistry


def manager(path, **kwargs):
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write", required_permissions=["test:write"]))
    return SpeculativeSagaManager(TurnEpochClock(), registry=registry,
                                  auth_context=AuthorizationContext(["test:write"]),
                                  journal=SQLiteActionJournal(path), **kwargs)


class Tasks(TaskManager):
    def __init__(self, saga, execute):
        self.execute = execute
        super().__init__(saga, lambda _: None)

    def _get_executor(self, step):
        return self.execute


@pytest.mark.asyncio
async def test_missing_journal_never_dispatches():
    saga = SpeculativeSagaManager(TurnEpochClock(), auth_context=AuthorizationContext())
    action = saga.stage_write("change", {}, manifest=ToolManifest("change", "write"))
    calls = []

    async def execute(args):
        calls.append(args)

    with pytest.raises(RuntimeError, match="journal"):
        await saga.commit_write(action.action_id, execute)
    assert calls == []


@pytest.mark.asyncio
async def test_executor_observes_committed_intent_and_event(tmp_path):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {"value": "approved"})

    async def execute(args):
        events = SQLiteActionJournal(saga.journal.path).events(action.action_id)
        assert [event["kind"] for event in events] == ["STAGED", "DISPATCH_INTENT"]
        assert events[-1]["record"]["args"] == args
        return {"provider_id": "simulated-operation"}

    await saga.commit_write(action.action_id, execute)
    events = saga.journal.events(action.action_id)
    assert events[-1]["kind"] == "FORWARD_COMMITTED"
    assert [e["version"] for e in events] == [0, 1, 2]
    assert action.state == ActionState.COMMITTED
    # Completion does not permit silent repetition of the same durable operation.
    with pytest.raises(DuplicateOperationError):
        saga.stage_write("change", {"value": "approved"})


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["DISPATCH_INTENT", "FORWARD_COMMITTED"])
async def test_journal_failure_fences_dispatch_or_preserves_uncertain_effect(tmp_path, monkeypatch, kind):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    calls = []
    original = saga.journal.transition

    def failing(record, version, event):
        if event == kind:
            raise JournalError("injected durable write failure")
        return original(record, version, event)

    monkeypatch.setattr(saga.journal, "transition", failing)

    async def execute(args):
        calls.append("applied")
        return {"effect": "persisted elsewhere"}

    with pytest.raises(JournalError):
        await saga.commit_write(action.action_id, execute)
    assert calls == ([] if kind == "DISPATCH_INTENT" else ["applied"])
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN
    assert saga._chain == []
    with pytest.raises(JournalError, match="latched"):
        saga.stage_write("change", {"another": "operation"})
    recovered = manager(saga.journal.path)
    assert recovered._actions[action.action_id].state == ActionState.IN_FLIGHT_UNKNOWN
    with pytest.raises(JournalError):
        await recovered.commit_write(action.action_id, execute)
    assert calls == ([] if kind == "DISPATCH_INTENT" else ["applied"])


def test_staging_failure_leaves_no_usable_action(tmp_path, monkeypatch):
    saga = manager(tmp_path / "journal.db")

    def fail(record):
        raise JournalError("disk unavailable")

    monkeypatch.setattr(saga.journal, "create", fail)
    with pytest.raises(JournalError):
        saga.stage_write("change", {})
    assert saga._actions == {}
    assert saga.journal.records() == []


@pytest.mark.asyncio
async def test_non_json_result_after_apply_cannot_report_commit(tmp_path):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    effects = []

    async def execute(args):
        effects.append("applied")
        return object()

    with pytest.raises(JournalError, match="JSON"):
        await saga.commit_write(action.action_id, execute)
    assert effects == ["applied"]
    assert action.result is not None
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN
    assert saga.journal.records()[0][1]["state"] == "IN_FLIGHT"


@pytest.mark.asyncio
@pytest.mark.parametrize("not_applied", [False, True])
async def test_forward_error_evidence_survives_reopen(tmp_path, not_applied):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})

    async def execute(args):
        if not_applied:
            raise MutationNotAppliedError("atomic simulated rejection", evidence="simulated:rejection")
        raise ConnectionError("reply lost after possible effect")

    with pytest.raises((MutationNotAppliedError, ConnectionError)):
        await saga.commit_write(action.action_id, execute)
    recovered = manager(saga.journal.path)._actions[action.action_id]
    assert recovered.state == (ActionState.ABORTED if not_applied else ActionState.IN_FLIGHT_UNKNOWN)
    assert recovered.not_applied_evidence == ("simulated:rejection" if not_applied else None)
    assert recovered.recovered


@pytest.mark.asyncio
async def test_recovered_success_is_stale_even_at_same_numeric_epoch(tmp_path):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"id": "remote"}))
    recovered = manager(saga.journal.path)
    restored = recovered._actions[action.action_id]
    assert recovered.epoch_clock.current == restored.capture_epoch == 0
    assert restored.state == ActionState.COMMITTED_STALE
    assert restored.compensate is None
    await recovered.reconcile_write(restored.action_id, lambda _: asyncio.sleep(0, result=True))
    assert restored.state == ActionState.COMMITTED_STALE
    assert restored.result == {"id": "remote"}
    with pytest.raises(DuplicateOperationError):
        recovered.stage_write("change", {})


@pytest.mark.asyncio
async def test_authoritative_absence_records_release_and_stale_writer_cannot_finish(tmp_path):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    recovered = manager(saga.journal.path)
    await recovered.reconcile_write(action.action_id, lambda _: asyncio.sleep(0, result=False))
    assert recovered._actions[action.action_id].state == ActionState.ABORTED
    effects = []

    async def execute(args):
        effects.append("must not run")

    with pytest.raises(JournalConflict):
        await saga.commit_write(action.action_id, execute)
    assert effects == []
    fresh = recovered.stage_write("change", {})
    assert fresh.action_id != action.action_id


@pytest.mark.asyncio
async def test_compensation_intent_and_unknown_block_repetition_across_restart(tmp_path):
    saga = manager(tmp_path / "journal.db")
    attempts = []

    async def undo(result):
        events = saga.journal.events(action.action_id)
        assert events[-1]["kind"] == "COMPENSATION_INTENT"
        attempts.append("possibly undone")
        raise ConnectionError("repair reply lost")

    action = saga.stage_write("change", {}, compensate=undo)
    await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={"id": "remote"}))
    await saga.compensate_action(action.action_id)
    assert action.compensation_state == CompensationState.UNKNOWN
    recovered = manager(saga.journal.path)
    restored = recovered._actions[action.action_id]
    restored.compensate = undo  # even explicitly rebinding cannot reset an old attempt
    await recovered.compensate_action(action.action_id)
    assert attempts == ["possibly undone"]
    assert restored.compensation_state == CompensationState.UNKNOWN
    assert restored.compensation_failed
    with pytest.raises(DuplicateOperationError):
        recovered.stage_write("change", {})


@pytest.mark.asyncio
async def test_stale_compensation_cas_never_calls_executor(tmp_path):
    saga = manager(tmp_path / "journal.db")
    calls = []

    async def undo(result):
        calls.append("forbidden")

    action = saga.stage_write("change", {}, compensate=undo)
    await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={}))
    version, record = saga.journal.records()[0]
    saga.journal.transition(record, version, "OTHER_OWNER_OBSERVATION")
    with pytest.raises(JournalConflict):
        await saga.compensate_action(action.action_id)
    assert calls == []


def test_concurrent_connections_cannot_claim_same_operation_key(tmp_path):
    path = tmp_path / "journal.db"
    sagas = [manager(path), manager(path)]
    barrier = threading.Barrier(2, timeout=5)

    def stage(saga):
        barrier.wait()
        try:
            return saga.stage_write("change", {}).action_id
        except DuplicateOperationError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(stage, sagas))
    assert sum(result is not None for result in results) == 1
    assert len(SQLiteActionJournal(path).records()) == 1


def test_event_failure_rolls_back_action_and_key_release(tmp_path):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    with closing(sqlite3.connect(saga.journal.path)) as db:
        db.execute("CREATE TRIGGER reject_event BEFORE INSERT ON events WHEN NEW.kind='ABORTED_BEFORE_DISPATCH' BEGIN SELECT RAISE(ABORT,'injected'); END")
        db.commit()
    with pytest.raises(JournalError):
        saga.abort_write(action.action_id)
    version, record = saga.journal.records()[0]
    assert version == 0 and record["state"] == "PENDING"
    assert len(saga.journal.events(action.action_id)) == 1
    with pytest.raises(DuplicateOperationError):
        manager(saga.journal.path).stage_write("change", {})


@pytest.mark.parametrize("path", [":memory:", "", "file:volatile?mode=memory"])
def test_volatile_database_configuration_rejected(path):
    with pytest.raises(JournalError):
        SQLiteActionJournal(path)


@pytest.mark.parametrize("configuration", ["future", "foreign", "wal", "corrupt", "incomplete"])
def test_unsupported_database_never_silently_reinitialized(tmp_path, configuration):
    path = tmp_path / "journal.db"
    if configuration == "corrupt":
        path.write_bytes(b"not a SQLite database")
    else:
        if configuration == "incomplete":
            SQLiteActionJournal(path)
        with closing(sqlite3.connect(path)) as db:
            if configuration == "future":
                db.execute("PRAGMA user_version=999")
            elif configuration == "foreign":
                db.execute("CREATE TABLE unrelated (value TEXT)")
            elif configuration == "wal":
                db.execute("PRAGMA journal_mode=WAL")
            else:
                db.execute("DROP TABLE task_events")
            db.commit()
    with pytest.raises(JournalError):
        SQLiteActionJournal(path)


@pytest.mark.asyncio
async def test_task_restore_recovers_intent_newer_than_its_checkpoint(tmp_path):
    saga = manager(tmp_path / "journal.db")
    calls = []

    async def execute(args):
        calls.append("must not replay")

    tasks = Tasks(saga, execute)
    steps = [TaskStep("change")]
    task = tasks.create_task("durable task", steps)
    # Crash window: intent exists, but task projection still has no action ID.
    action = saga.stage_write("change", {}, owner_task_id=task.task_id, owner_step_index=0)
    recovered = manager(saga.journal.path)
    restored_tasks = Tasks(recovered, execute)
    restored = restored_tasks.restore_task(task.task_id, steps=steps)
    assert restored.step_action_ids == [action.action_id]
    assert restored.state == TaskState.NEEDS_RECONCILIATION
    await restored_tasks.resume_task(task.task_id)
    assert restored.state == TaskState.NEEDS_RECONCILIATION
    assert calls == []


@pytest.mark.asyncio
async def test_completed_checkpoint_cannot_unlock_recovered_task(tmp_path):
    saga = manager(tmp_path / "journal.db")
    tasks = Tasks(saga, lambda _: asyncio.sleep(0, result={"id": "simulated"}))
    steps = [TaskStep("change")]
    task = tasks.create_task("completed projection", steps)
    await tasks.run_task(task.task_id)
    assert task.state == TaskState.COMPLETED
    recovered = manager(saga.journal.path)
    restored_tasks = Tasks(recovered, lambda _: pytest.fail("replayed"))
    restored = restored_tasks.restore_task(task.task_id, steps=steps)
    assert restored.state == TaskState.NEEDS_RECONCILIATION
    assert restored.current_step_index == 0
    assert restored.step_states == [StepState.NEEDS_RECONCILIATION]
    with pytest.raises(JournalError):
        restored_tasks.restore_checkpoint(task.task_id, tasks.checkpoint(task.task_id))


@pytest.mark.asyncio
async def test_task_checkpoint_failure_stops_dispatch_without_compensating(tmp_path, monkeypatch):
    saga = manager(tmp_path / "journal.db")
    effects, undos = [], []

    async def execute(args):
        effects.append(args)
        return {"id": "applied"}

    async def undo(result):
        undos.append(result)

    tasks = Tasks(saga, execute)
    task = tasks.create_task("checkpoint fault", [TaskStep("change", compensate=undo)])

    def fail(*args):
        raise JournalError("task checkpoint disk error")

    monkeypatch.setattr(saga.journal, "save_task", fail)
    with pytest.raises(JournalError):
        await tasks.run_task(task.task_id)
    assert len(effects) == 1
    assert undos == []
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert saga.journal.records()[0][1]["state"] == "COMMITTED"


def test_task_checkpoint_cas_and_definition_validation(tmp_path):
    saga = manager(tmp_path / "journal.db")
    tasks = Tasks(saga, None)
    task = tasks.create_task("projection", [TaskStep("change")])
    first, second = SQLiteCheckpointStore(saga.journal), SQLiteCheckpointStore(saga.journal)
    a, b = first.load(task.task_id), second.load(task.task_id)
    first.save(task.task_id, a)
    with pytest.raises(JournalConflict):
        second.save(task.task_id, b)
    restored = Tasks(manager(saga.journal.path), None)
    with pytest.raises(JournalError, match="definitions"):
        restored.restore_task(task.task_id, steps=[TaskStep("different")])


@pytest.mark.parametrize("window,forward,undo", [
    ("staged", 0, 0), ("intent", 0, 0), ("applied", 1, 0), ("committed", 1, 0),
    ("compensation_intent", 1, 0), ("compensation_applied", 1, 1), ("compensated", 1, 1),
])
@pytest.mark.asyncio
async def test_process_exit_windows_preserve_evidence_and_never_replay(tmp_path, window, forward, undo):
    path, ledger = tmp_path / "journal.db", tmp_path / "provider.jsonl"
    worker = Path(__file__).with_name("journal_crash_worker.py")
    result = subprocess.run([sys.executable, str(worker), str(path), str(ledger), window],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 23, result.stderr
    effects = [json.loads(line) for line in ledger.read_text().splitlines()] if ledger.exists() else []
    assert effects.count("forward") == forward
    assert effects.count("undo") == undo
    recovered = manager(path)
    action = next(iter(recovered._actions.values()))
    assert action.recovered and action.compensate is None
    if window in ("staged", "intent", "applied"):
        assert action.state == ActionState.IN_FLIGHT_UNKNOWN
    elif window == "compensated":
        assert action.state == ActionState.COMPENSATED
    else:
        assert action.state == ActionState.COMMITTED_STALE
    if window in ("compensation_intent", "compensation_applied"):
        assert action.compensation_state == CompensationState.UNKNOWN
    with pytest.raises(JournalError, match="Recovered"):
        await recovered.commit_write(action.action_id, lambda _: pytest.fail("replayed"))
    if window != "compensated":
        with pytest.raises(DuplicateOperationError):
            recovered.stage_write("change", {})
    assert (ledger.read_text().splitlines() if ledger.exists() else []) == [json.dumps(e) for e in effects]


@pytest.mark.asyncio
async def test_database_lock_fails_before_dispatch_without_retry(tmp_path):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    calls = []
    with closing(sqlite3.connect(saga.journal.path, isolation_level=None)) as locked:
        locked.execute("BEGIN IMMEDIATE")
        with pytest.raises(JournalError):
            await saga.commit_write(action.action_id, lambda args: calls.append(args))
        locked.execute("ROLLBACK")
    assert calls == []
    assert saga.journal.records()[0][1]["state"] == "PENDING"


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_epoch_record", [False, True])
async def test_epoch_advance_during_executor_never_records_current_commit(tmp_path, monkeypatch, fail_epoch_record):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    started, release = asyncio.Event(), asyncio.Event()
    original = saga.journal.transition

    def transition(record, version, event):
        if event == "TURN_SUPERSEDED" and fail_epoch_record:
            raise JournalError("epoch journal fault")
        return original(record, version, event)

    monkeypatch.setattr(saga.journal, "transition", transition)

    async def execute(args):
        started.set()
        await release.wait()
        return {"id": "applied-during-correction"}

    committing = asyncio.create_task(saga.commit_write(action.action_id, execute))
    await asyncio.wait_for(started.wait(), 1)
    await saga.epoch_clock.advance()
    release.set()
    if fail_epoch_record:
        with pytest.raises(JournalError):
            await committing
        assert action.state == ActionState.IN_FLIGHT_UNKNOWN
    else:
        await committing
        assert action.state == ActionState.COMMITTED_STALE
        assert [e["kind"] for e in saga.journal.events(action.action_id)] == [
            "STAGED", "DISPATCH_INTENT", "TURN_SUPERSEDED", "FORWARD_COMMITTED_STALE"]
    assert action.result == {"id": "applied-during-correction"}
    assert saga._chain == []
    assert not any(e["record"]["state"] == "COMMITTED" for e in saga.journal.events(action.action_id))


@pytest.mark.asyncio
async def test_cancellation_after_provider_apply_persists_unknown(tmp_path):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    applied = asyncio.Event()

    async def execute(args):
        applied.set()
        await asyncio.Event().wait()

    committing = asyncio.create_task(saga.commit_write(action.action_id, execute))
    await asyncio.wait_for(applied.wait(), 1)
    committing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await committing
    assert saga.journal.events(action.action_id)[-1]["record"]["state"] == "IN_FLIGHT_UNKNOWN"
    assert manager(saga.journal.path)._actions[action.action_id].state == ActionState.IN_FLIGHT_UNKNOWN


def test_failed_stage_event_does_not_leak_key_or_action(tmp_path):
    saga = manager(tmp_path / "journal.db")
    with closing(sqlite3.connect(saga.journal.path)) as db:
        db.execute("CREATE TRIGGER reject_stage BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'injected'); END")
        db.commit()
    with pytest.raises(JournalError):
        saga.stage_write("change", {})
    assert saga.journal.records() == []
    with closing(sqlite3.connect(saga.journal.path)) as db:
        db.execute("DROP TRIGGER reject_stage")
        db.commit()
    action = manager(saga.journal.path).stage_write("change", {})
    assert action.state == ActionState.PENDING


def test_simulation_cannot_silently_override_configured_journal(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    with pytest.raises(ValueError):
        SpeculativeSagaManager(TurnEpochClock(), journal=journal, simulation=True)
    with pytest.raises(ValueError):
        SpeculativeSagaManager(TurnEpochClock(), simulation="true")


@pytest.mark.asyncio
async def test_duplicate_durable_key_parks_task_without_unwinding_prior_effect(tmp_path):
    saga = manager(tmp_path / "journal.db")
    saga.stage_write("change", {"key": "occupied"})
    effects, undos = [], []

    async def execute(args):
        effects.append(args)
        return {"id": "prior-authorized-effect"}

    async def undo(result):
        undos.append(result)

    tasks = Tasks(saga, execute)
    task = tasks.create_task("blocked second operation", [
        TaskStep("change", build_args=lambda task, idx: {"key": "prior"}, compensate=undo),
        TaskStep("change", build_args=lambda task, idx: {"key": "occupied"}),
    ])
    await tasks.run_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert task.step_states[0] == StepState.COMMITTED
    assert effects == [{"key": "prior"}]
    assert undos == []


@pytest.mark.parametrize("field,value", [("step_states", []), ("step_results", []), ("current_step_index", True)])
def test_invalid_task_projection_shape_fails_restore(tmp_path, field, value):
    saga = manager(tmp_path / "journal.db")
    tasks = Tasks(saga, None)
    task = tasks.create_task("shape validation", [TaskStep("change")])
    version, checkpoint = saga.journal.load_task(task.task_id)
    checkpoint[field] = value
    saga.journal.save_task(task.task_id, checkpoint, version)
    with pytest.raises(JournalError, match="shape"):
        Tasks(manager(saga.journal.path), None).restore_task(task.task_id, steps=[TaskStep("change")])


def test_immutable_identity_and_state_regression_rejected_atomically(tmp_path):
    saga = manager(tmp_path / "journal.db")
    action = saga.stage_write("change", {})
    version, record = saga.journal.records()[0]
    altered = copy.deepcopy(record)
    altered["capture_epoch"] = False  # Python equality alone equates this with epoch 0.
    with pytest.raises(JournalError, match="immutable"):
        saga.journal.transition(altered, version, "INVALID_IDENTITY")
    saga.abort_write(action.action_id)
    version, record = saga.journal.records()[0]
    record["state"] = "PENDING"
    with pytest.raises(JournalError, match="transition"):
        saga.journal.transition(record, version, "RESET_TERMINAL")
    assert saga.journal.records()[0][1]["state"] == "ABORTED"
    assert len(saga.journal.events(action.action_id)) == 2
