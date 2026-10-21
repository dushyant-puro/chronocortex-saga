"""Real production dispatch with explicit simulation or a file-backed journal."""
import asyncio
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from runtime.action_journal import JournalError, JournalConflict, JournalDuplicate, SQLiteActionJournal
from runtime.policy_engine import AuthorizationContext, MutationApproval, ConfirmationRequiredError, payload_digest
from runtime.speculative_saga import (
    ActionState, ActionOwnershipError, CompensationState, DuplicateOperationError,
    MutationNotAppliedError, SpeculativeSagaManager, TurnEpochClock,
)
from runtime.task_engine import TaskManager, TaskStep, TaskState, StepState
from runtime.tool_contract import ToolManifest, ToolRegistry
from tests.lease_test_support import shared_test_journal


def saga_at(path=None):
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write"))
    return SpeculativeSagaManager(TurnEpochClock(), registry=registry,
                                  auth_context=AuthorizationContext(),
                                  journal=shared_test_journal(path) if path else None,
                                  simulation=path is None)


class Tasks(TaskManager):
    def _get_executor(self, step):
        async def execute(args):
            return dict(args)
        return execute


@pytest.mark.asyncio
async def test_unwind_does_not_cross_task_boundary():
    saga = saga_at()
    undos = []

    async def undo_a(result):
        undos.append("A")

    async def undo_b(result):
        undos.append("B")

    a = saga.stage_write("change", {"task": "A"}, compensate=undo_a,
                         owner_task_id="task-A", owner_step_index=0)
    b = saga.stage_write("change", {"task": "B"}, compensate=undo_b,
                         owner_task_id="task-B", owner_step_index=0)
    await saga.commit_write(a.action_id, lambda _: asyncio.sleep(0, result={}))
    await saga.commit_write(b.action_id, lambda _: asyncio.sleep(0, result={}))
    failed = saga.stage_write("change", {"failed": "A"}, owner_task_id="task-A", owner_step_index=1)

    async def reject(args):
        raise MutationNotAppliedError("simulated rejection", evidence="simulated:atomic-rejection")

    with pytest.raises(MutationNotAppliedError):
        await saga.commit_write(failed.action_id, reject)
    await saga.abort_chain_from(failed.action_id)
    assert undos == ["A"]
    assert b.state == ActionState.COMMITTED


@pytest.mark.asyncio
async def test_equal_payloads_in_distinct_durable_tasks_are_distinct_operations(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    tasks = Tasks(saga, lambda _: None)
    a = tasks.create_task("A", [TaskStep("change")])
    b = tasks.create_task("B", [TaskStep("change")])
    await tasks.run_task(a.task_id)
    await tasks.run_task(b.task_id)
    assert a.state == b.state == TaskState.COMPLETED
    first, second = [saga._actions[t.step_action_ids[0]] for t in (a, b)]
    assert first.idempotency_key != second.idempotency_key


@pytest.mark.asyncio
async def test_unknown_failed_action_id_cannot_authorize_global_unwind():
    saga = saga_at()
    undos = []

    async def undo(result):
        undos.append(result)

    action = saga.stage_write("change", {}, compensate=undo)
    await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0, result="keep"))
    try:
        await saga.abort_chain_from("not-a-recorded-action")
    except (ValueError, KeyError):
        pass
    assert undos == []


@pytest.mark.asyncio
async def test_equal_payloads_in_distinct_steps_have_distinct_operation_keys(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    tasks = Tasks(saga, lambda _: None)
    task = tasks.create_task("two operations", [TaskStep("change"), TaskStep("change")])
    await tasks.run_task(task.task_id)
    assert task.state == TaskState.COMPLETED
    assert len({saga._actions[a].idempotency_key for a in task.step_action_ids}) == 2


@pytest.mark.asyncio
async def test_replan_same_payload_creates_next_generation_only_after_compensation(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    undos = []

    async def undo(result):
        undos.append(result)

    tasks = Tasks(saga, lambda _: None)
    task = tasks.create_task("repeat by corrected plan", [TaskStep("change", compensate=undo)])
    await tasks.run_task(task.task_id)
    old = saga._actions[task.step_action_ids[0]]
    await tasks._replan_from_step(task, 0)
    fresh = saga._actions[task.step_action_ids[0]]
    assert task.state == TaskState.COMPLETED
    assert task.step_generations == [1]
    assert old.state == ActionState.COMPENSATED and fresh.state == ActionState.COMMITTED
    assert old.owner_generation == 0 and fresh.owner_generation == 1
    assert old.idempotency_key != fresh.idempotency_key
    assert old.chain_id == fresh.chain_id == f"task:{task.task_id}"
    assert undos == [{}]
    assert saga.journal.load_task(task.task_id)[1]["step_generations"] == [1]


@pytest.mark.asyncio
async def test_pending_argument_revision_invalidates_old_approval(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    saga.registry.register(ToolManifest("change", "write", requires_confirmation=True))
    value = "old"
    tasks = Tasks(saga, lambda _: None)
    task = tasks.create_task("approval", [TaskStep("change", build_args=lambda t, i: {"value": value})])
    with pytest.raises(ConfirmationRequiredError):
        await tasks.run_task(task.task_id)
    old = saga._actions[task.step_action_ids[0]]
    saga.auth_context.approval = MutationApproval.for_action(old)
    value = "new"
    with pytest.raises(ConfirmationRequiredError):
        await tasks.resume_task(task.task_id)
    fresh = saga._actions[task.step_action_ids[0]]
    assert old.state == ActionState.ABORTED and fresh.state == ActionState.PENDING
    assert fresh.owner_generation == 1
    saga.auth_context.approval = MutationApproval.for_action(fresh)
    await tasks.resume_task(task.task_id)
    assert task.step_results == [{"value": "new"}]


@pytest.mark.parametrize("uncertainty", ["forward", "repair"])
@pytest.mark.asyncio
async def test_new_key_or_payload_cannot_bypass_unresolved_generation(tmp_path, uncertainty):
    saga = saga_at(tmp_path / "journal.db")
    action = saga.stage_write("change", {}, owner_task_id="T", owner_step_index=0)
    record = saga._journal_record(action)
    if uncertainty == "forward":

        async def lost(args):
            raise ConnectionError("possibly applied")

        with pytest.raises(ConnectionError):
            await saga.commit_write(action.action_id, lost)
    else:

        async def undo(result):
            raise ConnectionError("possibly undone")

        action.compensate = undo
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0, result={}))
        await saga.compensate_action(action.action_id)
    with pytest.raises(DuplicateOperationError):
        saga.stage_write("change", {"changed": True}, owner_task_id="T", owner_step_index=0,
                         owner_generation=1, idempotency_key="attempted-new-key")
    # Exercise the store guard independently of the manager's cached history.
    record.update(action_id="forbidden-successor", args={"changed": True}, generation=1,
                  idempotency_key="attempted-new-key", state="PENDING", compensation_state="NOT_STARTED")
    with pytest.raises(JournalDuplicate):
        saga.journal.create(record)
    assert len(saga.journal.records()) == 1


def test_resolved_generation_cannot_be_reused_or_skipped(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    action = saga.stage_write("change", {}, owner_task_id="T", owner_step_index=0)
    saga.abort_write(action.action_id)
    record = saga._journal_record(action)
    record.update(action_id="new-action", state="PENDING", idempotency_key="different-key")
    for generation in (0, 2):
        record["generation"] = generation
        with pytest.raises(JournalDuplicate):
            saga.journal.create(record)
    assert len(saga.journal.records()) == 1


@pytest.mark.parametrize("fields", [
    {"owner_step_index": 0}, {"owner_task_id": "T"},
    {"owner_task_id": "T", "owner_step_index": 0, "owner_generation": True},
    {"owner_task_id": "T", "owner_step_index": 0, "owner_generation": -1},
    {"owner_task_id": "T", "owner_step_index": 0, "chain_id": "foreign"},
    {"chain_id": "task:T"},
])
def test_partial_or_forged_ownership_rejected(fields):
    saga = saga_at()
    with pytest.raises(ActionOwnershipError):
        saga.stage_write("change", {}, **fields)
    assert saga._actions == {}


@pytest.mark.parametrize("operation", ["accept", "reconcile", "cancel", "replan", "dispatch"])
@pytest.mark.asyncio
async def test_foreign_action_pointer_never_executes_queries_or_undoes_other_task(operation):
    saga = saga_at()
    calls = []

    async def undo(result):
        calls.append("forbidden undo")

    async def status(action):
        calls.append("forbidden status query")
        return True

    tasks = Tasks(saga, lambda _: None)
    a = tasks.create_task("A", [TaskStep("change", compensate=undo, status_check=status)])
    b = tasks.create_task("B", [TaskStep("change", compensate=undo)])
    foreign = saga.stage_write("change", {}, compensate=undo, owner_task_id=b.task_id, owner_step_index=0)
    if operation != "dispatch":
        await saga.commit_write(foreign.action_id, lambda _: asyncio.sleep(0, result={"private": "B"}))
    a.step_action_ids[0] = foreign.action_id
    a.state = TaskState.RUNNING
    if operation == "accept":
        assert not tasks._accept_step_result(a, 0)
    elif operation == "reconcile":
        a.step_states[0] = StepState.NEEDS_RECONCILIATION
        await tasks.resume_task(a.task_id)
    elif operation in {"cancel", "replan"}:
        a.step_states[0] = StepState.COMMITTED
        a.current_step_index = 1
        if operation == "cancel":
            await tasks.cancel_task(a.task_id)
        else:
            await tasks._replan_from_step(a, 0)
    else:
        with pytest.raises(ActionOwnershipError):
            await tasks._dispatch_write_step(a, 0, a.steps[0], {})
    assert calls == []
    assert foreign.state == (ActionState.PENDING if operation == "dispatch" else ActionState.COMMITTED)
    assert a.step_results == [None]


@pytest.mark.asyncio
async def test_closing_chain_fences_pending_and_late_success_without_automatic_undo():
    saga = saga_at()
    started, release = asyncio.Event(), asyncio.Event()
    undos = []

    async def undo(result):
        undos.append(result)

    async def execute(args):
        started.set()
        await release.wait()
        return {"effect": "applied"}

    action = saga.stage_write("change", {"first": 1}, compensate=undo, chain_id="batch")
    failed = saga.stage_write("change", {"failed": 1}, chain_id="batch")
    pending = saga.stage_write("change", {"pending": 1}, chain_id="batch")
    saga.abort_write(failed.action_id)
    committing = asyncio.create_task(saga.commit_write(action.action_id, execute))
    await asyncio.wait_for(started.wait(), 1)
    assert not await saga.abort_chain_from(failed.action_id)
    with pytest.raises(ActionOwnershipError):
        await saga.commit_write(pending.action_id, lambda _: pytest.fail("late dispatch"))
    release.set()
    await committing
    await asyncio.sleep(0)
    assert action.state == ActionState.COMMITTED_STALE
    assert undos == []
    with pytest.raises(ActionOwnershipError):
        saga.stage_write("change", {"new": 1}, chain_id="batch")


@pytest.mark.asyncio
async def test_other_connection_chain_closure_blocks_old_pending_dispatch(tmp_path):
    path = tmp_path / "journal.db"
    first = saga_at(path)
    failed = first.stage_write("change", {"failed": True}, chain_id="batch")
    first.abort_write(failed.action_id)
    second = saga_at(path)
    pending = second.stage_write("change", {"pending": True}, chain_id="batch")
    assert not await first.abort_chain_from(failed.action_id)  # unseen member: cannot claim unwind
    with pytest.raises(JournalError):
        await second.commit_write(pending.action_id, lambda _: pytest.fail("dispatch after closure"))
    with pytest.raises((JournalError, ActionOwnershipError)):
        saga_at(path).stage_write("change", {"new": True}, chain_id="batch")


@pytest.mark.parametrize("unseen", [False, True])
@pytest.mark.asyncio
async def test_ancestor_repair_waits_for_descendant_even_from_other_connection(tmp_path, unseen):
    saga = saga_at(tmp_path / "journal.db")
    undos = []

    async def undo(result):
        undos.append(result)

    ancestor = saga.stage_write("change", {"ancestor": True}, compensate=undo, chain_id="batch")
    await saga.commit_write(ancestor.action_id, lambda _: asyncio.sleep(0, result={}))
    writer = saga_at(saga.journal.path) if unseen else saga
    writer.stage_write("change", {"descendant": True}, chain_id="batch")
    if unseen:
        with pytest.raises(JournalConflict):
            await saga.compensate_action(ancestor.action_id)
    else:
        await saga.compensate_action(ancestor.action_id)
        assert ancestor.compensation_state == CompensationState.NOT_STARTED
    assert undos == []


@pytest.mark.asyncio
async def test_old_queued_correction_cannot_repair_a_fresh_generation():
    saga = saga_at()
    values, undos = ["old"], []

    async def undo(result):
        undos.append(result)

    tasks = Tasks(saga, lambda _: None)
    task = tasks.create_task("correction", [TaskStep("change", compensate=undo,
                             build_args=lambda t, i: {"destination": values[0]})])
    await tasks.run_task(task.task_id)
    values[0] = "new"
    await tasks._replan_from_step(task, 0, "destination", "old")
    fresh = task.step_action_ids[0]
    await tasks._replan_from_step(task, 0, "destination", "old")
    assert task.step_action_ids[0] == fresh and task.step_generations == [1]
    assert undos == [{"destination": "old"}]


@pytest.mark.asyncio
async def test_voice_batch_does_not_dispatch_descendant_after_unknown(monkeypatch):
    from agent import CCSAgent, PendingSlot
    ccs = CCSAgent(simulation=True, auth_context=AuthorizationContext())
    registry = ToolRegistry()
    for name in ("first", "second"):
        registry.register(ToolManifest(name, "write"))
    ccs.saga.set_tool_registry(registry)
    calls = []

    async def first(args):
        calls.append("first possibly applied")
        raise ConnectionError("lost reply")

    async def second(args):
        calls.append("forbidden descendant")

    monkeypatch.setattr("agent.WRITE_TOOLS", {"first": (first, None), "second": (second, None)})
    monkeypatch.setattr(ccs.guard, "resolve_current_value", lambda field: "grounded")
    ccs._pending_slots = {"x": PendingSlot("x", "x", "first", (0, 1)),
                          "y": PendingSlot("y", "y", "second", (1, 2))}
    await ccs._commit_all_pending_slots()
    assert calls == ["first possibly applied"]


def make_v1_fixture(path, states, owner="legacy-task"):
    """Explicit simulated format fixture matching schema 1, not provider evidence."""
    with closing(sqlite3.connect(path)) as db:
        db.executescript('''
            CREATE TABLE actions (action_id TEXT PRIMARY KEY, version INTEGER NOT NULL, record TEXT NOT NULL);
            CREATE TABLE key_claims (operation_key TEXT PRIMARY KEY, action_id TEXT NOT NULL UNIQUE REFERENCES actions(action_id));
            CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, action_id TEXT NOT NULL REFERENCES actions(action_id), version INTEGER NOT NULL, kind TEXT NOT NULL, record TEXT NOT NULL, created_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), UNIQUE(action_id,version));
            CREATE TABLE task_checkpoints (task_id TEXT PRIMARY KEY, version INTEGER NOT NULL, checkpoint TEXT NOT NULL);
            CREATE TABLE task_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES task_checkpoints(task_id), version INTEGER NOT NULL, checkpoint TEXT NOT NULL, UNIQUE(task_id,version));
            PRAGMA application_id=1128485697;
            PRAGMA user_version=1;
        ''')
        for idx, state in enumerate(states):
            record = {
                "action_id": f"legacy-{idx}", "tool_name": "change", "args": {},
                "digest": payload_digest("change", {}), "idempotency_key": f"provider-key-{idx}",
                "capture_epoch": 0, "session_id": "old-process", "task_id": owner,
                "step_index": 0 if owner else None, "state": state,
                "compensation_state": "SUCCEEDED" if state == "COMPENSATED" else "NOT_STARTED",
                "result": {}, "error_type": None, "compensation_error_type": None,
                "compensation_failed": False, "not_applied_evidence": None,
                "compensation_not_applied_evidence": None, "superseded_epoch": None,
            }
            payload = json.dumps(record)
            db.execute("INSERT INTO actions VALUES (?,0,?)", (record["action_id"], payload))
            db.execute("INSERT INTO events(action_id,version,kind,record) VALUES (?,0,'SIMULATED_V1_FIXTURE',?)", (record["action_id"], payload))
            if state not in {"ABORTED", "COMPENSATED"}:
                db.execute("INSERT INTO key_claims VALUES (?,?)", (record["idempotency_key"], record["action_id"]))
        if owner:
            cp = {"task_id": owner, "name": "legacy", "state": "RUNNING", "current_step_index": 0,
                  "step_states": ["IN_FLIGHT"], "step_results": [None],
                  "step_action_ids": [f"legacy-{len(states)-1}"], "step_dispatched_args": [{}],
                  "step_definitions": [{"tool_name": "change", "kind": "write"}], "error": None}
            payload = json.dumps(cp)
            db.execute("INSERT INTO task_checkpoints VALUES (?,0,?)", (owner, payload))
            db.execute("INSERT INTO task_events(task_id,version,checkpoint) VALUES (?,0,?)", (owner, payload))
        db.commit()


@pytest.mark.parametrize("owner", ["legacy-task", None])
def test_v1_migration_is_explicit_preserves_keys_and_fences_old_versions(tmp_path, owner):
    path = tmp_path / "legacy.db"
    states = ["COMPENSATED", "IN_FLIGHT_UNKNOWN"] if owner else ["COMMITTED"]
    make_v1_fixture(path, states, owner)
    with pytest.raises(JournalError, match="migration"):
        SQLiteActionJournal(path)
    journal = SQLiteActionJournal(path, migrate=True)
    records = journal.records()
    assert [record["idempotency_key"] for _, record in records] == [f"provider-key-{idx}" for idx in range(len(states))]
    assert [version for version, _ in records] == [1] * len(states)
    assert all(journal.events(record["action_id"])[-1]["kind"] == "SCHEMA_V2_IDENTITY" for _, record in records)
    if owner:
        assert [record["generation"] for _, record in records] == [0, 1]
        assert journal.load_task(owner)[1]["step_generations"] == [1]
        saga = saga_at(path)
        restored = Tasks(saga, lambda _: None).restore_task(owner, steps=[TaskStep("change")])
        assert restored.step_generations == [1] and restored.state == TaskState.NEEDS_RECONCILIATION
    else:
        assert records[0][1]["chain_id"] == "legacy:legacy-0"
        assert records[0][1]["generation"] is None
    assert SQLiteActionJournal(path, migrate=True).records() == records  # no second migration


def test_migration_rejects_ambiguous_overlapping_slot_and_rolls_back(tmp_path):
    path = tmp_path / "legacy.db"
    make_v1_fixture(path, ["COMMITTED", "PENDING"])
    with pytest.raises(JournalError, match="overlapping"):
        SQLiteActionJournal(path, migrate=True)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute("SELECT version FROM actions").fetchall() == [(0,), (0,)]
        assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 2
        assert db.execute("SELECT name FROM sqlite_master WHERE name='chains'").fetchone() is None


@pytest.mark.asyncio
async def test_durable_multi_step_repair_preserves_identity_and_reverse_order(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    undos = []

    async def undo(result):
        undos.append(result["step"])

    tasks = Tasks(saga, lambda _: None)
    task = tasks.create_task("reverse repair", [TaskStep("change", compensate=undo,
                             build_args=lambda task, idx: {"step": idx}) for _ in range(2)])
    await tasks.run_task(task.task_id)
    old_ids = list(task.step_action_ids)
    await tasks._replan_from_step(task, 0)
    assert undos == [1, 0]
    assert task.state == TaskState.COMPLETED and task.step_generations == [1, 1]
    records = {record["action_id"]: record for _, record in saga.journal.records()}
    assert all(records[action_id]["state"] == "COMPENSATED" for action_id in old_ids)
    assert all(records[action_id]["action_id"] == action_id for action_id in records)


@pytest.mark.asyncio
async def test_cancellation_closes_its_chain_and_prevents_later_pending_dispatch(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    tasks = Tasks(saga, lambda _: None)
    task = tasks.create_task("cancel before send", [TaskStep("change")])
    pending = saga.stage_write("change", {}, owner_task_id=task.task_id, owner_step_index=0)
    task.step_action_ids[0] = pending.action_id
    await tasks.cancel_task(task.task_id)
    assert pending.state == ActionState.ABORTED
    assert saga.journal.load_task(task.task_id)[1]["state"] == "CANCELLED"
    with pytest.raises(ActionOwnershipError):
        await saga.commit_write(pending.action_id, lambda _: pytest.fail("late dispatch"))
    with pytest.raises((JournalError, ActionOwnershipError)):
        saga_at(saga.journal.path).stage_write("change", {}, owner_task_id=task.task_id,
                                              owner_step_index=0, owner_generation=1)


@pytest.mark.parametrize("window,generation", [("replan_compensated", 0), ("replan_generation", 1)])
@pytest.mark.asyncio
async def test_process_exit_during_replan_never_completes_or_replays(tmp_path, window, generation):
    path, ledger = tmp_path / "journal.db", tmp_path / "provider.jsonl"
    result = subprocess.run([sys.executable, str(Path(__file__).with_name("journal_crash_worker.py")),
                             str(path), str(ledger), window], capture_output=True, text=True, timeout=15)
    assert result.returncode == 23, result.stderr
    assert [json.loads(line) for line in ledger.read_text().splitlines()] == ["forward", "undo"]
    saga = saga_at(path)
    record = saga.journal.records()[0][1]
    tasks = Tasks(saga, lambda _: pytest.fail("replayed"))
    task = tasks.restore_task(record["task_id"], steps=[TaskStep("change")])
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert task.step_generations == [generation]
    await tasks.resume_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert len(ledger.read_text().splitlines()) == 2


def test_store_rejects_boolean_generation_without_creating_a_slot(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    action = saga.stage_write("change", {}, owner_task_id="T", owner_step_index=0)
    candidate = saga._journal_record(action)
    candidate.update(action_id="another", idempotency_key="another", generation=True)
    with pytest.raises(JournalError, match="identity"):
        saga.journal.create(candidate)
    assert len(saga.journal.records()) == 1


def test_chain_close_and_event_roll_back_together(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    saga.stage_write("change", {}, chain_id="batch")
    with closing(sqlite3.connect(saga.journal.path)) as db:
        db.execute("CREATE TRIGGER reject_close BEFORE INSERT ON chain_events BEGIN SELECT RAISE(ABORT,'injected'); END")
        db.commit()
    with pytest.raises(JournalError):
        saga.close_chain("batch")
    with closing(sqlite3.connect(saga.journal.path)) as db:
        assert db.execute("SELECT status FROM chains WHERE chain_id='batch'").fetchone()[0] == "OPEN"
        assert db.execute("SELECT count(*) FROM chain_events").fetchone()[0] == 0
    assert saga._durability_fault is not None


@pytest.mark.asyncio
async def test_descendant_cannot_dispatch_before_its_predecessor(tmp_path):
    saga = saga_at(tmp_path / "journal.db")
    parent = saga.stage_write("change", {"parent": True}, chain_id="batch")
    child = saga.stage_write("change", {"child": True}, chain_id="batch")
    with pytest.raises(DuplicateOperationError):
        await saga.commit_write(child.action_id, lambda _: pytest.fail("out-of-order dispatch"))
    # Independently exercise SQL protection, not only the manager projection.
    child.state = ActionState.IN_FLIGHT
    with pytest.raises(JournalConflict):
        saga.journal.transition(saga._journal_record(child), child._journal_version, "DISPATCH_INTENT")
    child.state = ActionState.PENDING
    await saga.commit_write(parent.action_id, lambda _: asyncio.sleep(0, result={}))
    await saga.commit_write(child.action_id, lambda _: asyncio.sleep(0, result={}))
    assert child.state == ActionState.COMMITTED
