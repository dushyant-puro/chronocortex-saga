"""Durable workflow compatibility is checked before attaching executable handlers."""
from dataclasses import replace
import asyncio
from contextlib import closing
import copy
import hashlib
import json
import sqlite3
from pathlib import Path
import subprocess
import sys

import pytest

from runtime.action_journal import JournalError
from runtime.task_engine import TaskManager, TaskStep, TaskState, WorkflowMismatchError
from runtime.speculative_saga import ActionState
from runtime.action_journal import SQLiteActionJournal
from runtime.workflow_contract import WorkflowVersion, StepVersions, canonical
from tests.test_task_operation_ownership import saga_at
from tests.workflow_test_support import TEST_WORKFLOW, versioned_test_steps


@pytest.mark.parametrize("change", ["fields", "builder", "compensation", "status"])
def test_same_tool_name_is_not_a_workflow_compatibility_proof(tmp_path, change):
    saga = saga_at(tmp_path / "journal.db")
    original = versioned_test_steps([TaskStep("change")], saga.registry)[0]
    tasks = TaskManager(saga, lambda _: None)
    task = tasks.create_task("same tool, changed semantics", [original], workflow=TEST_WORKFLOW)
    altered = replace(original)
    if change == "fields":
        altered.required_fields = ["new-required-field"]
    elif change == "builder":
        altered.build_args = lambda t, i: {"different": "intent"}
    elif change == "compensation":
        async def undo(result):
            pytest.fail("incompatible repair attached")
        altered.compensate = undo
    else:
        async def status(action):
            pytest.fail("incompatible status checker attached")
        altered.status_check = status
    restored_tasks = TaskManager(saga_at(saga.journal.path), lambda _: None)
    with pytest.raises(JournalError, match="workflow"):
        restored_tasks.restore_task(task.task_id, steps=[altered], workflow=TEST_WORKFLOW)
    assert restored_tasks.get_task(task.task_id) is None


class Tasks(TaskManager):
    def __init__(self, manager, execute=None):
        self.execute = execute or (lambda args: asyncio.sleep(0, result=dict(args)))
        super().__init__(manager, lambda _: None)

    def _get_executor(self, step):
        return self.execute


def fixture(path, **step_kwargs):
    manager = saga_at(path)
    step = versioned_test_steps([TaskStep("change", **step_kwargs)], manager.registry)[0]
    tasks = Tasks(manager)
    task = tasks.create_task("SIMULATED workflow", [step], workflow=TEST_WORKFLOW)
    return manager, tasks, task, step


@pytest.mark.parametrize("revision", [None, True, 1, "", " ", " v1", "x" * 257])
def test_invalid_workflow_revision_never_creates_task_checkpoint(tmp_path, revision):
    manager = saga_at(tmp_path / "journal.db")
    tasks = Tasks(manager)
    steps = versioned_test_steps([TaskStep("change")], manager.registry)
    with pytest.raises(WorkflowMismatchError):
        tasks.create_task("invalid", steps, workflow=WorkflowVersion("SIMULATED:test", revision))
    assert tasks.snapshot() == []
    with closing(sqlite3.connect(manager.journal.path)) as db:
        assert db.execute("SELECT count(*) FROM task_checkpoints").fetchone()[0] == 0


def test_explicit_workflow_and_step_versions_required_only_for_durable_tasks(tmp_path):
    manager = saga_at(tmp_path / "journal.db")
    for workflow in (None, TEST_WORKFLOW):
        with pytest.raises(WorkflowMismatchError):
            Tasks(manager).create_task("unversioned", [TaskStep("change")], workflow=workflow)
    simulated = Tasks(saga_at())
    assert simulated.create_task("legacy simulated", [TaskStep("change")]).workflow is None


@pytest.mark.parametrize("revision_field", ["step_id", "arguments", "executor", "provider", "provider_scope", "compensation", "reconciliation", "validator"])
def test_changed_semantic_revision_rejected_before_restored_task_is_published(tmp_path, revision_field):
    async def undo(result):
        pytest.fail("repair must not run")

    async def status(action):
        pytest.fail("status must not run")

    manager = saga_at(tmp_path / "journal.db")
    manager.registry.register(replace(manager.registry.get("change"), validate_args=lambda args: None))
    step = versioned_test_steps([TaskStep("change", compensate=undo, status_check=status)], manager.registry)[0]
    tasks = Tasks(manager)
    task = tasks.create_task("pinned", [step], workflow=TEST_WORKFLOW)
    changed = replace(step, versions=replace(step.versions, **{revision_field: "SIMULATED:changed-v2"}))
    replacement = Tasks(manager)
    before = manager.journal.load_task(task.task_id)
    with pytest.raises(WorkflowMismatchError):
        replacement.restore_task(task.task_id, steps=[changed], workflow=TEST_WORKFLOW)
    assert replacement.get_task(task.task_id) is None
    assert manager.journal.load_task(task.task_id) == before


@pytest.mark.parametrize("workflow", [WorkflowVersion("different", "v1"), WorkflowVersion("SIMULATED:boundary-fixture", "v2")])
def test_workflow_identity_and_revision_must_match(tmp_path, workflow):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    with pytest.raises(WorkflowMismatchError):
        Tasks(manager).restore_task(task.task_id, [step], workflow=workflow)


@pytest.mark.parametrize("field,value", [
    ("timeout", 8.0), ("required_permissions", ["new-permission"]),
    ("requires_confirmation", True), ("idempotent", False),
    ("cancellable", False), ("requires_authoritative_commit", True),
    ("depends_on", ["different-prerequisite"]),
])
def test_registered_policy_and_provider_metadata_are_part_of_contract(tmp_path, field, value):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    manager.registry.register(replace(manager.registry.get("change"), **{field: value}))
    with pytest.raises(WorkflowMismatchError):
        Tasks(manager).restore_task(task.task_id, [step], workflow=TEST_WORKFLOW)


@pytest.mark.parametrize("operation", ["run", "replan", "dispatch", "repair", "accept"])
@pytest.mark.asyncio
async def test_same_process_handler_swap_is_fenced_even_with_unchanged_revision(tmp_path, operation):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    original_contract = tasks.checkpoint(task.task_id)["workflow_contract"]
    step.build_args = lambda t, i: pytest.fail("changed builder ran")
    with pytest.raises(WorkflowMismatchError):
        if operation == "run":
            await tasks.run_task(task.task_id)
        elif operation == "replan":
            await tasks._replan_from_step(task, 0)
        elif operation == "dispatch":
            await tasks._dispatch_write_step(task, 0, step, {})
        elif operation == "repair":
            await tasks._compensate_step(task, 0)
        else:
            tasks._accept_step_result(task, 0)
    assert manager.journal.records() == []
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert manager.journal.load_task(task.task_id)[1]["workflow_contract"] == original_contract


@pytest.mark.asyncio
async def test_same_presence_callback_replacement_with_same_revision_is_fenced(tmp_path):
    manager, tasks, task, step = fixture(tmp_path / "journal.db", build_args=lambda t, i: {"value": "old"})
    step.build_args = lambda t, i: pytest.fail("replacement with same revision executed")
    with pytest.raises(WorkflowMismatchError):
        await tasks.run_task(task.task_id)
    assert manager.journal.records() == []


@pytest.mark.asyncio
async def test_contract_change_during_forward_await_parks_without_undo_or_descendant(tmp_path):
    manager = saga_at(tmp_path / "journal.db")
    calls = []
    started, release = asyncio.Event(), asyncio.Event()

    async def undo(result):
        calls.append("forbidden undo")

    steps = versioned_test_steps([TaskStep("change", compensate=undo), TaskStep("change")], manager.registry)

    async def execute(args):
        calls.append("SIMULATED forward applied")
        started.set()
        await release.wait()
        return {"id": "SIMULATED:applied"}

    tasks = Tasks(manager, execute)
    task = tasks.create_task("two steps", steps, workflow=TEST_WORKFLOW)
    original = tasks.checkpoint(task.task_id)["workflow_contract"]
    running = asyncio.create_task(tasks.run_task(task.task_id))
    await asyncio.wait_for(started.wait(), 1)
    steps[1].versions = replace(steps[1].versions, executor="SIMULATED:changed")
    release.set()
    with pytest.raises(WorkflowMismatchError):
        await running
    assert calls == ["SIMULATED forward applied"]
    assert task.state == TaskState.NEEDS_RECONCILIATION and task.current_step_index == 0
    assert manager.journal.records()[0][1]["state"] == "COMMITTED"
    assert manager.journal.load_task(task.task_id)[1]["workflow_contract"] == original


@pytest.mark.asyncio
async def test_status_result_cannot_authorize_repair_after_contract_changed_during_query(tmp_path):
    task_ref = []

    async def status(action):
        task_ref[0].steps[0].versions = replace(task_ref[0].steps[0].versions, provider="changed")
        return True

    manager, tasks, task, step = fixture(tmp_path / "journal.db", status_check=status)

    async def uncertain(args):
        raise ConnectionError("SIMULATED outcome lost")

    tasks.execute = uncertain
    await tasks.run_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    task_ref.append(task)
    await tasks.resume_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert manager.journal.records()[0][1]["state"] == "IN_FLIGHT_UNKNOWN"


@pytest.mark.parametrize("alteration", ["remove", "revision", "definitions"])
def test_store_refuses_contract_change_atomically_even_with_correct_checkpoint_cas(tmp_path, alteration):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    version, checkpoint = manager.journal.load_task(task.task_id)
    changed = copy.deepcopy(checkpoint)
    if alteration == "remove":
        changed.pop("workflow_contract")
    elif alteration == "definitions":
        changed["step_definitions"][0]["tool_name"] = "different"
    else:
        changed["workflow_contract"]["revision"] = "changed"
        body = {k: v for k, v in changed["workflow_contract"].items() if k != "digest"}
        changed["workflow_contract"]["digest"] = hashlib.sha256(canonical(body).encode()).hexdigest()
    before_events = manager.journal.worker_events()
    with pytest.raises(JournalError, match="workflow"):
        manager.journal.save_task(task.task_id, changed, version)
    assert manager.journal.load_task(task.task_id) == (version, checkpoint)
    assert manager.journal.worker_events() == before_events


@pytest.mark.parametrize("corruption", ["digest", "schema", "legacy"])
def test_corrupt_or_unversioned_contract_is_readable_but_not_executable(tmp_path, corruption):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    version, checkpoint = manager.journal.load_task(task.task_id)
    if corruption == "legacy":
        checkpoint.pop("workflow_contract")
    elif corruption == "schema":
        checkpoint["workflow_contract"]["schema"] = True
    else:
        checkpoint["workflow_contract"]["digest"] = "0" * 64
    with closing(sqlite3.connect(manager.journal.path)) as db:
        db.execute("UPDATE task_checkpoints SET checkpoint=? WHERE task_id=?", (json.dumps(checkpoint), task.task_id))
        db.commit()
    assert manager.journal.load_task(task.task_id) == (version, checkpoint)
    replacement = Tasks(manager)
    with pytest.raises(WorkflowMismatchError):
        replacement.restore_task(task.task_id, [step], workflow=TEST_WORKFLOW)
    assert replacement.snapshot() == []


@pytest.mark.asyncio
async def test_exact_contract_survives_restart_and_pending_task_executes_once(tmp_path):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    recovered = saga_at(manager.journal.path)
    replacement = Tasks(recovered)
    restored = replacement.restore_task(task.task_id, [replace(step)], workflow=TEST_WORKFLOW)
    assert replacement.restore_task(task.task_id, [replace(step)], workflow=TEST_WORKFLOW) is restored
    await replacement.run_task(task.task_id)
    assert restored.state == TaskState.COMPLETED
    assert len(recovered.journal.records()) == 1
    assert tasks.checkpoint(task.task_id)["workflow_contract"] == replacement.checkpoint(task.task_id)["workflow_contract"]


def test_cached_restore_does_not_bypass_definition_check(tmp_path):
    manager, tasks, task, step = fixture(tmp_path / "journal.db", build_args=lambda t, i: {})
    changed = replace(step, build_args=lambda t, i: pytest.fail("cached rebind"))
    with pytest.raises(WorkflowMismatchError, match="rebound"):
        tasks.restore_task(task.task_id, [changed], workflow=TEST_WORKFLOW)
    assert task.steps[0].build_args is step.build_args


def test_checkpoint_copy_cannot_mutate_sealed_contract(tmp_path):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    copy_record = tasks.checkpoint(task.task_id)
    digest = copy_record["workflow_contract"]["digest"]
    copy_record["workflow_contract"]["steps"][0]["versions"]["executor"] = "changed"
    assert tasks.checkpoint(task.task_id)["workflow_contract"]["digest"] == digest
    assert tasks.checkpoint(task.task_id)["workflow_contract"] != copy_record["workflow_contract"]


def test_step_order_and_unique_step_ids_are_required(tmp_path):
    manager = saga_at(tmp_path / "journal.db")
    steps = versioned_test_steps([TaskStep("change"), TaskStep("change")], manager.registry)
    tasks = Tasks(manager)
    task = tasks.create_task("ordered", steps, workflow=TEST_WORKFLOW)
    with pytest.raises(WorkflowMismatchError):
        Tasks(manager).restore_task(task.task_id, list(reversed(steps)), workflow=TEST_WORKFLOW)
    with pytest.raises(WorkflowMismatchError, match="Duplicate"):
        tasks.create_task("duplicate", [steps[0], steps[0]], workflow=TEST_WORKFLOW)


def test_schema3_upgrade_preserves_evidence_and_fences_old_binary_format(tmp_path):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    manager.journal.release_lease()
    before = manager.journal.load_task(task.task_id)
    with closing(sqlite3.connect(manager.journal.path)) as db:
        db.execute("PRAGMA user_version=3")
        db.commit()
    with pytest.raises(JournalError, match="migration"):
        SQLiteActionJournal(manager.journal.path)
    migrated = SQLiteActionJournal(manager.journal.path, migrate=True)
    assert migrated.load_task(task.task_id) == before
    with closing(sqlite3.connect(manager.journal.path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 4
    with pytest.raises(JournalError, match="schema changed"):
        # A running schema-4 client also fails closed if its header changes.
        with closing(sqlite3.connect(manager.journal.path)) as db:
            db.execute("PRAGMA user_version=3")
            db.commit()
        migrated.save_task(task.task_id, before[1], before[0])


def test_schema3_upgrade_refuses_live_lease(tmp_path):
    path = tmp_path / "journal.db"
    journal = SQLiteActionJournal(path, clock_ms=lambda: 1000)
    journal.acquire_lease("SIMULATED worker", ttl_ms=100)
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA user_version=3")
        db.commit()
    with pytest.raises(JournalError, match="expired/released"):
        SQLiteActionJournal(path, migrate=True, clock_ms=lambda: 1000)
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3


@pytest.mark.asyncio
async def test_cancellation_records_intent_but_cannot_invoke_changed_repair(tmp_path):
    manager = saga_at(tmp_path / "journal.db")
    calls = []

    async def undo(result):
        calls.append("forbidden original undo")

    async def changed_undo(result):
        calls.append("forbidden replacement undo")

    steps = versioned_test_steps([TaskStep("change", compensate=undo),
                                 TaskStep("change", required_fields=["hold"])], manager.registry)
    tasks = Tasks(manager)
    task = tasks.create_task("cancel changed workflow", steps, workflow=TEST_WORKFLOW)
    await tasks.run_task(task.task_id)
    assert task.state == TaskState.WAITING_FOR_GROUNDING
    steps[0].compensate = changed_undo
    with pytest.raises(WorkflowMismatchError):
        await tasks.cancel_task(task.task_id)
    assert calls == []
    assert task.state == TaskState.CANCELLED
    assert manager.journal.load_task(task.task_id)[1]["state"] == "CANCELLED"
    assert manager.journal.records()[0][1]["state"] == "COMMITTED"


@pytest.mark.asyncio
async def test_contract_change_during_compensation_cannot_advance_generation(tmp_path):
    calls, task_ref = [], []

    async def undo(result):
        calls.append("SIMULATED undo applied")
        step = task_ref[0].steps[0]
        step.versions = replace(step.versions, provider="changed")

    manager, tasks, task, step = fixture(tmp_path / "journal.db", compensate=undo)
    task_ref.append(task)
    await tasks.run_task(task.task_id)
    with pytest.raises(WorkflowMismatchError):
        await tasks._replan_from_step(task, 0)
    assert calls == ["SIMULATED undo applied"]
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert task.step_generations == [0]
    assert manager.journal.records()[0][1]["state"] == "COMPENSATED"


@pytest.mark.asyncio
async def test_epoch_triggered_background_repair_cannot_bypass_workflow_guard(tmp_path):
    calls = []
    manager = saga_at(tmp_path / "journal.db")

    async def undo(result):
        calls.append("forbidden automatic undo")

    steps = versioned_test_steps([TaskStep("change", compensate=undo)], manager.registry)

    async def execute(args):
        calls.append("SIMULATED forward applied")
        steps[0].versions = replace(steps[0].versions, compensation="changed")
        await manager.epoch_clock.advance()
        return {"id": "SIMULATED:applied"}

    tasks = Tasks(manager, execute)
    task = tasks.create_task("correction during version change", steps, workflow=TEST_WORKFLOW)
    with pytest.raises(WorkflowMismatchError):
        await tasks.run_task(task.task_id)
    action = next(iter(manager._actions.values()))
    await action._compensation_task
    assert calls == ["SIMULATED forward applied"]
    assert action.state == ActionState.COMMITTED_STALE
    assert action.compensation_error is not None
    assert manager.journal.records()[0][1]["compensation_state"] == "NOT_STARTED"
    assert "COMPENSATION_INTENT" not in [e["kind"] for e in manager.journal.events(action.action_id)]


@pytest.mark.asyncio
async def test_manifest_list_mutation_is_detected_without_invoking_executor(tmp_path):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    manager.registry.get("change").required_permissions.append("changed-permission")
    with pytest.raises(WorkflowMismatchError):
        await tasks.run_task(task.task_id)
    assert manager.journal.records() == []


def test_legacy_checkpoint_cannot_be_implicitly_adopted_by_new_contract(tmp_path):
    manager, tasks, task, step = fixture(tmp_path / "journal.db")
    version, checkpoint = manager.journal.load_task(task.task_id)
    legacy = copy.deepcopy(checkpoint)
    legacy.pop("workflow_contract")
    with closing(sqlite3.connect(manager.journal.path)) as db:
        db.execute("UPDATE task_checkpoints SET checkpoint=? WHERE task_id=?", (json.dumps(legacy), task.task_id))
        db.commit()
    with pytest.raises(JournalError, match="immutable"):
        manager.journal.save_task(task.task_id, checkpoint, version)
    assert manager.journal.load_task(task.task_id) == (version, legacy)


@pytest.mark.asyncio
async def test_process_exit_preserves_contract_and_rejects_changed_restart_deployment(tmp_path):
    path, ledger = tmp_path / "journal.db", tmp_path / "effects.jsonl"
    worker = Path(__file__).with_name("workflow_crash_worker.py")
    process = subprocess.run([sys.executable, str(worker), str(path), str(ledger)],
                             capture_output=True, text=True, timeout=15)
    assert process.returncode == 23, process.stderr
    manager = saga_at(path)
    task_id = manager.journal.records()[0][1]["task_id"]
    workflow = WorkflowVersion("SIMULATED:crash-contract", "v1")
    step = TaskStep("change", versions=StepVersions(
        "change", "empty:v1", "execute:v1", "provider:v1", "SIMULATED:local-test"))
    changed = replace(step, versions=replace(step.versions, executor="execute:v2"))
    tasks = Tasks(manager, lambda _: pytest.fail("unknown forward replayed"))
    with pytest.raises(WorkflowMismatchError):
        tasks.restore_task(task_id, [changed], workflow=workflow)
    assert tasks.snapshot() == []
    restored = tasks.restore_task(task_id, [step], workflow=workflow)
    await tasks.resume_task(task_id)
    assert restored.state == TaskState.NEEDS_RECONCILIATION
    assert ledger.read_text().splitlines() == ['"SIMULATED forward"']
    assert manager.journal.records()[0][1]["state"] == "IN_FLIGHT"
