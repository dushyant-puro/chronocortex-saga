"""
tests/test_phase7_persistence.py

Phase 7 — Memory & Persistence test suite.

5 test scenarios:
  7a: Checkpoint written after step commit matches expected shape
  7b: restore_task reconstructs equivalent Task state
  7c: A step IN_FLIGHT at checkpoint time restores as NEEDS_RECONCILIATION
      and advances past reconciliation via saga.reconcile_write
  7d: Restoring the same checkpoint twice is idempotent (no duplicate dispatch)
  7e: Full regression (all existing tests pass)
"""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from runtime.persistence import FileCheckpointStore
from runtime.speculative_saga import (
    ActionState,
    SpeculativeSagaManager,
    TurnEpochClock,
)
from runtime.task_engine import (
    StepState,
    Task,
    TaskManager,
    TaskState,
    TaskStep,
)
def _simple_executor(result: Any):
    """Return an executor that resolves to `result` after a tiny delay."""
    async def _exec(args: dict[str, Any]) -> Any:
        await asyncio.sleep(0.005)
        return result
    return _exec


class _HarnessTaskManager(TaskManager):
    """Subclass of TaskManager for testing with custom executors."""
    def __init__(self, saga, resolve_field, executors=None, read_executors=None, store=None):
        super().__init__(saga, resolve_field, store=store)
        self._test_executors = executors or {}
        self._test_read_executors = read_executors or {}

    async def _dispatch_write_step(self, task, idx, step, args):
        executor = self._test_executors.get(step.tool_name)
        if executor is None:
            raise RuntimeError(f"No test executor for {step.tool_name}")
        action = self._saga.stage_write(
            step.tool_name,
            args,
            compensate=step.compensate,
        )
        task.step_action_ids[idx] = action.action_id
        task.step_states[idx] = StepState.IN_FLIGHT
        result = await self._saga.commit_write(action.action_id, executor)
        return result

    async def _dispatch_read_step(self, task, idx, step, args):
        executor = self._test_read_executors.get(step.tool_name)
        if executor is None:
            raise RuntimeError(f"No test read executor for {step.tool_name}")
        entity_hash = f"task-{task.task_id}-step-{idx}"
        action = self._saga.fire_speculative_read(
            step.tool_name,
            args,
            entity_hash=entity_hash,
            read_executor=executor,
        )
        task.step_action_ids[idx] = action.action_id
        task.step_states[idx] = StepState.IN_FLIGHT
        res = await self._saga.get_current_result(action.action_id)
        return res.value



# ===========================================================================
# Test 7a: Checkpoint written after step commit matches expected shape
# ===========================================================================

@pytest.mark.asyncio
async def test_7a_checkpoint_written_after_step_commit():
    """
    When a step commits, a checkpoint is saved to FileCheckpointStore.
    The JSON structure matches the expected shape.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileCheckpointStore(tmpdir)
        clock = TurnEpochClock()
        saga = SpeculativeSagaManager(clock)

        executors = {
            "step_a": _simple_executor({"a": "result_a"}),
            "step_b": _simple_executor({"b": "result_b"}),
        }

        tm = _HarnessTaskManager(
            saga=saga,
            resolve_field=lambda f: "val",
            executors=executors,
            store=store,
        )

        steps = [
            TaskStep(tool_name="step_a", build_args=lambda t, i: {"arg_a": "1"}, kind="write"),
            TaskStep(tool_name="step_b", build_args=lambda t, i: {"arg_b": "2"}, kind="write"),
        ]

        task = tm.create_task("checkpoint_shape_test", steps)
        result = await tm.run_task(task.task_id)

        assert result.state == TaskState.COMPLETED

        # Check loaded checkpoint from file
        cp = store.load(task.task_id)
        assert cp is not None
        assert cp["task_id"] == task.task_id
        assert cp["name"] == "checkpoint_shape_test"
        assert cp["state"] == "COMPLETED"
        assert cp["current_step_index"] == 2
        assert cp["step_states"] == ["COMMITTED", "COMMITTED"]
        assert cp["step_results"] == [{"a": "result_a"}, {"b": "result_b"}]
        assert cp["step_dispatched_args"] == [{"arg_a": "1"}, {"arg_b": "2"}]
        assert len(cp["step_action_ids"]) == 2
        assert cp["step_action_ids"][0] is not None
        assert cp["error"] is None

        print(f"\n[TEST 7a VERIFICATION] Checkpoint JSON shape verified: {cp}")


# ===========================================================================
# Test 7b: restore_task reconstructs equivalent Task state
# ===========================================================================

@pytest.mark.asyncio
async def test_7b_restore_reconstructs_equivalent_state():
    """
    Simulate process restart:
    1. Create and run task on TaskManager 1 with FileCheckpointStore
    2. Create a fresh TaskManager 2 pointing to the same store
    3. Call restore_task(task_id)
    4. Assert reconstructed Task state matches original Task state
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileCheckpointStore(tmpdir)
        clock1 = TurnEpochClock()
        saga1 = SpeculativeSagaManager(clock1)

        executors1 = {
            "step_a": _simple_executor({"a": "result_a"}),
            "step_b": _simple_executor({"b": "result_b"}),
        }

        tm1 = _HarnessTaskManager(
            saga=saga1,
            resolve_field=lambda f: "val" if f == "field_a" else None,  # step_b will wait
            executors=executors1,
            store=store,
        )

        steps1 = [
            TaskStep(
                tool_name="step_a",
                required_fields=["field_a"],
                build_args=lambda t, i: {"arg_a": "1"},
                kind="write",
            ),
            TaskStep(
                tool_name="step_b",
                required_fields=["field_b"],  # unresolvable
                build_args=lambda t, i: {"arg_b": "2"},
                kind="write",
            ),
        ]

        task1 = tm1.create_task("restore_test", steps1)
        res1 = await tm1.run_task(task1.task_id)
        assert res1.state == TaskState.WAITING_FOR_GROUNDING
        task_id = task1.task_id

        # Fresh process / fresh TaskManager 2 with fresh saga and same store
        clock2 = TurnEpochClock()
        saga2 = SpeculativeSagaManager(clock2)
        executors2 = {
            "step_a": _simple_executor({"a": "result_a"}),
            "step_b": _simple_executor({"b": "result_b"}),
        }

        tm2 = _HarnessTaskManager(
            saga=saga2,
            resolve_field=lambda f: "val" if f == "field_a" else None,
            executors=executors2,
            store=store,
        )

        # Re-create step definitions for process 2
        steps2 = [
            TaskStep(
                tool_name="step_a",
                required_fields=["field_a"],
                build_args=lambda t, i: {"arg_a": "1"},
                kind="write",
            ),
            TaskStep(
                tool_name="step_b",
                required_fields=["field_b"],
                build_args=lambda t, i: {"arg_b": "2"},
                kind="write",
            ),
        ]

        restored_task = tm2.restore_task(task_id, steps=steps2)
        assert restored_task is not None
        assert restored_task.task_id == task_id
        assert restored_task.name == "restore_test"
        assert restored_task.state == TaskState.WAITING_FOR_GROUNDING
        assert restored_task.current_step_index == 1
        assert restored_task.step_states[0] == StepState.COMMITTED
        assert restored_task.step_states[1] == StepState.WAITING_FOR_GROUNDING
        assert restored_task.step_results[0] == {"a": "result_a"}
        assert restored_task.step_dispatched_args[0] == {"arg_a": "1"}

        print(f"\n[TEST 7b VERIFICATION] Task restored across process restart: state={restored_task.state.name}, step_0={restored_task.step_states[0].name}")


# ===========================================================================
# Test 7c: Step IN_FLIGHT at checkpoint time restores as NEEDS_RECONCILIATION
# ===========================================================================

@pytest.mark.asyncio
async def test_7c_in_flight_restores_as_needs_reconciliation():
    """
    CRITICAL CONSTRAINT TEST:
    1. Create checkpoint where step 0 was IN_FLIGHT when process crashed.
    2. restore_task marks step 0 as NEEDS_RECONCILIATION.
    3. Advancing the task uses saga.reconcile_write with status_check:
       - Status check returns True -> step becomes COMMITTED, task completes.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileCheckpointStore(tmpdir)
        task_id = "task-in-flight-crash"

        # Manually create a checkpoint dict simulating a crash while step 0 was IN_FLIGHT
        crash_checkpoint = {
            "task_id": task_id,
            "name": "in_flight_crash_task",
            "state": "RUNNING",
            "current_step_index": 0,
            "step_states": ["IN_FLIGHT"],
            "step_results": [None],
            "step_action_ids": ["write-0-action-123"],
            "step_dispatched_args": [{"truck_id": "truck-17"}],
            "error": None,
        }
        store.save(task_id, crash_checkpoint)

        clock = TurnEpochClock()
        saga = SpeculativeSagaManager(clock)

        reconcile_called = False

        async def mock_status_check(action):
            nonlocal reconcile_called
            reconcile_called = True
            action.result = {"route_id": "route-reconciled"}
            return True  # Remote confirms operation executed and persisted

        steps = [
            TaskStep(
                tool_name="reroute_truck",
                required_fields=[],
                build_args=lambda t, i: {"truck_id": "truck-17"},
                kind="write",
                status_check=mock_status_check,
            ),
        ]

        tm = _HarnessTaskManager(
            saga=saga,
            resolve_field=lambda f: "val",
            executors={"reroute_truck": _simple_executor({"ok": True})},
            store=store,
        )

        # Restore task
        restored = tm.restore_task(task_id, steps=steps)
        assert restored is not None
        assert restored.state == TaskState.RUNNING
        assert restored.step_states[0] == StepState.NEEDS_RECONCILIATION

        # Advance task past reconciliation
        res = await tm.run_task(task_id)

        assert reconcile_called is True
        assert res.state == TaskState.COMPLETED
        assert res.step_states[0] == StepState.COMMITTED
        assert res.step_results[0] == {"route_id": "route-reconciled"}

        print(f"\n[TEST 7c VERIFICATION] NEEDS_RECONCILIATION step reconciled via saga.reconcile_write: result={res.step_results[0]}")


# ===========================================================================
# Test 7d: Restoring the same checkpoint twice is idempotent
# ===========================================================================

@pytest.mark.asyncio
async def test_7d_restore_idempotent_no_duplicate_dispatch():
    """
    Calling restore_task multiple times on the same task_id returns the exact
    same Task instance and does NOT duplicate state or execution.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileCheckpointStore(tmpdir)
        task_id = "task-idempotent-restore"

        checkpoint = {
            "task_id": task_id,
            "name": "idempotent_task",
            "state": "COMPLETED",
            "current_step_index": 1,
            "step_states": ["COMMITTED"],
            "step_results": [{"ok": True}],
            "step_action_ids": ["write-0-111"],
            "step_dispatched_args": [{"k": "v"}],
            "error": None,
        }
        store.save(task_id, checkpoint)

        clock = TurnEpochClock()
        saga = SpeculativeSagaManager(clock)

        dispatch_count = 0

        async def counting_exec(args):
            nonlocal dispatch_count
            dispatch_count += 1
            return {"ok": True}

        steps = [TaskStep(tool_name="step_a", kind="write")]

        tm = _HarnessTaskManager(
            saga=saga,
            resolve_field=lambda f: "val",
            executors={"step_a": counting_exec},
            store=store,
        )

        # Restore twice
        task_ref1 = tm.restore_task(task_id, steps=steps)
        task_ref2 = tm.restore_task(task_id, steps=steps)

        # Assert identical object reference
        assert task_ref1 is task_ref2

        # Run task — should be a no-op since task is COMPLETED
        result = await tm.run_task(task_id)
        assert result.state == TaskState.COMPLETED
        assert dispatch_count == 0  # Zero duplicate dispatches!

        print(f"\n[TEST 7d VERIFICATION] Restoring task twice returned same instance, 0 duplicate dispatches.")
