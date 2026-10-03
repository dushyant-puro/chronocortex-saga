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
    4. Assert restored Task fields match original Task fields EXACTLY:
       - task_id, name, state, current_step_index, step_states, step_results, step_dispatched_args
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

        # Explicit direct before/after equality assertions between original and restored task
        assert restored_task.task_id == task1.task_id
        assert restored_task.name == task1.name
        assert restored_task.state == task1.state
        assert restored_task.current_step_index == task1.current_step_index
        assert restored_task.step_states == task1.step_states
        assert restored_task.step_results == task1.step_results
        assert restored_task.step_dispatched_args == task1.step_dispatched_args

        print(f"\n[TEST 7b VERIFICATION] Explicit before/after equality verified across process restart: state={restored_task.state.name}, step_0={restored_task.step_states[0].name}")


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
# Test 7d: Restoring the same checkpoint twice is idempotent (Process Restart Case B)
# ===========================================================================

@pytest.mark.asyncio
async def test_7d_restore_idempotent_no_duplicate_dispatch():
    """
    Test 7d (Case B): Test surviving a process restart and idempotent restoration.
    1. Process 1: Create and execute a task to completion on TaskManager 1, saving checkpoint.
    2. Process 2: Discard TaskManager 1. Create a FRESH TaskManager 2 with fresh saga
       pointing to the same CheckpointStore, with executors that count dispatches.
    3. Call restore_task twice on TaskManager 2.
    4. Also create a FRESH TaskManager 3 pointing to the same CheckpointStore and call restore_task.
    5. Assert all restorations produce equivalent state, and running the tasks results in
       ZERO duplicate step dispatches.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileCheckpointStore(tmpdir)

        # --- Process 1: Execute task and write checkpoint ---
        clock1 = TurnEpochClock()
        saga1 = SpeculativeSagaManager(clock1)

        steps1 = [
            TaskStep(tool_name="step_a", build_args=lambda t, i: {"k": "v"}, kind="write")
        ]

        tm1 = _HarnessTaskManager(
            saga=saga1,
            resolve_field=lambda f: "val",
            executors={"step_a": _simple_executor({"ok": True})},
            store=store,
        )

        orig_task = tm1.create_task("idempotent_task", steps1)
        orig_result = await tm1.run_task(orig_task.task_id)
        assert orig_result.state == TaskState.COMPLETED
        task_id = orig_task.task_id

        # --- Process 2 (Restart): Discard TaskManager 1, create fresh TaskManager 2 ---
        clock2 = TurnEpochClock()
        saga2 = SpeculativeSagaManager(clock2)

        dispatch_count = 0

        async def counting_exec(args):
            nonlocal dispatch_count
            dispatch_count += 1
            return {"ok": True}

        executors2 = {"step_a": counting_exec}

        tm2 = _HarnessTaskManager(
            saga=saga2,
            resolve_field=lambda f: "val",
            executors=executors2,
            store=store,
        )

        steps2_a = [TaskStep(tool_name="step_a", build_args=lambda t, i: {"k": "v"}, kind="write")]
        steps2_b = [TaskStep(tool_name="step_a", build_args=lambda t, i: {"k": "v"}, kind="write")]

        # Restore twice on fresh TaskManager 2
        restored_ref1 = tm2.restore_task(task_id, steps=steps2_a)
        restored_ref2 = tm2.restore_task(task_id, steps=steps2_b)

        assert restored_ref1 is restored_ref2
        assert restored_ref1.state == orig_result.state
        assert restored_ref1.step_states == orig_result.step_states
        assert restored_ref1.step_results == orig_result.step_results

        # Run task on restored TaskManager 2 — must be a no-op
        res2 = await tm2.run_task(task_id)
        assert res2.state == TaskState.COMPLETED
        assert dispatch_count == 0  # Zero duplicate dispatches!

        # --- Process 3 (Another fresh instance): Verify independent restoration ---
        clock3 = TurnEpochClock()
        saga3 = SpeculativeSagaManager(clock3)
        tm3 = _HarnessTaskManager(
            saga=saga3,
            resolve_field=lambda f: "val",
            executors=executors2,
            store=store,
        )
        steps3 = [TaskStep(tool_name="step_a", build_args=lambda t, i: {"k": "v"}, kind="write")]
        restored_ref3 = tm3.restore_task(task_id, steps=steps3)

        assert restored_ref3.state == orig_result.state
        assert restored_ref3.step_states == orig_result.step_states
        assert restored_ref3.step_results == orig_result.step_results

        res3 = await tm3.run_task(task_id)
        assert res3.state == TaskState.COMPLETED
        assert dispatch_count == 0  # Still zero duplicate dispatches across fresh restarts!

        print(f"\n[TEST 7d VERIFICATION] Process restart restoration verified idempotent across fresh TaskManagers, 0 duplicate dispatches.")

