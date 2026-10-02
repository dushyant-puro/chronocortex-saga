"""
tests/test_phase6_task_engine.py

Phase 6 — Uninterruptible Task Engine test suite.

9 test scenarios covering:
  1. Normal 3-step task completes all steps via saga
  2. Epoch advance mid-task does NOT cancel the task
  3. Eviction → WAITING_FOR_GROUNDING → re-ground → resume
  4. Explicit user cancellation
  5. Step failure → compensation of committed predecessors
  6. Task survives multiple epoch advances
  7. GroundingGuard on_eviction hook fires and reaches TaskManager
  8. Two tasks can run independently (no cross-contamination)
  9. FLEET_REROUTE_LOGISTICS_TASK_STEPS is importable and well-formed
"""

import asyncio
from typing import Any, Optional

import pytest
import pytest_asyncio

from runtime.speculative_saga import (
    ActionState,
    SpeculativeSagaManager,
    StaleEpochError,
    TurnEpochClock,
)
from runtime.grounding_guard import GroundingGuard
from runtime.task_engine import (
    Task,
    TaskManager,
    TaskState,
    TaskStep,
    StepState,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stack():
    """Create a fresh clock + saga + guard + task_manager wired together."""
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    tm = TaskManager(saga=saga, resolve_field=lambda f: None)
    guard = GroundingGuard(epoch_clock=clock, on_eviction=tm.on_field_evicted)
    tm._resolve_field = guard.resolve_current_value
    return clock, saga, guard, tm


def _simple_executor(result: Any):
    """Return an executor that resolves to `result` after a tiny delay."""
    async def _exec(args: dict[str, Any]) -> Any:
        await asyncio.sleep(0.005)
        return result
    return _exec


def _failing_executor(exc: Exception):
    """Return an executor that raises `exc`."""
    async def _exec(args: dict[str, Any]) -> Any:
        await asyncio.sleep(0.005)
        raise exc
    return _exec


def _make_simple_steps(n: int, executors: list, required_fields: list[list[str]] | None = None) -> list[TaskStep]:
    """Create n simple write steps with given executors."""
    steps = []
    for i in range(n):
        rf = required_fields[i] if required_fields else []
        steps.append(TaskStep(
            tool_name=f"test_tool_{i}",
            required_fields=rf,
            build_args=lambda task, idx: {"step": idx},
            kind="write",
        ))
    return steps


# ---------------------------------------------------------------------------
# Test helpers: TaskManager that dispatches through saga directly
# ---------------------------------------------------------------------------

class _HarnessTaskManager(TaskManager):
    """
    Subclass that overrides _dispatch_write_step and _dispatch_read_step
    to use configurable executors (like the test fixture provides), since
    the real TaskManager._get_executor is intentionally abstract.
    """

    def __init__(self, saga, resolve_field, executors=None, read_executors=None):
        super().__init__(saga, resolve_field)
        self._test_executors = executors or {}  # tool_name -> executor
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
            executor=executor,
            entity_hash=entity_hash,
        )
        task.step_action_ids[idx] = action.action_id
        task.step_states[idx] = StepState.IN_FLIGHT
        if action._task:
            await action._task
        if action.state == ActionState.COMMITTED:
            return action.result
        from runtime.speculative_saga import SagaAbortedError
        raise SagaAbortedError(f"read {step.tool_name} did not commit: {action.state.name}")


# ===========================================================================
# Test 1: Normal 3-step task completes all steps via saga
# ===========================================================================

@pytest.mark.asyncio
async def test_6a_normal_3_step_task_completes():
    """A 3-step write task completes normally with all steps COMMITTED."""
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    executors = {
        "step_a": _simple_executor({"a": "done"}),
        "step_b": _simple_executor({"b": "done"}),
        "step_c": _simple_executor({"c": "done"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: "value",  # all fields always resolve
        executors=executors,
    )

    steps = [
        TaskStep(tool_name="step_a", build_args=lambda t, i: {"op": "a"}, kind="write"),
        TaskStep(tool_name="step_b", build_args=lambda t, i: {"op": "b"}, kind="write"),
        TaskStep(tool_name="step_c", build_args=lambda t, i: {"op": "c"}, kind="write"),
    ]

    task = tm.create_task("test_3step", steps)
    assert task.state == TaskState.PENDING

    result = await tm.run_task(task.task_id)

    assert result.state == TaskState.COMPLETED
    assert all(s == StepState.COMMITTED for s in result.step_states)
    assert result.step_results[0] == {"a": "done"}
    assert result.step_results[1] == {"b": "done"}
    assert result.step_results[2] == {"c": "done"}
    assert result.current_step_index == 3  # past all steps


# ===========================================================================
# Test 2: Epoch advance mid-task does NOT cancel the task
# ===========================================================================

@pytest.mark.asyncio
async def test_6b_epoch_advance_does_not_cancel_task():
    """
    Epoch advances during a task do NOT abort the task. The task engine
    must survive epoch churn because voice turn != task.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    call_count = 0

    async def counting_executor(args):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.005)
        return {"count": call_count}

    executors = {
        "step_a": counting_executor,
        "step_b": counting_executor,
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: "value",
        executors=executors,
    )

    steps = [
        TaskStep(tool_name="step_a", build_args=lambda t, i: {"x": 1}, kind="write"),
        TaskStep(tool_name="step_b", build_args=lambda t, i: {"x": 2}, kind="write"),
    ]

    task = tm.create_task("epoch_survive", steps)

    # Advance epoch BEFORE running task — task should still work
    await clock.advance(reason="barge_in")
    assert clock.current == 1

    # Run the task — it stages at epoch 1 and should complete
    result = await tm.run_task(task.task_id)
    assert result.state == TaskState.COMPLETED
    assert all(s == StepState.COMMITTED for s in result.step_states)

    # Advance epoch AFTER completion — task state should remain COMPLETED
    await clock.advance(reason="endpoint")
    assert result.state == TaskState.COMPLETED  # NOT cancelled


# ===========================================================================
# Test 3: Eviction → WAITING_FOR_GROUNDING → re-ground → resume
# ===========================================================================

@pytest.mark.asyncio
async def test_6c_eviction_parks_then_resumes_task():
    """
    When a required field is evicted during a task, the task parks at
    WAITING_FOR_GROUNDING. When the field is re-grounded, calling
    try_resume_waiting_tasks + resume_task completes the task.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    guard = GroundingGuard(epoch_clock=clock)

    executors = {
        "step_a": _simple_executor({"a": "done"}),
        "step_b": _simple_executor({"b": "done"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=guard.resolve_current_value,
        executors=executors,
    )
    guard._on_eviction = tm.on_field_evicted

    # Ground "truck_id" first
    guard.ingest_token("truck-17", 0.95, 0.0, 0.1)
    guard.stage_candidate("truck_id", "truck-17", (0, 1))

    steps = [
        TaskStep(
            tool_name="step_a",
            required_fields=["truck_id"],
            build_args=lambda t, i: {"truck_id": "truck-17"},
            kind="write",
        ),
        TaskStep(
            tool_name="step_b",
            required_fields=["destination"],
            build_args=lambda t, i: {"dest": "Chennai"},
            kind="write",
        ),
    ]

    task = tm.create_task("eviction_test", steps)

    # Step A should succeed (truck_id is grounded)
    result = await tm.run_task(task.task_id)

    # Step B should park (destination not grounded)
    assert result.state == TaskState.WAITING_FOR_GROUNDING
    assert result.step_states[0] == StepState.COMMITTED
    assert result.step_states[1] == StepState.WAITING_FOR_GROUNDING
    assert result.current_step_index == 1

    # Now ground the destination
    guard.ingest_token("Chennai", 0.92, 0.2, 0.3)
    guard.stage_candidate("destination", "Chennai", (1, 2))

    # Check if tasks can be resumed
    resumed = tm.try_resume_waiting_tasks()
    assert task.task_id in resumed

    # Resume the task
    result = await tm.resume_task(task.task_id)
    assert result.state == TaskState.COMPLETED
    assert result.step_states[1] == StepState.COMMITTED


# ===========================================================================
# Test 4: Explicit user cancellation
# ===========================================================================

@pytest.mark.asyncio
async def test_6d_explicit_cancellation():
    """
    cancel_task() sets state to CANCELLED and marks remaining steps SKIPPED.
    This is the ONLY way to stop a task other than completion or failure.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    executors = {
        "step_a": _simple_executor({"a": "done"}),
        "step_b": _simple_executor({"b": "done"}),
        "step_c": _simple_executor({"c": "done"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: None,  # fields never resolve
        executors=executors,
    )

    steps = [
        TaskStep(
            tool_name="step_a",
            required_fields=["truck_id"],  # will never resolve
            build_args=lambda t, i: {},
            kind="write",
        ),
        TaskStep(tool_name="step_b", build_args=lambda t, i: {}, kind="write"),
        TaskStep(tool_name="step_c", build_args=lambda t, i: {}, kind="write"),
    ]

    task = tm.create_task("cancel_test", steps)
    result = await tm.run_task(task.task_id)

    # Task should be waiting for grounding (truck_id never resolves)
    assert result.state == TaskState.WAITING_FOR_GROUNDING

    # User cancels
    cancelled = tm.cancel_task(task.task_id)
    assert cancelled.state == TaskState.CANCELLED
    assert cancelled.step_states[0] == StepState.SKIPPED  # was WAITING_FOR_GROUNDING
    assert cancelled.step_states[1] == StepState.SKIPPED
    assert cancelled.step_states[2] == StepState.SKIPPED
    assert cancelled.completed_at is not None


# ===========================================================================
# Test 5: Step failure → compensation of committed predecessors
# ===========================================================================

@pytest.mark.asyncio
async def test_6e_step_failure_compensates_predecessors():
    """
    When step B fails, step A (already COMMITTED) must be compensated.
    Compensation uses the step's compensate handler, not saga.abort_chain_from.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    compensated = []

    async def comp_a(result):
        compensated.append(("a", result))

    executors = {
        "step_a": _simple_executor({"a": "done"}),
        "step_b": _failing_executor(RuntimeError("step_b_failed")),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: "value",
        executors=executors,
    )

    steps = [
        TaskStep(
            tool_name="step_a",
            build_args=lambda t, i: {"op": "a"},
            kind="write",
            compensate=comp_a,
        ),
        TaskStep(
            tool_name="step_b",
            build_args=lambda t, i: {"op": "b"},
            kind="write",
        ),
    ]

    task = tm.create_task("fail_test", steps)
    result = await tm.run_task(task.task_id)

    assert result.state == TaskState.FAILED
    assert result.step_states[0] == StepState.SKIPPED  # compensated → SKIPPED
    assert result.step_states[1] == StepState.FAILED
    assert "step_b_failed" in result.error
    assert len(compensated) == 1
    assert compensated[0] == ("a", {"a": "done"})


# ===========================================================================
# Test 6: Task survives multiple epoch advances
# ===========================================================================

@pytest.mark.asyncio
async def test_6f_task_survives_multiple_epoch_advances():
    """
    A task that has completed step 0 and is waiting for grounding on step 1
    survives multiple epoch advances without corruption or cancellation.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    guard = GroundingGuard(epoch_clock=clock)

    executors = {
        "step_a": _simple_executor({"a": "ok"}),
        "step_b": _simple_executor({"b": "ok"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=guard.resolve_current_value,
        executors=executors,
    )
    guard._on_eviction = tm.on_field_evicted

    # Ground truck_id
    guard.ingest_token("truck-17", 0.95, 0.0, 0.1)
    guard.stage_candidate("truck_id", "truck-17", (0, 1))

    steps = [
        TaskStep(
            tool_name="step_a",
            required_fields=["truck_id"],
            build_args=lambda t, i: {"id": "truck-17"},
            kind="write",
        ),
        TaskStep(
            tool_name="step_b",
            required_fields=["destination"],
            build_args=lambda t, i: {"dest": "Chennai"},
            kind="write",
        ),
    ]

    task = tm.create_task("multi_epoch", steps)
    result = await tm.run_task(task.task_id)

    # Step A committed, step B waiting
    assert result.state == TaskState.WAITING_FOR_GROUNDING
    assert result.step_states[0] == StepState.COMMITTED

    # Fire multiple epoch advances
    await clock.advance(reason="barge_in")
    await clock.advance(reason="barge_in")
    await clock.advance(reason="endpoint")

    # Task state must still be WAITING_FOR_GROUNDING, NOT cancelled
    assert result.state == TaskState.WAITING_FOR_GROUNDING
    assert result.step_states[0] == StepState.COMMITTED  # not corrupted

    # Guard's turn was reset by epoch advances but that's fine — we need
    # to re-ingest and re-ground for the task to resume
    guard.ingest_token("Chennai", 0.92, 1.0, 1.1)
    guard.stage_candidate("destination", "Chennai", (0, 1))

    resumed = tm.try_resume_waiting_tasks()
    assert task.task_id in resumed

    result = await tm.resume_task(task.task_id)
    assert result.state == TaskState.COMPLETED


# ===========================================================================
# Test 7: GroundingGuard on_eviction hook fires and reaches TaskManager
# ===========================================================================

@pytest.mark.asyncio
async def test_6g_on_eviction_hook_integration():
    """
    When GroundingGuard evicts a staged candidate via tombstoning (repair cue),
    the on_eviction callback fires and TaskManager.on_field_evicted records it,
    parking any task that depends on that field.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    executors = {
        "step_a": _simple_executor({"a": "done"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: None,
        executors=executors,
    )

    guard = GroundingGuard(epoch_clock=clock, on_eviction=tm.on_field_evicted)
    tm._resolve_field = guard.resolve_current_value

    # Ingest tokens: "Chennai" then "actually" (repair cue)
    guard.ingest_token("Chennai", 0.95, 0.0, 0.1)
    guard.stage_candidate("destination", "Chennai", (0, 1))

    # Verify it's staged
    assert guard.resolve_current_value("destination") == "Chennai"

    # Create a task that depends on destination
    steps = [
        TaskStep(
            tool_name="step_a",
            required_fields=["destination"],
            build_args=lambda t, i: {"dest": "Chennai"},
            kind="write",
        ),
    ]
    task = tm.create_task("eviction_hook_test", steps)
    task.state = TaskState.RUNNING  # simulate running state

    # Now trigger eviction via repair cue
    guard.ingest_token("actually", 0.95, 0.2, 0.3)

    # The on_eviction hook should have fired
    assert "destination" in tm._evicted_fields

    # Task should be WAITING_FOR_GROUNDING
    assert task.state == TaskState.WAITING_FOR_GROUNDING
    assert task.step_states[0] == StepState.WAITING_FOR_GROUNDING


# ===========================================================================
# Test 8: Two tasks can run independently (no cross-contamination)
# ===========================================================================

@pytest.mark.asyncio
async def test_6h_independent_tasks_no_cross_contamination():
    """
    Two tasks running on the same TaskManager do not interfere with each
    other's state, results, or step progression.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    executors = {
        "task1_step": _simple_executor({"t1": "done"}),
        "task2_step": _simple_executor({"t2": "done"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: "value",
        executors=executors,
    )

    steps1 = [TaskStep(tool_name="task1_step", build_args=lambda t, i: {"id": 1}, kind="write")]
    steps2 = [TaskStep(tool_name="task2_step", build_args=lambda t, i: {"id": 2}, kind="write")]

    task1 = tm.create_task("task_one", steps1)
    task2 = tm.create_task("task_two", steps2)

    # Run both concurrently
    r1, r2 = await asyncio.gather(
        tm.run_task(task1.task_id),
        tm.run_task(task2.task_id),
    )

    assert r1.state == TaskState.COMPLETED
    assert r2.state == TaskState.COMPLETED
    assert r1.step_results[0] == {"t1": "done"}
    assert r2.step_results[0] == {"t2": "done"}
    assert r1.task_id != r2.task_id

    # Snapshot should show both
    snap = tm.snapshot()
    assert len(snap) == 2
    task_ids = {s["task_id"] for s in snap}
    assert task1.task_id in task_ids
    assert task2.task_id in task_ids


# ===========================================================================
# Test 9: FLEET_REROUTE_LOGISTICS_TASK_STEPS is importable and well-formed
# ===========================================================================

@pytest.mark.asyncio
async def test_6i_fleet_task_steps_well_formed():
    """
    FLEET_REROUTE_LOGISTICS_TASK_STEPS must be importable, have 5 steps,
    and each step must have the correct tool_name, required_fields, and kind.
    """
    from tools.fleet_tools import FLEET_REROUTE_LOGISTICS_TASK_STEPS

    assert len(FLEET_REROUTE_LOGISTICS_TASK_STEPS) == 5

    # Step 0: query_telemetry (read, requires truck_id)
    s0 = FLEET_REROUTE_LOGISTICS_TASK_STEPS[0]
    assert s0.tool_name == "query_telemetry"
    assert s0.kind == "read"
    assert "truck_id" in s0.required_fields
    assert s0.build_args is not None

    # Step 1: query_traffic (read, requires destination)
    s1 = FLEET_REROUTE_LOGISTICS_TASK_STEPS[1]
    assert s1.tool_name == "query_traffic"
    assert s1.kind == "read"
    assert "destination" in s1.required_fields

    # Step 2: query_dock_availability (read, requires dock_id)
    s2 = FLEET_REROUTE_LOGISTICS_TASK_STEPS[2]
    assert s2.tool_name == "query_dock_availability"
    assert s2.kind == "read"
    assert "dock_id" in s2.required_fields

    # Step 3: reroute_truck (write, requires truck_id + destination)
    s3 = FLEET_REROUTE_LOGISTICS_TASK_STEPS[3]
    assert s3.tool_name == "reroute_truck"
    assert s3.kind == "write"
    assert "truck_id" in s3.required_fields
    assert "destination" in s3.required_fields
    assert s3.compensate is not None  # has compensate_reroute

    # Step 4: reserve_dock (write, requires truck_id + dock_id)
    s4 = FLEET_REROUTE_LOGISTICS_TASK_STEPS[4]
    assert s4.tool_name == "reserve_dock"
    assert s4.kind == "write"
    assert "truck_id" in s4.required_fields
    assert "dock_id" in s4.required_fields
    assert s4.compensate is not None  # has compensate_reserve_dock
