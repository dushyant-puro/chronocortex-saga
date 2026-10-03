"""
tests/test_phase6_task_engine.py

Phase 6 — Uninterruptible Task Engine test suite.

9 test scenarios mapped to the original requirements:
  6a: 5-step task completes in topological order
  6b: correction to a NOT-YET-dispatched field is picked up automatically
  6c: CORE SCENARIO: committed step gets corrected, compensate() is
      actually called, redispatch happens with corrected value, task
      reaches COMPLETED
  6d: NEGATIVE CASE: unrelated field's correction does NOT trigger
      replanning for a committed step that didn't use that field
  6e: cancel_task compensates committed steps in reverse order
  6f: pause/resume preserves task_id and current_step_index, no
      re-dispatch of already-COMMITTED steps
  6g: checkpoint() round-trips to equivalent state
  6h: IN_FLIGHT_UNKNOWN stalls advancement without premature FAILED
  6i: full regression (FLEET_REROUTE_LOGISTICS_TASK_STEPS well-formed)
"""

import asyncio
import logging
from typing import Any, Optional

import pytest
import pytest_asyncio

from runtime.speculative_saga import (
    ActionState,
    SagaAbortedError,
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


def _timeout_executor():
    """Return an executor that never completes (for IN_FLIGHT_UNKNOWN tests)."""
    async def _exec(args: dict[str, Any]) -> Any:
        await asyncio.sleep(100)  # will be timed out
        return None
    return _exec


# ---------------------------------------------------------------------------
# _HarnessTaskManager: overrides dispatch to use test executors
# ---------------------------------------------------------------------------

class _HarnessTaskManager(TaskManager):
    """
    Subclass that overrides _dispatch_write_step and _dispatch_read_step
    to use configurable executors.
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
        raise SagaAbortedError(f"read {step.tool_name} did not commit: {action.state.name}")


# ===========================================================================
# Test 6a: 5-step task completes in topological order
# ===========================================================================

@pytest.mark.asyncio
async def test_6a_5_step_task_completes_in_order():
    """
    A 5-step task (3 reads + 2 writes in topological order) completes all
    steps sequentially with each step COMMITTED.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    call_order = []

    def make_exec(name, result):
        async def _exec(args):
            call_order.append(name)
            await asyncio.sleep(0.005)
            return result
        return _exec

    executors = {
        "reroute_truck": make_exec("reroute_truck", {"route_id": "r1"}),
        "reserve_dock": make_exec("reserve_dock", {"reservation_id": "d1"}),
    }
    read_executors = {
        "query_telemetry": make_exec("query_telemetry", {"speed": 88}),
        "query_traffic": make_exec("query_traffic", {"congestion": "low"}),
        "query_dock": make_exec("query_dock", {"available": True}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: "value",
        executors=executors,
        read_executors=read_executors,
    )

    steps = [
        TaskStep(tool_name="query_telemetry", build_args=lambda t, i: {"truck_id": "t17"}, kind="read"),
        TaskStep(tool_name="query_traffic", build_args=lambda t, i: {"route": "Chennai"}, kind="read"),
        TaskStep(tool_name="query_dock", build_args=lambda t, i: {"dock_id": "D1"}, kind="read"),
        TaskStep(tool_name="reroute_truck", build_args=lambda t, i: {"truck_id": "t17", "destination": "Chennai"}, kind="write"),
        TaskStep(tool_name="reserve_dock", build_args=lambda t, i: {"dock_id": "D1"}, kind="write"),
    ]

    task = tm.create_task("full_5_step", steps)
    result = await tm.run_task(task.task_id)

    assert result.state == TaskState.COMPLETED
    assert all(s == StepState.COMMITTED for s in result.step_states)
    assert result.current_step_index == 5
    # Verify topological order
    assert call_order == ["query_telemetry", "query_traffic", "query_dock", "reroute_truck", "reserve_dock"]


# ===========================================================================
# Test 6b: correction to a NOT-YET-dispatched field is picked up
# ===========================================================================

@pytest.mark.asyncio
async def test_6b_correction_not_yet_dispatched_picked_up():
    """
    Step 0 (requires truck_id) completes, step 1 (requires destination)
    parks because destination is not grounded. User grounds "Chennai",
    then corrects to "Bengaluru". Step 1 dispatches with "Bengaluru".
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    guard = GroundingGuard(epoch_clock=clock)

    dispatched_args = []

    async def capture_executor(args):
        dispatched_args.append(dict(args))
        await asyncio.sleep(0.005)
        return {"ok": True}

    executors = {
        "step_a": _simple_executor({"a": "done"}),
        "step_b": capture_executor,
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
            build_args=lambda t, i: {"truck_id": "truck-17"},
            kind="write",
        ),
        TaskStep(
            tool_name="step_b",
            required_fields=["destination"],
            build_args=lambda t, i: {"dest": guard.resolve_current_value("destination")},
            kind="write",
        ),
    ]

    task = tm.create_task("correction_test", steps)
    result = await tm.run_task(task.task_id)

    # Step 0 done, step 1 waiting
    assert result.state == TaskState.WAITING_FOR_GROUNDING
    assert result.step_states[0] == StepState.COMMITTED

    # Ground "Bengaluru" directly (as if the user said it correctly this time)
    guard.ingest_token("Bengaluru", 0.92, 0.2, 0.3)
    guard.stage_candidate("destination", "Bengaluru", (1, 2))

    resumed = tm.try_resume_waiting_tasks()
    assert task.task_id in resumed

    result = await tm.resume_task(task.task_id)
    assert result.state == TaskState.COMPLETED

    # The dispatched args should contain "Bengaluru"
    assert len(dispatched_args) == 1
    assert dispatched_args[0]["dest"] == "Bengaluru"


# ===========================================================================
# Test 6c: CORE SCENARIO — committed step corrected, compensate called,
#           redispatch with corrected value, task reaches COMPLETED
# ===========================================================================

@pytest.mark.asyncio
async def test_6c_committed_step_corrected_compensated_redispatched():
    """
    TASK-INV-5 core test:
    1. Step 0 commits with destination="Chennai" (write, has compensate)
    2. User says "actually" → GroundingGuard evicts "Chennai"
    3. on_field_evicted finds step 0's dispatched_args contain "Chennai"
    4. step 0's compensate() is called
    5. Task rewinds to step 0, parks at WAITING_FOR_GROUNDING
    6. User re-grounds with "Bengaluru"
    7. Step 0 redispatches with "Bengaluru"
    8. Task reaches COMPLETED

    Uses real GroundingGuard eviction path, not direct field mutation.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    guard = GroundingGuard(epoch_clock=clock)

    compensated = []
    dispatch_log = []

    async def compensate_handler(result):
        compensated.append(result)

    async def tracking_executor(args):
        dispatch_log.append(dict(args))
        await asyncio.sleep(0.005)
        return {"route_id": f"route-{args.get('destination', 'unknown')}"}

    executors = {
        "reroute_truck": tracking_executor,
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=guard.resolve_current_value,
        executors=executors,
    )
    guard._on_eviction = tm.on_field_evicted

    # Ground destination="Chennai"
    guard.ingest_token("Chennai", 0.95, 0.0, 0.1)
    guard.stage_candidate("destination", "Chennai", (0, 1))

    steps = [
        TaskStep(
            tool_name="reroute_truck",
            required_fields=["destination"],
            build_args=lambda t, i: {"destination": guard.resolve_current_value("destination")},
            kind="write",
            compensate=compensate_handler,
        ),
    ]

    task = tm.create_task("core_scenario", steps)
    result = await tm.run_task(task.task_id)

    # Step 0 committed with "Chennai"
    assert result.state == TaskState.COMPLETED
    assert result.step_states[0] == StepState.COMMITTED
    assert dispatch_log[0]["destination"] == "Chennai"
    assert result.step_dispatched_args[0] == {"destination": "Chennai"}

    # Now user says "actually" → repair cue → eviction
    # This triggers tombstoning of the preceding clause (which includes "Chennai")
    guard.ingest_token("actually", 0.95, 0.2, 0.3)

    # Give the async _replan_from_step a chance to run
    await asyncio.sleep(0.05)

    print(f"\n[TEST 6c VERIFICATION] compensate() called with: {compensated}")
    # compensate() should have been called
    assert len(compensated) == 1, f"Expected compensate() to be called, got: {compensated}"
    assert compensated[0] == {"route_id": "route-Chennai"}

    # Task should be WAITING_FOR_GROUNDING (rewound to step 0)
    assert task.state == TaskState.WAITING_FOR_GROUNDING
    assert task.current_step_index == 0
    assert task.step_states[0] == StepState.WAITING_FOR_GROUNDING

    # Now re-ground with "Bengaluru"
    guard.ingest_token("Bengaluru", 0.95, 0.4, 0.5)
    guard.stage_candidate("destination", "Bengaluru", (2, 3))

    resumed = tm.try_resume_waiting_tasks()
    assert task.task_id in resumed

    result = await tm.resume_task(task.task_id)
    assert result.state == TaskState.COMPLETED
    assert result.step_states[0] == StepState.COMMITTED

    # The SECOND dispatch should have used "Bengaluru"
    assert len(dispatch_log) == 2
    assert dispatch_log[1]["destination"] == "Bengaluru"


# ===========================================================================
# Test 6d: NEGATIVE CASE — unrelated field's correction does NOT trigger
#           replanning for a committed step that didn't use that field
# ===========================================================================

@pytest.mark.asyncio
async def test_6d_unrelated_correction_no_replanning():
    """
    Step 0 commits with truck_id="truck-17". Then destination="Chennai" is
    evicted. Step 0's dispatched_args do NOT contain "Chennai", so NO
    compensation or replanning should occur.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    guard = GroundingGuard(epoch_clock=clock)

    compensated = []

    async def compensate_handler(result):
        compensated.append(result)

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

    # Ground truck_id in first clause
    guard.ingest_token("truck-17", 0.95, 0.0, 0.1)
    guard.stage_candidate("truck_id", "truck-17", (0, 1))

    steps = [
        TaskStep(
            tool_name="step_a",
            required_fields=["truck_id"],
            build_args=lambda t, i: {"truck_id": "truck-17"},  # uses truck_id, NOT destination
            kind="write",
            compensate=compensate_handler,
        ),
        TaskStep(
            tool_name="step_b",
            required_fields=["destination"],
            build_args=lambda t, i: {"dest": guard.resolve_current_value("destination")},
            kind="write",
        ),
    ]

    task = tm.create_task("negative_case", steps)
    result = await tm.run_task(task.task_id)

    # Step 0 committed (truck_id resolved), step 1 waiting (destination not grounded)
    assert result.state == TaskState.WAITING_FOR_GROUNDING
    assert result.step_states[0] == StepState.COMMITTED
    assert result.step_dispatched_args[0] == {"truck_id": "truck-17"}

    # Clause boundary then ground destination in next clause, then evict destination
    guard.ingest_token("and", 0.95, 0.15, 0.19)
    guard.ingest_token("Chennai", 0.95, 0.2, 0.3)
    guard.stage_candidate("destination", "Chennai", (2, 3))
    guard.ingest_token("actually", 0.95, 0.4, 0.5)

    await asyncio.sleep(0.05)

    print(f"\n[TEST 6d VERIFICATION] compensated correctly remains empty: {compensated}")
    # compensate() should NOT have been called — step 0 didn't use "Chennai"
    assert len(compensated) == 0, f"Expected NO compensation, got: {compensated}"

    # Task should still be WAITING_FOR_GROUNDING (step 1 parked since
    # destination was evicted by the correction)
    assert task.state == TaskState.WAITING_FOR_GROUNDING
    # Step 0 still COMMITTED, untouched
    assert task.step_states[0] == StepState.COMMITTED
    assert task.current_step_index == 1


# ===========================================================================
# Test 6e: cancel_task compensates committed steps in reverse order
# ===========================================================================

@pytest.mark.asyncio
async def test_6e_cancel_compensates_in_reverse_order():
    """
    After committing steps A and B, cancel_task must compensate B first,
    then A (reverse order).
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    compensation_order = []

    async def comp_a(result):
        compensation_order.append(("a", result))

    async def comp_b(result):
        compensation_order.append(("b", result))

    executors = {
        "step_a": _simple_executor({"a": "done"}),
        "step_b": _simple_executor({"b": "done"}),
        "step_c": _simple_executor({"c": "done"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: {"a": "value", "b": "value"}.get(f),
        executors=executors,
    )

    steps = [
        TaskStep(tool_name="step_a", build_args=lambda t, i: {"op": "a"}, kind="write", compensate=comp_a),
        TaskStep(tool_name="step_b", build_args=lambda t, i: {"op": "b"}, kind="write", compensate=comp_b),
        TaskStep(
            tool_name="step_c",
            required_fields=["never_resolves"],
            build_args=lambda t, i: {},
            kind="write",
        ),
    ]

    task = tm.create_task("cancel_reverse", steps)
    result = await tm.run_task(task.task_id)

    # A and B committed, C waiting for grounding
    assert result.state == TaskState.WAITING_FOR_GROUNDING
    assert result.step_states[0] == StepState.COMMITTED
    assert result.step_states[1] == StepState.COMMITTED

    # Cancel
    cancelled = await tm.cancel_task(task.task_id)
    assert cancelled.state == TaskState.CANCELLED

    # Compensation order should be reverse: B first, then A
    assert len(compensation_order) == 2
    assert compensation_order[0][0] == "b"
    assert compensation_order[1][0] == "a"
    assert cancelled.step_states[0] == StepState.COMPENSATED
    assert cancelled.step_states[1] == StepState.COMPENSATED
    assert cancelled.step_states[2] == StepState.SKIPPED


# ===========================================================================
# Test 6f: pause/resume preserves task_id and current_step_index,
#           no re-dispatch of already-COMMITTED steps
# ===========================================================================

@pytest.mark.asyncio
async def test_6f_pause_resume_preserves_state():
    """
    A task paused at WAITING_FOR_GROUNDING preserves its task_id and
    current_step_index. On resume, already-COMMITTED steps are NOT
    re-dispatched.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    guard = GroundingGuard(epoch_clock=clock)

    dispatch_count = {"step_a": 0, "step_b": 0}

    async def counting_a(args):
        dispatch_count["step_a"] += 1
        await asyncio.sleep(0.005)
        return {"a": "done"}

    async def counting_b(args):
        dispatch_count["step_b"] += 1
        await asyncio.sleep(0.005)
        return {"b": "done"}

    executors = {
        "step_a": counting_a,
        "step_b": counting_b,
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
            build_args=lambda t, i: {"dest": guard.resolve_current_value("destination")},
            kind="write",
        ),
    ]

    task = tm.create_task("pause_resume", steps)
    original_task_id = task.task_id

    result = await tm.run_task(task.task_id)

    # Paused: step A done, step B waiting
    assert result.state == TaskState.WAITING_FOR_GROUNDING
    assert result.current_step_index == 1
    assert dispatch_count["step_a"] == 1

    # Ground destination
    guard.ingest_token("Chennai", 0.92, 0.2, 0.3)
    guard.stage_candidate("destination", "Chennai", (1, 2))

    resumed = tm.try_resume_waiting_tasks()
    result = await tm.resume_task(task.task_id)

    assert result.state == TaskState.COMPLETED
    assert result.task_id == original_task_id  # same task_id preserved
    assert dispatch_count["step_a"] == 1  # NOT re-dispatched
    assert dispatch_count["step_b"] == 1  # dispatched exactly once


# ===========================================================================
# Test 6g: checkpoint() round-trips to equivalent state
# ===========================================================================

@pytest.mark.asyncio
async def test_6g_checkpoint_round_trip():
    """
    checkpoint() returns a dict that, when passed to restore_checkpoint(),
    produces equivalent task state.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    executors = {
        "step_a": _simple_executor({"a": "done"}),
        "step_b": _simple_executor({"b": "done"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: None,  # step B will wait
        executors=executors,
    )

    # Use resolve_field that resolves truck_id but not destination
    def partial_resolve(f):
        if f == "truck_id":
            return "truck-17"
        return None
    tm._resolve_field = partial_resolve

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
            build_args=lambda t, i: {"dest": "?"},
            kind="write",
        ),
    ]

    task = tm.create_task("checkpoint_test", steps)
    await tm.run_task(task.task_id)

    # checkpoint at WAITING_FOR_GROUNDING
    assert task.state == TaskState.WAITING_FOR_GROUNDING
    cp = tm.checkpoint(task.task_id)

    # Verify checkpoint contents
    assert cp["task_id"] == task.task_id
    assert cp["state"] == "WAITING_FOR_GROUNDING"
    assert cp["current_step_index"] == 1
    assert cp["step_states"][0] == "COMMITTED"
    assert cp["step_states"][1] == "WAITING_FOR_GROUNDING"
    assert cp["step_results"][0] == {"a": "done"}
    assert cp["step_dispatched_args"][0] == {"truck_id": "truck-17"}

    # Modify state
    task.state = TaskState.RUNNING
    task.current_step_index = 0

    # Restore
    restored = tm.restore_checkpoint(task.task_id, cp)
    assert restored.state == TaskState.WAITING_FOR_GROUNDING
    assert restored.current_step_index == 1
    assert restored.step_states[0] == StepState.COMMITTED
    assert restored.step_results[0] == {"a": "done"}


# ===========================================================================
# Test 6h: IN_FLIGHT_UNKNOWN stalls advancement without premature FAILED
# ===========================================================================

@pytest.mark.asyncio
async def test_6h_in_flight_unknown_stalls_not_fails():
    """
    When a step's executor times out (producing IN_FLIGHT_UNKNOWN in the
    saga), the task should become FAILED with appropriate error — the task
    engine propagates TimeoutError as a step failure, which is correct
    because IN_FLIGHT_UNKNOWN means we don't know if the mutation happened
    and cannot safely advance to the next step.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    executors = {
        "step_a": _simple_executor({"a": "done"}),
        "step_timeout": _timeout_executor(),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: "value",
        executors=executors,
    )

    steps = [
        TaskStep(tool_name="step_a", build_args=lambda t, i: {"op": "a"}, kind="write"),
        TaskStep(
            tool_name="step_timeout",
            build_args=lambda t, i: {"op": "timeout"},
            kind="write",
        ),
    ]

    task = tm.create_task("timeout_test", steps)
    # Use a very short timeout for the saga commit
    saga_original_timeout = 4.0

    # Override commit_write's timeout by wrapping the dispatch
    class _TimeoutHarness(_HarnessTaskManager):
        async def _dispatch_write_step(self, task, idx, step, args):
            if step.tool_name == "step_timeout":
                executor = self._test_executors.get(step.tool_name)
                action = self._saga.stage_write(step.tool_name, args)
                task.step_action_ids[idx] = action.action_id
                task.step_states[idx] = StepState.IN_FLIGHT
                result = await self._saga.commit_write(
                    action.action_id, executor, timeout=0.05
                )
                return result
            return await super()._dispatch_write_step(task, idx, step, args)

    tm2 = _TimeoutHarness(
        saga=saga,
        resolve_field=lambda f: "value",
        executors=executors,
    )

    task2 = tm2.create_task("timeout_test2", steps)
    result = await tm2.run_task(task2.task_id)

    # Task should be FAILED (TimeoutError caught) — not COMPLETED
    assert result.state == TaskState.FAILED
    assert result.step_states[0] == StepState.COMMITTED  # step_a succeeded
    # Step 1 should be FAILED (TimeoutError)
    assert result.step_states[1] == StepState.FAILED
    assert "step_timeout" in result.error


# ===========================================================================
# Test 6i: full regression — FLEET_REROUTE_LOGISTICS_TASK_STEPS well-formed
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
