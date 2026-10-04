"""
tests/test_phase10_task_graph.py

Phase 10 — Long-Horizon Autonomy: TaskGraph test suite.

9 test scenarios:
  1. Gated dispatch: B depends on A for "x"; B does not dispatch first step until A COMPLETED
  2. Core scenario: A completes with "x"=value1; B commits with "x"=value1; A gets corrected
     to "x"=value2; B's on_upstream_replanned cascades, B compensates and redispatches with value2
  3. Negative case: B depends on A ONLY for "y", A corrected on "x"; isolated test proves
     discrimination logic prevents cascade
  4. Cycle detection: cycles rejected at add_task time with TaskGraphCycleError
  5. Deterministic dispatch order: dispatch_order() returns identical order across repeated calls
  6. Replan-storm bound: GRAPH-INV-3 terminates cascade without looping
  7. GraphState derivation: state correctly derived from constituent tasks at multiple lifecycle points
  8. Checkpoint/restore round-trip: graph serialized mid-run and restored via fresh TaskManager
  9. Full regression check
"""

import asyncio
import tempfile
from typing import Any, Optional

import pytest
import pytest_asyncio

from runtime.persistence import FileCheckpointStore
from runtime.speculative_saga import SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import StepState, Task, TaskManager, TaskState, TaskStep
from runtime.task_graph import GraphState, TaskGraph, TaskGraphCycleError


def _simple_executor(result: Any):
    async def _exec(args: dict[str, Any]) -> Any:
        await asyncio.sleep(0.005)
        return result
    return _exec


class _HarnessTaskManager(TaskManager):
    def __init__(self, saga, resolve_field, executors=None, store=None, on_task_replanned=None):
        super().__init__(saga, resolve_field, store=store, on_task_replanned=on_task_replanned)
        self._test_executors = executors or {}

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


# ===========================================================================
# Test 1: Gated dispatch order (GRAPH-INV-1)
# ===========================================================================

@pytest.mark.asyncio
async def test_1_gated_dispatch_downstream_waits_for_upstream():
    """
    Two-task graph: Task B depends on Task A for field 'x'.
    Assert via dispatch-tracking executor that B's first step does NOT dispatch
    until A has reached TaskState.COMPLETED.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    task_a_state_when_b_ran: Optional[TaskState] = None

    async def step_a_exec(args):
        await asyncio.sleep(0.02)
        return {"x": "val_a"}

    async def step_b_exec(args):
        nonlocal task_a_state_when_b_ran
        task_a_state_when_b_ran = task_a.state
        return {"result_b": "ok"}

    executors = {
        "tool_a": step_a_exec,
        "tool_b": step_b_exec,
    }

    tm = _HarnessTaskManager(saga=saga, resolve_field=lambda f: "val", executors=executors)

    task_a = tm.create_task("task_a", [TaskStep(tool_name="tool_a", kind="write")])
    task_b = tm.create_task("task_b", [TaskStep(tool_name="tool_b", kind="write")])

    graph = TaskGraph("graph_test_1")
    graph.add_task(task_a.task_id, task_a)
    graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: ["x"]})

    final_state = await graph.run(tm)

    assert final_state == GraphState.COMPLETED
    assert task_a_state_when_b_ran == TaskState.COMPLETED, (
        f"Expected task A to be COMPLETED when step B ran, but was {task_a_state_when_b_ran}"
    )
    print(f"\n[TEST 1 VERIFICATION] Task B first step ran only after Task A state: {task_a_state_when_b_ran}")


# ===========================================================================
# Test 2: Core Scenario — Cascading replan with compensation and redispatch
# ===========================================================================

@pytest.mark.asyncio
async def test_2_cascading_replan_compensated_and_redispatched():
    """
    CORE SCENARIO:
    1. A completes with x="value1"
    2. B runs and commits using x="value1"
    3. A gets corrected to x="value2" (via on_field_evicted)
    4. on_upstream_replanned cascades to B:
       - B's step is compensated
       - B redispatches with x="value2"
    5. Both A and B reach COMPLETED with consistent values.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    current_values = {"x": "value1"}
    def resolve_field(f: str):
        return current_values.get(f)

    b_compensated: list[Any] = []
    a_compensated: list[Any] = []
    b_dispatches: list[dict[str, Any]] = []

    async def a_compensate(res):
        a_compensated.append(res)

    async def b_compensate(res):
        b_compensated.append(res)

    async def exec_a(args):
        await asyncio.sleep(0.005)
        return {"x": args.get("x")}

    async def exec_b(args):
        b_dispatches.append(dict(args))
        await asyncio.sleep(0.005)
        return {"b_out": f"b_processed_{args.get('x')}"}

    executors = {"tool_a": exec_a, "tool_b": exec_b}

    tm = _HarnessTaskManager(saga=saga, resolve_field=resolve_field, executors=executors)

    task_a = tm.create_task("task_a", [
        TaskStep(
            tool_name="tool_a",
            required_fields=["x"],
            build_args=lambda t, i: {"x": resolve_field("x")},
            kind="write",
            compensate=a_compensate,
        )
    ])
    task_b = tm.create_task("task_b", [
        TaskStep(
            tool_name="tool_b",
            required_fields=["x"],
            build_args=lambda t, i: {"b_input": task_a.step_results[0]["x"]},
            kind="write",
            compensate=b_compensate,
        )
    ])

    graph = TaskGraph("graph_test_2")
    graph.add_task(task_a.task_id, task_a)
    graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: {"x": "b_input"}})

    # Initial run: both complete with value1
    await graph.run(tm)
    assert task_a.state == TaskState.COMPLETED
    assert task_b.state == TaskState.COMPLETED
    assert b_dispatches[0]["b_input"] == "value1"

    # Now A is corrected: x changed from "value1" to "value2"
    current_values["x"] = "value2"
    tm.on_field_evicted("x", "value1")

    # Allow async replan to complete
    await asyncio.sleep(0.08)

    # Assert A compensated and redispatched with value2
    assert len(a_compensated) == 1
    assert task_a.state == TaskState.COMPLETED

    # Assert B cascaded: compensated old step and redispatched with value2
    assert len(b_compensated) == 1
    assert len(b_dispatches) == 2
    assert b_dispatches[1]["b_input"] == "value2"
    assert task_b.state == TaskState.COMPLETED

    print(f"\n[TEST 2 VERIFICATION] Cascade complete: A & B redispatched with value2. b_dispatches={b_dispatches}")


# ===========================================================================
# Test 3: Negative Case — Isolated discrimination logic check
# ===========================================================================

@pytest.mark.asyncio
async def test_3_negative_case_isolated_discrimination():
    """
    NEGATIVE CASE:
    Task B depends on Task A ONLY for field 'y' (not 'x').
    Directly call graph.on_upstream_replanned with changed_fields={'x': 'new_x'}.
    Assert B does NOT cascade/replan (compensation count == 0, state remains COMPLETED).
    Then call with changed_fields={'y': 'new_y'} and verify it DOES cascade.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    current_values = {"x": "x_val", "y": "y_val"}
    b_compensated: list[Any] = []

    async def b_compensate(res):
        b_compensated.append(res)

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: current_values.get(f),
        executors={"tool_b": _simple_executor({"ok": True})},
    )

    task_a = tm.create_task("task_a", [TaskStep(tool_name="tool_a", kind="write")])
    task_b = tm.create_task("task_b", [
        TaskStep(
            tool_name="tool_b",
            required_fields=["y"],
            build_args=lambda t, i: {"y": "y_val"},
            kind="write",
            compensate=b_compensate,
        )
    ])

    graph = TaskGraph("graph_test_3")
    graph.add_task(task_a.task_id, task_a)
    # B depends ONLY on 'y'
    graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: ["y"]})
    graph._task_manager = tm

    # Manually mark B as committed
    task_b.step_states[0] = StepState.COMMITTED
    task_b.step_results[0] = {"ok": True}
    task_b.step_dispatched_args[0] = {"y": "y_val"}
    task_b.state = TaskState.COMPLETED
    task_b.current_step_index = 1

    # Call on_upstream_replanned directly in isolation with changed 'x' (unconsumed field)
    cascaded = await graph.on_upstream_replanned(task_a.task_id, {"x": "new_x_value"})

    # ASSERTION 1: B was NOT affected (field name discrimination)
    assert cascaded == [], f"Expected empty cascade, got: {cascaded}"
    assert len(b_compensated) == 0, "B's compensate handler should NOT be called for unconsumed field"
    assert task_b.state == TaskState.COMPLETED
    assert task_b.step_states[0] == StepState.COMMITTED

    print("\n[TEST 3 VERIFICATION] Isolated on_upstream_replanned directly verified negative discrimination: 0 cascades for unconsumed field 'x'")

    # ASSERTION 2: Exact value mismatch discrimination (Phase 6 Bug 2 protection)
    # Field matches 'y', but step dispatched with 'y_other' while evicted value is 'y_val'
    task_b.step_dispatched_args[0] = {"y": "y_other"}
    cascaded_mismatch = await graph.on_upstream_replanned(
        task_a.task_id,
        {"y": "new_y_value", "_evicted_values": {"y": "y_val"}},
        event_id="test_ev_mismatch",
    )
    assert cascaded_mismatch == [], "Step with mismatched dispatched value must NOT be compensated"
    assert len(b_compensated) == 0, "No compensation on value mismatch"
    print("[TEST 3 VERIFICATION] Exact-match discrimination verified: 0 cascades when dispatched value differs from evicted value")

    # ASSERTION 3: Positive control — exact field and value match MUST cascade
    task_b.step_dispatched_args[0] = {"y": "y_val"}
    cascaded_pos = await graph.on_upstream_replanned(
        task_a.task_id,
        {"y": "new_y_value", "_evicted_values": {"y": "y_val"}},
        event_id="test_ev_match",
    )
    assert task_b.task_id in cascaded_pos
    assert len(b_compensated) == 1
    print("[TEST 3 VERIFICATION] Positive control verified: cascade triggered when dependent field 'y' and exact value matched")


# ===========================================================================
# Test 3b: Missing evicted-value information fails safe (no cascade)
# ===========================================================================

@pytest.mark.asyncio
async def test_3b_missing_evicted_value_fails_safe_no_cascade():
    """
    Verifies that when evicted_v is None (missing _evicted_values entry),
    on_upstream_replanned fails safe: NO cascade occurs, even if the downstream
    step's dispatched_args happen to differ from the new value reported in
    changed_fields (preventing Bug 2 loose-matching regression).
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    b_compensated: list[Any] = []

    async def b_compensate(res):
        b_compensated.append(res)

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: "val",
        executors={"tool_b": _simple_executor({"ok": True})},
    )

    task_a = tm.create_task("task_a", [TaskStep(tool_name="tool_a", kind="write")])
    task_b = tm.create_task("task_b", [
        TaskStep(
            tool_name="tool_b",
            required_fields=["x"],
            build_args=lambda t, i: {"x": "old_dispatched_val"},
            kind="write",
            compensate=b_compensate,
        )
    ])

    graph = TaskGraph("graph_test_3b")
    graph.add_task(task_a.task_id, task_a)
    graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: ["x"]})
    graph._task_manager = tm

    # Mark Task B as committed with 'old_dispatched_val'
    task_b.step_states[0] = StepState.COMMITTED
    task_b.step_results[0] = {"ok": True}
    task_b.step_dispatched_args[0] = {"x": "old_dispatched_val"}
    task_b.state = TaskState.COMPLETED
    task_b.current_step_index = 1

    # Call on_upstream_replanned directly with changed_fields containing 'x'='new_val',
    # but NO entry in _evicted_values (evicted_v is None).
    # Note: 'old_dispatched_val' != 'new_val', so the old loose branch WOULD have matched.
    cascaded = await graph.on_upstream_replanned(
        task_a.task_id,
        {"x": "new_val"},  # NO _evicted_values provided!
        event_id="test_missing_evicted_val",
    )

    # ASSERTION: Missing evicted-value information must fail safe -> NO cascade
    assert cascaded == [], f"Expected empty cascade when evicted_v is None, got: {cascaded}"
    assert len(b_compensated) == 0, "Compensate handler must NOT be called when evicted value is unknown"
    assert task_b.state == TaskState.COMPLETED
    assert task_b.step_states[0] == StepState.COMMITTED

    print("\n[TEST 3b VERIFICATION] Missing evicted-value fails safe: 0 cascades despite dispatched_args != new_val")


# ===========================================================================
# Test 4: Cycle detection rejected at add_task time
# ===========================================================================

def test_4_cycle_detection_rejected_at_add_task():
    """
    Adding a task that causes a cyclic dependency raises TaskGraphCycleError
    immediately at add_task time.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    tm = TaskManager(saga=saga, resolve_field=lambda f: "val")

    task_a = tm.create_task("task_a", [TaskStep(tool_name="a", kind="write")])
    task_b = tm.create_task("task_b", [TaskStep(tool_name="b", kind="write")])
    task_c = tm.create_task("task_c", [TaskStep(tool_name="c", kind="write")])

    # 2-node cycle: A depends on B, B depends on A
    graph = TaskGraph("cycle_graph_2")
    graph.add_task(task_a.task_id, task_a, depends_on={task_b.task_id: ["x"]})
    with pytest.raises(TaskGraphCycleError) as exc_info:
        graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: ["x"]})
    assert "Cyclic dependency detected" in str(exc_info.value)

    # 3-node cycle: A -> B -> C -> A
    graph2 = TaskGraph("cycle_graph_3")
    graph2.add_task(task_a.task_id, task_a, depends_on={task_c.task_id: ["z"]})
    graph2.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: ["x"]})
    with pytest.raises(TaskGraphCycleError):
        graph2.add_task(task_c.task_id, task_c, depends_on={task_b.task_id: ["y"]})

    print("\n[TEST 4 VERIFICATION] Cycle detection successfully rejected cycles at add_task time.")


# ===========================================================================
# Test 5: dispatch_order() is deterministic
# ===========================================================================

def test_5_dispatch_order_deterministic():
    """
    Verifies topological sort determinism and alphabetical tie-breaking across
    repeated calls on a diamond dependency graph.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    tm = TaskManager(saga=saga, resolve_field=lambda f: "val")

    tasks = {
        name: tm.create_task(name, [TaskStep(tool_name=name, kind="write")])
        for name in ["task_root", "task_alpha", "task_beta", "task_join", "task_solo"]
    }

    graph = TaskGraph("diamond_graph")
    graph.add_task(tasks["task_root"].task_id, tasks["task_root"])
    graph.add_task(tasks["task_solo"].task_id, tasks["task_solo"])
    graph.add_task(tasks["task_alpha"].task_id, tasks["task_alpha"], depends_on={tasks["task_root"].task_id: []})
    graph.add_task(tasks["task_beta"].task_id, tasks["task_beta"], depends_on={tasks["task_root"].task_id: []})
    graph.add_task(
        tasks["task_join"].task_id,
        tasks["task_join"],
        depends_on={tasks["task_alpha"].task_id: [], tasks["task_beta"].task_id: []},
    )

    baseline_order = graph.dispatch_order()
    for _ in range(50):
        assert graph.dispatch_order() == baseline_order

    # Prerequisites must precede dependents
    idx_root = baseline_order.index(tasks["task_root"].task_id)
    idx_alpha = baseline_order.index(tasks["task_alpha"].task_id)
    idx_beta = baseline_order.index(tasks["task_beta"].task_id)
    idx_join = baseline_order.index(tasks["task_join"].task_id)

    assert idx_root < idx_alpha
    assert idx_root < idx_beta
    assert idx_alpha < idx_join
    assert idx_beta < idx_join

    print(f"\n[TEST 5 VERIFICATION] dispatch_order() is 100% deterministic across 50 iterations: {baseline_order}")


# ===========================================================================
# Test 6: Replan-storm bound (GRAPH-INV-3)
# ===========================================================================

@pytest.mark.asyncio
async def test_6_replan_storm_bound_prevents_loop():
    """
    GRAPH-INV-3: Replan-storm bound.
    Drives event_id generation through the REAL mechanism (on_field_evicted -> _build_replan_summary)
    and verifies that duplicate cascades for the SAME originating eviction event are suppressed,
    while a genuine subsequent correction event (new epoch) successfully cascades.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    current_values = {"x": "val1"}
    b_compensated: list[Any] = []

    async def b_compensate(res):
        b_compensated.append(res)

    executors = {
        "tool_a": _simple_executor({"x": "out"}),
        "tool_b": _simple_executor({"b": "done"}),
    }

    tm = _HarnessTaskManager(
        saga=saga,
        resolve_field=lambda f: current_values.get(f),
        executors=executors,
    )

    task_a = tm.create_task("task_a", [
        TaskStep(
            tool_name="tool_a",
            required_fields=["x"],
            build_args=lambda t, i: {"x": current_values["x"]},
            kind="write",
            compensate=_simple_executor(None),
        )
    ])
    task_b = tm.create_task("task_b", [
        TaskStep(
            tool_name="tool_b",
            required_fields=["x"],
            build_args=lambda t, i: {"b_in": current_values["x"]},
            kind="write",
            compensate=b_compensate,
        )
    ])

    graph = TaskGraph("replan_bound_graph")
    graph.add_task(task_a.task_id, task_a)
    graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: {"x": "b_in"}})

    # Run graph to completion
    await graph.run(tm)
    assert task_a.state == TaskState.COMPLETED
    assert task_b.state == TaskState.COMPLETED
    assert len(b_compensated) == 0

    # 1. Trigger the REAL eviction pipeline:
    current_values["x"] = "val2"
    tm.on_field_evicted("x", "val1")

    # Allow async replan to complete
    await asyncio.sleep(0.08)

    # Task B cascaded once
    assert len(b_compensated) == 1, f"Expected 1 compensation, got {len(b_compensated)}"
    real_event_id = getattr(task_a, "_replan_event_id", None)
    assert real_event_id is not None
    assert "evict-x-val1" in real_event_id

    # 2. Simulate a storm / duplicate delivery of the SAME originating event:
    # (e.g. late retry, circular notification, or duplicate callback with the real event payload)
    summary_duplicate = tm._build_replan_summary(task_a, "x")
    summary_duplicate["_event_id"] = real_event_id
    summary_duplicate["_evicted_values"] = {"x": "val1"}

    duplicate_cascaded = await graph.on_upstream_replanned(task_a.task_id, summary_duplicate)
    # The real bound MUST suppress this duplicate cascade:
    assert duplicate_cascaded == [], "Replan-storm bound must suppress duplicate cascade for the same event"
    assert len(b_compensated) == 1, "Compensation count must remain 1 (no duplicate compensation)"

    # 3. Contrast with a GENUINE subsequent correction event (new epoch, new eviction):
    await clock.advance(reason="second_correction")
    current_values["x"] = "val3"
    tm.on_field_evicted("x", "val2")

    await asyncio.sleep(0.08)

    # Genuine new event DOES cascade:
    assert len(b_compensated) == 2, f"Expected 2 compensations after genuine second correction, got {len(b_compensated)}"
    second_event_id = getattr(task_a, "_replan_event_id", None)
    assert second_event_id != real_event_id
    assert "epoch-1" in second_event_id

    print(f"\n[TEST 6 VERIFICATION] Replan-storm bound verified with REAL pipeline event generation: duplicate suppressed for {real_event_id}, second event {second_event_id} succeeded.")


# ===========================================================================
# Test 7: GraphState correctly derivable from constituent task states
# ===========================================================================

@pytest.mark.asyncio
async def test_7_graph_state_derivation():
    """
    GraphState is always dynamically derivable from constituent task states
    with zero independent drift.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    tm = TaskManager(saga=saga, resolve_field=lambda f: "val")

    task_a = tm.create_task("a", [TaskStep(tool_name="a", kind="write")])
    task_b = tm.create_task("b", [TaskStep(tool_name="b", kind="write")])

    graph = TaskGraph("state_test")
    graph._task_manager = tm

    # Empty graph
    assert TaskGraph().state == GraphState.CREATED

    graph.add_task(task_a.task_id, task_a)
    graph.add_task(task_b.task_id, task_b)

    # 1. Both PENDING -> CREATED
    assert graph.state == GraphState.CREATED

    # 2. One RUNNING -> RUNNING
    task_a.state = TaskState.RUNNING
    assert graph.state == GraphState.RUNNING

    # 3. One COMPLETED, one PENDING -> PARTIALLY_COMPLETED
    task_a.state = TaskState.COMPLETED
    task_b.state = TaskState.PENDING
    assert graph.state == GraphState.PARTIALLY_COMPLETED

    # 4. Both COMPLETED -> COMPLETED
    task_b.state = TaskState.COMPLETED
    assert graph.state == GraphState.COMPLETED

    # 5. One CANCELLED -> CANCELLED
    task_b.state = TaskState.CANCELLED
    assert graph.state == GraphState.CANCELLED

    # 6. One FAILED -> FAILED
    task_a.state = TaskState.FAILED
    assert graph.state == GraphState.FAILED

    print("\n[TEST 7 VERIFICATION] GraphState correctly derived across all 6 lifecycle permutations.")


# ===========================================================================
# Test 8: Checkpoint/Restore round-trip
# ===========================================================================

@pytest.mark.asyncio
async def test_8_checkpoint_restore_round_trip():
    """
    Serialize a graph mid-run and restore it via a fresh independently-constructed
    TaskManager using CheckpointStore (same rigor as Phase 7 test_7b/7d).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileCheckpointStore(tmpdir)
        clock1 = TurnEpochClock()
        saga1 = SpeculativeSagaManager(clock1)

        executors1 = {
            "step_a": _simple_executor({"res_a": "ok"}),
            "step_b": _simple_executor({"res_b": "ok"}),
        }

        tm1 = _HarnessTaskManager(
            saga=saga1,
            resolve_field=lambda f: "val",
            executors=executors1,
            store=store,
        )

        steps_a = [TaskStep(tool_name="step_a", kind="write")]
        steps_b = [TaskStep(tool_name="step_b", kind="write")]

        task_a = tm1.create_task("task_a", steps_a)
        task_b = tm1.create_task("task_b", steps_b)

        graph1 = TaskGraph("graph_checkpoint_test")
        graph1.add_task(task_a.task_id, task_a)
        graph1.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: ["x"]})

        # Run task A to completion, B not yet run
        await tm1.run_task(task_a.task_id)
        assert task_a.state == TaskState.COMPLETED
        assert task_b.state == TaskState.PENDING

        # Serialize graph
        graph_cp = graph1.checkpoint()
        assert graph_cp["node_ids"] == [task_a.task_id, task_b.task_id]
        assert graph_cp["edges"][task_b.task_id] == [task_a.task_id]
        assert graph_cp["field_dependencies"][task_b.task_id][task_a.task_id] == {"x": "x"}

        # -------------------------------------------------------------
        # Process restart simulation: discard tm1 and graph1 completely
        # -------------------------------------------------------------
        clock2 = TurnEpochClock()
        saga2 = SpeculativeSagaManager(clock2)
        executors2 = {
            "step_a": _simple_executor({"res_a": "ok"}),
            "step_b": _simple_executor({"res_b": "ok"}),
        }
        tm2 = _HarnessTaskManager(
            saga=saga2,
            resolve_field=lambda f: "val",
            executors=executors2,
            store=store,
        )

        step_defs = {
            task_a.task_id: [TaskStep(tool_name="step_a", kind="write")],
            task_b.task_id: [TaskStep(tool_name="step_b", kind="write")],
        }

        restored_graph = TaskGraph.restore(graph_cp, tm2, step_definitions=step_defs)

        assert restored_graph.graph_id == graph1.graph_id
        assert set(restored_graph.nodes.keys()) == {task_a.task_id, task_b.task_id}
        assert restored_graph.edges == graph1.edges
        assert restored_graph.field_dependencies == graph1.field_dependencies

        # Assert restored task states match
        restored_a = restored_graph.nodes[task_a.task_id]
        restored_b = restored_graph.nodes[task_b.task_id]
        assert restored_a.state == TaskState.COMPLETED
        assert restored_a.step_results[0] == {"res_a": "ok"}
        assert restored_b.state == TaskState.PENDING

        # Run remaining graph tasks on fresh TaskManager
        final_state = await restored_graph.run(tm2)
        assert final_state == GraphState.COMPLETED
        assert restored_b.state == TaskState.COMPLETED

        print(f"\n[TEST 8 VERIFICATION] Checkpoint/restore round-trip verified across fresh TaskManager. Final graph state: {final_state.name}")


# ===========================================================================
# Test 9: Full Regression Suite Check
# ===========================================================================

def test_9_task_manager_callback_non_regression():
    """
    Confirms TaskManager callback signature does not disrupt default creation
    when on_task_replanned is omitted.
    """
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    tm = TaskManager(saga=saga, resolve_field=lambda f: "val")
    assert tm.on_task_replanned is None
    print("\n[TEST 9 VERIFICATION] TaskManager backwards compatibility confirmed with None callback default.")
