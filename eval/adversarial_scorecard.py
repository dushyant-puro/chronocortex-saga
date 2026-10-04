"""
eval/adversarial_scorecard.py

Adversarial evaluation scorecard runner (Phase 13).
Executes composite scenarios multiple times deterministically, instrumenting
and aggregating real counted events (zero estimation / zero extrapolation):
  - Stale-action rate: COMMITTED_STALE or equivalent incorrect-commit events / total actions
  - Duplicate-action rate: tools executed more times than intended dispatches
  - Recovery rate: fraction of scenarios reaching the correct terminal state despite injection
  - False-commitment rate: committed writes whose data was retracted before dispatch
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Ensure repo root and tests dir in sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "tests"))

from runtime.grounding_guard import GroundingGuard
from runtime.persistence import FileCheckpointStore
from runtime.policy_engine import (
    AuthorizationContext,
    ConfirmationRequiredError,
    PermissionDeniedError,
    PolicyEngine,
)
from runtime.speculative_saga import (
    ActionKind,
    ActionState,
    SpeculativeSagaManager,
    StagedAction,
    TurnEpochClock,
)
from runtime.task_engine import (
    StepState,
    Task,
    TaskManager,
    TaskState,
    TaskStep,
)
from runtime.task_graph import GraphState, TaskGraph
from runtime.tool_contract import ToolManifest
from test_phase13_adversarial_eval import AdversarialTaskManager


@dataclass
class ScorecardMetrics:
    total_actions: int = 0
    stale_actions: int = 0
    duplicate_executions: int = 0
    scenarios_attempted: int = 0
    scenarios_recovered: int = 0
    false_commitments: int = 0


async def run_scenario_a(metrics: ScorecardMetrics) -> bool:
    """Scenario A: Full fleet task with policy gate."""
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    policy_engine = PolicyEngine()

    manifest_reroute = ToolManifest(
        tool_name="reroute_truck",
        kind="write",
        requires_confirmation=True,
    )

    reroute_calls = 0

    async def exec_telemetry(args):
        return {"truck_id": "t17", "speed": 85}

    async def exec_traffic(args):
        return {"route": "Chennai", "traffic": "clear"}

    async def exec_dock(args):
        return {"dock_id": "D1", "status": "open"}

    async def exec_reroute(args):
        nonlocal reroute_calls
        reroute_calls += 1
        return {"rerouted": True, "truck_id": args.get("truck_id")}

    async def exec_reserve(args):
        return {"reserved": True, "dock_id": args.get("dock_id")}

    executors = {
        "reroute_truck": exec_reroute,
        "reserve_dock": exec_reserve,
    }
    read_executors = {
        "query_telemetry": exec_telemetry,
        "query_traffic": exec_traffic,
        "query_dock": exec_dock,
    }
    manifests = {"reroute_truck": manifest_reroute}

    tm = AdversarialTaskManager(
        saga=saga,
        resolve_field=lambda f: "val",
        executors=executors,
        read_executors=read_executors,
        manifests=manifests,
        policy_engine=policy_engine,
        confirmed=False,
    )

    steps = [
        TaskStep(tool_name="query_telemetry", build_args=lambda t, i: {"truck_id": "t17"}, kind="read"),
        TaskStep(tool_name="query_traffic", build_args=lambda t, i: {"route": "Chennai"}, kind="read"),
        TaskStep(tool_name="query_dock", build_args=lambda t, i: {"dock_id": "D1"}, kind="read"),
        TaskStep(tool_name="reroute_truck", build_args=lambda t, i: {"truck_id": "t17", "destination": "Chennai"}, kind="write"),
        TaskStep(tool_name="reserve_dock", build_args=lambda t, i: {"dock_id": "D1"}, kind="write"),
    ]

    task = tm.create_task("fleet_5_step", steps)

    denial_occurred = False
    try:
        await tm.run_task(task.task_id)
    except ConfirmationRequiredError:
        denial_occurred = True

    tm.confirmed = True
    final_task = await tm.run_task(task.task_id)

    # Instrument actions
    for a in saga._actions.values():
        metrics.total_actions += 1
        if a.state == ActionState.COMMITTED_STALE:
            metrics.stale_actions += 1

    if reroute_calls > 1:
        metrics.duplicate_executions += (reroute_calls - 1)

    recovered = (
        denial_occurred
        and final_task.state == TaskState.COMPLETED
        and reroute_calls == 1
    )
    return recovered


async def run_scenario_b(metrics: ScorecardMetrics) -> bool:
    """Scenario B: Correction storm inside active task graph."""
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)

    current_grounded_val = "val0"
    b_dispatches: list[dict[str, Any]] = []
    b_compensations: list[Any] = []

    async def a_compensate(res):
        pass

    async def b_compensate(res):
        b_compensations.append(res)

    async def exec_a(args):
        return {"destination": args.get("destination")}

    async def exec_b(args):
        b_dispatches.append(dict(args))
        return {"dock": "assigned", "dest": args.get("b_dest")}

    executors = {"tool_a": exec_a, "tool_b": exec_b}

    tm = AdversarialTaskManager(
        saga=saga,
        resolve_field=lambda f: current_grounded_val,
        executors=executors,
    )
    guard = GroundingGuard(epoch_clock=clock, on_eviction=tm.on_field_evicted)

    task_a = tm.create_task("task_a", [
        TaskStep(
            tool_name="tool_a",
            required_fields=["destination"],
            build_args=lambda t, i: {"destination": current_grounded_val},
            kind="write",
            compensate=a_compensate,
        )
    ])
    task_b = tm.create_task("task_b", [
        TaskStep(
            tool_name="tool_b",
            required_fields=["destination"],
            build_args=lambda t, i: {"b_dest": current_grounded_val},
            kind="write",
            compensate=b_compensate,
        )
    ])

    graph = TaskGraph("graph_correction_storm")
    graph.add_task(task_a.task_id, task_a)
    graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: {"destination": "b_dest"}})

    guard.ingest_token("val0", 0.95, 0.0, 0.1)
    guard.stage_candidate("destination", "val0", (0, 1))
    await graph.run(tm)

    corrections = ["val1", "val2", "val3", "val4", "val5"]
    prev_val = "val0"
    for i, c in enumerate(corrections, start=1):
        tok_idx = guard.ingest_token(c, 0.95, float(i), float(i) + 0.1)
        current_grounded_val = c
        tm.on_field_evicted("destination", prev_val)
        guard.stage_candidate("destination", c, (tok_idx, tok_idx + 1))
        prev_val = c

    await asyncio.sleep(0.08)

    for a in saga._actions.values():
        metrics.total_actions += 1
        if a.state == ActionState.COMMITTED_STALE:
            metrics.stale_actions += 1

    # False-commitment check: did any final committed step have a retracted value?
    final_a_val = task_a.step_results[0].get("destination") if task_a.step_results[0] else None
    final_b_val = b_dispatches[-1].get("b_dest") if b_dispatches else None
    if final_a_val != "val5" or final_b_val != "val5":
        metrics.false_commitments += 1

    recovered = (
        task_a.state == TaskState.COMPLETED
        and task_b.state == TaskState.COMPLETED
        and final_a_val == "val5"
        and final_b_val == "val5"
    )
    return recovered


async def run_scenario_c(metrics: ScorecardMetrics) -> bool:
    """Scenario C: Policy denial during an active cascade."""
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    policy_engine = PolicyEngine()

    manifest_b = ToolManifest(
        tool_name="admin_tool_b",
        kind="write",
        required_permissions=["fleet:admin"],
    )

    current_val = "initial_val"
    b_dispatches: list[dict[str, Any]] = []

    async def exec_a(args):
        return {"field_x": current_val}

    async def exec_b(args):
        b_dispatches.append(dict(args))
        return {"b_done": True}

    async def b_compensate(res):
        pass

    executors = {"tool_a": exec_a, "admin_tool_b": exec_b}
    manifests = {"admin_tool_b": manifest_b}
    admin_context = AuthorizationContext(granted_permissions={"fleet:admin"})
    unauthorized_context = AuthorizationContext(granted_permissions={"fleet:viewer"})

    tm = AdversarialTaskManager(
        saga=saga,
        resolve_field=lambda f: current_val,
        executors=executors,
        manifests=manifests,
        policy_engine=policy_engine,
        auth_context=admin_context,
    )

    task_a = tm.create_task("task_a", [
        TaskStep(tool_name="tool_a", required_fields=["field_x"], build_args=lambda t, i: {"field_x": current_val}, kind="write")
    ])
    task_b = tm.create_task("task_b", [
        TaskStep(tool_name="admin_tool_b", required_fields=["field_x"], build_args=lambda t, i: {"field_x": current_val}, kind="write", compensate=b_compensate)
    ])

    graph = TaskGraph("graph_policy_cascade")
    graph.add_task(task_a.task_id, task_a)
    graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: ["field_x"]})

    await graph.run(tm)

    tm.auth_context = unauthorized_context
    current_val = "updated_val"

    denial_occurred = False
    try:
        await graph.on_upstream_replanned(
            task_a.task_id,
            {"field_x": "updated_val", "_evicted_values": {"field_x": "initial_val"}},
            event_id="cascade_scorecard_perm_denial",
        )
    except PermissionDeniedError:
        denial_occurred = True

    for a in saga._actions.values():
        metrics.total_actions += 1
        if a.state == ActionState.COMMITTED_STALE:
            metrics.stale_actions += 1

    recovered = (
        denial_occurred
        and task_b.state == TaskState.FAILED
        and len(b_dispatches) == 1
    )
    return recovered


async def run_scenario_d(metrics: ScorecardMetrics) -> bool:
    """Scenario D: IN_FLIGHT_UNKNOWN across simulated restart inside graph."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileCheckpointStore(tmpdir)
        task_a_id = "task-a-scorecard"
        task_b_id = "task-b-scorecard"

        cp_a = {
            "task_id": task_a_id,
            "name": "task_a",
            "state": "RUNNING",
            "current_step_index": 0,
            "step_states": ["IN_FLIGHT"],
            "step_results": [None],
            "step_action_ids": ["write-action-crash"],
            "step_dispatched_args": [{"truck_id": "t17"}],
            "error": None,
        }
        cp_b = {
            "task_id": task_b_id,
            "name": "task_b",
            "state": "PENDING",
            "current_step_index": 0,
            "step_states": ["PENDING"],
            "step_results": [None],
            "step_action_ids": [None],
            "step_dispatched_args": [None],
            "error": None,
        }
        store.save(task_a_id, cp_a)
        store.save(task_b_id, cp_b)

        clock2 = TurnEpochClock()
        saga2 = SpeculativeSagaManager(clock2)

        async def status_check(action):
            action.result = {"verified": True}
            return True

        b_executed = False

        async def exec_b(args):
            nonlocal b_executed
            b_executed = True
            return {"dock": "ok"}

        steps_a = [TaskStep(tool_name="tool_a", kind="write", status_check=status_check)]
        steps_b = [TaskStep(tool_name="tool_b", kind="write")]

        tm2 = AdversarialTaskManager(
            saga=saga2,
            resolve_field=lambda f: "val",
            executors={"tool_b": exec_b},
            store=store,
        )

        restored_a = tm2.restore_task(task_a_id, steps=steps_a)
        restored_b = tm2.restore_task(task_b_id, steps=steps_b)

        graph2 = TaskGraph("graph_scorecard_restart")
        graph2.add_task(task_a_id, restored_a)
        graph2.add_task(task_b_id, restored_b, depends_on={task_a_id: ["verified"]})

        final_state = await graph2.run(tm2)

        for a in saga2._actions.values():
            metrics.total_actions += 1
            if a.state == ActionState.COMMITTED_STALE:
                metrics.stale_actions += 1

        recovered = (
            restored_a.state == TaskState.COMPLETED
            and restored_b.state == TaskState.COMPLETED
            and b_executed
            and final_state == GraphState.COMPLETED
        )
        return recovered


async def main() -> None:
    print("================================================================================")
    print("        CHRONOCORTEX-SAGA / PRISM — ADVERSARIAL EVALUATION SCORECARD            ")
    print("================================================================================")

    metrics = ScorecardMetrics()
    iterations_per_scenario = 5

    scenarios = [
        ("Scenario A (Fleet Task Policy Gate)", run_scenario_a),
        ("Scenario B (Correction Storm in TaskGraph)", run_scenario_b),
        ("Scenario C (Policy Denial in Active Cascade)", run_scenario_c),
        ("Scenario D (IN_FLIGHT_UNKNOWN Across Restart)", run_scenario_d),
    ]

    for name, runner in scenarios:
        scenario_successes = 0
        for _ in range(iterations_per_scenario):
            metrics.scenarios_attempted += 1
            ok = await runner(metrics)
            if ok:
                metrics.scenarios_recovered += 1
                scenario_successes += 1
        print(f"  [RUN] {name:<45} : {scenario_successes}/{iterations_per_scenario} passed")

    stale_rate = metrics.stale_actions / max(1, metrics.total_actions)
    dup_rate = metrics.duplicate_executions / max(1, metrics.total_actions)
    recovery_rate = metrics.scenarios_recovered / max(1, metrics.scenarios_attempted)
    false_commit_rate = metrics.false_commitments / max(1, metrics.total_actions)

    print("\n--------------------------------------------------------------------------------")
    print("                          REAL COUNTED METRICS SUMMARY                          ")
    print("--------------------------------------------------------------------------------")
    print(f"Total Evaluated Actions       : {metrics.total_actions}")
    print(f"Total Scenarios Evaluated     : {metrics.scenarios_attempted}")
    print(f"Total Scenarios Recovered     : {metrics.scenarios_recovered}")
    print(f"Stale Actions (COMMITTED_STALE): {metrics.stale_actions}")
    print(f"Duplicate Tool Executions     : {metrics.duplicate_executions}")
    print(f"False Commitments             : {metrics.false_commitments}")
    print("--------------------------------------------------------------------------------")
    print(f"STALE-ACTION RATE             : {stale_rate * 100:.2f}%  (target: 0.00%)")
    print(f"DUPLICATE-ACTION RATE         : {dup_rate * 100:.2f}%  (target: 0.00%)")
    print(f"RECOVERY RATE                 : {recovery_rate * 100:.2f}%  (target: 100.00%)")
    print(f"FALSE-COMMITMENT RATE         : {false_commit_rate * 100:.2f}%  (target: 0.00%)")
    print("================================================================================")


if __name__ == "__main__":
    asyncio.run(main())
