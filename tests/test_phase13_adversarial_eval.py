"""
tests/test_phase13_adversarial_eval.py

Phase 13: Adversarial Evaluation — Composite Scenarios across Saga, GroundingGuard,
TaskGraph, Persistence, and PolicyEngine.

4 Composite Scenarios:
  1a: Full fleet scenario with policy gate (5 steps, confirmation required, resumes to completion)
  1b: Correction storm inside an active task graph (5 rapid corrections via GroundingGuard)
  1c: Policy denial during an active cascade (permission denied on replan cascade, fails safe)
  1d: IN_FLIGHT_UNKNOWN across simulated restart inside a graph (reconciliation required)
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from typing import Any, Optional

import pytest
import pytest_asyncio

from runtime.grounding_guard import GroundingGuard
from runtime.persistence import FileCheckpointStore
from runtime.policy_engine import (
    AuthorizationContext,
    ConfirmationRequiredError,
    PermissionDeniedError,
    PolicyEngine,
    PolicyError,
)
from runtime.speculative_saga import (
    ActionKind,
    ActionState,
    SagaAbortedError,
    SpeculativeSagaManager,
    StagedAction,
    StaleEpochError,
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


# ---------------------------------------------------------------------------
# Test Harness
# ---------------------------------------------------------------------------

class AdversarialTaskManager(TaskManager):
    """
    Harness TaskManager composing TaskEngine with PolicyEngine and custom executors.
    """
    def __init__(
        self,
        saga: SpeculativeSagaManager,
        resolve_field: Any,
        executors: Optional[dict[str, Any]] = None,
        read_executors: Optional[dict[str, Any]] = None,
        manifests: Optional[dict[str, ToolManifest]] = None,
        policy_engine: Optional[PolicyEngine] = None,
        auth_context: Optional[AuthorizationContext] = None,
        confirmed: bool = False,
        store: Optional[Any] = None,
        on_task_replanned: Optional[Any] = None,
    ) -> None:
        super().__init__(saga, resolve_field, store=store, on_task_replanned=on_task_replanned)
        self._test_executors = executors or {}
        self._test_read_executors = read_executors or {}
        self.manifests = manifests or {}
        self.policy_engine = policy_engine
        self.auth_context = auth_context
        self.confirmed = confirmed

    async def _dispatch_write_step(self, task: Task, idx: int, step: TaskStep, args: dict[str, Any]) -> Any:
        executor = self._test_executors.get(step.tool_name)
        if executor is None:
            raise RuntimeError(f"No test executor for {step.tool_name}")
        manifest = self.manifests.get(step.tool_name)

        # If action was already staged and is PENDING (e.g. from prior confirmation denial),
        # reuse it rather than staging duplicate action on same idempotency key.
        action = None
        action_id = task.step_action_ids[idx]
        if action_id is not None:
            existing = self._saga._actions.get(action_id)
            if existing and existing.state == ActionState.PENDING:
                action = existing

        if action is None:
            action = self._saga.stage_write(
                step.tool_name,
                args,
                compensate=step.compensate,
                manifest=manifest,
            )
            task.step_action_ids[idx] = action.action_id

        result = await self._saga.commit_write(
            action.action_id,
            executor,
            manifest=manifest,
            policy_engine=self.policy_engine,
            auth_context=self.auth_context,
            confirmed=self.confirmed,
        )
        task.step_states[idx] = StepState.IN_FLIGHT
        return result

    async def _dispatch_read_step(self, task: Task, idx: int, step: TaskStep, args: dict[str, Any]) -> Any:
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
        raise SagaAbortedError(f"read {step.tool_name} did not commit")

    async def run_task(self, task_id: str) -> Task:
        task = self._tasks[task_id]
        if task.state == TaskState.CANCELLED:
            return task
        task.state = TaskState.RUNNING

        while task.current_step_index < len(task.steps):
            if task.state == TaskState.CANCELLED:
                for i in range(task.current_step_index, len(task.steps)):
                    task.step_states[i] = StepState.SKIPPED
                return task

            idx = task.current_step_index
            step = task.steps[idx]

            # NEEDS_RECONCILIATION check
            if task.step_states[idx] == StepState.NEEDS_RECONCILIATION:
                action_id = task.step_action_ids[idx]
                status_check = step.status_check
                if action_id is None or status_check is None:
                    task.step_states[idx] = StepState.FAILED
                    task.state = TaskState.FAILED
                    task.error = f"step {idx} ({step.tool_name}) NEEDS_RECONCILIATION missing status_check"
                    self._save_checkpoint(task)
                    return task

                # Ensure action exists in saga so reconcile_write can be called on it
                action = self._saga._actions.get(action_id)
                if action is None:
                    action = StagedAction(
                        action_id=action_id,
                        kind=ActionKind.STAGED_WRITE,
                        tool_name=step.tool_name,
                        args=task.step_dispatched_args[idx] or {},
                        capture_epoch=self._saga.epoch_clock.current,
                        idempotency_key=action_id,
                        compensate=step.compensate,
                        state=ActionState.IN_FLIGHT_UNKNOWN,
                    )
                    self._saga._actions[action_id] = action

                await self._saga.reconcile_write(action_id, status_check)
                action = self._saga._actions.get(action_id)
                if action and action.state in (ActionState.COMMITTED, ActionState.COMMITTED_STALE):
                    task.step_states[idx] = StepState.COMMITTED
                    task.step_results[idx] = action.result
                    task.current_step_index += 1
                    self._save_checkpoint(task)
                    continue
                else:
                    task.step_states[idx] = StepState.FAILED
                    task.state = TaskState.FAILED
                    self._save_checkpoint(task)
                    return task

            # Grounding check
            if not self._check_fields_resolved(step):
                task.step_states[idx] = StepState.WAITING_FOR_GROUNDING
                task.state = TaskState.WAITING_FOR_GROUNDING
                self._save_checkpoint(task)
                return task

            args = step.build_args(task, idx) if step.build_args else {}
            task.step_dispatched_args[idx] = dict(args)

            try:
                if step.kind == "write":
                    result = await self._dispatch_write_step(task, idx, step, args)
                else:
                    result = await self._dispatch_read_step(task, idx, step, args)
                task.step_results[idx] = result
                task.step_states[idx] = StepState.COMMITTED
                task.current_step_index += 1
                self._save_checkpoint(task)

            except ConfirmationRequiredError:
                # Confirmation requirement: action remains PENDING.
                # Task remains RUNNING / resumable pending operator confirmation.
                # Prior committed steps are preserved (NOT compensated).
                task.step_states[idx] = StepState.PENDING
                self._save_checkpoint(task)
                raise

            except PermissionDeniedError as exc:
                # Permission denial: security violation halts the task.
                task.step_states[idx] = StepState.FAILED
                task.state = TaskState.FAILED
                task.error = str(exc)
                self._save_checkpoint(task)
                raise

            except StaleEpochError:
                if not self._check_fields_resolved(step):
                    task.step_states[idx] = StepState.WAITING_FOR_GROUNDING
                    task.state = TaskState.WAITING_FOR_GROUNDING
                    self._save_checkpoint(task)
                    return task
                continue

            except Exception as exc:
                task.step_states[idx] = StepState.FAILED
                task.state = TaskState.FAILED
                task.error = f"step {idx} ({step.tool_name}): {exc}"
                task.completed_at = time.monotonic()
                await self._compensate_committed_steps(task, idx)
                self._save_checkpoint(task)
                return task

        task.state = TaskState.COMPLETED
        task.completed_at = time.monotonic()
        self._save_checkpoint(task)
        if getattr(task, "_replanning_fields", None):
            self._notify_task_replanned(task)
            task._replanning_fields.clear()
        return task


# ===========================================================================
# Scenario 1a: Full Fleet Scenario with Policy Gate
# ===========================================================================

@pytest.mark.asyncio
async def test_1a_full_fleet_scenario_with_policy_gate():
    """
    Scenario (a):
    Build the real 5-step fleet task:
      step 0: query_telemetry (read)
      step 1: query_traffic (read)
      step 2: query_dock (read)
      step 3: reroute_truck (write, requires_confirmation=True)
      step 4: reserve_dock (write)

    1. Dispatch with confirmed=False:
       - ConfirmationRequiredError surfaces cleanly.
       - Action remains PENDING (Phase 12 placement guarantee).
       - Steps 0-2 remain COMMITTED; step 3 remains PENDING; task remains RUNNING.
       - Reroute executor was called 0 times.
    2. Re-dispatch with confirmed=True:
       - Task resumes and completes all 5 steps to COMPLETED.
       - Reroute executor was called exactly 1 time total.
    """
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

    # 1. First dispatch: unconfirmed
    with pytest.raises(ConfirmationRequiredError):
        await tm.run_task(task.task_id)

    # Verify task state is NOT corrupted:
    assert task.state == TaskState.RUNNING, f"Task should remain RUNNING, got {task.state.name}"
    assert task.step_states[0] == StepState.COMMITTED
    assert task.step_states[1] == StepState.COMMITTED
    assert task.step_states[2] == StepState.COMMITTED
    assert task.step_states[3] == StepState.PENDING
    assert task.step_states[4] == StepState.PENDING

    # Verify StagedAction remains PENDING (Phase 12 placement guarantee)
    action_id = task.step_action_ids[3]
    assert action_id is not None
    action = saga._actions[action_id]
    assert action.state == ActionState.PENDING, f"Action must be PENDING, got {action.state.name}"
    assert reroute_calls == 0, "Executor must not have been called during denial"

    # 2. Operator confirms: re-dispatch with confirmed=True
    tm.confirmed = True
    final_task = await tm.run_task(task.task_id)

    assert final_task.state == TaskState.COMPLETED
    assert all(s == StepState.COMMITTED for s in final_task.step_states)
    assert reroute_calls == 1, f"Executor must be called exactly once total, got {reroute_calls}"
    print("\n[SCENARIO 1a VERIFICATION] Full fleet task policy gate passed: denied unconfirmed -> confirmed -> COMPLETED (1 execution).")


# ===========================================================================
# Scenario 1b: Correction Storm Inside an Active Task Graph
# ===========================================================================

@pytest.mark.asyncio
async def test_1b_correction_storm_inside_active_task_graph():
    """
    Scenario (b):
    2-task TaskGraph where Task B depends on Task A for field 'destination'.
    Initial run commits with 'val0'.
    5 corrections fire in rapid succession via GroundingGuard candidate staging
    ('val1' through 'val5').

    Assert:
      - Only the FINAL correction's value ('val5') ends up in both A and B.
      - Count actual compensate() calls fired on B.
      - Both A and B reach TaskState.COMPLETED.
    """
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

    guard = GroundingGuard(
        epoch_clock=clock,
        on_eviction=tm.on_field_evicted,
    )

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

    # 1. Initial run: both complete with val0
    guard.ingest_token("val0", 0.95, 0.0, 0.1)
    guard.stage_candidate("destination", "val0", (0, 1))
    await graph.run(tm)

    assert task_a.state == TaskState.COMPLETED
    assert task_b.state == TaskState.COMPLETED
    assert b_dispatches[0]["b_dest"] == "val0"

    # 2. Rapid correction storm: 5 corrections in succession
    corrections = ["val1", "val2", "val3", "val4", "val5"]
    prev_val = "val0"
    for i, c in enumerate(corrections, start=1):
        tok_idx = guard.ingest_token(c, 0.95, float(i), float(i) + 0.1)
        current_grounded_val = c
        # Evict old candidate and stage new candidate
        tm.on_field_evicted("destination", prev_val)
        guard.stage_candidate("destination", c, (tok_idx, tok_idx + 1))
        prev_val = c

    # Allow async replan cascades to resolve
    await asyncio.sleep(0.1)

    # 3. Assert only the FINAL value ('val5') is committed in both tasks
    assert task_a.state == TaskState.COMPLETED
    assert task_b.state == TaskState.COMPLETED
    assert task_a.step_results[0]["destination"] == "val5"
    assert b_dispatches[-1]["b_dest"] == "val5"

    actual_compensations = len(b_compensations)
    print(f"\n[SCENARIO 1b VERIFICATION] Correction storm complete: final committed value is 'val5'. Task B actual compensations: {actual_compensations}.")


# ===========================================================================
# Scenario 1c: Policy Denial During an Active Cascade
# ===========================================================================

@pytest.mark.asyncio
async def test_1c_policy_denial_during_active_cascade():
    """
    Scenario (c):
    TaskGraph where upstream Task A replans, cascading to Task B.
    Task B's tool requires permission 'fleet:admin'.
    During the cascade, caller's AuthorizationContext lacks 'fleet:admin'.

    Assert:
      - PermissionDeniedError surfaces cleanly from the cascade path.
      - Task B does not silently loop or falsely succeed.
      - Resulting TaskState is FAILED (explicit design decision documented).
    """
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

    executors = {"tool_a": exec_a, "admin_tool_b": exec_b}
    manifests = {"admin_tool_b": manifest_b}

    # Initial context has permission 'fleet:admin'
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

    async def b_compensate(res):
        pass

    task_a = tm.create_task("task_a", [
        TaskStep(tool_name="tool_a", required_fields=["field_x"], build_args=lambda t, i: {"field_x": current_val}, kind="write")
    ])
    task_b = tm.create_task("task_b", [
        TaskStep(tool_name="admin_tool_b", required_fields=["field_x"], build_args=lambda t, i: {"field_x": current_val}, kind="write", compensate=b_compensate)
    ])

    graph = TaskGraph("graph_policy_cascade")
    graph.add_task(task_a.task_id, task_a)
    graph.add_task(task_b.task_id, task_b, depends_on={task_a.task_id: ["field_x"]})

    # Initial run: both complete with permission
    await graph.run(tm)
    assert task_a.state == TaskState.COMPLETED
    assert task_b.state == TaskState.COMPLETED
    assert len(b_dispatches) == 1

    # Revoke permission before cascade
    tm.auth_context = unauthorized_context

    # Upstream replan: field_x changes to 'updated_val'
    current_val = "updated_val"

    # Trigger cascade with real exact-match evicted values
    with pytest.raises(PermissionDeniedError) as exc_info:
        await graph.on_upstream_replanned(
            task_a.task_id,
            {"field_x": "updated_val", "_evicted_values": {"field_x": "initial_val"}},
            event_id="cascade_event_perm_denial",
        )

    assert "fleet:admin" in str(exc_info.value)
    # Explicit design decision: Task B resulting state is TaskState.FAILED
    assert task_b.state == TaskState.FAILED
    assert task_b.step_states[0] == StepState.FAILED
    assert len(b_dispatches) == 1, "Disallowed redispatch must not execute"
    print("\n[SCENARIO 1c VERIFICATION] PermissionDeniedError surfaced cleanly during cascade; Task B set to TaskState.FAILED.")


# ===========================================================================
# Scenario 1d: IN_FLIGHT_UNKNOWN Across Simulated Restart Inside Graph
# ===========================================================================

@pytest.mark.asyncio
async def test_1d_in_flight_unknown_across_restart_inside_graph():
    """
    Scenario (d):
    TaskGraph with Task A and downstream Task B.
    Task A's step goes IN_FLIGHT_UNKNOWN before a simulated process restart.
    After restore from FileCheckpointStore:
      - Task A's step is NEEDS_RECONCILIATION.
      - Graph does NOT auto-retry without reconciliation.
      - Explicit reconcile_write resolves it, after which graph proceeds to COMPLETED.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileCheckpointStore(tmpdir)
        clock1 = TurnEpochClock()
        saga1 = SpeculativeSagaManager(clock1)

        task_a_id = "task-a-restart"
        task_b_id = "task-b-restart"

        # 1. Process 1: Task A step 0 was IN_FLIGHT when process crashed
        cp_a = {
            "task_id": task_a_id,
            "name": "task_a",
            "state": "RUNNING",
            "current_step_index": 0,
            "step_states": ["IN_FLIGHT"],
            "step_results": [None],
            "step_action_ids": ["write-action-proc1"],
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

        # 2. Process 2: Fresh TaskManager + Fresh TaskGraph
        clock2 = TurnEpochClock()
        saga2 = SpeculativeSagaManager(clock2)

        reconcile_called = False

        async def status_check(action):
            nonlocal reconcile_called
            reconcile_called = True
            action.result = {"verified": True}
            return True

        b_executed = False

        async def exec_b(args):
            nonlocal b_executed
            b_executed = True
            return {"dock": "ok"}

        steps_a = [
            TaskStep(
                tool_name="tool_a",
                kind="write",
                status_check=status_check,
            )
        ]
        steps_b = [
            TaskStep(
                tool_name="tool_b",
                kind="write",
            )
        ]

        tm2 = AdversarialTaskManager(
            saga=saga2,
            resolve_field=lambda f: "val",
            executors={"tool_b": exec_b},
            store=store,
        )

        restored_a = tm2.restore_task(task_a_id, steps=steps_a)
        restored_b = tm2.restore_task(task_b_id, steps=steps_b)

        assert restored_a is not None
        assert restored_b is not None

        # Assert step 0 is NEEDS_RECONCILIATION
        assert restored_a.step_states[0] == StepState.NEEDS_RECONCILIATION

        graph2 = TaskGraph("graph_proc2")
        graph2.add_task(task_a_id, restored_a)
        graph2.add_task(task_b_id, restored_b, depends_on={task_a_id: ["verified"]})

        # Run graph — Task A step 0 will trigger reconcile_write via its status_check
        final_state = await graph2.run(tm2)

        assert reconcile_called, "Explicit status_check reconciliation must have been called"
        assert restored_a.state == TaskState.COMPLETED
        assert restored_a.step_states[0] == StepState.COMMITTED
        assert restored_b.state == TaskState.COMPLETED
        assert b_executed, "Downstream Task B must have dispatched after Task A reconciled"
        assert final_state == GraphState.COMPLETED
        print("\n[SCENARIO 1d VERIFICATION] Process restart verified: NEEDS_RECONCILIATION -> explicit reconcile_write -> Graph COMPLETED.")
