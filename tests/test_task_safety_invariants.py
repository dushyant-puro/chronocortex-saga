"""Production task-loop regressions. All provider effects here are SIMULATED."""

import asyncio

import pytest

from runtime.speculative_saga import ActionState, SpeculativeSagaManager, TurnEpochClock
from runtime.task_engine import StepState, TaskManager, TaskState, TaskStep
from runtime.policy_engine import ConfirmationRequiredError, PolicyEngine
from runtime.tool_contract import ToolManifest


class ExecutorBindings(TaskManager):
    """Bind tools only; never replace dispatch or task lifecycle logic."""

    def __init__(self, saga, executors, **kwargs):
        super().__init__(saga, resolve_field=lambda _: "grounded", **kwargs)
        self.executors = executors

    def _get_executor(self, step):
        return self.executors[step.tool_name]


async def settle(awaitable):
    """Watchdog only; test ordering is controlled by events, not elapsed time."""
    return await asyncio.wait_for(awaitable, timeout=3)


@pytest.mark.asyncio
async def test_late_stale_result_never_completes_task_or_dispatches_descendant():
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    started, release = asyncio.Event(), asyncio.Event()
    effects = []

    async def mutate(args):
        started.set()
        await release.wait()
        effects.append("obsolete route applied")
        return {"operation_id": "remote-1"}

    async def descendant(args):
        effects.append("descendant")

    manager = ExecutorBindings(saga, {"mutate": mutate, "descendant": descendant})
    task = manager.create_task("corrected", [TaskStep("mutate"), TaskStep("descendant")])
    running = asyncio.create_task(manager.run_task(task.task_id))
    await settle(started.wait())
    await clock.advance("correction")
    release.set()
    await settle(running)

    action_id = task.step_action_ids[0]
    assert saga._actions[action_id].state == ActionState.COMMITTED_STALE
    assert task.state.name == "NEEDS_RECONCILIATION"
    assert task.step_states[0] == StepState.NEEDS_RECONCILIATION
    assert task.current_step_index == 0
    assert task.completed_at is None
    assert effects == ["obsolete route applied"]
    await manager.resume_task(task.task_id)
    assert task.step_action_ids[0] == action_id
    assert effects == ["obsolete route applied"]


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [None, "status_error"])
async def test_resume_keeps_unknown_identity_without_redispatch_or_compensation(outcome):
    saga = SpeculativeSagaManager(TurnEpochClock())
    effects, checks, compensations = [], [], []

    async def mutate(args):
        effects.append("duplicate mutation")

    async def undo(result):
        compensations.append(result)

    async def status(action):
        checks.append(action.action_id)
        if outcome == "status_error":
            raise ConnectionError("status service unavailable")
        return None

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("unknown", [TaskStep("prior", compensate=undo),
                                           TaskStep("mutate", status_check=status)])
    task.step_states[0] = StepState.COMMITTED
    task.step_results[0] = {"prior_effect": "still valid"}
    task.current_step_index = 1
    action = saga.stage_write("mutate", {"destination": "Chennai"})
    action.state = ActionState.IN_FLIGHT_UNKNOWN
    task.state = TaskState.RUNNING
    task.step_states[1] = StepState.NEEDS_RECONCILIATION
    task.step_action_ids[1] = action.action_id
    task.step_dispatched_args[1] = dict(action.args)

    for _ in range(2):
        await manager.resume_task(task.task_id)
        assert task.state.name == "NEEDS_RECONCILIATION"
        assert task.step_states[1] == StepState.NEEDS_RECONCILIATION
        assert task.step_action_ids[1] == action.action_id
        assert task.current_step_index == 1
        assert task.completed_at is None
        assert manager.snapshot()[0]["state"] == "NEEDS_RECONCILIATION"
        assert manager.checkpoint(task.task_id)["step_action_ids"][1] == action.action_id
    assert checks == [action.action_id, action.action_id]
    assert effects == compensations == []
    assert task.step_states[0] == StepState.COMMITTED


@pytest.mark.asyncio
async def test_concurrent_run_and_resume_dispatch_once():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release, contender = asyncio.Event(), asyncio.Event(), asyncio.Event()
    effects = []

    async def mutate(args):
        effects.append("one effect")
        started.set()
        await release.wait()
        return {"operation_id": "one"}

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("same task", [TaskStep("mutate")])
    first = asyncio.create_task(manager.run_task(task.task_id))
    await settle(started.wait())

    async def resume():
        contender.set()
        return await manager.resume_task(task.task_id)

    second = asyncio.create_task(resume())
    await settle(contender.wait())
    assert not second.done(), "A concurrent resume must wait for the active task runner"
    assert task.state == TaskState.RUNNING
    assert task.step_states[0] == StepState.IN_FLIGHT
    release.set()
    await settle(asyncio.gather(first, second))
    assert effects == ["one effect"]
    assert task.state == TaskState.COMPLETED
    assert task.current_step_index == 1
    assert len(saga._actions) == 1


@pytest.mark.asyncio
async def test_explicit_cancellation_during_last_write_is_not_overwritten():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release = asyncio.Event(), asyncio.Event()
    effects = []

    async def mutate(args):
        started.set()
        await release.wait()
        effects.append("applied despite local cancellation")
        return {"operation_id": "cancelled-task-effect"}

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("cancel", [TaskStep("mutate")])
    running = asyncio.create_task(manager.run_task(task.task_id))
    await settle(started.wait())
    await manager.cancel_task(task.task_id)
    release.set()
    await settle(running)
    assert task.state == TaskState.CANCELLED
    assert task.step_states[0] == StepState.NEEDS_RECONCILIATION
    assert task.step_results[0] == {"operation_id": "cancelled-task-effect"}
    assert task.current_step_index == 0
    assert effects == ["applied despite local cancellation"]


@pytest.mark.asyncio
async def test_permission_denial_does_not_compensate_authorized_prior_step(monkeypatch):
    from runtime.policy_engine import AuthorizationContext, PermissionDeniedError

    saga = SpeculativeSagaManager(TurnEpochClock())
    effects, undos = [], []
    original_commit = saga.commit_write
    manifest = ToolManifest("mutate", kind="write", required_permissions=["fleet.write"])

    async def with_policy(action_id, executor, **kwargs):
        return await original_commit(action_id, executor, manifest=manifest,
                                     policy_engine=PolicyEngine(), auth_context=AuthorizationContext())

    monkeypatch.setattr(saga, "commit_write", with_policy)

    async def mutate(args):
        effects.append("unauthorized")

    async def undo(result):
        undos.append(result)

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("denied", [TaskStep("prior", compensate=undo), TaskStep("mutate")])
    task.step_states[0] = StepState.COMMITTED
    task.step_results[0] = {"prior": "authorized"}
    task.current_step_index = 1
    with pytest.raises(PermissionDeniedError):
        await manager.run_task(task.task_id)
    assert task.state == TaskState.FAILED
    assert task.step_states[0] == StepState.COMMITTED
    assert saga._actions[task.step_action_ids[1]].state == ActionState.PENDING
    assert effects == undos == []


@pytest.mark.asyncio
async def test_cancelling_runner_keeps_dispatched_write_unknown():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started = asyncio.Event()

    async def mutate(args):
        started.set()
        await asyncio.Event().wait()

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("runner cancellation", [TaskStep("mutate")])
    running = asyncio.create_task(manager.run_task(task.task_id))
    await settle(started.wait())
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert task.state.name == "NEEDS_RECONCILIATION"
    assert task.step_states[0] == StepState.NEEDS_RECONCILIATION
    assert saga._actions[task.step_action_ids[0]].state == ActionState.IN_FLIGHT_UNKNOWN


@pytest.mark.asyncio
async def test_reconciled_current_success_advances_without_repeating_mutation():
    saga = SpeculativeSagaManager(TurnEpochClock())
    checks, effects = [], []

    async def status(action):
        checks.append(action.action_id)
        action.result = {"operation_id": "confirmed-remote"}
        return True

    async def mutate(args):
        effects.append("must not dispatch")

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("resolved", [TaskStep("mutate", status_check=status)])
    action = saga.stage_write("mutate", {})
    action.state = ActionState.IN_FLIGHT_UNKNOWN
    task.state = TaskState.RUNNING
    task.step_states[0] = StepState.NEEDS_RECONCILIATION
    task.step_action_ids[0] = action.action_id
    await manager.resume_task(task.task_id)
    assert task.state == TaskState.COMPLETED
    assert task.step_results == [{"operation_id": "confirmed-remote"}]
    assert checks == [action.action_id]
    assert effects == []


@pytest.mark.asyncio
async def test_pending_confirmation_preserves_prior_progress_and_reuses_action(monkeypatch):
    saga = SpeculativeSagaManager(TurnEpochClock())
    effects, undos = [], []
    approval = {"confirmed": False}
    manifest = ToolManifest("mutate", kind="write", requires_confirmation=True)
    original_commit = saga.commit_write

    async def with_policy(action_id, executor, **kwargs):
        return await original_commit(action_id, executor, manifest=manifest,
                                     policy_engine=PolicyEngine(), **approval)

    monkeypatch.setattr(saga, "commit_write", with_policy)

    async def mutate(args):
        effects.append(dict(args))
        return {"operation_id": "approved"}

    async def undo(result):
        undos.append(result)

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("approval", [TaskStep("prior", compensate=undo), TaskStep("mutate")])
    task.step_states[0] = StepState.COMMITTED
    task.step_results[0] = {"prior": "keep"}
    task.current_step_index = 1
    with pytest.raises(ConfirmationRequiredError):
        await manager.run_task(task.task_id)
    action_id = task.step_action_ids[1]
    assert task.state.name == "WAITING_FOR_AUTHORIZATION"
    assert task.step_states[0] == StepState.COMMITTED
    assert saga._actions[action_id].state == ActionState.PENDING
    assert effects == undos == []
    approval["confirmed"] = True
    await manager.resume_task(task.task_id)
    assert task.state == TaskState.COMPLETED
    assert task.step_action_ids[1] == action_id
    assert len(effects) == 1
    assert undos == []


@pytest.mark.asyncio
async def test_grounding_callback_cannot_erase_unknown_operation():
    saga = SpeculativeSagaManager(TurnEpochClock())
    manager = ExecutorBindings(saga, {})
    task = manager.create_task("unknown correction", [TaskStep("mutate", required_fields=["destination"])])
    action = saga.stage_write("mutate", {"destination": "Chennai"})
    action.state = ActionState.IN_FLIGHT_UNKNOWN
    task.state = TaskState.RUNNING
    task.step_states[0] = StepState.NEEDS_RECONCILIATION
    task.step_action_ids[0] = action.action_id
    manager.on_field_evicted("destination", "Chennai")
    assert manager.try_resume_waiting_tasks() == []
    await manager.resume_task(task.task_id)
    assert task.step_action_ids[0] == action.action_id
    assert task.step_states[0] == StepState.NEEDS_RECONCILIATION
    assert task.state.name == "NEEDS_RECONCILIATION"


@pytest.mark.asyncio
async def test_compensated_stale_result_is_not_current_task_success():
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    started, release = asyncio.Event(), asyncio.Event()
    undo_started, undo_release, undo_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    undos = []

    async def mutate(args):
        started.set()
        await release.wait()
        return {"operation_id": "obsolete"}

    async def undo(result):
        undos.append(result)
        undo_started.set()
        await undo_release.wait()
        undo_done.set()

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("repair", [TaskStep("mutate", compensate=undo)])
    running = asyncio.create_task(manager.run_task(task.task_id))
    await settle(started.wait())
    await clock.advance("correction")
    release.set()
    await settle(running)
    await settle(undo_started.wait())
    assert task.current_step_index == 0
    assert task.state.name == "NEEDS_RECONCILIATION"
    undo_release.set()
    await settle(undo_done.wait())
    await manager.resume_task(task.task_id)
    assert saga._actions[task.step_action_ids[0]].state == ActionState.COMPENSATED
    assert task.state.name == "NEEDS_RECONCILIATION"
    assert len(undos) == 1


@pytest.mark.asyncio
async def test_correction_during_status_check_cannot_become_current_success():
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    started, release = asyncio.Event(), asyncio.Event()

    async def status(action):
        started.set()
        await release.wait()
        action.result = {"operation_id": "old-intent"}
        return True

    manager = ExecutorBindings(saga, {})
    task = manager.create_task("status race", [TaskStep("mutate", status_check=status)])
    action = saga.stage_write("mutate", {})
    action.state = ActionState.IN_FLIGHT_UNKNOWN
    task.state = TaskState.RUNNING
    task.step_states[0] = StepState.NEEDS_RECONCILIATION
    task.step_action_ids[0] = action.action_id
    running = asyncio.create_task(manager.resume_task(task.task_id))
    await settle(started.wait())
    await clock.advance("correction")
    release.set()
    await settle(running)
    assert action.state == ActionState.COMMITTED_STALE
    assert task.state.name == "NEEDS_RECONCILIATION"
    assert task.current_step_index == 0


@pytest.mark.asyncio
async def test_unrelated_tasks_are_not_serialized_by_task_lock():
    saga = SpeculativeSagaManager(TurnEpochClock())
    first_started, second_started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def first(args):
        first_started.set()
        await release.wait()
        return {"operation_id": "one"}

    async def second(args):
        second_started.set()
        return {"operation_id": "two"}

    manager = ExecutorBindings(saga, {"first": first, "second": second})
    a = manager.create_task("one", [TaskStep("first")])
    b = manager.create_task("two", [TaskStep("second")])
    running = asyncio.create_task(manager.run_task(a.task_id))
    await settle(first_started.wait())
    await settle(manager.run_task(b.task_id))
    assert second_started.is_set()
    assert b.state == TaskState.COMPLETED
    assert not release.is_set()
    release.set()
    await settle(running)
