"""Process-local ownership tests; all external effects are SIMULATED."""

import asyncio

import pytest

from runtime.speculative_saga import (
    ActionState, CompensationState, DuplicateOperationError, MutationNotAppliedError,
    SpeculativeSagaManager, TurnEpochClock,
)
from runtime.task_engine import StepState, TaskManager, TaskState, TaskStep


class ExecutorBindings(TaskManager):
    def __init__(self, saga, executors, resolve_field=lambda _: "grounded"):
        super().__init__(saga, resolve_field=resolve_field)
        self.executors = executors

    def _get_executor(self, step):
        return self.executors[step.tool_name]


async def settle(awaitable):
    return await asyncio.wait_for(awaitable, timeout=3)


def stale_action(saga, compensate):
    action = saga.stage_write("mutate", {}, compensate=compensate, idempotency_key="op-1")
    action.state = ActionState.COMMITTED_STALE
    action.result = {"operation_id": "remote-1"}
    return action


@pytest.mark.asyncio
async def test_concurrent_reconcilers_do_not_run_two_compensations():
    saga = SpeculativeSagaManager(TurnEpochClock())
    status_started, status_release, contender = asyncio.Event(), asyncio.Event(), asyncio.Event()
    undo_started, undo_release = asyncio.Event(), asyncio.Event()
    status_calls, effects = [], []

    async def undo(result):
        effects.append("one compensation")
        undo_started.set()
        await undo_release.wait()

    action = stale_action(saga, undo)

    async def status(a):
        status_calls.append(a.action_id)
        status_started.set()
        await status_release.wait()
        return True

    first = asyncio.create_task(saga.reconcile_write(action.action_id, status))
    await settle(status_started.wait())

    async def second_resolver():
        contender.set()
        await saga.reconcile_write(action.action_id, status)

    second = asyncio.create_task(second_resolver())
    await settle(contender.wait())
    status_release.set()
    await settle(undo_started.wait())
    undo_release.set()
    await settle(asyncio.gather(first, second))
    assert status_calls == [action.action_id]
    assert effects == ["one compensation"]
    assert action.state == ActionState.COMPENSATED


@pytest.mark.asyncio
async def test_auto_compensation_and_reconciliation_share_ownership():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release, contender = asyncio.Event(), asyncio.Event(), asyncio.Event()
    effects, status_calls = [], []

    async def undo(result):
        effects.append("one compensation")
        started.set()
        await release.wait()

    action = stale_action(saga, undo)
    auto = asyncio.create_task(saga._auto_compensate(action))
    await settle(started.wait())

    async def status(a):
        status_calls.append(a.action_id)
        return True

    async def resolve():
        contender.set()
        await saga.reconcile_write(action.action_id, status)

    resolver = asyncio.create_task(resolve())
    await settle(contender.wait())
    release.set()
    await settle(asyncio.gather(auto, resolver))
    assert effects == ["one compensation"]
    assert status_calls == []
    assert action.state == ActionState.COMPENSATED


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_ambiguous_compensation_is_not_automatically_retried(cancelled):
    saga = SpeculativeSagaManager(TurnEpochClock())
    applied = asyncio.Event()
    effects = []

    async def undo(result):
        effects.append("remote undo applied")
        applied.set()
        if cancelled:
            await asyncio.Event().wait()
        raise ConnectionError("undo applied but acknowledgement lost")

    action = stale_action(saga, undo)
    running = asyncio.create_task(saga._auto_compensate(action))
    await settle(applied.wait())
    if cancelled:
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    else:
        await settle(running)
    assert action.state == ActionState.COMMITTED_STALE
    assert action.compensation_failed
    assert action.compensation_state == CompensationState.UNKNOWN
    assert action.compensation_error is not None
    assert saga.snapshot()[0]["compensation_state"] == "UNKNOWN"

    async def original_effect_status(a):
        # Seeing the original effect still present does NOT prove a delayed
        # compensation cannot apply. Never blindly repeat the compensator.
        return True

    await saga.reconcile_write(action.action_id, original_effect_status)
    await saga._auto_compensate(action)
    assert effects == ["remote undo applied"]
    assert action.state == ActionState.COMMITTED_STALE
    with pytest.raises(DuplicateOperationError):
        saga.stage_write("mutate", {}, idempotency_key="op-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [0, 1, "false", {}, []])
async def test_malformed_status_cannot_release_unknown_operation(outcome):
    saga = SpeculativeSagaManager(TurnEpochClock())
    action = saga.stage_write("mutate", {}, idempotency_key="op-1")
    action.state = ActionState.IN_FLIGHT_UNKNOWN

    async def status(a):
        return outcome

    with pytest.raises(TypeError, match="True, False, or None"):
        await saga.reconcile_write(action.action_id, status)
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN
    with pytest.raises(DuplicateOperationError):
        saga.stage_write("mutate", {}, idempotency_key="op-1")


@pytest.mark.asyncio
async def test_concurrent_chain_unwinds_compensate_each_action_once():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release, contender = asyncio.Event(), asyncio.Event(), asyncio.Event()
    effects = []

    async def undo(result):
        effects.append("one chain compensation")
        started.set()
        await release.wait()

    action = saga.stage_write("mutate", {}, compensate=undo)

    async def mutate(args):
        return {"operation_id": "remote-1"}

    await saga.commit_write(action.action_id, mutate)
    first = asyncio.create_task(saga.abort_chain_from("downstream-failure"))
    await settle(started.wait())

    async def unwind():
        contender.set()
        await saga.abort_chain_from("downstream-failure")

    second = asyncio.create_task(unwind())
    await settle(contender.wait())
    release.set()
    await settle(asyncio.gather(first, second))
    assert effects == ["one chain compensation"]
    assert action.state == ActionState.COMPENSATED


@pytest.mark.asyncio
async def test_status_cancellation_releases_ownership_without_resolving_unknown():
    saga = SpeculativeSagaManager(TurnEpochClock())
    entered = asyncio.Event()
    action = saga.stage_write("mutate", {})
    action.state = ActionState.IN_FLIGHT_UNKNOWN

    async def blocked_status(a):
        entered.set()
        await asyncio.Event().wait()

    running = asyncio.create_task(saga.reconcile_write(action.action_id, blocked_status))
    await settle(entered.wait())
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN

    async def known_applied(a):
        a.result = {"operation_id": "remote-1"}
        return True

    await settle(saga.reconcile_write(action.action_id, known_applied))
    assert action.state == ActionState.COMMITTED
    assert saga._chain == [action]


@pytest.mark.asyncio
async def test_unrelated_actions_can_reconcile_while_one_compensation_waits():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release = asyncio.Event(), asyncio.Event()

    async def undo(result):
        started.set()
        await release.wait()

    a = stale_action(saga, undo)
    b = saga.stage_write("other", {})
    b.state = ActionState.IN_FLIGHT_UNKNOWN
    running = asyncio.create_task(saga._auto_compensate(a))
    await settle(started.wait())

    async def status(action):
        return True

    await settle(saga.reconcile_write(b.action_id, status))
    assert b.state == ActionState.COMMITTED
    assert a.compensation_state == CompensationState.IN_FLIGHT
    release.set()
    await settle(running)


@pytest.mark.asyncio
async def test_explicit_compensation_rejection_is_visible_and_not_automatically_retried():
    saga = SpeculativeSagaManager(TurnEpochClock())
    attempts = []

    async def undo(result):
        attempts.append("rejected")
        raise MutationNotAppliedError("atomic undo rejection", evidence="fake:undo-rejected-before-apply")

    action = stale_action(saga, undo)
    await saga._auto_compensate(action)
    await saga._auto_compensate(action)
    assert attempts == ["rejected"]
    assert action.state == ActionState.COMMITTED_STALE
    assert action.compensation_state == CompensationState.NOT_APPLIED
    assert action.compensation_not_applied_evidence == "fake:undo-rejected-before-apply"
    assert action.compensation_failed


@pytest.mark.asyncio
async def test_authoritative_absence_resolves_compensation_uncertainty_without_repeating_undo():
    saga = SpeculativeSagaManager(TurnEpochClock())
    effects = []

    async def undo(result):
        effects.append("undo applied")
        raise ConnectionError("lost undo reply")

    action = stale_action(saga, undo)
    await saga._auto_compensate(action)

    async def status(a):
        # Fake provider guarantees absence and no delayed future application.
        assert effects == ["undo applied"]
        return False

    await saga.reconcile_write(action.action_id, status)
    assert action.state == ActionState.COMPENSATED
    assert action.compensation_state == CompensationState.SUCCEEDED
    assert not action.compensation_failed
    await saga.reconcile_write(action.action_id, status)
    assert effects == ["undo applied"]
    replacement = saga.stage_write("mutate", {}, idempotency_key="op-1")
    assert replacement.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_current_action_with_unknown_compensation_cannot_complete_task():
    from runtime.task_engine import StepState, TaskManager, TaskState, TaskStep

    saga = SpeculativeSagaManager(TurnEpochClock())
    effects, status_calls = [], []

    async def mutate(args):
        return {"operation_id": "remote-1"}

    async def undo(result):
        effects.append("undo may have applied")
        raise ConnectionError("undo response lost")

    action = saga.stage_write("mutate", {}, compensate=undo, idempotency_key="op-1")
    await saga.commit_write(action.action_id, mutate)
    await saga.compensate_action(action.action_id)
    assert action.state == ActionState.COMMITTED
    assert action.compensation_state == CompensationState.UNKNOWN
    with pytest.raises(DuplicateOperationError):
        saga.stage_write("mutate", {}, idempotency_key="op-1")

    async def status(a):
        status_calls.append(a.action_id)
        return None

    manager = TaskManager(saga, resolve_field=lambda _: "grounded")
    task = manager.create_task("uncertain undo", [TaskStep("mutate", status_check=status)])
    task.state = TaskState.RUNNING
    task.step_states[0] = StepState.NEEDS_RECONCILIATION
    task.step_action_ids[0] = action.action_id
    await manager.resume_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert task.current_step_index == 0
    assert status_calls == [action.action_id]
    assert effects == ["undo may have applied"]


@pytest.mark.asyncio
async def test_conflicting_concurrent_status_cannot_overwrite_first_resolution():
    saga = SpeculativeSagaManager(TurnEpochClock())
    entered, release, contender = asyncio.Event(), asyncio.Event(), asyncio.Event()
    action = saga.stage_write("mutate", {})
    action.state = ActionState.IN_FLIGHT_UNKNOWN
    contradictory_queries = []

    async def applied(a):
        entered.set()
        await release.wait()
        a.result = {"operation_id": "remote-1"}
        return True

    async def absent(a):
        contradictory_queries.append(a.action_id)
        return False

    first = asyncio.create_task(saga.reconcile_write(action.action_id, applied))
    await settle(entered.wait())

    async def second_resolution():
        contender.set()
        await saga.reconcile_write(action.action_id, absent)

    second = asyncio.create_task(second_resolution())
    await settle(contender.wait())
    release.set()
    await settle(asyncio.gather(first, second))
    assert contradictory_queries == []
    assert action.state == ActionState.COMMITTED
    assert saga._chain == [action]


@pytest.mark.asyncio
async def test_task_cancel_and_direct_saga_repair_use_same_compensation_owner():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release, contender = asyncio.Event(), asyncio.Event(), asyncio.Event()
    effects = []

    async def mutate(args):
        return {"operation_id": "remote-1"}

    async def undo(result):
        effects.append("one undo")
        started.set()
        await release.wait()

    manager = ExecutorBindings(saga, {"mutate": mutate}, resolve_field=lambda _: None)
    task = manager.create_task("cancel", [TaskStep("mutate", compensate=undo),
                                           TaskStep("later", required_fields=["missing"])])
    await manager.run_task(task.task_id)
    action_id = task.step_action_ids[0]
    direct = asyncio.create_task(saga.compensate_action(action_id))
    await settle(started.wait())

    async def cancel():
        contender.set()
        return await manager.cancel_task(task.task_id)

    cancelling = asyncio.create_task(cancel())
    await settle(contender.wait())
    release.set()
    await settle(asyncio.gather(direct, cancelling))
    assert effects == ["one undo"]
    assert task.state == TaskState.CANCELLED
    assert task.step_states == [StepState.COMPENSATED, StepState.SKIPPED]


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_record", [False, True])
async def test_replan_cannot_invent_compensation_or_erase_evidence(missing_record):
    saga = SpeculativeSagaManager(TurnEpochClock())
    effects = []

    async def mutate(args):
        effects.append("original applied")
        return {"operation_id": "remote-1"}

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("no safe repair", [TaskStep("mutate")])
    await manager.run_task(task.task_id)
    action_id = task.step_action_ids[0]
    if missing_record:
        saga._actions.pop(action_id)
    await manager._replan_from_step(task, 0, "destination", "old")
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert task.step_states[0] == StepState.NEEDS_RECONCILIATION
    assert task.step_action_ids[0] == action_id
    assert task.step_results[0] == {"operation_id": "remote-1"}
    assert task.current_step_index == 0
    await manager.resume_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert effects == ["original applied"]


@pytest.mark.asyncio
async def test_repeated_cancel_does_not_unwind_ancestors_past_unknown_compensation():
    saga = SpeculativeSagaManager(TurnEpochClock())
    undos = []

    async def mutate(args):
        return dict(args)

    async def undo_a(result):
        undos.append("ancestor must wait")

    async def undo_b(result):
        undos.append("downstream undo applied")
        raise ConnectionError("lost undo reply")

    manager = ExecutorBindings(saga, {"a": mutate, "b": mutate}, resolve_field=lambda _: None)
    task = manager.create_task("cancel order", [TaskStep("a", compensate=undo_a),
        TaskStep("b", compensate=undo_b), TaskStep("later", required_fields=["missing"])])
    await manager.run_task(task.task_id)
    action_id = task.step_action_ids[1]
    await manager.cancel_task(task.task_id)
    await manager.cancel_task(task.task_id)
    assert task.state == TaskState.CANCELLED
    assert task.step_states[0] == StepState.COMMITTED
    assert task.step_states[1] == StepState.NEEDS_RECONCILIATION
    assert task.step_action_ids[1] == action_id
    assert saga._actions[action_id].compensation_state == CompensationState.UNKNOWN
    assert undos == ["downstream undo applied"]


@pytest.mark.asyncio
async def test_overlapping_replans_do_not_erase_a_fresh_dispatch_identity():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release, contender = asyncio.Event(), asyncio.Event(), asyncio.Event()
    effects, undos = [], []
    destination = "old"

    async def mutate(args):
        effects.append(args["destination"])
        return dict(args)

    async def undo(result):
        undos.append(result)
        started.set()
        await release.wait()

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("overlap", [TaskStep("mutate", compensate=undo,
        build_args=lambda t, i: {"destination": destination})])
    await manager.run_task(task.task_id)
    original_id = task.step_action_ids[0]
    destination = "new"
    task.state = TaskState.REPLANNING
    first = asyncio.create_task(manager._replan_from_step(task, 0))
    await settle(started.wait())

    async def second_replan():
        contender.set()
        await manager._replan_from_step(task, 0)

    second = asyncio.create_task(second_replan())
    await settle(contender.wait())
    release.set()
    await settle(asyncio.gather(first, second))
    assert effects == ["old", "new"]
    assert undos == [{"destination": "old"}]
    assert task.state == TaskState.COMPLETED
    assert task.step_action_ids[0] != original_id
    assert saga._actions[task.step_action_ids[0]].state == ActionState.COMMITTED


@pytest.mark.asyncio
async def test_legacy_restore_cannot_repeat_an_unrecorded_compensation():
    saga = SpeculativeSagaManager(TurnEpochClock())
    undos = []

    async def undo(result):
        undos.append("must not repeat unknown historical undo")

    async def status(action):
        action.result = {"operation_id": "prior-process-operation"}
        return True

    manager = TaskManager(saga, resolve_field=lambda _: "grounded")
    task = manager.create_task("restored", [TaskStep("mutate", compensate=undo, status_check=status)])
    task.state = TaskState.RUNNING
    task.step_states[0] = StepState.NEEDS_RECONCILIATION
    task.step_action_ids[0] = "prior-process-operation"
    await manager.resume_task(task.task_id)
    action = saga._actions["prior-process-operation"]
    assert action.state == ActionState.COMMITTED_STALE
    assert action.compensation_state == CompensationState.UNKNOWN
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert undos == []


@pytest.mark.asyncio
async def test_old_unknown_compensation_blocks_key_after_newer_operation_replaced_index():
    saga = SpeculativeSagaManager(TurnEpochClock())

    async def mutate(args):
        return dict(args)

    async def undo(result):
        raise ConnectionError("old operation undo outcome unknown")

    older = saga.stage_write("mutate", {"version": 1}, compensate=undo, idempotency_key="shared")
    await saga.commit_write(older.action_id, mutate)
    newer = saga.stage_write("mutate", {"version": 2}, idempotency_key="shared")
    await saga.commit_write(newer.action_id, mutate)
    assert saga._ikey_index["shared"] == newer.action_id
    await saga.compensate_action(older.action_id)
    with pytest.raises(DuplicateOperationError):
        saga.stage_write("mutate", {"version": 3}, idempotency_key="shared")


@pytest.mark.asyncio
async def test_user_cancel_during_replan_is_not_overwritten_by_redispatch():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release, contender = asyncio.Event(), asyncio.Event(), asyncio.Event()
    effects, undos = [], []

    async def mutate(args):
        effects.append("original applied")
        return {"operation_id": "remote-1"}

    async def undo(result):
        undos.append(result)
        started.set()
        await release.wait()

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("cancel repair", [TaskStep("mutate", compensate=undo),
        TaskStep("later", required_fields=["missing"])])
    manager._resolve_field = lambda _: None
    await manager.run_task(task.task_id)
    original_id = task.step_action_ids[0]
    task.state = TaskState.REPLANNING
    replanning = asyncio.create_task(manager._replan_from_step(task, 0))
    await settle(started.wait())

    async def cancel():
        contender.set()
        await manager.cancel_task(task.task_id)

    cancelling = asyncio.create_task(cancel())
    await settle(contender.wait())
    release.set()
    await settle(asyncio.gather(replanning, cancelling))
    assert task.state == TaskState.CANCELLED
    assert task.step_action_ids[0] == original_id
    assert effects == ["original applied"]
    assert len(undos) == 1


@pytest.mark.asyncio
async def test_chain_unwind_stops_at_unknown_compensation_on_every_attempt():
    saga = SpeculativeSagaManager(TurnEpochClock())
    undos = []

    async def mutate(args):
        return dict(args)

    async def undo_a(result):
        undos.append("ancestor must wait")

    async def undo_b(result):
        undos.append("downstream undo applied")
        raise ConnectionError("reply lost")

    a = saga.stage_write("a", {}, compensate=undo_a)
    b = saga.stage_write("b", {}, compensate=undo_b)
    await saga.commit_write(a.action_id, mutate)
    await saga.commit_write(b.action_id, mutate)
    await saga.abort_chain_from("downstream-failure")
    await saga.abort_chain_from("downstream-failure")
    assert saga._chain == [a, b]
    assert b.compensation_state == CompensationState.UNKNOWN
    assert a.compensation_state == CompensationState.NOT_STARTED
    assert undos == ["downstream undo applied"]


@pytest.mark.asyncio
async def test_repeated_negative_forward_status_cannot_invent_success_for_unknown_historical_undo():
    saga = SpeculativeSagaManager(TurnEpochClock())
    action = saga.stage_write("mutate", {}, idempotency_key="op-1")
    action.state = ActionState.IN_FLIGHT_UNKNOWN
    action.compensation_state = CompensationState.UNKNOWN

    async def forward_not_applied(a):
        return False

    for _ in range(2):
        await saga.reconcile_write(action.action_id, forward_not_applied)
        assert action.state == ActionState.ABORTED
        assert action.compensation_state == CompensationState.UNKNOWN
        with pytest.raises(DuplicateOperationError):
            saga.stage_write("mutate", {}, idempotency_key="op-1")


@pytest.mark.asyncio
async def test_cancel_does_not_undo_ancestor_while_descendant_write_can_still_apply():
    saga = SpeculativeSagaManager(TurnEpochClock())
    started, release = asyncio.Event(), asyncio.Event()
    effects, undos = [], []

    async def ancestor(args):
        effects.append("ancestor applied")
        return {"operation_id": "ancestor"}

    async def undo_ancestor(result):
        undos.append("ancestor must wait")

    async def descendant(args):
        started.set()
        await release.wait()
        effects.append("descendant applied after local cancellation")
        return {"operation_id": "descendant"}

    manager = ExecutorBindings(saga, {"ancestor": ancestor, "descendant": descendant})
    task = manager.create_task("cancel active descendant", [TaskStep("ancestor", compensate=undo_ancestor),
        TaskStep("descendant")])
    running = asyncio.create_task(manager.run_task(task.task_id))
    await settle(started.wait())
    await manager.cancel_task(task.task_id)
    assert undos == []
    release.set()
    await settle(running)
    assert task.state == TaskState.CANCELLED
    assert task.step_states == [StepState.COMMITTED, StepState.NEEDS_RECONCILIATION]
    assert effects == ["ancestor applied", "descendant applied after local cancellation"]
    assert undos == []
