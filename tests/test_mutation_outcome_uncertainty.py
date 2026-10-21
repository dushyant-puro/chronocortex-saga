"""SIMULATED external effects, with deterministic response-loss injection."""

import asyncio

import pytest

from runtime.speculative_saga import (
    ActionState, DuplicateOperationError, MutationNotAppliedError, SagaAbortedError,
    SpeculativeSagaManager, TurnEpochClock,
)
from runtime.task_engine import StepState, TaskManager, TaskState, TaskStep
from runtime.tool_contract import ToolManifest


@pytest.mark.asyncio
@pytest.mark.parametrize("corrected", [False, True])
@pytest.mark.parametrize("error_type", [ConnectionError, ValueError, RuntimeError])
async def test_post_apply_exception_preserves_unknown_and_blocks_retry(corrected, error_type):
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    applied, lose_response = asyncio.Event(), asyncio.Event()
    provider_ledger = []
    compensations = []
    failure = error_type("reply failed; outcome cannot be inferred")

    async def mutate(args):
        provider_ledger.append(dict(args))
        applied.set()
        await lose_response.wait()
        raise failure

    async def compensate(result):
        compensations.append(result)

    action = saga.stage_write("mutate", {"destination": "Chennai"},
                              compensate=compensate, idempotency_key="operation-1")
    running = asyncio.create_task(saga.commit_write(action.action_id, mutate))
    await asyncio.wait_for(applied.wait(), timeout=3)
    if corrected:
        await clock.advance("correction after remote apply")
    lose_response.set()
    with pytest.raises(error_type) as caught:
        await running
    assert caught.value is failure
    assert provider_ledger == [{"destination": "Chennai"}]
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN
    assert action.error is failure
    assert action.not_applied_evidence is None
    assert action not in saga._chain
    assert compensations == []
    with pytest.raises(DuplicateOperationError):
        saga.stage_write("mutate", dict(action.args), idempotency_key=action.idempotency_key)
    with pytest.raises(SagaAbortedError):
        await saga.commit_write(action.action_id, mutate)
    assert len(provider_ledger) == 1


@pytest.mark.asyncio
async def test_ordinary_exception_without_observed_effect_is_still_unknown():
    saga = SpeculativeSagaManager(TurnEpochClock())
    action = saga.stage_write("mutate", {})

    async def rejected(args):
        raise RuntimeError("looks like a rejection, but supplies no contract evidence")

    with pytest.raises(RuntimeError):
        await saga.commit_write(action.action_id, rejected)
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN


class ExecutorBindings(TaskManager):
    """Bind providers without replacing dispatch or task lifecycle behavior."""

    def __init__(self, saga, executors):
        super().__init__(saga, resolve_field=lambda _: "grounded")
        self.executors = executors

    def _get_executor(self, step):
        return self.executors[step.tool_name]


@pytest.mark.asyncio
@pytest.mark.parametrize("correction_before_status", [False, True])
async def test_task_preserves_identity_and_prior_progress_until_status_evidence(correction_before_status):
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    provider_ledger, compensations, status_ids = [], [], []
    status_available = False

    async def mutate(args):
        provider_ledger.append("remote-operation-1")
        raise ConnectionError("response lost after remote commit")

    async def compensate(result):
        compensations.append(result)

    async def status(action):
        status_ids.append((action.action_id, action.idempotency_key))
        if not status_available:
            return None
        assert provider_ledger == ["remote-operation-1"]
        action.result = {"operation_id": provider_ledger[0]}
        return True

    manager = ExecutorBindings(saga, {"mutate": mutate})
    task = manager.create_task("lost reply", [TaskStep("prior", compensate=compensate),
                                             TaskStep("mutate", status_check=status)])
    task.step_states[0] = StepState.COMMITTED
    task.step_results[0] = {"authorized_prior_effect": True}
    task.current_step_index = 1
    await manager.run_task(task.task_id)
    action_id = task.step_action_ids[1]
    action = saga._actions[action_id]
    operation_key = action.idempotency_key
    assert task.state == TaskState.NEEDS_RECONCILIATION
    assert task.step_states[0] == StepState.COMMITTED
    assert task.step_states[1] == StepState.NEEDS_RECONCILIATION
    assert task.current_step_index == 1

    await manager.resume_task(task.task_id)
    assert task.state == TaskState.NEEDS_RECONCILIATION
    if correction_before_status:
        await clock.advance("corrected while response unresolved")
    status_available = True
    await manager.resume_task(task.task_id)

    assert task.step_action_ids[1] == action_id
    assert action.idempotency_key == operation_key
    assert status_ids == [(action_id, operation_key)] * 2
    assert provider_ledger == ["remote-operation-1"]
    assert compensations == []
    if correction_before_status:
        assert action.state == ActionState.COMMITTED_STALE
        assert task.state == TaskState.NEEDS_RECONCILIATION
        assert task.current_step_index == 1
    else:
        assert action.state == ActionState.COMMITTED
        assert task.state == TaskState.COMPLETED
        assert task.step_results[1] == {"operation_id": "remote-operation-1"}


@pytest.mark.asyncio
@pytest.mark.parametrize("corrected", [False, True])
async def test_documented_atomic_rejection_can_abort_but_does_not_automatically_retry(corrected):
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    entered, reject = asyncio.Event(), asyncio.Event()
    attempts, provider_ledger = [], []
    failure = MutationNotAppliedError("atomic rejection", evidence="fake-provider:atomic-reject:op-1")

    async def mutate(args):
        attempts.append("one request")
        entered.set()
        await reject.wait()
        # The fake provider's atomic reject happens before any ledger change.
        raise failure

    action = saga.stage_write("mutate", {}, idempotency_key="operation-1")
    running = asyncio.create_task(saga.commit_write(action.action_id, mutate))
    await asyncio.wait_for(entered.wait(), timeout=3)
    if corrected:
        await clock.advance("correction before documented rejection")
    reject.set()
    with pytest.raises(MutationNotAppliedError) as caught:
        await running
    assert caught.value is failure
    assert action.state == ActionState.ABORTED
    assert action.error is failure
    assert action.not_applied_evidence == "fake-provider:atomic-reject:op-1"
    assert saga.snapshot()[0]["not_applied_evidence"] == action.not_applied_evidence
    assert action not in saga._chain
    assert attempts == ["one request"]
    assert provider_ledger == []
    with pytest.raises(SagaAbortedError):
        await saga.commit_write(action.action_id, mutate)
    replacement = saga.stage_write("mutate", {}, idempotency_key=action.idempotency_key)
    assert replacement.state == ActionState.PENDING
    assert attempts == ["one request"]  # staging does not dispatch/retry


@pytest.mark.parametrize("evidence", [None, "", "   "])
def test_not_applied_claim_requires_evidence_reference(evidence):
    with pytest.raises(ValueError, match="evidence reference"):
        MutationNotAppliedError("unsubstantiated rejection", evidence=evidence)


@pytest.mark.asyncio
async def test_pre_dispatch_validation_failure_never_dispatches_or_becomes_unknown():
    saga = SpeculativeSagaManager(TurnEpochClock())
    attempts = []

    def validate(args):
        raise ValueError("invalid destination")

    async def mutate(args):
        attempts.append("must not dispatch")

    manifest = ToolManifest("mutate", kind="write", validate_args=validate)
    action = saga.stage_write("mutate", {}, manifest=manifest)
    with pytest.raises(ValueError, match="invalid destination"):
        await saga.commit_write(action.action_id, mutate)
    assert action.state == ActionState.PENDING
    assert action.error is None
    assert action.not_applied_evidence is None
    assert attempts == []


@pytest.mark.asyncio
async def test_voice_handler_does_not_unwind_chain_after_ambiguous_write(monkeypatch):
    from agent import CCSAgent, PendingSlot

    ccs = CCSAgent()
    provider_ledger, compensations = [], []

    async def prior_mutation(args):
        return {"prior_operation": "keep"}

    async def compensate(result):
        compensations.append(result)

    prior = ccs.saga.stage_write("prior", {}, compensate=compensate)
    await ccs.saga.commit_write(prior.action_id, prior_mutation)

    async def mutate(args):
        provider_ledger.append(dict(args))
        raise ValueError("malformed response after provider apply")

    monkeypatch.setattr("agent.WRITE_TOOLS", {"mutate": (mutate, compensate)})
    monkeypatch.setattr(ccs.guard, "resolve_current_value", lambda _: "Chennai")
    ccs._pending_slots["destination"] = PendingSlot(
        field_name="destination", raw_value="Chennai", tool_name="mutate", token_range=(0, 1))
    await ccs._commit_all_pending_slots()
    unknown = [a for a in ccs.saga._actions.values() if a.tool_name == "mutate"]
    assert len(unknown) == 1
    assert unknown[0].state == ActionState.IN_FLIGHT_UNKNOWN
    assert prior.state == ActionState.COMMITTED
    assert ccs.saga._chain == [prior]
    assert provider_ledger == [{"destination": "Chennai"}]
    assert compensations == []


@pytest.mark.asyncio
async def test_simulated_dock_conflict_has_non_execution_evidence(monkeypatch):
    from tools import fleet_tools

    async def no_delay(*args, **kwargs):
        pass

    monkeypatch.setattr(fleet_tools, "_simulated_network", no_delay)
    existing = {"original": {"dock_id": "D1", "truck_id": "truck-original"}}
    monkeypatch.setattr(fleet_tools, "_DOCK_RESERVATIONS", dict(existing))
    saga = SpeculativeSagaManager(TurnEpochClock())
    action = saga.stage_write("reserve_dock", {"dock_id": "D1", "truck_id": "truck-new"})
    with pytest.raises(MutationNotAppliedError):
        await saga.commit_write(action.action_id, fleet_tools.reserve_dock)
    assert action.state == ActionState.ABORTED
    assert action.not_applied_evidence == "simulated:dock-conflict-before-insert"
    assert fleet_tools._DOCK_RESERVATIONS == existing
