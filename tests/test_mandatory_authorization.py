"""Real authorization boundary; explicit simulation isolates it from disk tests.

No default grants or synthetic manifests. Durable defaults/crash behavior are
tested separately with the real manager in test_durable_action_intent.py.
"""
import asyncio

import pytest

from runtime.policy_engine import (
    AuthorizationContext, ConfirmationRequiredError, MutationApproval,
    PermissionDeniedError, PolicyEngine,
)
from runtime.speculative_saga import ActionState, SpeculativeSagaManager, TurnEpochClock
from runtime.tool_contract import ToolManifest, ToolRegistry


@pytest.mark.asyncio
@pytest.mark.parametrize("with_engine", [False, True])
async def test_omitted_context_never_dispatches(with_engine):
    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    action = saga.stage_write("change", {}, manifest=ToolManifest("change", "write"))
    calls = []

    async def execute(args):
        calls.append(args)

    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, execute,
                                policy_engine=PolicyEngine() if with_engine else None)
    assert action.state == ActionState.PENDING
    assert calls == []


@pytest.mark.asyncio
async def test_omitting_engine_cannot_bypass_permissions():
    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    action = saga.stage_write("change", {}, manifest=ToolManifest(
        "change", "write", required_permissions=["admin"]))
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0),
                                auth_context=AuthorizationContext())
    assert action.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_call_manifest_cannot_downgrade_registry():
    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write", required_permissions=["admin"]))
    saga = SpeculativeSagaManager(TurnEpochClock(), registry=registry, simulation=True)
    action = saga.stage_write("change", {})
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0),
                                manifest=ToolManifest("change", "write"),
                                auth_context=AuthorizationContext())
    assert action.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_unmanifested_write_denied_even_with_context():
    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    action = saga.stage_write("unknown", {})
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0),
                                auth_context=AuthorizationContext(["admin"]))
    assert action.state == ActionState.PENDING


@pytest.mark.parametrize("value", ["true", "false", 1, 0, [], {}])
def test_context_confirmation_is_strict_boolean(value):
    with pytest.raises(TypeError):
        AuthorizationContext(confirmed=value)


@pytest.mark.asyncio
async def test_permissive_custom_engine_and_legacy_option_cannot_bypass_baseline():
    class AllowAll:
        def check(self, *args, **kwargs):
            pass

    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    action = saga.stage_write("change", {}, manifest=ToolManifest(
        "change", "write", required_permissions=["admin"]))
    for engine in (AllowAll(), PolicyEngine(default_deny=False)):
        with pytest.raises(PermissionDeniedError):
            await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0),
                                    policy_engine=engine, auth_context=AuthorizationContext())
    assert action.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_bare_confirmation_bool_does_not_approve_any_payload():
    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    action = saga.stage_write("change", {}, manifest=ToolManifest(
        "change", "write", requires_confirmation=True))
    with pytest.raises(ConfirmationRequiredError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0), confirmed=True,
                                auth_context=AuthorizationContext(confirmed=True))
    assert action.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_approval_cannot_be_replayed_on_another_action():
    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    manifest = ToolManifest("change", "write", requires_confirmation=True)
    first = saga.stage_write("change", {}, manifest=manifest)
    context = AuthorizationContext(approval=MutationApproval.for_action(first))
    saga.abort_write(first.action_id)
    second = saga.stage_write("change", {}, manifest=manifest)
    with pytest.raises(ConfirmationRequiredError):
        await saga.commit_write(second.action_id, lambda _: asyncio.sleep(0), auth_context=context)
    assert second.state == ActionState.PENDING


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["args", "tool_name", "capture_epoch"])
async def test_approval_rejects_altered_identity_or_payload(field):
    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    action = saga.stage_write("change", {"nested": ["original"]}, manifest=ToolManifest(
        "change", "write", requires_confirmation=True))
    context = AuthorizationContext(approval=MutationApproval.for_action(action))
    if field == "args":
        action.args["nested"][0] = "altered"
    elif field == "tool_name":
        action.tool_name = "another"
    else:
        # Approval's epoch cannot be reused for an action staged in a later turn.
        await saga.epoch_clock.advance()
        action = saga.stage_write("change", {"nested": ["original"]}, manifest=action.manifest)
    with pytest.raises((ConfirmationRequiredError, PermissionDeniedError)):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0), auth_context=context)
    assert action.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_authorized_exact_payload_dispatches_once():
    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    action = saga.stage_write("change", {"destination": "Chennai"}, manifest=ToolManifest(
        "change", "write", required_permissions=["fleet:write"], requires_confirmation=True))
    context = AuthorizationContext(["fleet:write"], approval=MutationApproval.for_action(action))
    calls = []

    async def execute(args):
        calls.append(args)
        return "success"

    assert await saga.commit_write(action.action_id, execute, auth_context=context) == "success"
    assert calls == [{"destination": "Chennai"}]
    assert action.state == ActionState.COMMITTED


@pytest.mark.asyncio
async def test_staging_and_dispatch_detach_nested_payloads():
    args = {"nested": ["approved"]}
    saga = SpeculativeSagaManager(TurnEpochClock(), auth_context=AuthorizationContext(), simulation=True)
    action = saga.stage_write("change", args, manifest=ToolManifest("change", "write"))
    args["nested"][0] = "changed by caller"
    started, release = asyncio.Event(), asyncio.Event()
    received = []

    async def execute(payload):
        started.set()
        await release.wait()
        received.append(payload["nested"][0])

    committing = asyncio.create_task(saga.commit_write(action.action_id, execute))
    await asyncio.wait_for(started.wait(), 1)
    action.args["nested"][0] = "changed while awaiting"
    release.set()
    await committing
    assert received == ["approved"]


@pytest.mark.asyncio
async def test_changed_pending_payload_denied_without_confirmation_requirement():
    saga = SpeculativeSagaManager(TurnEpochClock(), auth_context=AuthorizationContext(), simulation=True)
    action = saga.stage_write("change", {"value": 1}, manifest=ToolManifest("change", "write"))
    action.args["value"] = 2
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0))
    assert action.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_registry_update_cannot_erase_staged_policy_and_can_revoke_grants():
    registry = ToolRegistry()
    manifest = ToolManifest("change", "write", required_permissions=["old"])
    registry.register(manifest)
    saga = SpeculativeSagaManager(TurnEpochClock(), registry=registry, simulation=True)
    action = saga.stage_write("change", {})
    # Even mutation of the original list cannot weaken the staging snapshot.
    manifest.required_permissions.clear()
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0),
                                auth_context=AuthorizationContext())
    registry.register(ToolManifest("change", "write", required_permissions=["new"]))
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0),
                                auth_context=AuthorizationContext(["old"]))
    assert action.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_task_manager_default_denial_retains_progress_without_unwind():
    from runtime.task_engine import TaskManager, TaskState, TaskStep, StepState

    class Manager(TaskManager):
        def _get_executor(self, step):
            async def execute(args):
                pytest.fail("Unauthorized executor called")
            return execute

    registry = ToolRegistry()
    registry.register(ToolManifest("change", "write"))
    saga = SpeculativeSagaManager(TurnEpochClock(), registry=registry, simulation=True)
    manager = Manager(saga, lambda _: None)
    task = manager.create_task("unauthorized", [TaskStep("change")])
    with pytest.raises(PermissionDeniedError):
        await manager.run_task(task.task_id)
    assert task.state == TaskState.FAILED
    await manager.resume_task(task.task_id)  # terminal denial never redispatches
    assert saga._actions[task.step_action_ids[0]].state == ActionState.PENDING
    assert task.step_states[0] != StepState.COMMITTED
    assert len(saga._actions) == 1


@pytest.mark.asyncio
async def test_voice_policy_denial_never_unwinds_prior_effect(monkeypatch):
    from agent import CCSAgent, PendingSlot
    ccs = CCSAgent(simulation=True)
    compensations = []

    async def compensate(result):
        compensations.append(result)

    prior = ccs.saga.stage_write("reroute_truck", {"truck_id": "T", "destination": "old"},
                                 compensate=compensate)
    await ccs.saga.commit_write(prior.action_id, lambda _: asyncio.sleep(0, result="prior"),
                                auth_context=AuthorizationContext(["fleet:write"]))
    monkeypatch.setattr(ccs.guard, "resolve_current_value", lambda field:
                        {"truck_id": "T", "destination": "new"}[field])
    for field in ("truck_id", "destination"):
        ccs._pending_slots[field] = PendingSlot(field, "untrusted", "reroute_truck", (0, 1))
    await ccs._commit_all_pending_slots()
    assert prior.state == ActionState.COMMITTED
    assert compensations == []
    pending = [a for a in ccs.saga._actions.values() if a.state == ActionState.PENDING]
    assert len(pending) == 1


@pytest.mark.parametrize("args", [{1: "value"}, {"value": float("nan")}, {"value": (1, 2)}])
def test_ambiguous_non_json_arguments_never_stage(args):
    saga = SpeculativeSagaManager(TurnEpochClock(), simulation=True)
    with pytest.raises(ValueError):
        saga.stage_write("change", args, manifest=ToolManifest("change", "write"))
    assert saga.snapshot() == []


@pytest.mark.asyncio
async def test_mutated_context_cannot_turn_permission_check_into_substring_search():
    context = AuthorizationContext()
    context.granted_permissions = "not-admin-but-contains-admin"
    saga = SpeculativeSagaManager(TurnEpochClock(), auth_context=context, simulation=True)
    action = saga.stage_write("change", {}, manifest=ToolManifest(
        "change", "write", required_permissions=["admin"]))
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0))
    assert action.state == ActionState.PENDING


@pytest.mark.asyncio
async def test_validator_cannot_resurrect_an_aborted_action():
    from runtime.speculative_saga import SagaAbortedError
    saga = SpeculativeSagaManager(TurnEpochClock(), auth_context=AuthorizationContext(), simulation=True)

    def validate(args):
        saga.abort_write(action.action_id)

    action = saga.stage_write("change", {}, manifest=ToolManifest("change", "write", validate_args=validate))
    with pytest.raises(SagaAbortedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0))
    assert action.state == ActionState.ABORTED


@pytest.mark.asyncio
async def test_validator_cannot_rewrite_the_payload_passed_to_executor():
    def validate(args):
        args["nested"][0] = "validator replacement"

    saga = SpeculativeSagaManager(TurnEpochClock(), auth_context=AuthorizationContext(), simulation=True)
    action = saga.stage_write("change", {"nested": ["original"]}, manifest=ToolManifest(
        "change", "write", validate_args=validate))
    result = await saga.commit_write(action.action_id, lambda args: asyncio.sleep(0, result=args))
    assert result == {"nested": ["original"]}


@pytest.mark.asyncio
async def test_call_registry_cannot_replace_host_registry():
    host, override = ToolRegistry(), ToolRegistry()
    host.register(ToolManifest("change", "write", required_permissions=["admin"]))
    override.register(ToolManifest("change", "write"))
    saga = SpeculativeSagaManager(TurnEpochClock(), registry=host, auth_context=AuthorizationContext(), simulation=True)
    action = saga.stage_write("change", {}, registry=override)
    with pytest.raises(PermissionDeniedError):
        await saga.commit_write(action.action_id, lambda _: asyncio.sleep(0), registry=override)
    assert action.state == ActionState.PENDING
