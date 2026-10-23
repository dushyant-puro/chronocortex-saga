"""Receipt-bearing status queries; remote evidence below is SIMULATED."""
import asyncio
from dataclasses import replace

import pytest

from test_durable_security import runtime_at, approve
from runtime.policy_engine import PermissionDeniedError
from runtime.speculative_saga import ActionState, SpeculativeSagaManager, TurnEpochClock


async def unknown(env):
    action = env.manager.stage_write("change", {"target": "known-resource"})
    approve(env, action)
    async def lost(_):
        raise ConnectionError("SIMULATED lost acknowledgement")
    with pytest.raises(ConnectionError):
        await env.manager.commit_write(action.action_id, lost)
    return action


def observation(action, receipt=None):
    from runtime.reconciliation import ReconciledEffect
    return ReconciledEffect(action.action_id, action._staged_digest,
                            receipt if receipt is not None else {"proof": "SIMULATED:terminal"})


@pytest.mark.asyncio
async def test_positive_query_persists_receipt_in_same_resolution_event(tmp_path):
    env = runtime_at(tmp_path)
    action = await unknown(env)
    async def status(a):
        return observation(a)
    await env.manager.reconcile_write(action.action_id, status)
    assert action.state == ActionState.COMMITTED
    event = env.journal.events(action.action_id)[-1]
    assert event["kind"] == "RECONCILED_PRESENT"
    assert event["record"]["result"] == action.result == {"proof": "SIMULATED:terminal"}
    recovered = SpeculativeSagaManager(TurnEpochClock(), journal=env.journal)
    assert recovered._actions[action.action_id].result == action.result


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["action", "digest", "nan", "object", "oversized", "nonobject"])
async def test_invalid_receipt_cannot_change_unknown_or_durable_evidence(tmp_path, bad):
    env = runtime_at(tmp_path)
    action = await unknown(env)
    before = env.journal.records()
    async def status(a):
        value = observation(a)
        if bad == "action":
            return replace(value, action_id="another-action")
        if bad == "digest":
            return replace(value, digest="0" * 64)
        return replace(value, receipt={"nan": {"x": float("nan")}, "object": {"x": object()},
                                       "oversized": {"x": "a" * 17000}, "nonobject": []}[bad])
    with pytest.raises((ValueError, PermissionDeniedError)):
        await env.manager.reconcile_write(action.action_id, status)
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN and action.result is None
    assert env.journal.records() == before


@pytest.mark.asyncio
async def test_epoch_change_during_status_preserves_stale_receipt(tmp_path):
    env = runtime_at(tmp_path)
    action = await unknown(env)
    async def status(a):
        await env.manager.epoch_clock.advance("SIMULATED correction")
        return observation(a)
    await env.manager.reconcile_write(action.action_id, status)
    assert action.state == ActionState.COMMITTED_STALE
    assert action not in env.manager._chain
    assert env.journal.records()[0][1]["result"] == {"proof": "SIMULATED:terminal"}


@pytest.mark.asyncio
async def test_query_cannot_overwrite_existing_forward_receipt(tmp_path):
    env = runtime_at(tmp_path)
    action = await unknown(env)
    action.result = {"original": "SIMULATED:receipt"}
    action.state = ActionState.COMMITTED_STALE
    env.manager._persist_action(action, "RECONCILED_PRESENT_STALE")
    async def status(a):
        return observation(a, {"new": "SIMULATED:read"})
    await env.manager.reconcile_write(action.action_id, status)
    assert action.result == {"original": "SIMULATED:receipt"}


@pytest.mark.asyncio
async def test_payload_edit_during_query_cannot_attach_receipt_to_changed_operation(tmp_path):
    env = runtime_at(tmp_path)
    action = await unknown(env)
    async def status(a):
        value = observation(a)
        a.args["target"] = "different-resource"
        return value
    with pytest.raises(PermissionDeniedError):
        await env.manager.reconcile_write(action.action_id, status)
    assert action.state == ActionState.IN_FLIGHT_UNKNOWN and action.result is None
    assert env.journal.records()[0][1]["args"] == {"target": "known-resource"}


@pytest.mark.asyncio
async def test_concurrent_positive_observations_query_and_append_once(tmp_path):
    env = runtime_at(tmp_path)
    action = await unknown(env)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def status(a):
        calls.append(a.action_id)
        entered.set()
        await release.wait()
        return observation(a)
    first = asyncio.create_task(env.manager.reconcile_write(action.action_id, status))
    await asyncio.wait_for(entered.wait(), 3)
    second = asyncio.create_task(env.manager.reconcile_write(action.action_id, status))
    release.set()
    await asyncio.wait_for(asyncio.gather(first, second), 3)
    assert calls == [action.action_id] and env.manager._chain == [action]


@pytest.mark.asyncio
async def test_receipt_is_detached_from_provider_owned_mutable_object(tmp_path):
    env = runtime_at(tmp_path)
    action = await unknown(env)
    receipt = {"nested": {"value": "SIMULATED:terminal"}}
    async def status(a):
        return observation(a, receipt)
    await env.manager.reconcile_write(action.action_id, status)
    receipt["nested"]["value"] = "changed"
    assert action.result["nested"]["value"] == "SIMULATED:terminal"
