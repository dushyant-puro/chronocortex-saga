"""
tests/test_phase4_speculative_cognition.py

New Phase 4 (PRISM roadmap) — Speculative Cognition Expansion:
1. Read deduplication (within-epoch exact tool+canonical args cache)
2. Read deduplication invalidation upon epoch advance
3. get_latency_stats() real dispatch latency measurement reporting
4. Fleet domain prediction rule: query_telemetry completion triggers
   query_traffic iff destination is resolvable via resolve_current_value
   (HARD CONSTRAINT: never fires if resolve_current_value returns None)
5. Prefetched read staleness handling upon epoch advance
6. Full regression across all existing suites
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from typing import Any

import pytest

# Ensure repo root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runtime.grounding_guard import GroundingGuard
from runtime.speculative_saga import (
    ActionKind,
    ActionState,
    SpeculativeSagaManager,
    TurnEpochClock,
)
from tools.fleet_tools import READ_TOOLS


def _make_env() -> tuple[TurnEpochClock, SpeculativeSagaManager, GroundingGuard]:
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    guard = GroundingGuard(epoch_clock=clock)
    return clock, saga, guard


# ============================================================================
# 1. READ DEDUPLICATION
# ============================================================================

@pytest.mark.asyncio
async def test_01_read_deduplication_same_epoch():
    """Two identical speculative read requests (same tool, same args) within
    the same epoch result in exactly one real executor call."""
    clock, saga, _ = _make_env()

    call_count = 0
    gate = asyncio.Event()

    async def counting_executor(args: dict[str, Any]) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        await gate.wait()
        return {"telemetry": "ok", "truck_id": args["truck_id"]}

    # Fire first read
    a1 = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=counting_executor,
        entity_hash="truck_id:truck-17",
    )

    # Fire second read with identical tool & args in same epoch
    a2 = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=counting_executor,
        entity_hash="truck_id:truck-17",
    )

    # Both requests return the same action instance
    assert a1.action_id == a2.action_id
    assert a1 is a2

    # Release executor
    gate.set()
    await a1._task

    # Crucial assertion: executor called exactly once, not twice
    assert call_count == 1
    assert a1.state == ActionState.COMMITTED
    assert a1.result == {"telemetry": "ok", "truck_id": "truck-17"}


@pytest.mark.asyncio
async def test_01b_racing_concurrent_identical_speculative_reads():
    """Two coroutines call fire_speculative_read with identical tool_name+args
    in the same epoch at effectively the same moment, synchronized via asyncio.Event.
    Assert exactly ONE real executor call occurs (not two)."""
    clock, saga, _ = _make_env()

    sync_event = asyncio.Event()
    executor_gate = asyncio.Event()
    call_count = 0
    actions: list[Any] = []

    async def counting_executor(args: dict[str, Any]) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        await executor_gate.wait()
        return {"data": args, "count": call_count}

    async def caller():
        await sync_event.wait()
        act = saga.fire_speculative_read(
            tool_name="query_telemetry",
            args={"truck_id": "truck-99"},
            executor=counting_executor,
            entity_hash="truck_id:truck-99",
        )
        actions.append(act)

    task_0 = asyncio.create_task(caller())
    task_1 = asyncio.create_task(caller())

    # Release both callers simultaneously
    sync_event.set()
    await asyncio.gather(task_0, task_1)

    assert len(actions) == 2
    act_0, act_1 = actions[0], actions[1]
    assert act_0.action_id == act_1.action_id
    assert act_0 is act_1

    # Release the mock executor
    executor_gate.set()
    await act_0._task

    # Exactly one executor dispatch occurred
    assert call_count == 1
    assert act_0.state == ActionState.COMMITTED
    assert act_0.result == {"data": {"truck_id": "truck-99"}, "count": 1}



# ============================================================================
# 2. DEDUP CACHE INVALIDATION ON EPOCH ADVANCE
# ============================================================================

@pytest.mark.asyncio
async def test_02_dedup_cache_invalidated_on_epoch_advance():
    """A deduplicated read's cached result is correctly invalidated when
    the epoch advances before caller checks get_current_result."""
    clock, saga, _ = _make_env()

    call_count = 0

    async def executor(args: dict[str, Any]) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return {"speed": 88, "call": call_count}

    # Fire read in epoch 0
    a1 = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=executor,
        entity_hash="truck_id:truck-17",
    )
    await a1._task
    assert a1.state == ActionState.COMMITTED
    assert saga.get_current_result("truck_id:truck-17") == {"speed": 88, "call": 1}

    # Advance epoch to 1
    await clock.advance(reason="turn_boundary")

    # get_current_result must return None for stale epoch
    assert saga.get_current_result("truck_id:truck-17") is None

    # Now fire the same tool + args again in epoch 1
    # Dedup cache must NOT return the epoch 0 action; it must fire a fresh read
    a2 = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=executor,
        entity_hash="truck_id:truck-17",
    )
    assert a2.action_id != a1.action_id
    assert a2.capture_epoch == 1
    await a2._task

    # Call count increased to 2
    assert call_count == 2
    assert saga.get_current_result("truck_id:truck-17") == {"speed": 88, "call": 2}


# ============================================================================
# 3. LATENCY MEASUREMENT
# ============================================================================

@pytest.mark.asyncio
async def test_03_latency_stats_reporting():
    """get_latency_stats() reports real measured values (not zero, not a
    hardcoded constant) for at least one completed speculative read."""
    clock, saga, _ = _make_env()

    # Initial stats should be empty
    empty_stats = saga.get_latency_stats()
    assert empty_stats["count"] == 0
    assert empty_stats["min_ms"] is None

    async def sleep_executor(args: dict[str, Any]) -> dict[str, Any]:
        await asyncio.sleep(0.02)  # ~20ms sleep
        return {"data": args}

    a1 = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=sleep_executor,
        entity_hash="truck_id:truck-17",
    )
    await a1._task

    assert a1.dispatch_latency_ms is not None
    assert a1.dispatch_latency_ms > 0.0

    stats = saga.get_latency_stats()
    assert stats["count"] == 1
    assert stats["min_ms"] is not None
    assert stats["max_ms"] is not None
    assert stats["mean_ms"] is not None
    assert stats["min_ms"] == stats["max_ms"] == stats["mean_ms"]
    assert stats["min_ms"] > 0.0  # Real non-zero measurement


# ============================================================================
# 4. FLEET-SPECIFIC PREDICTION RULE (WITH HARD SAFETY CONSTRAINT)
# ============================================================================

@pytest.mark.asyncio
async def test_04_prediction_rule_grounded_vs_none():
    """
    Fleet prediction rule: when query_telemetry completes, query_traffic
    is speculatively pre-fired IFF destination is resolvable via
    resolve_current_value.

    HARD CONSTRAINT: if resolve_current_value returns None, query_traffic
    MUST NOT be fired (no guessing, no empty string, no placeholder args).
    """
    clock, saga, guard = _make_env()

    traffic_calls: list[dict[str, Any]] = []

    async def mock_telemetry(args: dict[str, Any]) -> dict[str, Any]:
        return {"speed_kph": 85, "truck_id": args["truck_id"]}

    async def mock_traffic(args: dict[str, Any]) -> dict[str, Any]:
        traffic_calls.append(dict(args))
        return {"congestion": "low", "route": args["route"]}

    # Register the narrow fleet-specific prediction hook
    def fleet_prediction_hook(action, result):
        if action.tool_name == "query_telemetry":
            destination = guard.resolve_current_value("destination")
            if destination is not None:
                saga.fire_speculative_read(
                    tool_name="query_traffic",
                    args={"route": destination},
                    executor=mock_traffic,
                    entity_hash=f"route:{destination}",
                )

    saga.set_prediction_hook(fleet_prediction_hook)

    # ------------------------------------------------------------------------
    # Sub-case A: HARD CONSTRAINT TEST (destination is None)
    # ------------------------------------------------------------------------
    # At this point, guard has no staged candidate for 'destination'.
    assert guard.resolve_current_value("destination") is None

    a_telemetry_none = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=mock_telemetry,
        entity_hash="truck_id:truck-17",
    )
    await a_telemetry_none._task

    # Crucial assertion: query_traffic was NOT called
    assert len(traffic_calls) == 0, (
        f"HARD CONSTRAINT VIOLATION: query_traffic was fired with args {traffic_calls} "
        f"even though resolve_current_value('destination') was None!"
    )

    # ------------------------------------------------------------------------
    # Sub-case B: Happy path with grounded destination
    # ------------------------------------------------------------------------
    # User says 'Bengaluru' with high confidence
    tok_idx = guard.ingest_token("Bengaluru", 0.95, 1.0, 1.5)
    guard.stage_candidate("destination", "Bengaluru", (tok_idx, tok_idx + 1))
    assert guard.resolve_current_value("destination") == "Bengaluru"

    # Fire telemetry for truck-42
    a_telemetry_grounded = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-42"},
        executor=mock_telemetry,
        entity_hash="truck_id:truck-42",
    )
    await a_telemetry_grounded._task

    # Now query_traffic MUST have been pre-fired with the real grounded destination
    assert len(traffic_calls) == 1
    assert traffic_calls[0] == {"route": "Bengaluru"}

    # Ensure pre-fetched traffic action completes and is accessible
    prefetched_traffic = saga.get_current_result("route:Bengaluru")
    assert prefetched_traffic is not None
    assert prefetched_traffic["route"] == "Bengaluru"


# ============================================================================
# 5. PREFETCHED READ STALENESS HANDLING
# ============================================================================

@pytest.mark.asyncio
async def test_05_prefetched_read_stale_on_epoch_advance():
    """A prefetched/predicted read that becomes stale (epoch advances before
    consumption) is correctly excluded by get_current_result."""
    clock, saga, guard = _make_env()

    async def mock_telemetry(args: dict[str, Any]) -> dict[str, Any]:
        return {"speed_kph": 90}

    async def mock_traffic(args: dict[str, Any]) -> dict[str, Any]:
        return {"route": args["route"], "congestion": "moderate"}

    def prediction_hook(action, result):
        if action.tool_name == "query_telemetry":
            dest = guard.resolve_current_value("destination")
            if dest is not None:
                saga.fire_speculative_read(
                    tool_name="query_traffic",
                    args={"route": dest},
                    executor=mock_traffic,
                    entity_hash=f"route:{dest}",
                )

    saga.set_prediction_hook(prediction_hook)

    # Stage grounded destination in epoch 0
    t_idx = guard.ingest_token("Chennai", 0.92, 0.0, 0.5)
    guard.stage_candidate("destination", "Chennai", (t_idx, t_idx + 1))

    # Fire telemetry -> triggers traffic prefetch
    t_action = saga.fire_speculative_read(
        tool_name="query_telemetry",
        args={"truck_id": "truck-17"},
        executor=mock_telemetry,
        entity_hash="truck_id:truck-17",
    )
    await t_action._task

    # Prefetched traffic is available in epoch 0
    res_ep0 = saga.get_current_result("route:Chennai")
    assert res_ep0 is not None
    assert res_ep0["route"] == "Chennai"

    # Epoch advances (e.g. user correction or barge-in)
    await clock.advance(reason="barge_in")

    # The prefetched traffic result MUST now be excluded (stale)
    res_ep1 = saga.get_current_result("route:Chennai")
    assert res_ep1 is None
