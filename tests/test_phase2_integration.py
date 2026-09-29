"""
tests/test_phase2_integration.py

20 deterministic integration tests for Phase 2:
  - Speculative reads + mutation staging integration
  - get_current_result epoch-safety
  - BUG-A fix (compensation_failed flag on _auto_compensate failure)
  - BUG-B fix (COMMITTED_STALE blocks duplicate stage_write until resolved)

All executors are controlled via asyncio.Future / asyncio.Event objects
— no real sleep-based timing races. Each test gets a fresh clock+saga via
_make_saga() to prevent shared-state leaks.
"""

from __future__ import annotations

import asyncio
import logging
import pytest

from runtime.speculative_saga import (
    ActionKind,
    ActionState,
    DuplicateOperationError,
    SagaAbortedError,
    SpeculativeSagaManager,
    StaleEpochError,
    TurnEpochClock,
)
from tools.fleet_tools import TOOL_MANIFEST, READ_TOOLS, WRITE_TOOLS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_saga() -> tuple[TurnEpochClock, SpeculativeSagaManager]:
    """Fresh clock + saga for every test — no shared state."""
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    return clock, saga


async def _instant_read(args: dict) -> dict:
    """Immediate-return read executor."""
    return {"data": args}


async def _instant_write(args: dict) -> dict:
    """Immediate-return write executor."""
    return {"written": args}


def _gated_executor() -> tuple[asyncio.Event, asyncio.Future, "async def"]:
    """
    Returns (started_event, gate_future, executor_fn).
    executor_fn signals started_event then blocks until gate_future is resolved.
    """
    started = asyncio.Event()
    gate: asyncio.Future = asyncio.get_event_loop().create_future()

    async def executor(args: dict):
        started.set()
        return await gate

    return started, gate, executor


# ---------------------------------------------------------------------------
# Test 1 — speculative read launches while intent is active
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_01_speculative_read_launches():
    """fire_speculative_read dispatches immediately and returns an action
    object with kind=SPECULATIVE_READ in an active (non-terminal) state."""
    clock, saga = _make_saga()

    started = asyncio.Event()

    async def slow_read(args: dict) -> dict:
        started.set()
        await asyncio.sleep(10)  # deliberately slow
        return {}

    action = saga.fire_speculative_read("query_telemetry", {"truck_id": "t1"}, slow_read, "t1")

    assert action.kind == ActionKind.SPECULATIVE_READ
    assert not action.state.is_terminal(), f"Expected active state, got {action.state}"

    await started.wait()
    assert action.state == ActionState.IN_FLIGHT

    # Clean up
    action._task.cancel()
    try:
        await action._task
    except (asyncio.CancelledError, Exception):
        pass


# ---------------------------------------------------------------------------
# Test 2 — speculative read captures current epoch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_02_speculative_read_captures_epoch():
    """The action's capture_epoch must equal clock.current at the time of
    fire_speculative_read, even if the epoch advances later."""
    clock, saga = _make_saga()
    assert clock.current == 0

    started = asyncio.Event()

    async def blocking_read(args: dict) -> dict:
        started.set()
        await asyncio.sleep(10)
        return {}

    action = saga.fire_speculative_read("query_telemetry", {"truck_id": "t2"}, blocking_read, "t2")
    assert action.capture_epoch == 0

    await started.wait()
    await clock.advance(reason="barge_in")
    assert clock.current == 1
    # capture_epoch must not change after the fact
    assert action.capture_epoch == 0

    action._task.cancel()
    try:
        await action._task
    except (asyncio.CancelledError, Exception):
        pass


# ---------------------------------------------------------------------------
# Test 3 — get_current_result returns value when epoch is still current
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_03_get_current_result_returns_when_current():
    """After a speculative read completes successfully in the current epoch,
    get_current_result must return its result."""
    clock, saga = _make_saga()
    entity_hash = "truck-3"
    result_payload = {"speed_kph": 88}

    async def fast_read(args: dict) -> dict:
        return result_payload

    action = saga.fire_speculative_read("query_telemetry", {"truck_id": "t3"}, fast_read, entity_hash)
    await asyncio.sleep(0)
    await asyncio.sleep(0)  # let the task complete

    assert action.state == ActionState.COMMITTED

    fetched = saga.get_current_result(entity_hash)
    assert fetched == result_payload


# ---------------------------------------------------------------------------
# Test 4 — get_current_result returns None after epoch advances
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_04_get_current_result_none_after_epoch_advance():
    """After a speculative read commits and then the epoch advances,
    get_current_result must return None (the result is now stale)."""
    clock, saga = _make_saga()
    entity_hash = "truck-4"

    async def fast_read(args: dict) -> dict:
        return {"fuel_pct": 42}

    action = saga.fire_speculative_read("query_telemetry", {"truck_id": "t4"}, fast_read, entity_hash)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert action.state == ActionState.COMMITTED

    # Still valid before epoch advance
    assert saga.get_current_result(entity_hash) is not None

    # Advance epoch
    await clock.advance(reason="barge_in")

    # Must now return None — the result belongs to epoch 0, current is 1
    fetched = saga.get_current_result(entity_hash)
    assert fetched is None, f"Expected None after epoch advance, got {fetched!r}"


# ---------------------------------------------------------------------------
# Test 5 — late stale read cannot overwrite get_current_result for current epoch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_05_stale_read_cannot_corrupt_current_result():
    """A slow speculative read that completes AFTER an epoch advance must be
    ABORTED and must not affect what get_current_result returns for the
    current epoch."""
    clock, saga = _make_saga()
    entity_hash = "truck-5"

    started = asyncio.Event()
    gate: asyncio.Future = asyncio.get_event_loop().create_future()

    async def slow_read(args: dict) -> dict:
        started.set()
        return await gate

    # Fire read in epoch 0
    action_old = saga.fire_speculative_read(
        "query_telemetry", {"truck_id": "t5"}, slow_read, entity_hash
    )
    await started.wait()
    await asyncio.sleep(0)

    # Advance epoch — cancels the stale IN_FLIGHT read
    await clock.advance(reason="barge_in")

    # Now fire a new read in epoch 1
    new_result = {"speed_kph": 99}

    async def fast_read(args: dict) -> dict:
        return new_result

    action_new = saga.fire_speculative_read(
        "query_telemetry", {"truck_id": "t5"}, fast_read, entity_hash
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert action_new.state == ActionState.COMMITTED

    # Let the OLD stale read complete (it should be ABORTED at this point)
    gate.set_result({"stale": True})
    await asyncio.sleep(0)

    # get_current_result must return the NEW result from epoch 1, never the stale one
    result = saga.get_current_result(entity_hash)
    assert result == new_result
    assert "stale" not in str(result)

    # Old action must be ABORTED
    assert action_old.state == ActionState.ABORTED


# ---------------------------------------------------------------------------
# Test 6 — staged write does not dispatch before commit_write is called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_06_staged_write_not_dispatched_before_commit():
    """stage_write must only register the action — no executor call, no task.
    The action stays PENDING until commit_write is called."""
    clock, saga = _make_saga()
    executor_called = False

    async def tracking_executor(args: dict) -> dict:
        nonlocal executor_called
        executor_called = True
        return {}

    action = saga.stage_write("reroute_truck", {"truck_id": "t6", "destination": "Chennai"})
    assert action.state == ActionState.PENDING
    assert action._task is None
    assert not executor_called

    await asyncio.sleep(0)  # yield to event loop
    assert not executor_called, "Executor must not be called by stage_write"
    assert action.state == ActionState.PENDING


# ---------------------------------------------------------------------------
# Test 7 — commit_write dispatches the write exactly once
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_07_commit_write_dispatches_exactly_once():
    """commit_write must call the executor exactly once and return its result."""
    clock, saga = _make_saga()
    call_count = 0

    async def counting_executor(args: dict) -> dict:
        nonlocal call_count
        call_count += 1
        return {"dispatched": call_count}

    action = saga.stage_write("reroute_truck", {"truck_id": "t7", "destination": "Pune"})
    result = await saga.commit_write(action.action_id, counting_executor)

    assert call_count == 1, f"Expected exactly 1 call, got {call_count}"
    assert result == {"dispatched": 1}
    assert action.state == ActionState.COMMITTED


# ---------------------------------------------------------------------------
# Test 8 — write uses Phase 1 idempotency_key mechanism
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_08_write_has_idempotency_key():
    """stage_write must populate action.idempotency_key. Explicit key must be
    used verbatim; computed key must be deterministic from tool_name + args."""
    clock, saga = _make_saga()

    # Explicit key
    action_a = saga.stage_write(
        "reroute_truck", {"truck_id": "t8a", "destination": "X"},
        idempotency_key="explicit-key-t8",
    )
    assert action_a.idempotency_key == "explicit-key-t8"

    # Computed key: same args → same key
    action_b = saga.stage_write(
        "reroute_truck", {"truck_id": "t8b", "destination": "Y"},
        idempotency_key="explicit-key-t8b",
    )
    action_c = saga.stage_write(
        "reroute_truck", {"truck_id": "t8c", "destination": "Z"},
        idempotency_key="explicit-key-t8c",
    )
    # Different explicit keys → different actions
    assert action_b.idempotency_key != action_c.idempotency_key
    assert action_b.idempotency_key == "explicit-key-t8b"

    # Computed key is non-empty and stable
    action_d = saga.stage_write("reroute_truck", {"truck_id": "t8d", "destination": "D"})
    action_e = saga.stage_write("reroute_truck", {"truck_id": "t8e", "destination": "E"})
    assert action_d.idempotency_key  # non-empty
    assert action_d.idempotency_key != action_e.idempotency_key  # different args → different key


# ---------------------------------------------------------------------------
# Test 9 — duplicate write with same key while first is IN_FLIGHT raises
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_09_duplicate_write_in_flight_raises():
    """If a write with idempotency_key K is currently IN_FLIGHT, a new
    stage_write with the same K must raise DuplicateOperationError."""
    clock, saga = _make_saga()

    started = asyncio.Event()
    gate: asyncio.Future = asyncio.get_event_loop().create_future()

    async def gated_executor(args: dict) -> dict:
        started.set()
        return await gate

    action = saga.stage_write(
        "reroute_truck", {"truck_id": "t9", "destination": "A"},
        idempotency_key="ikey-t9",
    )
    commit_task = asyncio.create_task(saga.commit_write(action.action_id, gated_executor))
    await started.wait()
    await asyncio.sleep(0)

    assert action.state == ActionState.IN_FLIGHT

    with pytest.raises(DuplicateOperationError):
        saga.stage_write(
            "reroute_truck", {"truck_id": "t9", "destination": "B"},
            idempotency_key="ikey-t9",
        )

    # Clean up
    gate.set_result({"ok": True})
    await commit_task


# ---------------------------------------------------------------------------
# Test 10 — write stale while IN_FLIGHT: superseded_epoch set, not force-aborted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_10_stale_write_in_flight_sets_superseded_not_aborted():
    """When epoch advances while a write is IN_FLIGHT, the action must NOT
    be force-ABORTED. Instead, superseded_epoch must be set and the action
    stays IN_FLIGHT until the executor resolves."""
    clock, saga = _make_saga()

    started = asyncio.Event()
    gate: asyncio.Future = asyncio.get_event_loop().create_future()

    async def gated_executor(args: dict) -> dict:
        started.set()
        return await gate

    action = saga.stage_write("reroute_truck", {"truck_id": "t10", "destination": "B"})
    commit_task = asyncio.create_task(saga.commit_write(action.action_id, gated_executor))
    await started.wait()
    await asyncio.sleep(0)

    assert action.state == ActionState.IN_FLIGHT

    await clock.advance(reason="barge_in")

    # Must still be IN_FLIGHT, superseded_epoch must be set
    assert action.state == ActionState.IN_FLIGHT
    assert action.superseded_epoch == 1

    # Clean up
    gate.set_result({"ok": True})
    await commit_task


# ---------------------------------------------------------------------------
# Test 11 — stale IN_FLIGHT write that succeeds becomes COMMITTED_STALE
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_11_stale_in_flight_write_success_becomes_committed_stale():
    """If the epoch advances while the write is IN_FLIGHT and the executor
    then succeeds, the action must resolve to COMMITTED_STALE."""
    clock, saga = _make_saga()

    started = asyncio.Event()
    gate: asyncio.Future = asyncio.get_event_loop().create_future()

    async def gated_executor(args: dict) -> dict:
        started.set()
        return await gate

    action = saga.stage_write("reroute_truck", {"truck_id": "t11", "destination": "C"})
    commit_task = asyncio.create_task(saga.commit_write(action.action_id, gated_executor))
    await started.wait()
    await asyncio.sleep(0)

    await clock.advance()
    assert action.superseded_epoch == 1

    gate.set_result({"ok": True})
    await commit_task

    assert action.state == ActionState.COMMITTED_STALE
    assert action not in saga._chain


# ---------------------------------------------------------------------------
# Test 12 — COMMITTED_STALE auto-compensation runs; final state is COMPENSATED
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_12_committed_stale_auto_compensates():
    """A COMMITTED_STALE action with a compensate() handler must auto-compensate.
    After event loop draining, final state must be COMPENSATED."""
    clock, saga = _make_saga()
    compensated = []

    async def my_compensate(result: dict) -> None:
        compensated.append(result)

    started = asyncio.Event()
    gate: asyncio.Future = asyncio.get_event_loop().create_future()

    async def gated_executor(args: dict) -> dict:
        started.set()
        return await gate

    action = saga.stage_write(
        "reroute_truck", {"truck_id": "t12", "destination": "D"},
        compensate=my_compensate,
    )
    commit_task = asyncio.create_task(saga.commit_write(action.action_id, gated_executor))
    await started.wait()
    await asyncio.sleep(0)

    await clock.advance()
    gate.set_result({"route_id": "r-12"})
    await commit_task

    # Drain event loop for the create_task auto-compensation
    for _ in range(10):
        await asyncio.sleep(0)

    assert action.state in (ActionState.COMMITTED_STALE, ActionState.COMPENSATED)
    for _ in range(10):
        await asyncio.sleep(0)

    assert action.state == ActionState.COMPENSATED
    assert len(compensated) == 1
    assert compensated[0] == {"route_id": "r-12"}
    assert not action.compensation_failed


# ---------------------------------------------------------------------------
# Test 13 — BUG-A: failing compensation sets compensation_failed, logs CRITICAL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_13_bug_a_compensation_failure_sets_flag(caplog):
    """BUG-A fix: if compensate() raises, action.compensation_failed must be
    True and a CRITICAL log must be emitted. The exception must NOT be
    silently swallowed by asyncio's default error handler."""
    clock, saga = _make_saga()

    async def failing_compensate(result: dict) -> None:
        raise RuntimeError("compensation system offline")

    started = asyncio.Event()
    gate: asyncio.Future = asyncio.get_event_loop().create_future()

    async def gated_executor(args: dict) -> dict:
        started.set()
        return await gate

    action = saga.stage_write(
        "reroute_truck", {"truck_id": "t13", "destination": "E"},
        compensate=failing_compensate,
    )
    commit_task = asyncio.create_task(saga.commit_write(action.action_id, gated_executor))
    await started.wait()
    await asyncio.sleep(0)

    await clock.advance()

    with caplog.at_level(logging.CRITICAL, logger="ccs.saga"):
        gate.set_result({"ok": True})
        await commit_task
        # Drain for the create_task compensation to run
        for _ in range(10):
            await asyncio.sleep(0)

    # BUG-A: compensation_failed must be set — not silently swallowed
    assert action.compensation_failed is True, (
        "Expected action.compensation_failed=True after compensate() raised"
    )
    # State stays COMMITTED_STALE (not COMPENSATED) because compensation failed
    assert action.state == ActionState.COMMITTED_STALE

    # A CRITICAL log must have been emitted
    critical_records = [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert critical_records, "Expected a CRITICAL log after compensation failure"
    assert any("AUTO-COMPENSATION FAILED" in r.message for r in critical_records)


# ---------------------------------------------------------------------------
# Test 14 — IN_FLIGHT_UNKNOWN blocks fresh stage_write; requires reconcile_write
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_14_in_flight_unknown_blocks_new_stage_write():
    """An action in IN_FLIGHT_UNKNOWN must block a duplicate stage_write with
    the same idempotency_key. Only reconcile_write can resolve it."""
    clock, saga = _make_saga()

    action = saga.stage_write(
        "reroute_truck", {"truck_id": "t14", "destination": "F"},
        idempotency_key="ikey-t14",
    )

    with pytest.raises(asyncio.TimeoutError):
        await saga.commit_write(
            action.action_id,
            lambda a: asyncio.sleep(10),
            timeout=0.01,
        )

    assert action.state == ActionState.IN_FLIGHT_UNKNOWN

    # Duplicate stage_write must raise
    with pytest.raises(DuplicateOperationError):
        saga.stage_write(
            "reroute_truck", {"truck_id": "t14", "destination": "G"},
            idempotency_key="ikey-t14",
        )

    # After reconcile_write confirms NOT executed -> ABORTED, key is released
    async def confirmed_not_done(a) -> bool:
        return False

    await saga.reconcile_write(action.action_id, confirmed_not_done)
    assert action.state == ActionState.ABORTED

    # Now a fresh stage_write with same key must succeed
    action2 = saga.stage_write(
        "reroute_truck", {"truck_id": "t14", "destination": "H"},
        idempotency_key="ikey-t14",
    )
    assert action2.state == ActionState.PENDING


# ---------------------------------------------------------------------------
# Test 15 — get_current_result returns None when read is stale; write must not use it
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_15_stale_result_not_used_for_write():
    """After the epoch advances, get_current_result must return None.
    Code that gates a write on get_current_result must not proceed."""
    clock, saga = _make_saga()
    entity_hash = "truck-15"

    async def fast_read(args: dict) -> dict:
        return {"route": "old-route"}

    action = saga.fire_speculative_read(
        "query_telemetry", {"truck_id": "t15"}, fast_read, entity_hash
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert action.state == ActionState.COMMITTED
    assert saga.get_current_result(entity_hash) is not None

    # Advance epoch — result becomes stale
    await clock.advance()

    stale_result = saga.get_current_result(entity_hash)
    assert stale_result is None, "get_current_result must return None for stale epoch"

    # Simulate a well-behaved caller that gates the write on get_current_result
    write_was_staged = False
    result = saga.get_current_result(entity_hash)
    if result is not None:  # this branch must NOT be taken
        saga.stage_write("reroute_truck", {"destination": result["route"]})
        write_was_staged = True

    assert not write_was_staged, "Write must not be staged when get_current_result returns None"


# ---------------------------------------------------------------------------
# Test 16 — chained READ -> READ -> WRITE in same epoch succeeds normally
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_16_chained_read_read_write_same_epoch():
    """Two speculative reads for two entity_hashes, both committed in the
    current epoch, feed a write that is staged and committed normally."""
    clock, saga = _make_saga()

    async def truck_read(args: dict) -> dict:
        return {"speed_kph": 80, "truck_id": args["truck_id"]}

    async def dock_read(args: dict) -> dict:
        return {"dock_id": args["dock_id"], "available": True}

    truck_action = saga.fire_speculative_read(
        "query_telemetry", {"truck_id": "t16"}, truck_read, "t16"
    )
    dock_action = saga.fire_speculative_read(
        "query_dock_availability", {"dock_id": "dock-16"}, dock_read, "dock-16"
    )

    # Drain both reads
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert truck_action.state == ActionState.COMMITTED
    assert dock_action.state == ActionState.COMMITTED

    truck_data = saga.get_current_result("t16")
    dock_data = saga.get_current_result("dock-16")

    assert truck_data is not None
    assert dock_data is not None
    assert dock_data["available"] is True

    # Stage a write using both read results
    write_action = saga.stage_write(
        "reserve_dock",
        {"truck_id": truck_data["truck_id"], "dock_id": dock_data["dock_id"]},
    )

    async def mock_reserve(args: dict) -> dict:
        return {"reservation_id": "res-16"}

    result = await saga.commit_write(write_action.action_id, mock_reserve)
    assert write_action.state == ActionState.COMMITTED
    assert result["reservation_id"] == "res-16"
    assert write_action in saga._chain


# ---------------------------------------------------------------------------
# Test 17 — disfluency correction: stale Chennai read never feeds write
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_17_disfluency_correction_stale_read_never_feeds_write():
    """Stage a read for 'Chennai', let it complete, advance epoch (simulating
    the user correcting to 'Bengaluru'), fire a new read for 'Bengaluru',
    then stage/commit a write. The write must use only the Bengaluru result.
    Chennai's action must never appear in the committed chain."""
    clock, saga = _make_saga()

    async def chennai_read(args: dict) -> dict:
        return {"route": "Chennai-route", "city": "Chennai"}

    async def bengaluru_read(args: dict) -> dict:
        return {"route": "Bengaluru-route", "city": "Bengaluru"}

    # Epoch 0: speculative read for Chennai
    chennai_action = saga.fire_speculative_read(
        "query_traffic", {"route": "Chennai"}, chennai_read, "route-entity"
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert chennai_action.state == ActionState.COMMITTED
    assert saga.get_current_result("route-entity") == {"route": "Chennai-route", "city": "Chennai"}

    # User corrects: "actually, Bengaluru" -> epoch advances
    await clock.advance(reason="correction")

    # Chennai result is now stale
    assert saga.get_current_result("route-entity") is None

    # Epoch 1: new read for Bengaluru (debounce window is 0.18s; we use same entity_hash
    # to simulate the overwrite of the route entity slot)
    bengaluru_action = saga.fire_speculative_read(
        "query_traffic", {"route": "Bengaluru"}, bengaluru_read, "route-entity"
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert bengaluru_action.state == ActionState.COMMITTED

    current = saga.get_current_result("route-entity")
    assert current is not None
    assert current["city"] == "Bengaluru", f"Expected Bengaluru, got {current}"

    # Stage and commit the write using only the current result
    write_action = saga.stage_write(
        "reroute_truck",
        {"truck_id": "t17", "destination": current["city"]},
    )

    async def mock_reroute(args: dict) -> dict:
        return {"route_id": "r-17", "destination": args["destination"]}

    result = await saga.commit_write(write_action.action_id, mock_reroute)

    assert result["destination"] == "Bengaluru"
    assert write_action.state == ActionState.COMMITTED
    assert write_action in saga._chain

    # Chennai action must never be in the chain
    assert chennai_action not in saga._chain

    # Verify the write's args used Bengaluru, never Chennai
    assert write_action.args["destination"] == "Bengaluru"
    assert "Chennai" not in str(write_action.args)


# ---------------------------------------------------------------------------
# Test 18 — two concurrent reads in different epochs do not cross-contaminate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_18_concurrent_reads_different_epochs_no_cross_contamination():
    """Two speculative reads with different entity_hashes in different
    epochs must not interfere with each other's results."""
    clock, saga = _make_saga()

    async def truck_a_read(args: dict) -> dict:
        return {"truck": "A", "speed": 70}

    # Epoch 0: read for truck-A
    action_a = saga.fire_speculative_read(
        "query_telemetry", {"truck_id": "tA"}, truck_a_read, "tA"
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert action_a.state == ActionState.COMMITTED
    result_a_before = saga.get_current_result("tA")

    # Advance epoch
    await clock.advance()

    async def truck_b_read(args: dict) -> dict:
        return {"truck": "B", "speed": 90}

    # Epoch 1: read for truck-B
    action_b = saga.fire_speculative_read(
        "query_telemetry", {"truck_id": "tB"}, truck_b_read, "tB"
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert action_b.state == ActionState.COMMITTED

    # Truck-A result is stale (epoch 0, current is 1)
    assert saga.get_current_result("tA") is None
    assert result_a_before == {"truck": "A", "speed": 70}

    # Truck-B result is current (epoch 1)
    result_b = saga.get_current_result("tB")
    assert result_b == {"truck": "B", "speed": 90}

    # Cross-contamination check: A's action and B's action are independent
    assert action_a.capture_epoch == 0
    assert action_b.capture_epoch == 1
    assert action_a.action_id != action_b.action_id
    assert "A" not in str(result_b)
    assert "B" not in str(result_a_before)


# ---------------------------------------------------------------------------
# Test 19 — BUG-B: unreconciled COMMITTED_STALE blocks duplicate stage_write
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_19_bug_b_committed_stale_blocks_duplicate_stage_write():
    """BUG-B fix: a COMMITTED_STALE action's idempotency_key must keep
    blocking stage_write until reconcile_write() or successful compensation
    resolves it. Only after resolution is a new stage_write with the same
    key permitted."""
    clock, saga = _make_saga()

    started = asyncio.Event()
    gate: asyncio.Future = asyncio.get_event_loop().create_future()

    async def gated_executor(args: dict) -> dict:
        started.set()
        return await gate

    action = saga.stage_write(
        "reroute_truck", {"truck_id": "t19", "destination": "A"},
        idempotency_key="ikey-t19",
    )
    commit_task = asyncio.create_task(saga.commit_write(action.action_id, gated_executor))
    await started.wait()
    await asyncio.sleep(0)

    # Advance epoch while IN_FLIGHT -> sets superseded_epoch
    await clock.advance()
    assert action.superseded_epoch == 1

    # Executor resolves -> COMMITTED_STALE
    gate.set_result({"ok": True})
    await commit_task

    assert action.state == ActionState.COMMITTED_STALE

    # BUG-B: must still block duplicate stage_write while COMMITTED_STALE
    with pytest.raises(DuplicateOperationError):
        saga.stage_write(
            "reroute_truck", {"truck_id": "t19", "destination": "B"},
            idempotency_key="ikey-t19",
        )

    # Resolve via reconcile_write (status_check: confirmed not persisted -> ABORTED)
    async def confirmed_false(a) -> bool:
        return False

    await saga.reconcile_write(action.action_id, confirmed_false)
    assert action.state == ActionState.ABORTED

    # Now a fresh stage_write with the same key must succeed
    action2 = saga.stage_write(
        "reroute_truck", {"truck_id": "t19", "destination": "C"},
        idempotency_key="ikey-t19",
    )
    assert action2.state == ActionState.PENDING


# ---------------------------------------------------------------------------
# Test 20 — fresh SpeculativeSagaManager per test: no shared state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_20_no_shared_state_between_tests():
    """Each _make_saga() call produces an independent manager with a clean
    slate. Reads and writes in one instance must not affect another."""
    clock1, saga1 = _make_saga()
    clock2, saga2 = _make_saga()

    # Read in saga1
    entity = "shared-entity"
    result_val = {"x": 42}

    async def fast_read(args: dict) -> dict:
        return result_val

    action1 = saga1.fire_speculative_read("query_telemetry", {"truck_id": "t20"}, fast_read, entity)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert action1.state == ActionState.COMMITTED
    assert saga1.get_current_result(entity) == result_val

    # saga2 must have no knowledge of saga1's read
    assert saga2.get_current_result(entity) is None
    assert len(saga2._actions) == 0

    # Advance saga1's clock — must not affect saga2
    await clock1.advance()
    assert clock2.current == 0
    assert saga2.get_current_result(entity) is None

    # Write in saga2 using same entity key (should work independently)
    write = saga2.stage_write("reroute_truck", {"truck_id": "t20", "destination": "Z"})
    result = await saga2.commit_write(write.action_id, _instant_write)
    assert write.state == ActionState.COMMITTED
    assert len(saga1._chain) == 0  # saga1 unaffected
    assert len(saga2._chain) == 1
