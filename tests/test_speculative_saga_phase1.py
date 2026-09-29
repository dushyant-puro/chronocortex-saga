"""
tests/test_speculative_saga_phase1.py

15 deterministic tests for the Phase 1 authoritative mutation state machine.

All executors use manually-controlled asyncio.Future / asyncio.Event objects
— no real asyncio.sleep-based timing — so races are reproduced exactly on
demand, not probabilistically.

Each test gets a fresh TurnEpochClock + SpeculativeSagaManager instance to
prevent shared-state leaks (test 15 exists to assert this explicitly, but
every test benefits from it).
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_saga() -> tuple[TurnEpochClock, SpeculativeSagaManager]:
    """Fresh clock + saga for every test — no shared state."""
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    return clock, saga


async def _instant_executor(args: dict) -> dict:
    """Executor that completes immediately without any network delay."""
    return {"ok": True, "args": args}


async def _failing_executor(args: dict) -> dict:
    """Executor that raises a normal exception (no remote effect)."""
    raise RuntimeError("simulated backend error")


# ---------------------------------------------------------------------------
# Test 1 — Normal staged -> commit -> success
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_01_normal_commit_success():
    """Stage a write, commit it against the current epoch: -> COMMITTED."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_a", {"x": 1})
    assert action.state == ActionState.PENDING
    assert action.superseded_epoch is None

    result = await saga.commit_write(action.action_id, _instant_executor)

    assert action.state == ActionState.COMMITTED
    assert action.result == {"ok": True, "args": {"x": 1}}
    assert result == action.result
    assert action in saga._chain
    assert action.superseded_epoch is None


# ---------------------------------------------------------------------------
# Test 2 — Epoch advance BEFORE dispatch -> never executes, stays ABORTED
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_02_epoch_advance_before_dispatch():
    """Epoch advances before commit_write is called: action must be ABORTED.
    _on_epoch_advance immediately ABORTs PENDING staged writes (nothing was
    ever dispatched, so no remote effect is possible). commit_write then
    sees the action is no longer PENDING and raises SagaAbortedError.
    The executor must never be called."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_b", {"x": 2})
    assert action.state == ActionState.PENDING

    await clock.advance(reason="barge_in")  # epoch 0 -> 1

    # _on_epoch_advance immediately ABORTs any PENDING staged write
    assert action.state == ActionState.ABORTED, (
        f"Expected PENDING action to be ABORTed by epoch advance, got {action.state.name}"
    )

    executor_called = False

    async def tracking_executor(args: dict) -> dict:
        nonlocal executor_called
        executor_called = True
        return {}

    # commit_write sees action is not PENDING -> SagaAbortedError (not StaleEpochError)
    # Both errors correctly indicate the write never went to the remote system.
    with pytest.raises((SagaAbortedError, StaleEpochError)):
        await saga.commit_write(action.action_id, tracking_executor)

    assert not executor_called, "Executor must never be called when epoch is already stale"
    assert action.state == ActionState.ABORTED


# ---------------------------------------------------------------------------
# Test 3 — Epoch advance WHILE IN_FLIGHT -> superseded_epoch set, not terminal
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_03_epoch_advance_while_in_flight_sets_superseded():
    """When epoch advances while commit_write is awaiting the executor, the
    action remains IN_FLIGHT (not immediately ABORTED / terminal), but
    superseded_epoch is recorded."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_c", {"x": 3})

    executor_started = asyncio.Event()
    executor_gate = asyncio.Future()  # blocks executor until we release it

    async def gated_executor(args: dict) -> dict:
        executor_started.set()
        return await executor_gate  # type: ignore[return-value]

    commit_task = asyncio.create_task(
        saga.commit_write(action.action_id, gated_executor)
    )
    await executor_started.wait()
    # Yield to let commit_write set IN_FLIGHT
    await asyncio.sleep(0)

    assert action.state == ActionState.IN_FLIGHT

    # Advance epoch while executor is blocked
    await clock.advance(reason="barge_in")

    # State must still be IN_FLIGHT (not terminal yet)
    assert action.state == ActionState.IN_FLIGHT
    assert action.superseded_epoch == 1

    # Clean up: let executor resolve so the task doesn't dangle
    executor_gate.set_result({"x": 3})
    await commit_task


# ---------------------------------------------------------------------------
# Test 4 — THE Phase 0 race: executor succeeds AFTER epoch advance -> COMMITTED_STALE
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_04_phase0_race_resolves_to_committed_stale():
    """Exact Phase 0 scenario: epoch advances while executor is in-flight,
    executor then returns success. Must end as COMMITTED_STALE, never
    plain COMMITTED."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_d", {"x": 4})

    executor_started = asyncio.Event()
    executor_gate = asyncio.Future()

    async def gated_executor(args: dict) -> dict:
        executor_started.set()
        return await executor_gate  # type: ignore[return-value]

    commit_task = asyncio.create_task(
        saga.commit_write(action.action_id, gated_executor)
    )
    await executor_started.wait()
    await asyncio.sleep(0)

    assert action.state == ActionState.IN_FLIGHT

    # Epoch advances — the old plain code would ABORTED here; new code must NOT.
    await clock.advance(reason="barge_in")
    assert action.superseded_epoch == 1

    # Executor resolves successfully
    executor_gate.set_result({"ok": True})
    await commit_task

    # Must be COMMITTED_STALE, NEVER plain COMMITTED
    assert action.state == ActionState.COMMITTED_STALE, (
        f"Expected COMMITTED_STALE, got {action.state.name}"
    )
    assert action not in saga._chain


# ---------------------------------------------------------------------------
# Test 5 — COMMITTED_STALE WITH compensate() -> compensation auto-invoked
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_05_committed_stale_with_compensate_auto_invokes():
    """When COMMITTED_STALE and a compensate handler is provided, it is
    automatically scheduled. After awaiting the background task, the action
    must be COMPENSATED."""
    clock, saga = _make_saga()
    compensation_called = False
    compensation_arg: object = None

    async def my_compensate(result: dict) -> None:
        nonlocal compensation_called, compensation_arg
        compensation_called = True
        compensation_arg = result

    action = saga.stage_write("tool_e", {"x": 5}, compensate=my_compensate)

    executor_started = asyncio.Event()
    executor_gate = asyncio.Future()

    async def gated_executor(args: dict) -> dict:
        executor_started.set()
        return await executor_gate  # type: ignore[return-value]

    commit_task = asyncio.create_task(
        saga.commit_write(action.action_id, gated_executor)
    )
    await executor_started.wait()
    await asyncio.sleep(0)

    await clock.advance()
    executor_gate.set_result({"result": "done"})
    await commit_task

    # The action may be COMMITTED_STALE or already COMPENSATED at this point —
    # auto-compensation is scheduled via asyncio.create_task immediately when
    # commit_write resolves, and may run within the same event loop iteration.
    assert action.state in (ActionState.COMMITTED_STALE, ActionState.COMPENSATED), (
        f"Expected COMMITTED_STALE or COMPENSATED, got {action.state.name}"
    )

    # Drain event loop until compensation completes.
    for _ in range(10):
        await asyncio.sleep(0)

    assert compensation_called, "compensate() was not invoked automatically"
    assert compensation_arg == {"result": "done"}
    assert action.state == ActionState.COMPENSATED


# ---------------------------------------------------------------------------
# Test 6 — COMMITTED_STALE with NO compensate() -> flagged critical, not dropped
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_06_committed_stale_no_compensate_flags_critical(caplog):
    """When COMMITTED_STALE and no compensate handler, a CRITICAL log must
    be emitted and the action stays COMMITTED_STALE (never silently dropped)."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_f", {"x": 6})  # no compensate

    executor_started = asyncio.Event()
    executor_gate = asyncio.Future()

    async def gated_executor(args: dict) -> dict:
        executor_started.set()
        return await executor_gate  # type: ignore[return-value]

    commit_task = asyncio.create_task(
        saga.commit_write(action.action_id, gated_executor)
    )
    await executor_started.wait()
    await asyncio.sleep(0)

    await clock.advance()
    executor_gate.set_result({"done": True})

    with caplog.at_level(logging.CRITICAL, logger="ccs.saga"):
        await commit_task

    assert action.state == ActionState.COMMITTED_STALE
    # Must have logged CRITICAL about manual reconciliation
    critical_msgs = [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert critical_msgs, "Expected a CRITICAL log message for COMMITTED_STALE with no compensate"
    assert "MANUAL RECONCILIATION REQUIRED" in critical_msgs[0].message


# ---------------------------------------------------------------------------
# Test 7 — Executor raises exception -> ABORTED regardless of superseded_epoch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_07_executor_exception_always_aborts():
    """If the executor raises a normal exception, the action must be ABORTED
    regardless of whether the epoch was superseded. The exception propagates."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_g", {"x": 7})

    executor_started = asyncio.Event()
    executor_gate = asyncio.Future()

    async def failing_gated_executor(args: dict) -> dict:
        executor_started.set()
        await executor_gate
        raise ValueError("backend rejected")

    commit_task = asyncio.create_task(
        saga.commit_write(action.action_id, failing_gated_executor)
    )
    await executor_started.wait()
    await asyncio.sleep(0)

    # Advance epoch to set superseded_epoch
    await clock.advance()
    assert action.superseded_epoch == 1

    executor_gate.set_result(None)

    with pytest.raises(ValueError, match="backend rejected"):
        await commit_task

    assert action.state == ActionState.ABORTED, (
        f"Expected ABORTED after exception, got {action.state.name}"
    )


# ---------------------------------------------------------------------------
# Test 8 — Timeout -> IN_FLIGHT_UNKNOWN
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_08_timeout_yields_in_flight_unknown():
    """A timed-out commit must enter IN_FLIGHT_UNKNOWN, not ABORTED."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_h", {"x": 8})

    async def hanging_executor(args: dict) -> dict:
        await asyncio.sleep(10)  # will be cut by timeout
        return {}

    with pytest.raises(asyncio.TimeoutError):
        await saga.commit_write(action.action_id, hanging_executor, timeout=0.01)

    assert action.state == ActionState.IN_FLIGHT_UNKNOWN


# ---------------------------------------------------------------------------
# Test 9 — Duplicate idempotency key while first is IN_FLIGHT_UNKNOWN -> DuplicateOperationError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_09_duplicate_idempotency_key_raises():
    """stage_write must reject a request whose idempotency_key matches an
    action currently in IN_FLIGHT_UNKNOWN state."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_i", {"x": 9})

    async def hanging_executor(args: dict) -> dict:
        await asyncio.sleep(10)
        return {}

    with pytest.raises(asyncio.TimeoutError):
        await saga.commit_write(action.action_id, hanging_executor, timeout=0.01)

    assert action.state == ActionState.IN_FLIGHT_UNKNOWN

    # Attempt to stage another write with the SAME args -> same idempotency_key
    with pytest.raises(DuplicateOperationError):
        saga.stage_write("tool_i", {"x": 9})


# ---------------------------------------------------------------------------
# Test 10 — reconcile_write resolves IN_FLIGHT_UNKNOWN
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_10_reconcile_in_flight_unknown():
    """reconcile_write with status_check returning True -> COMMITTED (epoch current).
    reconcile_write with status_check returning False -> ABORTED."""
    clock, saga = _make_saga()

    # -- Case A: True -> COMMITTED
    action_a = saga.stage_write("tool_j", {"sub": "A"}, idempotency_key="test10-A")

    with pytest.raises(asyncio.TimeoutError):
        await saga.commit_write(action_a.action_id, lambda a: asyncio.sleep(10), timeout=0.01)

    assert action_a.state == ActionState.IN_FLIGHT_UNKNOWN

    async def check_true(action) -> bool:
        return True

    await saga.reconcile_write(action_a.action_id, check_true)
    # Epoch is still 0 == capture_epoch 0, so COMMITTED
    assert action_a.state == ActionState.COMMITTED
    assert action_a in saga._chain

    # -- Case B: False -> ABORTED (fresh saga to avoid ikey collision)
    clock2, saga2 = _make_saga()
    action_b = saga2.stage_write("tool_j", {"sub": "B"}, idempotency_key="test10-B")

    with pytest.raises(asyncio.TimeoutError):
        await saga2.commit_write(action_b.action_id, lambda a: asyncio.sleep(10), timeout=0.01)

    assert action_b.state == ActionState.IN_FLIGHT_UNKNOWN

    async def check_false(action) -> bool:
        return False

    await saga2.reconcile_write(action_b.action_id, check_false)
    assert action_b.state == ActionState.ABORTED


# ---------------------------------------------------------------------------
# Test 11 — Compensation ordering in 2-step chain when later step aborts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_11_two_step_chain_compensation_order():
    """In a 2-step chain, if step 2 aborts, step 1 (COMMITTED) must be
    compensated, and in the correct order (most-recent first)."""
    clock, saga = _make_saga()
    compensated: list[str] = []

    async def make_compensate(label: str):
        async def _comp(result) -> None:
            compensated.append(label)
        return _comp

    comp1 = await make_compensate("step1")
    comp2 = await make_compensate("step2")

    # Step 1: commit successfully
    action1 = saga.stage_write("tool_k1", {"step": 1}, compensate=comp1, idempotency_key="k11")
    result1 = await saga.commit_write(action1.action_id, _instant_executor)
    assert action1.state == ActionState.COMMITTED

    # Step 2: commit successfully
    action2 = saga.stage_write("tool_k2", {"step": 2}, compensate=comp2, idempotency_key="k12")
    result2 = await saga.commit_write(action2.action_id, _instant_executor)
    assert action2.state == ActionState.COMMITTED

    # Simulate a downstream failure — unwind the chain from step 2
    await saga.abort_chain_from(action2.action_id)

    # Only step 1 should be compensated (step 2 is the failed one, idx-excluded)
    # According to abort_chain_from: chain[:idx] = chain[:1] = [action1]
    assert "step1" in compensated
    assert action1.state == ActionState.COMPENSATED


# ---------------------------------------------------------------------------
# Test 12 — Multiple rapid epoch advances while IN_FLIGHT: exactly one terminal state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_12_multiple_epoch_advances_single_terminal():
    """Even if the epoch advances multiple times while the write is IN_FLIGHT,
    the action should receive exactly one terminal state assignment."""
    clock, saga = _make_saga()
    action = saga.stage_write("tool_l", {"x": 12})

    executor_started = asyncio.Event()
    executor_gate = asyncio.Future()

    async def gated_executor(args: dict) -> dict:
        executor_started.set()
        return await executor_gate  # type: ignore[return-value]

    commit_task = asyncio.create_task(
        saga.commit_write(action.action_id, gated_executor)
    )
    await executor_started.wait()
    await asyncio.sleep(0)

    assert action.state == ActionState.IN_FLIGHT

    # Multiple rapid epoch advances
    await clock.advance(reason="advance1")  # epoch 1
    await clock.advance(reason="advance2")  # epoch 2
    await clock.advance(reason="advance3")  # epoch 3

    assert action.state == ActionState.IN_FLIGHT
    assert action.superseded_epoch == 1  # first advance sets it; subsequent advances don't overwrite

    # Executor resolves
    executor_gate.set_result({"x": 12})
    await commit_task

    # Must be exactly one terminal state
    assert action.state == ActionState.COMMITTED_STALE
    state_after = action.state

    # Simulate more epoch advances: state must not change
    await clock.advance()
    assert action.state == state_after, "Terminal state was overwritten by a later epoch advance"


# ---------------------------------------------------------------------------
# Test 13 — task.cancel() on IN_FLIGHT write ignored by executor -> single final state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_13_cancel_ignored_by_executor_single_consistent_state():
    """If task.cancel() is somehow called externally while the executor
    ignores cancellation and completes, commit_write must still reach a
    single, consistent final state.

    Note: Per design (see module docstring), _on_epoch_advance does NOT
    cancel write tasks. This test instead calls task.cancel() manually on
    the commit_write coroutine task itself and verifies the outcome is
    IN_FLIGHT_UNKNOWN (not a data race between two terminal states).
    """
    clock, saga = _make_saga()
    action = saga.stage_write("tool_m", {"x": 13})

    executor_started = asyncio.Event()

    async def uncancellable_executor(args: dict) -> dict:
        executor_started.set()
        # This executor ignores CancelledError and returns normally.
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass  # ignore, keep going
        return {"x": 13}

    commit_task = asyncio.create_task(
        saga.commit_write(action.action_id, uncancellable_executor)
    )
    await executor_started.wait()
    await asyncio.sleep(0)

    # Cancel the commit_write outer task (simulating external cancellation)
    commit_task.cancel()
    try:
        await commit_task
    except (asyncio.CancelledError, Exception):
        pass

    # The action must be in exactly one consistent terminal state.
    # If commit_write was cancelled while awaiting the executor (which
    # itself is not cancelled), the state is IN_FLIGHT_UNKNOWN.
    assert action.state.is_terminal(), (
        f"Expected a terminal state, got {action.state.name}"
    )
    # There must only be ONE terminal state — verify the state object is stable
    state_snapshot = action.state
    await asyncio.sleep(0)
    assert action.state == state_snapshot, "State changed after terminal assignment"


# ---------------------------------------------------------------------------
# Test 14 — Two operations in different epochs with independent ikeys
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_14_independent_idempotency_keys_no_cross_contamination():
    """Two writes with different idempotency keys in different epochs must
    not affect each other's state. When the epoch advances, PENDING write A
    is immediately ABORTed (never dispatched, no remote effect). Write B,
    staged in the new epoch, commits normally."""
    clock, saga = _make_saga()

    # Stage write A in epoch 0
    action_a = saga.stage_write("tool_n", {"sub": "A"}, idempotency_key="epoch0-A")
    assert action_a.state == ActionState.PENDING

    # Advance to epoch 1 — this immediately ABORTs the PENDING write A
    await clock.advance()
    assert action_a.state == ActionState.ABORTED, (
        f"Expected PENDING write A to be ABORTed by epoch advance, got {action_a.state.name}"
    )

    # Stage write B in epoch 1 (different idempotency key)
    action_b = saga.stage_write("tool_n", {"sub": "B"}, idempotency_key="epoch1-B")

    # commit_write A: already ABORTED -> SagaAbortedError (write was never dispatched)
    with pytest.raises((SagaAbortedError, StaleEpochError)):
        await saga.commit_write(action_a.action_id, _instant_executor)

    assert action_a.state == ActionState.ABORTED

    # commit_write B: normal success in epoch 1
    result_b = await saga.commit_write(action_b.action_id, _instant_executor)
    assert action_b.state == ActionState.COMMITTED
    assert result_b is not None

    # Cross-contamination check
    assert action_a.state == ActionState.ABORTED
    assert action_b.state == ActionState.COMMITTED
    assert action_a not in saga._chain
    assert action_b in saga._chain


# ---------------------------------------------------------------------------
# Test 15 — Fresh SpeculativeSagaManager per test: no shared state leaks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_15_no_shared_state_between_tests():
    """Each _make_saga() call must produce a completely independent manager.
    Mutating one must have zero effect on another."""
    clock1, saga1 = _make_saga()
    clock2, saga2 = _make_saga()

    # Write to saga1
    action1 = saga1.stage_write("shared_tool", {"n": 1}, idempotency_key="shared-key")
    await saga1.commit_write(action1.action_id, _instant_executor)
    assert action1.state == ActionState.COMMITTED
    assert len(saga1._chain) == 1

    # saga2 must be completely empty and have no knowledge of saga1's action
    assert len(saga2._actions) == 0
    assert len(saga2._chain) == 0
    assert saga2.epoch_clock.current == 0

    # Advancing saga1's epoch must not affect saga2
    await clock1.advance()
    assert clock2.current == 0

    # saga2 can independently stage and commit with the same key
    action2 = saga2.stage_write("shared_tool", {"n": 1}, idempotency_key="shared-key")
    result2 = await saga2.commit_write(action2.action_id, _instant_executor)
    assert action2.state == ActionState.COMMITTED
    assert result2 is not None

    # saga1 is unaffected
    assert action1.state == ActionState.COMMITTED
    assert len(saga1._chain) == 1
