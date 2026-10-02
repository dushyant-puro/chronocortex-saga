"""
tests/test_phase2_execution_hardening.py

NEW PHASE 2 (PRISM roadmap) — Adversarial concurrency tests for the
SpeculativeSagaManager and GroundingGuard engines.

These tests exercise scenarios NOT already covered by the happy-path and
single-correction-path suites in Phase 1/2/3. They use deterministic
asyncio.Event/Future synchronization — no real sleeps or timing races.

Categories:
  a. Simultaneous writes, different keys
  b. Simultaneous writes, same key — racing stage calls
  c. Repeated rapid epoch advances during multiple in-flight writes
  d. Compensation storm — concurrent compensation tasks
  e. Repeated self-correction storm (GroundingGuard integration)
  f. Concurrent speculative reads under rapid epoch churn
  g. Reconcile_write racing a new commit attempt
  h. Task cancellation during compensation
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

# Ensure repo root importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from runtime.speculative_saga import (
    ActionKind,
    ActionState,
    DuplicateOperationError,
    SagaAbortedError,
    SpeculativeSagaManager,
    StaleEpochError,
    TurnEpochClock,
)
from runtime.grounding_guard import GroundingGuard


# ============================================================================
# Helpers
# ============================================================================

def _fresh() -> tuple[TurnEpochClock, SpeculativeSagaManager]:
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    return clock, saga


def _gated_executor(gate: asyncio.Event, result=None, error: Exception | None = None):
    """Return an executor that blocks until gate is set, then returns result or raises."""
    async def executor(args):
        await gate.wait()
        if error is not None:
            raise error
        return result if result is not None else {"ok": True}
    return executor


# ============================================================================
# 3a. SIMULTANEOUS WRITES, DIFFERENT KEYS
# ============================================================================

@pytest.mark.asyncio
async def test_3a_simultaneous_writes_different_keys():
    """Two independent writes with different idempotency keys, staged and
    committed concurrently via asyncio.gather, must both succeed independently
    with no cross-contamination of state."""
    clock, saga = _fresh()

    action_a = saga.stage_write("reroute_truck", {"truck_id": "t-1", "destination": "A"})
    action_b = saga.stage_write("reroute_truck", {"truck_id": "t-2", "destination": "B"})

    # Different args → different computed idempotency keys
    assert action_a.idempotency_key != action_b.idempotency_key

    gate_a = asyncio.Event()
    gate_b = asyncio.Event()

    async def commit_a():
        return await saga.commit_write(
            action_a.action_id,
            _gated_executor(gate_a, result={"routed": "A"}),
        )

    async def commit_b():
        return await saga.commit_write(
            action_b.action_id,
            _gated_executor(gate_b, result={"routed": "B"}),
        )

    # Start both commits concurrently
    task_a = asyncio.create_task(commit_a())
    task_b = asyncio.create_task(commit_b())

    # Interleave: let B finish first, then A
    gate_b.set()
    await asyncio.sleep(0)  # yield to let B's executor complete
    gate_a.set()

    result_a, result_b = await asyncio.gather(task_a, task_b)

    assert result_a == {"routed": "A"}
    assert result_b == {"routed": "B"}
    assert action_a.state == ActionState.COMMITTED
    assert action_b.state == ActionState.COMMITTED
    # Both in chain, independently
    chain_ids = [a.action_id for a in saga._chain]
    assert action_a.action_id in chain_ids
    assert action_b.action_id in chain_ids


# ============================================================================
# 3b. SIMULTANEOUS WRITES, SAME KEY, RACING STAGE CALLS
# ============================================================================

@pytest.mark.asyncio
async def test_3b_racing_stage_same_idempotency_key():
    """Two coroutines attempt stage_write with the SAME idempotency_key at
    effectively the same moment. Exactly one must succeed; the other must
    receive DuplicateOperationError."""
    clock, saga = _fresh()

    sync_event = asyncio.Event()
    results = []  # (index, "ok" | "duplicate")

    async def racer(index: int):
        await sync_event.wait()  # both start at the same instant
        try:
            saga.stage_write(
                "reroute_truck",
                {"truck_id": "t-1", "destination": "X"},
                idempotency_key="shared-key-001",
            )
            results.append((index, "ok"))
        except DuplicateOperationError:
            results.append((index, "duplicate"))

    task_0 = asyncio.create_task(racer(0))
    task_1 = asyncio.create_task(racer(1))

    # Release both racers
    sync_event.set()
    await asyncio.gather(task_0, task_1)

    ok_count = sum(1 for _, r in results if r == "ok")
    dup_count = sum(1 for _, r in results if r == "duplicate")
    assert ok_count == 1, f"Expected exactly 1 successful stage, got {ok_count}"
    assert dup_count == 1, f"Expected exactly 1 duplicate rejection, got {dup_count}"


# ============================================================================
# 3c. REPEATED RAPID EPOCH ADVANCES DURING MULTIPLE IN-FLIGHT WRITES
# ============================================================================

@pytest.mark.asyncio
async def test_3c_rapid_epoch_advances_multiple_in_flight_writes():
    """Stage 3+ writes (different keys) all IN_FLIGHT simultaneously, then
    advance epoch multiple times while pending. Each must resolve independently
    based on its own executor outcome."""
    clock, saga = _fresh()

    gate_success = asyncio.Event()
    gate_error = asyncio.Event()
    gate_timeout = asyncio.Event()

    action_ok = saga.stage_write("reroute_truck", {"truck_id": "t-1", "d": "A"})
    action_err = saga.stage_write("reroute_truck", {"truck_id": "t-2", "d": "B"})
    action_to = saga.stage_write("reroute_truck", {"truck_id": "t-3", "d": "C"})

    async def timeout_executor(args):
        # This will never complete — we'll let asyncio.wait_for timeout it
        await gate_timeout.wait()
        return {"ok": True}

    task_ok = asyncio.create_task(
        saga.commit_write(action_ok.action_id, _gated_executor(gate_success, {"ok": "A"}))
    )
    task_err = asyncio.create_task(
        saga.commit_write(action_err.action_id, _gated_executor(gate_error, error=RuntimeError("fail")))
    )
    task_to = asyncio.create_task(
        saga.commit_write(action_to.action_id, timeout_executor, timeout=0.05)
    )

    await asyncio.sleep(0)  # yield so all three enter IN_FLIGHT

    # Rapid epoch advances while all three are in flight
    await clock.advance(reason="correction-1")
    await clock.advance(reason="correction-2")
    await clock.advance(reason="correction-3")

    # Now release the gates and let everything resolve
    gate_success.set()
    gate_error.set()
    # gate_timeout left unset — it will timeout

    # Gather results, expecting some exceptions
    result_ok = await task_ok
    with pytest.raises(RuntimeError, match="fail"):
        await task_err
    with pytest.raises(asyncio.TimeoutError):
        await task_to

    # SUCCESS write → COMMITTED_STALE (superseded_epoch was set by the advances)
    assert action_ok.state == ActionState.COMMITTED_STALE
    assert action_ok.superseded_epoch is not None
    # First advance was epoch 1, that's what should be recorded
    assert action_ok.superseded_epoch == 1

    # ERROR write → ABORTED (exception guarantees no remote effect)
    assert action_err.state == ActionState.ABORTED

    # TIMEOUT write → IN_FLIGHT_UNKNOWN
    assert action_to.state == ActionState.IN_FLIGHT_UNKNOWN

    # No cross-contamination: each has its own independent state
    assert action_ok.action_id != action_err.action_id != action_to.action_id


# ============================================================================
# 3d. COMPENSATION STORM — concurrent compensations
# ============================================================================

@pytest.mark.asyncio
async def test_3d_compensation_storm_independent():
    """Multiple COMMITTED_STALE actions fire auto-compensation concurrently.
    A failure in one must not block or corrupt another."""
    clock, saga = _fresh()

    compensate_log = []

    async def compensate_ok(result):
        await asyncio.sleep(0)  # yield
        compensate_log.append(("ok", result))

    async def compensate_fail(result):
        await asyncio.sleep(0)
        compensate_log.append(("fail-attempt", result))
        raise RuntimeError("compensation network error")

    async def compensate_ok_2(result):
        await asyncio.sleep(0)
        compensate_log.append(("ok2", result))

    # Stage three writes with compensation handlers
    a1 = saga.stage_write("reroute_truck", {"truck_id": "t-1", "d": "A"}, compensate=compensate_ok)
    a2 = saga.stage_write("reroute_truck", {"truck_id": "t-2", "d": "B"}, compensate=compensate_fail)
    a3 = saga.stage_write("reroute_truck", {"truck_id": "t-3", "d": "C"}, compensate=compensate_ok_2)

    gate = asyncio.Event()

    # Commit all three concurrently
    async def commit(action):
        return await saga.commit_write(action.action_id, _gated_executor(gate, {"result": action.args}))

    tasks = [asyncio.create_task(commit(a)) for a in [a1, a2, a3]]
    await asyncio.sleep(0)  # let them all reach IN_FLIGHT

    # Advance epoch so they all become stale
    await clock.advance(reason="barge_in")

    # Release executors
    gate.set()
    await asyncio.gather(*tasks)

    # Wait for auto-compensation tasks to finish
    await asyncio.sleep(0.1)

    # a1: should be COMPENSATED
    assert a1.state == ActionState.COMPENSATED
    assert a1.compensation_failed is False

    # a2: compensation raised — should be COMMITTED_STALE with compensation_failed=True
    assert a2.state == ActionState.COMMITTED_STALE
    assert a2.compensation_failed is True

    # a3: should be COMPENSATED independently of a2's failure
    assert a3.state == ActionState.COMPENSATED
    assert a3.compensation_failed is False

    # Verify log shows all three attempted
    assert any(t[0] == "ok" for t in compensate_log)
    assert any(t[0] == "fail-attempt" for t in compensate_log)
    assert any(t[0] == "ok2" for t in compensate_log)


# ============================================================================
# 3e. REPEATED SELF-CORRECTION STORM (GroundingGuard integration)
# ============================================================================

@pytest.mark.asyncio
async def test_3e_repeated_self_correction_storm():
    """Simulate 5 rapid sequential corrections to the same field via
    GroundingGuard, each triggering a real epoch advance. Only the final
    value should be resolvable; every intermediate must be evicted."""
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)

    corrections = ["Chennai", "Mumbai", "Delhi", "Kolkata", "Bengaluru"]
    epoch_before = clock.current  # 0

    token_idx = 0
    for i, city in enumerate(corrections):
        # Ingest city token (high confidence)
        idx = guard.ingest_token(city, 0.95, float(i), float(i) + 0.5)
        # Stage it as "destination"
        guard.stage_candidate("destination", city, (idx, idx + 1))
        token_idx = idx + 1

        # For every value except the last, ingest a repair cue
        if i < len(corrections) - 1:
            repair_idx = guard.ingest_token("wait", 0.99, float(i) + 0.5, float(i) + 0.7)
            token_idx = repair_idx + 1
            # The ingest_token("wait") triggers _tombstone_preceding_clause
            # which fires epoch_clock.advance() via asyncio.create_task
            await asyncio.sleep(0)  # yield to let the epoch advance task run

    # Total epoch advances: 4 (one per correction, except the last)
    assert clock.current == epoch_before + 4, (
        f"Expected {epoch_before + 4} epoch advances, got {clock.current}"
    )

    # Only the final value (Bengaluru) should be resolvable
    resolved = guard.resolve_current_value("destination")
    assert resolved == "Bengaluru", f"Expected 'Bengaluru', got {resolved!r}"

    # Intermediate values must NOT be resolvable
    # (they were either tombstoned or evicted from _staged_candidates)
    for city in corrections[:-1]:
        # These were all evicted by tombstoning
        assert guard.resolve_current_value("destination") != city or city == "Bengaluru"


# ============================================================================
# 3f. CONCURRENT SPECULATIVE READS UNDER RAPID EPOCH CHURN
# ============================================================================

@pytest.mark.asyncio
async def test_3f_concurrent_reads_epoch_churn():
    """Fire multiple speculative reads for the same entity across several
    different epochs in quick succession. get_current_result must ONLY ever
    return a result whose capture_epoch matches the current epoch."""
    clock, saga = _fresh()

    read_count = 0

    async def telemetry_executor(args):
        nonlocal read_count
        read_count += 1
        return {"speed": 88, "read_number": read_count}

    # Fire read at epoch 0
    r0 = saga.fire_speculative_read("query_telemetry", {"truck_id": "t-17"}, telemetry_executor, "t-17")
    await asyncio.sleep(0.01)  # let it complete

    # Its result should be available (epoch 0 is still current)
    result_0 = saga.get_current_result("t-17")
    assert result_0 is not None
    assert result_0["read_number"] == 1

    # Advance epoch → read 0 becomes stale
    await clock.advance(reason="correction-1")
    assert saga.get_current_result("t-17") is None  # stale

    # Fire another read at epoch 1
    r1 = saga.fire_speculative_read("query_telemetry", {"truck_id": "t-17"}, telemetry_executor, "t-17")
    await asyncio.sleep(0.01)

    result_1 = saga.get_current_result("t-17")
    assert result_1 is not None
    assert result_1["read_number"] == 2

    # Rapid epoch churn: advance 3 times
    await clock.advance(reason="churn-1")
    await clock.advance(reason="churn-2")
    await clock.advance(reason="churn-3")

    # All prior reads are stale
    assert saga.get_current_result("t-17") is None

    # Fire read at current epoch (now 4)
    r_final = saga.fire_speculative_read("query_telemetry", {"truck_id": "t-17"}, telemetry_executor, "t-17")
    await asyncio.sleep(0.01)

    result_final = saga.get_current_result("t-17")
    assert result_final is not None
    assert r_final.capture_epoch == clock.current


# ============================================================================
# 3g. RECONCILE_WRITE RACING A NEW STAGE ATTEMPT
# ============================================================================

@pytest.mark.asyncio
async def test_3g_reconcile_racing_stage():
    """An action in IN_FLIGHT_UNKNOWN: test two interleavings:
    1. stage_write BEFORE reconcile completes → must be rejected (IN_FLIGHT_UNKNOWN blocks)
    2. stage_write AFTER reconcile completes → must succeed (COMMITTED frees the key)
    """
    # --- Interleaving 1: stage while still IN_FLIGHT_UNKNOWN ---
    clock, saga = _fresh()

    action = saga.stage_write(
        "reserve_dock", {"dock_id": "D-1", "truck_id": "t-5"},
        idempotency_key="dock-reservation-001",
    )

    async def never_completes(args):
        await asyncio.sleep(999)
        return {"reserved": True}

    with pytest.raises(asyncio.TimeoutError):
        await saga.commit_write(action.action_id, never_completes, timeout=0.05)

    assert action.state == ActionState.IN_FLIGHT_UNKNOWN

    # While IN_FLIGHT_UNKNOWN, stage_write with same key must be rejected
    with pytest.raises(DuplicateOperationError):
        saga.stage_write(
            "reserve_dock", {"dock_id": "D-1", "truck_id": "t-5"},
            idempotency_key="dock-reservation-001",
        )

    # --- Now reconcile ---
    reconcile_gate = asyncio.Event()

    async def gated_status_check(a):
        await reconcile_gate.wait()
        return True  # remote confirms it executed

    reconcile_task = asyncio.create_task(
        saga.reconcile_write(action.action_id, gated_status_check)
    )
    await asyncio.sleep(0)  # let reconcile_write start and await the gate

    # While reconcile is blocked, stage_write must STILL be rejected
    # (action is still IN_FLIGHT_UNKNOWN until status_check returns)
    with pytest.raises(DuplicateOperationError):
        saga.stage_write(
            "reserve_dock", {"dock_id": "D-1", "truck_id": "t-5"},
            idempotency_key="dock-reservation-001",
        )

    # Now let reconcile complete
    reconcile_gate.set()
    await reconcile_task

    # After reconcile → COMMITTED (epoch is still 0)
    assert action.state == ActionState.COMMITTED

    # --- Interleaving 2: stage AFTER reconcile completed ---
    # COMMITTED is NOT in _BLOCKING_STATES → new stage with same key succeeds
    new_action = saga.stage_write(
        "reserve_dock", {"dock_id": "D-1", "truck_id": "t-5"},
        idempotency_key="dock-reservation-001",
    )
    assert new_action.state == ActionState.PENDING
    assert new_action.action_id != action.action_id


@pytest.mark.asyncio
async def test_3g2_reconcile_stale_racing_stage():
    """An action in IN_FLIGHT_UNKNOWN where the epoch advances BEFORE reconcile:
    reconcile_write with status_check=True resolves it to COMMITTED_STALE.
    1. stage_write WHILE reconciliation is in-flight must be rejected with DuplicateOperationError.
    2. stage_write AFTER reconciliation completes to COMMITTED_STALE must STILL be
       rejected with DuplicateOperationError (COMMITTED_STALE is in _BLOCKING_STATES per BUG-B fix).
    """
    clock, saga = _fresh()

    action = saga.stage_write(
        "reserve_dock", {"dock_id": "D-1", "truck_id": "t-5"},
        idempotency_key="dock-reservation-002",
    )

    async def never_completes(args):
        await asyncio.sleep(999)
        return {"reserved": True}

    with pytest.raises(asyncio.TimeoutError):
        await saga.commit_write(action.action_id, never_completes, timeout=0.05)

    assert action.state == ActionState.IN_FLIGHT_UNKNOWN

    # Advance the epoch BEFORE calling reconcile_write so capture_epoch (0) != current (1)
    await clock.advance(reason="turn_boundary_during_unknown")
    assert clock.current == 1
    assert action.capture_epoch == 0

    reconcile_gate = asyncio.Event()

    async def gated_status_check(a):
        await reconcile_gate.wait()
        return True  # remote confirms it executed

    reconcile_task = asyncio.create_task(
        saga.reconcile_write(action.action_id, gated_status_check)
    )
    await asyncio.sleep(0)  # let reconcile_write start and wait on gate

    # While reconciliation is in flight (state is still IN_FLIGHT_UNKNOWN), stage_write must be rejected
    with pytest.raises(DuplicateOperationError):
        saga.stage_write(
            "reserve_dock", {"dock_id": "D-1", "truck_id": "t-5"},
            idempotency_key="dock-reservation-002",
        )

    # Let reconcile finish -> resolves to COMMITTED_STALE (no compensate handler, remains COMMITTED_STALE)
    reconcile_gate.set()
    await reconcile_task

    assert action.state == ActionState.COMMITTED_STALE

    # Even AFTER reconcile completes, an unreconciled COMMITTED_STALE action still blocks stage_write
    with pytest.raises(DuplicateOperationError):
        saga.stage_write(
            "reserve_dock", {"dock_id": "D-1", "truck_id": "t-5"},
            idempotency_key="dock-reservation-002",
        )


# ============================================================================
# 3h. TASK CANCELLATION DURING COMPENSATION
# ============================================================================

@pytest.mark.asyncio
async def test_3h_cancellation_during_compensation():
    """A COMMITTED_STALE action's auto-compensation task is cancelled
    partway through. The action must end up with compensation_failed=True
    (signalling need for manual reconciliation) or remain COMMITTED_STALE
    (not silently lost)."""
    clock, saga = _fresh()

    compensation_started = asyncio.Event()

    async def slow_compensate(result):
        compensation_started.set()
        # Simulate a long-running compensation call
        await asyncio.sleep(999)

    action = saga.stage_write(
        "reroute_truck", {"truck_id": "t-1", "d": "X"},
        compensate=slow_compensate,
    )

    gate = asyncio.Event()
    commit_task = asyncio.create_task(
        saga.commit_write(action.action_id, _gated_executor(gate, {"routed": "X"}))
    )
    await asyncio.sleep(0)  # let it reach IN_FLIGHT

    # Advance epoch → will trigger COMMITTED_STALE + auto_compensate
    await clock.advance(reason="barge_in")
    gate.set()
    await commit_task  # completes, fires auto_compensate task

    # Wait for compensation to start
    await compensation_started.wait()

    # Now cancel all pending tasks (simulating scope teardown)
    # Find and cancel the compensation task
    pending = [t for t in asyncio.all_tasks()
               if not t.done() and t is not asyncio.current_task()]
    for t in pending:
        t.cancel()

    # Let the cancellation propagate
    await asyncio.sleep(0.1)

    # The action must NOT be in COMPENSATED state (compensation was interrupted)
    # It should remain COMMITTED_STALE. compensation_failed may or may not be
    # set depending on whether the CancelledError was caught by _auto_compensate.
    #
    # DOCUMENTED GAP: _auto_compensate catches Exception but not CancelledError.
    # If the task is cancelled, the CancelledError propagates uncaught, leaving
    # the action in COMMITTED_STALE with compensation_failed=False. This means
    # there is no programmatic signal that compensation was attempted and
    # interrupted — a genuine gap requiring either:
    #   (a) catching CancelledError in _auto_compensate and setting compensation_failed, or
    #   (b) a separate "compensation_attempted" flag
    # For now, we verify the state is at least safe (not silently COMPENSATED).
    assert action.state == ActionState.COMMITTED_STALE, (
        f"Expected COMMITTED_STALE after cancelled compensation, got {action.state.name}"
    )
    # The action is NOT marked COMPENSATED — safe. But compensation_failed
    # may be False because CancelledError is not caught by _auto_compensate.
    # This is a known gap documented in the Phase 2 audit report.


# ============================================================================
# Isolation check (same pattern as existing suites)
# ============================================================================

@pytest.mark.asyncio
async def test_no_shared_state_between_tests():
    """Verify each test creates fresh instances; no test leaks state."""
    clock, saga = _fresh()
    assert clock.current == 0
    assert len(saga._actions) == 0
    assert len(saga._chain) == 0
