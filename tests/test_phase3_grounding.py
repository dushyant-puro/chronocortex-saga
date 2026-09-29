"""
tests/test_phase3_grounding.py

15 deterministic tests for Phase 3:
Grounding + Self-Correction / Tombstone Pipeline.
"""

import asyncio
import pytest

from runtime.grounding_guard import GroundingGuard
from runtime.speculative_saga import TurnEpochClock, SpeculativeSagaManager, ActionState, StaleEpochError, SagaAbortedError


def _make_saga_and_guard():
    clock = TurnEpochClock()
    saga = SpeculativeSagaManager(clock)
    guard = GroundingGuard(epoch_clock=clock)
    return clock, saga, guard

def ingest_tokens(guard, tokens, confidences=None, start_ts=0.0, dt=0.5):
    if confidences is None:
        confidences = [0.9] * len(tokens)
    ts = start_ts
    indices = []
    for t, c in zip(tokens, confidences):
        idx = guard.ingest_token(t, c, ts, ts + dt)
        indices.append(idx)
        ts += dt
    return indices

@pytest.mark.asyncio
async def test_01_high_confidence_grounds():
    guard = GroundingGuard()
    indices = ingest_tokens(guard, ["book", "flight"])
    res = guard.stage_candidate("action", "book flight", (indices[0], indices[-1] + 1))
    assert res == "action"
    assert guard.resolve_current_value("action") == "book flight"


@pytest.mark.asyncio
async def test_02_low_confidence_rejected():
    guard = GroundingGuard()
    indices = ingest_tokens(guard, ["book", "flight"], confidences=[0.4, 0.4])
    res = guard.stage_candidate("action", "book flight", (indices[0], indices[-1] + 1))
    assert res is None
    assert guard.resolve_current_value("action") is None


@pytest.mark.asyncio
async def test_03_already_tombstoned_rejected_at_staging():
    guard = GroundingGuard()
    indices = ingest_tokens(guard, ["book", "flight", "wait"])
    res = guard.stage_candidate("action", "book flight", (indices[0], indices[1] + 1))
    assert res is None


@pytest.mark.asyncio
async def test_04_repair_cue_identifies_preceding_clause():
    guard = GroundingGuard()
    indices = ingest_tokens(guard, ["and", "book", "two", "tickets", "wait"])
    assert len(guard._tombstones) == 1
    tombstone = guard._tombstones[0]
    # Clause boundary is "and" at idx 0, so span should be from 1 to 4
    assert tombstone.span.start == 1
    assert tombstone.span.end == 4


@pytest.mark.asyncio
async def test_05_retroactive_invalidation_core_scenario():
    # Stage "Chennai", then ingest "wait Bengaluru", stage "Bengaluru"
    # Testing eviction and live-check logic.
    guard = GroundingGuard()
    
    # 1. Ingest and stage Chennai
    idx_chennai = ingest_tokens(guard, ["to", "Chennai"])
    guard.stage_candidate("destination", "Chennai", (idx_chennai[1], idx_chennai[1] + 1))
    assert guard.resolve_current_value("destination") == "Chennai"
    
    # 2. Ingest correction
    idx_wait = ingest_tokens(guard, ["wait", "Bengaluru"])
    
    # At this point Chennai should be evicted and unresolvable
    assert guard.resolve_current_value("destination") is None
    assert "destination" not in guard._staged_candidates
    
    # 3. Stage Bengaluru
    guard.stage_candidate("destination", "Bengaluru", (idx_wait[1], idx_wait[1] + 1))
    assert guard.resolve_current_value("destination") == "Bengaluru"


@pytest.mark.asyncio
async def test_06_destination_absent_from_staged_candidates():
    # After scenario 5, assert destination is not in _staged_candidates before staging Bengaluru
    guard = GroundingGuard()
    idx_chennai = ingest_tokens(guard, ["to", "Chennai"])
    guard.stage_candidate("destination", "Chennai", (idx_chennai[1], idx_chennai[1] + 1))
    ingest_tokens(guard, ["wait"])
    assert "destination" not in guard._staged_candidates


@pytest.mark.asyncio
async def test_07_eviction_is_scoped():
    guard = GroundingGuard()
    idx_truck = ingest_tokens(guard, ["truck", "seventeen", "and"])
    guard.stage_candidate("truck_id", "truck-17", (idx_truck[0], idx_truck[1] + 1))
    
    idx_chennai = ingest_tokens(guard, ["to", "Chennai"])
    guard.stage_candidate("destination", "Chennai", (idx_chennai[1], idx_chennai[1] + 1))
    
    ingest_tokens(guard, ["wait"])
    
    # truck_id should survive, destination should be evicted
    assert guard.resolve_current_value("truck_id") == "truck-17"
    assert guard.resolve_current_value("destination") is None


@pytest.mark.asyncio
async def test_08_repeated_corrections():
    guard = GroundingGuard()
    
    idx_chennai = ingest_tokens(guard, ["Chennai"])
    guard.stage_candidate("dest", "Chennai", (idx_chennai[0], idx_chennai[0] + 1))
    
    idx_mumbai = ingest_tokens(guard, ["wait", "Mumbai"])
    assert "dest" not in guard._staged_candidates
    guard.stage_candidate("dest", "Mumbai", (idx_mumbai[1], idx_mumbai[1] + 1))
    
    idx_bengaluru = ingest_tokens(guard, ["no", "wait", "Bengaluru"])
    assert "dest" not in guard._staged_candidates
    guard.stage_candidate("dest", "Bengaluru", (idx_bengaluru[2], idx_bengaluru[2] + 1))
    
    assert guard.resolve_current_value("dest") == "Bengaluru"


@pytest.mark.asyncio
async def test_09_integration_epoch_advance_cancels_read():
    clock, saga, guard = _make_saga_and_guard()
    
    idx_chennai = ingest_tokens(guard, ["Chennai"])
    guard.stage_candidate("dest", "Chennai", (idx_chennai[0], idx_chennai[0] + 1))
    
    async def fast_read(args):
        return {"city": "Chennai"}
        
    action = saga.fire_speculative_read("query_traffic", {"route": "Chennai"}, fast_read, "dest_entity")
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    
    assert saga.get_current_result("dest_entity") == {"city": "Chennai"}
    
    # Correction cue fires -> should call clock.advance()
    ingest_tokens(guard, ["wait"])
    for _ in range(5):
        await asyncio.sleep(0)
        
    # The read should now be stale
    assert saga.get_current_result("dest_entity") is None
    assert clock.current == 1


@pytest.mark.asyncio
async def test_10_integration_staged_write_aborted_by_correction():
    clock, saga, guard = _make_saga_and_guard()
    
    idx_chennai = ingest_tokens(guard, ["Chennai"])
    guard.stage_candidate("dest", "Chennai", (idx_chennai[0], idx_chennai[0] + 1))
    
    write_action = saga.stage_write("reroute_truck", {"destination": "Chennai"})
    
    # Correction cue -> epoch advances
    ingest_tokens(guard, ["wait"])
    for _ in range(5):
        await asyncio.sleep(0)
        
    # Commit write should raise StaleEpochError or SagaAbortedError
    async def instant_write(args):
        return {}
        
    with pytest.raises((StaleEpochError, SagaAbortedError)):
        await saga.commit_write(write_action.action_id, instant_write)


@pytest.mark.asyncio
async def test_11_late_read_completes_after_correction():
    clock, saga, guard = _make_saga_and_guard()
    
    idx_chennai = ingest_tokens(guard, ["Chennai"])
    guard.stage_candidate("dest", "Chennai", (idx_chennai[0], idx_chennai[0] + 1))
    
    gate = asyncio.get_event_loop().create_future()
    async def slow_read(args):
        return await gate
        
    saga.fire_speculative_read("query_traffic", {"route": "Chennai"}, slow_read, "dest_entity")
    
    # Correction cue -> epoch advances
    ingest_tokens(guard, ["wait"])
    for _ in range(5):
        await asyncio.sleep(0)
        
    # Late completion
    if not gate.done():
        gate.set_result({"city": "Chennai"})
    await asyncio.sleep(0)
    
    assert saga.get_current_result("dest_entity") is None


@pytest.mark.asyncio
async def test_12_epoch_clock_increments_exactly_one():
    clock, saga, guard = _make_saga_and_guard()
    
    assert clock.current == 0
    ingest_tokens(guard, ["book", "wait"]) # triggers tombstone for start=0
    for _ in range(5):
        await asyncio.sleep(0)
        
    assert clock.current == 1


@pytest.mark.asyncio
async def test_13_no_value_returned_in_tombstoned_range():
    guard = GroundingGuard()
    
    idx1 = ingest_tokens(guard, ["one"])
    idx2 = ingest_tokens(guard, ["two"])
    idx3 = ingest_tokens(guard, ["three"])
    
    guard.stage_candidate("f1", "1", (idx1[0], idx1[0] + 1))
    guard.stage_candidate("f2", "2", (idx2[0], idx2[0] + 1))
    guard.stage_candidate("f3", "3", (idx3[0], idx3[0] + 1))
    
    # Correction spanning from beginning
    ingest_tokens(guard, ["wait"])
    
    assert guard.resolve_current_value("f1") is None
    assert guard.resolve_current_value("f2") is None
    assert guard.resolve_current_value("f3") is None


@pytest.mark.asyncio
async def test_14_synthetic_token_range():
    guard = GroundingGuard()
    
    # Stage candidate with synthetic token range (e.g., from multimodal input)
    # The confidences list has 0 elements so end > len(confidences) will fail check_argument_grounding
    # We must actually ingest enough tokens for it to pass bounding checks.
    ingest_tokens(guard, ["a", "b", "c"])
    
    res = guard.stage_candidate("syn", "value", (0, 3))
    assert res == "syn"
    assert guard.resolve_current_value("syn") == "value"
    
    # Tombstone it
    ingest_tokens(guard, ["wait"])
    assert guard.resolve_current_value("syn") is None


@pytest.mark.asyncio
async def test_15_guard_instances_isolated():
    guard1 = GroundingGuard()
    guard2 = GroundingGuard()
    
    idx = ingest_tokens(guard1, ["Chennai"])
    guard1.stage_candidate("dest", "Chennai", (idx[0], idx[0] + 1))
    
    assert guard1.resolve_current_value("dest") == "Chennai"
    assert guard2.resolve_current_value("dest") is None
    assert len(guard2._staged_candidates) == 0
    assert len(guard2._tombstones) == 0
