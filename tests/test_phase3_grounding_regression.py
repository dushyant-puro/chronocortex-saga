import pytest
import asyncio
from runtime.grounding_guard import GroundingGuard
from runtime.speculative_saga import TurnEpochClock, SpeculativeSagaManager

def ingest_tokens(guard: GroundingGuard, tokens: list[str]) -> list[int]:
    """Helper to ingest tokens with a dummy clock."""
    return [guard.ingest_token(tok, 0.9, 0.0, 0.0) for tok in tokens]

@pytest.mark.asyncio
async def test_01_candidate_survives_correction_epoch_advance():
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)
    assert clock.current == 0

    ingest_tokens(guard, ["truck", "17", "and", "Chennai"])
    guard.stage_candidate("truck_id", "truck-17", (0, 2))
    guard.stage_candidate("destination", "Chennai", (3, 4))

    # Self correction
    ingest_tokens(guard, ["wait"])
    await asyncio.sleep(0)  # let advance() run

    assert clock.current == 1  # Epoch advanced
    
    # Valid non-tombstoned candidate survives
    assert guard.resolve_current_value("truck_id") == "truck-17"

@pytest.mark.asyncio
async def test_02_tombstoned_candidate_becomes_invalid():
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)
    
    ingest_tokens(guard, ["truck", "17", "and", "Chennai"])
    guard.stage_candidate("truck_id", "truck-17", (0, 2))
    guard.stage_candidate("destination", "Chennai", (3, 4))

    # Self correction
    ingest_tokens(guard, ["wait", "Bengaluru"])
    await asyncio.sleep(0)
    
    guard.stage_candidate("destination", "Bengaluru", (5, 6))
    
    # Chennai tombstoned
    assert guard.resolve_current_value("destination") == "Bengaluru"
    assert "Chennai" not in [x.span.tokens for x in guard._tombstones] # check tombstone content not value

@pytest.mark.asyncio
async def test_03_truck_id_remains_valid_after_destination_correction():
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)
    
    ingest_tokens(guard, ["truck", "17", "and", "Chennai"])
    guard.stage_candidate("truck_id", "truck-17", (0, 2))
    guard.stage_candidate("destination", "Chennai", (3, 4))
    
    ingest_tokens(guard, ["wait", "Bengaluru"])
    await asyncio.sleep(0)
    
    assert guard.resolve_current_value("truck_id") == "truck-17"

@pytest.mark.asyncio
async def test_04_reset_turn_clears_candidates():
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)
    
    ingest_tokens(guard, ["truck", "17"])
    guard.stage_candidate("truck_id", "truck-17", (0, 2))
    assert guard.resolve_current_value("truck_id") == "truck-17"
    
    guard.reset_turn()
    assert guard.resolve_current_value("truck_id") is None
    assert len(guard._staged_candidates) == 0

@pytest.mark.asyncio
async def test_05_candidate_cannot_leak_to_next_turn():
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)
    
    # Turn 1
    ingest_tokens(guard, ["truck", "17"])
    guard.stage_candidate("truck_id", "truck-17", (0, 2))
    
    # Turn ends
    guard.reset_turn()
    
    # For safety, artificially try to poke _staged_candidates to simulate a leak
    # and prove resolve_current_value blocks it because of _turn_id mismatch
    guard._staged_candidates["leaked_field"] = ("value", (0, 1), guard._turn_id - 1)
    
    assert guard.resolve_current_value("leaked_field") is None

@pytest.mark.asyncio
async def test_06_correction_followed_by_new_destination():
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)
    
    ingest_tokens(guard, ["truck", "17", "and", "Chennai"])
    guard.stage_candidate("truck_id", "truck-17", (0, 2))
    guard.stage_candidate("destination", "Chennai", (3, 4))
    
    ingest_tokens(guard, ["wait", "Bengaluru"])
    await asyncio.sleep(0)
    guard.stage_candidate("destination", "Bengaluru", (5, 6))
    
    assert guard.resolve_current_value("truck_id") == "truck-17"
    assert guard.resolve_current_value("destination") == "Bengaluru"

@pytest.mark.asyncio
async def test_07_async_runtime_epoch_invalidates_stale_speculative_work():
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)
    saga = SpeculativeSagaManager(epoch_clock=clock)
    
    ingest_tokens(guard, ["truck", "17", "to", "Chennai"])
    
    # Mock read executor
    async def mock_read(args):
        return args
    
    action = saga.fire_speculative_read(
        tool_name="test_tool",
        args={"truck": 17},
        executor=mock_read,
        entity_hash="hash"
    )
    
    # Trigger correction
    ingest_tokens(guard, ["wait"])
    await asyncio.sleep(0) # Lets the epoch clock advance
    
    assert clock.current == 1
    
    result = saga.get_current_result("hash")
    assert result is None # It was invalidated by epoch advance!

@pytest.mark.asyncio
async def test_08_tombstoned_candidate_never_returned():
    clock = TurnEpochClock()
    guard = GroundingGuard(epoch_clock=clock)
    
    ingest_tokens(guard, ["truck", "17"])
    guard.stage_candidate("truck_id", "truck-17", (0, 2))
    
    # We simulate tombstones overlapping the truck_id manually to test resolve_current_value
    ingest_tokens(guard, ["wait"]) # "wait" alone might tombstone preceding clause
    await asyncio.sleep(0)
    
    assert guard.resolve_current_value("truck_id") is None
