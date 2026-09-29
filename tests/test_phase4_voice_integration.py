import pytest
import asyncio
from runtime.speculative_saga import ActionState
from agent import CCSAgent, PendingSlot

class MockUserInputTranscribedEvent:
    def __init__(self, transcript: str, is_final: bool = False):
        self.type = "user_input_transcribed"
        self.transcript = transcript
        self.is_final = is_final
        self.item_id = "test-item"
        self.speaker_id = "user-1"
        self.language = "en"
        self.created_at = 1234567890

class MockUserStateChangedEvent:
    def __init__(self, new_state: str):
        self.type = "user_state_changed"
        self.new_state = new_state

class MockSession:
    def __init__(self):
        self.handlers = {}
        self.interrupted = False

    def on(self, event_name, handler):
        self.handlers[event_name] = handler
        # Support both decorator and normal call
        return handler

    def emit(self, event_name, event_data):
        if event_name in self.handlers:
            self.handlers[event_name](event_data)

    def interrupt(self):
        self.interrupted = True

    async def start(self, room):
        pass

@pytest.fixture
def ccs_setup(monkeypatch):
    # Mock network delay to 0
    async def fast_network(*args, **kwargs):
        pass
    monkeypatch.setattr("tools.fleet_tools._simulated_network", fast_network)
    
    ccs = CCSAgent()
    session = MockSession()
    ccs.session = session
    
    # Mirror the entrypoint event attachments manually for the test
    def on_transcribed(ev):
        ccs.on_interim_transcript(ev.transcript, getattr(ev, "is_final", False), 1.0, 0.0, 0.0)
    session.on("user_input_transcribed", on_transcribed)
    
    def on_state(ev):
        if ev.new_state == "speaking":
            ccs.on_barge_in()
        elif ev.new_state == "listening":
            asyncio.create_task(ccs._confirm_turn_boundary())
    session.on("user_state_changed", on_state)
    
    return ccs, session

@pytest.mark.asyncio
async def test_01_agent_construction(ccs_setup):
    ccs, session = ccs_setup
    assert ccs.epoch_clock is not None
    assert ccs.saga is not None

@pytest.mark.asyncio
async def test_02_interim_transcript_handler(ccs_setup):
    ccs, session = ccs_setup
    ev = MockUserInputTranscribedEvent("truck-17")
    session.emit("user_input_transcribed", ev)
    
    assert "truck_id" in ccs._pending_slots
    assert ccs._pending_slots["truck_id"].raw_value == "truck-17"

@pytest.mark.asyncio
async def test_03_final_transcript_planning(ccs_setup):
    ccs, session = ccs_setup
    ev = MockUserInputTranscribedEvent("truck-17", is_final=True)
    session.emit("user_input_transcribed", ev)
    
    # Simulate turn boundary
    ev_state = MockUserStateChangedEvent("listening")
    session.emit("user_state_changed", ev_state)
    await asyncio.sleep(0) # Let tasks run
    
    # Should have committed and cleared
    assert len(ccs._pending_slots) == 0

@pytest.mark.asyncio
async def test_04_correction_advances_epoch(ccs_setup):
    ccs, session = ccs_setup
    session.emit("user_input_transcribed", MockUserInputTranscribedEvent("truck-17"))
    
    # Correction word "wait"
    session.emit("user_input_transcribed", MockUserInputTranscribedEvent("truck-17 wait"))
    await asyncio.sleep(0)
    
    assert ccs.epoch_clock.current == 1

@pytest.mark.asyncio
async def test_05_barge_in_invokes_interrupt(ccs_setup):
    ccs, session = ccs_setup
    
    session.emit("user_state_changed", MockUserStateChangedEvent("speaking"))
    await asyncio.sleep(0)
    
    assert session.interrupted is True
    assert ccs.epoch_clock.current == 1

@pytest.mark.asyncio
async def test_06_stale_speculative_read(ccs_setup):
    ccs, session = ccs_setup
    session.emit("user_input_transcribed", MockUserInputTranscribedEvent("truck-17"))
    
    # Barge in (advances epoch)
    session.emit("user_state_changed", MockUserStateChangedEvent("speaking"))
    await asyncio.sleep(0)
    
    # Check current result (should be invalidated)
    res = ccs.saga.get_current_result("truck_id:truck-17")
    assert res is None

@pytest.mark.asyncio
async def test_07_interim_no_mutation(ccs_setup):
    ccs, session = ccs_setup
    session.emit("user_input_transcribed", MockUserInputTranscribedEvent("truck-17 Chennai"))
    await asyncio.sleep(0)
    
    actions = list(ccs.saga._actions.values())
    write_actions = [a for a in actions if a.kind.name == "STAGED_WRITE"]
    assert len(write_actions) == 0

@pytest.mark.asyncio
async def test_08_full_simulated_scenario(ccs_setup):
    ccs, session = ccs_setup
    
    session.emit("user_input_transcribed", MockUserInputTranscribedEvent("truck-17"))
    session.emit("user_input_transcribed", MockUserInputTranscribedEvent("truck-17 and Chennai"))
    
    # Correction
    session.emit("user_input_transcribed", MockUserInputTranscribedEvent("truck-17 and Chennai wait Bengaluru"))
    await asyncio.sleep(0)
    
    # End turn
    session.emit("user_state_changed", MockUserStateChangedEvent("listening"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    
    actions = list(ccs.saga._actions.values())
    write_actions = [a for a in actions if a.kind.name == "STAGED_WRITE"]
    assert len(write_actions) == 1
    
    assert write_actions[0].args["destination"] == "Bengaluru"
    assert write_actions[0].state == ActionState.COMMITTED

@pytest.mark.asyncio
async def test_09_duplicate_idempotency_protected(ccs_setup):
    ccs, session = ccs_setup
    # Inject a committed action with a specific key
    session.emit("user_input_transcribed", MockUserInputTranscribedEvent("truck-17 and Chennai"))
    session.emit("user_state_changed", MockUserStateChangedEvent("listening"))
    await asyncio.sleep(0)
    
    actions = list(ccs.saga._actions.values())
    write_actions = [a for a in actions if a.kind.name == "STAGED_WRITE"]
    assert len(write_actions) == 1
    assert write_actions[0].state == ActionState.COMMITTED
