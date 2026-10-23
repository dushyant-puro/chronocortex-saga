"""Pinned real SDK types/hooks, SIMULATED evidence; no audio/provider connection."""
import asyncio
from types import SimpleNamespace

import pytest
from livekit.agents import Agent, StopResponse, stt, llm
from livekit.agents.types import TimedString

from voice.livekit_observer import LiveKitEvidenceStream, VoiceObserverAgent, start_observer
from runtime.voice_ingress import VoiceIngress, VoiceEvidenceError


def event(text=None, *, final=False, words=True, offset=0.0, confidence=0.95):
    if text is None:
        return stt.SpeechEvent(type=stt.SpeechEventType.START_OF_SPEECH)
    values = [TimedString(word, start_time=offset + i * .5, end_time=offset + i * .5 + .4,
                          confidence=confidence) for i, word in enumerate(text.split())] if words else None
    return stt.SpeechEvent(type=stt.SpeechEventType.FINAL_TRANSCRIPT if final else stt.SpeechEventType.INTERIM_TRANSCRIPT,
                          alternatives=[stt.SpeechData(language="en", text=text, confidence=1.0, words=values)])


def capture():
    source = LiveKitEvidenceStream(VoiceIngress())
    source.feed(event())
    return source


def test_real_sdk_word_metadata_and_same_length_revision():
    source = capture()
    source.feed(event("Chennai"))
    source.feed(event("Bengaluru", final=True))
    proposal = source.complete("Bengaluru", "message-1")
    assert proposal.text == "Bengaluru" and proposal.grounded
    assert proposal.tokens[0].confidence == .95 and proposal.tokens[0].start_ts == 0


def test_segment_finals_accumulate_but_listening_end_hint_never_completes():
    source = capture()
    source.feed(event("truck-17", final=True))
    source.feed(stt.SpeechEvent(type=stt.SpeechEventType.END_OF_SPEECH))
    assert source.ingress.latest is None
    source.feed(event("and Chennai", final=True, offset=.5))
    assert source.complete("truck-17 and Chennai", "message-1").grounded


@pytest.mark.parametrize("metadata", ["absent", "sentinel", "mismatch", "offset", "utterance-only"])
def test_unsupported_metadata_never_borrows_utterance_confidence(metadata):
    source = capture()
    value = event("Chennai", final=True)
    data = value.alternatives[0]
    if metadata in ("absent", "utterance-only"):
        data.words = None
    elif metadata == "sentinel":
        data.words = [TimedString("Chennai")]
    elif metadata == "mismatch":
        data.words = [TimedString("Bengaluru", start_time=0., end_time=.4, confidence=.99)]
    else:
        data.words[0].start_time_offset = 1000.
    source.feed(value)
    proposal = source.complete("Chennai", "message-1")
    assert not proposal.grounded and proposal.tokens[0].confidence is None


def test_invalid_current_event_latches_until_new_turn():
    source = capture()
    source.feed(event("Chennai", final=True))
    invalid = event("Bengaluru", final=True, offset=.5, confidence=float("nan"))
    assert source.feed(invalid) is False
    assert not source.complete("Chennai", "message-1").grounded


def test_old_pipeline_finalizer_and_transcript_cannot_clear_new_stream():
    old = capture()
    current = LiveKitEvidenceStream(old.ingress)
    current.feed(event())
    old.feed(event("Chennai", final=True))
    old.close()
    current.feed(event("Bengaluru", final=True))
    assert current.complete("Bengaluru", "message-1").grounded
    with pytest.raises(VoiceEvidenceError):
        old.complete("Chennai", "old-message")


def test_old_media_times_and_ambiguous_overlap_cannot_attach_to_new_turn():
    source = capture()
    source.feed(event("Chennai", final=True))
    assert source.complete("Chennai", "message-1").grounded
    source.feed(event())
    source.feed(event("Chennai", final=True))  # Old audio times after new START.
    assert not source.complete("Chennai", "message-2").grounded
    source.feed(event())
    source.feed(event("Bengaluru", offset=1.0))
    source.feed(event())  # Cannot bind a new utterance while old one is open.
    source.feed(event("Chennai", final=True, offset=2.0))
    assert not source.complete("Chennai", "message-3").grounded


def test_duplicate_final_and_completion_do_not_append_or_publish_twice():
    source = capture()
    value = event("Chennai", final=True)
    source.feed(value)
    assert source.feed(value) is False
    assert source.complete("Chennai", "message-1").grounded
    assert source.complete("Chennai", "message-1") is None
    assert len(source.ingress.latest.tokens) == 1


@pytest.mark.asyncio
async def test_actual_stt_hook_yields_sdk_events_and_completion_suppresses_reply(monkeypatch, caplog):
    entered, release = asyncio.Event(), asyncio.Event()
    values = [event(), event("PRIVATE-Chennai", final=True)]
    async def sdk_stream(*_):
        for value in values:
            yield value
        entered.set()
        await release.wait()
    monkeypatch.setattr(Agent.default, "stt_node", sdk_stream)
    observer = VoiceObserverAgent()
    delivered = []
    async def consume():
        async for value in observer.stt_node(None, None):
            delivered.append(value)
    operation = asyncio.create_task(consume())
    await asyncio.wait_for(entered.wait(), 3)
    with caplog.at_level("INFO", logger="ccs.voice"):
        with pytest.raises(StopResponse):
            await observer.on_user_turn_completed(llm.ChatContext(), llm.ChatMessage(role="user", content=["PRIVATE-Chennai"]))
    assert observer.ingress.latest.grounded and delivered == values
    assert "PRIVATE-Chennai" not in caplog.text
    assert observer.tools == []
    release.set()
    await asyncio.wait_for(operation, 3)
    assert observer.ingress.latest is None  # Ephemeral data drops on stream close.


@pytest.mark.asyncio
async def test_transcript_only_completion_remains_unscored_and_never_responds():
    observer = VoiceObserverAgent()
    with pytest.raises(StopResponse):
        await observer.on_user_turn_completed(llm.ChatContext(), llm.ChatMessage(role="user", content=["yes approve payment"]))
    assert not observer.ingress.latest.grounded
    assert observer.ingress.latest.metadata()["authorization"] == "NONE"


@pytest.mark.asyncio
async def test_unconfigured_nonstreaming_host_fails_before_connection():
    class Context:
        async def connect(self):
            pytest.fail("Unconfigured voice host connected")
    with pytest.raises(ValueError):
        await start_observer(Context(), None)


@pytest.mark.asyncio
async def test_legacy_entrypoint_and_mutation_helpers_are_closed_outside_simulation():
    from agent import CCSAgent, entrypoint
    from runtime.policy_engine import PolicyError
    with pytest.raises(RuntimeError, match="retired"):
        await entrypoint(SimpleNamespace(connect=lambda: pytest.fail("retired host connected")))
    legacy = CCSAgent()
    with pytest.raises(PolicyError):
        legacy.on_interim_transcript("Chennai", True, 1.0, 1, 2)
    with pytest.raises(PolicyError):
        await legacy._confirm_turn_boundary()
    with pytest.raises(PolicyError):
        await legacy._commit_all_pending_slots()
    with pytest.raises(PolicyError):
        legacy.on_barge_in()


@pytest.mark.asyncio
async def test_explicit_word_stt_host_uses_real_sdk_session_without_models_tools_or_recording(monkeypatch):
    from livekit.agents import AgentSession, UserStateChangedEvent
    class WordSTT(stt.STT):
        def __init__(self):
            super().__init__(capabilities=stt.STTCapabilities(streaming=True, interim_results=True, aligned_transcript="word"))
        async def _recognize_impl(self, *args, **kwargs):
            pytest.fail("Offline host test invoked STT inference")
    class Session(AgentSession):
        async def start(self, agent, *, room, record):
            self.captured = (agent, room, record)
        def interrupt(self):
            self.interrupted = True
    class Context:
        room = object()
        connected = False
        async def connect(self):
            self.connected = True
    monkeypatch.setattr("voice.livekit_observer.AgentSession", Session)
    context = Context()
    observer, session = await start_observer(context, WordSTT())
    assert context.connected and session.captured == (observer, context.room, False)
    assert observer.tools == []
    session.emit("user_state_changed", UserStateChangedEvent(old_state="listening", new_state="speaking"))
    assert session.interrupted is True and observer.ingress.latest is None
