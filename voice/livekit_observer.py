"""Verified LiveKit word-evidence hook; no LLM/tools, approvals or durable tasks."""
from collections import deque
from importlib.metadata import version
import inspect
import logging

from livekit.agents import Agent, AgentSession, StopResponse, stt
from livekit.agents.types import TimedString, NotGiven

from runtime.grounding_guard import TokenEvidence
from runtime.voice_ingress import VoiceIngress, VoiceEvidenceError, bounded_text

logger = logging.getLogger("ccs.voice")
VERIFIED_LIVEKIT_VERSION = "1.8.8"


def _optional(value):
    return None if isinstance(value, NotGiven) else value


class LiveKitEvidenceStream:
    """Consumes the SDK's ordered stream; request IDs are NOT utterance IDs.

    A local generation excludes old pipeline callbacks. Final segments accumulate
    until the SDK completion hook; listening/end-of-speech alone never seals.
    Unsupported/ambiguous metadata stays ungrounded. No receipt clocks are used.
    """
    def __init__(self, ingress):
        self.ingress = ingress
        self.stream = ingress.open_stream()
        self.turn = None
        self._revision = 0
        self._segments = []
        self._last_final = None
        self._max_seen = 0.0
        self._minimum = 0.0
        self._ambiguous = False
        self._seen_completions = deque(maxlen=32)

    @property
    def current(self):
        return self.ingress._open and self.stream == self.ingress._stream

    def _begin(self):
        overlap = self.turn is not None and self._revision > 0
        self.turn = self.ingress.begin_turn(self.stream)
        self._revision, self._segments, self._last_final = 0, [], None
        self._minimum = self._max_seen
        self._ambiguous = overlap

    def _tokens(self, data, text):
        values = data.words
        if values is None or self._ambiguous:
            return tuple(TokenEvidence(word) for word in text.split())
        if type(values) is not list or len(values) > 4096:
            raise VoiceEvidenceError("STT_WORDS_INVALID")
        result = []
        for word in values:
            if not isinstance(word, TimedString):
                raise VoiceEvidenceError("STT_WORD_TYPE_INVALID")
            offset = _optional(word.start_time_offset)
            if offset not in (None, 0):
                # A different media basis requires an explicit provider adapter.
                result.append(TokenEvidence(str(word).strip()))
            else:
                result.append(TokenEvidence(str(word).strip(), _optional(word.confidence),
                                            _optional(word.start_time), _optional(word.end_time)))
        if text.split() != [token.text for token in result]:
            return tuple(TokenEvidence(word) for word in text.split())
        for token in result:
            token.validate()
        return tuple(result)

    def feed(self, event):
        if not self.current:
            return False
        if not isinstance(event, stt.SpeechEvent):
            self.ingress.invalidate("STT_WORD_EVIDENCE_UNAVAILABLE")
            return False
        if event.type == stt.SpeechEventType.START_OF_SPEECH:
            self._begin()
            return True
        if event.type not in (stt.SpeechEventType.INTERIM_TRANSCRIPT, stt.SpeechEventType.FINAL_TRANSCRIPT):
            return False  # END_OF_SPEECH, usage and preflight are not final turns.
        if self.turn is None:
            return False  # Unscoped/late data cannot open a turn implicitly.
        try:
            if not event.alternatives:
                raise VoiceEvidenceError("STT_ALTERNATIVE_UNAVAILABLE")
            data = event.alternatives[0]
            text = bounded_text(data.text)
            tokens = self._tokens(data, text)
            final = event.type == stt.SpeechEventType.FINAL_TRANSCRIPT
            payload = (text, tokens)
            if final and payload == self._last_final:
                return False
            previous = self._minimum
            if self._segments:
                known = [t.end_ts for _, seq in self._segments for t in seq if t.end_ts is not None]
                previous = max([previous, *known])
            for token in tokens:
                if token.start_ts is not None and token.start_ts < previous:
                    raise VoiceEvidenceError("STT_MEDIA_ORDER_INVALID")
                if token.end_ts is not None:
                    previous = token.end_ts
                    self._max_seen = max(self._max_seen, token.end_ts)
            segments = [*self._segments, payload]
            if len(segments) > 64:
                raise VoiceEvidenceError("STT_SEGMENT_CAPACITY")
            self._revision += 1
            self.ingress.replace(self.turn, self._revision, " ".join(part for part, _ in segments),
                                 tuple(t for _, seq in segments for t in seq), final=final)
            if final:
                self._segments.append(payload)
                self._last_final = payload
            return True
        except (ValueError, TypeError, AttributeError):
            self.ingress.invalidate("STT_EVIDENCE_REJECTED")
            return False

    def complete(self, text, message_id):
        if not self.current:
            raise VoiceEvidenceError("STT_STREAM_SUPERSEDED")
        if type(message_id) is not str or not 1 <= len(message_id) <= 256:
            self.ingress.invalidate()
            raise VoiceEvidenceError("STT_COMPLETION_ID_INVALID")
        if message_id in self._seen_completions:
            return None
        self._seen_completions.append(message_id)
        if self.turn is None:
            self._begin()  # Text-only completion: no final acoustic evidence.
        proposal = self.ingress.complete(self.turn, text)
        self.turn, self._segments, self._last_final = None, [], None
        self._revision = 0
        return proposal

    def close(self):
        self.ingress.close_stream(self.stream)


class VoiceObserverAgent(Agent):
    def __init__(self):
        super().__init__(instructions="Observe voice evidence only. Never execute or approve tasks.",
                         tools=[], llm=None, tts=None)
        self.ingress = VoiceIngress()
        self._capture = None

    async def stt_node(self, audio, model_settings):
        capture = LiveKitEvidenceStream(self.ingress)
        self._capture = capture
        try:
            source = Agent.default.stt_node(self, audio, model_settings)
            if inspect.isawaitable(source):
                source = await source
            if source is None:
                self.ingress.invalidate("STT_STREAM_UNAVAILABLE")
                return
            async for event in source:
                if not capture.current:
                    break  # Old pipeline data never reaches the new SDK turn.
                capture.feed(event)
                yield event
        finally:
            capture.close()

    async def on_user_turn_completed(self, turn_ctx, new_message):
        if self._capture is None:
            self._capture = LiveKitEvidenceStream(self.ingress)
        try:
            proposal = self._capture.complete(new_message.text_content or "", new_message.id)
            if proposal is not None:
                logger.info("Voice proposal: %s", proposal.metadata())
        except VoiceEvidenceError:
            logger.warning("Voice completion rejected; evidence unavailable")
        # Do not start a reply/model or accidentally expose text as tool authority.
        raise StopResponse()


async def start_observer(ctx, stt_provider):
    """Trusted host API; caller owns/configures the selected STT and credentials.

    No provider is auto-selected and no environment/model fallback is allowed.
    Tests exercise this with SDK objects and a stub session, not actual audio.
    """
    if version("livekit-agents") != VERIFIED_LIVEKIT_VERSION:
        raise ValueError("The voice hook requires the pinned, verified LiveKit SDK")
    if (not isinstance(stt_provider, stt.STT) or not stt_provider.capabilities.streaming or
            stt_provider.capabilities.aligned_transcript != "word"):
        raise ValueError("Explicit streaming STT with word alignment required")
    agent = VoiceObserverAgent()
    session = AgentSession(stt=stt_provider, llm=None, tts=None, vad=None, turn_detection="stt")
    @session.on("user_state_changed")
    def on_state(event):
        if event.new_state == "speaking":
            session.interrupt()  # Playback only; no task cancellation/epoch.
    await ctx.connect()
    await session.start(agent, room=ctx.room, record=False)
    return agent, session
