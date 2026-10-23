"""Ephemeral, revision-aware voice proposals with no task or mutation capability."""
from dataclasses import dataclass
import threading
import re

from runtime.grounding_guard import GroundingGuard, TokenEvidence


class VoiceEvidenceError(ValueError):
    """Sanitized protocol error; never include raw transcript or provider payload."""


@dataclass(frozen=True)
class TurnHandle:
    stream: int
    turn: int


@dataclass(frozen=True)
class VoiceProposal:
    handle: TurnHandle
    revision: int
    text: str
    tokens: tuple[TokenEvidence, ...]
    grounded: bool
    reason: str

    def metadata(self):
        # Transcripts/words/identities are deliberately excluded from logs.
        return {"stream": self.handle.stream, "turn": self.handle.turn,
                "revision": self.revision, "token_count": len(self.tokens),
                "grounded": self.grounded, "reason": self.reason,
                "authorization": "NONE", "durable_task_effect": "NONE"}


def bounded_text(text):
    if type(text) is not str or len(text) > 16_384:
        raise VoiceEvidenceError("VOICE_TEXT_INVALID_OR_TOO_LARGE")
    try:
        if len(text.encode("utf-8")) > 16_384:
            raise VoiceEvidenceError("VOICE_TEXT_INVALID_OR_TOO_LARGE")
    except UnicodeError:
        raise VoiceEvidenceError("VOICE_TEXT_INVALID_ENCODING") from None
    words = text.split()
    if len(words) > 4096:
        raise VoiceEvidenceError("VOICE_TEXT_EXCEEDS_WORD_CAPACITY")
    return " ".join(words)


class VoiceIngress:
    """Owning-loop, synchronous input processing; no queue or await races.

    Handles/revisions fence local stale callbacks, not provider truth. They are
    not credentials. A trusted adapter must establish stream/turn association.
    Final acoustic evidence is a proposal, never approval or a durable task.
    """
    def __init__(self):
        self._owner = threading.get_ident()
        self._stream = 0
        self._open = False
        self._turn = 0
        self.handle = None
        self.latest = None
        self._guard = GroundingGuard()  # No task callbacks or shared task epoch.
        self._clear()

    def _clear(self):
        self._revision = 0
        self._payload = None
        self._tokens = ()
        self._text = ""
        self._final = False
        self._closed = False
        self._fault = None
        self._guard.reset_turn()

    def _owned(self):
        if threading.get_ident() != self._owner:
            raise VoiceEvidenceError("VOICE_INPUT_REQUIRES_OWNING_THREAD")

    def open_stream(self):
        self._owned()
        self._stream += 1
        self._open = True
        self._turn = 0
        self.handle = self.latest = None
        self._clear()
        return self._stream

    def close_stream(self, stream):
        self._owned()
        if type(stream) is int and stream == self._stream:
            self._open = False
            self.handle = self.latest = None
            self._clear()
            self._closed = True

    def begin_turn(self, stream):
        self._owned()
        if type(stream) is not int or stream != self._stream or not self._open:
            raise VoiceEvidenceError("VOICE_STREAM_SUPERSEDED")
        self._turn += 1
        self._clear()
        self.handle = TurnHandle(stream, self._turn)
        self.latest = None
        return self.handle

    def invalidate(self, reason="VOICE_INPUT_REJECTED"):
        self._owned()
        self._guard.reset_turn()
        self._tokens, self._text, self._final = (), "", False
        self._fault = (reason if type(reason) is str and len(reason) <= 64 and
                       re.fullmatch(r"[A-Z_]+", reason) else "VOICE_INPUT_REJECTED")
        self.latest = None

    def _current(self, handle):
        self._owned()
        if (type(handle) is not TurnHandle or type(handle.stream) is not int or type(handle.turn) is not int or
                handle != self.handle or handle.stream != self._stream or not self._open):
            raise VoiceEvidenceError("VOICE_TURN_SUPERSEDED")

    def replace(self, handle, revision, text, tokens, *, final):
        self._current(handle)
        if self._closed or self._fault:
            raise VoiceEvidenceError("VOICE_TURN_CLOSED_OR_REJECTED")
        try:
            if type(revision) is not int or not 1 <= revision <= 10_000:
                raise VoiceEvidenceError("VOICE_REVISION_INVALID")
            if revision < self._revision:
                return False  # A valid late revision cannot erase newer evidence.
            normalized = bounded_text(text)
            if (type(revision) is not int or not 1 <= revision <= 10_000 or
                    type(final) is not bool or type(tokens) is not tuple or len(tokens) > 4096):
                raise VoiceEvidenceError("VOICE_REVISION_INVALID")
            for token in tokens:
                if type(token) is not TokenEvidence:
                    raise VoiceEvidenceError("VOICE_TOKEN_INVALID")
                token.validate()
            if normalized.split() != [token.text for token in tokens]:
                raise VoiceEvidenceError("VOICE_TOKEN_TEXT_UNALIGNED")
            payload = (normalized, tokens, final)
            if revision == self._revision:
                if payload == self._payload:
                    return False
                raise VoiceEvidenceError("VOICE_REVISION_CONFLICT")
            self._guard.replace_tokens(tokens)
            self._text, self._tokens, self._final = normalized, tokens, final
            self._revision, self._payload = revision, payload
            self.latest = None
            return True
        except (ValueError, TypeError, RecursionError):
            self.invalidate()
            raise VoiceEvidenceError("VOICE_REVISION_REJECTED") from None

    def complete(self, handle, final_text):
        self._current(handle)
        try:
            normalized = bounded_text(final_text)
        except ValueError:
            self.invalidate()
            raise VoiceEvidenceError("VOICE_COMPLETION_REJECTED") from None
        if self._closed:
            if self.latest is not None and normalized == self._text:
                return self.latest
            raise VoiceEvidenceError("VOICE_TURN_ALREADY_COMPLETED")
        self._closed = True
        if self._fault or not self._final or normalized != self._text:
            reason = self._fault or ("VOICE_FINAL_NOT_OBSERVED" if not self._final else "VOICE_FINAL_UNALIGNED")
            self._guard.reset_turn()
            # Keep only current completed text, with explicitly unavailable data.
            tokens = tuple(TokenEvidence(word) for word in normalized.split())
            self.latest = VoiceProposal(handle, self._revision, normalized, tokens, False, reason)
            return self.latest
        indices = [i for i in range(len(self._tokens)) if not self._guard.is_tombstoned(i)]
        tokens = tuple(self._tokens[i] for i in indices)
        grounded = bool(tokens) and all(self._guard.check_argument_grounding(self._tokens[i].text, (i, i + 1))[0]
                                        for i in indices)
        reason = "VOICE_EVIDENCE_GROUNDED" if grounded else "VOICE_EVIDENCE_BELOW_FLOOR"
        if not tokens:
            reason = "VOICE_EVIDENCE_EMPTY"
        elif any(t.confidence is None for t in tokens):
            reason = "VOICE_CONFIDENCE_UNAVAILABLE"
        elif any(t.start_ts is None or t.end_ts is None for t in tokens):
            reason = "VOICE_TIMING_UNAVAILABLE"
        self.latest = VoiceProposal(handle, self._revision, " ".join(t.text for t in tokens), tokens, grounded, reason)
        return self.latest
