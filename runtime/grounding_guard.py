"""
runtime/grounding_guard.py

Acoustic-Temporal Grounding Guard (ATGG) + Zero-Hop Deterministic
Self-Repair (ZH-SR).

Key design decision vs. the original spec: tombstoning happens at the
TOKEN-ID level fed into the LLM context, not just on the display
transcript string. Masking only the displayed string while the model's
KV-cache / prompt still contains the retracted tokens means a
self-correction ("book two tickets — wait, no, three") can still leak
into generation via attention over the cached repandum span. We tombstone
by token index range and physically exclude that range when we
serialize context for the LLM call (no cache reuse across a tombstone
boundary).
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional

from runtime.speculative_saga import TurnEpochClock

logger = logging.getLogger("ccs.grounding_guard")


# Domain-adaptive confidence floors. FDB-v3's chained (depth 2-3) tasks
# compound low-confidence args across hops, so higher-stakes domains get
# a stricter bar than the flat 0.60 in the original design.
DOMAIN_CONFIDENCE_FLOOR: dict[str, float] = {
    "finance_billing": 0.78,
    "travel_identity": 0.68,
    "housing_location": 0.62,
    "ecommerce_support": 0.60,
    "default": 0.60,
}

REPAIR_CUES = re.compile(
    r"\b(wait|no wait|scratch that|actually|i mean|sorry|correction|not that|"
    r"let me redo that|change that to)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class TokenEvidence:
    text: str
    confidence: Optional[float] = None
    start_ts: Optional[float] = None
    end_ts: Optional[float] = None

    def validate(self):
        if type(self.text) is not str or not self.text or self.text.strip() != self.text or len(self.text) > 256:
            raise ValueError("Invalid bounded token text")
        for value in (self.confidence, self.start_ts, self.end_ts):
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
                raise ValueError("Evidence must be finite numeric data or unavailable")
        if self.confidence is not None and not 0 <= self.confidence <= 1:
            raise ValueError("Confidence must be between zero and one")
        if ((self.start_ts is not None and self.start_ts < 0) or
                (self.end_ts is not None and self.end_ts < 0) or
                (self.start_ts is not None and self.end_ts is not None and self.end_ts <= self.start_ts)):
            raise ValueError("Invalid media-relative token timing")


@dataclass
class TokenSpan:
    """Token-index range (inclusive start, exclusive end) into a running
    ASR token buffer, with per-token confidence and timing."""
    start: int
    end: int
    tokens: list[str]
    confidences: list[Optional[float]]
    start_ts: Optional[float]
    end_ts: Optional[float]

    def mean_confidence(self) -> float:
        return (sum(self.confidences) / len(self.confidences)
                if self.confidences and all(c is not None for c in self.confidences) else 0.0)


@dataclass
class Tombstone:
    span: TokenSpan
    reason: str
    created_at: float = field(default_factory=time.monotonic)


class GroundingGuard:
    """
    Owns the running ASR token buffer for the current turn and the set of
    tombstoned (retracted) spans within it. Tool-argument candidates are
    checked against acoustic grounding before being allowed to reach the
    saga manager.
    """

    def __init__(
        self,
        epoch_clock: Optional[TurnEpochClock] = None,
        on_eviction: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.epoch_clock = epoch_clock
        self._on_eviction = on_eviction
        self._tokens: list[str] = []
        self._confidences: list[Optional[float]] = []
        self._timestamps: list[tuple[Optional[float], Optional[float]]] = []
        self._tombstones: list[Tombstone] = []
        self._staged_candidates: dict[str, tuple[str, tuple[int, int], int]] = {}
        self._turn_id: int = 0

    # ---- ingestion ------------------------------------------------------

    def ingest_token(self, token: str, confidence: Optional[float], start_ts: Optional[float], end_ts: Optional[float]) -> int:
        """Append one ASR token (interim or final). Returns its index."""
        TokenEvidence(token, confidence, start_ts, end_ts).validate()
        if len(self._tokens) >= 4096:
            raise ValueError("Turn evidence exceeds token capacity")
        self._tokens.append(token)
        self._confidences.append(confidence)
        self._timestamps.append((start_ts, end_ts))
        idx = len(self._tokens) - 1

        # Detect a repair cue arriving; if found, tombstone the span
        # immediately preceding it back to the last clause boundary.
        if REPAIR_CUES.search(token.lower()):
            self._tombstone_preceding_clause(idx)
        return idx

    def replace_tokens(self, evidence: tuple[TokenEvidence, ...]) -> None:
        """Validate before replacement; every revision evicts old candidates.

        Ingest callers must handle a rejected ASR update by invalidating that
        input. This atomic primitive does not guess whether invalid input was a
        revision. No old prefix survives a same-length or shorter hypothesis.
        """
        if type(evidence) not in (tuple, list) or len(evidence) > 4096:
            raise ValueError("Invalid bounded token sequence")
        for token in evidence:
            if type(token) is not TokenEvidence:
                raise ValueError("Explicit token evidence required")
            token.validate()
        evictions = [(name, value) for name, (value, _, _) in self._staged_candidates.items()]
        self.reset_turn()
        for name, value in evictions:
            if self._on_eviction is not None:
                try:
                    self._on_eviction(name, value)
                except Exception:
                    logger.error("Candidate eviction callback failed")
        for token in evidence:
            self.ingest_token(token.text, token.confidence, token.start_ts, token.end_ts)

    def reset_turn(self) -> None:
        self._tokens.clear()
        self._confidences.clear()
        self._timestamps.clear()
        self._tombstones.clear()
        self._staged_candidates.clear()
        self._turn_id += 1

    # ---- tombstoning ------------------------------------------------

    _CLAUSE_BOUNDARY = re.compile(r"[,.;]|\band\b|\bbut\b", re.IGNORECASE)

    def _tombstone_preceding_clause(self, repair_cue_idx: int) -> None:
        start = 0
        for i in range(repair_cue_idx - 1, -1, -1):
            if self._CLAUSE_BOUNDARY.match(self._tokens[i]):
                start = i + 1
                break
        if start >= repair_cue_idx:
            return
        span = TokenSpan(
            start=start,
            end=repair_cue_idx,
            tokens=self._tokens[start:repair_cue_idx],
            confidences=self._confidences[start:repair_cue_idx],
            start_ts=self._timestamps[start][0],
            end_ts=self._timestamps[repair_cue_idx - 1][1],
        )
        self._tombstones.append(Tombstone(span=span, reason="repair_cue"))

        # Evict overlapping staged candidates
        fields_to_remove = []
        for field_name, (_, (c_start, c_end), _) in self._staged_candidates.items():
            if max(start, c_start) < min(repair_cue_idx, c_end):
                fields_to_remove.append(field_name)
        for field_name in fields_to_remove:
            evicted_value, _, _ = self._staged_candidates[field_name]
            self._staged_candidates.pop(field_name, None)
            if self._on_eviction is not None:
                try:
                    self._on_eviction(field_name, evicted_value)
                except Exception:
                    logger.exception("on_eviction callback failed for field %s", field_name)

        # Let Phase 2 machinery see this correction as a stale-epoch event
        if self.epoch_clock is not None:
            asyncio.create_task(self.epoch_clock.advance(reason="self_correction"))

    def is_tombstoned(self, token_idx: int) -> bool:
        return any(t.span.start <= token_idx < t.span.end for t in self._tombstones)

    def clean_context_tokens(self) -> list[str]:
        """
        The ONLY safe way to build an LLM-facing context: exclude
        tombstoned ranges entirely, forcing a fresh prompt encode across
        the tombstone boundary rather than trusting a cached prefix that
        still contains the repandum.
        """
        return [tok for i, tok in enumerate(self._tokens) if not self.is_tombstoned(i)]

    # ---- acoustic-temporal grounding check ------------------------------

    def check_argument_grounding(
        self,
        arg_value: str,
        source_token_range: tuple[int, int],
        domain: str = "default",
    ) -> tuple[bool, float, str]:
        """
        Returns (is_grounded, mean_confidence, reason).

        A candidate tool argument is grounded only if:
          1. its full source token range lies outside any tombstone, AND
          2. mean ASR confidence over that range clears the domain floor.
        """
        if (type(source_token_range) not in (tuple, list) or len(source_token_range) != 2 or
                any(type(v) is not int for v in source_token_range)):
            return False, 0.0, "argument range must contain exact integers"
        start, end = source_token_range
        if any(self.is_tombstoned(i) for i in range(start, end)):
            return False, 0.0, "argument overlaps a tombstoned (retracted) span"

        if end > len(self._confidences) or start < 0 or start >= end:
            return False, 0.0, "argument range out of bounds"

        confs = self._confidences[start:end]
        if any(c is None for c in confs):
            return False, 0.0, "ASR confidence unavailable"
        if any(a is None or b is None for a, b in self._timestamps[start:end]):
            return False, 0.0, "ASR token timing unavailable"
        mean_conf = sum(confs) / len(confs)
        floor = DOMAIN_CONFIDENCE_FLOOR.get(domain, DOMAIN_CONFIDENCE_FLOOR["default"])
        if mean_conf < floor:
            return False, mean_conf, f"mean confidence {mean_conf:.2f} below {domain} floor {floor:.2f}"

        return True, mean_conf, "grounded"

    # ---- candidate staging and resolution -------------------------------

    def stage_candidate(
        self,
        field_name: str,
        raw_value: str,
        token_range: tuple[int, int],
        domain: str = "default",
    ) -> Optional[str]:
        """
        Runs check_argument_grounding; on success, stores into
        _staged_candidates and returns field_name. On failure returns None.
        """
        is_grounded, _, _ = self.check_argument_grounding(raw_value, token_range, domain)
        if is_grounded:
            self._staged_candidates[field_name] = (raw_value, token_range, self._turn_id)
            return field_name
        return None

    def resolve_current_value(self, field_name: str) -> Optional[str]:
        """
        The ONLY method that should ever hand back a value fit for use as a tool argument.

        Returns None if:
          - field_name is not in _staged_candidates
          - the stored token_range currently overlaps ANY tombstone (live check)
          - the candidate was captured in a prior turn
        Otherwise returns the stored value.
        """
        if field_name not in self._staged_candidates:
            return None
        
        raw_value, token_range, capture_turn = self._staged_candidates[field_name]
        start, end = token_range
        
        # Re-check live for overlapping tombstones
        if any(self.is_tombstoned(i) for i in range(start, end)):
            return None
            
        if capture_turn != self._turn_id:
            return None
            
        return raw_value


# --------------------------------------------------------------------------
# Zero-Hop Deterministic Self-Repair
# --------------------------------------------------------------------------

class RepairOutcome(Enum):
    OK = "ok"
    UNRESOLVED = "unresolved"


_PHONE_RE = re.compile(r"[^\d+]")
_DATE_WORDS = {
    "tomorrow": lambda: 1,
    "today": lambda: 0,
}


def zero_hop_repair(field_name: str, raw_value: str, expected_type: str) -> tuple[RepairOutcome, Optional[str]]:
    """
    In-memory, sub-2ms deterministic repair for common ASR/formatting
    errors. No LLM roundtrip. Extend the dispatch table per-field as
    FDB-v3 domains dictate; this covers the common cases across all four
    benchmark domains (Travel/Identity, Finance/Billing, Housing/Location,
    E-Commerce).
    """
    value = raw_value.strip()

    if expected_type == "phone":
        digits = _PHONE_RE.sub("", value)
        if 7 <= len(digits.lstrip("+")) <= 15:
            return RepairOutcome.OK, digits
        return RepairOutcome.UNRESOLVED, None

    if expected_type == "currency_amount":
        cleaned = re.sub(r"[^\d.,]", "", value).replace(",", "")
        try:
            return RepairOutcome.OK, f"{float(cleaned):.2f}"
        except ValueError:
            return RepairOutcome.UNRESOLVED, None

    if expected_type == "relative_date":
        lowered = value.lower()
        for word, _offset_fn in _DATE_WORDS.items():
            if word in lowered:
                return RepairOutcome.OK, word  # caller resolves offset -> ISO date
        return RepairOutcome.UNRESOLVED, None

    if expected_type == "yes_no":
        lowered = value.lower()
        if lowered in {"yes", "yeah", "yep", "affirmative", "correct"}:
            return RepairOutcome.OK, "yes"
        if lowered in {"no", "nope", "negative", "cancel"}:
            return RepairOutcome.OK, "no"
        return RepairOutcome.UNRESOLVED, None

    # Fallback: pass through non-empty strings unchanged.
    return (RepairOutcome.OK, value) if value else (RepairOutcome.UNRESOLVED, None)
