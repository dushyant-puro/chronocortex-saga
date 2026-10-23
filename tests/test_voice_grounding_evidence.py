"""Strict acoustic evidence and replacement semantics; all words are SIMULATED."""
import pytest

from runtime.grounding_guard import GroundingGuard


@pytest.mark.parametrize("confidence", [None, float("nan"), float("inf"), -0.1, 1.1, True, "0.99"])
def test_unavailable_or_invalid_confidence_never_grounds(confidence):
    guard = GroundingGuard()
    try:
        index = guard.ingest_token("Chennai", confidence, 0.0, 0.4)
    except (ValueError, TypeError):
        assert guard.clean_context_tokens() == []
        return
    assert guard.stage_candidate("destination", "Chennai", (index, index + 1)) is None


@pytest.mark.parametrize("start,end", [(None, None), (0, 0), (1, 0), (-1, 0),
                                        (float("nan"), 1), (0, float("inf")), (True, 1)])
def test_missing_or_invalid_timing_cannot_ground(start, end):
    guard = GroundingGuard()
    try:
        index = guard.ingest_token("Chennai", 0.99, start, end)
    except (ValueError, TypeError):
        assert guard.clean_context_tokens() == []
        return
    assert guard.stage_candidate("destination", "Chennai", (index, index + 1)) is None


def test_same_length_revision_evicts_old_value_before_new_candidate():
    from runtime.grounding_guard import TokenEvidence
    evictions = []
    guard = GroundingGuard(on_eviction=lambda *args: evictions.append(args))
    guard.replace_tokens((TokenEvidence("Chennai", 0.99, 0, 0.4),))
    assert guard.stage_candidate("destination", "Chennai", (0, 1))
    guard.replace_tokens((TokenEvidence("Bengaluru", 0.99, 0, 0.4),))
    assert guard.resolve_current_value("destination") is None
    assert evictions == [("destination", "Chennai")]
    assert guard.clean_context_tokens() == ["Bengaluru"]


def test_metadata_downgrade_evicts_even_when_text_is_unchanged():
    from runtime.grounding_guard import TokenEvidence
    guard = GroundingGuard()
    guard.replace_tokens((TokenEvidence("Chennai", 0.99, 0, 0.4),))
    assert guard.stage_candidate("destination", "Chennai", (0, 1))
    guard.replace_tokens((TokenEvidence("Chennai", None, None, None),))
    assert guard.resolve_current_value("destination") is None
    assert guard.stage_candidate("destination", "Chennai", (0, 1)) is None


def test_invalid_replacement_is_atomic_and_preserves_previous_evidence():
    from runtime.grounding_guard import TokenEvidence
    guard = GroundingGuard()
    guard.replace_tokens((TokenEvidence("Chennai", 0.99, 0, 0.4),))
    assert guard.stage_candidate("destination", "Chennai", (0, 1))
    with pytest.raises(ValueError):
        guard.replace_tokens((TokenEvidence("Bengaluru", 0.99, 0, 0.4), TokenEvidence("bad", float("nan"), 1, 2)))
    assert guard.resolve_current_value("destination") == "Chennai"
    assert guard.clean_context_tokens() == ["Chennai"]


@pytest.mark.parametrize("span", [(True, 1), (0, 1.0), (0, 2), (-1, 1), (1, 0)])
def test_malformed_ranges_cannot_ground(span):
    guard = GroundingGuard()
    guard.ingest_token("Chennai", 0.99, 0, 0.4)
    try:
        result = guard.stage_candidate("destination", "Chennai", span)
    except (ValueError, TypeError):
        return
    assert result is None
