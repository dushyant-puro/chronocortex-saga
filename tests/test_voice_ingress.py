"""SIMULATED evidence only; no microphone, ASR inference, journal or mutations."""
from dataclasses import replace

import pytest

from runtime.grounding_guard import TokenEvidence
from runtime.voice_ingress import VoiceIngress, VoiceEvidenceError


def words(text, confidence=0.95):
    return tuple(TokenEvidence(word, confidence, i * 0.5, i * 0.5 + 0.4)
                 for i, word in enumerate(text.split()))


def started():
    voice = VoiceIngress()
    stream = voice.open_stream()
    return voice, stream, voice.begin_turn(stream)


@pytest.mark.parametrize("old,new", [("Chennai", "Bengaluru"), ("truck-17 Chennai", "truck-19 Chennai"),
                                       ("truck-17 and Chennai", "Chennai"), ("Chennai", "Chennai tomorrow")])
def test_entire_hypothesis_replaced_for_every_revision_shape(old, new):
    voice, _, turn = started()
    voice.replace(turn, 1, old, words(old), final=False)
    voice.replace(turn, 2, new, words(new), final=True)
    proposal = voice.complete(turn, new)
    assert proposal.text == new and proposal.grounded
    assert proposal.metadata()["authorization"] == "NONE"


def test_stale_duplicate_and_conflicting_revisions_have_distinct_behavior():
    voice, _, turn = started()
    voice.replace(turn, 2, "Bengaluru", words("Bengaluru"), final=True)
    assert voice.replace(turn, 1, "Chennai", words("Chennai"), final=True) is False
    assert voice.replace(turn, 2, "Bengaluru", words("Bengaluru"), final=True) is False
    with pytest.raises(VoiceEvidenceError):
        voice.replace(turn, 2, "Chennai", words("Chennai"), final=True)
    assert not voice.complete(turn, "Bengaluru").grounded


def test_metadata_downgrade_on_same_text_cannot_reuse_scored_words():
    voice, _, turn = started()
    voice.replace(turn, 1, "Chennai", words("Chennai"), final=False)
    voice.replace(turn, 2, "Chennai", (TokenEvidence("Chennai"),), final=True)
    proposal = voice.complete(turn, "Chennai")
    assert not proposal.grounded and proposal.tokens[0].confidence is None


def test_new_turn_and_new_stream_reject_late_old_callbacks():
    voice, stream, old = started()
    current = voice.begin_turn(stream)
    with pytest.raises(VoiceEvidenceError):
        voice.replace(old, 1, "Chennai", words("Chennai"), final=True)
    voice.replace(current, 1, "Bengaluru", words("Bengaluru"), final=True)
    new_stream = voice.open_stream()
    latest = voice.begin_turn(new_stream)
    voice.close_stream(stream)  # An old pipeline finalizer cannot close the new one.
    with pytest.raises(VoiceEvidenceError):
        voice.complete(current, "Bengaluru")
    voice.replace(latest, 1, "Delhi", words("Delhi"), final=True)
    assert voice.complete(latest, "Delhi").grounded


@pytest.mark.parametrize("final,text", [(False, "Chennai"), (True, "Bengaluru")])
def test_endpoint_or_unaligned_final_cannot_promote_old_evidence(final, text):
    voice, _, turn = started()
    voice.replace(turn, 1, "Chennai", words("Chennai"), final=final)
    proposal = voice.complete(turn, text)
    assert not proposal.grounded and proposal.text == text
    assert all(token.confidence is None for token in proposal.tokens)


@pytest.mark.parametrize("bad", ["nan", "mismatch", "oversize", "revision", "negative", "bool", "token-type"])
def test_rejected_current_update_invalidates_old_candidate(bad):
    voice, _, turn = started()
    voice.replace(turn, 1, "Chennai", words("Chennai"), final=True)
    tokens, text, revision, final = words("Bengaluru"), "Bengaluru", 2, True
    if bad == "nan":
        tokens = (replace(tokens[0], confidence=float("nan")),)
    elif bad == "mismatch":
        tokens = words("Chennai")
    elif bad == "oversize":
        text = "x" * 17000
    elif bad == "revision":
        revision = 10001
    elif bad == "negative":
        revision = -1
    elif bad == "bool":
        final = 1
    else:
        tokens = ({"text": "Bengaluru"},)
    with pytest.raises(VoiceEvidenceError):
        voice.replace(turn, revision, text, tokens, final=final)
    assert not voice.complete(turn, "Chennai").grounded


def test_correction_repandum_excluded_and_completed_proposal_is_immutable():
    voice, _, turn = started()
    text = "Chennai wait Bengaluru"
    voice.replace(turn, 1, text, words(text), final=True)
    proposal = voice.complete(turn, text)
    assert proposal.text == "wait Bengaluru" and proposal.grounded
    assert "Chennai" not in str(proposal.metadata())
    assert voice.complete(turn, text) is proposal
    with pytest.raises(VoiceEvidenceError):
        voice.replace(turn, 2, "Chennai", words("Chennai"), final=True)


def test_conversational_turns_have_no_durable_task_or_authority_dependency():
    voice, stream, turn = started()
    voice.replace(turn, 1, "yes cancel task-1", words("yes cancel task-1"), final=True)
    proposal = voice.complete(turn, "yes cancel task-1")
    assert proposal.metadata()["durable_task_effect"] == "NONE"
    voice.begin_turn(stream)
    assert voice.latest is None
    assert not hasattr(voice, "journal") and not hasattr(voice, "saga")


def test_closed_stream_cannot_reopen_and_invalid_completion_cannot_reuse_old_words():
    voice, stream, turn = started()
    voice.replace(turn, 1, "Chennai", words("Chennai"), final=True)
    with pytest.raises(VoiceEvidenceError):
        voice.complete(turn, "x" * 17000)
    assert not voice.complete(turn, "Chennai").grounded
    voice.close_stream(stream)
    with pytest.raises(VoiceEvidenceError):
        voice.begin_turn(stream)


def test_other_thread_cannot_mutate_voice_scope():
    from concurrent.futures import ThreadPoolExecutor
    voice, stream, turn = started()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(VoiceEvidenceError, match="OWNING_THREAD"):
            pool.submit(voice.begin_turn, stream).result(timeout=3)
    assert voice.handle == turn


@pytest.mark.parametrize("text", ["x" * 16385, "界" * 6000, "a " * 4097, "bad\ud800encoding"],
                         ids=["character-capacity", "utf8-capacity", "word-capacity", "invalid-unicode"])
def test_completion_bounds_apply_before_unscored_fallback_and_discard_old_evidence(text):
    voice, _, turn = started()
    voice.replace(turn, 1, "Chennai", words("Chennai"), final=True)
    with pytest.raises(VoiceEvidenceError):
        voice.complete(turn, text)
    assert not voice.complete(turn, "Chennai").grounded


def test_diagnostics_cannot_smuggle_provider_payload_and_bool_is_not_stream_identity():
    voice, _, turn = started()
    voice.close_stream(True)
    voice.replace(turn, 1, "Chennai", words("Chennai"), final=True)
    voice.invalidate("PRIVATE:provider payload or transcript")
    proposal = voice.complete(turn, "Chennai")
    assert "PRIVATE" not in str(proposal.metadata())
    assert proposal.reason == "VOICE_INPUT_REJECTED"
