# Voice evidence boundary

Stacked on `codex/stripe-sandbox-reconciliation` at
`b894a86e080d83c39d20238b148b5e276282fc50`. PUBLIC MAIN VERIFIED remains
Phase 0 at `4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`; safety PRs are unmerged.
This phase is offline implementation and SDK contract validation. Speech,
confidence and timing in tests are SIMULATED; no live microphone/STT/provider
quality, latency or word-error rate is claimed.

## Evidence is not authorization

`TokenEvidence` preserves unavailable confidence and media-relative timestamps
as None. NaN, infinity, booleans masquerading as numbers, out-of-range confidence
and invalid/zero-duration timing are rejected. Missing confidence or timing
cannot pass grounding. Local receipt/wall-clock time is not word timing.
Numerical validation cannot determine provenance: trusted STT adapters must
identify the time basis and must never manufacture data or broadcast an
utterance confidence across words.

`GroundingGuard.replace_tokens` validates a bounded complete hypothesis before
replacement, evicts every previous candidate, and rebuilds correction tombstones.
Same-length edits, deletions, metadata downgrades and earlier-token changes can
never survive because a previous word count stayed constant. Rejected low-level
replacement is atomic; ingress must invalidate a rejected current ASR update,
rather than continue offering an older candidate. A calibrated confidence
threshold is still a research/real-provider evaluation gate, not proof that a
speaker's statement is true or authorized.

An ASR endpoint/final transcript, a VAD listening state, an interruption and a
spoken yes/no are conversational evidence. None grants CCS permissions, consumes
an approval, dispatches or compensates an effect, or finishes/cancels a durable
task. Durable tasks remain under the journal, supervisor and authenticated
workflow/approval boundary. Proposed voice text is ephemeral until explicitly
bound to that separate protocol. Loss of the voice process must never replay a
durable effect.

## SDK boundary references

The installed/pinned `livekit-agents==1.8.3` source was inspected directly.
High-level `UserInputTranscribedEvent` has text/finality/item/speaker/language
and receipt time, but no per-word acoustic confidence or media timing. Raw
`stt.SpeechData.words` may carry `TimedString` metadata; its missing-value
sentinel must remain unavailable. Utterance confidence defaults are not a
substitute for per-word data. An STT final can finalize a segment rather than
the whole conversational turn. VAD listening does not establish a final turn.

Official references checked 2026-10-23:
[events](https://docs.livekit.io/reference/agents/events/),
[pipeline nodes/hooks](https://docs.livekit.io/agents/logic/nodes/), and
[STT SDK types](https://docs.livekit.io/reference/python/livekit/agents/stt/index.html).
Published SDK docs may move ahead; the installed pinned package controls the
implemented hook signatures. No new STT provider is enabled by this phase.

## Regression and rollback

`tests/test_voice_grounding_evidence.py` covers unavailable/malformed evidence,
range validation, full-hypothesis replacement and atomic rejection. Sixteen
cases failed against the parent; six additional rejection cases already passed.
Legacy fleet tests remain explicitly SIMULATED and now use valid synthetic
timing instead of zero-duration values; they are not live voice acceptance.

On regression, disable voice proposal ingestion and retain the durable task and
action journal. Do not roll back provider state, clear unknown key claims or
restore stale approvals. Voice turn state is ephemeral and must be reacquired;
never recreate a mutation from a saved transcript as process recovery.
