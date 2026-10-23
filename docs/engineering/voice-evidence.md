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

The original hook inspected `livekit-agents==1.8.3` directly. The
[dependency advisory phase](dependency-advisories.md) moves the verified guard
and pinned agent/plugin pair to `1.8.8` so its declared OpenTelemetry range can
include the telemetry parsing fix. SDK-object, proposal, interruption and
no-authority regressions are rerun against that installed release; earlier or
unverified SDK versions fail before connection. This remains protocol evidence,
not real audio acceptance.
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

## Revision and SDK proposal protocol

`runtime/voice_ingress.py` has no journal, saga, provider, approval or task
reference. It holds one bounded current hypothesis and one immutable proposal.
Opening a new stream/turn invalidates prior ephemeral candidates. Local handles
fence stale callbacks; increasing revisions replace complete hypotheses.
Duplicate identical revisions are no-ops, stale revisions are ignored, and a
conflicting or malformed current update invalidates the turn. Text is capped
at 16 KiB, words at 4096 and revisions at 10,000 per turn. A completed proposal
requires a final hypothesis aligned with the framework's final text. Unsupported
or conflicting evidence stays ungrounded. Metadata logs omit transcript values.

`voice/livekit_observer.py` uses the pinned SDK's actual `Agent.stt_node` and
`on_user_turn_completed` hooks. It captures SpeechData word metadata before
yielding events back to the SDK. It never copies utterance confidence or event
creation time onto words. Missing TimedString values, text/word misalignment,
unsupported timing offsets and ambiguous overlapping utterances are ungrounded.
Known media times cannot move behind completed-segment/previous-turn boundaries.
This is local SDK stream ordering, not a provider-independent guarantee that
arbitrary delayed or reordered network events have correct utterance identity.
Provider request IDs are deliberately not treated as utterance IDs. Real
provider behavior/diarization/timing calibration remains an acceptance gate.

Multiple final segments accumulate until the SDK turn-completion hook. Duplicate
finals do not append twice. END_OF_SPEECH/listening never seals a proposal, and
a final arriving after the end hint may still supply evidence. Stream generations
exclude old pipeline data and old finalizers from a new stream. Disconnect clears
ephemeral proposals. Corrections remove repandum tokens from the proposal, but
this is not a claim to erase every prior model cache or conversation item.

The observer has no LLM/TTS/tools; its completion hook raises the SDK's
StopResponse to suppress generation. A speaking event only interrupts playback.
It never advances a durable task's epoch or calls task cancellation/repair.
`start_observer(ctx, stt_provider)` requires an explicitly supplied streaming,
word-aligned STT instance and the verified SDK version before connecting. It
disables session recording. Provider selection, secret injection, room identity,
audio consent/retention, SDK logging configuration and real audio tests are
separate deployment gates. Word alignment capability alone does not promise
confidence availability; missing per-word metadata is still ungrounded.

No new dependency or provider is auto-selected. The previous live entrypoint
fabricated confidence/times, treated silence as commit and configured an
unverified STT pipeline. It is retired and fails before connection. Existing
CCSAgent fleet demo methods require explicit simulation. They remain regression
fixtures for prior saga/policy fixes, not the production voice integration.
Callers must migrate to the observer and a separately authenticated durable
workflow boundary; synthetic scalar confidence is not a compatibility fallback.

`tests/test_voice_ingress.py` exercises full revisions, stale handles, duplicates,
conflicts, rejected updates, final-text alignment, unavailable data and proposal
scope. `tests/test_livekit_voice_observer.py` uses real pinned SDK dataclasses,
hooks and a session subclass with SIMULATED data; it tests segment accumulation,
late finals, old streams, ambiguous overlap, absent word metadata, reply
suppression, redacted logs and fail-closed legacy entrypoints. These are SDK
contract tests, not microphone, WebRTC, live STT or real-provider acceptance.

## Deterministic offline evaluation

After the README's Python 3.13 environment setup, run in PowerShell:

```powershell
& .\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
& .\.venv\Scripts\python.exe -m eval.voice_replay `
  --output results/voice-evidence-scorecard.json
Get-Content results/voice-evidence-scorecard.json
```

The versioned bundled fixture `eval/fixtures/voice_evidence_cases.json` supplies
ten explicit, independent expectations: same-length replacement, missing word
confidence/timing, silence without final evidence, late final, corrected
repandum, multiple/duplicate final segments, old pipeline events, invalid
confidence and final-text misalignment. The runner constructs pinned SDK events
and invokes the actual ingress/bridge. No STT inference, provider, microphone,
audio corpus, model credentials or FDB assets are used.

The scorecard is labeled SIMULATED, hashes canonical fixture semantics (stable
across Windows/Linux line endings), records pass/fail reasons, and omits raw
transcript values. It writes atomically. Invalid/oversized/ambiguous fixture
schemas fail with exit 2; a violated expectation exits 1; all expectations pass
with exit 0. The CI runtime matrix runs this command and retains the explicitly
simulated JSON scorecard for seven days. `tests/test_voice_replay.py` deliberately
changes an expectation and checks failure, plus parsing, bounds and redaction.

`tests/test_voice_task_isolation.py` runs an actual WorkerSupervisor and SQLite
two-step workflow with an independent SIMULATED effect ledger. While step one
awaits, voice revisions/new turns/disconnect leave its epoch and work unchanged.
Successful work finishes both steps; lost acknowledgement stays UNKNOWN and the
second step is parked. Spoken yes/cancel cannot release an exact-approval gate
or cancel the durable objective. This tests the voice/task boundary; it is not
evidence that task corrections can already be inferred and applied from speech.

These ten protocol cases are a regression corpus, not held-out ASR evaluation,
voice quality evidence, a production SLO, a latency/word-error measurement or a
novelty claim. The existing adversarial task scorecard and its parked restart
trials have not been replaced or inflated with these synthetic cases. Actual
microphone/WebRTC/provider tests, calibrated confidence, participant identity,
ambiguous provider ordering, room access/privacy, authenticated proposal-to-task
binding, durable voice evidence retention and independently sourced evaluation
assets remain acceptance gates. The live voice placeholder remains skipped.

Final boundary review additionally checks raw character size before UTF-8
encoding, rejects invalid Unicode and applies the 4096-word cap to text-only
completions as well as scored revisions. A rejected completion cannot fall back
to older scored words. Diagnostic reasons accept bounded code strings only,
and a boolean cannot impersonate stream generation 1 when closing a pipeline.
These checks protect capacity and log integrity even when acoustic data is absent.
