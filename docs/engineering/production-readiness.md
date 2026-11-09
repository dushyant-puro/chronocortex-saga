# Production-readiness ledger

Evidence review date: **2026-11-10**. This ledger separates implemented offline
foundations from outstanding acceptance. It is not a production-readiness
certificate or a completion percentage.

The [console qualification follow-up](console-read-recovery.md#qualification-findings)
records unresolved Windows renewal/process-drill failures and distinguishes
passing runs from reliability acceptance. Those findings remain part of gates
4, 6 and 7 below.
The [diagnostic follow-up](failure-diagnostics.md) adds safe failure categories
and deterministic contention schedules; it does not close those historical findings.
The [Windows setup qualification](windows-setup-qualification.md) adds opt-in
checkpoints and redacted timeout evidence while preserving the 20-second test
budget and native credential checks. Its historical timeout cause remains open.
The [reviewed task admission API](reviewed-task-admission.md) now binds explicit
local-credential acceptance to current evidence, a versioned workflow and exact
immutable arguments. It creates PENDING work atomically without dispatch or
operation approval. Its opt-in [protected UI](proposal-review-ui.md) is now
implemented; configured-host delivery, participant identity and live acceptance
remain open. Schema 7 requires an explicit offline migration.
The 2026-11-07 [durable receipt follow-up](durable-admission-receipts.md)
adds positive journal admission readback after re-login or pending-review loss,
with current authority and original-reviewer checks. It does not close durable
inbox/delivery, cross-tab discovery or REAL PROVIDER reconciliation acceptance.
That phase's initial Windows PR run also exposed a shutdown/drain lock-contention
failure (`FORWARD_COMMITTED`, renewal `AUTHORIZE`/`SQLITE_BUSY`). Passing local and
push runs do not close its unconfirmed cause; see the qualification record above.
The [local acceptance walkthrough](local-acceptance.md) now lets an operator
test the real console/task/auth/supervisor path with two separately approved
SIMULATED effects, including disconnect and cancellation. It pre-admits a
canned task; protected voice-proposal review and real assets remain open.

**PUBLIC MAIN VERIFIED:** `4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`, the original
Phase 0 baseline. The safety stack remains unmerged. **LOCAL BRANCH VERIFIED:**
the durable-task phase at `79b91a2` has 1,201 Windows runtime tests passing with
two documented skips and all ten push/PR CI jobs passing. See
[PR #24](https://github.com/dushyant-puro/chronocortex-saga/pull/24) for exact
evidence. Later phase results belong to their own PRs; do not transfer a pass
from one commit to another.

## Implemented foundations

- Durable action/task/event records, operation claims, task ownership, workflow
  versions, worker leases/fencing and supervised lifetimes.
- Conservative UNKNOWN/stale classifications, exact authorization/approval
  binding, authenticated local control and an operator console backed by SQLite.
- A restricted sandbox-provider adapter, supervised drill and GET-only preflight/
  reconciliation commands; real-provider acceptance is still outstanding.
- Revision-aware voice evidence, proposal-only SDK hooks and voice/task isolation;
  live audio and the protected proposal-review product integration remain outstanding.
- Locked Python graphs, metadata/advisory gates, read-only recovery preflight,
  encrypted backup/drill tooling and offline key rotation.
- Reproducible simulated voice, process-fault, mutation-comparison and whole-task
  evaluations with explicit negative controls and separate parked/completed counts.
- The [offline-wheel tooling](offline-wheels.md) adds archive capture/integrity
  verification. Its PR records current acceptance; off-host custody remains work.

These are substantial foundations. Passing offline tests does not close the
seven release-acceptance areas below. **SIMULATED** effects cannot be relabeled
**REAL PROVIDER**, and a skipped live-voice placeholder is not audio acceptance.

For practical testing, three areas gate a connected research alpha (review
product flow, real voice and real provider acceptance); the other four concern
operations, recovery, independent research and release qualification. You can
test the isolated local walkthrough before those larger gates close. This is
a dependency grouping, not a promise of seven small PRs or a release date.

## Current implementation priority

Per the user's direction, continue the product/integration surfaces around the
passing runtime and defer historical intermittent-CI diagnosis to final
integration/release qualification. Retain all findings; do not speculate about
fixes or waive their release gates. New safety regressions still block their
affected feature. The product integration includes the opt-in
[proposal-review UI](proposal-review-ui.md), followed by durable inbox/delivery,
cross-tab recovery and a configured local review walkthrough. Positive receipt
readback across sessions is implemented in the bounded follow-up above.
The subsequent [admitted-history inbox](admitted-proposal-inbox.md) adds protected
cross-tab identity discovery. Persistent delivery of pending proposals and its
retention/correction semantics remain open; this history does not close them.
The [persistent review delivery](persistent-review-delivery.md) follow-up adds
opt-in, capacity/expiry-bounded inspection evidence in schema 8. It can be read
after restart with current authority but cannot restore admission/current voice
authority. Correction/replacement, consent and operational erasure/backup custody
remain open; schema-8 migration and rollback require the new runbook.
Its core `391c33d` passed 1,535 runtime tests (2 skipped), 55 browser cases,
both environment audits and the independent 5/5 SIMULATED process-fault drill
from fresh GitHub source. This does not close any of the seven workstreams.
The [schema-9 reauthorization follow-up](review-reauthorization.md) adds explicit
fresh review succession for unadmitted retained tasks, with stable task identity
and atomic stale-review fences. Admitted-task replacement/reconciliation remains
blocked and needs separate policy/acceptance; manual host and live assets remain.
At implementation `38be355` ([PR #41](https://github.com/dushyant-puro/chronocortex-saga/pull/41)),
all 37 new cases, 61 local browser cases, both audits and the 5/5 SIMULATED
process drill passed, but the full Windows suite has 1 failure/1,572 passes/2
skips in the existing watchdog subprocess. A Linux push browser job also failed
in an existing receipt/logout journey with `SQLITE_BUSY`. Causes remain
unconfirmed, counterpart passes do not close the gates, and final-integration
diagnosis remains deferred under the current user priority.
Final-head `a243b36` CI was subsequently verified: all five PR jobs passed,
but push Windows runtime failed the sandbox lease-takeover test (PENDING versus
IN_FLIGHT, with a renewal/JOURNAL_LEASE observation). This differs from the local
watchdog exit and remains unconfirmed. See the redacted notification evidence
and completed-CI note in PR #41; counterpart passes do not close its gate.
The next [schema-10 bounded replacement phase](zero-activity-task-replacement.md)
supports only explicitly cancelled, zero-recorded-activity work. Any step/action
evidence remains blocked; manual integration and real provider acceptance remain.
Its core `902f3a8` ([PR #42](https://github.com/dushyant-puro/chronocortex-saga/pull/42))
passed fresh-source Windows runtime 1,621/2 skipped, browser 70/70, both audits
and independent SIMULATED process faults 5/5 cases/122 checks. Existing audited
environments were reused. These offline results do not close inherited failure
causes or any real-provider, voice, operational or release gate.

## Seven remaining acceptance areas

The [manual review walkthrough](manual-review-walkthrough.md) now connects
synthetic trusted evidence to explicit review/admission and two supervised
approvals. Core `452783b` ([PR #43](https://github.com/dushyant-puro/chronocortex-saga/pull/43))
passed fresh-source runtime 1,632/2 skipped, all 73 browser cases, both audits
and the independent SIMULATED drill 5/5 cases/122 checks using existing audited
locked environments. Participant identity, real audio/provider assets and
recorded-effect replacement acceptance remain open; the local walkthrough is
no longer an outstanding implementation item.

The current offline operations phase is the
[configured recovery host](configured-recovery-host.md): a single-process,
loopback console/recovery worker with strict configuration and authenticated
readiness observations. It does not install a native service, dispatch work,
verify snapshot freshness or close operational acceptance. Native watchdog,
service identity/custody and measured deployment tests remain subsequent work.
External provider acceptance remains blocked on real sandbox assets; no extension
to effectful replacement is authorized by simulated evidence alone.

| Area | Current boundary | Work and exit evidence still required |
| --- | --- | --- |
| 1. Real provider acceptance | Adapter, approval flow and offline/process faults exist. | Configure a dedicated sandbox and protected restricted credential; pass GET-only preflight, one exact approved mutation and authoritative readback; demonstrate uncertainty handling without replay and retain redacted evidence. Requires real account access. |
| 2. Real voice acceptance | Pinned SDK hooks and synthetic protocol tests exist. | Choose/configure the STT deployment; test microphone/WebRTC, actual revision/confidence/timing semantics, playback interruption, disconnect/cleanup, participant identity and consent/retention. No fabricated confidence fallback. Requires audio/provider assets. |
| 3. Authenticated proposal-to-task product flow | The local review/admission API atomically binds exact evidence and authority. The opt-in UI adds explicit human review, tab receipt retention and positive journal receipt recovery across sessions. | Build durable delivery/inbox, cross-tab discovery and manual configured-host integration; establish participant identity, consent/retention and correction/replacement UX; qualify the resulting end-to-end product flow. Speech alone must never become approval or cancellation authority. |
| 4. Deployment, identity and operations | Modular local-host supervisor/console protocol exists. | Supply a reviewed Windows Service/systemd/container deployment, external watchdog, secrets/configuration lifecycle and TLS/remote identity if exposed beyond the local host. Validate startup/shutdown/readiness, log/audit retention, capacity/backpressure, failure recovery and measured SLOs. Keep the architecture modular and simple. |
| 5. Disaster recovery and artifact custody | Encrypted snapshots, memory drills, rotation and wheel verification exist. | Establish retained key/archive inventories and redundant off-host recovery copies; test bootstrap/native deployment assets. Design/test live recovery activation with independent freshness, worker fencing and current provider reconciliation. A valid old snapshot must not revive dispatch authority. |
| 6. Independent research evaluation and novelty | Small selected synthetic corpora, transparent toy controls and one integration ablation exist. | Add licensed/independently sourced held-out cases, broader cross-task/worker/security scenarios, fair comparator implementations and further ablations. Refresh related work, predeclare metrics/denominators, retain raw evidence and measure real audio/provider latency in a disclosed environment. No universal novelty or deployment-rate claim follows from current scores. |
| 7. Integration review and release qualification | Changes are preserved as stacked draft PRs; public main is still Phase 0. | Review the safety stack, resolve integration findings and run migration/rollback/security/quality checks on a consolidated release candidate. Validate the chosen deployable artifact and real acceptance evidence, then explicitly approve merge/tag/release/deployment. A collection of green branch tests does not qualify main automatically. |

## How much work remains

There are **seven major acceptance workstreams**, with different sizes and
dependencies. Two require real external access (provider and voice); three are
substantive product/deployment/recovery engineering; two concern independent
research and release qualification. Each can require multiple bounded PRs.
There is substantial integration and acceptance work left; the project is past
its initial safety foundation but has not reached a production release candidate.

A defensible percentage or calendar estimate needs the chosen hosting/STT setup,
available live test assets and review findings. Test count and PR count are not
reliable proxies. This ledger uses concrete gates so completion can be assessed
without inventing progress or performance numbers.

While external access is unavailable, useful bounded next steps are a deployable
local service/configuration wrapper, the authenticated proposal-review contract,
recovery-activation design/tests and a broader independent evaluation corpus.
Do not extend synthetic scorecards merely to make task counts larger. Prioritize
the safety/product/operations gate that the next actual deployment needs.

## Required release discipline

Keep meaningful source changes in reasonably sized commits and one bounded phase
per PR. Every safety change needs regression/adversarial tests, a clean-checkout
run and an explanation of the invariant protected. Record exact source, locks,
environment, skipped cases, real-vs-simulated status and unresolved gates.

On regression stop admission/dispatch and preserve the current journal, claims,
approvals, generations and provider evidence. Code rollback, coroutine
cancellation and restoring old data do not roll back a remote effect. Never
retry an unknown mutation or clear evidence to obtain a passing demonstration.
