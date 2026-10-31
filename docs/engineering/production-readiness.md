# Production-readiness ledger

Evidence review date: **2026-10-28**. This ledger separates implemented offline
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
  live audio and authenticated proposal-to-task binding remain outstanding.
- Locked Python graphs, metadata/advisory gates, read-only recovery preflight,
  encrypted backup/drill tooling and offline key rotation.
- Reproducible simulated voice, process-fault, mutation-comparison and whole-task
  evaluations with explicit negative controls and separate parked/completed counts.
- The [offline-wheel tooling](offline-wheels.md) adds archive capture/integrity
  verification. Its PR records current acceptance; off-host custody remains work.

These are substantial foundations. Passing offline tests does not close the
seven release-acceptance areas below. **SIMULATED** effects cannot be relabeled
**REAL PROVIDER**, and a skipped live-voice placeholder is not audio acceptance.

## Seven remaining acceptance areas

| Area | Current boundary | Work and exit evidence still required |
| --- | --- | --- |
| 1. Real provider acceptance | Adapter, approval flow and offline/process faults exist. | Configure a dedicated sandbox and protected restricted credential; pass GET-only preflight, one exact approved mutation and authoritative readback; demonstrate uncertainty handling without replay and retain redacted evidence. Requires real account access. |
| 2. Real voice acceptance | Pinned SDK hooks and synthetic protocol tests exist. | Choose/configure the STT deployment; test microphone/WebRTC, actual revision/confidence/timing semantics, playback interruption, disconnect/cleanup, participant identity and consent/retention. No fabricated confidence fallback. Requires audio/provider assets. |
| 3. Authenticated proposal-to-task product flow | Voice can propose; the console owns authorized commands. | Bind reviewed evidence to principal/task/generation and exact arguments; define durable evidence/privacy retention and correction/revocation semantics; test stale/cross-principal proposals and the resulting console flow. Speech alone must never become approval or cancellation authority. |
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
