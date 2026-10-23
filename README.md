# ChronoCortex-Saga

**Voice turn ≠ task.** Conversational interruption and correction must not erase
durable work or make uncertain remote effects safe to retry.

This branch contains a stacked research safety implementation and an
authenticated local operator console. **PUBLIC MAIN VERIFIED** is still Phase 0
at `4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`; the safety PRs are unmerged.
Provider-effect tests are **SIMULATED**. **REAL PROVIDER** acceptance has not been
run, and the live voice test remains a skipped placeholder. This is not a production-readiness
claim.

The console reads the SQLite journal, reviews exact operation evidence, issues
scoped approvals, records cancellation intent, and records investigation requests.
It does not dispatch or retry provider mutations. Cancellation cannot roll back
a remote effect; UNKNOWN evidence remains unresolved until authoritative
reconciliation.
An explicitly started supervised worker can act on a recorded exact approval.

Python 3.13 / PowerShell:

```powershell
py -3.13 -m venv .venv
New-Item -ItemType Directory -Path results -Force | Out-Null
.\.venv\Scripts\python.exe -m pip --isolated install --require-hashes --only-binary=:all: --report results/install-runtime.json -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m scripts.dependency_audit --profile runtime --output results/environment-runtime.json
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

Both runtime and browser requirements now lock the transitive graph and archive
hashes. CI installs them into fresh environments and rejects metadata drift.
Read the [locked environment runbook](docs/engineering/locked-environments.md)
for supported targets, browser setup, reviewed updates, evidence and limits.

For journal setup, protected offline credentials, running the console, browser
tests, architecture, limitations, migration and rollback, read the
[operator console runbook](docs/engineering/operator-control-console.md).
Production startup generates no demo data, credentials, provider adapter or
background worker.

Earlier invariants and validation are documented in the
[security authority](docs/engineering/durable-security-authority.md),
[workflow contract](docs/engineering/workflow-contract-versioning.md),
[worker ownership](docs/engineering/worker-lease-fencing.md), and
[durable action intent](docs/engineering/durable-action-intent.md) reports.
The [worker supervision runbook](docs/engineering/worker-supervision.md) covers
owned operation lifetimes, drain/timeout behavior, a recovery-only host command,
and observed heartbeat health. Real provider, voice, identity, deployment and
independent evaluation gates remain.

The [provider reconciliation contract](docs/engineering/provider-reconciliation.md)
selects a restricted Stripe test-mode adapter for terminal cancellation of an
existing, known PaymentIntent. It requires durable intent and exact approval;
the voice boundary and default worker do not dispatch it. Automated HTTP fixtures remain **SIMULATED**;
real sandbox acceptance is a separate gate. Unknown mutations are never retried.
`python -m providers.reconcile_stripe --help` describes the supervised,
GET-only command for one previously recorded compatible operation; it cannot
create or replay a cancellation.

`python -m providers.preflight_stripe --help` describes a separate GET-only
setup check. It needs a protected restricted test-key file and the expected
sandbox account ID; an existing PaymentIntent ID is optional. It never opens a
journal or creates, cancels or reconciles an operation. Read access does not
prove write permission, and preflight success is not the real cancellation
acceptance gate.
Follow the [Windows Stripe sandbox setup guide](docs/engineering/stripe-sandbox-setup.md)
to create a dedicated sandbox, save its restricted key through a masked prompt
outside the checkout, and run account-only preflight. The key helper makes no
provider calls; the preflight command contacts the real API only when explicitly run.

The [supervised sandbox drill](docs/engineering/supervised-stripe-drill.md) adds
an explicitly enabled, one-known-resource cancellation host. It stages a fresh
action, waits for an exact console approval, makes at most one cancel POST, and
verifies readback without replay or compensation. The console records authority;
the worker owns execution. This host is separate from the default recovery-only
worker and has not passed real-provider acceptance.

Run `python -m eval.provider_faults` for independent worker/service/proxy process
drills over real loopback TCP. The **SIMULATED** service has its own durable
effect ledger; lost responses, process death and hidden status visibility must
recover with GET only. The [process fault runbook](docs/engineering/provider-process-faults.md)
documents reproduction, evidence and remaining real-provider gates.

The [voice evidence boundary](docs/engineering/voice-evidence.md) replaces the
unsafe live demo path with a proposal-only LiveKit word-evidence hook. Missing
confidence/timing stays unavailable, revisions replace previous hypotheses,
and speech endpoints never authorize mutations or complete durable tasks.
The old `python -m agent` entrypoint is disabled; its fleet harness remains
explicitly SIMULATED. No new voice provider or microphone is enabled.
Run `python -m eval.voice_replay` for the credential-free, deterministic voice
protocol scorecard. Its bundled synthetic cases are separate from ASR quality
and live audio acceptance.
