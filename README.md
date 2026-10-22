# ChronoCortex-Saga

**Voice turn ≠ task.** Conversational interruption and correction must not erase
durable work or make uncertain remote effects safe to retry.

This branch contains a stacked research safety implementation and an
authenticated local operator console. **PUBLIC MAIN VERIFIED** is still Phase 0
at `4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`; the safety PRs are unmerged.
Provider-effect tests are **SIMULATED**. No **REAL PROVIDER** is enabled and the
live voice test remains a skipped placeholder. This is not a production-readiness
claim.

The console reads the SQLite journal, reviews exact operation evidence, issues
scoped approvals, records cancellation intent, and records investigation requests.
It does not dispatch or retry provider mutations. Cancellation cannot roll back
a remote effect; UNKNOWN evidence remains unresolved until authoritative
reconciliation.

Python 3.13 / PowerShell:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

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
The next release gate is supervised worker operation and recovery; real provider,
voice, identity and independent evaluation work remains.
