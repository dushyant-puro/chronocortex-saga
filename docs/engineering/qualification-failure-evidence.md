# Pre-cleanup qualification failure evidence

This phase addresses missing observations in the previous Linux process-drill
barrier failure and adds a deterministic safety regression for post-effect
SQLite contention. It does not claim to identify or fix the actual historical
Linux or Windows causal schedules. See [implementation.md](../../implementation.md)
for the preserved findings and exact earlier source/CI evidence.

## New observation boundary

`eval.provider_faults.run_case` captures failures inside its peer `ExitStack`,
before peer teardown can change the ledgers or replace the worker's terminal
observations. The snapshot has an internal typed representation; arbitrary
exception-attached dictionaries are not copied into scorecards.

Only these fields leave the fixture boundary:

- At most eight peers, each with at most eight allowlisted protocol milestones,
  state/failure enums, bounded exit-code information and an invalid-output flag.
- Separate read-only observations of the CCS journal, proxy and provider
  ledgers: fixed counters and bounded state/worker-event samples.
- Explicit SIMULATED labeling and flags denying retry/dispatch authority and
  root-cause confirmation.

No transcript, credential, request argument, provider payload, operation ID,
URL/path, arbitrary event text, exception text or raw peer stderr is exported.
This is a trusted internal fake-provider harness, not a general reader for
hostile databases or protection from malicious Python code in the test process.

Each database is observed in its own query-only transaction. The snapshots are
**not globally atomic** and cannot establish causal order between processes.
Missing/contended/corrupt/unsupported files produce UNAVAILABLE rather than
fabricated zero counters. WAL is rejected from its header before SQLite can
create shared-memory sidecars. No database is created, migrated or repaired.
Observation query progress/busy waits are bounded; an OS/filesystem hang remains
outside Python's timeout guarantees.

## Failure preservation and CI

Scorecards include the captured evidence on failed cases. Direct pytest process
acceptance cases additionally publish redacted JSON under
`results/provider-failure-evidence/`; CI retains only `*.json` there, alongside
JUnit, even if later scorecard commands do not run. Raw fixture directories
contain synthetic credentials/journals and must never be uploaded wholesale.

Publication flushes a new `.partial` file, then creates the final unique JSON
name with an atomic no-clobber hard link. There is no overwrite/copy fallback.
Incomplete files remain `.partial` and are excluded from the CI glob. Failed
observers, logs or artifact storage cannot replace the original exception,
authorize retries or skip owned peer cleanup. This phase does not change the
existing 20-second peer barriers or production worker/SQLite budgets.

These records are a bounded failure-time sample, not a complete replay log,
freshness proof or remote-provider reconciliation receipt. Published artifacts
must be attributed through their exact CI source metadata or parent scorecard.

## Deterministic contention regression

The new worker schedule deliberately applies one SIMULATED effect, takes a real
SQLite exclusive lock, then lets the executor return. Local finalization fails
without recording a commit receipt. The test checks that dispatch intent and
operation identity survive, new writes are fenced, restart projects the action
as UNKNOWN, and replay is rejected. The lock is controlled by the fixture; it
does not establish who held the lock in the historical Windows CI failure.

Run with the locked runtime in PowerShell:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_failure_evidence.py tests/test_provider_process_faults.py tests/test_worker_supervisor.py
.\.venv\Scripts\python.exe -m pytest -q --junitxml=results/runtime-tests.xml
.\.venv\Scripts\python.exe -m eval.provider_faults --output results/provider-process-scorecard.json
```

The harness digest now includes `failure_evidence.py`; earlier harness hashes
must not be reused for these results. Exact-source qualification belongs to
this phase's PR. REAL PROVIDER/live audio acceptance has not run.

## Rollback and remaining gate

This adds no runtime schema, credential, provider endpoint, automatic retry or
worker authority policy. Roll back the observer/evaluator/CI capture together
while retaining failure artifacts and existing journals/claims/receipts. Never
retry an uncertain mutation or restore old journal data as code rollback.

The next observed failure must correlate peer milestones, unavailable counters
and original error categories to establish a causal reproduction. Only then
should a production scheduling/storage/protocol fix be proposed. The historical
reliability findings and seven production-acceptance workstreams remain open.
