# Bounded failure diagnostics and deterministic contention

Base: `codex/console-read-recovery`, `c0fc03f`. Review date: 2026-10-30.
This phase improves failure evidence. It does not claim to have identified or
fixed the causes of the earlier Windows CI renewal, peer-exit and incomplete
lost-response failures. The [qualification findings](console-read-recovery.md#qualification-findings)
remain open until evidence distinguishes a lifecycle defect from host/storage
delays or another failure.

## What changes

The worker logs the failed startup/renewal phase and one closed diagnostic
category. Renewal phases distinguish deadline checking, authority checking, lease
renewal, returned-deadline validation and heartbeat persistence. The existing
`RENEWAL_FAILED`/`STARTUP_FAILED` outcomes, StopReport fields, heartbeat records,
leases, admission gates, database settings and time budgets are unchanged.
The gate closes before logging. An ordinary logging-sink exception cannot replace
the original renewal error or reopen admission. A blocked sink still needs an
external watchdog; these logs are not durable audit records or a liveness proof.

The console's server log keeps its existing request correlation ID and adds a
category for journal/unexpected failures. HTTP status/error bodies remain the
same. Internal SQLite categories are not added to client responses. The console
continues to fail closed when its journal cannot be read.

`runtime.failure_diagnostics.classify_failure` examines at most eight explicit
causes, handles cycles, and never serializes exception messages, dynamic class
names, SQL, arguments, paths, credentials, transcript values or tracebacks.
Recognized native SQLite primary codes distinguish busy/locked, I/O,
corrupt/not-a-database, read-only and full conditions; unknown, boolean, negative,
oversized or missing codes remain `SQLITE_OTHER`. Other fixed categories cover
timeouts, cancellation, OS errors, permission/policy failures, journal/lease
failures and `UNCLASSIFIED`. These are observations about available exceptions,
not authentication of an error's origin or proof of a remote effect's outcome.
The classifier operates inside trusted Python; it is not a hostile-plugin sandbox.

The independent-process evaluator preserves failed case/check counts and adds a
small `diagnostic` object to incomplete cases. Known peer failures identify the
role, wait class and allowlisted expected barrier; peer/main/HTTP-handler errors
can supply only an allowlisted category. A peer exit timeout still fails after
the existing 20-second wait and reports `TIMEOUT`; it does not explain why the
process was slow. Unrecognized exception text or arbitrary peer labels cannot
become report data. Parent exceptions retain a category without invented peer
context. The harness fingerprint now also includes the classifier source.
Direct pytest callers also get this closed peer context in captured error logs,
so a failure before scorecard generation does not lose the role/wait evidence.
An ordinary peer diagnostic-sink failure cannot replace the original failure.

Successful-case schema and the aggregate CLI summary remain compatible. Failure
diagnostics are additive schema-1 fields; strict consumers must allow these
documented optional fields. No additional cases are counted as passed, and no
performance or availability metric is inferred from diagnostic counts.

## Reproducible evidence

Tests hold a real SQLite `BEGIN EXCLUSIVE` lock before a competing operation.
They use a short **test-only** connection timeout and controlled worker clocks;
they do not depend on sleeps, machine load or a production deadline expiring.
Authorization, renewal and heartbeat persistence each produce a native busy
cause, a failed renewal report, closed admission and one attempt. The lock is
released explicitly, and the journal/effect evidence is retained unchanged.
An API test checks a 503 under contention and a later successful read. A real
subprocess database-open failure verifies redacted peer output. Further tests
cover invalid/deep/cyclic causes, poisoned string conversion, malformed peer
labels, failed logging sinks and incomplete-score accounting.

These schedules reproduce possible failure mechanisms. They do not retrospectively
prove that SQLite contention caused the historical CI failures. The peer fixtures
and effects remain **SIMULATED**; **REAL PROVIDER** and real audio are not run.

Use the matching [locked environment](locked-environments.md) and retain results
outside a live journal directory. For example, in PowerShell:

```powershell
New-Item -ItemType Directory -Path results -Force | Out-Null
.\.venv\Scripts\python.exe -m pytest tests/test_failure_diagnostics.py `
  tests/test_worker_supervisor.py tests/test_control_api.py tests/test_provider_process_faults.py `
  -q -p no:cacheprovider --junitxml=results/failure-diagnostics-tests.xml
if ($LASTEXITCODE -ne 0) { throw 'Diagnostic regression checks failed; preserve evidence.' }
.\.venv\Scripts\python.exe -m eval.provider_faults --output results/provider-process-scorecard.json `
  2> results/provider-process-stderr.log
if ($LASTEXITCODE -ne 0) { throw 'SIMULATED drill incomplete or failed; inspect retained evidence.' }
```

For the next real failure, retain the exact commit/locks, installed environment,
OS/Python/SQLite identities, failed case/check and diagnostic fields, request ID
or worker operation/phase, and process exit evidence. Correlate operator-controlled
host storage/watchdog observations before attributing a cause. Protect ordinary
host logs under the deployment's retention/access policy. Do not enable raw
provider payload logging or publish arbitrary stderr to obtain more detail.

## Rollback and next gate

Diagnostics never authorize acquisition, takeover, restore, dispatch, replay or
compensation. On failure preserve the current journal, claims, approvals and
provider evidence. Revert the diagnostic code independently if needed; it has no
schema migration and no new configuration/secrets. Do not clear uncertainty,
extend lease/database timeouts, weaken assertions or retry a mutation to make
a drill appear healthy. A new independent simulated trial is distinct from
retrying an unknown operation in an existing journal.

Exact-head clean-checkout and CI evidence belongs to this phase's PR. The next
qualification gate is to use these diagnostics on an observed failure, establish
its cause, and fix it with a deterministic regression. The seven larger
[production-readiness workstreams](production-readiness.md) remain open.
