# Journal contention diagnosis

This phase adds observations, **not a fix or causal reproduction of historical
hosted CI failures**. All controlled tool/provider effects are SIMULATED; SQLite
connections, threads and the external-process writer are real.

The retained Windows PR #32 trace at `5d43fd0` shows `BEGIN IMMEDIATE` failures
during walkthrough polling and journal open, plus renewal AUTHORIZE/SQLITE_BUSY.
It does not identify the lock holder. Increasing the 250 ms default timeout,
retrying a mutation, extending leases or changing journal mode would not answer
that question. Those policies remain unchanged: DELETE journal, synchronous
EXTRA, exclusive dispatch leases and fail-closed uncertainty handling.

## What is observed

Each journal transaction registers a process-local ticket. Fixed boundary
phases distinguish connection/configuration, BEGIN, schema/lease validation,
body, COMMIT, clock preservation and cleanup. Fixed operation labels distinguish
open, action/task reads, authentication and known writer events. Unknown labels
become OTHER; SQL statements/parameters are never traced.

Only actual SQLite BUSY/LOCKED error codes produce a warning. A closed JSON
snapshot is captured before cleanup. The journal attempts rollback/close,
unregisters its observation, then emits the warning. Logging never runs under
the observer mutex or before that cleanup attempt. Logger failure/reentrancy
cannot replace the original exception or confer authority.

The `ccs.journal` warning contains:

- Category, boundary phase and operation kind for the failed transaction.
- Count of tracked peers for the same normalized resolved path; at most eight
  peer phase/kind/SAME_THREAD-or-OTHER_THREAD entries, with truncation explicit.
- PROCESS_LOCAL_ONLY scope and TRACKED_LOCAL/PARTIAL/UNAVAILABLE coverage.

No paths, raw thread/PID values, SQL text, arguments, request/operation IDs,
credentials or exception messages enter that payload. Registry memory is capped
at 128 transaction entries. Overflow is disclosed conservatively as partial or
unavailable coverage; it is not silently reported as complete. Only scalar
overflow presence is reported. Tickets/path/thread references are removed on
normal exit, errors and cleanup failure. There are no success-event logs.

The short observer mutex protects only this metadata. No caller holds it across
a SQLite call, provider await or logging sink. It does not serialize SQLite
writers, change transaction boundaries or provide fairness/retry authority.

## Interpretation limits

Tracked peers are **not proven SQLite lock owners**: a peer may be connecting,
waiting or cleaning up. A SAME_THREAD BODY peer is evidence of overlapping
instrumented transactions on that thread; a controlled nested test establishes
that mechanism, not that it caused an earlier CI run. Other processes and direct
SQLite connections are invisible. Zero tracked peers does not mean no writer,
no contention, no remote effect or permission to retry. Path aliases/hard links
may prevent matching. These are separate observations, not a global snapshot.

The observer records neither wait duration nor an SLO. Its overhead is not
benchmarked. Logging can itself block on an OS/sink failure after cleanup;
hard process termination can lose captured logs. If connection close itself
fails, attempted cleanup is not proof that the connection released its locks.
The original persistence failure still stops dispatch; provider uncertainty
must be reconciled through authoritative evidence, not through this telemetry.

## Verification and next gate

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_journal_observation.py --log-cli-level=WARNING
.\.venv\Scripts\python.exe -m pytest -q --junitxml=results/runtime-tests.xml
.\.venv\Scripts\python.exe -m eval.provider_faults --output results/provider-process-scorecard.json
```

The 16 focused cases cover controlled same-thread/other-thread/external-process
contention, blocked COMMIT and rollback preservation, diagnostic emission after
cleanup, mutex/SQL ordering, bounded capacity/redaction, malformed codes,
reentrant/broken sinks, success silence and clock-preservation errors. Test
watchdogs bound coordination; ordering comes from explicit barriers/locks.

On a real failure, retain the exact source, failing-test/case identity and safe
warning alongside worker phase/category. Correlate observed peers with the
call paths, then build a deterministic causal schedule before changing lock
policy. A later passing CI run does not close an earlier unknown cause.

Rollback is a code revert. No schema migration or data rewrite is needed;
preserve journals, approvals, operation claims, UNKNOWN records and remote IDs.
This phase adds no provider integration, live audio or UI authority.

## Source-attributed qualification

Implementation `7b775ff`, [draft PR #35](https://github.com/dushyant-puro/chronocortex-saga/pull/35):
16 focused diagnostic cases passed. Independent clean GitHub source passed
1,449 runtime tests with 2 skips, and 25 browser-suite tests using the unchanged
audited locked runtime/browser environments. This is clean-source verification,
not a fresh environment installation. Distributed evaluation passed 5/5
SIMULATED cases and 122 checks. Its expanded harness SHA-256 is
`5da8b57656fbf31ebf0d11f0a1fd42e611a4c19694917cd9178247b6bbab8edf`.
Hosted outcomes are tracked per head in the PR; no historical lock-holder
identification or production-readiness claim follows from these passes.
