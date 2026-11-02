# CI thread-stack watchdog

The runtime CI command uses the existing pinned pytest 9.1.1 faulthandler:
`-o faulthandler_timeout=60 -o faulthandler_exit_on_timeout=false`.
The installed plugin implementation was inspected: it arms
`faulthandler.dump_traceback_later` around each test protocol, cancels the
pending dump afterward, and passes the explicit exit policy to CPython.
No additional plugin/package is installed.

## Observation is not cancellation

A test that exceeds 60 seconds emits a Python thread-stack dump. That interval
is a diagnostic trigger, **not a new test timeout**. The watchdog neither
terminates the process nor cancels a coroutine, rolls back a journal or proves
anything about a remote effect. The existing 15-minute job budget, native
PowerShell 20-second test budget, SQLite/lease budgets and provider no-retry
policy remain unchanged. A later test failure retains its nonzero exit status.
There is no automatic rerun or altered assertion.

This CI-only change covers a gap in the transaction observer: a native/OS call
that stalls before returning an exception may never emit its contention record.
Stacks may identify the active Python call sites before the job budget ends.
They do not establish lock ownership or which provider effect occurred.

## Metadata and scope

Faulthandler writes thread identifiers and code filenames, function names and
line numbers to stderr. It does not dump locals/argument values. These stack
traces are **not** the redacted journal-observation JSON format: absolute source
paths and thread metadata can appear in CI logs. The controlled fixture verifies
that a private local value is absent from the stack output. Ordinary pytest
failure reporting has its existing source-context behavior.

Enabled only in the runtime GitHub Actions test command, using synthetic test
assets. Production workers, live provider commands and browser CI commands are
unchanged. Hard runner termination or an unavailable stderr sink may still lose
the dump. This is best-effort diagnosis, not durable recovery evidence or an SLO.

## Verification

Three subprocess cases run the actual installed pytest. The harness reads the
checked-in workflow's exit-policy option; only its synthetic dump interval is
shortened to one second. An explicit event/stdin barrier keeps the test blocked
until the parent observes the thread dump.

- Nonterminating policy: blocked work remains alive, receives an explicit
  release, and the child passes.
- The same schedule with a deliberate failure after release preserves exit 1.
- A terminating-policy negative control exits before release, demonstrating why
  CI explicitly selects `false`. This controlled child has no external effects.

Plugin autoload, explicit plugin environment overrides, conftest loading and
ambient pytest addopts are isolated in the synthetic child. Output retained by
the harness is bounded and not printed wholesale on a coordination failure.
Child coordination has its own watchdog; it cannot extend CI or worker leases.
Coordination and normal child exit share a 15-second allowance, replacing
separate 10-second/5-second allowances without increasing their combined bound.
Cleanup has separate bounded process/pipe-drain attempts. A development run
reported 1 failed/2 passed when the controlled failing child exceeded its
five-second exit allowance; that result is retained. Its cause was not proven.
The final synthetic child uses `--tb=no` because this regression checks
post-dump exit status, not pytest traceback rendering; the real CI command
keeps ordinary failure tracebacks. No production/test-job deadline was extended.

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_ci_hang_diagnostics.py
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider -o faulthandler_timeout=60 -o faulthandler_exit_on_timeout=false --junitxml=results/runtime-tests.xml
```

## Open findings and next boundary

PR #35 core source `7b775ff` Windows job `125103254471` reported 1 failed,
1,448 passed, 2 skipped: native launcher timeout at VERIFY_ACL with the original
20-second budget. Its push jobs all passed. Report-head `d1ac8f5` Windows job
`125104138346` passed 1,449 tests/2 skips, then its simulated provider drill
passed 4/5 cases and failed waiting for RESPONSE_WITHHELD_AFTER_APPLY. Before
cleanup, the worker had reported OBSERVATION_FAILED and SQLITE_BUSY; separate
oracle snapshots observed zero POSTs/effects and no dispatch intent. The holder
and causal schedule remain unconfirmed. Same-source push jobs all passed.

The subprocess protocol currently excludes raw stderr from public artifacts.
Consequently its worker's new journal observations were not relayed into the
safe failure snapshot. The next diagnostic boundary is validated forwarding of
those fixed-schema records, **not uploading raw peer stderr**. The CI watchdog
does not close that gap or explain the historical Windows failure.

Rollback: revert the workflow options/tests; preserve all failure evidence and
journals. No runtime/schema migration, provider action, real voice integration,
merge or deployment is part of this phase.

## Source-attributed qualification

At `2d7e5ad`, [draft PR #36](https://github.com/dushyant-puro/chronocortex-saga/pull/36),
the final three focused subprocess tests passed. Independent fresh GitHub source
passed 1,452 runtime tests with 2 skips using the exact CI watchdog options and
the unchanged audited runtime environment. This is clean-source verification,
not a fresh installation claim. Earlier development failure evidence is retained
separately. No new real provider, browser or performance result is claimed.
Hosted qualification is recorded per source in the PR and does not explain
the historical Windows failures.
