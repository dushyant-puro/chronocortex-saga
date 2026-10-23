# Safety phase 1k: supervised worker lifetime and recovery

Base: production/operator-control-console, ee18fbe53b08f5eedf496ffae54acbaf83dbc831.
**PUBLIC MAIN VERIFIED** remains Phase 0 at
4162ec17f4bda15c13de3fa1f0e1e33e3fe35591. The safety stack is unmerged.
All external effects used for validation are **SIMULATED**; no **REAL PROVIDER**
or voice integration is enabled by this host.

## Core lifetime contract

WorkerSupervisor owns a fresh journal, an explicit offline credential, one lease,
one runtime and its operations. Startup authenticates ccs:work, acquires ownership,
then calls a trusted synchronous runtime factory for construction/hydration. It
does not bind provider callbacks to recovered actions, replay pending work, adopt
changed workflows, acquire ownership repeatedly, or recreate a failed runtime.
Recovery requires another supervisor and another journal/runtime instance.

The existing journal and durable security authority remain decisive. This phase
adds no service/database and does not change schema 6. New host observations use
the existing worker audit table. Shared in-process lease capabilities carry their
lifecycle gate across bind_lease connections; existing manually owned trusted
host APIs remain compatible. These Python capabilities are not authentication
against malicious Python code, IPC tokens or remote provider fencing tokens.

| Invariant | Enforcement and adversarial evidence |
| --- | --- |
| A cancelled conversational waiter does not cancel work | submit returns a WorkerOperation whose wait shields the owned coroutine. A barrier test cancels its consumer and then persists the original forward receipt. |
| Admission closes before drain | New submissions, staged writes, new task checkpoints, forward intent, repair intent and status queries reject once stopping starts. The intent transaction rechecks ccs:work independently of domain grants/approval consumption. Bound connections share the gate. |
| Awaiting operations retain their evidence | Forward, compensation and reconciliation entrypoints register their actual task, including direct calls and background repair. Existing result/checkpoint persistence remains possible under a valid lease while draining. |
| A drain timeout does not mean rollback | asyncio.wait bounds the drain without cancelling children. A timeout stops renewal, reports RECOVERY_REQUIRED and retains the lease until expiry. It does not clear claims, report successful cancellation, repeat a mutation, or release ownership with outstanding callbacks. |
| Failed renewal cannot restore admission | The first renewal/authority/persistence failure closes admission. No retry or automatic reacquisition occurs. The background loop and shutdown I/O are owned and awaited. |
| Event-loop delay cannot manufacture a fresh lease budget | A local monotonic deadline starts before acquisition/renewal I/O, with a safety margin. Expired deadlines and late renewal replies permanently fence this runtime even when wall time has not advanced. |
| Replacement does not replay an uncertain effect | A timed-out worker returns after a new generation starts. Its late result is fenced; the replacement projects UNKNOWN and cannot invoke the old executor. Receipt memory and durable intent are distinguished. |
| Stop is attributable and idempotent | Host observations and release are lease fenced. Repeated stop returns the same report; cancellation of a shutdown waiter does not cancel owned cleanup. Failed final observation/release reports recovery required. |

The factory must not launch provider calls or unowned background work. Trusted
adapters route mutations and status queries through the saga; arbitrary Python
or direct HTTP provider calls cannot be intercepted by this supervisor. New
operations use one owning event loop. Bounded operation capacity rejects before
calling the submitted operation factory. Legacy direct callers retain their
explicit cancellation semantics; use WorkerOperation.wait at conversational
boundaries to isolate consumer cancellation.

Default lease TTL is 30 seconds, renewal interval 5 seconds, monotonic safety
margin 1 second, drain budget 10 seconds and capacity 32 operations. Exact integer
settings and renewal/TTL relationships are validated. Timing values are policy,
not measured production latency or availability. Host wall time must remain
trustworthy; local monotonic deadlines complement the journal's persisted
observed wall-clock high-water marks. Neither detects hostile clock/database
snapshot manipulation or supplies multi-host consensus.

Normal shutdown closes admission, drains actual active tasks while renewal
continues, awaits renewal I/O, records stop and releases ownership. Existing
unresolved UNKNOWN actions do not count as active coroutines and remain unresolved
after release; a new generation still cannot replay them. A late receipt after a
timeout can persist while the old lease remains valid, but cannot reopen admission.
After expiry/takeover, the old process may retain only an in-memory receipt;
provider reconciliation remains necessary. No late-receipt inbox is claimed.

Startup and renewal I/O have separate owned tasks. Cancelling a waiter cannot
abandon acquisition or a renewal thread; shutdown awaits that owned I/O before
release. A stop during acquisition prevents later runtime hydration. Both races
have deterministic threading/event barriers. Final host role and admission are
also tested after preflight validators change authority or request shutdown,
before journaled intent. A heartbeat failure caused by credential revocation
still drains an already-issued forward receipt while the lease remains valid.

Async drain budgets do not interrupt a hung OS/disk call. A blocked event loop
cannot run a signal handler or heartbeat. External process supervision is still
needed to terminate an unresponsive process. Python runner cleanup/forced process
termination may cancel Python calls; that is never evidence of remote rollback.
Do not force-clear its lease or retry uncertain mutations.

## Core host API

```python
from runtime.action_journal import SQLiteActionJournal
from runtime.worker_supervisor import WorkerSupervisor

host = WorkerSupervisor(SQLiteActionJournal(journal_path), worker_credential)
# trusted_factory(journal, auth_context): synchronous construction/hydration only
runtime = await host.start(trusted_factory)
try:
    operation = host.submit(lambda runtime: runtime.run_durable_task(task_id))
    result = await operation.wait()  # waiter cancellation leaves the task owned
finally:
    report = await host.stop()      # a false drained/released flag needs recovery
```

The runtime.run_durable_task name above is an adapter interface example, not a
provided executor or an automatic checkpoint rebinder. The stock LiveKit host
remains unbound and mutation-denied; fabricated voice confidence is not made safe
by owning a lease. Real voice ordering/confidence and provider-specific recovery
remain separate gates.

Core regression command, from PowerShell:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_worker_supervisor.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

## Recovery host and managed credential input

The provided command is deliberately **RECOVERY_ONLY**: no provider adapter,
remote status handler, task queue or voice session is connected. It authenticates,
owns/renews a lease, projects recovered action evidence without replay, and waits
for shutdown. Its startup inventory is a record count, not a recovered-task
completion claim. Integrators supply an explicit trusted adapter to the library
after provider and workflow contracts have been reviewed.

Use a separate worker principal, not the operator's credential. First prepare the
protected local directory/journal using the
[Windows ACL setup](operator-control-console.md#windows-setup-new-local-installation).
For that existing directory, create a new worker credential offline:

```powershell
$env:CCS_JOURNAL = Join-Path $env:LOCALAPPDATA 'ChronoCortexSaga\ccs.db'
$ccsWorkerSecret = Join-Path $env:LOCALAPPDATA 'ChronoCortexSaga\worker.secret'
.\.venv\Scripts\python.exe -m control.provision --journal $env:CCS_JOURNAL --principal local-worker --permission ccs:work --no-console-access --credential-file $ccsWorkerSecret
.\.venv\Scripts\python.exe -m worker --journal $env:CCS_JOURNAL --credential-file $ccsWorkerSecret --check
.\.venv\Scripts\python.exe -m worker --journal $env:CCS_JOURNAL --credential-file $ccsWorkerSecret
```

The parked host needs ccs:work only. A later adapter additionally needs its
operation's domain grants and explicit forward/repair approvals. Provisioning a
worker with --no-console-access does not silently add ccs:observe. The check mode
actually tests startup/ownership/hydration/release; it refuses a current worker's
lease, never force-takes ownership, and makes no provider calls.

Credentials are read from a bounded regular, single-link file; malformed input,
symlinks and directories are rejected. POSIX additionally checks current-user
ownership and owner-only permissions. On Windows the administrator must protect
the directory and inherited file ACL, as in the prior runbook; this reader does
not certify Windows ACLs or a hostile filesystem. Credential bytes are not CLI
arguments, output, logs or environment variables. Missing/expired/revoked authority
fails closed. Rotation requires a controlled restart with a newly supplied
credential and fresh runtime; there is no automatic token replacement or fallback.
The journal must already exist at schema 6; this command never initializes or
migrates a deployment implicitly.

Ctrl+C schedules a graceful stop. Windows SIGBREAK and POSIX SIGTERM are mapped
to the same request. Signal handlers only schedule event-loop work, then restore
the previous handlers on exit. A Windows process kill is a hard stop and does not
run this drain protocol. A service manager must distinguish graceful control from
forced termination. CLI exit 0 requires a drained, released, non-failed stop;
failure, abandoned operations or persistence/authority loss return nonzero.

## Observability boundary

Authenticated console snapshots read lease and supervision evidence in the same
SQLite transaction. Existing lease-state fields remain compatible. The new view
shows generation, expiry, last report, bounded heartbeat freshness, reported phase,
reported active-operation count and a non-secret failure code. A manually held
lease reports NOT_OBSERVED rather than supervised readiness. Observations from
older generations cannot describe a replacement. Malformed/unknown evidence,
clock rollback, stale reports, expired leases, stopping/failed phases and failure
codes cannot report READY_REPORTED. Final stop/failure events cannot claim ready
through a conflicting summary.

READY_REPORTED means only that a current generation recently reported readiness.
It is not proof of process liveness, authoritative dispatch eligibility or provider
health. Death/stall is detected only after an observation becomes stale or a lease
expires; active counts are snapshots, not a live census of remote effects.
The console retains stale/offline labeling and never changes lifecycle evidence
or dispatches a provider operation from a health badge.

Chromium tests run the real supervisor alongside the real HTTP/SQLite console:
manual lease -> fresh reported readiness -> graceful stop and expired lease.
Existing offline, response-loss, review, XSS, keyboard and responsive tests remain.

## Independent process drills and release gate

Four process cases use an independently fsynced **SIMULATED** provider ledger:

- Exit immediately after forward apply: dispatch intent survives; generation 2
  hydrates UNKNOWN and cannot replay the original executor.
- Exit immediately after compensation apply: repair intent survives; generation
  2 projects compensation UNKNOWN and never repeats undo.
- Request stop during a forward await: a second owner is denied until drain
  retains the receipt and releases the lease.
- Reach the drain deadline, then take over after expiry: the old process keeps
  its late receipt in memory but cannot publish success; the journal keeps its
  unresolved dispatch identity and the independent ledger has one forward apply.

These are process-death and concurrency results, not real-provider, power-loss,
multi-host or production performance claims. The live voice placeholder remains
skipped. Baseline on the parent commit was 519 passing runtime cases and one
skipped placeholder; exact-head final full-suite, browser, clean-environment and
Windows/Linux CI evidence is recorded in the draft PR. Local final validation:
**595 passed, 1 skipped**, including 39 lifecycle/process cases, plus **10 passed**
in Chromium. The 76 added runtime cases cover the worker and console boundaries.
Final exact-head clean-environment and CI results are recorded in the draft PR.
No live provider assets or live voice credentials were used.

Windows CI then exposed a login-time journal-unavailable failure. Controlled
SQLite-lock tests reproduced six anonymous/malformed-cookie reads taking the
writer path (503 instead of an immediate 401). A held bootstrap response also
reproduced login being enabled before the initial session lookup finished.
Anonymous syntax rejection now occurs before any database transaction; valid
sessions still recheck authority/time and fail closed with 503 when busy. The
form stays disabled until bootstrap finishes, and its submit handler enforces
the same gate. No timeout increase, automatic login retry or weaker authorization
was introduced. A failed-script/native-form case additionally verifies that a
credential can never enter a navigation URL; the JSON-only endpoint rejects the
native POST without reflecting the secret.

Deployment is still a local-host protocol. A Windows Service/systemd/container
wrapper, external watchdog, TLS/identity deployment, full transitive dependency
lock, audit retention/capacity evaluation and backup/restore drills remain.
The next provider phase must select a documented sandbox with operation identity,
idempotency retention, authoritative status lookup and delayed-application rules,
then test real forward/status/repair uncertainty before connecting a dispatcher.
The current recovery host and UI do not claim those provider guarantees.

On regression, stop admission/dispatch, retain the current schema-6 journal,
lease generation, operation identities and security/control/effect evidence.
Use a compatible reader and reconcile current facts before any new mutation.
Never restore an older backup that discards subsequent effects, clear claims,
unconsume approvals or reopen cancelled chains as rollback machinery.

The design uses Python's documented
[shield and wait semantics](https://docs.python.org/3.13/library/asyncio-task.html),
[signal/thread limitations](https://docs.python.org/3.13/library/signal.html), and
[SQLite transaction isolation](https://sqlite.org/isolation.html). These support
the protocol choices and do not certify this implementation.
