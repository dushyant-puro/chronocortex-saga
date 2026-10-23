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
