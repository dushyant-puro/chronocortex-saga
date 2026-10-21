# Safety phase 1g: exclusive local worker execution authority

Stacked on `production/task-operation-ownership` at
`a2b7e129c39921c91591b3b778103b10ccf9e463`. That phase was already committed,
pushed and clean before this branch began. **PUBLIC MAIN VERIFIED** remains
`4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`; this safety stack is unmerged.
All external effects in these tests are **SIMULATED**. No **REAL PROVIDER** is
selected, enabled or fenced by this implementation.

## Boundary and invariants

The journal now requires explicit worker ownership for every action creation,
action transition, chain closure and task checkpoint write. One database has
one writer lease, deliberately keeping coordination simple. Multiple tasks can
run under that worker. Independent journal objects can share a trusted worker
capability with `bind_lease`; they do not become independent owners.

| Invariant | Acceptance evidence |
| --- | --- |
| No mutation authority by merely opening the journal | Unbound connections cannot stage actions, save checkpoints, close chains, renew or release ownership. Investigation reads remain available. |
| Only the current unexpired generation can mutate execution state | Ownership and expiry are checked after BEGIN IMMEDIATE, in the same transaction as action/task/chain writes. Old generations cannot release keys, close chains or publish task success. |
| Expired ownership cannot be renewed or silently reacquired by an existing runtime | Renewal requires a live token; each journal object binds once. Takeover uses a fresh object and a strictly larger persisted generation, even if the owner name repeats. |
| Lease loss during a remote await never means rollback | Forward success after expiry/takeover fails to publish COMMITTED, retains its receipt in memory and parks locally. The durable dispatch intent remains unresolved. No automatic undo occurs. |
| Repair ambiguity cannot authorize another attempt | Lease loss after simulated undo leaves durable compensation IN_FLIGHT and local UNKNOWN. Repeated repair does not call the handler again. |
| Task and status-query paths obey the same boundary | Task completion after lease loss parks; a status result returned after expiry cannot authorize automatic repair. |
| Worker attribution commits with the mutation | Worker owner/generation, operation kind, subject and observed time append to worker_events in the same transaction. An injected audit insert failure rolls back action and key creation. |
| Restart/takeover cannot establish remote absence | A separate process applies an fsynced simulated effect and exits or returns late. The replacement acquires generation 2 and hydrates UNKNOWN without replay or an automatically rebound compensator. |
| Detected clock rollback fails closed | A persisted time high-water mark rejects backward time. Denied lease checks persist their clock observation before raising, without action/task changes, so observed expiry cannot be undone by rolling the clock backward. |

The first test on the parent implementation reproduced unowned staging. Three
other initial red cases demonstrated missing lease/clock APIs; they were not
claimed as executions of a pre-existing takeover implementation. The completed
focused suite contains 40 cases, including independent connections and two
separate-process scenarios. The existing action/chain tests deliberately share
one explicit **SIMULATED** worker capability, preserving their original CAS and
chain-boundary purpose. New ownership tests use independent capabilities.

## Lease protocol and limits

`acquire_lease(owner, ttl_ms=30000)` atomically grants authority only when no
live lease exists. TTL must be an integer from 1 through 86400000 milliseconds.
There is no force-takeover API. Equality with the expiry time means expired.
`renew_lease` extends a live generation; `release_lease` ends authority without
ending, cancelling or reversing any remote operation. A released generation
cannot be reused. Acquisition, binding, renewal, release and execution writes
are attributed in `worker_events()`.

The host must acquire before constructing its running saga/task manager, renew
well before expiry, and stop dispatch when renewal fails. Reopening after loss
means constructing a fresh runtime from the journal. Never attach the new
generation to cached tasks/actions from the previous worker. The API prevents
rebinding a journal instance; trusted host code must also respect this runtime
lifecycle. `WorkerLease` is a trusted in-process capability, not authenticated
identity, an access-control boundary against malicious Python code, or a token
that a provider validates. Protect database and host access separately.

The default clock is system Unix time in integer milliseconds. All workers on
this supported local host must share a trustworthy clock. Forward jumps may
expire authority early; detected backward jumps stop writes until the clock
recovers. The high-water mark detects rollback relative to durable observations,
not arbitrary undetected clock manipulation, old database snapshot restoration,
or a hostile machine. `clock_ms` injection exists for deterministic tests, not
for supplying different clock domains to production workers.

SQLite DELETE journaling, synchronous EXTRA, reliable local disk locking/flush
semantics and one shared database remain assumptions. Network shares,
independent databases, multiple hosts with different clocks, power-loss hardware
guarantees and hostile SQL writers have not been validated. These are local
execution leases, not a general distributed consensus protocol.

A process may pause immediately after its dispatch intent commits and send the
request after ownership expires. This lease cannot fence that provider request.
The replacement must preserve the unresolved operation/key and use the selected
provider's authoritative reconciliation protocol before retry or repair. Losing
a lease is never evidence that a forward call or compensation did not apply.

After a late successful return is fenced, the old process retains the receipt
in memory, but cannot update the action journal with it under expired authority.
If it crashes, that receipt may be lost; the durable dispatch marker and retained
operation identity remain the recovery authority. A future authenticated,
append-only observation inbox could preserve late receipts without allowing
stale execution-state writes. This phase does not claim that capability.

## Host use and compatibility

The trusted application host supplies ownership explicitly:

```python
from uuid import uuid4
from runtime.action_journal import SQLiteActionJournal

journal = SQLiteActionJournal(journal_path)
lease = journal.acquire_lease(f"worker:{uuid4()}", ttl_ms=30000)
# Construct the saga/task manager with this journal and explicit authorization.
# The host supervisor renews before expiry (for example every 10 seconds).
journal.renew_lease(ttl_ms=30000)
# Stop dispatch and account for outstanding calls before graceful shutdown.
journal.release_lease()
```

This is an API example, not a production supervisor. No automatic background
renewal, watchdog or takeover loop is introduced. The existing LiveKit
entrypoint opens an unbound journal and therefore fails closed for mutation;
merely setting CCS_ACTION_JOURNAL does not grant execution ownership. Wiring a
tested host/session lifetime and shutdown protocol remains an operations and
voice-integration release blocker. Default mutation authorization also remains
denied. Explicit volatile simulation remains available for simulated tests; it
cannot bypass a configured journal or establish provider guarantees.

## Schema 3 migration and rollback

Fresh databases initialize schema 3. Opening schema 1 or 2 normally fails;
`migrate=True` is an explicit **offline** upgrade. Schema 1 first applies the
previous guarded identity migration. Schema 2 adds lease/audit tables without
rewriting actions, events, claims, generations or task projections. Migration
does not grant a worker lease, clear uncertainty or enable execution. Injected
migration failure rolls back the schema change. Only simulated fixtures have
been migrated during validation; no live journal was migrated.

Stop all old writers before migration. Use the backup/integrity-check procedure
in [the prior phase](task-operation-ownership.md), choosing a new exclusive
backup name such as `.before-schema3.sqlite3`. Then, from the repository on
PowerShell:

```powershell
.\.venv\Scripts\python.exe -c "import os; from runtime.action_journal import SQLiteActionJournal; SQLiteActionJournal(os.environ['CCS_ACTION_JOURNAL'], migrate=True)"
.\.venv\Scripts\python.exe -m pytest tests/test_worker_lease_fencing.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe eval/adversarial_scorecard.py
```

On regression, stop dispatch, preserve the schema 3 journal, sidecars, keys,
generations, worker events and provider evidence. Keep a schema 3 reader
available. Older schema 2 code rejects the upgraded journal. No destructive
downgrade is supplied; restoring an old backup is permitted only if no later
dispatch/evidence would be lost. Otherwise reconcile the current journal.
Reverting code, releasing ownership or killing a worker does not reverse remote
effects. Do not clear claims/leases or switch to simulation as a rollback.

## Validation and next gate

Local Windows validation: **335 passed, 1 skipped**, including all 40 focused
lease cases. The existing **SIMULATED** scorecard reports 15/20 completed trials,
with five legacy restart trials parked, zero observed duplicate executions and
zero observed false commitments in those trials. These are observed fixture
counts, not measured production rates. Fresh-checkout verification and CI are
recorded against the exact commit in the draft PR.

The legacy **SIMULATED** scorecard must keep reporting incomplete restart
trials honestly. The skipped live-session placeholder is not voice validation.
Top-level dependencies are pinned; transitive dependencies remain unlocked.
No throughput, latency, deployment or real-provider result is claimed.

Next scoped phase: version durable workflow definitions so a restarted task
cannot attach changed argument builders, compensation semantics or provider
contracts merely because tool names match. Follow with authenticated host
identity/durable approvals, a supervised worker/voice lifecycle, provider
sandbox selection and authoritative reconciliation, independent evaluation,
dependency reproducibility and operational recovery drills. This draft stack
must be reviewed before merge; public main does not acquire these protections
until then.
