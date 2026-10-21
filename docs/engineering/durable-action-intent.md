# Safety phase 1e: durable action intent and conservative restart

Stacked on `production/mutation-authorization` at
`5203635cf1def34bf274cebd05ad32d58cb7967c`. Public main is still the Phase 0
baseline until the safety stack is reviewed and merged. All tested provider
effects remain **SIMULATED**; no **REAL PROVIDER** has been selected or enabled.

## Invariants and acceptance criteria

| Invariant | Acceptance evidence |
| --- | --- |
| No forward executor before durable dispatch intent | An executor opens an independent connection and observes STAGED and DISPATCH_INTENT; missing journal, SQL lock and injected transaction failures prevent execution. |
| No compensation executor before durable repair intent | COMPENSATION_INTENT precedes the handler; stale CAS and crash windows cannot repeat an earlier attempt. |
| An event and its action transition are atomic | SQL triggers abort event insertion; action, version and key claim roll back together. |
| A stale writer cannot overwrite a newer record | Action and task checkpoint CAS reject obsolete versions; terminal-state regression and identity changes are rejected. |
| A lost outcome never authorizes replay | Process exits after intent, provider apply and repair apply retain identity and uncertainty; keys stay claimed. |
| Old task projections cannot establish current success | Restore reattaches task/step-owned actions even when newer than the task checkpoint; recovered completed writes become stale and tasks park. |
| Persistence failure cannot advance a task or unwind prior effects | Journal/checkpoint failures latch dispatch closed; task and voice handlers surface recovery needs without compensation of predecessors. |

`tests/test_durable_action_intent.py` contains 44 cases, including seven subprocess
crash windows. The first missing-journal regression failed against the parent
implementation before the change. Crash workers use `os._exit`, skipping Python
cleanup. A separate fsynced file models provider effects; it is independent of
the saga's action state. These tests exercise process failure, not physical power
loss, defective disk firmware or a real provider.

## Store and transaction boundaries

`SQLiteActionJournal` uses Python's standard-library SQLite binding. A new empty
database migrates transactionally from schema 0 to schema 1. The application ID,
schema version, required tables and integrity check are verified. Foreign,
corrupt, incomplete or future schemas fail closed; no silent reset/downgrade or
legacy JSON import is performed. Later schema versions require explicit migrations.

Schema 1 stores action records, durable operation-key claims, action events,
versioned task checkpoints and task events. Action identity includes its ID,
canonical arguments/digest, tool, operation key, capture epoch, process session
and optional task/step owner. Identity comparisons use canonical JSON so Python
equality cannot equate epoch `0` with boolean `False`. Write and task IDs now use
full UUIDs. Arbitrary callbacks and Python objects are never deserialized.

Transactions use `BEGIN IMMEDIATE`, rollback-journal DELETE mode and
`synchronous=EXTRA`, verified on each connection. Unsupported journal modes are
rejected rather than silently changed. Connections close before provider awaits.
Action CAS + event + any key release commit together. Task checkpoint CAS + its
event commit together. **Action and task-projection updates are separate
transactions**: recovery explicitly handles intent newer than a checkpoint.
The action journal is authoritative for whether dispatch may have occurred.

These settings follow SQLite's documented [atomic-commit mechanism](https://www.sqlite.org/atomiccommit.html)
and [synchronous guarantees](https://www.sqlite.org/pragma.html#pragma_synchronous).
Their durability depends on working filesystem locking and flush semantics.
Use a local filesystem, not a network share. SQL work is synchronous with a
bounded lock timeout; voice-loop latency and throughput have not been benchmarked.

The database contains request/result data. Protect the directory with host ACLs
and appropriate disk encryption. Keep it out of version control. Event history
is append-only through this runtime API, not tamper-proof against a database
administrator. Runtime task deletion is disabled; retention, backup/restore,
redaction and operational migration tooling remain future work. Do not copy an
active database or remove its journal sidecars as a backup strategy.

## Dispatch, uncertainty and key retention

Staging persists the sealed intent. After authorization/validation, DISPATCH_INTENT
commits before entering the executor. That marker means **may have dispatched**,
not that the provider received it. A crash immediately after the marker is
intentionally indistinguishable from a lost response after provider apply.

Returned success is recorded before chain append/task advancement. Non-JSON
receipts or a failed outcome transaction leave durable dispatch intent unresolved,
retain the local receipt for diagnosis and latch further dispatch closed. Generic
errors, timeout and cancellation persist IN_FLIGHT_UNKNOWN; trusted explicit
non-execution evidence persists ABORTED. Compensation has its own persisted
intent and outcome; unknown attempts are never reset or automatically retried.

Committed operations keep their key claim. A fresh action with the same default
payload-derived key cannot silently repeat a completed durable operation. Claims
release only after a recorded known-no-effect/compensated outcome without an
unknown repair. The default fingerprint is a conservative duplicate guard, not
a provider idempotency guarantee or complete semantic intent identifier. A
distinct intentionally repeated operation needs a distinct trusted operation
key; changing a key to retry an uncertain write is forbidden. Task-generation
identity and per-task chain scope require a later phase.
Executor signatures still receive arguments only; stable keys are not yet
propagated to a real provider header/request field. That adapter contract must
be defined and tested when a provider is selected.

Database contention, failed transactions and checkpoint write/CAS errors stop
execution. The process cannot clear the fault latch and resume writes. Preserve
the database and reopen for conservative inspection/recovery. A durable duplicate
key parks the task without undoing its prior effects. The voice entrypoint logs
storage failure separately from remote failure and does not unwind on that basis.

## Restart and task behavior

Opening the journal hydrates records only. Previously PENDING/IN_FLIGHT records
are represented as IN_FLIGHT_UNKNOWN; previously COMMITTED writes become
COMMITTED_STALE. Repair intent becomes UNKNOWN. Original action IDs, process
session, numeric capture epoch, payload, result and evidence references survive.
A restarted clock with the same number does not establish original intent validity.
Recovered actions cannot enter `commit_write`, and no compensator is auto-bound.

Trusted read-only status reconciliation can record operation-specific presence
or authoritative absence using the existing adapter contract. A positive result
does not revive conversational validity or prove an earlier repair did not run.
Absence must exclude delayed future application. CAS must succeed before repair
dispatch or key release. This phase does not implement a richer provider evidence
receipt or a universal read-after-write reconciliation algorithm.

Journaled `TaskManager` instances default to `SQLiteCheckpointStore` in the same
database; incompatible external checkpoint stores are rejected. Task creation is
persisted. On restore, callers supply matching tool/kind step definitions; shape
errors fail closed. Trusted builder/handler code still comes from the host, not
the database; full workflow-definition versioning remains required.

Task/step ownership recovers actions whose IDs never reached the task checkpoint.
Recovered write evidence parks the task and rewinds its unresolved index. A
COMPLETED checkpoint cannot unlock downstream work on its own. Caller-provided
`restore_checkpoint` is rejected in journaled mode. Legacy file checkpoints remain
available in simulation; they are not automatically promoted to provider evidence.

## Configuration and reproduction

`SpeculativeSagaManager(..., journal=SQLiteActionJournal(path))` and
`CCSAgent(journal=...)` accept the journal. Without one, mutation dispatch denies
by default. `simulation=True` explicitly permits volatile simulation and cannot
override a configured journal. Legacy state-machine tests opt into that mode;
authorization tests still use real policy checks without default grants.

For the voice entrypoint, configure a local database outside the checkout:

```powershell
$ccsData = Join-Path $env:LOCALAPPDATA 'ChronoCortex-Saga'
New-Item -ItemType Directory -Path $ccsData -Force | Out-Null
$env:CCS_ACTION_JOURNAL = Join-Path $ccsData 'actions.sqlite3'
```

This does not grant write permissions or turn simulated tools into real providers.
Authentication/operator approval remains a separate trusted integration boundary.

From a fresh checkout, Python 3.13:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m pytest tests/test_durable_action_intent.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe eval/adversarial_scorecard.py
```

The legacy **SIMULATED** scorecard remains 15/20 completed trials; five legacy
restart trials stay safely parked. New crash tests validate evidence retention
and replay prevention, not automatic task completion. The live-session test is
still a skipped placeholder. Top-level requirements are pinned; transitive
dependencies are not yet locked.

## Next stage and rollback

Next: task-scoped operation generations and chain ownership, then explicit
worker ownership/leases and fencing against stale workers. Same-file SQLite CAS
is useful local coordination, not distributed/provider fencing. Independent
databases or direct provider clients are outside this protection. Reconciliation
evidence, authenticated principals/tenants, durable approvals, real voice
confidence/revisions, documented sandbox providers and deployment operations
remain release blockers. No production-readiness or universal novelty claim is made.

On regression, stop new dispatch and preserve the database, sidecars, IDs and
provider evidence. Keep this schema reader available; do not downgrade to an
older runtime that ignores the journal, replay a JSON checkpoint, clear unknown
states, delete key claims or switch to simulation to restore a demo. Reverting
code does not reverse provider effects. Any future database migration needs a
verified backup/restore procedure before application; no automatic destructive
migration or merge of this draft stack is authorized by passing these tests.
