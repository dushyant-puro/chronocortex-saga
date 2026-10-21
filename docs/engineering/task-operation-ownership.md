# Safety phase 1f: task operation generations and chain ownership

Stacked on `production/durable-action-intent` at
`b48b897f5a43aa4f6b89951ec34f51ebc411c946`. Public main remains Phase 0
until the safety stack is reviewed and merged. All exercised provider effects
remain **SIMULATED**. No **REAL PROVIDER** is enabled.

## Invariants and acceptance

| Invariant | Enforcement and regression evidence |
| --- | --- |
| Failure in one task cannot unwind another task | Explicit chain identity; no fallback from unknown action ID to a manager-wide chain. |
| Equal arguments do not collapse distinct task operations | Default task operation key derives from task ID, step index and generation; two tasks and two steps with equal payloads execute independently. |
| A generation identifies one operation | Durable unique task/step/generation slots; changing arguments or supplying a different key cannot rebind a slot. |
| Advancing generation cannot hide uncertainty | Only the next generation, after recorded ABORTED/COMPENSATED and resolved repair, can be staged. Unknown forward or repair blocks advancement. |
| Task action pointers cannot confer ownership | Dispatch, completion, reconciliation, replan and compensation check exact task, step, generation, tool and chain. Foreign pointers park without querying, copying results or undoing the foreign action. |
| Stop intent precedes repair | Closing a chain persists before unwind/cancellation; pending members abort and SQL rejects later dispatch. Late success is stale or conservatively unknown, never accepted as current success. |
| Ancestor repair cannot overtake a descendant | Local checks plus a transactional journal check reject compensation intent while later chain members remain unresolved. |
| Interrupted replan cannot manufacture completion | Persist replan intent before repair; persist generation changes before replacement staging. Process exits after repair or generation save restore parked tasks. |

Three initial regressions reproduced cross-task unwind, collisions between
distinct durable tasks with equal payloads, and global unwind from an unknown
action ID on the parent implementation. The new suite has 36 cases, including
two additional process-exit windows and offline migration/rollback tests.
The earlier durable-intent suite's seven crash windows remain in use.

## Operation identity and generation rules

A task write carries immutable `owner_task_id`, `owner_step_index`,
`owner_generation` and `chain_id`. Task generations start at zero. The default
key hashes task ID, step index and generation; the sealed argument digest
remains separate. Provider operation keys are still not propagated through a
real adapter; they remain local identity/duplicate guards.

One slot cannot accept a second action, even after known non-execution releases
the operation-key claim. A replacement uses the next generation. Neither a new
payload nor a caller-supplied key can bypass unresolved history. Independent
tasks have independent identities; they can still conflict on the same provider
resource. Resource reservations, business constraints and provider idempotency
remain the adapter/provider's responsibility.

Replanning compensates affected committed steps in reverse order. Only after
successful repair does it clear that step's old action pointer and increment
its generation. A changed pending payload first aborts the old pending action,
then advances generation and stages a new request. Any confirmation-required
replacement needs a fresh action-bound approval. An old queued correction is
ignored when its field/value no longer matches the dispatched generation.

Task checkpoints include generation counters and `replan_from`. Replan intent
is saved before any awaited repair. If recovery finds that marker, it parks the
affected steps, even when an old projection contains completed steps. A journal
action newer than the checkpoint supplies its generation and identity, not
permission to replay. Automatic completion after restart remains unsupported.

## Chain boundaries and stop behavior

Task chains use the reserved ID `task:<task_id>`. Unowned calls are isolated by
default. A caller may explicitly supply a non-task `chain_id` for a sequential
batch. Task chains follow logical step/generation order, so preparing a later
step early cannot make it an ancestor. Standalone batches follow staging order,
which must reflect dependency order. Forward dispatch waits for resolved predecessors; local and SQL guards
enforce this order. This is not a general dependency-graph scheduler. A voice batch receives
one explicit chain and stops at its first denial, uncertainty or known failure.

`abort_chain_from` requires a recorded failed write. It closes only that chain
and returns whether its safe predecessors were unwound. An applied/unknown failed
write, an unresolved descendant, a missing handler, changed journal versions or
unseen members prevents a success claim. No arbitrary failed ID can select every
committed action. After closure, a new action cannot join the chain and an old
pending action cannot dispatch. Recovered closed chains retain that boundary.

Task cancellation and known-failure unwind close the task's chain before repair.
Closure is permanent in this runtime API; a new intentional task needs a new
task ID. Cancellation still does not roll back a provider call. A late success
on a closed chain is not automatically compensated: unresolved dependency order
needs explicit reconciliation. A closure seen only at the SQL commit boundary
can conservatively latch uncertainty rather than publish current success.

Compensation attempts use the existing action owner/lock and durable attempt
protocol. Later unresolved members block ancestor repair, including members
created through another connection. Once repair is in flight or unknown,
new chain staging is blocked. A changed SQL version or storage failure stops
dispatch; it is not silently retried. Read-only/global snapshots are projections,
not a source of authorization or a distributed ownership lease.

These ownership strings come from trusted host orchestration. They do not
authenticate a user, tenant or administrative caller. Protected APIs and principal
binding remain future work; do not accept arbitrary task ownership metadata from
an untrusted client.

## Schema 2 and offline migration

Fresh journals use schema 2: `chains`, `operation_slots` and `chain_events` are
added to the schema 1 tables. Chain closure plus its event is atomic. Action
identity now includes generation and chain. SQL independently checks slots,
unresolved generations/repair, chain closure and active task projections before
dispatch, even if a manager's cache is stale.

Opening schema 1 normally fails closed. Migration requires an explicit
`SQLiteActionJournal(path, migrate=True)` call **offline, with all workers
stopped and a verified backup**. It does not establish that an old remote call
finished; preserve all unknown operations for reconciliation.

Migration preserves existing provider/operation keys and old events. It adds an
identity migration event and increments action/task versions so old version
holders cannot overwrite them. Historical task-owned actions receive successive
generations only when earlier actions are proven resolved. Unowned legacy calls
receive isolated legacy chain IDs rather than an invented shared dependency
chain. Unresolved overlapping operations in one legacy slot abort the entire
migration for manual audit. No partial schema or identity update is committed.

Use Python 3.13 in the repository environment. After stopping workers, back up
the existing journal outside the checkout before migration:

```powershell
@'
import os, sqlite3
from contextlib import closing
from pathlib import Path
p = Path(os.environ['CCS_ACTION_JOURNAL'])
if not p.is_file():
    raise SystemExit('Existing journal required')
backup = p.with_name(p.name + '.before-schema2.sqlite3')
with backup.open('xb'):
    pass  # refuse to overwrite any previous backup
with closing(sqlite3.connect(p)) as src, closing(sqlite3.connect(backup)) as dst:
    src.backup(dst)
    if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
        raise SystemExit('Backup integrity check failed; do not migrate')
print(backup)
'@ | .\.venv\Scripts\python.exe -

.\.venv\Scripts\python.exe -c "import os; from runtime.action_journal import SQLiteActionJournal; SQLiteActionJournal(os.environ['CCS_ACTION_JOURNAL'], migrate=True)"
```

Protect the backup with the same ACL/encryption requirements as the source.
Integrity checking does not validate provider state. This procedure does not
start workers, grant permissions, clear unknown outcomes or enable a provider.
Do not migrate while old writers remain active. Schema 1 readers reject schema
2; no destructive downgrade is supplied.

## Validation and remaining work

From a clean checkout with dependencies installed:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_task_operation_ownership.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe eval/adversarial_scorecard.py
```

Legacy fixtures now declare real task/step/generation ownership and explicit
chains where intended. The old chain test uses an actual aborted descendant;
it no longer treats a committed effect as safely skippable. No policy bypass or
copied production task loop was added to make the tests pass.

The legacy **SIMULATED** scorecard remains 15/20 completed trials. Its five
legacy restart trials stay parked; generation bookkeeping does not turn missing
provider evidence into successful recovery. The live-session test remains a
skipped placeholder. Top-level dependencies are pinned; transitive versions
remain unlocked. No throughput, voice latency or real-provider result is claimed.

Next: explicit worker ownership, leases and fencing. SQLite coordination is
limited to writers sharing this database and these protocols. It does not fence
a remote provider, independent databases or arbitrary host/provider clients.
Workflow-definition versioning, authenticated principals and durable approvals,
richer provider reconciliation evidence, real ASR confidence/revisions, sandbox
provider selection, independent evaluation and operations remain release blockers.

On regression, stop dispatch and preserve schema 2 journals, sidecars, IDs and
provider evidence. Keep this schema reader available. Do not clear slots/claims,
reuse generations or switch to volatile simulation as rollback. A pre-migration
backup may be restored only if no subsequent dispatch/evidence would be lost;
otherwise reconcile the current journal. Reverting code or restoring a database
does not reverse any remote effect. Do not merge this draft stack automatically.
