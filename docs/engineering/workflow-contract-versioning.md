# Safety phase 1h: pinned durable workflow contracts

Stacked on `production/worker-lease-fencing` at
`25e77900c1ce4ec6040fa4729c836038e9a5759b`. **PUBLIC MAIN VERIFIED** remains
`4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`; the safety stack remains unmerged.
All provider effects in validation are **SIMULATED**. No **REAL PROVIDER** is
selected, enabled or validated here.

Previously, durable task restoration compared only tool names and read/write
kinds. Four initial tests reproduced acceptance of changed required fields,
argument builders, compensation handlers and status handlers under those same
names. Those definitions could change the meaning of a previously recorded
task without changing its operation identity.

## Invariants and acceptance criteria

| Invariant | Evidence |
| --- | --- |
| A journaled task declares its workflow before any dispatch | Creation requires an explicit workflow ID/revision and per-step revisions. Invalid/missing identities leave no task checkpoint. Explicit volatile simulation retains its legacy API. |
| Restoration checks executable compatibility before publishing a task | Canonical workflow records include ordered unique step IDs, tool kind/name, required fields, handler presence/revisions, provider contract revision/scope and registered manifest metadata. Every component must match. |
| A checkpoint update cannot redefine its workflow | The journal verifies the digest, checks tool definitions against the contract and compares with the prior immutable contract inside checkpoint CAS. Removal, replacement and implicit legacy adoption fail atomically. |
| Cached restoration does not bypass validation | Repeated restore checks both the supplied contract and callback references; it cannot silently rebind changed handlers. |
| Runtime changes cannot authorize execution or repair | Task run, dispatch, acceptance, replan and compensation check the sealed contract. In-process callback replacement is rejected even if its revision string stays the same. |
| Await boundaries cannot unlock another definition | Contract changes during a forward call, compensation or status query park the task without advancing a generation, dispatching a descendant or automatically undoing an effect. Valid action evidence remains recorded. |
| Background saga repair obeys the task contract too | Task-owned actions carry a process-local execution guard. Epoch-triggered automatic compensation checks it before COMPENSATION_INTENT. A failed background guard is surfaced without an unobserved task exception or handler invocation. |
| A crash cannot turn deployment drift into recovery | An independent process records intent, fsyncs a simulated effect and exits. Restart under a changed executor revision is rejected; matching definitions restore UNKNOWN and do not replay the mutation. |
| Old journals are not silently assigned invented versions | Schema upgrade preserves existing evidence. A task lacking a recorded workflow contract is inspectable but cannot resume through TaskManager or acquire a new contract through checkpoint save. |

The focused suite contains 55 cases. Existing durable action/lease fixtures now
declare explicit **SIMULATED** versions through a construction-only test mixin;
production task loops, dispatch, authorization and guards remain unchanged in
those fixtures. The legacy replan crash tests now supply matching compensation
definitions on restore instead of omitting them. The schema-1 legacy task test
now verifies read-only rejection because its original workflow version is
unknown, while preserving its identity/generation evidence.

## Contract API and trust boundary

```python
from runtime.workflow_contract import WorkflowVersion, StepVersions
from runtime.task_engine import TaskStep

step = TaskStep(
    "change",
    build_args=build_change_args,
    compensate=undo_change,
    status_check=query_change_status,
    versions=StepVersions(
        step_id="apply-change",
        arguments="change-args:v1",
        executor="change-adapter:v1",
        provider="chosen-provider-contract:v1",
        provider_scope="sandbox:account-identifier:region",
        compensation="change-undo:v1",
        reconciliation="change-status:v1",
        # validator="change-validator:v1" if the manifest has validate_args
    ),
)
workflow = WorkflowVersion("change-workflow", "v1")
task = tasks.create_task("Change", [step], workflow=workflow)
# On a fresh runtime with the same registry and a valid worker lease:
restored = tasks.restore_task(task.task_id, steps=[step], workflow=workflow)
```

This is a contract example, not an implemented or selected provider. Provider
scope must identify the intended environment/account/region without secrets.
The runtime compares the declaration; it does not independently discover or
verify the provider endpoint, account or credentials. That binding belongs in
the future real-provider adapter and authenticated host integration.

Argument, executor and provider revisions are always explicit. Compensation,
reconciliation and validator revisions must be present exactly when their
corresponding step/manifest callbacks are present. Argument-builder presence is
also recorded. The registry manifest snapshot includes permissions,
confirmation, timeout, dependencies and behavioral flags; mutable lists cannot
silently alter the stored snapshot. The provider contract revision also covers
manifest-level handler semantics. Step order is significant, including two
steps invoking the same tool. Revisions are nonempty bounded strings, not
booleans or implicit numeric defaults.

These are **trusted deployment declarations**, not automatically proven
equivalence of Python code. The host must map each revision to an immutable,
reviewed implementation and retain old compatible deployments. Changing
argument-building semantics, executors, validators, compensation/reconciliation
meaning, provider scope or workflow order requires new revisions. Reusing a
revision for different code is a deployment error this cross-process comparison
cannot detect. No function-source, bytecode or closure hash is claimed to solve
that problem. Dynamic grounded values may legitimately change while the pinned
builder's semantics remain unchanged.

Within one process the engine additionally remembers callback object references
and detects their replacement. It cannot detect arbitrary changes to mutable
closure contents, monkey-patching internals, malicious host code or an adapter
that lies about its revision. Executor binding still comes from the trusted
TaskManager host implementation. The digest is SHA-256 over canonical finite
JSON for deterministic comparison and corruption detection; it is not a
signature or authentication mechanism. Database ACLs, authenticated principals,
deployment artifact attestation and code-to-version registries remain separate
release blockers.

Guards protect the task-engine execution path and its task-owned saga callbacks.
Lower-level direct saga APIs remain trusted host/operator APIs; this phase does
not convert arbitrary Python callers into authenticated workflow principals.
On process restart, executable code is supplied explicitly and no callable is
deserialized from the journal. Recovered actions retain existing conservative
reconciliation rules; matching definitions alone never prove absence or permit
an unknown forward/compensation attempt to be repeated.

On runtime mismatch the task parks with its original contract. Cancellation can
still record CANCELLED and close the chain; a changed repair handler remains
fenced. A forward effect that completed under the original staged arguments
can remain durably COMMITTED while the task parks. This records execution
evidence, not approval to continue under changed definitions. Repair that
already applied remains recorded even if a later workflow check fails; no task
generation advances from that failure.

## Migration, rollout and rollback

Fresh journals initialize schema 4. This version fence makes previous schema-3
code reject the new journal rather than ignore workflow metadata. Schema 1/2/3
upgrades require explicit `migrate=True` with workers stopped. Schema 3 also
requires its worker lease to be expired or released. Existing actions, events,
operation claims, task projections and worker generations are preserved. The
upgrade never guesses a workflow ID/revision, clears unknown effects or grants
execution authority. The migration fixtures are **SIMULATED**; no live journal
was upgraded during this phase.

Back up the stopped database using the integrity-checked, exclusive-file backup
procedure in [the ownership phase](task-operation-ownership.md), with a fresh
name such as `.before-schema4.sqlite3`. Preserve ACLs and sidecars. Then use
PowerShell from the checkout:

```powershell
.\.venv\Scripts\python.exe -c "import os; from runtime.action_journal import SQLiteActionJournal; SQLiteActionJournal(os.environ['CCS_ACTION_JOURNAL'], migrate=True)"
.\.venv\Scripts\python.exe -m pytest tests/test_workflow_contract_versioning.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe eval/adversarial_scorecard.py
```

After migration, inspect legacy tasks and keep them out of automatic execution.
An audited explicit legacy adoption/definition-migration tool is not provided.
Do not modify stored contract IDs to make a deployment pass. Retain compatible
versioned deployments for newly pinned tasks. Workflow incompatibility should
be resolved by restoring the reviewed matching deployment, without replaying
any unresolved remote attempt.

On regression, stop dispatch and preserve the schema-4 journal, worker events,
workflow contracts, request IDs and provider evidence. Keep a schema-4 reader
available. No destructive downgrade is supplied. Restore an older backup only
if no subsequent dispatch/evidence would be lost; otherwise reconcile the
current journal. Code rollback, schema rollback, cancellation or swapping a
workflow version never reverses a remote effect. Do not clear claims, reuse
generations, adopt legacy tasks implicitly or switch to simulation as rollback.

## Validation and next gate

Local Windows full suite: **390 passed, 1 skipped**. Exact-commit
fresh-checkout/CI results are recorded in the draft PR. The focused suite
passes all 55 cases. The legacy
**SIMULATED** scorecard remains 15/20 completed trials, with five restart trials
parked, zero observed duplicate executions and zero observed false commitments
in those fixture runs. These counts do not establish production rates. The
live-session test remains a skipped placeholder. No real voice, provider,
deployment, throughput or latency result is claimed. Top-level requirements
are pinned; transitive dependencies remain unlocked.

Next scoped security phase: authenticated host principals and durable,
action-bound approvals with expiry/revocation/replay protection. Then integrate
a tested lease supervisor and real voice lifecycle, choose a provider with
documented sandbox/idempotency/reconciliation guarantees, bind its live scope
to the contract, add independent evaluation and operational recovery drills.
This draft stack requires review before merge; public main still lacks these
protections.
