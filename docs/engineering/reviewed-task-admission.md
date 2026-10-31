# Authenticated proposal review and durable task admission

Status: **LOCAL BRANCH VERIFIED** only after the qualification recorded in this
branch's PR. Voice samples and external effects in this phase are **SIMULATED**.
No **REAL PROVIDER** or microphone acceptance is claimed. This is a trusted local
host API, not the completed browser product flow or a production release.

## Protected boundary

`VoiceIngress -> explicit credential-authenticated review -> explicit accept ->
SQLite initial task checkpoint -> explicit deployment restore -> existing task /
saga / authorization / reconciliation path`.

`VoiceIngress` still cannot create work, approve an operation, cancel a task or
call a tool. `ProposalAdmissionService` accepts only the actual current grounded
proposal object from its configured ingress on the owning thread. Copies,
superseded turns, disconnected streams and unavailable confidence/timing fail
closed. The source label is a trusted adapter association; it does **not** prove
speaker identity or confer a participant's authority. Queued or undelivered
provider events cannot be fenced by a local revision counter.

The deployment allowlists versioned `AdmissionTemplate` workflows with static
per-step JSON arguments. Lazy builders and dependence on mutable voice fields
are rejected. Arguments are supplied for human review, not extracted or
semantically verified by this API. Acoustic grounding cannot establish that the
structured destination, amount or other fields express the speaker's intent.
The eventual UI must show the transcript, mapping and exact deployment identity
before exposing explicit acceptance. Saying “yes” is not acceptance.

Both review and acceptance require a real local bearer credential with
`ccs:task:create` and all forward tool permissions in the pinned workflow.
Acceptance reauthenticates inside the same SQLite transaction that inserts the
task, its checkpoint event and a payload-free `TASK_ADMITTED` security event.
The exact principal, principal version and credential ID must still match.
Revocation, expiry, backward clocks or changed grants deny admission.

The binding seals task identity/name, reviewed arguments, workflow digest,
source stream/turn/revision/evidence digest, reviewer and review lifetime.
Runtime handler replacement is checked in process even with unchanged declared
versions. Version declarations remain deployment assertions, not code hashes.

Admission creates **PENDING** work only. It neither registers a running task nor
issues/consumes operation approvals. A trusted worker must explicitly restore
the matching deployment and schedule it through `TaskManager`. Existing worker
credentials, leases, operation-bound approvals, workflow checks, UNKNOWN handling
and compensation/reconciliation rules still apply. Admission is not permission
to bypass any of those checks.

## Trusted host integration

These calls belong in trusted host code after normal journal/credential and
registered executor setup, never in a model tool or a transcript callback:

```python
from runtime.proposal_admission import AdmissionTemplate, ProposalAdmissionService

template = AdmissionTemplate("Reviewed workflow", workflow_version, tuple(static_steps))
admissions = ProposalAdmissionService(
    task_manager, voice_ingress, [template], source_id="configured-adapter-label"
)
review = admissions.review(
    voice_ingress.latest, workflow_version.workflow_id, per_step_json_arguments,
    token=reviewer_credential.token,
)
# Present the current transcript plus review.details() through a protected UI.
# Only its explicit authenticated acceptance handler may perform the next call.
receipt = admissions.admit(review.review_id, token=reviewer_credential.token)
# Return receipt.task_id and receipt.binding_digest as the durable receipt.
# A separate trusted scheduling decision may restore; this still does not run:
task = task_manager.restore_task(
    receipt.task_id, steps=static_steps, workflow=workflow_version
)
```

No production endpoint, browser route, scheduler or live voice hook invokes this
API automatically. Existing trusted host `create_task` remains available for
non-voice work. Direct Python/SQL access is an administrative trust boundary;
these classes do not isolate malicious code in the host process.

## Idempotency, correction and failure

One review allocates one task ID before any write. Repeating acceptance with
that same valid review preserves current task progress and cannot insert a
second task or reset its checkpoint. Independent connections serialize through
SQLite. A different binding cannot take over an existing task ID.

There is no automatic retry. An exception after commit may mean the task exists:
inspect the allocated task ID and binding digest. Do not manufacture a new
review as a recovery mechanism. This insertion idempotency is not remote-provider
idempotency or compensation. Admitted tasks retain existing effect uncertainty
rules, including an applied-but-lost response and conservative parking.

Reviews are bounded to 128 pending entries per service, expire within five
minutes (default one minute, further limited by credential expiry), and are not
restored after process death. The same ingress/service must remain alive until
acceptance. New reviews purge expired entries. Arguments have at most 32 step
objects, 4,096 nodes, depth 32, a 65,536-character aggregate string/key budget,
65,536 canonical bytes, finite numbers and integers of at most 4,096 bits. Exact
JSON types are required; tuples and non-string keys are rejected.

A new turn or disconnect invalidates a pending review. After admission, voice
corrections and grounding-field evictions cannot rewrite or compensate its
immutable plan. Use explicit authenticated task cancellation; a replacement
task requires a new review and careful handling of any unresolved original
effect. Cancelling a coroutine or revoking the reviewer's credential does not
undo an effect. Review expiry governs admission, not the lifetime of the task.

## Privacy and persistence

Pending review data remains in memory. Durable provenance omits raw transcript,
word timings and confidence; it keeps an unsalted SHA-256 evidence digest, local
source identifiers, reviewer identity and exact reviewed arguments. A digest
is neither encryption nor proof of provider truth and may permit guessing
low-entropy content. Task/checkpoint event arguments can contain sensitive data.
Protect the journal, snapshots and audit retention accordingly. This phase does
not supply consent, transcript retention, remote login or a retention service.
Credentials are never part of the binding or security event. Review details
must not be copied into general logs or exposed to an unauthorized viewer.

## Schema 7, migration and rollback

Schema 7 fences older workers that would ignore reviewed argument provenance.
The SQL table layout is unchanged; the version bump protects checkpoint semantics.
All existing v1–v6 migrations remain explicit and offline. A current lease blocks
migration. Stop admission/dispatch and confirm the old process has stopped;
lease expiry alone does not prove that. Preserve journals, claims, receipts,
approvals and external operation IDs before upgrading. Do not reset clocks.

After preserving and checking a snapshot, the offline migration can be invoked
from the qualified environment in PowerShell (replace the existing journal path):

```powershell
$env:CCS_MIGRATION_JOURNAL = (Resolve-Path -LiteralPath 'C:\CCS\data\journal.db').Path
@'
import os
from pathlib import Path
from runtime.action_journal import SQLiteActionJournal
path = Path(os.environ['CCS_MIGRATION_JOURNAL'])
assert path.is_file(), 'Existing journal required'
SQLiteActionJournal(path, migrate=True)
print('Offline journal migration completed; no dispatch started')
'@ | .\.venv\Scripts\python.exe -
```

Read-only preflight and memory backup drills accept both schema 6 and 7; bundles
record the actual schema and reject manifest/snapshot disagreement. Their pass
does not authorize activation or establish freshness/provider reconciliation.

On a regression, stop admission and dispatch and retain the schema-7 evidence.
Use a schema-7-compatible reader/forward fix. Do **not** lower `user_version`,
run a schema-6 worker against admitted work or restore an old snapshot as a
code-rollback shortcut. Old data cannot roll back remote effects. No in-place
downgrade or production recovery activation is implemented here.

## Qualification and outstanding work

`tests/test_proposal_admission.py` exercises real local authentication, SQLite
transactions, competing connections and the production task/saga loop with
synthetic voice and simulated effects. It includes stale/cross-principal review,
credential drift, immutable arguments/provenance, transaction rollback, lost
admission acknowledgement, separate operation approval, UNKNOWN no-retry,
restart, capacity, migration and legacy/new backup evidence cases.

Run in PowerShell from the checkout with its locked environment:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_proposal_admission.py
.\.venv\Scripts\python.exe -m pytest -q --junitxml=results/runtime-tests.xml
```

Exact commit/environment/clean-checkout and CI evidence belongs in this phase's
PR. Still open: protected transcript-to-arguments review UI/API, durable proposal
inbox and cross-process delivery, identity/session association, consent/retention,
dependent/result-derived plans, explicit correction/replacement UX, live voice,
real provider acceptance, deployment and release qualification. No universal
novelty, latency, throughput or production-readiness claim follows from these tests.
