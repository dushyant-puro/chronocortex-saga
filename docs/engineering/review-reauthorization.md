# Reauthorize an unadmitted retained review

Status: **LOCAL BRANCH VERIFIED** implementation; qualification is recorded in
the phase PR. Voice/tools are **SIMULATED**. No REAL PROVIDER, microphone,
deployment or existing user-journal migration was run.

## Operator flow and limits

Inspect a retained unadmitted review, then explicitly select **Review current
correction**. A configured owning-loop host prepares a fresh review from its
actual current grounded `VoiceProposal`, allowlisted workflow and trusted exact
arguments. It does not reconstruct current voice from stored JSON or accept
browser transcript/workflow/argument values. Current source identity/meaning
still needs the separate real-voice and human mapping acceptance gates.

The new review has a new review ID/digest and the **same task ID**. Its format-2
binding includes the previous review ID/digest. The operator must inspect the
new transcript and exact arguments and explicitly accept again. Preparing the
review never admits, executes, approves, cancels or reconciles work.

This flow requires explicit retention opt-in, an unexpired retained source,
the original principal and current creation/domain grants, and fresh session/
CSRF authority. Credential rotation/new sessions are allowed for that principal;
the new binding names the current reviewer/credential and session scope. The
source must still be the current durable review head when publication commits.

Any existing task checkpoint blocks this flow, including CANCELLED, COMPLETED,
FAILED and unresolved task states. Existing task operation-slot/chain evidence
also blocks it. Replacing admitted work needs separate authenticated cancellation
intent, effect reconciliation and a replacement policy; cancellation alone does
not prove that a remote side effect was undone. That path remains unimplemented.

## Atomic fencing and races

Schema 9 adds `proposal_review_fences`, a bounded per-task current review/digest
and predecessor-expiry high-water mark. Within one publication transaction,
the host reauthenticates, rechecks the predecessor and absence of task/effect
evidence, updates retained evidence and its fence, and appends a
`PROPOSAL_REAUTHORIZED` security event. Event metadata contains old/new IDs and
digests, not transcripts/arguments. Failure rolls back the update and audit.

Admission SQL checks the fence before inserting a task. An old pending object,
other journal connection or delayed POST cannot bypass that check. If original
admission commits first, successor publication fails. If succession commits
first, old admission fails. Competing successors cannot both replace the same
predecessor. Duplicate acceptance of the winning binding preserves progress and
one admission event. These are journal-local guarantees, not global provider
idempotency or semantic-objective deduplication across independently created tasks.

Fences persist through the maximum deadline of all predecessors. A shorter new
session/TTL and deletion of expired new evidence cannot unlock a still-valid
older review. New retained bindings require a matching fence; absent metadata
fails closed. Both retained rows and fences have a global 128-entry capacity.
Reauthorization updates one task slot instead of evicting unrelated identities.
Expired fence cleanup runs on publication; logical cleanup is not secure erasure.

## Retention, failure recovery and UI

Explicit succession replaces the task's retained raw evidence with the current
review. The previous raw payload is not kept as a second delivery record. Its
review/digest lineage remains in the successor binding and security event; this
is not a full raw-transcript archive. Retention remains opt-in, bounded and subject
to the [privacy/custody limitations](persistent-review-delivery.md#capacity-expiry-and-privacy).

The UI sends only predecessor review ID/digest. It checks the returned task and
predecessor identity, clears prior evidence when preparing or rejecting a
response, resets the acceptance checkbox and fences late replies across sessions.
An uncertain admission receipt disables correction preparation; this flow cannot
clear or replace that uncertainty. Lost/malformed succession acknowledgement
parks the flow without replay; preserve the task ID and explicitly reload
retained history to inspect the current review head. Preparing another review
still requires a new explicit request. No provider mutation is retried.

## Schema 9 migration and rollback

New journals use schema 9. Ordinary opens refuse schemas v1–v8. Migration is
explicit/offline: stop admission/dispatch, confirm the previous process stopped,
preserve/verify evidence, and confirm no active lease. Lease expiry alone is
not process termination or remote rollback. Schema-8 retained records are
validated and seed fences atomically; legacy task records remain unchanged.

From a qualified environment, replace only the existing journal path:

```powershell
$env:CCS_MIGRATION_JOURNAL = (Resolve-Path -LiteralPath 'C:\CCS\data\journal.db').Path
@'
import os
from pathlib import Path
from runtime.action_journal import SQLiteActionJournal
path = Path(os.environ['CCS_MIGRATION_JOURNAL'])
assert path.is_file(), 'Existing journal required'
SQLiteActionJournal(path, migrate=True)
print('Offline schema-9 migration completed; no dispatch started')
'@ | .\.venv\Scripts\python.exe -
Remove-Item Env:CCS_MIGRATION_JOURNAL
```

Read-only preflight/backup supports 6/7/8/9 and validates new fence identity,
capacity and delivery/task consistency. A legacy header cannot hide new tables
or format-2 bindings. These checks do not authorize activation, prove freshness
or reconcile provider effects. Backup custody now includes retained speech.

On regression stop affected admission/preparation, retain schema-9 journals,
claims, receipts, fences and lineage, and use a compatible reader/forward fix.
An old schema-8 binary must refuse the journal. Do not lower its header, drop
fences, clear UNKNOWN or restore an older snapshot to simulate rollback.
No existing user journal is migrated by this implementation phase.

## Reproduce and remaining work

Use the [locked environments](locked-environments.md), then:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_review_reauthorization.py tests/test_proposal_delivery.py tests/test_proposal_admission.py -q
.\.venv-browser\Scripts\python.exe -m pytest browser/console_cases.py browser/proposal_cases.py browser/inbox_cases.py browser/delivery_cases.py browser/reauthorization_cases.py -q
```

Deterministic tests cover both commit orders, competing successors, audit
rollback, shorter-expiry retention, other connections, metadata loss/corruption,
lineage validation, current authority/credential rotation, all admitted states,
unadmitted chain evidence, restore without execution and schema-8 migration.
Chromium cases cover explicit acceptance, stable identity, rejected replies,
lost acknowledgements, uncertain-admission blocking and principal changes.
Screenshots are under ignored `results/reauthorization-browser/`.

Next: policy and acceptance for replacing admitted tasks, configured manual host
integration, real voice/provider acceptance, operational retention/recovery
custody, independent evaluation and consolidated security/release qualification.
Seven workstreams remain open. Historical intermittent CI investigation remains
deferred to final integration; its release gates are not waived.
