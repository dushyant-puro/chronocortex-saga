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

## Qualification record — partial, release gates open

Implementation `38be355e06c00bd500b1449345536d6872fb4ca3`, following core
`e0343c0`, [draft PR #41](https://github.com/dushyant-puro/chronocortex-saga/pull/41).
Independent fresh GitHub source observed on 2026-11-09:

| Check | Observed result |
| --- | --- |
| Full Windows runtime | 1,572 passed, 1 failed, 2 skipped. **Not a full-suite pass.** |
| New reauthorization/recovery cases | All 37 passed; included in the runtime total. One extra schema-backup parameter is also included. |
| Local browser suite | 61 passed: 59 Chromium journeys plus 2 fixture-readiness cases. Six new journeys are included. |
| Independent SIMULATED process-fault drill | 5/5 cases, 122 checks; unchanged harness SHA-256 `5da8b57656fbf31ebf0d11f0a1fd42e611a4c19694917cd9178247b6bbab8edf`. |
| Runtime/browser profiles | Both audits passed; existing audited locked environments reused, not newly installed. |

The runtime failure is the existing
`test_watchdog_dump_does_not_cancel_work_or_replace_final_test_status[False]`:
its synthetic child returned `3221225477`, expected `0`. Root cause is unconfirmed.
No watchdog policy, timeout, native acceptance check or retry was weakened.
The two skips remain POSIX mode on Windows and live SDK initialization without
a key. The earlier `e0343c0` full run passed 1,569 tests with 2 skips; that result
is not substituted for the updated implementation's failed full-run verdict.

Exact-source hosted CI also retained a
[Linux push browser failure](https://github.com/dushyant-puro/chronocortex-saga/actions/runs/42579533155/job/129034724529):
the existing delayed durable-receipt test timed out filling a hidden login field,
with a `SQLITE_BUSY` console observation (60 passed, 1 failed). Its cause is
unconfirmed. The corresponding PR Linux browser and both Windows browser jobs
passed; Linux runtime/advisory jobs passed. Windows runtime jobs were pending
at the last observation. Passing counterparts do not erase failed evidence.

JUnit, audits, screenshots, drill scorecard and hashes are retained in ignored
`results/reauthorization-qualification/acceptance.json`, with redacted Linux
failure logs/CI snapshot alongside it. Development failures (incorrect test-hook
instance, hidden old evidence after rejection, malformed legacy-binding parser
and an initial nested test transaction) were corrected and retained separately.
The parser defect was reproduced with four cases before the fix; those cases
now return sanitized failure verdicts without changing database bytes.

The record follow-up changes documentation only. This phase implements the
bounded feature but does not close runtime/CI reliability or release acceptance.
Preserve these findings for the deferred final-integration diagnosis; seven
workstreams remain open. No existing user journal or real provider was changed.
