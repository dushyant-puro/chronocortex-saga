# Replacement of cancelled, zero-activity tasks

Status: **LOCAL BRANCH VERIFIED** implementation; phase qualification is recorded
in its PR. Voice/tools are **SIMULATED**. No REAL PROVIDER, microphone, production
deployment or existing user-journal migration was run.

## Narrow policy

This flow supports an explicitly cancelled, reviewed task with no recorded step
activity. It is not a general replacement or compensation engine. The current
principal must be the original reviewer and hold current observation, task
creation, cancellation and original/new workflow-domain grants. Delegated
replacement of another principal's task is not supplied.

The source must have a validated proposal binding, state `CANCELLED`, initial
step index, all step states `PENDING`, zero integer generations, no results,
action IDs, dispatched arguments or replan marker, and one matching CLOSED task
chain. No operation slot or action record may belong to the original task or its
chain. Even PENDING, unknown or compensated action evidence blocks this flow.
Strict typing rejects JSON `false` masquerading as numeric zero.

These checks establish **journal-local zero recorded activity**, assuming CCS's
dispatch-intent protocol and correct/current journal custody. They do not prove
the absence of out-of-band provider actions, snapshot staleness or remote effects.
Cancellation alone never authorizes replay. Tasks with any activity need a
separate reconciliation/replacement policy and real-provider acceptance; this
stage deliberately refuses them.

## Explicit operator and host flow

1. Record cancellation through the existing authenticated task decision flow.
2. Open **Replace a cancelled task with no recorded activity**, enter the original
   task ID and explicitly inspect eligibility. A read never cancels or admits.
3. Select **Review current replacement**. A configured owning-loop host uses its
   actual current grounded proposal, allowlisted workflow and trusted arguments.
   Browser arguments/transcripts/workflows are never accepted as authority.
4. Inspect the new task ID, original checkpoint ID/version/digest, transcript and
   exact new arguments. Explicitly accept again. Separate operation approvals
   remain necessary before execution.

`POST /api/proposals/replacement-status` accepts exactly `task_id` under current
session/CSRF authority and returns a protected eligibility observation. It works
without a proposal controller. `POST /api/proposals/replacement` accepts exactly
`task_id` and the observed `checkpoint_digest`; preparation requires a configured
controller and explicit retained-review opt-in. It creates an unadmitted review,
not a task, claim, executor call or automatic cancellation.

Format-3 bindings seal `replaces_task = {task_id, version, digest}`. The new task
has a different ID; the cancelled original is preserved. Reauthorizing an
unadmitted replacement candidate preserves that original descriptor. Current
source eligibility/digest/permissions are rechecked during preparation,
publication and final admission. A stale eligible response is never authority.

## Atomic claim and recovery invariants

Schema 10 adds `task_replacements`: one original task can have only one admitted
replacement, and each replacement has one original. Admission inserts the new
checkpoint, permanent claim and metadata-only replacement/admission audit in the
same transaction. Competing prepared candidates, including separate journal
connections, cannot both win. Duplicate acceptance of the winning binding
preserves progress and the existing claim. Missing/mismatched claims fail closed.

The original checkpoint cannot be written through the journal after the claim;
its chain stays closed. Original history, current checkpoint bytes and claimed
version/digest must remain consistent. Source history must precede the new task's
first event, preventing cyclic replacement lineage. Claims are permanent audit/
idempotency records, not expiring retained speech. Disk/retention capacity remains
an operations gate; there is no claim eviction or reset API.

An uncertain original/replacement admission disables new preparation in that
browser context. Another open review must be closed first. Lost preparation
acknowledgement never automatically retries or admits: preserve the original ID
and inspect retained reviews. Lost admission acknowledgement retains the new task
identity; explicit receipt readback recovers that same task and one claim.
No mutation or provider call is retried. Late eligibility/preparation responses
cannot populate a different principal's screen. All untrusted content is text.

## Schema 10 migration and rollback

New journals use schema 10. Existing v1–v9 journals require explicit offline
migration; normal opens refuse them. Stop admission/dispatch, confirm the old
process stopped and preserve/verify journal, approvals, receipts, claims and
provider IDs. Active leases block migration; expiry alone is not process
termination or provider rollback. Existing records are preserved and migration
does not create replacement claims.

From a qualified checkout/environment, replace only the existing journal path:

```powershell
$env:CCS_MIGRATION_JOURNAL = (Resolve-Path -LiteralPath 'C:\CCS\data\journal.db').Path
@'
import os
from pathlib import Path
from runtime.action_journal import SQLiteActionJournal
path = Path(os.environ['CCS_MIGRATION_JOURNAL'])
assert path.is_file(), 'Existing journal required'
SQLiteActionJournal(path, migrate=True)
print('Offline schema-10 migration completed; no dispatch started')
'@ | .\.venv\Scripts\python.exe -
Remove-Item Env:CCS_MIGRATION_JOURNAL
```

Backup/preflight supports legacy 6/7/8/9 and new 10, validates claim/origin/history/
replacement consistency and rejects legacy headers hiding format-3 semantics.
Snapshots may include retained speech. A pass does not authorize activation,
prove freshness or reconcile a provider. Keep the existing protected/encrypted
custody plan and retained-review privacy limits.

On regression stop affected preparation/admission, preserve schema-10 evidence
and use a compatible reader/forward fix. Old schema-9 binaries must refuse the
journal. Do not lower headers, delete claims, reopen original chains, clear
UNKNOWN or restore an old snapshot as a rollback shortcut. No existing user
journal is migrated during implementation/qualification.

## Reproduce and remaining work

Use the [locked environments](locked-environments.md), then:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_task_replacement.py tests/test_review_reauthorization.py tests/test_recovery_preflight.py tests/test_journal_backup.py -q
.\.venv-browser\Scripts\python.exe -m pytest browser/console_cases.py browser/proposal_cases.py browser/inbox_cases.py browser/delivery_cases.py browser/reauthorization_cases.py browser/replacement_cases.py -q
```

Tests include real console cancellation, zero-activity/strict-type guards, any
owned action (including corrupted slot metadata), two-connection races, changed
source at publication/admission, audit rollback, claim corruption, credential
rotation, reauthorization/restore, atomic migration and backup checks. Chromium
cases cover fresh explicit acceptance, lost preparation/admission replies,
identity mismatch, unknown-admission blocking, stale source and principal changes.
Voice/tools/fault injection remain SIMULATED; desktop/mobile artifacts are under
ignored `results/replacement-browser/`.

Next: configured manual review-host integration; any extension to tasks with
recorded effects needs separate authoritative reconciliation and provider-specific
acceptance. Real voice/provider, deployment/retention/recovery custody, independent
research and consolidated security/release qualification remain open across seven
workstreams. Historical/native/CI findings remain deferred release gates.

## Qualification record

Core `902f3a8d3592fc6b254322b7c9e489558c9e662c`,
[draft PR #42](https://github.com/dushyant-puro/chronocortex-saga/pull/42),
qualified on 2026-11-09 from independent fresh GitHub source:

| Check | Observed result |
| --- | --- |
| Full Windows runtime | 1,621 passed, 2 skipped. |
| Full browser suite | 70 passed: 68 Chromium journeys plus 2 fixture-readiness cases. |
| New replacement regressions | 47 runtime cases and 9 Chromium journeys, included above; one extra backup-schema parameter is also included. |
| Independent SIMULATED fault drill | 5/5 cases, 122 checks. Harness SHA-256 `5da8b57656fbf31ebf0d11f0a1fd42e611a4c19694917cd9178247b6bbab8edf`. |
| Runtime/browser profiles | Both audits passed against fresh source. |

Existing audited locked environments were reused, not newly installed. Runtime
skips remain POSIX mode on Windows and live SDK initialization without a key.
The native full suite completed before browser/fault-drill qualification; no
policies/budgets changed, and this scheduling is not a diagnosis of prior failures.
JUnit (synthetic credential values redacted if present), audits, screenshots,
drill, hashes and CI snapshot are retained under ignored
`results/task-replacement-qualification/acceptance.json`. One development fixture
deleted a chain before its dependent event; its 1-failure/36-pass run is excluded
from exact-source totals. The corrected fixture and new cases passed above.

Local offline passes do not close the inherited watchdog/native/CI gates or
establish real provider/voice/production acceptance. `release_qualified` remains
false. Implementation-head Linux runtime/browser and advisory CI jobs passed at
the last observation, as did push Windows browser; Windows runtime and PR Windows
browser jobs were pending. Final-head CI has separate
outcomes. The record follow-up changes documentation only. No existing user
journal was migrated or real provider contacted.
