# Persistent review evidence delivery

Status: **LOCAL BRANCH VERIFIED** implementation; exact-source qualification is
recorded in the phase PR. Voice/tools remain **SIMULATED**. No REAL PROVIDER,
microphone, production deployment or existing user-journal migration was run.

## Delivery is inspection, not restored authority

A trusted owning-loop host can explicitly construct `ProposalAdmissionService`
with `retain_reviews=True`. The default is `False`: existing hosts do not start
persisting speech just because they upgrade. Publication reauthenticates inside
the SQL write transaction and stores the exact checkpoint/binding and voice
evidence before exposing a process-local review. Publication failure exposes no
new pending review and never admits or dispatches a task.

Schema 8 adds `proposal_deliveries` in the same journal, with original reviewer,
task/review identity and expiry bound to the payload. Evidence hashes, source
handles, transcript/token alignment and binding/workflow consistency are checked
on read and backup inspection. Hashes detect inconsistent stored data; they do
not authenticate provider truth or a hostile administrator's database edits.

`POST /api/proposals/deliveries` accepts exactly `{"review_id": null}` to list
or a review ID to inspect. It uses the exact-origin console session and current
CSRF, original principal and current creation/domain grants. Reads work without
any voice ingress, proposal controller, old browser cache or old process. No
credential, workflow, arguments or raw speech is accepted from the browser.
Other-principal, expired and insufficiently scoped details are unavailable.

Every response is `INSPECTION_ONLY`, with `admission_authorized: false` and
`execution_authorized: false`. Details also state
`current_evidence_verified: false`. Restart can reuse stream/turn counters;
stored JSON must never be injected as `VoiceIngress.latest` or an in-memory
admission capability. The UI renders exact text and arguments, with no admission
checkbox/button/callback. Old response generations cannot populate a new
principal's view. It stores no transcript/arguments/identifiers in browser storage.

An original live, session-bound review can still be admitted through its existing
current-evidence path. A correction makes that old live review inadmissible;
its archived evidence is still historical, not relabeled as current. Admitted
reviews leave the unadmitted list; an exact retained detail can show admission
evidence without implying execution. Reads never cancel, replace, replay or
dispatch tasks, and do not settle another tab's uncertain receipt. Reauthorizing
or replacing a task remains a separately designed product step.

## Capacity, expiry and privacy

Retention is globally bounded to **128 records**, each at most **256 KiB** of
canonical payload. The original review lifetime is at most five minutes and
also bounded by the creating credential/session expiry. A full store refuses
publication; there is no eviction/retry that silently discards unexpired reviews.
Admitted records can consume capacity until their expiry.

Successful authenticated reads and publications delete expired logical rows.
An unavailable detail read commits expiry cleanup before returning 404. This is
lazy cleanup, not a timer, secure erasure, disk-space bound or a deletion SLA.
No traffic means expired bytes may remain on disk. Deleted SQLite pages,
already-read browser content, backups and archived snapshots can retain speech
after expiry. Do not enable this opt-in for real voice without consent and an
approved retention/custody plan. At-rest transcript encryption, scheduled
cleanup/compaction and backup expiry are not supplied by this stage. Protect the
journal/parent directory with deployment ACLs and use the existing encrypted
backup/custody procedure for off-host retention; avoid raw payload logs/dumps.

## Schema 8, migration and rollback

New journals use schema 8. Existing v1–v7 journals require explicit offline
migration; an active lease blocks it. Stop admission/dispatch and confirm the
old process is stopped: an expired lease alone is insufficient. Preserve and
verify the current journal, claims, receipts, approvals and provider IDs first.
No existing user journal is migrated automatically by an ordinary opener.

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
print('Offline schema-8 migration completed; no dispatch started')
'@ | .\.venv\Scripts\python.exe -
Remove-Item Env:CCS_MIGRATION_JOURNAL
```

Read-only preflight/backup drills accept schemas 6, 7 and 8; schema-8 records
are bounded and validated, and a legacy header cannot hide the new table.
Bundles bind the actual snapshot schema. A pass never authorizes activation,
establishes snapshot freshness or reconciles a remote effect. Backup snapshots
now include retained speech when this feature is enabled.

On regression disable new retention/publication, stop affected admission, and
retain schema-8 journals/evidence. Use a schema-8-compatible reader/forward fix.
An old schema-7 binary must refuse this journal; reverting code is not a database
downgrade. Never lower the header, drop evidence tables, clear UNKNOWN or restore
an older snapshot as a rollback shortcut. Interrupted migration rolls back both
DDL and header; tests verify task/claim preservation and old-handle fencing.

## Reproduce and remaining work

Use the [locked environments](locked-environments.md), then:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_proposal_delivery.py tests/test_proposal_admission.py tests/test_recovery_preflight.py tests/test_journal_backup.py -q
.\.venv-browser\Scripts\python.exe -m pytest browser/console_cases.py browser/proposal_cases.py browser/inbox_cases.py browser/delivery_cases.py -q
```

Tests include opt-in/default privacy, atomic capacity/oversize refusal, fresh
process and new-session delivery, repeated voice-counter rejection for admission,
stale corrections, current authority, corruption, expiry cleanup and clock
rollback, backup validation and atomic offline migration. Browser cases use real
Chromium/HTTP/SQLite with SIMULATED voice/faults; desktop/mobile artifacts are
under ignored `results/delivery-browser/`. Earlier development failure evidence
is retained and excluded from final qualification.

Next: explicit correction/replacement and reauthorization semantics, configured
manual review-host integration, live voice/provider acceptance, operational
retention/custody and independent research/release qualification. Seven major
readiness workstreams remain open. Historical intermittent CI investigation stays
parked for final integration; its release gates are not waived.

## Qualification record

Core `391c33d56ab60a696ecba910118253f427f4ffa7`,
[draft PR #40](https://github.com/dushyant-puro/chronocortex-saga/pull/40),
qualified on 2026-11-09 from independent fresh GitHub source:

| Check | Observed result |
| --- | --- |
| Full Windows runtime suite | 1,535 passed, 2 skipped. |
| Browser suite | 55 passed: 53 Chromium journeys plus 2 fixture-readiness cases. |
| New delivery regressions | 31 runtime cases and 6 Chromium journeys, included above. One added backup schema parameter is also included. |
| Independent SIMULATED process-fault drill | 5/5 cases; 122 checks passed. Harness SHA-256 `5da8b57656fbf31ebf0d11f0a1fd42e611a4c19694917cd9178247b6bbab8edf`. |
| Runtime/browser profiles | Both dependency audits passed against fresh source. |

Existing audited locked environments were reused; this is clean source, not a
fresh environment/OS/interpreter installation. Runtime skips remain POSIX mode
and live SDK initialization without an API key. JUnit, audits, drill scorecard,
screenshots and artifact hashes are retained under ignored
`results/review-delivery-qualification/acceptance.json`. Earlier capacity error
classification and test-exception expectation failures were corrected and are
excluded from these exact-source totals. No existing user journal was migrated.

At the last core CI observation, push/PR Linux runtime/browser and advisory jobs
passed; Windows jobs were pending. Final-head CI must be observed separately.
The qualification follow-up changes documentation only. These results establish
offline behavior at this source, not real-provider/audio or production acceptance.
