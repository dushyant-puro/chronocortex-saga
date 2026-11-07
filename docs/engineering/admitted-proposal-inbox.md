# Admitted proposal inbox and cross-tab discovery

Status: **LOCAL BRANCH VERIFIED** implementation. Voice/tools in qualification
are **SIMULATED**; no REAL PROVIDER or microphone acceptance. This is read-only
admitted history, not persistent delivery of pending reviews or a release gate.

## Authority and data flow

An authenticated operator selects **Load admitted proposals**. The console calls
`POST /api/proposals/inbox` with exactly `{"before": null}` and the current
same-origin session/CSRF header. No principal selector, credential, transcript,
workflow or arguments are accepted. The endpoint works without a proposal
controller or voice ingress. It reads existing schema-7 task checkpoints; no
new database or admission/event write is needed.

Each read authenticates and filters within one journal transaction. Only
validated proposal bindings whose original reviewer is the current principal
and whose creation/domain/current-task-effect permissions are still granted
are returned. The same predicate/validation helpers protect exact receipt reads.
Rotated credentials for the same principal can discover history. Existing general
task observation remains unchanged and can have a broader permission scope.

A row contains only name, task/review/digest identity, historical `ADMITTED`
state and `execution_authorized: false`. No raw speech, arguments or credentials
are returned. **Confirm receipt** explicitly rechecks that exact identity using
the durable receipt API. Neither operation admits, dispatches, approves, retries
or reconciles an effect. Admission history does not describe current task state
or establish execution/external-effect success.

The browser renders untrusted names/IDs as text, validates response scope,
identity and authority flags, and clears rows on failed/malformed reads. Session
generation fences discard delayed inbox/receipt replies across disconnect or
principal changes. No inbox state is written to browser storage. Loading this
history does not clear another panel/tab's retained uncertain receipt, resolve
an unknown admission, or infer that creating another objective is safe.

## Bounded pagination and completeness

Each page scans at most 200 checkpoints plus one lookahead row, newest first,
using SQLite rowid keyset positions. The opaque-to-operators decimal string
`next_before` is a scan cursor, never a capability or proof of completeness.
The API validates its type/range and binds it as SQL data. **Older journal
records** explicitly requests that cursor; no unbounded auto-pagination runs.
Rows hidden by authority or lacking proposal provenance still consume scan
positions, so a page may be empty with older matches remaining. The UI says so.

Each page rechecks current authority/evidence. This is not a frozen snapshot:
later admissions appear on explicit refresh; task/effect permissions can change
between pages. No totals, delivery acknowledgements or unread counts are claimed.
Corrupt provenance in a scanned page fails the entire read rather than silently
reporting a complete result. Exact receipt/task inspection remains independent.
The existing task-effect permission scan/SQLite contention policy is unchanged;
the bounded checkpoint window is not a latency/SLO guarantee.

## Remaining product acceptance

Pending proposal review is still process/session-local. This stage does not
persist unadmitted transcripts, redeliver a stale review, restore an old review's
authority, implement notifications/read acknowledgements or replace/cancel a
task after a speech correction. It does not deduplicate semantic objectives
across tabs. Persistent pending delivery needs retention/capacity/expiry and
explicit correction/replacement semantics with separately reviewed admission.
The configured manual review-host walkthrough remains next integration work;
the canned launcher is unchanged. Seven production workstreams remain open.

## Reproduce and rollback

Use the [locked environments](locked-environments.md), then:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_admission_inbox.py tests/test_admission_receipts.py -q
.\.venv-browser\Scripts\python.exe -m pytest browser/console_cases.py browser/proposal_cases.py browser/inbox_cases.py -q
```

Tests cover bounded/empty filtered pages, new inserts between pages, reopened
SQLite and new sessions without receipt cache, current principal/grants/CSRF,
cursor injection/overflow, corrupt/legacy evidence, new-tab discovery after
lost acknowledgement, mismatched replies, stale responses after principal
changes, text safety/mobile layout and zero replay/dispatch. API/SQLite paths
are real; voice/tools and injected HTTP pagination/fault responses are SIMULATED.
Screenshots are retained under ignored `results/inbox-browser/`.

Rollback is a code revert preserving journals, task/events, UNKNOWN effects and
retained receipt IDs. No schema/dependency/worker/provider policy changes.
Historical intermittent CI diagnosis remains parked for final integration
without waiving its release gates. Exact-source results belong to this phase PR.
