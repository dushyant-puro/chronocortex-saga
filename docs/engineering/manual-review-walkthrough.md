# Manual review, admission and supervised execution

**LOCAL BRANCH VERIFIED / SIMULATED.** This host connects the existing styled
console to real journal, authentication, proposal admission, approval and worker
boundaries. Its input is a host-generated synthetic transcript and its only
effects are two inserts into a separate SQLite fixture ledger. It does not
enable a microphone, call a provider, migrate an existing journal or resume a run.

## Start on Windows

Use `codex/manual-review-walkthrough`. Install the
[locked runtime environment](locked-environments.md#clean-windows-installation)
first if `.venv` is missing. In PowerShell in the repository root:

```powershell
.\scripts\start_review_walkthrough.ps1
```

If the local execution policy blocks that script, use a process-only override:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_review_walkthrough.ps1
```

The launcher verifies a protected current-user/SYSTEM ACL before provisioning
any credentials or journal. It prints a unique `CCS-Acceptance-...` directory
under local application data. Wait for `MANUAL_REVIEW_READY`, then open
**http://127.0.0.1:8877** in the browser. Use `-Port 8878` if that port is busy;
the printed URL is authoritative. `-Python`, `-StorageParent` and `-Diagnostics`
retain the original acceptance launcher's behavior and protection checks.

Do not double-click `control/static/index.html`. Its new file-opening guide
explains startup, hides the inactive console and disables credential submission.
The server supplies the styles, modules and authenticated APIs. Its strict CSP
and explicit asset allowlist remain in force.

In a second PowerShell window, replace the example with the **exact directory
printed by this run**:

```powershell
$reviewRun = 'C:\Users\YOUR_USER\AppData\Local\CCS-Acceptance-PRINTED_ID'
Set-Clipboard -Value (Get-Content -Raw -LiteralPath (Join-Path $reviewRun 'operator.secret'))
```

Paste into **Operator credential**, choose **Connect to journal**, then clear
the clipboard with `Set-Clipboard -Value ''`. Never put the credential in a URL,
chat, screenshot or commit. This is authenticated same-machine HTTP, not SSO
or authorization for remote hosting.

## Exercise the actual flow

1. Confirm **SIMULATED MANUAL REVIEW**. The journal starts with no admitted
   task, action, approval or fixture effect.
2. Choose **Review current proposal**. Inspect `SIMULATED cargo`, the workflow
   and both exact step arguments. Review alone creates no task or effect.
3. Check **I reviewed this transcript and these exact arguments.**, then accept
   the reviewed task. Review bindings expire after 60 seconds; if stale, inspect
   durable evidence and prepare a fresh review. Admission does not authorize
   execution. Finish/close the review panel and refresh journal evidence.
4. Inspect the PENDING `SIMULATED.reserve` action. Choose **Approve forward
   operation**, enter execution principal `SIMULATED-local-worker`, review the
   exact evidence, check its confirmation and choose **Record decision**.
5. Refresh. Only the first effect should be committed. The distinct
   `SIMULATED.confirm` action needs its **own** approval for the same principal.
6. Approve it, refresh the overview and selected evidence, and inspect
   **COMPLETED**, two committed actions and the consumed approval records.
   Browser acknowledgment and task completion are separate observations.

The host selects **one admitted task per new run** using durable journal order.
Additional admissions remain parked and are never selected after cancellation,
completion or uncertainty. Start a new protected run for another execution.
This bound matches the two-row fixture ledger; it is explicit in the banner.
It is not a general-purpose provider worker or a task replacement executor.

Use a separate fresh run to test cancellation while waiting for approval:
record cancellation intent and inspect the durable CANCELLED checkpoint.
Existing effects remain; cancellation is not compensation. Any lost executor
response parks the task as NEEDS_RECONCILIATION / IN_FLIGHT_UNKNOWN, without
retry or descendant dispatch. Preserve that directory for inspection.

## Ownership, retention and rollback

The ASGI lifespan constructs voice ingress, admission and controller on the
same event loop that owns the supervised runtime. An outer mount delegates to
the full protected console only after startup; requests without lifespan fail
closed. Browser text cannot manufacture trusted evidence or tool arguments.
The operator has current create/observe/approve/cancel/reconcile and simulation
domain grants; the executor has work and simulation grants. No seeder principal
admits the task on the user's behalf.

Synthetic review retention is explicitly enabled for this fixture. The normal
bounded retention, current session/grant checks, expiry and inspection-only
archive rules apply; retention is not renewed authority. No raw human voice or
provider credential is accepted by this host.

Stop the foreground server with **Ctrl+C**. Supervised drain/release must finish;
a parked/failed operation or uncertain shutdown causes an unsuccessful exit.
Preserve all run files. Existing directories are refused, never resumed or
overwritten. To roll back the product change, stop the host and use the previous
console/launcher from a separate checkout; do not lower schema headers, erase
effects or restore over newer journals. This phase adds no schema migration.

## Qualification and remaining gates

Runtime integration covers empty startup, explicit admission, two approvals,
current authority/CSRF/origin, forged browser evidence, changed binding digest,
cancellation, single selection, unknown apply outcome and fail-closed startup.
Native launcher tests verify both modes' ACL readback and module selection.
Chromium covers desktop/mobile direct-file entry and the full served manual
flow with screenshots. Both modes remain offline and compatible with the
original pre-admitted walkthrough. Exact-source results are recorded in the
phase PR and implementation ledger after qualification.

Published implementation `452783b00e461ff73257ab749a441292808ebcda`
([draft PR #43](https://github.com/dushyant-puro/chronocortex-saga/pull/43)) was
qualified on 2026-11-09 from a fresh GitHub clone using the existing audited
locked runtime/browser environments: **1,632 runtime passed, 2 skipped; 73
browser passed; both profile audits passed; SIMULATED process drill 5/5 cases,
122 checks**. The browser total comprises 71 Chromium journeys and two fixture
readiness checks. Runtime ran first; no watchdog, retry or lease budgets changed.
The skips are the POSIX directory mode check on Windows and live SDK startup
without an API key. The unchanged harness SHA-256 is
`5da8b57656fbf31ebf0d11f0a1fd42e611a4c19694917cd9178247b6bbab8edf`.
Local evidence is `results/manual-review-qualification/acceptance.json`, with
redacted JUnit and the review/completed screenshots alongside it.

To reproduce with the separately installed browser profile, run the full browser
list from `.github/workflows/tests.yml`; run ordinary runtime tests as documented
in [implementation.md](../../implementation.md#reproduce-on-windows). Targeted
checks for this phase are:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_manual_review.py tests/test_local_acceptance.py tests/test_windows_local_acceptance.py -q -p no:cacheprovider
.\.venv-browser\Scripts\python.exe -m pytest browser/manual_review_cases.py -q -p no:cacheprovider
```

These commands create isolated temporary journals. No live credentials are
needed. CI must finish independently; Linux runtime/browser and advisories had
passed and Windows jobs remained pending at the last implementation observation.

Live voice, real provider acceptance, recorded-activity replacement/reconciliation,
operations packaging, independent evaluation and consolidated security/release
qualification remain. Inherited intermittent CI findings stay open for the
planned final diagnosis; passing runs here do not establish that they are fixed.
