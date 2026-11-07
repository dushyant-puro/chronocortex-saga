# Protected proposal-review UI

Status: opt-in product UI over the existing session/admission API. Qualification
voice/effects are **SIMULATED**. No live microphone/provider is configured by
this feature, and the existing canned local acceptance launcher is unchanged.

## Operator flow

A trusted owning-loop host wires `ProposalReviewController` into `create_app`;
see [the API contract](session-proposal-review.md). Authenticated snapshots now
include a capability boolean. The console shows **Review current proposal**
only when that host is configured and the session has task-creation permission.
Visibility confers no authority: the server still checks every workflow grant,
CSRF, session/credential version, current evidence and expiry inside admission.
Unconfigured hosts keep their existing operator flow.

The reviewer sees the actual protected transcript, exact per-step arguments,
task ID, source adapter/turn/revision, expiry, reviewer and binding/workflow
digests. Source labels and acoustic grounding do not verify speaker identity
or prove that arguments follow from the transcript. All untrusted values are
rendered as text, including HTML-shaped argument strings.

Admission requires checking **I reviewed this transcript and these exact
arguments** and explicitly selecting **Admit this task**. Only the review ID
and binding digest are submitted. No browser arguments/workflow/transcript are
accepted as task authority. A positive receipt must match the reviewed task ID
and digest. Admission never restores a task, issues an operation approval or
calls an executor. Separate execution approvals remain necessary.

## Receipt recovery and privacy

Before sending admission, the UI stores and reads back only three identifiers:
review ID, task ID and binding digest. Storage is tab-scoped `sessionStorage`,
keyed by authenticated principal. It contains no transcript, arguments, bearer
credential, session cookie or CSRF token. Browser identifiers are hints for
protected lookup, not journal authority. If storage cannot be verified, the
admission request is not sent.

An unparseable/mismatched/lost receipt preserves those IDs and parks admission.
Reloading the tab restores only the IDs, with no trusted admitted state. The
operator must explicitly **Read admission status**. This calls the protected
status endpoint with the same identity; it never resends admission. A positive
authoritative status resolves the receipt; the operator can then close/clear it.

A negative status does not prove a previously delayed admission request can
never arrive. It therefore does not re-enable admission or create another task.
Corrupt retained metadata also parks the flow. There is no automatic retry,
replacement proposal or authority inferred from the cache.
Unconfirmed, stale and storage-failure messages use warning styling and an
accessible alert; confirmed admission uses a separate positive status message.

Disconnect clears private review content and invalidates outstanding UI
responses. A delayed response cannot populate another principal's screen.
Receipt identifiers remain in the original principal's tab slot for recovery;
they are not shown on a different principal's UI. Closing the tab removes tab
storage. Copy the displayed task ID for independent journal inspection.

## Remaining product work

This cache is **not a durable inbox**. Host restart loses process-local reviews;
new sessions cannot read a review bound to an old session. The subsequent
[durable receipt phase](durable-admission-receipts.md) reads positive admission
evidence without that old review, using retained IDs and current authority.
If status is
unavailable, preserve the ID and inspect that exact durable task. An unresolved
receipt remains parked; the UI does not offer a speculative reset. Server-side
durable delivery/inbox, cross-tab identity recovery and explicit correction/
replacement UX are the next product phase. Review expiry/correction still
rejects stale admission, while previously admitted tasks retain their identity.

The ordinary launcher still runs its documented pre-admitted fake workflow.
This UI is exercised in the configured browser fixture; a manual configured
review-host walkthrough remains a separate integration step. Real voice needs
participant identity, privacy/consent and actual audio acceptance. Seven major
production acceptance workstreams remain open.

## Reproduce and boundaries

Use the [locked browser environment](locked-environments.md), then:

```powershell
.\.venv\Scripts\python.exe -m pytest browser/console_cases.py browser/proposal_cases.py -q
```

The new cases cover exact human acceptance with zero execution, network/JSON/
identity receipt loss and reload, a negative status without replay, receipt
storage failure before send, stale voice correction, cross-principal delayed
response, HTML-shaped text and a 390px viewport/corrupt cache. Desktop/mobile
screenshots are written under ignored `results/proposal-browser/`. Browser CI
includes both suites. Existing control/admission regression tests also run.

Rollback is an atomic code revert, preserving admitted tasks, operation claims,
UNKNOWN effects and any retained receipt identifiers. No schema, worker loop,
provider reconciliation or remote cancellation semantics change here.

## Qualification record

Core implementation `c0c6a96`, [draft PR #37](https://github.com/dushyant-puro/chronocortex-saga/pull/37):
independent fresh GitHub source passed 1,453 runtime tests (2 skipped) and
34 browser-suite cases (32 Chromium journeys plus two existing fixture-readiness
cases). Existing audited runtime/browser environments were reused; locks and
dependencies are unchanged. Skips remain POSIX mode and unconfigured live SDK
initialization. The UI warning follow-up changes only client presentation,
browser checks and documentation; exact-source evidence is recorded in the PR.
No new real provider/audio acceptance or production-readiness claim follows.
