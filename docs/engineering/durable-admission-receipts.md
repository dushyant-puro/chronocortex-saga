# Durable admission receipt recovery

Status: **LOCAL BRANCH VERIFIED** implementation; qualification is recorded in
the phase PR. Voice/tools remain **SIMULATED**. This is positive historical
admission readback, not a durable inbox or REAL PROVIDER reconciliation.

## Protected read contract

`POST /api/proposals/receipt` accepts exactly `task_id`, `review_id` and
`binding_digest`. Use the same-origin console session and its current CSRF
header. IDs are retained lookup hints, never authority. No credential, argument,
workflow or transcript is accepted in this body. The endpoint works on an
ordinary console host without a proposal controller, owning voice loop or
process-local pending review.

Within one journal transaction it authenticates the current session, validates
the persisted proposal binding against its task/workflow and digest, and checks
the original reviewing principal plus current creation/domain/task-effect
permissions. A newly issued credential for that same principal can read history;
this does not restore authority to the original credential. Historical review
expiry and later voice correction do not erase an already admitted task.

A positive response contains only the exact task/review/digest identity,
`state: ADMITTED` and `execution_authorized: false`. It means that admission was
persisted, not that the task is currently active, an executor ran, or an external
effect succeeded. No transcript or arguments are returned. Checkpoint/task events,
actions, approvals and worker/provider state are not changed by this lookup.
Existing authentication bookkeeping/SQLite transaction policy is unchanged.

Missing, mismatched, other-principal, insufficiently scoped and legacy records
return the same unavailable receipt error. Corrupt binding evidence fails closed.
An unavailable response cannot establish that a delayed admission will never
arrive. Preserve the exact IDs; do not replay admission or manufacture a new task.
The existing task-observation permission model is unchanged; this endpoint adds
a stricter same-reviewer requirement specifically for admission receipts.

## Browser recovery

After an admission attempt, **Read admission status** reads the durable receipt
instead of the old session-bound review. Re-login in the same tab can recover
the principal-scoped retained identifiers even if pending reviews were lost.
The UI validates all three returned identities and the separate execution flag.
Unconfirmed reads remain parked. Disconnect/session generation fencing prevents
late responses from populating another operator's screen. No automatic retry of
admission is introduced. Pre-admission status still uses the existing live
review endpoint and its session/expiry checks.

This does not save identifiers after closing/clearing tab storage, list pending
proposals, deliver notifications, permit a different principal to accept an old
review, or reconstruct an unadmitted review after restart. Those require the
remaining durable inbox/delivery and explicit correction/replacement design.
The default canned walkthrough is unchanged.

## Reproduce and rollback

Use the [locked environments](locked-environments.md), then:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_admission_receipts.py tests/test_session_proposal_review.py -q
.\.venv-browser\Scripts\python.exe -m pytest browser/console_cases.py browser/proposal_cases.py -q
```

Tests include reopening SQLite, new sessions/credential rotation, expired and
corrected review history, missing/revoked permissions, same-shaped unavailable
results, corrupt provenance, HTTP origin/CSRF/body guards, receipt identity
mismatch, re-login without pending reviews, delayed cross-principal responses
and zero execution/admission replay. No remote effect acceptance is claimed.

No schema/dependency/worker/provider change. Revert the phase code atomically;
retain journals, UNKNOWN effects and receipt identifiers. Older UI status may be
unavailable after session loss; never clear evidence or retry a mutation to work
around that limitation. Historical intermittent CI investigations stay parked
for final integration, without waiving release gates.
