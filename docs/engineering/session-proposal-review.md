# Protected proposal-review API

Status: **LOCAL BRANCH VERIFIED** API slice. Voice/effects in qualification are
**SIMULATED**. This does not complete the review UI, durable inbox, real voice,
speaker authentication or provider acceptance workstreams.

## Authority and data flow

A trusted deployment creates `VoiceIngress`, `ProposalAdmissionService` and
`ProposalReviewController` on the same owning thread/event loop. The controller
pins one allowlisted workflow and an immutable copy of its exact structured
arguments. Pass it explicitly to `control.app.create_app(journal,
proposal_reviews=controller)` using the **same journal object**. The journal
must already have the required worker ownership. Existing hosts, including the
local canned acceptance walkthrough, leave this capability disabled.

There is no HTTP endpoint for posting transcripts, acoustic confidence, workflow
code or arbitrary task arguments. The trusted host must supply the actual
current grounded `VoiceProposal`. Grounding is acoustic evidence, not verified
human identity or proof that structured arguments follow from the transcript.
The reviewer must inspect that mapping. The API does not call a model.

The ordinary console login creates an HttpOnly same-origin session referencing
the provisioned credential ID. This bridge never recovers, retains or mints a
bearer secret. Review/admission require `ccs:observe`, `ccs:task:create` and every
workflow domain grant. Host code and direct SQL remain trusted, not a hostile
Python sandbox.

## Protocol

All three endpoints use POST with `application/json`, exact configured Origin,
the session cookie and current `X-CCS-CSRF`. Existing host/origin restrictions,
16 KiB request cap, body deadline, CSP and `Cache-Control: no-store` apply.

| Endpoint | Exact body | Result |
| --- | --- | --- |
| `/api/proposals/review` | `{}` | Exact protected transcript, immutable binding (workflow digest, arguments, reviewer, source revision, expiry), stable review/task IDs, current/expired/admitted flags; no task insertion. |
| `/api/proposals/admit` | `{"review_id":"...","binding_digest":"..."}` | Explicit admission receipt for that exact binding; no approval, restore, executor call or dispatch. |
| `/api/proposals/status` | Same exact IDs/digest | Protected readback of the existing review and matching durable task, with no admission/replay. POST keeps private identifiers out of URL/access logs and enforces CSRF. |

The browser must retain the returned review ID, task ID and digest before
offering an explicit acceptance decision. A lost admission response is uncertain:
use status or inspect that exact durable task. **Do not create another review
as an automatic retry.** Status cannot itself admit a task. Replaying the same
explicit admission while authority/evidence remain valid is identity-idempotent;
it cannot reset task progress or create a second task/event.

Reviews are bound to the **originating session**, not merely a principal or
credential. Another login using the same credential cannot read/admit that
pending review, and the bearer admission API cannot bypass its session binding.
Each admission rechecks session expiry/revocation, CSRF, credential and principal
version/permissions **inside the SQLite transaction that inserts the task**.
Review lifetime is bounded by both session and credential expiry. No cached
actor confers authority. Current ingress identity/revision and pinned deployment
references are checked on the owning thread without an intervening await before
the synchronous SQL admission boundary.

## Limits, failure and rollback

Pending reviews/transcripts are process-local and bounded by the existing
128-review capacity. New reviews purge expired entries. Status may show an
expired review to its still-authorized originating session; expired evidence
cannot be admitted. Correction invalidates unadmitted prior evidence. It does
not cancel or replace an already admitted durable task.

Restart loses the pending inbox and its session binding. A committed task and
binding survive in schema 7; inspect its previously returned task ID. There is
no durable delivery/replacement workflow or UI auto-recovery in this slice.
Raw transcripts remain absent from persisted admission bindings/security events;
the protected HTTP response deliberately contains them for human review. Real
voice consent/retention and participant identity still need separate acceptance.

The API is opt-in; wrong-thread access fails closed. Synchronous bounded SQLite
work can briefly block the owning loop under contention, within existing journal
budgets. Moving it to an arbitrary threadpool would violate ingress ownership;
future load/backpressure qualification must measure this boundary.

Rollback: disable host wiring or revert this code. No migration or downgrade is
needed. Preserve any admitted tasks/UNKNOWN operations and evidence; never clear
the journal or retry a provider mutation to reverse admission. This change does
not introduce provider access or live audio.

## Reproduce

From the qualified locked runtime environment, no additional packages/keys:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_session_proposal_review.py tests/test_proposal_admission.py tests/test_control_api.py
.\.venv\Scripts\python.exe -m pytest -q --junitxml=results/runtime-tests.xml
```

Regression/adversarial schedules include cross-session and bearer bypass,
revocation at the final SQL boundary, missing domain/create grants, stale
correction/digest/expiry, same-origin/CSRF/body rejection, anonymous evidence
privacy, lost acknowledgment readback and zero dispatch/approval on admission.

At implementation `4275d62`, the focused suite passed 141 tests. Both the
workspace and an independent clean GitHub checkout passed 1,433 runtime tests
with 2 skips, using the unchanged audited runtime environment. Clean-source
distributed evaluation passed 5/5 SIMULATED cases and 122 checks. CI outcomes
are recorded per head in [PR #33](https://github.com/dushyant-puro/chronocortex-saga/pull/33).
These passes do not close the earlier hosted SQLite contention/cancellation
findings in the [implementation record](../../implementation.md).
