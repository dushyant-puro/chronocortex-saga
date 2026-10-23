# One approved Stripe sandbox cancellation

This is a deliberately narrow acceptance host for one known, disposable test
PaymentIntent. **LOCAL BRANCH VERIFIED** includes real SQLite, console approval,
worker supervision and protocol regressions. All provider effects exercised in
this phase are **SIMULATED**. No **REAL PROVIDER** request, payment-object creation,
live payment or real audio call has been made. **PUBLIC MAIN VERIFIED** remains
Phase 0 at `4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`; this safety stack is unmerged.

The host is an explicit CLI, not a background dispatcher or voice tool. It
creates no durable task, adopts no recovered executor and declares no workflow
complete. The console records exact approval in the journal; the supervised
worker owns the request. Neither an LLM nor a transcript supplies authorization.

## Safety contract

1. An explicit `--allow-sandbox-cancellation` flag, an existing compatible
   journal, a protected worker credential and a restricted `rk_test_` key are
   required. There is no environment-key, URL/proxy override, auto-approve,
   recovered-action-ID or replay option.
2. The supervisor acquires fresh ownership. Current `ccs:work`, `ccs:reconcile`
   and `payments:cancel_sandbox` grants are required before any provider read.
   A preflight verifies the account, exact known object and `livemode: false`.
   This first drill accepts only `requires_payment_method` or
   `requires_confirmation`, deliberately narrower than the provider's full
   [cancellation API](https://docs.stripe.com/api/payment_intents/cancel).
3. A new action and its stable operation key are durably staged. The host emits
   its action ID, exact arguments, digest and execution principal, then waits
   1..300 seconds for a console approval. The default is 120 seconds. Existing
   claims remain decisive: opening a fresh host never authorizes replay of a
   recovered PENDING/UNKNOWN operation.
4. Approval lookup is non-consuming and uses the durable audit sequence to select the latest committed console
   APPROVE decision for this action. No older-decision fallback is allowed.
   Dispatch independently rechecks current issuer/executor authority, expiry,
   immutable operation binding, open chain and worker ownership, and consumes
   approval atomically with DISPATCH_INTENT. Revocation after lookup can deny it.
5. The existing adapter attests the account and makes **at most one cancel POST**.
   It sends no create/confirm/capture/refund request, retries no POST, and invokes
   no compensator. Provider idempotency is an additional mechanism, not an excuse
   to replay uncertainty; API-v1 keys can expire and retain error responses.
   See [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests).
6. An uncertain result permits one GET-only reconciliation attempt while the
   host still admits work. An open/missing read remains UNKNOWN. Positive
   terminal evidence can attach a redacted receipt without repeating the POST.
   A separate readback then verifies the terminal goal and records only action
   identity, request references and the observation result as a lease-fenced
   `PROVIDER_READBACK` worker event. This event never changes host readiness.
7. The standalone drill chain closes before owned work completes. Claims,
   approvals, receipts and uncertainty are retained. A POST receipt remains a
   fact if a later GET contradicts it; readback verification then fails and the
   host does not report acceptance success or invent a compensating action.
8. One supervised operation owns preflight, staging, approval wait, dispatch,
   verification and cleanup. Normal Ctrl+C closes admission and drains it before
   releasing the lease. Before dispatch, timeout/normal stop records a known
   local abort. After dispatch, stop never claims rollback. No new readback is
   admitted during shutdown; an applied effect can therefore retain COMMITTED
   evidence with an incomplete drill result.

The CLI uses a 30-second drain budget, a 20-second saga dispatch timeout and the
adapter's existing 15-second network-operation deadline. These are policy values,
not latency measurements. Hung OS/disk calls still require external process
supervision. Forced process exit or coroutine cleanup is not remote compensation.
Trusted Python embeddings can call `run(..., handle_signals=False)` and own their
shutdown boundary; Python signal registration is restricted to the main thread.
Provider factories, event sinks and clocks are trusted Python test/integration
seams, never command-line configuration. Event payloads are detached finite JSON.
Provider selection checks `is None`, not truthiness: a supplied falsey factory
must never fall through to real transport while output is labeled SIMULATED.
This invariant is tested in preflight, recovery and the drill command.

Preflight is a point-in-time observation, not a remote resource lock or an
atomic status precondition. The approval plan reports its `preflight_status`
and `remote_state_locked: false`. Use an isolated, operator-owned test fixture
with no other writers. If another actor confirms/captures the object before
Stripe handles cancellation, the cancellation's provider-defined consequences
can differ. In particular, Stripe documents automatic refund of remaining
capturable amounts for `requires_capture`. Sending no refund endpoint request
does not prove the provider performed no such consequence. This host proves
only the terminal cancellation goal; it does not claim remote version fencing,
exclusive ownership of the provider object, financial settlement correctness or
attribution. Actual concurrent-provider-writer acceptance remains an open gate.

## Windows preparation

Use this branch's Python 3.13 environment. First complete the
[sandbox/key/preflight guide](stripe-sandbox-setup.md). A known disposable,
unconfirmed `pi_...` is still required from a separately reviewed setup procedure.
This host does not create it. If creation's acknowledgement is lost, investigate
its outcome rather than blindly creating another object. Sandbox credentials and
that asset have not yet been supplied for the real acceptance gate.

The CCS journal and credential directory must already exist and be protected.
For a new installation, follow the
[Windows journal ACL setup](operator-control-console.md#windows-setup-new-local-installation).
Do not reset an existing journal, force-clear a lease or weaken its ACL. The
prior worker must release ownership or expire naturally. This phase changes no
schema and adds no automatic migration.

Provision **new**, separately scoped worker/operator principals and secret files
offline. These commands refuse existing principal/file names. Change the names
deliberately for an existing deployment; do not rerun blindly after an error.

```powershell
$ccsState = Join-Path $env:LOCALAPPDATA 'ChronoCortexSaga'
$ccsJournal = Join-Path $ccsState 'ccs.db'
$ccsDrillWorker = Join-Path $ccsState 'stripe-drill-worker.secret'
$ccsDrillOperator = Join-Path $ccsState 'stripe-drill-operator.secret'

.\.venv\Scripts\python.exe -m control.provision `
  --journal $ccsJournal --principal stripe-drill-worker `
  --permission ccs:work --permission ccs:reconcile `
  --permission payments:cancel_sandbox --no-console-access `
  --credential-file $ccsDrillWorker
if ($LASTEXITCODE -ne 0) { throw 'Worker provisioning failed; inspect before another attempt' }

.\.venv\Scripts\python.exe -m control.provision `
  --journal $ccsJournal --principal stripe-drill-operator `
  --permission ccs:approve --permission ccs:reconcile `
  --permission payments:cancel_sandbox `
  --credential-file $ccsDrillOperator
if ($LASTEXITCODE -ne 0) { throw 'Operator provisioning failed; inspect before another attempt' }
```

The operator receives console observation access and exact approval/revocation
authority; it receives neither `ccs:work` nor `ccs:repair`. The worker receives
no console access or approval-issuance role. These local principals do not certify
a real human identity or enforce separation between administrators who control
both secret files. Production identity and secret-vault integration remain gates.

Start the console in one PowerShell terminal:

```powershell
.\.venv\Scripts\python.exe -m control --journal $ccsJournal
```

Use the existing protected-credential login procedure at `http://127.0.0.1:8877`.
Do not place either bearer secret in argv, URLs, chat or logs. The console supports
loopback and optional local TLS; this is not an Internet-facing deployment recipe.

## Explicit real-sandbox command

Run this **only after** account/object GET preflight succeeds and the known test
object has been reviewed. In another terminal, repeat the non-secret path variables
and replace the two ID placeholders:

```powershell
$ccsState = Join-Path $env:LOCALAPPDATA 'ChronoCortexSaga'
$ccsJournal = Join-Path $ccsState 'ccs.db'
$ccsDrillWorker = Join-Path $ccsState 'stripe-drill-worker.secret'
$ccsStripeKey = Join-Path $env:LOCALAPPDATA 'ChronoCortexSaga-StripeSandbox\stripe-test.key'

.\.venv\Scripts\python.exe -m providers.sandbox_drill `
  --journal $ccsJournal --credential-file $ccsDrillWorker `
  --stripe-key-file $ccsStripeKey `
  --account-id 'acct_REPLACE_WITH_REVIEWED_SANDBOX' `
  --payment-intent-id 'pi_REPLACE_WITH_KNOWN_DISPOSABLE_TEST_OBJECT' `
  --approval-wait-seconds 120 --allow-sandbox-cancellation
$LASTEXITCODE
```

This command enables an actual provider mutation host. The enable flag alone
does not authorize dispatch. When `STRIPE_SANDBOX_APPROVAL_REQUIRED` appears:

1. Copy only its action ID into the console's **Find a record > Action** lookup.
2. Review the stored account ID, test PaymentIntent ID, contract/API version,
   digest and action identity. Terminal cancellation has no inverse in this
   adapter. Never substitute another action's approval.
3. Click **Approve forward operation**, enter the printed execution principal
   (`stripe-drill-worker` with the provisioning above), review the payload, and
   record the exact decision. The UI explains that a supervised worker may
   dispatch after approval. It never dispatches from the HTTP command handler.
4. Inspect the final CLI result and refresh both the console snapshot and action
   detail. Retain the journal, redacted CLI output and independent sandbox request
   logs. Count the actual cancel POSTs in Stripe's logs; a local DISPATCH_INTENT
   count is not proof of how many requests reached the provider.

The approval window is bounded. Normal expiry without admission records ABORTED
before dispatch, but an exceptional/forced exit can leave conservative recovery
evidence. Do not rerun the CLI to resolve it. A new host cannot rebind the old
executor, and duplicate operation claims remain enforced.

## Results and recovery

Successful default-transport observations use `REAL_PROVIDER_TEST_MODE`;
injected fixtures use `SIMULATED`. Inspect both `STRIPE_SANDBOX_DRILL_FINISHED`
and `STRIPE_SANDBOX_DRILL_STOPPED`, not just the executor's Python return value.

| Exit | Meaning |
| --- | --- |
| 0 | Current COMMITTED receipt, independently verified canceled readback durably observed, and clean drain/release. This one goal was verified; no task or live-money certification follows. |
| 1 | Configuration, authority, journal or host failure. An effect may already exist. Preserve evidence and investigate before GET-only recovery. |
| 2 | An admitted operation remains IN_FLIGHT_UNKNOWN after attempted status investigation. Retain the key claim; do not repeat the mutation. |
| 3 | Blocked/ineligible, approval-window expiry, stopped before verification, stale result or incomplete/contradictory readback. This code does not prove absence of a remote effect. |

`dispatch_intent_recorded` reports a local journal fact. The adapter may have
failed account attestation before POST, or the remote endpoint may have applied
the operation before an acknowledgement was lost. Neither possibility can be
settled by that flag. `receipt_present` and `readback_verified` are distinct.
A contradictory readback retains the known POST receipt, records a false
readback observation, closes continuation and exits 3. An unavailable observation
write cannot yield exit 0. Provider actor attribution is not claimed.

For a compatible unresolved recorded operation, use the existing GET-only
`python -m providers.reconcile_stripe` command after ownership releases or
expires, with its **CCS action ID**, not the `pi_` ID. Recovered positive evidence
remains COMMITTED_STALE; it does not authorize workflow resumption or inverse
effects. See the [provider recovery runbook](provider-reconciliation.md).

## Validation, remaining gates and rollback

Deterministic tests cover real console decisions and SQLite authority, one POST,
lost acknowledgements, delayed provider application, fresh-host GET-only recovery,
unbound recovered PENDING records, revoked credentials/permissions/approval,
deadline crossing, shutdown during approval I/O and POST, epoch changes,
lease takeover, redacted output, readback contradiction/persistence failure,
enablement validation and separation of provider observations from host readiness.
The Chromium integration clicks the real approval UI, verifies the single
SIMULATED mutation, refreshes journal evidence and checks the recorded receipt.
Successful detail refresh clears only the superseded-evidence notice; a failed
read cannot clear it, and an unavailable decision response is not cleared by an
unrelated successful lookup or automatically retried.

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_forward_approval_lookup.py tests/test_sandbox_drill.py tests/test_worker_health.py -q -p no:cacheprovider
.\.venv\Scripts\python.exe -m pytest browser/console_cases.py -q -p no:cacheprovider
```

These are protocol/integration checks, not measured Stripe behavior, acoustic
accuracy or performance numbers. Independent processes/network response-loss
proxy drills, actual sandbox permissions/approval rules/request logs, known
resource setup and externally repeatable real-provider acceptance remain open.

To roll back, stop this explicit host, drain owned work and retain the schema-6
journal, claims, approvals, receipts and provider IDs. Return to the preceding
verified safety branch's recovery-only/default hosts. The new readback worker
event does not change schema or host health projection. Do not restore an older
journal, delete uncertainty, clear claims, replay POST, issue a refund as an
assumed inverse or return to unsafe public main. Key revocation withdraws future
access; it never undoes an already-issued request.
