# Provider phase: operation identity and recoverable evidence

Stacked on `codex/worker-supervision` at `833be7201d08a91cbb4da5f3dde72996d422ef36`.
PUBLIC MAIN VERIFIED remains Phase 0 at `4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`.
LOCAL BRANCH VERIFIED is an implementation under test, not a deployed service.

## Receipt-bearing status

Trusted status adapters may return `ReconciledEffect(action_id, digest, receipt)`
in addition to the existing exact `True`, `False`, `None` contract. It means the
specified effect is present under the adapter's documented semantics. The core
checks action identity, the sealed payload and strict finite JSON before changing
state. Receipts are detached and bounded to 16 KiB. A missing receipt and resolved
state are saved in the same journal transaction/event. An existing receipt stays
unchanged, preserving history and receipt-bound compensation approvals. Schema 6
already supports JSON results, so this requires no migration.

This object does not verify provider truth. Adapters remain trusted. Returning
False requires evidence that also excludes delayed future application; a 404,
timeout, list/search miss or current open state does not meet that requirement.
A positive status query still rechecks epoch, chain closure and durable task
generation. Recovery never restores conversational validity. Reconciliation
never calls the original executor. A different key cannot justify replaying an
unknown mutation.

`tests/test_reconciliation_receipt.py` uses real SQLite and SIMULATED provider
evidence. Cases cover atomic receipt persistence/reload, malformed/oversize data,
identity substitution, payload edits during a query, concurrent resolution,
stale epochs and retaining original receipts.

## Provider decision (official documentation checked 2026-10-23)

| Candidate | Identity and idempotency | Reconciliation constraint | Decision |
| --- | --- | --- | --- |
| Stripe API v1 sandbox, cancel a known PaymentIntent | Existing resource ID; POST idempotency key; v1 keys may be pruned after at least 24 hours | GET by known ID; canceled is terminal; open/missing/error remains UNKNOWN | Selected for a narrow sandbox adapter |
| Stripe create a PaymentIntent | Server assigns ID; same bounded idempotency retention | Lost create response also loses direct lookup identity; search is not absence proof | Excluded from automated dispatch |
| Google Calendar insert | Client-chosen ID supported; distributed ID collision detection is not guaranteed | Retrieval is useful; a miss does not exclude a delayed insert | Not selected for the first adapter |
| GitHub create issue | Server-assigned issue number; inspected create contract has no idempotency parameter | Search/list cannot prove an uncertain create cannot arrive later | Not selected for the first adapter |

Sources: [Stripe sandboxes](https://docs.stripe.com/testing-use-cases),
[v1 idempotency/retention](https://docs.stripe.com/api/idempotent_requests),
[cancel contract](https://docs.stripe.com/api/payment_intents/cancel),
[retrieve contract](https://docs.stripe.com/api/payment_intents/retrieve),
[Calendar IDs](https://developers.google.com/workspace/calendar/api/v3/reference/events),
[GitHub create issue](https://docs.github.com/en/rest/issues/issues#create-an-issue).
The selection and exclusion conclusions are engineering inferences from these
contracts, not claims that another provider cannot support a different design.

## Narrow effect semantics

The selected goal is **this known sandbox PaymentIntent is terminally canceled**.
It is not proof that CCS was the unique actor who canceled it, an exact-once
execution claim, a refund/settlement claim or a provider-wide fencing guarantee.
Another actor reaching that same irreversible terminal goal is acceptable for
this operation. A GET finding canceled may resolve a lost response; other states
must remain UNKNOWN because an earlier cancellation can still arrive.

There is no reverse operation for a canceled PaymentIntent. The adapter must
declare no compensator. A stale successful cancellation remains COMMITTED_STALE
and requires operator review; creating a replacement intent is a separate
business action with new approval, never automatic rollback. Live keys, payment
creation, confirmation, capture, refunds, Connect scope switching, webhooks and
automatic retries are outside this phase's adapter contract.

The pinned API version is `2026-09-30.endive`, verified against Stripe's
[versioning documentation](https://docs.stripe.com/api/versioning). Sandbox
keys must be restricted `rk_test_` keys. See the provider's
[key types](https://docs.stripe.com/keys) and
[restricted-key permissions](https://docs.stripe.com/keys/restricted-api-keys).
Only provider-backed sandbox runs may be labeled REAL PROVIDER (sandbox): the
provider API is real, the transactions are test objects and move no real money.
In-process HTTP fixtures must always be labeled SIMULATED.

## Adapter boundary

`providers/stripe_sandbox.py` has one fixed HTTPS origin, no redirects, ambient
proxy/env-key discovery, Connect headers or transport retries. The effective key
account is checked through GET `/v1/account` before every operation, then the
exact PaymentIntent ID is requested. This endpoint was also checked against
[Stripe's official OpenAPI specification](https://github.com/stripe/openapi/blob/master/openapi/spec3.json).
Only restricted test-key syntax is accepted. Test-key syntax proves neither
account ownership nor isolation: select a separate sandbox in the Dashboard,
pin its account ID, and verify the actual API before claiming that gate.

Responses require HTTP 200, JSON, identity encoding, a bounded request reference
and the pinned API version when returned. Raw response bytes are capped at 64
KiB before parsing; duplicate keys, nonfinite values, malformed JSON, mismatched
objects/IDs, live-mode objects and invalid terminal evidence fail closed. A
terminal receipt includes the requested scope, deterministic idempotency key,
request ID and cancellation timestamp. Account email, customer information,
client secrets and unfiltered provider errors are not retained. Connection/read
errors, HTTP errors (including 4xx, rate limit and 5xx), redirects, parser failures
and timeouts never become proof of nonexecution.

For this one irreversible goal, the operation key is deterministic from the
account, known resource and versioned contract; the supported `stage` helper
does not accept a replacement key. A new API/contract version is a deployment
change, not permission to bypass old unresolved work. The adapter's status
callback rejects changed keys, payloads and tool names. Runtime key claims,
durable current credentials, `payments:cancel_sandbox` and a separate exact
forward approval apply before execution. New integration hosts must use
WorkerSupervisor; the default recovery worker stays unconfigured.

The provider's at-least-24-hour idempotency retention is not an eternal lock.
There is no retry loop inside this adapter, including preflight failures. The
15-second overall provider deadline and 5-second I/O timeouts limit local waits;
they do not bound remote execution or prove remote cancellation. Only GET
reconciliation is repeatable. A current open intent, succeeded intent or missing
resource never returns False. No automatic repair handler is registered.

This Python adapter is a trusted host capability, not a sandbox for arbitrary
Python. Direct HTTP or direct executor use can bypass CCS authorization. Do not
expose the object or provider key to model/ASR text, plugins or the browser. The
restricted provider key still authorizes more than a single cancellation; the
CCS manifest/approval layer narrows use inside the host. Egress enforcement,
external key rotation and secret-store deployment remain operational gates.

## Test and acceptance matrix

| Layer | Evidence | Gate |
| --- | --- | --- |
| Core receipt resolution | Real SQLite, operation/digest seal, atomic receipt/state, reload, concurrent resolution, stale epoch | Automated local/CI |
| Provider protocol | SIMULATED transport: exact request, account scope, key rejection, bounded parser, 4xx/5xx/redirects, lost ack, cancellation, delayed apply | Automated local/CI |
| Provider after restart | SIMULATED independent effect ledger; only GET recovery; original POST count remains one | Automated local/CI |
| Real sandbox | Protected restricted test key, separate sandbox account, known test PaymentIntent; status and explicit mutation drill | Not yet verified; credentials/assets required |
| Production deployment | Live payment actions, settlement/refund claims, voice, dispatcher, identity/TLS/OS service | Outside this adapter's release gate |

No throughput, availability or financial correctness rates follow from these
tests. Fixture responses are deliberately small protocol models; real sandbox
behavior and permission setup must be verified independently.

## Rollback and compatibility

Disable provider admission/stop the host, drain owned operations under its lease,
and retain schema-6 journal/action/event/key/approval evidence and provider IDs.
The default recovery-only host is a compatible safe reader, but has no provider
status binding. Do not clear claims, remove consumed approvals, replay POSTs,
restore an older journal or create a replacement PaymentIntent as rollback.
Use an authenticated, version-compatible GET status adapter to gather current
facts. Unknown facts stay parked. Completed terminal cancellation is irreversible.

Existing Boolean status callbacks remain supported; their truth remains the
adapter's responsibility. Provider receipts do not change stored action identity
or workflow contracts. A deployment changing API/provider/argument semantics
requires a new explicit workflow contract and review of unresolved older work.

## GET-only recovery command (Windows)

The operator has requested offline validation first and has no sandbox account
yet. No provider secret or `.env` file is configured in this workspace, and no
REAL PROVIDER call has been run. The following is a future integration runbook,
not evidence that the real sandbox gate passed.

`python -m providers.reconcile_stripe` starts a WorkerSupervisor, acquires a
fresh lease, hydrates evidence without executors, and queries exactly one
compatible recorded action. It has no creation, forward execution or repair
command. Credentials need current `ccs:work`, `ccs:reconcile` and
`payments:cancel_sandbox`. An incompatible provider/account/version/key, unknown
action or existing repair attempt is rejected. The original worker must release
its lease or expire naturally; this command cannot force takeover.

Use the protected state-directory ACL instructions in
[worker-supervision.md](worker-supervision.md) before creating either secret
file. Issue a separate worker credential offline using `control.provision`
with explicit permissions and `--no-console-access`. Keep the Stripe restricted
test key outside Git and use a separate sandbox with only the required account
read and PaymentIntent read/write permissions; recovery itself only reads.
Confirm the exact permission setup against the actual sandbox API. No secret
values belong in argv, environment variables, browser URLs, transcripts or logs.
Secret files contain the exact ASCII token with no trailing newline. POSIX
owner-only mode is enforced; Windows protection depends on the directory ACL.

After a future approved supervised integration has recorded a compatible action:

```powershell
$ccsPython = '.\.venv\Scripts\python.exe'
& $ccsPython -m providers.reconcile_stripe `
  --journal 'C:\CCS\state\ccs.db' `
  --credential-file 'C:\CCS\state\reconciler.key' `
  --stripe-key-file 'C:\CCS\state\stripe-test.key' `
  --account-id 'acct_REPLACE_WITH_SANDBOX_ACCOUNT' `
  --action-id 'REPLACE_WITH_RECORDED_ACTION_ID'
$LASTEXITCODE
```

Exit 0 means positive status was durably recorded and ownership released. A
recovered action remains COMMITTED_STALE; 0 never means the task is valid to
resume or that cancellation was reversed. Exit 2 means this query found no
positive terminal proof; UNKNOWN stays UNKNOWN and any historical receipt/state
is retained. Keep unresolved key claims and investigate. Exit 1 means failure; inspect journal
facts and authority/provider configuration before retrying a **GET query**.
The command sanitizes errors and prints stop/release metadata. It labels real
default-transport observations REAL_PROVIDER_TEST_MODE, which is a provider
test object, not live money or a production certification. Tests inject a
SIMULATED transport and explicitly label it as such.

Trusted Python hosts may register `provider.manifest()`, stage through
`provider.stage(saga, known_id)` and submit `saga.commit_write(action_id,
provider.execute, timeout=20.0)` through the existing supervisor **after** exact
durable operator approval. Keep the API client open until owned work drains.
This is an integration API, not an enabled dispatcher. A successful executor
return still requires inspecting durable action/task state: an epoch advance
can retain its receipt as COMMITTED_STALE. Do not infer business success from
the Python return value alone. A process restart does not authorize executing
a recovered PENDING/UNKNOWN action; use GET-only reconciliation.

## Remaining acceptance gates

1. Create an isolated sandbox and known disposable test PaymentIntent through
   a separately reviewed setup procedure. Its creation is itself a remote
   mutation; a lost creation acknowledgement must not be blindly repeated.
2. Verify restricted-key permissions, account attestation, pinned API version,
   object mode and real request references with GET-only access first.
3. Run one explicitly approved, supervised terminal cancellation. Preserve the
   journal and provider request logs; verify direct response and subsequent GET
   agree. No automatic creation, confirmation, capture, refund or inverse action.
4. In a controlled local proxy/test transport, discard the mutation response
   after forwarding it, restart the host, then query the known resource. Compare
   provider request logs to ensure only one POST. A proxy-driven live drill is
   separate from the deterministic in-process fixtures and is not yet built.
5. Verify stale conversational intent, lease loss, key revocation and an open
   or missing read remain safe under the actual provider. Release deployment
   only after these gates are independently repeatable. A sandbox does not
   certify live payment/settlement behavior.

Offline reproduction uses the README's Python 3.13 setup with no provider
credentials. `pytest -q` collects all protocol/core/command tests, and
`pytest browser/console_cases.py -q` collects browser regressions after installing
`requirements-browser.txt` and Chromium. The existing live voice placeholder is
the only intentionally skipped test. Real provider acceptance has not been run;
it is not counted as a passing simulated test.
