# Stripe sandbox setup for Windows

This guide prepares an operator-owned sandbox and performs read-only access
checks. It does not create a payment, cancel an intent, enable a dispatcher or
certify the real-provider acceptance gate. The adapter and offline tests are
**LOCAL BRANCH VERIFIED**. HTTP fixtures are **SIMULATED**. No **REAL PROVIDER**
request has been run as part of this implementation phase.

**PUBLIC MAIN VERIFIED** remains Phase 0 at
`4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`; the safety stack is unmerged. Use
`codex/stripe-sandbox-preflight`, which includes the preceding provider and voice
safety work, rather than assuming these commands exist on public main.

## 1. Create a dedicated sandbox

Sign in to the [Stripe Dashboard](https://dashboard.stripe.com/), or use its
signup flow with your actual account details. Complete account verification and
enable two-factor authentication. Our test does not require enabling live
payments. If Stripe blocks access because of account eligibility, invitation or
verification requirements, report that blocker; do not substitute another country
or create a live integration to work around it.

In the account picker, select **Switch to sandbox > Create sandbox**. Name it
`CCS-Reconciliation`. Choose **Create an account from scratch**, then **Create
sandbox**. Confirm that the Dashboard banner identifies the selected sandbox.
Stripe recommends a separate general sandbox for new integrations; its sandbox
payments do not go through payment networks. The exact creation steps are in
[Manage sandboxes](https://docs.stripe.com/sandboxes/dashboard/manage), and the
isolation model is in [Sandboxes](https://docs.stripe.com/sandboxes).

Keep this sandbox for the manual integration drill. A later automated provider
suite should use a separate sandbox. Neither is the credential-free HTTP fake
used by this repository's normal CI.

## 2. Create a restricted test key

Remain in `CCS-Reconciliation`. Open **API keys** using the Dashboard search or
Developer/Workbench navigation, then **Create restricted key**. Confirm the
sandbox banner again after navigation; a generic Dashboard link can open the
currently selected account.

Name the key `ccs-sandbox-integration`. Start from zero permissions. If Stripe
asks the intended use and offers **Authorizing agent access to your account**,
select it. Stripe documents additional approval rules for agent-tagged keys;
these are separate from CCS's exact durable approval. A provider approval hold
must be investigated during acceptance, never bypassed by retrying the mutation.
See [API keys for autonomous agents](https://docs.stripe.com/keys#agent-keys).

Configure the following resource permissions for the key's own account:

| Resource | Permission | Purpose |
| --- | --- | --- |
| Account / Accounts | Read | The adapter first verifies the effective account with `GET /v1/account`. |
| Payment Intents | Write | The later approved cancellation requires write access; write includes read access for preflight/readback. |
| Other resources | None | This adapter does not use customers, charges/refunds, payouts, transfers or subscriptions. |

Dashboard group names can vary. An Accounts row can appear in a Connect group;
that does not require enabling a Connect integration. Set the resource in the
normal **Permissions** column; leave any separate **Connect permissions** column
at None. If the expected row is absent, report the visible labels rather than
granting all resources. We will verify the actual account endpoint and adjust
only the required permission if it is denied.

For a first read-only session you may choose Payment Intents **Read** instead,
and deliberately grant Write only when preparing the cancellation drill.
Preflight cannot distinguish these keys' write capabilities. Stripe's resource
permission model is documented in
[Restricted API keys](https://docs.stripe.com/keys/restricted-api-keys).

Complete Stripe's verification and create the key. Its prefix must be `rk_test_`.
The CCS adapter rejects unrestricted `sk_test_` keys, publishable keys, live keys
and whitespace/newlines. Payment Intents Write is broader than cancellation;
the fixed CCS adapter narrows its requests, but it cannot narrow the key's
provider-wide privileges if that key is stolen. Keep it dedicated to this sandbox.

Do not paste the key into chat, GitHub, a command argument, a source file, `.env`
or a screenshot. You will paste it into a masked local prompt in the next step.
Stripe's [key guidance](https://docs.stripe.com/keys) treats restricted keys as
secrets. You do not need a publishable key, webhook secret, Stripe CLI, LiveKit
account or OpenAI key for this check.

## 3. Save the key through the Windows helper

In PowerShell, from the repository checkout:

```powershell
Set-Location -LiteralPath 'C:\Users\dushi\Desktop\ChronoCortex Saga'
git status --short --branch
& .\scripts\save_stripe_sandbox_key.ps1
```

The helper creates a **new** folder at
`$env:LOCALAPPDATA\ChronoCortexSaga-StripeSandbox`. It disables inherited access,
grants only your current Windows account and SYSTEM full control, and verifies
the directory ACL **before** asking for a key. Paste the `rk_test_` key at the
masked prompt and press Enter. The helper writes exactly the ASCII token, without
a BOM or trailing newline, to `stripe-test.key`. It prints the file path only.
No provider call is made.

On this workstation the usual full path is:

```text
C:\Users\dushi\AppData\Local\ChronoCortexSaga-StripeSandbox\stripe-test.key
```

Use the path actually printed. The helper refuses existing directories/files,
network/removable drives, relative/drive-relative paths, missing parents,
reparse-point ancestors and Git checkout/worktree ancestors. It does not reset
an existing installation's ACL or overwrite a key.
You can select a new folder with `-Directory 'C:\absolute\new-folder'`; its parent
must already exist on a local fixed disk and must be trusted. Keep it outside the
checkout and shared/synchronized folders. Administrative elevation is normally
unnecessary for the default location.

If setup fails, the helper prints a fixed code and exits 1. `DIRECTORY_EXISTS`
means inspect the existing folder instead of rerunning or deleting it blindly.
`ACL_NOT_VERIFIED` means no key prompt should have occurred. `KEY_INPUT_INVALID`
means the exact restricted test-key format was not accepted. `KEY_SAVE_FAILED`
can leave a partial new file: preserve and inspect the state, and expire/replace
the key through Stripe if its confidentiality is uncertain. The helper never
prints the underlying exception or key. An execution-policy error occurs before
the helper starts; report it rather than disabling an organization's policy.

The folder ACL limits Windows access; the file is **not encrypted** and this is
not a production secrets vault or protection against administrators/malware
running as your user. The key necessarily exists briefly in process memory.
The BSTR and byte buffer are cleared after use; managed-string erasure is not
guaranteed. Deployed secret injection remains a separate operations gate.

Clear the current clipboard after pasting:

```powershell
Set-Clipboard -Value ''
```

If clipboard history or synchronization retained the copied key, remove that
entry too. Inspect file existence and the parent ACL without displaying the key:

```powershell
$ccsStripeDirectory = Join-Path $env:LOCALAPPDATA 'ChronoCortexSaga-StripeSandbox'
$ccsStripeKeyFile = Join-Path $ccsStripeDirectory 'stripe-test.key'
Get-Item -LiteralPath $ccsStripeKeyFile | Select-Object FullName,Length
Get-Acl -LiteralPath $ccsStripeDirectory | Format-List Owner,AreAccessRulesProtected,AccessToString
```

`AreAccessRulesProtected` should be True; the access list should contain your
Windows account and SYSTEM. Do not use `Get-Content` to print the key.

## 4. Record the sandbox account identity

While still viewing this sandbox, find its **Account ID**, starting with `acct_`,
in the account details/settings. Navigation labels vary. If the selected sandbox's
Dashboard URL includes an `acct_...` segment, record that candidate ID and verify
it through preflight. Do not substitute the live parent account or another
sandbox. The expected ID is supplied explicitly so a key for the wrong account
fails before any PaymentIntent request. The ID and the local key-file path are
metadata, not authentication secrets; do not include the key itself when sharing
the setup result.

## 5. Install dependencies and run account-only preflight

Use Python 3.13 and this branch's pinned requirements. If you already have this
checkout's `.venv`, reuse it. Otherwise create it:

```powershell
if (-not (Test-Path -LiteralPath '.\.venv\Scripts\python.exe')) {
    py -3.13 -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Python environment creation failed' }
}
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
.\.venv\Scripts\python.exe -m pip check
if ($LASTEXITCODE -ne 0) { throw 'Dependency validation failed' }
```

Replace the account placeholder with the sandbox's actual ID. This command
contacts Stripe's real HTTPS API in test mode using **GET only**:

```powershell
$ccsStripeKeyFile = Join-Path $env:LOCALAPPDATA 'ChronoCortexSaga-StripeSandbox\stripe-test.key'
$ccsStripeAccount = 'acct_REPLACE_WITH_YOUR_SANDBOX_ID'
.\.venv\Scripts\python.exe -m providers.preflight_stripe `
  --stripe-key-file $ccsStripeKeyFile `
  --account-id $ccsStripeAccount
$LASTEXITCODE
```

Exit 0 produces `STRIPE_SANDBOX_PREFLIGHT_COMPLETED`, with source
`REAL_PROVIDER_TEST_MODE`, the expected account and a Stripe request reference.
For account-only inspection, `object_mode_verified` is false and
`payment_intent` is null. `mutation_attempted`, `write_permission_verified` and
`journal_updated` remain false. This is evidence of account read access only.
The API version is pinned in the adapter's request header; no Dashboard account
API-version change is needed. Changing the version is a reviewed contract change.

| Failure code / exit | Meaning and next action |
| --- | --- |
| `CREDENTIAL_INPUT_INVALID`, exit 1 | Check the protected path and exact `rk_test_` format; do not print the file. |
| `SCOPE_INVALID`, exit 1 | Replace placeholders with a valid account/optional object ID. |
| `STRIPE_HTTP_401`, exit 1 | Check whether the key is valid or expired in this sandbox. |
| `STRIPE_HTTP_403`, exit 1 | Inspect the request in sandbox Workbench; grant only the missing read permission. |
| `STRIPE_ACCOUNT_MISMATCH`, exit 1 | Key and expected account differ; verify the selected sandbox. |
| Protocol, timeout or other HTTP failure, exit 1 | Preserve the sanitized code/request context and investigate; no POST occurred. |
| Invalid arguments, exit 2 | Use `python -m providers.preflight_stripe --help`. |

Use the key's **View request logs** menu in Stripe to check the real GET request.
Never share a full request/response dump containing credentials or customer data.

## 6. Prepare the first approved provider drill

At this point, report the sandbox name, account ID, local key-file path, and the
preflight event/code. **Do not send the key.** Do not create or confirm a payment
manually for this integration. We will prepare a separately reviewed, one-shot
setup for one disposable, unconfirmed test PaymentIntent; capture its `pi_` ID
before any cancellation. A lost creation acknowledgement is investigated rather
than blindly repeating creation.

Once that known test object exists, the same preflight can add
`--payment-intent-id 'pi_REPLACE_WITH_KNOWN_TEST_OBJECT'` to verify its exact ID,
test mode and current status with a second GET. An open status is a read
observation, never evidence that an earlier mutation failed or is safe to retry.

The subsequent drill must use a supervised worker, durable action identity,
exact scoped approval, one cancellation attempt, persisted receipt and GET-only
recovery. The existing `providers.reconcile_stripe` command requires a recorded
CCS action ID, not merely a `pi_` ID; it is not an initial setup command. There is
currently no all-in-one forward cancellation CLI. The remaining acceptance,
response-loss and rollback requirements are in
[provider-reconciliation.md](provider-reconciliation.md#remaining-acceptance-gates).
Creating credentials or passing preflight does not complete those gates, and
provider sandbox results cannot certify live-money behavior.

## Offline verification and rollback

No key is needed for the regular runtime suite or the focused preflight tests:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_stripe_preflight.py tests/test_stripe_sandbox.py tests/test_stripe_reconciler.py tests/test_windows_stripe_key_setup.py -q -p no:cacheprovider
```

The Windows helper tests use dummy keys and disposable real ACLs, including
failed enforcement, overwrite races and junction ancestors. They run on Windows
CI and are explicitly skipped on Linux; provider protocol tests run on both.
No measured provider latency, idempotency behavior or REAL PROVIDER outcome is
claimed by these tests.

Preflight has no durable CCS state to roll back. To withdraw sandbox access,
expire the dedicated restricted key in Stripe, stop using its local file, and
preserve any later provider/journal evidence for investigation. Key revocation
does not reverse an already-issued remote mutation. Do not delete a sandbox or
recorded test object while an operation's outcome is unresolved. The preceding
branch can still read the journal: this phase changes no journal schema, approvals,
operation keys, provider endpoints or mutation retry policy.
