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
