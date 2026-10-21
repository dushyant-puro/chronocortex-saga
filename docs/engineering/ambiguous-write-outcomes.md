# Safety phase 1b: ambiguous mutation failures

This change is stacked on `production/safety-invariants` at
`0413fa3186219529a7196902c4ce3fef1c49a5d5`. It addresses ambiguous executor
exceptions only. Public main remains Phase 0. All provider-effect tests are
SIMULATED; no REAL PROVIDER execution is established.

## Invariant: an exception is not proof of non-execution

Once a write executor is invoked, a generic exception means
`IN_FLIGHT_UNKNOWN`. The executor may have applied the effect before a
connection error, response-decoding error, or other local failure. The original
exception is retained as `action.error` and re-raised. The action stays outside
the committed chain and its existing idempotency key blocks staging a duplicate.
No automatic retry or compensation is attempted on this unknown outcome.

The task engine parks the original action for reconciliation, preserving earlier
progress. If status is unavailable, resume remains parked. A positive status
with current intent permits completion; a correction before reconciliation
produces a stale effect and remains parked for repair. The voice adapter also
recognizes unknown exceptions and does not unwind prior actions on that basis.

## Explicit known-not-applied contract

A trusted tool adapter can raise:

```python
raise MutationNotAppliedError(
    "documented atomic rejection",
    evidence="redacted-provider-rejection-reference",
)
```

The evidence reference is required and stored in `action.not_applied_evidence`,
which is exposed by `snapshot()`. Use a redacted code or reference to an
operation-specific record, not a raw response, credential, transcript, or PII.
The original exception remains available for diagnostics.

Only this explicit post-dispatch exception permits `ABORTED`. It does not
automatically retry. A caller may stage a new request after confirmed rejection;
the old action remains ABORTED and cannot itself be committed again.

The adapter must guarantee both that this operation made no persisted change
and that it will not apply later. Do not infer this from an HTTP status alone,
a missing eventual-consistency read, a timeout, connection closure, cancellation,
or an exception's class/message. Do not indiscriminately wrap generic errors in
MutationNotAppliedError. An available compensator is not a proof of non-execution.

The runtime checks the presence of the reference, not its truth. This is a
trusted-adapter contract, not cryptographic/provider verification. Real providers
still require documented idempotency/reconciliation semantics and contract tests.

The simulated dock reservation adapter uses the explicit error only for its
synchronous occupied-dock check before insertion. Other unexpected errors retain
the conservative default. Existing RuntimeError catches remain compatible because
MutationNotAppliedError subclasses RuntimeError.

Pre-dispatch validation, authorization and stale-epoch failures retain their
existing behavior: they do not invoke the executor and do not acquire a fabricated
unknown remote outcome. Read failures still use the read lifecycle.

## Regression coverage

`tests/test_mutation_outcome_uncertainty.py` covers an independent fake-provider
ledger with lost responses, generic failures before observed application,
corrections after application and during reconciliation, blocked same-action and
same-key retries, explicit atomic rejection with evidence, missing evidence,
validation before dispatch, production task resume, the voice caller's unwind
boundary, and simulated dock conflicts. Timing is controlled by events; timeouts
on test waits are watchdogs, not race scheduling.

The historical tests that required generic exceptions to mean ABORTED now require
IN_FLIGHT_UNKNOWN. Their old behavior was unsafe, not a compatibility guarantee.

## Reproduce and rollback

From a clean checkout with Python 3.13, follow the PowerShell environment setup
in [safety-phase1.md](safety-phase1.md), then run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_mutation_outcome_uncertainty.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

Do not revert to the old exception policy while unresolved actions exist. Stop
new mutation dispatch, preserve operation identities/evidence, and reconcile or
escalate them. Reverting code does not undo remote effects. Current idempotency
indexes and exception evidence remain process-local, not a durable journal.

## Remaining blockers

Exclusive ownership was the next blocker at this PR's baseline; the stacked
change in [reconciliation-ownership.md](reconciliation-ownership.md) addresses
process-local saga and task repair ownership. Optional/fail-open mutation
authorization, durable intent before dispatch, transactional action/event storage,
compensation lifecycle, distributed fencing and real voice/provider verification
remain separate required work. This PR is not a production-readiness claim.
