# Safety phase 1d: mandatory forward-write authorization

Stacked on `production/reconciliation-ownership` at
`1b5dfa5db6e6894b18ed0f2ff1d708f3d4fea333`. The public main branch remains
the Phase 0 baseline until the stacked changes are reviewed and merged.
All exercised external effects are **SIMULATED**. No **REAL PROVIDER** is enabled.

## Protected invariants

1. `commit_write` never dispatches without an explicit `AuthorizationContext`
   and a matching write manifest. Omitting `policy_engine` cannot bypass the
   mandatory baseline. `PolicyEngine(default_deny=False)` cannot disable it.
   A custom policy engine may add checks but cannot remove baseline checks.
2. Registered policy and the staging snapshot both apply. A caller-supplied
   registry cannot replace the manager's registry. A call-time manifest may
   tighten policy, but cannot remove required permissions or confirmation.
   Missing registered tools and manifest/name mismatches deny dispatch.
3. Confirmation-required writes need `MutationApproval` matching action ID,
   capture epoch and a canonical SHA-256 digest of tool name and arguments.
   Bare `confirmed=True` is insufficient. Restaging after correction or a
   new epoch requires a fresh approval, even if arguments happen to match.
4. Staging detaches nested arguments and permission lists from caller-owned
   objects. Pending argument changes deny dispatch. Executors receive a
   detached payload, sealed synchronously after authorization, so later edits
   cannot change the request. Validators receive detached arguments too.
   Callback aborts cannot be overwritten by dispatch.
5. Authorization denial remains pre-dispatch: the action stays PENDING unless
   explicitly aborted by another control path. Denial never establishes a remote
   failure and never authorizes compensation of previous effects.

The payload contract accepts JSON objects with string keys and JSON values.
NaN, infinities, tuple values, arbitrary objects and non-string object keys are
rejected before staging. Permission strings use exact membership; malformed or
mutated permission collections and truthy non-boolean confirmation values deny.

## Trusted host integration and compatibility

Credentials must come from trusted application code independently of ASR,
model output, tool arguments and grounded slots. `AuthorizationContext` is an
in-process trust object, not authentication, a bearer token or a signed network
credential. Do not deserialize it directly from a client or expose arbitrary
executor/manifest callbacks through an API. Validators are trusted pure checks,
not a place to perform external effects. Python host code remains trusted.

Host injection is available on `SpeculativeSagaManager(..., auth_context=...)`
and `CCSAgent(auth_context=...)`; a trusted call-time context can override the
host context for a specific operation. Fleet forward-write manifests now require
`fleet:write`. The default voice entrypoint supplies no credentials, so its
forward mutations are denied. Reads retain their existing behavior; this phase
does not implement read-data access control.

For a confirmation-required simulated action, trusted operator code can use:

```python
from runtime.policy_engine import AuthorizationContext, MutationApproval

# Only after the trusted operator has approved this exact staged request:
context = AuthorizationContext(
    granted_permissions={"fleet:write"},
    approval=MutationApproval.for_action(action),
)
result = await saga.commit_write(action.action_id, simulated_executor, auth_context=context)
```

There is no automatic approval from a conversational "yes". Approval creation is
deliberately absent from production ASR handlers. The adversarial test harness
creates approvals only from its explicit simulated operator switch.

Existing signatures remain available, but unsafe behavior intentionally changes:
permission metadata is enforced, absent credentials deny, and a blanket boolean
does not approve confirmation-required writes. Task permission denial retains
the existing FAILED behavior without unwinding prior effects; resume of that
terminal task is a no-op. Confirmation denial retains WAITING_FOR_AUTHORIZATION
and can reuse the original pending action after exact approval. The voice handler
retains a denied pending action in its ledger and logs its ID, without unwinding
prior effects. A durable approval workflow and control UI remain future work.

## Test migration and reproducibility

`tests/test_mandatory_authorization.py` imports the production manager directly
and has no default grants. Five initial regressions failed on the parent code:
omitted context with/without an engine, omitted engine with insufficient grants,
call-time manifest downgrade and unmanifested dispatch.

Legacy state-machine tests now explicitly import `SimulatedAuthorizedSaga` from
`tests/saga_test_support.py`. That helper grants only `fleet:write` and synthesizes
manifests for fake test tools when no registry/manifest is supplied. It inherits
the production commit and policy implementation unchanged; it does not monkeypatch
enforcement or approve confirmation-required actions. The scorecard uses the same
explicit simulation helper. Policy tests continue to use the real manager.
Old tests asserting that permissions were inert or omission bypassed checks now
assert denial and then explicit authorized success where appropriate.

Coverage includes missing credentials, policy/registry overrides, registry policy
changes, payload and approval replay attacks, malformed credentials/JSON, nested
alias mutation during an event-controlled dispatch, validator abort/mutation,
task denial/resume and voice denial with an earlier committed effect.

PowerShell, from the repository root with Python 3.13:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m pytest tests/test_mandatory_authorization.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe eval/adversarial_scorecard.py
```

The scorecard still distinguishes completion from safe parking: 15/20 simulated
trials reach completion; five legacy restart trials remain parked. This change
does not make them crash-safe or establish provider behavior. The live-session
test remains a skipped placeholder, not verified voice integration. CI runs
Windows and Linux. Top-level dependencies are pinned; transitive versions are
not yet locked.

## Remaining blockers and rollback

The stacked [durable-intent stage](durable-action-intent.md) adds a local SQLite
dispatch journal, versioned task projections, CAS and process-crash tests.
Distributed ownership and automatic restart completion remain unverified. Process-local
locks, mutable in-memory objects and legacy checkpoints do not establish durable
authorization, crash recovery or distributed ownership. Principal/tenant identity,
approval expiry/revocation and a protected operator endpoint remain required.
Administrative reconciliation and compensation entrypoints remain trusted host
APIs, not independently authenticated remote operations. Per-task chain scope,
distributed fencing, richer provider evidence, real voice confidence/revisions,
provider selection and deployment operations remain release blockers.

On regression, stop new dispatch and preserve task/action/evidence records.
Keep denied actions and unresolved provider outcomes for inspection. Do not
restore permissive credentials, set UNKNOWN to ABORTED, replay checkpoints or
blindly retry operations to restore the demo. Reverting this commit restores an
unsafe authorization bypass and does not undo any remote effect. Review stacked
PRs before merge; a code rollback is never provider reconciliation.
