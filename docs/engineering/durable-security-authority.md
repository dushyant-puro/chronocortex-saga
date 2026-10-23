# Safety phase 1i: local authenticated principals and durable approvals

Stacked on `production/workflow-contract-versioning` at
`c879cf7b9ad169d747f3b740db30ab9857e09489`. **PUBLIC MAIN VERIFIED** remains
`4162ec17f4bda15c13de3fa1f0e1e33e3fe35591`; the draft safety stack is unmerged.
Credential verification and SQLite authorization use the real implementation.
External tool effects in validation remain **SIMULATED**. No **REAL PROVIDER**
has been selected or enabled.

The first regression reproduced a durable mutation authorized solely by a
caller-constructed permission set. That is now rejected: a journaled dispatch
requires possession of a locally provisioned credential and current server-side
permissions. Legacy in-process MutationApproval objects authorize only explicit
volatile simulation, not journaled execution.

## Invariants and acceptance criteria

| Invariant | Evidence |
| --- | --- |
| Caller claims are not durable authority | Self-declared grants, booleans, a forged principal snapshot and in-process approval objects cannot authenticate dispatch. Runtime policy uses fresh server grants. |
| Revocation wins if it commits before intent | Credentials, principal status/permissions, issuer authority and approval status are checked in the same BEGIN IMMEDIATE transaction as dispatch/repair intent. A validator that revokes after preflight still prevents execution. |
| Approval consumption and dispatch are atomic | Approval use, actor attribution, action/event updates and worker attribution commit together. Injected dispatch and compensation event failures roll them all back. Two independent connections can consume an approval only once. |
| An approval authorizes one exact operation | Bindings include action/tool, payload digest, epoch, process session, task/step/generation, chain, operation key, policy digest, direction and available workflow/provider-scope contracts. Another principal or action cannot redeem it. |
| Undo is another mutation | Every compensation requires current domain permission, ccs:repair and a separate COMPENSATION approval bound to the original receipt. A forward approval cannot authorize undo. Unknown forward/repair attempts cannot be reapproved for replay. |
| A mutable receipt cannot redirect undo | The intent transaction compares the requested receipt with durable effect evidence; the handler gets a detached copy. Concurrent receipt edits cannot change its target or overwrite the approved historical receipt. |
| Expiry/revocation cannot erase a returned effect | Losing principal/credential/approval authority after intent does not prevent persisting the valid forward or repair outcome. New mutations still require fresh authority; worker lease/CAS fencing remains in force. |
| Reconciliation does not bypass repair authorization | Status queries require authentication, domain grants and ccs:reconcile. Missing legacy policy additionally requires ccs:legacy-reconcile. Returned facts can persist after query-time revocation, but any new repair is checked independently. |
| Security state survives restart | Process exits after consumed dispatch intent or simulated apply retain consumed approval and UNKNOWN evidence, without replay. Clock high-water marks prevent observed expiry from being resurrected by a backward clock or restart. |
| Bearer secrets do not enter the journal or normal representations | Credentials use independent random IDs and 32-byte random secrets. Only SHA-256 verifiers persist; fixed-format comparisons use secrets.compare_digest. Context/request/credential repr excludes bearer secrets. SQLite dump tests check both full tokens and secret suffixes. |

The focused credential/admin and durable integration suites contain 74 cases.
Earlier action, lease and workflow fixtures explicitly provision **SIMULATED**
principals and issue repair approvals through the real authority. Their test
helper automates an operator decision only for fake effects; no authorization
check or production execution loop is bypassed. Existing migration fixtures
explicitly remove newer tables to represent their older simulated formats.

## Authority model and race semantics

The authority lives in the existing journal, without an extra service or
database. Principals have active status, permissions and a security version.
Credentials have a public lookup ID, a verifier, expiry and permanent revocation
state. Updating principal grants/status increments its version and invalidates
outstanding approvals. Disabling a principal also permanently revokes its
existing credentials; re-enabling requires newly issued credentials.

Approvals expire within five minutes, identify both issuer and execution
principal, record both principal versions, and reference the issuer credential.
Issuing an approval requires ccs:approve plus the operation's current recorded
domain permissions. Revoking one also requires current operation permissions.
Consumption rechecks issuer status/version/grants and credential expiry/revocation.
Credential lifetimes are bounded to 24 hours. Equality with an expiry time means
expired. Regranting the same permissions does not resurrect an older approval.

The linearization point is the committed intent transaction. If revocation
commits first, the executor is denied and the action remains PENDING (or repair
NOT_STARTED). If intent commits first, later revocation cannot retract that
authorization or stop a request already authorized for dispatch. A process may
pause after commit and send the request later; a local credential or lease
cannot fence a remote provider. Preserve the unresolved operation and use the
selected provider's reconciliation guarantees before any retry or repair.

Consumed approvals stay consumed after success, unknown outcome and a known
no-effect rejection. They are never reset as retry machinery. A genuinely new,
safely eligible operation needs a new identity/approval. A missing repair approval
fences automatic compensation before COMPENSATION_INTENT; an available callback
does not itself grant authority to execute it. Task repair parks rather than
advancing a generation when authority is unavailable.

Denied authorization rolls back action, approval and audit changes. Only its
observed clock high-water marks are committed separately to prevent clock
rollback resurrection. Successful authentication/admin/approval/intent events
retain non-secret actor IDs, versions, direction and binding metadata. A full
denial-monitoring/retention service is not implemented yet. The local disk,
locking/flush, trustworthy shared host clock and worker lease assumptions from
the prior phases still apply.

## Provisioning and host API

Provisioning, grant updates and credential issuance/revocation are **trusted
offline administration APIs**. Never expose them as model tools, transcript
commands or unauthenticated network routes. This phase authenticates possession
of a provisioned host credential; it does not implement verified human identity,
password login, MFA, browser sessions, OAuth/OIDC or trusted transport.

```python
from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import AuthorizationContext

journal = SQLiteActionJournal(journal_path)
# Trusted administrator, outside conversational/model input:
journal.security.provision_principal("worker", {"fleet:write", "ccs:reconcile"})
worker = journal.security.issue_credential("worker", ttl_ms=3600000)
# Deliver/store worker.token through the host's protected secret mechanism.
# Do not print it, put it in URLs, commit it, or serialize the context.
context = AuthorizationContext(credential=worker.token)
# Supply context to the saga, with explicit workflow and worker lease ownership.

# An authenticated, separately provisioned approver reviews the exact action:
context.approval_id = journal.security.issue_approval(
    approver_token, "worker", action.action_id, direction="FORWARD", ttl_ms=60000)
```

The example does not select a provider, start a worker or grant repair authority.
An actual repair additionally requires the executor/approver's domain grants,
ccs:repair and its own approval ID in `context.repair_approval_id`. Explicit
`auth_context=` overrides are available on compensation and reconciliation so a
trusted host can supply a current operator context. The stock LiveKit entrypoint
still supplies neither a provisioned credential nor supervised lease ownership;
mutation remains denied there.

The bearer verifier design applies to high-entropy random tokens, not human
passwords. Authority objects, raw SQL, provisioning methods and Python process
memory remain inside the trusted host boundary. This is not protection from a
compromised administrator, malicious Python code or unauthorized database access.
Use OS access controls and an appropriate secret manager. Request/result and
actor metadata in the journal can be sensitive even without bearer secrets.

Workflow/provider scope is checked against the recorded declarations where a
task checkpoint supplies them. Lower-level unowned actions have no verified
live provider scope. These declarations do not prove the credentials, endpoint,
tenant or region of a real provider; that adapter binding remains a release
blocker. No executable handler is deserialized from the journal. Approver and
executor may be separate principals, but a two-person approval policy is not
enforced automatically by this API.

## Schema 5, migration and rollback

Fresh journals initialize schema 5. Normal opens of schemas 1 through 4 fail;
explicit offline `migrate=True` preserves existing actions, events, claims,
workflow contracts, task checkpoints and worker generations. Schema 3/4 workers
must be stopped and their leases released/expired. Security tables start empty;
migration never invents credentials, permissions or approvals. Old actions with
no recorded authorization policy cannot acquire new dispatch/repair authority.
Their evidence remains inspectable, and legacy status queries require explicit
legacy-reconciliation authority. No live journal was migrated during this work.

Back up the stopped database with the exclusive-file, integrity-checked procedure
in [task-operation ownership](task-operation-ownership.md), choosing a fresh name
such as `.before-schema5.sqlite3`. Preserve its ACLs and sidecars. PowerShell:

```powershell
.\.venv\Scripts\python.exe -c "import os; from runtime.action_journal import SQLiteActionJournal; SQLiteActionJournal(os.environ['CCS_ACTION_JOURNAL'], migrate=True)"
.\.venv\Scripts\python.exe -m pytest tests/test_security_authority.py tests/test_durable_security.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe eval/adversarial_scorecard.py
```

On regression, stop dispatch and preserve the schema-5 journal, approvals,
security events, workflow contracts, operation IDs and provider evidence. Keep a
schema-5 reader available. Do not clear consumed/revoked approvals, reset security
versions, restore old credential validity, remove claims or switch to volatile
simulation. Restore an old backup only if no subsequent dispatch/evidence would
be lost; otherwise reconcile the current journal. Code/schema rollback, process
termination, credential revocation and cancellation do not reverse remote effects.

## Validation, sources and next control-surface gate

Local Windows full suite: **464 passed, 1 skipped**. Exact-commit
fresh-checkout/new-environment and Windows/Linux CI evidence is recorded in the
draft PR. All 74 focused cases pass. The legacy
**SIMULATED** scorecard remains 15/20 completed trials, with five restart trials
parked. Its zero observed duplicate/false commitments are fixture counts, not
production rates. The live voice test remains a skipped placeholder. No live
provider, production deployment, throughput or latency result is claimed.
Transitive dependency locking remains outstanding.

The design uses [Python's security-token primitives](https://docs.python.org/3.13/library/secrets.html),
follows [OWASP's operation-specific transaction authorization guidance](https://cheatsheetseries.owasp.org/cheatsheets/Transaction_Authorization_Cheat_Sheet.html)
and [deny-by-default/current-request authorization guidance](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html),
and relies on [SQLite's serialized write transactions](https://sqlite.org/isolation.html).
These sources inform the design; they do not certify this implementation.

The next UI/control phase should keep one modular host process and this journal
as the authority. Acceptance requires authenticated, scoped read/control APIs;
task/workflow and action-evidence views; an approval inbox showing exact payload,
provider scope, actor, expiry and direction; explicit cancellation versus remote
effect status; and recovery controls that never blindly retry UNKNOWN. Client
buttons must reflect fresh server eligibility, while the backend independently
rechecks every action. The frontend must not store the production bearer token
in browser local storage, manufacture task state, hide stale/offline data, or
present simulated confidence/effects as real.

Responsive layout, keyboard/screen-reader access, stale/error/loading states and
browser interaction tests belong in that phase. A tested lease supervisor and
shutdown protocol, secure transport/session/CSRF controls, identity-provider
integration, real STT confidence/revisions, provider sandbox selection and
independent operational evaluation remain release gates. No UI implementation
or complete production-readiness claim is made by this security PR.

## Worker discovery of a console approval

`SecurityAuthority.find_forward_approval(executor_token, action_id)` is a
non-consuming lookup for a currently authenticated worker with the recorded
operation grants. It validates the pending action, open chain, task projection,
exact binding, executor version, issuer authority and expiry. It selects only
the latest committed console APPROVE decision for that action; a revoked,
expired, consumed or incompatible latest decision never falls back to an older
grant. Direct offline approvals remain available to explicitly configured trusted
callers but are not discovered by this console-driven path.

Polling does not issue approvals or append authentication/audit events. It updates
the existing security clock high-water mark. The returned ID is not a reservation
or permission snapshot: dispatch rechecks all authority and consumes approval
inside the existing DISPATCH_INTENT transaction. Revocation, chain closure, epoch
change or permission loss after lookup can still deny dispatch. No journal schema
or authorization semantics change is needed. Regression cases use real SQLite
and console decisions with SIMULATED provider callbacks.
