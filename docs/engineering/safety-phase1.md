# Safety phase 1: task outcomes must follow action evidence

Baseline: published `phase-13-adversarial-eval`,
`e789c439ead141c125edca416da0f370999541f1`.
Public `main` remains the older Phase 0 implementation. This branch is a
research implementation, not a production-readiness claim.

## Scope and invariants

This first safety PR addresses the production task/action boundary. It does
not claim to complete the entire safety roadmap.

| Invariant | Behavior | Regression file |
| --- | --- | --- |
| TASK-EVIDENCE | Only an action recorded as COMMITTED advances its task step. A returned result, COMMITTED_STALE, or COMPENSATED is insufficient. | `tests/test_task_safety_invariants.py` |
| TASK-UNKNOWN | Resume and grounding callbacks preserve unresolved action identity, step index, result evidence, and earlier progress. Missing/unavailable status checks park the task. | same |
| TASK-CANCEL | Explicit cancellation cannot become completion when the last dispatched write returns. Coroutine cancellation preserves unresolved dispatched writes and propagates CancelledError. | same |
| TASK-OWNER-LOCAL | Run and resume share a per-task asyncio lock; unrelated tasks can run concurrently. This is process-local serialization, not distributed fencing. | same |
| TASK-AUTH | Confirmation denial surfaces and parks for authorization. Permission denial surfaces without automatically compensating earlier effects. | same |
| TASK-RESTORE | A new process's turn counter cannot prove a legacy checkpoint's intent is current. Positive remote status alone cannot unblock dependent work. | `tests/test_phase7_persistence.py`, `tests/test_phase13_adversarial_eval.py` |

New task states are appended to the enum to preserve existing numeric values:
`NEEDS_RECONCILIATION` and `WAITING_FOR_AUTHORIZATION`. Checkpoints serialize
state names. Older code cannot read these new names; do not downgrade a live
checkpoint store to an older version without an explicit migration.

## Outcomes

- Current COMMITTED action: record its result and advance.
- Unknown, stale, compensated, missing action/evidence, or unavailable status:
  preserve the original step as NEEDS_RECONCILIATION and stop descendants.
- Timeout: preserve prior committed steps; do not assume failure or undo them.
- Confirmation required: WAITING_FOR_AUTHORIZATION, original staged action
  remains PENDING. Resume with caller-supplied approval reuses that action.
- Permission denied: surface the error, fail the task, retain prior effects.
- User cancellation with an in-flight write: retain CANCELLED as user intent,
  record the step's outcome separately, and require reconciliation/repair.

When a legacy checkpoint has no live action ledger record, reconstruction uses
an invalid epoch sentinel (-1), not the new process's current epoch. The saga
therefore classifies a confirmed effect as COMMITTED_STALE (not valid as current).
The task stays parked even if compensation subsequently succeeds. Safe automatic
continuation requires durable validity/provenance and an explicit repair policy,
which belong to subsequent work. In particular, automatic retry of a parked step
is not implemented by this PR. Operators must not edit checkpoint states to resume.

## Evaluation integrity

The adversarial test harness now inherits the production `run_task` loop; it
only binds test executors and policy. Its previous copied task loop concealed
the production policy-swallowing defect. The scorecard is SIMULATED, comprises
four scenarios repeated five times, and is not a provider or crash-safety oracle.
Legacy restart scenarios now remain safely parked rather than reporting task
completion. Do not relabel safe parking as recovery to preserve an old score.

The original policy-swallowing script is retained as a historical bug reproducer:
its exit code 1 means the old bug did not reproduce. Regression acceptance is
the pytest suite, which checks the replacement behavior and actual effects.

## Reproduce on Windows PowerShell

From a fresh checkout of this branch:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe -m eval.adversarial_scorecard
```

No credentials are needed for deterministic tests. The existing live-session
test is a placeholder, not integration evidence. CI runs the suite on Windows
and Linux with Python 3.13. Top-level requirements are pinned; transitive
dependencies are not yet locked. Record installed versions when comparing runs.

## Remaining release blockers, in order

1. Generic post-dispatch exceptions were the next blocker at this PR's baseline.
   The stacked change described in [ambiguous-write-outcomes.md](ambiguous-write-outcomes.md)
   preserves unknown unless a trusted adapter supplies non-execution evidence.
2. Process-local repair ownership is addressed by the stacked change in
   [reconciliation-ownership.md](reconciliation-ownership.md). Distributed ownership
   and durable compensation intent are still required.
3. Authorization remains optional/fail-open at the saga boundary. Make mutation
   policy mandatory, fail closed, and bind approval to immutable arguments.
4. Intent is not durably saved before dispatch; checkpoint errors are suppressed;
   action identity/validity is not a transactional journal. Implement the store
   protocol before claiming crash-safe mutation execution.
5. Replanning and cancellation compensation need their own durable lifecycle,
   per-task chain boundaries, evidence, and unknown-compensation handling.
6. Distributed leases/CAS/fencing, real providers, ASR revision/confidence wiring,
   security/operations and independent benchmark oracles remain unverified.

SIMULATED tools remain the only mutation backend. REAL PROVIDER execution is not
approved by passing this suite. Never blindly retry an unresolved mutation.

## Rollback

Keep the published Phase 13 branch and public main intact. Do not merge this PR
automatically. If a regression appears, stop new mutation dispatch and preserve
task/action records for inspection; reverting code cannot undo remote effects.
Keep this reader version available for checkpoints containing the new state names.
Do not restore an older checkpoint and replay its mutations. Local locks offer
no protection against another process or a stale worker.
