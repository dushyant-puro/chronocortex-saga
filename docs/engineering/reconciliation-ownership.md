# Safety phase 1c: reconciliation and compensation ownership

Stacked on `production/ambiguous-write-outcomes` at
`c6433064dd6ef63a399f29c23161e5389174601f`. Public main remains Phase 0.
All external effects exercised here are SIMULATED. No REAL PROVIDER or
distributed execution guarantee is established.

## Protected invariants

- One process-local owner resolves an action at a time. Direct reconciliation,
  automatic stale compensation, chain unwind, task cancellation, task failure
  recovery and task replanning use the same action-scoped lock.
- At most one automatic compensation attempt is dispatched for an action in
  this process. A timeout-like error or cancellation does not justify a retry.
- A compensation outcome cannot be inferred from the original effect still
  being present. A delayed earlier compensation may still apply.
- Task success requires COMMITTED with compensation NOT_STARTED. Repair in
  flight or unknown cannot be presented as current task success.
- Missing action evidence or a missing compensator parks write repair. It cannot
  mark a write compensated, erase its operation identity, or redispatch it.
- Reverse-order unwind stops at unresolved repair. Repeated cancellation or
  unwind cannot skip that boundary and undo its ancestors.
- Cancellation cannot undo ancestors while an in-flight descendant write can
  still apply. CANCELLED records intent; unresolved step outcomes remain visible.

## Compensation outcomes

| Before | Evidence | After | Original action |
| --- | --- | --- | --- |
| NOT_STARTED | owner dispatches compensator | IN_FLIGHT | retained |
| IN_FLIGHT | compensator returns successfully under its adapter contract | SUCCEEDED | COMPENSATED |
| IN_FLIGHT | ordinary exception or coroutine cancellation | UNKNOWN | retained; never silently COMPENSATED |
| IN_FLIGHT | MutationNotAppliedError with reference | NOT_APPLIED | retained; explicit repair policy required |
| UNKNOWN / NOT_APPLIED | repeated compensation or positive original-effect status | unchanged | no automatic compensator redispatch |

`compensation_failed` remains for compatibility. New explicit state, error type,
and any redacted non-execution reference are exposed in snapshots. Cancellation
propagates after recording uncertainty. Diagnostic errors remain local; this is
not an append-only audit journal.

## Reconciliation contract

Status callbacks return exactly `True`, `False`, or `None`. Values such as `0`,
`1`, strings, lists and dictionaries are rejected without releasing the action.
The action lock is held across status and repair awaits. A second caller checks
the resolved state after obtaining ownership and does not run a competing query
or append the action to the chain twice.

`True` confirms the original operation occurred. Classification rechecks epoch
validity after the awaited query. It never proves a prior compensation did not
execute or cannot arrive later. `None` preserves uncertainty. `False` requires
authoritative operation-specific absence, including exclusion of delayed future
application; an eventually consistent read miss is insufficient.

For an unknown original operation, negative status yields ABORTED. For a known
applied effect, authoritative absence records COMPENSATED rather than rewriting
history as never-applied. Missing historical compensation evidence stays unknown;
repeated negative forward-status reads cannot invent compensation success.

Completed resolution is idempotent locally. Repeated calls on resolved actions
are no-ops. Callbacks must not re-enter resolution for the same action or its
owning task loop: they are adapter queries/compensators, not orchestration hooks.

## Task integration and recovery

Task replan/run/resume share the task lock. Replanning releases it before
redispatch to avoid recursive locking. Task cancellation remains an immediate
intent signal; a late repair result cannot turn cancellation into redispatch.
An unresolved prior step rewinds the parked index and retains its identity and
result. Read-only invalidation needs no remote undo.

Legacy task snapshots lack durable action and compensation intent. Reconstructed
actions therefore have unverified conversational validity and UNKNOWN
compensation history. They must not automatically repeat a potentially
dispatched historical compensator. Safe automatic continuation after restart
requires the later transactional intent/action/event store phase.

Duplicate staging checks all retained writes for the key, including unresolved
compensation on an older action after a newer completed operation replaced the
index entry. This is an in-memory linear scan, not provider-enforced idempotency
or an implementation with measured production-scale performance.

## Tests and reproduction

`tests/test_reconciliation_ownership.py` uses event-controlled overlap and an
independent record of simulated effects. Ten initial regressions failed against
the parent implementation before the fix. Coverage includes competing and
contradictory status resolutions, background repair, chain/task repair, cancellation,
malformed status, uncertain and rejected compensation, overlapping replans,
legacy restart and older-operation key blocking. Watchdog timeouts bound hangs;
elapsed sleep timing does not select interleavings.

One legacy graph fixture now establishes an actual committed saga action instead
of fabricating only a task step. The old stale-effect negative-status expectation
is corrected from ABORTED to COMPENSATED. The old cancellation test now cancels
the tracked compensation task, not every task in the event loop.

Use the clean-checkout PowerShell setup in [safety-phase1.md](safety-phase1.md):

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_reconciliation_ownership.py -q
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
```

## Remaining blockers and rollback

Locks and attempt metadata are process-local. Durable dispatch intent, transactional
events/CAS, ownership leases/fencing and provider-enforced operation identities
remain required before multi-process deployment. Authorization is still optional
and fail-open in the saga boundary. The legacy manager-wide chain still needs
explicit task boundaries; task repair now addresses action IDs individually.
The bool status callback needs a richer provider-specific evidence contract before
real-provider release. Real voice confidence/revision handling and operations
remain separate work.

Do not merge the stacked PRs automatically. On regression, stop new mutation
dispatch and preserve identities, outcomes and evidence. Do not downgrade readers,
reset UNKNOWN to NOT_STARTED, blindly retry a compensator, or replay an old
checkpoint as rollback. Reverting code does not reverse remote effects.
