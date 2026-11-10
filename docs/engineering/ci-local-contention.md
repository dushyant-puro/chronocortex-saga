# CI notification investigation: local journal contention

The notifications were workflow failures, not a missing frontend. The latest
published source `515949a` subsequently passed all ten push/PR jobs; that does not
erase earlier failures or prove they were repaired.

## Verified failed jobs

| Source/run | Failed job and evidence |
| --- | --- |
| `6d575a0`, PR 42676390426 | Linux browser 129358773617: replacement eligibility read, `SQLITE_BUSY` at CONFIGURE with a local peer at COMMIT; 78 passed/1 failed. |
| `df55191`, PR 42675482535 | Windows browser 129355579009: manual walkthrough READ_ACTIONS at BEGIN versus a local STAGED/COMMIT; 78 passed/1 failed. |
| Same PR | Linux runtime 129355579443: opening a journal during manual admission, CONFIGURE versus STAGED/COMMIT; 1,637 passed/1 failed/38 skipped. |
| Same PR | Linux browser 129355579505: login/overview with CONFIGURE versus local GENERAL/COMMIT; 78 passed/1 failed. |
| `df55191`, push 42675474913 | Linux browser 129355553719: supervised console fixture with CONFIGURE versus GENERAL/COMMIT; 78 passed/1 failed. |

Redacted logs are preserved locally as `results/ci-job-<job-id>.redacted.log`,
with the inventory in `results/ci-notification-failures.json`. The screenshots
omit run IDs; the first matches the verified single Linux-browser failure and
the second matches the three-job PR failure. This is a match from job outcomes,
not access to the user's mailbox.

## Correction and invariant

Every resolved local journal path now shares a bounded process-local lock across
connection configuration, BEGIN, transaction body, COMMIT/rollback and close.
Previously independent local connections could configure PRAGMAs or begin writes
while another thread committed. The new gate avoids that overlap before opening
a second connection. It is not a distributed lock: SQLite leases, SQL CAS,
ownership checks, security clocks and provider uncertainty rules remain decisive.
External processes and raw SQLite clients remain subject to SQLite contention.

Gate acquisition now has an independent `local_timeout` (default **5 seconds**),
while SQLite's `timeout` remains **0.25 seconds**. The first version reused the
quarter-second value for both; Windows CI showed LOCAL_QUEUE_TIMEOUT even after
SQL connection setup was serialized. That first correction was insufficient.
These are separate bounded stages, not a new total request deadline or a latency
SLO. The queue policy was explicitly revised; SQL/lease/watchdog timeouts were
not increased. No transaction was retried, provider mutation replayed or durability
mode weakened. Self-nested requests refuse immediately because their owner cannot
make progress while waiting. A timed-out waiter opens no
connection and runs no transaction body. `LOCAL_QUEUE_TIMEOUT` / `WAIT_LOCAL`
observations distinguish this from real SQLITE_BUSY; logs still omit paths,
payloads and bearer values. A weak registry avoids retaining abandoned paths.

Follow-up job evidence includes 129838187003, 129838187020, 129838505915,
129839371683 and 129839401287: Windows runtime/browser queue timeouts at the old
quarter-second bound, plus a test-only HTTP ReadTimeout. The process test now
uses the console's existing ten-second request boundary instead of an unsupported
one-second test-only SLO. No test assertions, authority controls or unknown-effect
checks are skipped. Original failures remain recorded; counterpart passes are
not substitutions for failed jobs.

Regression tests cover shared-path serialization, independent journals, bounded
refusal, rollback/release after body failure, nested refusal and diagnostics.
Existing external-process locks, blocked COMMIT rollback, worker fencing,
cancellation and adversarial authority tests remain required. Full exact-source
qualification is recorded below after testing; a passing run is not proof that
all future resource contention is impossible.

The unfinished managed-service implementation is preserved in Git stash with
message `Preserve unfinished managed-service work before CI investigation` on
branch `codex/managed-recovery-service`. Do not drop it or overwrite its files.
It must be integrated and tested after this correctness fix; no native service
has been installed, started or qualified.

## Separate replacement-control synchronization defect

Revised-policy Windows push browser job 129846095061 recorded 78 passed/1 failed
in `test_explicit_cancel_inspect_review_accept_creates_one_linked_task`, with the
replacement-prepare button unexpectedly enabled. This log had no queue/SQLite
contention observation. The review controller's state already rejected another
preparation, but its dependent DOM controls were refreshed only by the next
five-second journal poll. The test's five-second assertion exposed that gap.

A new regression holds the next snapshot response, opens the review, checks that
preparation disables, then closes the review and checks it re-enables without
receiving another snapshot. It failed before the correction. Proposal rendering
now notifies replacement/retained-review controls immediately; no polling or
assertion timeout changed. All 16 replacement/reauthorization journeys passed
afterward. Server authority and single replacement-claim checks are unchanged.

## Qualification and rollback

Clean policy implementation `4cb3b42` passed **1,686 runtime tests, 2 skipped**,
all **79 then-existing browser cases**, both profile audits and SIMULATED process
faults **5/5 cases, 122 checks**. Existing audited locked environments were reused.
Its Windows push browser failure above remains a failure, not a pass substituted
from local or PR results. The later UI-only correction adds one held-snapshot
journey; its full browser and hosted outcomes belong to PR #45. Python runtime,
worker, provider, acceptance, security and operational backends are unchanged by
that UI-only correction. The process-drill fingerprint is now
`c104faaf5aefc14ab87be584c4aa5b485fe57d3e2818bffe6bda22620935d756` because
the harness includes journal-observation source, which this change extends.
The evaluator itself and its 122 checks were not relaxed.

Keep earlier failed jobs, local qualification and current hosted outcomes
separate. Final results and redacted JUnit are retained in ignored `results/`
and CI artifacts; release qualification and all seven readiness areas remain
open. No real provider or microphone is used in these checks.

There is no schema migration. On regression stop affected admission/dispatch,
preserve the journal/claims/receipts and use a compatible reader or forward fix.
Reverting the queue/control code can reintroduce contention/stale affordances;
it does not undo remote effects. Never clear UNKNOWN, remove claims or retry
a mutation to obtain a passing run.
