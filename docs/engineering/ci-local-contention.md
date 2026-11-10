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
