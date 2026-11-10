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

Gate acquisition uses the journal's existing timeout; SQL busy handling retains
its existing timeout too. These are separate bounded stages, not a new total
request deadline. No existing timeout was raised, transaction retried, provider
mutation replayed or durability mode weakened. A timed-out waiter opens no
connection and runs no transaction body. `LOCAL_QUEUE_TIMEOUT` / `WAIT_LOCAL`
observations distinguish this from real SQLITE_BUSY; logs still omit paths,
payloads and bearer values. A weak registry avoids retaining abandoned paths.

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
