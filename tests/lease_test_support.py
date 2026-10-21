"""Legacy boundary fixtures deliberately share ONE simulated worker capability.

These fixtures test action CAS/chain ownership, not competing worker ownership.
Lease-specific tests construct independent real capabilities instead. The crash
fixture uses the same explicit trusted test worker; this is not restart takeover.
"""
from runtime.action_journal import SQLiteActionJournal, WorkerLease


def shared_test_journal(path, journal_type=SQLiteActionJournal):
    journal = journal_type(path)
    if not journal.worker_events():
        journal.acquire_lease("SIMULATED-test-worker", ttl_ms=3_600_000)
    else:
        journal.bind_lease(WorkerLease("SIMULATED-test-worker", 1))
    return journal
