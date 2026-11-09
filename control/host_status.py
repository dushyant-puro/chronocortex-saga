"""Current recovery-host observation; never dispatch/restore authorization."""
import asyncio

from runtime.policy_engine import PermissionDeniedError
from runtime.worker_supervisor import WorkerSupervisor


class HostStatusController:
    def __init__(self, host, credential):
        if type(host) is not WorkerSupervisor:
            raise TypeError('A supervised recovery host is required')
        self.host, self._credential = host, credential
        self.failed = False

    def _probe(self):
        host = self.host
        runtime = host.runtime  # checks the local monotonic lifecycle gate
        if len(runtime.registry):
            raise PermissionDeniedError('Recovery status requires an empty tool registry')
        with host.journal._transaction() as db:
            host.journal._require_lease(db)
            now = host.journal.security._now(db)
            actor = host.journal.security._authenticate(db, self._credential, now)
            if 'ccs:work' not in actor.granted_permissions:
                raise PermissionDeniedError('Current worker authority required')
        host.runtime  # stop/deadline may have won while SQL was awaiting

    async def inspect(self):
        ready = False
        try:
            await asyncio.to_thread(self._probe)
            self.host.runtime
            ready = True
        except Exception:
            # Same fail-closed operational policy as heartbeat failure. No
            # repeated probe, renewal retry or lease reacquisition is attempted.
            self.failed = True
            self.host.request_stop()
        return {'schema': 1, 'mode': 'RECOVERY_ONLY', 'ready': ready,
                'state': self.host.state, 'provider': 'NOT_CONFIGURED',
                'voice': 'NOT_CONFIGURED', 'dispatch_authorized': False,
                'restore_authorized': False, 'provider_reconciled': False,
                'freshness_verified': False}
