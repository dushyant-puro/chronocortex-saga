"""Single-host worker ownership, bounded drain, and durable heartbeat observations.

This supervises trusted Python adapters using the journaled saga entrypoints.
It cannot interrupt a hung OS call or fence a provider request already authorized.
"""
import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
import inspect
import math
import threading
import time
import uuid

from runtime.action_journal import JournalError, JournalLeaseError
from runtime.policy_engine import AuthorizationContext, PermissionDeniedError


class WorkerUnavailable(JournalLeaseError):
    """This runtime cannot admit new work. Construct a fresh host for recovery."""


@dataclass(frozen=True)
class WorkerSettings:
    lease_ttl_ms: int = 30_000
    renew_interval_ms: int = 5_000
    safety_margin_ms: int = 1_000
    drain_timeout_ms: int = 10_000
    max_operations: int = 32

    def __post_init__(self):
        values = (self.lease_ttl_ms, self.renew_interval_ms, self.safety_margin_ms,
                  self.drain_timeout_ms, self.max_operations)
        if any(type(value) is not int for value in values):
            raise ValueError("Worker settings require exact integers")
        if not (1000 <= self.lease_ttl_ms <= 86_400_000 and
                0 < self.safety_margin_ms < self.lease_ttl_ms and
                0 < self.renew_interval_ms * 2 < self.lease_ttl_ms - self.safety_margin_ms and
                1 <= self.drain_timeout_ms <= 600_000 and 1 <= self.max_operations <= 1024):
            raise ValueError("Invalid worker timing or capacity settings")


@dataclass(frozen=True)
class StopReport:
    state: str
    drained: bool
    lease_released: bool
    pending_operations: int
    failure_code: str | None


class WorkerOperation:
    """Consumer cancellation ends its wait, not the owned durable operation."""
    def __init__(self, task):
        self.id = uuid.uuid4().hex  # host observation ID, not a durable action ID
        self._task = task

    async def wait(self):
        return await asyncio.shield(self._task)

    def done(self):
        return self._task.done()

    def result(self):
        return self._task.result()


class _Lifecycle:
    def __init__(self, host):
        self.host = host
        self.lock = threading.RLock()
        self.state = "NEW"
        self.deadline = None
        self.failure = None
        self.tasks = {}

    def now(self):
        value = self.host._monotonic()
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise WorkerUnavailable("Invalid worker monotonic clock")
        return value

    def fault(self, code):
        with self.lock:
            if self.failure is None:
                self.failure = code
            if self.state not in {"STOPPED", "RECOVERY_REQUIRED"}:
                self.state = "FAILED"
        if self.host._loop is not None:
            self.host._loop.call_soon_threadsafe(self.host.request_stop)

    def check(self, db=None):
        with self.lock:
            if self.state != "READY":
                raise WorkerUnavailable("Worker is not accepting new work")
            try:
                if self.deadline is None or self.now() >= self.deadline:
                    raise WorkerUnavailable("Worker renewal deadline elapsed")
            except WorkerUnavailable:
                self.fault("MONOTONIC_DEADLINE")
                raise
            if db is not None:
                try:
                    authority = self.host.journal.security
                    actor = authority._authenticate(db, self.host._credential, authority._now(db))
                    if "ccs:work" not in actor.permissions:
                        raise PermissionDeniedError("Current worker permission required")
                except PermissionDeniedError:
                    self.fault("AUTHORITY_LOST")
                    raise

    def renewed(self, began):
        with self.lock:
            deadline = began + (self.host.settings.lease_ttl_ms - self.host.settings.safety_margin_ms) / 1000
            if self.state not in {"STARTING", "READY", "DRAINING"} or self.now() >= deadline:
                raise WorkerUnavailable("Late renewal cannot restore this runtime")
            # A late reply must not resurrect a deadline already crossed.
            if self.deadline is not None and self.now() >= self.deadline:
                raise WorkerUnavailable("Worker renewal arrived after its deadline")
            self.deadline = deadline

    @contextmanager
    def operation(self, kind):
        if asyncio.get_running_loop() is not self.host._loop:
            raise WorkerUnavailable("Worker operations must use the owning event loop")
        task = asyncio.current_task()
        with self.lock:
            # Status queries also require current host authority. Mutations
            # repeat this check atomically with their journaled intent.
            with self.host.journal._transaction() as db:
                self.check(db)
            active = self.host._active_tasks()
            if task not in active and len(active) >= self.host.settings.max_operations:
                raise WorkerUnavailable("Worker operation capacity reached")
            self.tasks[task] = self.tasks.get(task, 0) + 1
        try:
            yield
        finally:
            with self.lock:
                self.tasks[task] -= 1
                if not self.tasks[task]:
                    del self.tasks[task]


class WorkerSupervisor:
    """Own one fresh journal/runtime; never reacquire into cached Python state.

    Factory is trusted, synchronous construction/hydration only. It must not
    launch unowned provider calls. All mutations use saga/journal entrypoints.
    """
    def __init__(self, journal, credential, *, settings=None, owner=None, monotonic=None):
        self.settings = settings or WorkerSettings()
        self.journal = journal
        self._credential = credential
        self.owner = owner or f"worker:{uuid.uuid4().hex}"
        self._monotonic = monotonic or time.monotonic
        self._gate = _Lifecycle(self)
        journal.install_lifecycle(self._gate)
        self._loop = None
        self._runtime = None
        self._lease = None
        self._start_task = None
        self._stop_task = None
        self._heartbeat_task = None
        self._submitted = set()
        self._heartbeat_stop = asyncio.Event()
        self.stopping = asyncio.Event()
        self.stopped = asyncio.Event()
        self._pulse_lock = asyncio.Lock()
        self.report = None

    @property
    def state(self):
        with self._gate.lock:
            return self._gate.state

    @property
    def runtime(self):
        self._gate.check()
        return self._runtime

    def _active_tasks(self):
        with self._gate.lock:
            return {task for task in self._submitted | self._gate.tasks.keys() if not task.done()}

    def _observation(self):
        return {"phase": self.state, "active_operations": len(self._active_tasks()),
                "failure_code": self._gate.failure,
                "heartbeat_ttl_ms": self.settings.renew_interval_ms * 2 + self.settings.safety_margin_ms}

    async def _observe(self, kind):
        details = self._observation()
        if kind in {"HOST_STOPPED", "HOST_ABANDONED"}:
            details["phase"] = "STOPPED" if kind == "HOST_STOPPED" else "RECOVERY_REQUIRED"
        await asyncio.to_thread(self.journal.observe_worker, kind, details)

    def _authorize_host(self):
        actor = self.journal.security.authenticate(self._credential)
        if "ccs:work" not in actor.permissions:
            raise PermissionDeniedError("Worker credential requires ccs:work")
        return actor

    async def start(self, factory):
        if self._start_task is not None or self.state != "NEW" or not callable(factory):
            raise WorkerUnavailable("Start requires a new supervisor and trusted runtime factory")
        self._loop = asyncio.get_running_loop()
        self._gate.state = "STARTING"
        self._start_task = asyncio.create_task(self._start(factory), name="ccs-worker-start")
        try:
            return await asyncio.shield(self._start_task)
        except asyncio.CancelledError:
            self.request_stop()
            raise
        except Exception:
            self.request_stop()
            await asyncio.shield(self._stop_task)
            raise

    async def _start(self, factory):
        try:
            await asyncio.to_thread(self._authorize_host)
            if self.state != "STARTING":
                raise WorkerUnavailable("Startup was stopped")
            began = self._gate.now()
            self._lease = await asyncio.to_thread(self.journal.acquire_lease, self.owner, ttl_ms=self.settings.lease_ttl_ms)
            self._gate.renewed(began)
            await self._observe("HOST_STARTING")
            self._runtime = factory(self.journal, AuthorizationContext(credential=self._credential))
            if inspect.isawaitable(self._runtime):
                if inspect.iscoroutine(self._runtime):
                    self._runtime.close()
                raise WorkerUnavailable("Runtime factory must only construct synchronously")
            with self._gate.lock:
                if self.state != "STARTING" or self._gate.now() >= self._gate.deadline:
                    raise WorkerUnavailable("Startup did not retain worker authority")
                self._gate.state = "READY"
            await self._observe("HOST_READY")
            if self.state != "READY":
                raise WorkerUnavailable("Startup was stopped")
            self._heartbeat_task = asyncio.create_task(self._heartbeat(), name="ccs-worker-heartbeat")
            return self._runtime
        except BaseException:
            self._gate.fault("STARTUP_FAILED")
            raise

    def submit(self, operation):
        if asyncio.get_running_loop() is not self._loop or not callable(operation):
            raise WorkerUnavailable("Submit a callable on the owning event loop")
        self._gate.check()
        if len(self._active_tasks()) >= self.settings.max_operations:
            raise WorkerUnavailable("Worker operation capacity reached")
        async def run():
            # Stop can win after admission but before this coroutine starts.
            self._gate.check()
            return await operation(self._runtime)
        task = asyncio.create_task(run(), name="ccs-worker-operation")
        self._submitted.add(task)
        def completed(done):
            self._submitted.discard(done)
            if not done.cancelled():
                done.exception()  # retain on handle; avoid unobserved exception logs
        task.add_done_callback(completed)
        return WorkerOperation(task)

    async def pulse(self):
        """One renewal/heartbeat. A failed or late attempt is never retried."""
        async with self._pulse_lock:
            if self.state not in {"READY", "DRAINING"}:
                raise WorkerUnavailable("Worker cannot renew")
            try:
                began = self._gate.now()
                if began >= self._gate.deadline:
                    raise WorkerUnavailable("Worker renewal deadline elapsed")
                await asyncio.to_thread(self._authorize_host)
                await asyncio.to_thread(self.journal.renew_lease, ttl_ms=self.settings.lease_ttl_ms)
                self._gate.renewed(began)
                await self._observe("HOST_HEARTBEAT")
            except BaseException:
                self._gate.fault("RENEWAL_FAILED")
                raise

    async def _heartbeat(self):
        try:
            while not self._heartbeat_stop.is_set():
                try:
                    await asyncio.wait_for(self._heartbeat_stop.wait(), self.settings.renew_interval_ms / 1000)
                except asyncio.TimeoutError:
                    if not self._heartbeat_stop.is_set():
                        await self.pulse()
        except BaseException:
            self._gate.fault("RENEWAL_FAILED")

    def request_stop(self):
        """Call on the event loop; signal handlers should schedule this callback."""
        if self._loop is None:
            raise WorkerUnavailable("No running worker loop")
        with self._gate.lock:
            if self.state in {"NEW", "STARTING", "READY"}:
                self._gate.state = "DRAINING"
        self.stopping.set()
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._shutdown(), name="ccs-worker-stop")

    async def stop(self):
        self.request_stop()
        if asyncio.current_task() in self._active_tasks():
            raise WorkerUnavailable("Await shutdown outside owned worker operations")
        return await asyncio.shield(self._stop_task)

    async def _shutdown(self):
        # Owned startup/renewal I/O is awaited, never abandoned by cancellation.
        # An OS/disk hang can therefore exceed the asynchronous drain budget.
        if self._start_task is not None:
            await asyncio.gather(self._start_task, return_exceptions=True)
        if self._lease is not None:
            try:
                await self._observe("HOST_DRAINING")
            except (JournalError, PermissionDeniedError):
                self._gate.fault("OBSERVATION_FAILED")
        pending = self._active_tasks()
        if pending:
            await asyncio.wait(pending, timeout=self.settings.drain_timeout_ms / 1000)
        self._heartbeat_stop.set()
        if self._heartbeat_task is not None:
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
        pending = self._active_tasks()
        released = False
        if self._lease is not None:
            try:
                await self._observe("HOST_ABANDONED" if pending else "HOST_STOPPED")
                if not pending:
                    await asyncio.to_thread(self.journal.release_lease)
                    released = True
            except (JournalError, PermissionDeniedError):
                self._gate.fault("STOP_PERSISTENCE_FAILED")
        drained = not pending
        self._gate.state = "STOPPED" if drained and (released or self._lease is None) else "RECOVERY_REQUIRED"
        self.report = StopReport(self.state, drained, released, len(pending), self._gate.failure)
        self.stopped.set()
        return self.report
