"""Bound local connection setup and transactions; SQLite retains process authority."""
import os
import math
import threading
import weakref


class Gate:
    def __init__(self):
        self.lock = threading.Lock()

    def acquire(self, timeout):
        try:
            value = float(timeout)
        except (TypeError, ValueError, OverflowError):
            raise ValueError('Invalid local queue timeout') from None
        if not math.isfinite(value) or value > threading.TIMEOUT_MAX:
            raise ValueError('Invalid local queue timeout')
        # SQLite treats a negative timeout as no wait; threading uses -1 for
        # an unlimited wait. Never inherit that incompatible interpretation.
        return self.lock.acquire(timeout=max(0, value))


_guard = threading.Lock()
_gates = weakref.WeakValueDictionary()


def for_path(path):
    key = os.path.normcase(str(path))  # Journal paths are already resolved.
    with _guard:
        gate = _gates.get(key)
        if gate is None:
            gate = Gate()
            _gates[key] = gate
        return gate
