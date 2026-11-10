"""Bound local connection setup and transactions; SQLite retains process authority."""
import os
import threading
import weakref


class Gate:
    def __init__(self):
        self.lock = threading.Lock()


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
