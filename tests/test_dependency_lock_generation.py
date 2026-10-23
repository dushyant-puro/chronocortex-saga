"""Failure injection around candidate validation and lock-bundle publication."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import dependency_audit as audit
from scripts import lock_dependencies as generation
from test_dependency_audit import fixture, environment


def prepare(tmp_path, monkeypatch):
    fixture(tmp_path)
    monkeypatch.setattr(generation, "ROOT", tmp_path)
    monkeypatch.setattr(generation.importlib.metadata, "version", lambda _: audit.UV_VERSION)
    original = {path: (tmp_path / path).read_bytes() for path in (*audit.LOCKS.values(), audit.MANIFEST)}
    return original


def resolver(root, *, fail_browser=False):
    def run(command, **kwargs):
        target = Path(command[command.index("--output-file") + 1])
        browser = "browser" in command[5]
        if browser and fail_browser:
            return SimpleNamespace(returncode=1)
        target.write_bytes((root / target.name).read_bytes())
        return SimpleNamespace(returncode=0)
    return run


def test_resolution_failure_never_publishes_half_of_a_candidate_bundle(tmp_path, monkeypatch):
    original = prepare(tmp_path, monkeypatch)
    monkeypatch.setattr(generation.subprocess, "run", resolver(tmp_path, fail_browser=True))
    with pytest.raises(ValueError, match="resolution failed"):
        generation.generate()
    assert all((tmp_path / path).read_bytes() == value for path, value in original.items())


def test_partial_publication_is_detectably_inconsistent_not_silently_accepted(tmp_path, monkeypatch):
    prepare(tmp_path, monkeypatch)
    monkeypatch.setattr(generation.subprocess, "run", resolver(tmp_path))
    replace = generation.os.replace
    def fail_second(source, target):
        if Path(target).name == audit.LOCKS["browser"]:
            raise OSError("SIMULATED_DISK_FAILURE")
        return replace(source, target)
    monkeypatch.setattr(generation.os, "replace", fail_second)
    with pytest.raises(OSError):
        generation.generate()
    with pytest.raises(audit.LockError, match="CHANGED"):
        audit.load_locks(tmp_path, environment())
    assert not any(path.name.startswith("tmp") for path in tmp_path.iterdir())


def test_unpinned_resolver_cannot_start_network_resolution(tmp_path, monkeypatch):
    original = prepare(tmp_path, monkeypatch)
    monkeypatch.setattr(generation.importlib.metadata, "version", lambda _: "0.0.0")
    monkeypatch.setattr(generation.subprocess, "run", lambda *a, **k: pytest.fail("Unpinned tool reached resolver"))
    with pytest.raises(ValueError, match="Pinned"):
        generation.generate()
    assert all((tmp_path / path).read_bytes() == value for path, value in original.items())
