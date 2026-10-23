import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from control.provision import provision
from runtime.action_journal import SQLiteActionJournal
from runtime.credential_input import CredentialInputError, read_credential_file
from tests.control_test_support import seed_control
from worker.__main__ import shutdown_signals


def secret(path, raw):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)


def invoke(path, credential, *extra):
    return subprocess.run([sys.executable, "-m", "worker", "--journal", str(path), "--credential-file", str(credential), *extra],
                          capture_output=True, text=True, timeout=20)


@pytest.mark.parametrize("raw", [b"", b"x" * 200, b"a" * 32 + b"." + b"x" * 42, b"\xff" * 76, b"a" * 32 + b"." + b"x" * 43 + b"\n"])
def test_malformed_secret_file_is_bounded_and_never_echoed(tmp_path, raw):
    path = tmp_path / "credential.secret"
    secret(path, raw)
    with pytest.raises(CredentialInputError) as exc:
        read_credential_file(path)
    assert repr(raw) not in str(exc.value)


def test_secret_file_requires_regular_single_link_and_posix_private_permissions(tmp_path):
    path = tmp_path / "credential.secret"
    raw = b"a" * 32 + b"." + b"x" * 43
    secret(path, raw)
    assert read_credential_file(path) == raw.decode()
    with pytest.raises(CredentialInputError):
        read_credential_file(tmp_path)
    if os.name != "nt":
        path.chmod(0o644)
        with pytest.raises(CredentialInputError):
            read_credential_file(path)
        path.chmod(0o600)
    link = tmp_path / "hardlink.secret"
    os.link(path, link)
    with pytest.raises(CredentialInputError):
        read_credential_file(path)


def test_worker_provisioning_does_not_silently_add_console_authority(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    path = tmp_path / "worker.secret"
    provision(journal, "worker", {"ccs:work"}, path, console_access=False)
    assert journal.security.authenticate(read_credential_file(path)).permissions == {"ccs:work"}


def test_cli_check_hydrates_without_replay_and_releases_lease(tmp_path):
    env = seed_control(tmp_path / "journal.db")
    before = env.journal.records()
    env.journal.release_lease()
    path = tmp_path / "worker.secret"
    provision(env.journal, "recovery-worker", {"ccs:work"}, path, console_access=False)
    token = read_credential_file(path)
    result = invoke(env.journal.path, path, "--check")
    assert result.returncode == 0, result.stderr
    assert token not in result.stdout + result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert lines[0]["mode"] == "RECOVERY_ONLY" and lines[0]["provider"] == "NOT_CONFIGURED"
    assert lines[0]["recovered_action_records"] == 4
    assert lines[-1]["drained"] and lines[-1]["lease_released"]
    assert env.journal.records() == before
    assert not any(event["kind"] in {"DISPATCH_INTENT", "COMPENSATION_INTENT"} and event["generation"] == 2 for event in env.journal.worker_events())


def test_invalid_or_unprivileged_worker_credential_never_acquires_a_lease(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    path = tmp_path / "viewer.secret"
    provision(journal, "viewer", {"ccs:observe"}, path)
    result = invoke(journal.path, path, "--check")
    assert result.returncode == 1 and "failed closed" in result.stderr
    assert read_credential_file(path) not in result.stdout + result.stderr
    assert journal.worker_events() == []


def test_cli_never_initializes_or_migrates_implicitly(tmp_path):
    missing = tmp_path / "missing.db"
    result = invoke(missing, tmp_path / "missing.secret", "--check")
    assert result.returncode == 2 and not missing.exists()
    journal = SQLiteActionJournal(tmp_path / "old.db")
    with closing(sqlite3.connect(journal.path)) as db:
        db.execute("PRAGMA user_version=5")
        db.commit()
    path = tmp_path / "worker.secret"
    secret(path, b"a" * 32 + b"." + b"x" * 43)
    result = invoke(journal.path, path, "--check")
    assert result.returncode == 1
    with closing(sqlite3.connect(journal.path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 5


@pytest.mark.asyncio
async def test_signal_handler_only_schedules_stop_and_restores_previous_handlers():
    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    previous = signal.getsignal(signal.SIGINT)
    with shutdown_signals(loop, SimpleNamespace(request_stop=stopping.set)):
        handler = signal.getsignal(signal.SIGINT)
        handler(signal.SIGINT, None)
        assert not stopping.is_set()
        await asyncio.wait_for(stopping.wait(), 1)
    assert signal.getsignal(signal.SIGINT) is previous
