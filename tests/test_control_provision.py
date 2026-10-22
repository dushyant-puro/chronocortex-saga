from contextlib import closing
import os
import sqlite3
import subprocess
import sys

import pytest

from control.provision import provision
from runtime.action_journal import SQLiteActionJournal
from runtime.policy_engine import PermissionDeniedError


def test_offline_provisioning_delivers_exclusive_secret_and_minimum_observation_scope(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    path = tmp_path / "operator.secret"
    credential_id = provision(journal, "operator", {"fleet:write", "ccs:approve"}, path)
    token = path.read_text()
    actor = journal.security.authenticate(token)
    assert actor.credential_id == credential_id
    assert actor.permissions == {"fleet:write", "ccs:approve", "ccs:observe"}
    with closing(sqlite3.connect(journal.path)) as db:
        assert token not in "\n".join(db.iterdump())
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        provision(journal, "another", set(), path)
    assert path.read_text() == token
    with pytest.raises(PermissionDeniedError):
        journal.security.issue_credential("another")


def test_delivery_failure_revokes_the_issued_secret_and_removes_partial_file(tmp_path, monkeypatch):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    path = tmp_path / "operator.secret"
    captured = []
    original = journal.security.issue_credential
    def issue(principal):
        credential = original(principal)
        captured.append(credential)
        return credential
    def fail(_):
        raise OSError("SIMULATED durable secret delivery failure")
    monkeypatch.setattr(journal.security, "issue_credential", issue)
    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError):
        provision(journal, "operator", set(), path)
    assert not path.exists()
    with pytest.raises(PermissionDeniedError):
        journal.security.authenticate(captured[0].token)


def test_cli_emits_only_public_metadata_and_refuses_overwrite(tmp_path):
    journal = SQLiteActionJournal(tmp_path / "journal.db")
    path = tmp_path / "operator.secret"
    args = [sys.executable, "-m", "control.provision", "--journal", str(journal.path),
            "--principal", "operator", "--credential-file", str(path)]
    result = subprocess.run(args, text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr
    token = path.read_text()
    assert token not in result.stdout + result.stderr
    assert token.split(".")[1] not in result.stdout + result.stderr
    result = subprocess.run(args, text=True, capture_output=True, timeout=10)
    assert result.returncode == 1
    assert path.read_text() == token
