"""Real console/auth/task/supervisor paths; local ledger effects are SIMULATED."""
from contextlib import closing
import asyncio
import json
from pathlib import Path
import sqlite3
import socket
import subprocess
import sys
import time
import uuid
from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient

from acceptance import local
from acceptance.local import WORKER, LocalTasks, create_app, prepare
from runtime.credential_input import read_credential_file
from runtime.action_journal import SQLiteActionJournal

ORIGIN = "http://127.0.0.1:8877"


def wait_for(check):
    # Watchdog only: ordering comes from durable approval and action evidence.
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(.01)
    raise AssertionError("Acceptance did not reach its expected durable state")


def effects(app):
    with closing(sqlite3.connect(app.state.acceptance_files.ledger)) as db:
        return db.execute("SELECT step FROM effects ORDER BY step").fetchall()


def connect(app, client):
    token = read_credential_file(app.state.acceptance_files.root / "operator.secret")
    response = client.post("/api/login", json={"credential": token}, headers={"Origin": ORIGIN})
    assert response.status_code == 200
    return response.json()["csrf_token"]


def pending(app):
    journal = SQLiteActionJournal(app.state.acceptance_files.journal)
    found = [r for _, r in journal.records() if r["state"] == "PENDING"]
    return found[-1]["action_id"] if found else None


def command(client, csrf, kind, target, **extra):
    record_kind = "tasks" if kind == "cancel" else "actions"
    detail = client.get(f"/api/records/{record_kind}/{target}")
    assert detail.status_code == 200
    result = client.post(f"/api/commands/{kind}/{target}",
                        json={"operation_id": str(uuid.uuid4()), "review_digest": detail.json()["review_digest"], **extra},
                        headers={"Origin": ORIGIN, "X-CCS-CSRF": csrf})
    assert result.status_code == 200, result.text


def approve(client, csrf, target):
    command(client, csrf, "approve", target, direction="FORWARD", executor=WORKER, ttl_ms=60000)


def test_two_console_approvals_complete_one_durable_task_even_after_disconnect(tmp_path, capsys):
    app = create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN) as client:
        page = client.get("/")
        assert "SIMULATED LOCAL ACCEPTANCE" in page.text and "Simulation mode" in page.text
        assert "script-src 'self'" in page.headers["content-security-policy"]
        assert client.get("/api/snapshot").status_code == 401
        first = wait_for(lambda: pending(app))
        assert effects(app) == []
        csrf = connect(app, client)
        approve(client, csrf, first)
        client.post("/api/logout", json={}, headers={"Origin": ORIGIN, "X-CCS-CSRF": csrf})
        second = wait_for(lambda: (p if (p := pending(app)) and p != first else None))
        assert effects(app) == [(1,)]
        csrf = connect(app, client)
        approve(client, csrf, second)
        wait_for(lambda: effects(app) == [(1,), (2,)])
        wait_for(lambda: client.get(f"/api/records/tasks/{app.state.acceptance_task_id}").json().get("state") == "COMPLETED")
        assert first != second
        token = read_credential_file(tmp_path / "operator.secret")
    assert app.state.acceptance_host.report.drained and app.state.acceptance_host.report.lease_released
    assert token not in capsys.readouterr().out


@pytest.mark.parametrize("after_first", [False, True])
def test_cancellation_keeps_existing_effects_and_never_runs_the_next_step(tmp_path, after_first):
    app = create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN) as client:
        first = wait_for(lambda: pending(app))
        csrf = connect(app, client)
        if after_first:
            approve(client, csrf, first)
            wait_for(lambda: (p := pending(app)) and p != first)
        command(client, csrf, "cancel", app.state.acceptance_task_id)
        wait_for(lambda: app.state.acceptance_operation.done())
        assert effects(app) == ([(1,)] if after_first else [])
        assert client.get(f"/api/records/tasks/{app.state.acceptance_task_id}").json()["state"] == "CANCELLED"
    assert effects(app) == ([(1,)] if after_first else [])


def test_stop_does_not_dispatch_or_erase_waiting_action_and_directory_cannot_resume(tmp_path):
    app = create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN):
        action_id = wait_for(lambda: pending(app))
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    assert effects(app) == []
    assert any(r["action_id"] == action_id and r["state"] == "PENDING"
               for _, r in SQLiteActionJournal(app.state.acceptance_files.journal).records())
    with pytest.raises(ValueError, match="new empty"):
        prepare(tmp_path)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}


def test_current_worker_revocation_stops_the_driver_without_execution(tmp_path):
    app = create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN):
        wait_for(lambda: pending(app))
        journal = SQLiteActionJournal(app.state.acceptance_files.journal)
        journal.security.update_principal(WORKER, active=False)
        wait_for(lambda: app.state.acceptance_operation.done())
        assert effects(app) == []


def test_applied_but_lost_response_parks_task_and_never_retries_or_runs_descendant(tmp_path, monkeypatch):
    original = LocalTasks._get_executor
    def uncertain(self, step):
        execute = original(self, step)
        async def lost(args):
            await execute(args)
            raise TimeoutError("SIMULATED response lost after durable fake effect")
        return lost
    monkeypatch.setattr(LocalTasks, "_get_executor", uncertain)
    app = create_app(tmp_path)
    with TestClient(app, base_url=ORIGIN) as client:
        first = wait_for(lambda: pending(app))
        csrf = connect(app, client)
        approve(client, csrf, first)
        wait_for(lambda: app.state.acceptance_operation.done())
        assert effects(app) == [(1,)]
        journal = SQLiteActionJournal(app.state.acceptance_files.journal)
        assert journal.load_task(app.state.acceptance_task_id)[1]["state"] == "NEEDS_RECONCILIATION"
        records = journal.records()
        assert len(records) == 1 and records[0][1]["action_id"] == first
        assert records[0][1]["state"] == "IN_FLIGHT_UNKNOWN"
    assert effects(app) == [(1,)]


def test_run_directory_under_git_or_nonprivate_posix_storage_is_refused(tmp_path):
    (tmp_path / ".git").mkdir()
    child = tmp_path / "child"
    child.mkdir(mode=0o700)
    with pytest.raises(ValueError):
        prepare(child)
    assert not list(child.iterdir())


@pytest.mark.parametrize("clean_drain", [True, False])
def test_cli_distinguishes_uvicorn_sigint_after_real_drain_from_interrupted_startup(tmp_path, monkeypatch, clean_drain):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    monkeypatch.setattr(local.SafeParser, "parse_args", lambda _: SimpleNamespace(directory=tmp_path, port=port))
    class SignalServer:
        started = clean_drain
        def __init__(self, config):
            self.app = config.app
        def run(self, **_):
            if clean_drain:
                async def serve_then_drain():
                    async with self.app.router.lifespan_context(self.app):
                        async def ready():
                            while not pending(self.app):
                                await asyncio.sleep(.01)
                        await asyncio.wait_for(ready(), timeout=3)
                asyncio.run(serve_then_drain())
            # Reproduce Uvicorn's documented-in-code signal re-raise boundary;
            # actual task startup and drain above are not mocked.
            raise KeyboardInterrupt()
    monkeypatch.setattr(local.uvicorn, "Server", SignalServer)
    if clean_drain:
        local.main()
    else:
        with pytest.raises(SystemExit) as caught:
            local.main()
        assert caught.value.code == 1
    assert (tmp_path / "acceptance.db").is_file()


@pytest.mark.parametrize("arguments", [["--host", "0.0.0.0"], ["--provider-key", "PRIVATE"], ["--port", "80"]])
def test_cli_cannot_enable_external_binding_or_provider_credentials(arguments, tmp_path):
    result = subprocess.run([sys.executable, "-m", "acceptance.local", "--directory", str(tmp_path), *arguments],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 2 and "PRIVATE" not in result.stdout + result.stderr
    assert not list(tmp_path.iterdir())
