"""Operational ownership/readiness does not activate or replay durable effects."""
import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time

import pytest
import httpx2 as httpx
from starlette.testclient import TestClient

from control.app import create_app as console_app
from control.host_status import HostStatusController
from control.provision import provision
from operations import recovery
from operations.config import ConfigurationError, RecoveryConfig, load
from runtime.action_journal import JournalLeaseError, SQLiteActionJournal
from runtime.credential_input import read_credential_file
from runtime.tool_contract import ToolManifest
from runtime.worker_supervisor import WorkerUnavailable
from tests.control_test_support import seed_control

ORIGIN = 'http://127.0.0.1:8877'


def setup(tmp_path, seeded=False):
    journal_path = tmp_path / 'journal.db'
    if seeded:
        env = seed_control(journal_path)
        env.journal.release_lease()
    journal = SQLiteActionJournal(journal_path)
    provision(journal, 'recovery-worker', {'ccs:work'}, tmp_path / 'worker.secret', console_access=False)
    provision(journal, 'recovery-viewer', set(), tmp_path / 'viewer.secret')
    return RecoveryConfig(journal_path, tmp_path / 'worker.secret')


def login(client, directory):
    response = client.post('/api/login', json={'credential': read_credential_file(directory / 'viewer.secret')},
                           headers={'Origin': ORIGIN})
    assert response.status_code == 200, response.text


def payload(config):
    return {'schema': 1, 'mode': 'RECOVERY_ONLY', 'journal': str(config.journal),
            'worker_credential_file': str(config.worker_credential_file), 'port': config.port}


def write_config(tmp_path, data):
    path = tmp_path / 'host.json'
    path.write_text(json.dumps(data), encoding='utf-8')
    path.chmod(0o600)
    return path


def records(path):
    with closing(sqlite3.connect(path)) as db:
        return {table: db.execute('SELECT * FROM ' + table).fetchall()
                for table in ('actions', 'events', 'task_checkpoints', 'task_events', 'approvals', 'key_claims')}


def test_existing_evidence_is_not_replayed_and_observation_is_authenticated(tmp_path, capsys):
    config = setup(tmp_path, seeded=True)
    before = records(config.journal)
    app = recovery.create_app(config)
    with TestClient(app, base_url=ORIGIN) as client:
        assert 'RECOVERY ONLY' in client.get('/').text
        assert client.get('/api/host-status').status_code == 401
        login(client, tmp_path)
        assert client.get('/api/snapshot').json()['capabilities'] == {'proposal_review': False, 'host_status': True}
        response = client.get('/api/host-status')
        assert response.status_code == 200 and response.json()['ready'] is True
        for key in ('dispatch_authorized', 'restore_authorized', 'freshness_verified', 'provider_reconciled'):
            assert response.json()[key] is False
        assert len(app.state.recovery_host.runtime.registry) == 0
        assert records(config.journal) == before
    assert recovery.stopped_cleanly(app) and records(config.journal) == before
    output = capsys.readouterr().out
    assert 'RECOVERY_HOST_READY' in output and 'RECOVERY_HOST_STOPPED' in output
    assert str(tmp_path) not in output and read_credential_file(config.worker_credential_file) not in output


@pytest.mark.parametrize('change', ['worker_revoked', 'work_removed', 'credential_revoked', 'lease_replaced', 'clock_backward', 'local_deadline'])
def test_failed_readiness_closes_host_once_without_reacquisition(tmp_path, change):
    config = setup(tmp_path)
    app = recovery.create_app(config)
    exits = []
    app.state.request_exit = lambda: exits.append(True)
    with TestClient(app, base_url=ORIGIN) as client:
        login(client, tmp_path)
        journal = app.state.recovery_host.journal
        if change == 'worker_revoked':
            journal.security.update_principal('recovery-worker', active=False)
        elif change == 'work_removed':
            journal.security.update_principal('recovery-worker', permissions=set())
        elif change == 'credential_revoked':
            journal.security.revoke_credential(read_credential_file(config.worker_credential_file).split('.')[0])
        elif change == 'lease_replaced':
            # Deterministic hostile/stale-owner evidence, not a supported takeover.
            with journal._transaction() as db:
                db.execute('UPDATE worker_lease SET generation=generation+1')
        elif change == 'clock_backward':
            with journal._transaction() as db:
                db.execute('UPDATE security_clock SET last_seen_ms=last_seen_ms+60000')
            # Operator authentication fails before a privileged HTTP probe.
            assert client.get('/api/host-status').status_code in (401, 403)
            observed = client.portal.call(app.state.host_status.inspect)
            assert observed['ready'] is False and app.state.host_status.failed
        else:
            app.state.recovery_host._monotonic = lambda: 10**20
        if change != 'clock_backward':
            response = client.get('/api/host-status')
            assert response.status_code == 503
            assert app.state.host_status.failed
            assert response.json()['ready'] is False
            assert response.json()['dispatch_authorized'] is False
    assert exits == [True] and not recovery.stopped_cleanly(app)
    assert records(config.journal)['actions'] == []


def test_operator_revocation_and_cross_origin_do_not_trigger_health_or_stop_host(tmp_path):
    config = setup(tmp_path)
    app = recovery.create_app(config)
    with TestClient(app, base_url=ORIGIN) as client:
        login(client, tmp_path)
        assert client.get('/api/host-status', headers={'Origin': 'http://evil.test'}).status_code == 403
        assert client.post('/api/host-status', json={}, headers={'Origin': ORIGIN}).status_code == 405
        assert not app.state.host_status.failed
        SQLiteActionJournal(config.journal).security.update_principal('recovery-viewer', active=False)
        assert client.get('/api/host-status').status_code == 401
        assert not app.state.host_status.failed and app.state.recovery_host.state == 'READY'
    assert recovery.stopped_cleanly(app)


def test_second_host_cannot_acquire_or_report_ready(tmp_path):
    config = setup(tmp_path)
    first, second = recovery.create_app(config), recovery.create_app(config)
    with TestClient(first, base_url=ORIGIN):
        with pytest.raises(JournalLeaseError):
            with TestClient(second, base_url=ORIGIN):
                pytest.fail('Competing host started')
        assert first.state.recovery_host.state == 'READY'
    assert recovery.stopped_cleanly(first)


def test_stop_wins_during_readiness_probe_and_cannot_report_ready(tmp_path, monkeypatch):
    config = setup(tmp_path)
    app = recovery.create_app(config)
    with TestClient(app, base_url=ORIGIN) as client:
        login(client, tmp_path)
        original = app.state.host_status._probe
        def stop_before_response():
            original()
            asyncio.run_coroutine_threadsafe(stop(), app.state.recovery_host._loop).result(timeout=3)
        async def stop():
            app.state.recovery_host.request_stop()
        monkeypatch.setattr(app.state.host_status, '_probe', stop_before_response)
        response = client.get('/api/host-status')
        assert response.status_code == 503 and not response.json()['ready']
    assert not recovery.stopped_cleanly(app)


def test_dispatch_capability_cannot_be_labelled_as_a_recovery_ready_host(tmp_path):
    config = setup(tmp_path)
    app = recovery.create_app(config)
    with TestClient(app, base_url=ORIGIN) as client:
        login(client, tmp_path)
        app.state.recovery_host.runtime.registry.register(ToolManifest('SIMULATED.unexpected', 'write'))
        response = client.get('/api/host-status')
        assert response.status_code == 503 and not response.json()['ready']
    assert not recovery.stopped_cleanly(app)


def test_independent_worker_failure_requests_server_exit_without_health_request(tmp_path):
    config = setup(tmp_path)
    app = recovery.create_app(config)
    exits = []
    app.state.request_exit = lambda: exits.append(True)
    with TestClient(app, base_url=ORIGIN) as client:
        app.state.recovery_host._monotonic = lambda: 10**20
        with pytest.raises(WorkerUnavailable):
            client.portal.call(app.state.recovery_host.pulse)
        client.portal.call(lambda: asyncio.wait_for(app.state.recovery_host.stopped.wait(), timeout=3))
    assert exits == [True] and not recovery.stopped_cleanly(app)
    assert not app.state.host_status.failed  # Failure came from renewal, not a probe.


@pytest.mark.parametrize('case', ['extra', 'provider', 'mode', 'schema_bool', 'port_bool', 'low_port',
                                'relative', 'missing', 'settings_extra', 'settings_bool', 'settings_ratio',
                                'null', 'list', 'duplicate', 'nan', 'oversize', 'malformed', 'secret'])
def test_configuration_rejects_ambiguous_or_unsafe_values_without_mutation(tmp_path, case):
    config = setup(tmp_path)
    before = records(config.journal)
    data = payload(config)
    if case in ('extra', 'provider', 'secret'):
        data[{'extra': 'host', 'provider': 'provider', 'secret': 'credential'}[case]] = 'PRIVATE'
    elif case == 'mode': data['mode'] = 'EXECUTE'
    elif case == 'schema_bool': data['schema'] = True
    elif case == 'port_bool': data['port'] = True
    elif case == 'low_port': data['port'] = 80
    elif case == 'relative': data['journal'] = 'journal.db'
    elif case == 'missing': data['journal'] = str(tmp_path / 'absent.db')
    elif case == 'settings_extra': data['worker_settings'] = {'restart': True}
    elif case == 'settings_bool': data['worker_settings'] = {'max_operations': True}
    elif case == 'settings_ratio': data['worker_settings'] = {'lease_ttl_ms': 1000}
    elif case == 'null': data = None
    elif case == 'list': data = []
    path = write_config(tmp_path, data)
    if case == 'duplicate': path.write_text('{"schema":1,"schema":1}', encoding='utf-8')
    elif case == 'nan': path.write_text('{"port":NaN}', encoding='utf-8')
    elif case == 'oversize': path.write_text(' ' * 16385, encoding='utf-8')
    elif case == 'malformed': path.write_bytes(b'\xffPRIVATE')
    with pytest.raises(ConfigurationError) as caught:
        load(path)
    assert 'PRIVATE' not in str(caught.value) and str(tmp_path) not in str(caught.value)
    assert records(config.journal) == before and not (tmp_path / 'absent.db').exists()


def test_configuration_roundtrip_and_explicit_timing_settings(tmp_path):
    config = setup(tmp_path)
    data = payload(config)
    data['worker_settings'] = {'max_operations': 4}
    loaded = load(write_config(tmp_path, data))
    assert loaded.settings.max_operations == 4 and loaded.settings.lease_ttl_ms == 30000
    assert loaded.journal == config.journal


def test_windows_utf8_bom_configuration_is_supported(tmp_path):
    config = setup(tmp_path)
    path = write_config(tmp_path, payload(config))
    path.write_text(json.dumps(payload(config)), encoding='utf-8-sig')
    assert load(path) == config


def test_config_hardlink_and_directory_are_refused(tmp_path):
    path = write_config(tmp_path, {})
    os.link(path, tmp_path / 'linked.json')
    for target in (path, tmp_path):
        with pytest.raises(ConfigurationError): load(target)


@pytest.mark.parametrize('path', ['relative.db', '\\\\unreachable.invalid\\share\\journal.db'])
def test_unsupported_storage_paths_are_rejected_before_any_file_probe(tmp_path, monkeypatch, path):
    def probe(_):
        pytest.fail('Unsupported storage must be rejected before filesystem/network probing')
    monkeypatch.setattr(Path, 'is_file', probe)
    with pytest.raises(ConfigurationError):
        RecoveryConfig(Path(path), tmp_path / 'worker.secret')


def test_status_controller_must_match_the_actual_supervised_journal(tmp_path):
    config = setup(tmp_path)
    app = recovery.create_app(config)
    with pytest.raises(ValueError):
        console_app(SQLiteActionJournal(config.journal), host_status=app.state.host_status)
    with pytest.raises(TypeError): HostStatusController(object(), 'PRIVATE')


def test_busy_port_is_detected_before_journal_or_lease_changes(tmp_path):
    config = setup(tmp_path)
    before = config.journal.read_bytes()
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        data = payload(config)
        data['port'] = listener.getsockname()[1]
        path = write_config(tmp_path, data)
        result = subprocess.run([sys.executable, '-m', 'operations.recovery', '--config', str(path), '--check'],
                                capture_output=True, text=True, timeout=15)
    assert result.returncode == 1 and str(tmp_path) not in result.stderr
    assert config.journal.read_bytes() == before


def test_cli_check_acquires_and_releases_without_replay(tmp_path):
    config = setup(tmp_path, seeded=True)
    before = records(config.journal)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        data = payload(config)
        data['port'] = listener.getsockname()[1]
    result = subprocess.run([sys.executable, '-m', 'operations.recovery', '--config', str(write_config(tmp_path, data)), '--check'],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert events[-1]['lease_released'] and events[-1]['drained']
    assert records(config.journal) == before


def test_serving_process_exits_unsuccessfully_after_current_worker_revocation(tmp_path):
    config = setup(tmp_path, seeded=True)
    before = records(config.journal)
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    data = payload(config)
    data['port'] = port
    origin = f'http://127.0.0.1:{port}'
    with (tmp_path / 'process.log').open('w+', encoding='utf-8') as log:
        process = subprocess.Popen([sys.executable, '-m', 'operations.recovery', '--config',
                                   str(write_config(tmp_path, data))], stdout=log, stderr=log)
        try:
            with httpx.Client(base_url=origin, timeout=1, trust_env=False) as client:
                deadline = time.monotonic() + 10
                while True:
                    assert process.poll() is None and time.monotonic() < deadline
                    try:
                        if client.get('/').status_code == 200: break
                    except httpx.TransportError:
                        pass
                    time.sleep(.02)
                login_response = client.post('/api/login', json={
                    'credential': read_credential_file(tmp_path / 'viewer.secret')}, headers={'Origin': origin})
                assert login_response.status_code == 200
                SQLiteActionJournal(config.journal).security.update_principal('recovery-worker', active=False)
                response = client.get('/api/host-status')
                assert response.status_code == 503 and response.json()['ready'] is False
            assert process.wait(timeout=10) == 1
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
        log.seek(0)
        output = log.read()
    assert '"healthcheck_failed": true' in output
    assert read_credential_file(config.worker_credential_file) not in output
    assert str(tmp_path) not in output and records(config.journal) == before


@pytest.mark.parametrize('args', [['--host', '0.0.0.0'], ['--provider-key', 'PRIVATE'], ['--config', 'PRIVATE', '--configx', 'bad']])
def test_cli_rejects_external_or_provider_overrides_without_echoing_arguments(args):
    result = subprocess.run([sys.executable, '-m', 'operations.recovery', *args], capture_output=True, text=True, timeout=15)
    assert result.returncode == 2 and 'PRIVATE' not in result.stderr
