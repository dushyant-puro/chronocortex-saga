"""One configured loopback recovery host and operator console. No executors."""
import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
import json
import socket

from starlette.responses import Response
from starlette.routing import Route
import uvicorn

from control.app import ControlConfig, STATIC, create_app as console_app
from control.host_status import HostStatusController
from operations.config import RecoveryConfig, load
from runtime.action_journal import SQLiteActionJournal
from runtime.credential_input import read_credential_file
from runtime.worker_supervisor import WorkerSupervisor
from worker.__main__ import recovery_runtime

BANNER = ('<aside class="notice" role="note" aria-label="Recovery mode">'
          'RECOVERY ONLY · Existing journal evidence · No executors, provider calls, '
          'live voice or automatic task replay. Readiness is not dispatch authority.</aside>')


def create_app(config):
    if type(config) is not RecoveryConfig:
        raise TypeError('A validated recovery configuration is required')
    # Validate again before construction: a missing journal must never be
    # implicitly created. Trusted local administrators protect parent paths.
    config.validate_paths()
    credential = read_credential_file(config.worker_credential_file)
    host = WorkerSupervisor(SQLiteActionJournal(config.journal), credential, settings=config.settings)
    status = HostStatusController(host, credential)
    app = console_app(host.journal, ControlConfig(config.origin), host_status=status)
    app.state.recovery_host = host
    app.state.host_status = status
    app.state.request_exit = lambda: None

    async def page(_):
        html = (STATIC / 'index.html').read_text(encoding='utf-8')
        html = html.replace('<body>', '<body>' + BANNER, 1)
        html = html.replace('LOCAL CONTROL HOST · PROVIDER EXECUTION NOT CONFIGURED', 'RECOVERY ONLY · DISPATCH NOT CONFIGURED')
        return Response(html, media_type='text/html')
    app.router.routes.insert(0, Route('/', page))

    @asynccontextmanager
    async def lifespan(_):
        watcher = None
        try:
            await host.start(recovery_runtime)
            async def watch():
                await host.stopping.wait()
                app.state.request_exit()
            watcher = asyncio.create_task(watch(), name='ccs-recovery-exit')
            print(json.dumps({'event': 'RECOVERY_HOST_READY', 'mode': 'RECOVERY_ONLY',
                              'provider': 'NOT_CONFIGURED', 'url': config.origin,
                              'dispatch_authorized': False}), flush=True)
            yield
        finally:
            report = await asyncio.shield(host.stop())
            if watcher is not None:
                await watcher
            print(json.dumps({'event': 'RECOVERY_HOST_STOPPED', 'healthcheck_failed': status.failed,
                              **asdict(report)}), flush=True)
    app.router.lifespan_context = lifespan
    return app


def stopped_cleanly(app):
    report = app.state.recovery_host.report
    return (not app.state.host_status.failed and report is not None and report.drained and report.lease_released and
            report.failure_code is None)


class SafeParser(argparse.ArgumentParser):
    def error(self, _):
        self.exit(2, 'Invalid recovery-host arguments. Use --help; configuration contains paths, never secrets.\n')


def main():
    parser = SafeParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--config', required=True)
    parser.add_argument('--check', action='store_true', help='Acquire/hydrate/drain/release once; no HTTP serving or replay')
    args = parser.parse_args()
    try:
        config = load(args.config)
        # Binding precedes journal construction/acquisition, including --check.
        # A busy port cannot start a hidden worker or touch its journal.
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', config.port))
            app = create_app(config)
            if args.check:
                async def check():
                    async with app.router.lifespan_context(app):
                        pass
                asyncio.run(check())
            else:
                server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=config.port,
                    proxy_headers=False, access_log=False, log_level='warning',
                    limit_concurrency=32, timeout_keep_alive=5))
                app.state.request_exit = lambda: setattr(server, 'should_exit', True)
                try:
                    server.run(sockets=[listener])
                except KeyboardInterrupt:
                    if not stopped_cleanly(app):
                        raise RuntimeError('Unverified shutdown') from None
                if not server.started:
                    raise RuntimeError('Server did not start')
            if not stopped_cleanly(app):
                raise RuntimeError('Recovery host requires inspection')
    except Exception:
        parser.exit(1, 'Recovery host failed closed. Preserve journal evidence; inspect configuration, authority, ownership and shutdown. No automatic restart or replay.\n')


if __name__ == '__main__':
    main()
