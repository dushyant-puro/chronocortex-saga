"""Private SIMULATED subprocess peers for eval.provider_faults.

Only loopback sockets and built-in fake identities are accepted. The production
adapter keeps its fixed HTTPS origin; this module supplies its trusted test seam.
Neither this service nor its proxy models Stripe's idempotency implementation.
"""
import argparse
import asyncio
from contextlib import closing
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import sqlite3
import sys
import threading
from types import SimpleNamespace

import httpx2 as httpx

from providers import reconcile_stripe, sandbox_drill
from providers.stripe_sandbox import API_VERSION, ORIGIN, StripeSandboxCancellation
from runtime.action_journal import SQLiteActionJournal


ACCOUNT = "acct_Simulated12345"
INTENT = "pi_Simulated123456789"
KEY = "rk_test_" + "SIMULATED" * 4
PRIVATE = "SIMULATED_PRIVATE_RESPONSE_FIELD"
RESOURCE = "/v1/payment_intents/" + INTENT
ROUTES = {("GET", "/v1/account"), ("GET", RESOURCE), ("POST", RESOURCE + "/cancel")}


def emit(event, **fields):
    print(json.dumps({"event": event, "source": "SIMULATED", **fields}), flush=True)


def port(value):
    if type(value) is not int or not 1 <= value <= 65535:
        raise ValueError("Explicit loopback port required")
    return value


class LoopbackTransport(httpx.AsyncBaseTransport):
    """Actual TCP, zero retries, no DNS, environment proxies, TLS or redirects."""
    def __init__(self, target_port):
        self.target_port = port(target_port)
        self.inner = httpx.AsyncHTTPTransport(retries=0, trust_env=False)

    async def handle_async_request(self, request):
        if (str(request.url).split("/v1/", 1)[0] != ORIGIN or request.url.query or
                (request.method, request.url.path) not in ROUTES or
                request.headers.get("authorization") != "Bearer " + KEY):
            raise ValueError("SIMULATED scope required")
        local = httpx.Request(request.method, f"http://127.0.0.1:{self.target_port}" + request.url.path,
                              headers=request.headers, content=await request.aread(), extensions=request.extensions)
        local.headers["host"] = f"127.0.0.1:{self.target_port}"
        return await self.inner.handle_async_request(local)

    async def aclose(self):
        await self.inner.aclose()


def factory(target_port):
    port(target_port)
    def make(config):
        if config.account_id != ACCOUNT or config.key != KEY:
            raise ValueError("Built-in fake identities required")
        return StripeSandboxCancellation(config, transport=LoopbackTransport(target_port))
    return make


def ledger(path):
    # Separate process/file from the CCS journal; commit before sending a reply.
    db = sqlite3.connect(path, timeout=5)
    db.execute("PRAGMA synchronous=EXTRA")
    return db


def initialize(path):
    with closing(ledger(path)) as db, db:
        db.execute("CREATE TABLE requests (seq INTEGER PRIMARY KEY, method TEXT, path TEXT, operation_key TEXT)")
        db.execute("CREATE TABLE effects (seq INTEGER PRIMARY KEY, operation_key TEXT)")


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, *_):
        emit("HTTP_HANDLER_FAILED")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass  # Never serialize incoming headers or untrusted exception bodies.

    def reply(self, status, data, reference="req_Simulated00000000"):
        raw = json.dumps(data).encode()
        self.send_response(status)
        for key, value in {"Content-Type": "application/json", "Content-Length": str(len(raw)),
                           "Connection": "close", "Request-Id": reference, "Stripe-Version": API_VERSION}.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(raw)
        self.close_connection = True

    def drop(self):
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.connection.close()

    def do_GET(self):
        self.request_received()

    def do_POST(self):
        self.request_received()

    def request_received(self):
        if ((self.command, self.path) not in ROUTES or
                self.headers.get("Authorization") != "Bearer " + KEY or
                self.headers.get("Stripe-Version") != API_VERSION or
                self.headers.get("Transfer-Encoding") is not None or
                self.headers.get("Content-Length", "0") != "0" or
                (self.command == "POST" and not self.headers.get("Idempotency-Key"))):
            self.reply(400, {"error": "SIMULATED_SCOPE_REJECTED"})
            return
        with closing(ledger(self.server.ledger_path)) as db, db:
            seq = db.execute("INSERT INTO requests(method,path,operation_key) VALUES (?,?,?)",
                             (self.command, self.path, self.headers.get("Idempotency-Key"))).lastrowid
            if self.server.role == "service" and self.command == "POST":
                if db.execute("SELECT count(*) FROM effects").fetchone()[0]:
                    duplicate = True
                else:
                    duplicate = False
                    db.execute("INSERT INTO effects(operation_key) VALUES (?)", (self.headers["Idempotency-Key"],))
            applied = db.execute("SELECT count(*) FROM effects").fetchone()[0] > 0
        reference = f"req_Simulated{seq:08d}"
        if self.server.role == "service":
            if self.command == "POST" and duplicate:
                self.reply(409, {"error": "SIMULATED_DUPLICATE"}, reference)
            elif self.path == "/v1/account":
                self.reply(200, {"object": "account", "id": ACCOUNT, "email": PRIVATE}, reference)
            else:
                visible = self.command == "POST" or not self.server.hidden.is_set()
                canceled = applied and visible
                self.reply(200, {"object": "payment_intent", "id": INTENT, "livemode": False,
                                 "status": "canceled" if canceled else "requires_payment_method",
                                 "canceled_at": 1700000000 if canceled else None,
                                 "amount_capturable": 0, "client_secret": PRIVATE}, reference)
            return
        if self.command == "POST" and self.server.fault == "before_forward":
            emit("REQUEST_DISCARDED_BEFORE_FORWARD")
            self.drop()
            return
        try:
            with closing(http.client.HTTPConnection("127.0.0.1", self.server.upstream, timeout=5)) as upstream:
                headers = {key: self.headers[key] for key in
                           ("Authorization", "Stripe-Version", "Idempotency-Key") if key in self.headers}
                upstream.request(self.command, self.path, headers=headers)
                response = upstream.getresponse()
                raw = response.read(65_537)
                if response.status != 200 or len(raw) > 65_536:
                    self.reply(502, {"error": "SIMULATED_UPSTREAM_INVALID"})
                    return
                data = json.loads(raw)
                reference = response.getheader("Request-Id")
            if self.command == "POST":
                # Upstream replied only after committing its independent ledger.
                # No response bytes have been sent to the worker at this barrier.
                emit("RESPONSE_WITHHELD_AFTER_APPLY")
                if self.server.fault == "hold_after_apply":
                    if not self.server.release.wait(timeout=30):
                        emit("BARRIER_EXPIRED")
                self.drop()
                return
            self.reply(200, data, reference)
        except (OSError, http.client.HTTPException, ValueError):
            self.reply(502, {"error": "SIMULATED_UPSTREAM_UNAVAILABLE"})


def serve(args):
    initialize(args.ledger)
    server = Server(("127.0.0.1", 0), Handler)
    server.role, server.ledger_path, server.fault = args.role, args.ledger, args.fault
    server.upstream = port(args.upstream) if args.role == "proxy" else None
    server.hidden, server.release = threading.Event(), threading.Event()
    if args.hidden:
        server.hidden.set()
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .05}, daemon=True)
    thread.start()
    emit("READY", port=server.server_port)
    try:
        for command in sys.stdin:
            if command == "stop\n":
                break
            if command == "drop\n":
                server.release.set()
                emit("DROP_RELEASED")
            elif command == "reveal\n" and args.role == "service":
                server.hidden.clear()
                emit("VISIBILITY_RELEASED")
            else:
                raise ValueError("Unknown simulated peer command")
    finally:
        server.release.set()
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


async def worker(args):
    root = Path(args.root)
    def journal(path):
        return SQLiteActionJournal(path, clock_ms=lambda: int((root / "clock").read_text()))
    # Isolated child-only clock seam. Production CLI has no clock/URL override.
    sandbox_drill.SQLiteActionJournal = reconcile_stripe.SQLiteActionJournal = journal
    options = SimpleNamespace(journal=str(root / "ccs.db"), credential_file=str(root / "worker.key"),
                              stripe_key_file=str(root / "stripe.key"), account_id=ACCOUNT,
                              payment_intent_id=INTENT, allow_sandbox_cancellation=True,
                              approval_wait_seconds=120, action_id=args.action)
    make = factory(args.upstream)
    if args.role == "worker":
        return await sandbox_drill.run(options, provider_factory=make)
    return await reconcile_stripe.run(options, provider_factory=make)


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("role", choices=("service", "proxy", "worker", "recover"))
    parser.add_argument("--ledger")
    parser.add_argument("--root")
    parser.add_argument("--upstream", type=int)
    parser.add_argument("--action")
    parser.add_argument("--fault", choices=("drop_after_apply", "hold_after_apply", "before_forward"), default="drop_after_apply")
    parser.add_argument("--hidden", action="store_true")
    args = parser.parse_args()
    try:
        if args.role in {"service", "proxy"}:
            serve(args)
            code = 0
        else:
            code = asyncio.run(worker(args))
    except Exception:
        emit("PEER_FAILED")
        code = 1
    raise SystemExit(code)


if __name__ == "__main__":
    main()
