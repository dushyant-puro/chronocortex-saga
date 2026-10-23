"""Independent-process, real loopback TCP fault drills. All effects SIMULATED.

No provider credentials, external hosts, custom endpoints or live mode switches.
Worker, fault proxy and provider service run in separate OS processes. The
service's ledger is an independent effect oracle, never a CCS state projection.
"""
import argparse
from contextlib import closing, ExitStack
import hashlib
import json
import os
from pathlib import Path
import queue
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from control.provision import provision
from eval.provider_fault_peer import ACCOUNT, INTENT, KEY, PRIVATE
from providers.stripe_sandbox import API_VERSION, CONTRACT, PERMISSION
from runtime.action_journal import SQLiteActionJournal
from runtime.control_store import ControlStore
from runtime.credential_input import read_credential_file


CASES = ("lost_response_after_apply", "worker_killed_after_apply", "worker_killed_before_dispatch",
         "request_discarded_before_forward", "effect_hidden_from_status_reads")
ROOT = Path(__file__).resolve().parent.parent


class DrillFailure(RuntimeError):
    """Only bounded internal failure codes escape the evaluator."""


class Peer:
    def __init__(self, role, *arguments):
        env = os.environ.copy()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        self.process = subprocess.Popen([sys.executable, "-u", "-m", "eval.provider_fault_peer", role, *map(str, arguments)],
                                        cwd=ROOT, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True,
                                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        self.messages, self.events, self.errors = queue.Queue(), [], []
        self.invalid_output = False
        def output():
            try:
                for index, line in enumerate(self.process.stdout):
                    if index >= 128 or len(line) > 16_384:
                        self.invalid_output = True
                        continue
                    try:
                        record = json.loads(line)
                        if type(record) is not dict or type(record.get("event")) is not str:
                            raise ValueError()
                        self.events.append(record)
                        self.messages.put(record)
                    except ValueError:
                        self.invalid_output = True
            finally:
                self.messages.put(None)
        def errors():
            # Drain continuously to prevent pipe deadlock; retain a bounded sample.
            for line in self.process.stderr:
                if sum(map(len, self.errors)) < 16_384:
                    self.errors.append(line[:4096])
        self.threads = [threading.Thread(target=output, daemon=True), threading.Thread(target=errors, daemon=True)]
        for thread in self.threads:
            thread.start()

    def send(self, command):
        try:
            self.process.stdin.write(command + "\n")
            self.process.stdin.flush()
        except (OSError, ValueError):
            raise DrillFailure("PEER_CONTROL_UNAVAILABLE") from None

    def receive(self, event):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                record = self.messages.get(timeout=max(.001, deadline - time.monotonic()))
            except queue.Empty:
                break
            if record is None or record["event"] in {"PEER_FAILED", "BARRIER_EXPIRED", "HTTP_HANDLER_FAILED"}:
                raise DrillFailure("PEER_ENDED_BEFORE_BARRIER")
            if record["event"] == event:
                return record
        raise DrillFailure("PEER_BARRIER_TIMEOUT")

    def finish(self, expected):
        try:
            code = self.process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            raise DrillFailure("PEER_EXIT_TIMEOUT") from None
        self._join()
        if code != expected or self.invalid_output:
            raise DrillFailure("PEER_EXIT_OR_PROTOCOL_MISMATCH")

    def kill(self):
        self.process.kill()  # Fault injection/fixture teardown, never rollback.
        self.process.wait(timeout=5)
        self._join()

    def _join(self):
        for thread in self.threads:
            thread.join(timeout=2)
        if any(thread.is_alive() for thread in self.threads):
            raise DrillFailure("PEER_PIPE_DRAIN_TIMEOUT")

    def close(self):
        try:
            if self.process.poll() is None:
                self.send("stop")
                try:
                    self.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.kill()
            self._join()
        finally:
            for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                stream.close()


def private_file(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
    with os.fdopen(descriptor, "w", encoding="ascii", newline="") as stream:
        stream.write(value)


def setup(root):
    root.mkdir()
    (root / "clock").write_text("1000")
    journal = SQLiteActionJournal(root / "ccs.db", clock_ms=lambda: int((root / "clock").read_text()))
    provision(journal, "worker", {"ccs:work", "ccs:reconcile", PERMISSION}, root / "worker.key", console_access=False)
    provision(journal, "operator", {"ccs:approve", PERMISSION}, root / "operator.key")
    private_file(root / "stripe.key", KEY)
    return journal


def approve(journal, root, action):
    store = ControlStore(journal)
    token, identity = store.login(read_credential_file(root / "operator.key"))
    try:
        detail = store.detail(token, "actions", action)
        store.command(token, identity["csrf_token"], "APPROVE", action,
                      {"operation_id": str(uuid.uuid4()), "review_digest": detail["review_digest"],
                       "direction": "FORWARD", "executor": "worker", "ttl_ms": 300_000})
    finally:
        store.logout(token, identity["csrf_token"])


def rows(path, table):
    if table not in {"requests", "effects", "key_claims", "approvals", "task_checkpoints", "chains"}:
        raise ValueError("Internal oracle table required")
    with closing(sqlite3.connect(path)) as db:
        return db.execute("SELECT * FROM " + table).fetchall()


def validate_evidence(record, provider_requests, proxy_requests, effects, *, expected_state, expected_posts, expected_effects):
    """Compare independent facts; a local intent/receipt cannot replace the oracle."""
    post_keys = [row[3] for row in provider_requests if row[1] == "POST"]
    proxy_keys = [row[3] for row in proxy_requests if row[1] == "POST"]
    receipt = record["result"]
    return {
        "expected_durable_state": record["state"] == expected_state,
        "one_proxy_mutation_attempt": len(proxy_keys) == expected_posts,
        "provider_post_count": len(post_keys) == expected_effects,
        "independent_effect_count": len(effects) == expected_effects,
        "wire_operation_identity_matches_journal": all(key == record["idempotency_key"] for key in proxy_keys + post_keys),
        "effect_identity_matches_wire": all(row[1] == record["idempotency_key"] for row in effects),
        "receipt_requires_positive_terminal_evidence": (
            receipt is None if expected_state in {"PENDING", "IN_FLIGHT_UNKNOWN"} else
            type(receipt) is dict and receipt.get("observed_via") == "GET" and
            receipt.get("payment_intent_id") == INTENT and receipt.get("account_id") == ACCOUNT and
            receipt.get("status") == "canceled" and receipt.get("effect") == "terminal_cancellation" and
            receipt.get("mode") == "test" and receipt.get("provider") == "stripe" and
            receipt.get("provider_contract") == CONTRACT and receipt.get("api_version") == API_VERSION and
            type(receipt.get("canceled_at")) is int and receipt["canceled_at"] > 0 and
            receipt.get("idempotency_key") == record["idempotency_key"] and receipt.get("actor_attribution") == "not_claimed"),
        "no_inverse_or_unscoped_requests": all((row[1], row[2]) in {
            ("GET", "/v1/account"), ("GET", "/v1/payment_intents/" + INTENT),
            ("POST", "/v1/payment_intents/" + INTENT + "/cancel")} for row in proxy_requests + provider_requests),
    }


def run_case(root, name):
    if name not in CASES:
        raise ValueError("Unknown fault scenario")
    journal = setup(root)
    checks = {}
    with ExitStack() as stack:
        peers = []
        def start(role, *args):
            peer = Peer(role, *args)
            peers.append(peer)
            stack.callback(peer.close)
            return peer
        service_args = ["--ledger", root / "provider.db"]
        if name == "effect_hidden_from_status_reads":
            service_args.append("--hidden")
        service = start("service", *service_args)
        service_port = service.receive("READY")["port"]
        fault = ("hold_after_apply" if name == "worker_killed_after_apply" else
                 "before_forward" if name == "request_discarded_before_forward" else "drop_after_apply")
        proxy = start("proxy", "--ledger", root / "proxy.db", "--upstream", service_port, "--fault", fault)
        proxy_port = proxy.receive("READY")["port"]
        worker = start("worker", "--root", root, "--upstream", proxy_port)
        plan = worker.receive("STRIPE_SANDBOX_APPROVAL_REQUIRED")
        action_id = plan["action_id"]
        killed = name in {"worker_killed_after_apply", "worker_killed_before_dispatch"}
        before_dispatch = name == "worker_killed_before_dispatch"
        if not before_dispatch:
            approve(journal, root, action_id)
            proxy.receive("REQUEST_DISCARDED_BEFORE_FORWARD" if fault == "before_forward" else "RESPONSE_WITHHELD_AFTER_APPLY")
        if killed:
            record = journal.records()[0][1]
            checks["kill_barrier_precedes_local_receipt"] = record["result"] is None and record["state"] == ("PENDING" if before_dispatch else "IN_FLIGHT")
            checks["independent_effect_at_kill_barrier"] = len(rows(root / "provider.db", "effects")) == (0 if before_dispatch else 1)
            worker.kill()
            checks["killed_worker_did_not_release_lease"] = not any(e["kind"] == "RELEASED" for e in journal.worker_events())
            before = len(rows(root / "proxy.db", "requests"))
            premature = start("recover", "--root", root, "--upstream", proxy_port, "--action", action_id)
            premature.finish(1)
            checks["premature_takeover_makes_no_provider_request"] = len(rows(root / "proxy.db", "requests")) == before
            checks["premature_takeover_did_not_acquire"] = sum(e["kind"] == "ACQUIRED" for e in journal.worker_events()) == 1
            # All readers using the clock are now dead. Atomic replacement needs
            # no sleep or real-time expiry, and never changes a production clock.
            (root / "clock.next").write_text("31001")
            os.replace(root / "clock.next", root / "clock")
            if not before_dispatch:
                proxy.send("drop")
                proxy.receive("DROP_RELEASED")
        else:
            worker.finish(0 if name == "lost_response_after_apply" else 2)
            final = next(e for e in worker.events if e["event"] == "STRIPE_SANDBOX_DRILL_FINISHED")
            checks["host_reports_same_local_state"] = final["state"] == journal.records()[0][1]["state"]
            checks["unknown_has_no_false_readback"] = final["readback_verified"] is (name == "lost_response_after_apply")

        recovery_requests = len(rows(root / "proxy.db", "requests"))
        unknown = name in {"worker_killed_before_dispatch", "request_discarded_before_forward", "effect_hidden_from_status_reads"}
        recover = start("recover", "--root", root, "--upstream", proxy_port, "--action", action_id)
        recover.finish(2 if unknown else 0)
        recovery = next(e for e in recover.events if e["event"] == "STRIPE_STATUS_QUERY_COMPLETED")
        checks["recovery_query_result_explicit"] = recovery["terminal_status_observed"] is (not unknown)
        checks["recovery_reports_no_replay_or_inverse"] = recovery["automatic_replay"] is False and recovery["automatic_compensation"] is False
        record = journal.records()[0][1]
        checks["uncertain_read_retains_unknown_projection"] = recovery["state"] == ("IN_FLIGHT_UNKNOWN" if unknown else "COMMITTED_STALE")
        if name == "effect_hidden_from_status_reads":
            checks["hidden_effect_not_treated_as_absent"] = len(rows(root / "provider.db", "effects")) == 1 and record["result"] is None
            service.send("reveal")
            service.receive("VISIBILITY_RELEASED")
            visible = start("recover", "--root", root, "--upstream", proxy_port, "--action", action_id)
            visible.finish(0)
            record = journal.records()[0][1]
        proxy_rows = rows(root / "proxy.db", "requests")
        provider_rows, effect_rows = rows(root / "provider.db", "requests"), rows(root / "provider.db", "effects")
        expected_effects = 0 if name in {"worker_killed_before_dispatch", "request_discarded_before_forward"} else 1
        # A recovered PENDING record projects UNKNOWN without rewriting history
        # when a GET provides no new evidence. Keep raw and projected facts apart.
        state = ("PENDING" if before_dispatch else "IN_FLIGHT_UNKNOWN" if
                 name == "request_discarded_before_forward" else "COMMITTED_STALE")
        checks.update(validate_evidence(record, provider_rows, proxy_rows, effect_rows,
                                       expected_state=state, expected_posts=0 if before_dispatch else 1,
                                       expected_effects=expected_effects))
        checks["all_restart_requests_are_get_only"] = len(proxy_rows) > recovery_requests and all(r[1] == "GET" for r in proxy_rows[recovery_requests:])
        events = journal.events(action_id)
        checks["durable_intent_count_not_confused_with_network_count"] = sum(e["kind"] == "DISPATCH_INTENT" for e in events) == (0 if before_dispatch else 1)
        checks["operation_claim_retained"] = len(rows(journal.path, "key_claims")) == 1
        checks["no_task_completion_fabricated"] = rows(journal.path, "task_checkpoints") == []
        with closing(sqlite3.connect(journal.path)) as db:
            checks["approval_consumed_only_with_dispatch"] = db.execute("SELECT consumed FROM approvals").fetchall() == ([] if before_dispatch else [(1,)])
        checks["recovery_owner_released_cleanly"] = journal.worker_events()[-1]["kind"] == "RELEASED"
        checks["restarted_owner_uses_new_generation"] = len({e["generation"] for e in journal.worker_events() if e["kind"] == "ACQUIRED"}) == (3 if name == "effect_hidden_from_status_reads" else 2)
        serialized = (json.dumps(journal.records()) + json.dumps(events) + json.dumps(journal.worker_events()) +
                      json.dumps([p.events for p in peers]) + "".join("".join(p.errors) for p in peers))
        checks["secrets_and_private_provider_fields_redacted"] = all(secret not in serialized for secret in
            (KEY, PRIVATE, read_credential_file(root / "worker.key"), read_credential_file(root / "operator.key")))
        checks["all_observations_labeled_simulated"] = all(e.get("source", "SIMULATED") == "SIMULATED" for p in peers for e in p.events)
        checks["no_peer_protocol_or_barrier_failure"] = all(not p.invalid_output and not any(e["event"] in
            {"HTTP_HANDLER_FAILED", "BARRIER_EXPIRED"} for e in p.events) for p in peers)
        evidence = {"durable_state": record["state"], "first_recovery_projection": recovery["state"],
                    "action_id": action_id, "operation_key": record["idempotency_key"],
                    "proxy_post_attempts": sum(r[1] == "POST" for r in proxy_rows),
                    "provider_post_requests": sum(r[1] == "POST" for r in provider_rows), "provider_effects": len(effect_rows),
                    "recovery_http_methods": [r[1] for r in proxy_rows[recovery_requests:]],
                    "provider_request_ledger": provider_rows, "proxy_request_ledger": proxy_rows,
                    "provider_effect_ledger": effect_rows,
                    "action_event_kinds": [e["kind"] for e in events]}
    return {"id": name, "passed": all(checks.values()), "checks": checks, "evidence": evidence}


def score():
    cases = []
    with tempfile.TemporaryDirectory(prefix="ccs-simulated-process-") as directory:
        for name in CASES:
            try:
                cases.append(run_case(Path(directory) / name, name))
            except Exception as exc:
                cases.append({"id": name, "passed": False, "checks": {"drill_completed": False},
                              "failure": str(exc) if isinstance(exc, DrillFailure) else "INCOMPLETE_DRILL"})
    digest = hashlib.sha256()
    for path in (Path(__file__), Path(__file__).with_name("provider_fault_peer.py")):
        digest.update(path.name.encode() + b"\0" + path.read_bytes().replace(b"\r\n", b"\n"))
    return {"schema": 1, "label": "SIMULATED", "harness_sha256": digest.hexdigest(), "case_count": len(cases),
            "cases_passed": sum(c["passed"] for c in cases), "cases_failed": sum(not c["passed"] for c in cases),
            "checks_passed": sum(sum(c["checks"].values()) for c in cases),
            "checks_failed": sum(sum(not v for v in c["checks"].values()) for c in cases), "cases": cases,
            "real_provider": "NOT_RUN", "real_audio": "NOT_RUN", "performance_claim": "NONE"}


def write_report(path, report):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
            temporary = stream.name
            json.dump(report, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--output", default="results/provider-process-scorecard.json")
    args = parser.parse_args()
    try:
        report = score()
        write_report(args.output, report)
    except (OSError, ValueError):
        parser.exit(2, "SIMULATED fault evaluation could not preserve its scorecard.\n")
    print(json.dumps({k: report[k] for k in ("label", "harness_sha256", "case_count", "cases_passed", "cases_failed", "checks_passed", "checks_failed")}))
    raise SystemExit(0 if report["cases_failed"] == 0 else 1)


if __name__ == "__main__":
    main()
