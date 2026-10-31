"""Independent-process, real loopback TCP fault drills. All effects SIMULATED.

No provider credentials, external hosts, custom endpoints or live mode switches.
Worker, fault proxy and provider service run in separate OS processes. The
service's ledger is an independent effect oracle, never a CCS state projection.
"""
import argparse
from contextlib import closing, ExitStack, contextmanager
import hashlib
import json
import logging
import os
from pathlib import Path
import platform
import queue
import re
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
from runtime.failure_diagnostics import CATEGORIES, classify_failure
from eval import failure_evidence
from eval.failure_evidence import CASES


ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger("ccs.eval.provider_faults")


class DrillFailure(RuntimeError):
    """Only bounded internal failure codes escape the evaluator."""
    CODES = frozenset({"PEER_CONTROL_UNAVAILABLE", "PEER_ENDED_BEFORE_BARRIER", "PEER_BARRIER_TIMEOUT",
                       "PEER_EXIT_TIMEOUT", "PEER_EXIT_OR_PROTOCOL_MISMATCH", "PEER_PIPE_DRAIN_TIMEOUT"})
    EVENTS = frozenset({"READY", "STRIPE_SANDBOX_APPROVAL_REQUIRED", "REQUEST_DISCARDED_BEFORE_FORWARD",
                        "RESPONSE_WITHHELD_AFTER_APPLY", "DROP_RELEASED", "VISIBILITY_RELEASED"})

    def __init__(self, code, *, role=None, wait=None, event=None, category=None):
        self.code = code if type(code) is str and code in self.CODES else "INCOMPLETE_DRILL"
        self.diagnostic = {"category": category if type(category) is str and category in CATEGORIES else "UNCLASSIFIED",
                           "peer_role": role if type(role) is str and role in {"service", "proxy", "worker", "recover"} else "UNKNOWN",
                           "wait": wait if type(wait) is str and wait in {"CONTROL", "BARRIER", "EXIT", "PIPE_DRAIN"} else "UNKNOWN",
                           "expected_event": event if type(event) is str and event in self.EVENTS else None}
        super().__init__(self.code)


class Peer:
    def failure(self, code, **details):
        failure = DrillFailure(code, role=self.role, **details)
        # Direct pytest callers may fail before score() writes JSON. Preserve
        # the same bounded context in captured logs, never raw peer stderr.
        try:
            logger.error("SIMULATED peer failure; code=%s diagnostic=%s", failure.code, failure.diagnostic)
        except Exception:
            pass
        return failure

    def __init__(self, role, *arguments):
        self.role = role
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
        except (OSError, ValueError) as error:
            raise self.failure("PEER_CONTROL_UNAVAILABLE", wait="CONTROL", category=classify_failure(error)) from None

    def receive(self, event):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                record = self.messages.get(timeout=max(.001, deadline - time.monotonic()))
            except queue.Empty:
                break
            if record is None or record["event"] in {"PEER_FAILED", "BARRIER_EXPIRED", "HTTP_HANDLER_FAILED"}:
                raise self.failure("PEER_ENDED_BEFORE_BARRIER", wait="BARRIER", event=event,
                                   category=None if record is None else record.get("failure_category"))
            if record["event"] == event:
                return record
        raise self.failure("PEER_BARRIER_TIMEOUT", wait="BARRIER", event=event, category="TIMEOUT")

    def finish(self, expected):
        try:
            code = self.process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            raise self.failure("PEER_EXIT_TIMEOUT", wait="EXIT", category="TIMEOUT") from None
        self._join()
        if code != expected or self.invalid_output:
            failure = next((e for e in reversed(self.events) if e["event"] == "PEER_FAILED"), {})
            raise self.failure("PEER_EXIT_OR_PROTOCOL_MISMATCH", wait="EXIT", category=failure.get("failure_category"))

    def kill(self):
        self.process.kill()  # Fault injection/fixture teardown, never rollback.
        self.process.wait(timeout=5)
        self._join()

    def _join(self):
        for thread in self.threads:
            thread.join(timeout=2)
        if any(thread.is_alive() for thread in self.threads):
            raise self.failure("PEER_PIPE_DRAIN_TIMEOUT", wait="PIPE_DRAIN", category="TIMEOUT")

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


@contextmanager
def failure_boundary(root, name, peers, output):
    """Observe before ExitStack tears down peers; diagnostics never replace failure."""
    try:
        yield
    except Exception as error:
        try:
            evidence = failure_evidence.snapshot(root, peers)
            error.failure_evidence = evidence
        except Exception:
            evidence = None
        if evidence is not None:
            try:
                logger.error("SIMULATED pre-cleanup failure observations: %s",
                             json.dumps(evidence.data(), sort_keys=True, allow_nan=False))
            except Exception:
                pass
        if evidence is not None and output is not None:
            try:
                failure_evidence.publish(output, name, evidence)
            except Exception:
                pass  # Preserve original error even if observation storage fails.
        raise


def run_case(root, name, *, failure_output=None):
    if name not in CASES:
        raise ValueError("Unknown fault scenario")
    checks = {}
    peers = []
    with ExitStack() as stack, failure_boundary(root, name, peers, failure_output):
        journal = setup(root)
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
                case = {"id": name, "passed": False, "checks": {"drill_completed": False},
                              "failure": exc.code if isinstance(exc, DrillFailure) else "INCOMPLETE_DRILL",
                              "diagnostic": exc.diagnostic if isinstance(exc, DrillFailure) else {"category": classify_failure(exc)}}
                if type(getattr(exc, "failure_evidence", None)) is failure_evidence.CapturedEvidence:
                    case["failure_evidence"] = exc.failure_evidence.data()
                cases.append(case)
    digest = hashlib.sha256()
    for path in (Path(__file__), Path(__file__).with_name("provider_fault_peer.py"),
                 Path(__file__).with_name("failure_evidence.py"), ROOT / "runtime/failure_diagnostics.py"):
        digest.update(path.name.encode() + b"\0" + path.read_bytes().replace(b"\r\n", b"\n"))
    return {"schema": 1, "label": "SIMULATED", "harness_sha256": digest.hexdigest(), "provenance": provenance(),
            "case_count": len(cases),
            "cases_passed": sum(c["passed"] for c in cases), "cases_failed": sum(not c["passed"] for c in cases),
            "checks_passed": sum(sum(c["checks"].values()) for c in cases),
            "checks_failed": sum(sum(not v for v in c["checks"].values()) for c in cases), "cases": cases,
            "real_provider": "NOT_RUN", "real_audio": "NOT_RUN", "performance_claim": "NONE"}


def provenance():
    result = {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
              "platform": platform.system(), "source_commit": None, "source_dirty": None}
    try:
        revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, timeout=5)
        status = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"],
                                cwd=ROOT, capture_output=True, text=True, timeout=5)
        if revision.returncode == 0 and re.fullmatch(r"[0-9a-f]{40,64}", revision.stdout.strip()):
            result["source_commit"] = revision.stdout.strip()
        if status.returncode == 0:
            result["source_dirty"] = bool(status.stdout)
    except (OSError, subprocess.TimeoutExpired):
        pass  # Unavailable source identity is never represented as clean.
    return result


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
