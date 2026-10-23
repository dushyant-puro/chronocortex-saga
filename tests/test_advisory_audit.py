"""SIMULATED scanner outcomes; no advisory network or business provider calls."""
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import advisory_audit as command
from scripts import dependency_audit as locks
from test_dependency_audit import fixture, lock, rewrite_manifest


def prepare(root):
    manifest = fixture(root)
    (root / "requirements/security.in").write_text("pip-audit==" + command.TOOL_VERSION + "\n")
    (root / "requirements-security.txt").write_text(lock(["pip-audit==" + command.TOOL_VERSION]))
    rewrite_manifest(root, manifest)


class Scanner:
    def __init__(self, fault=None, *, each=False):
        self.calls, self.fault, self.each = [], fault, each

    def __call__(self, args, **kwargs):
        self.calls.append(args)
        assert args[1:4] == ["-I", "-m", "pip_audit"]
        assert "--disable-pip" in args and "--no-deps" in args and "--strict" in args
        assert "--fix" not in args and "--ignore-vuln" not in args and "--dry-run" not in args
        assert args[args.index("--vulnerability-service") + 1] == "pypi"
        assert not any(key.startswith(("PIP_", "UV_")) for key in kwargs["env"])
        pins = Path(args[args.index("--requirement") + 1]).read_text().splitlines()
        data = {"dependencies": [{"name": name, "version": version, "vulns": []}
                                  for name, version in (pin.split("==") for pin in pins)], "fixes": []}
        code, raw = 0, None
        if (len(self.calls) == 1 or self.each) and self.fault is not None:
            code, raw = self.fault(data)
        Path(args[args.index("--output") + 1]).write_bytes(json.dumps(data).encode() if raw is None else raw)
        return SimpleNamespace(returncode=code)


def execute(root, scanner, **kwargs):
    return command.run(root=root, runner=scanner, tool_version=lambda: command.TOOL_VERSION,
                       environment_check=lambda: {"passed": True}, **kwargs)


def test_complete_coverage_of_all_targets_profiles_and_unique_graphs(tmp_path, monkeypatch):
    prepare(tmp_path)
    monkeypatch.setenv("PIP_AUDIT_OUTPUT", "PRIVATE-UNAPPROVED-OUTPUT")
    monkeypatch.setenv("UV_INDEX", "https://example.invalid")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "PRIVATE-PROVIDER-KEY")
    monkeypatch.setenv("OPENAI_API_KEY", "PRIVATE-PROVIDER-KEY")
    scanner = Scanner()
    report = execute(tmp_path, scanner)
    assert report["source"] == "SIMULATED" and report["passed"] and report["coverage_complete"]
    assert report["disposition"] == "NO_KNOWN_ADVISORIES" and report["advisory_count"] == 0
    scopes = {(scope["target"], scope["profile"]) for group in report["groups"] for scope in group["scopes"]}
    assert scopes == {(target, profile) for target in ("windows-amd64", "linux-x86_64") for profile in locks.LOCKS}
    assert len(scanner.calls) == len(command.inventory(tmp_path)) == 5
    assert report["automatic_remediation"] is False and report["package_security_certified"] is False
    assert "PRIVATE" not in json.dumps(report)


def test_scanner_child_has_no_provider_credentials_or_ambient_netrc(tmp_path, monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "PRIVATE-PROVIDER-KEY")
    monkeypatch.setenv("OPENAI_API_KEY", "PRIVATE-PROVIDER-KEY")
    monkeypatch.setenv("NETRC", "PRIVATE-NETRC-PATH")
    monkeypatch.setenv("HTTPS_PROXY", "https://PRIVATE.example.invalid")
    env = command.scanner_environment(tmp_path)
    assert not {"STRIPE_SECRET_KEY", "OPENAI_API_KEY", "HTTPS_PROXY"} & env.keys()
    assert env["HOME"] == env["USERPROFILE"] == str(tmp_path)
    assert Path(env["NETRC"]).read_text() == ""
    assert "PRIVATE" not in json.dumps(env)


def test_falsey_runner_never_selects_real_scanner(tmp_path, monkeypatch):
    prepare(tmp_path)
    class Falsey(Scanner):
        def __bool__(self): return False
    monkeypatch.setattr(command.subprocess, "run", lambda *a, **k: pytest.fail("Unexpected real advisory query"))
    assert execute(tmp_path, Falsey())["passed"]


def advisory():
    return {"id": "GHSA-simulated-1234", "aliases": ["CVE-2026-12345"], "fix_versions": ["2.0"],
            "description": "PRIVATE: hostile description", "url": "https://PRIVATE.example.invalid"}


def test_known_findings_fail_without_fix_or_suppression(tmp_path):
    prepare(tmp_path)
    def vulnerable(data):
        data["dependencies"][0]["vulns"] = [advisory()]
        return 1, None
    report = execute(tmp_path, Scanner(vulnerable))
    assert report["coverage_complete"] and not report["passed"]
    assert report["disposition"] == "ADVISORIES_FOUND" and report["finding_packages"] == report["advisory_count"] == 1
    assert report["ignored_advisories"] == [] and report["automatic_remediation"] is False
    assert report["unique_advisories"] == report["unique_vulnerable_packages"] == 1
    serialized = json.dumps(report)
    assert "CVE-2026-12345" in serialized and "PRIVATE" not in serialized


def test_shared_advisory_is_not_counted_as_distinct_vulnerabilities(tmp_path):
    prepare(tmp_path)
    def vulnerable(data):
        for dependency in data["dependencies"]:
            if dependency["name"] == "sample":
                dependency["vulns"] = [advisory()]
                return 1, None
        return 0, None
    report = execute(tmp_path, Scanner(vulnerable, each=True))
    assert report["coverage_complete"] and not report["passed"]
    assert report["advisory_count"] == 4 and report["finding_packages"] == 4
    assert report["unique_advisories"] == report["unique_vulnerable_packages"] == 1


@pytest.mark.parametrize("fault", ["missing", "extra", "duplicate", "wrong_version", "skipped", "wrong_name",
    "bad_json", "duplicate_key", "nonfinite", "fixes", "clean_exit_with_findings", "error_exit", "unknown_exit",
    "bad_aliases", "bad_fix_versions", "bad_id", "duplicate_advisory", "invalid_vulns"])
def test_incomplete_or_inconsistent_query_never_reports_clear(tmp_path, fault):
    prepare(tmp_path)
    def corrupt(data):
        first = data["dependencies"][0]
        if fault == "missing": data["dependencies"].pop()
        elif fault == "extra": data["dependencies"].append({"name": "unreviewed", "version": "1.0", "vulns": []})
        elif fault == "duplicate": data["dependencies"][1] = first.copy()
        elif fault == "wrong_version": first["version"] = "999.0"
        elif fault == "skipped": data["dependencies"][0] = {"name": first["name"], "skip_reason": "PRIVATE"}
        elif fault == "wrong_name": first["name"] = "https://PRIVATE.example.invalid"
        elif fault == "bad_json": return 0, b"<html>PRIVATE ERROR</html>"
        elif fault == "duplicate_key": return 0, b'{"dependencies":[],"dependencies":[],"fixes":[]}'
        elif fault == "nonfinite": return 0, b'{"dependencies":NaN,"fixes":[]}'
        elif fault == "fixes": data["fixes"] = [{"name": "sample", "new_version": "2.0"}]
        elif fault == "error_exit": return 1, None
        elif fault == "unknown_exit": return 2, None
        elif fault == "invalid_vulns": first["vulns"] = {}
        else:
            item = advisory()
            if fault == "bad_aliases": item["aliases"] = "CVE-2026-12345"
            elif fault == "bad_fix_versions": item["fix_versions"] = [True]
            elif fault == "bad_id": item["id"] = "https://PRIVATE.example.invalid"
            first["vulns"] = [item, item] if fault == "duplicate_advisory" else [item]
        return (1 if fault in {"bad_aliases", "bad_fix_versions", "bad_id", "duplicate_advisory"} else 0), None
    report = execute(tmp_path, Scanner(corrupt))
    assert not report["passed"] and not report["coverage_complete"] and report["disposition"] == "UNKNOWN"
    assert "PRIVATE" not in json.dumps(report)


@pytest.mark.parametrize("failure", ["timeout", "missing_output", "oversized"])
def test_unavailable_evidence_is_unknown_not_success(tmp_path, monkeypatch, failure):
    prepare(tmp_path)
    scanner = Scanner()
    if failure == "oversized":
        monkeypatch.setattr(command, "MAX_REPORT_BYTES", 8)
    else:
        def scanner(*args, **kwargs):
            if failure == "timeout": raise subprocess.TimeoutExpired("PRIVATE", 1)
            return SimpleNamespace(returncode=0)
    report = execute(tmp_path, scanner)
    assert not report["coverage_complete"] and not report["passed"] and report["disposition"] == "UNKNOWN"
    assert "PRIVATE" not in json.dumps(report)
    if failure == "timeout": assert report["groups"][0]["failure"] == "ADVISORY_QUERY_TIMEOUT"


@pytest.mark.parametrize("failure", ["version", "environment", "stale_input", "during_scan"])
def test_unverified_or_changed_inputs_cannot_clear(tmp_path, failure):
    prepare(tmp_path)
    scanner = Scanner()
    version = lambda: "0.0.0" if failure == "version" else command.TOOL_VERSION
    check = lambda: {"passed": failure != "environment"}
    if failure == "stale_input":
        with (tmp_path / "requirements/runtime.in").open("a") as stream: stream.write("\n# PRIVATE CHANGE")
    elif failure == "during_scan":
        def change(data):
            with (tmp_path / "requirements/runtime.in").open("a") as stream: stream.write("\n# PRIVATE CHANGE")
            return 0, None
        scanner = Scanner(change)
    report = command.run(root=tmp_path, runner=scanner, tool_version=version, environment_check=check)
    assert not report["passed"] and not report["coverage_complete"]
    assert "PRIVATE" not in json.dumps(report)
    if failure != "during_scan": assert scanner.calls == []


def test_unknown_marker_scope_cannot_be_silently_skipped(tmp_path):
    manifest = fixture(tmp_path)
    (tmp_path / "requirements.txt").write_text(lock(["sample==1.0 ; platform_release == 'unknown'"]))
    rewrite_manifest(tmp_path, manifest)
    with pytest.raises(command.AdvisoryError, match="MARKER_SCOPE"):
        command.inventory(tmp_path)


def test_committed_inventory_covers_every_profile_and_tool_pin():
    groups = command.inventory()
    assert len(groups) >= 3
    assert sum(len(group["scopes"]) for group in groups) == 6
    assert any(group["packages"].get("pip-audit") == command.TOOL_VERSION for group in groups)


@pytest.mark.parametrize("disposition,complete,passed,code", [
    ("NO_KNOWN_ADVISORIES", True, True, 0), ("ADVISORIES_FOUND", True, False, 1), ("UNKNOWN", False, False, 2)])
def test_cli_exit_and_report_preserve_each_verdict(tmp_path, monkeypatch, disposition, complete, passed, code):
    output = tmp_path / "report.json"
    monkeypatch.setattr(command, "run", lambda: {"source": "SIMULATED", "coverage_complete": complete, "passed": passed, "disposition": disposition})
    monkeypatch.setattr(sys, "argv", ["advisory_audit", "--output", str(output)])
    with pytest.raises(SystemExit) as result: command.main()
    assert result.value.code == code and json.loads(output.read_text())["disposition"] == disposition
