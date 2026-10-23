"""Adversarial metadata, stale locks and real pip hash rejection; no provider."""
import hashlib
import json
import subprocess
import sys
import zipfile

from packaging.markers import default_environment
import pytest

from scripts import dependency_audit as audit


def environment(system="win32"):
    return {**default_environment(), "implementation_name": "cpython", "python_version": "3.13",
            "python_full_version": "3.13.9", "sys_platform": system}


def lock(entries):
    return "\n".join(sorted(audit.OPTIONS)) + "\n" + "\n".join(
        entry + " --hash=sha256:" + "a" * 64 for entry in entries) + "\n"


def fixture(root):
    (root / "requirements").mkdir()
    (root / "requirements/runtime.in").write_text("sample==1.0\n")
    (root / "requirements/browser.in").write_text("-r runtime.in\nviewer==2.0\n")
    (root / "requirements/security.in").write_text("auditor==4.0\n")
    (root / "requirements/locking.txt").write_text("uv==" + audit.UV_VERSION + " --hash=sha256:" + "a" * 64 + "\n")
    (root / "requirements-dev.txt").write_text("-r requirements.txt\n")
    (root / "requirements.txt").write_text(lock(["sample==1.0", "windows-only==3.0 ; sys_platform == 'win32'"]))
    (root / "requirements-browser.txt").write_text(lock(["sample==1.0", "windows-only==3.0 ; sys_platform == 'win32'", "viewer==2.0"]))
    (root / "requirements-security.txt").write_text(lock(["auditor==4.0"]))
    manifest = {"schema": audit.SCHEMA_VERSION, "python_series": "3.13", "targets": ["win32/AMD64", "linux/x86_64"],
                "resolver": {"name": "uv", "version": audit.UV_VERSION, "index": "https://pypi.org/simple", "cutoff": "2026-10-23T00:00:00Z"}}
    rewrite_manifest(root, manifest)
    return manifest


def rewrite_manifest(root, manifest):
    manifest["inputs"] = {path: audit.digest(root / path) for path in audit.INPUTS}
    manifest["locks"] = {path: audit.digest(root / path) for path in audit.LOCKS.values()}
    (root / audit.MANIFEST).write_text(json.dumps(manifest))


class Distribution:
    def __init__(self, name, version, direct=None):
        self.metadata, self.version, self.direct = {"Name": name}, version, direct
    def read_text(self, filename):
        assert filename == "direct_url.json"
        return self.direct


def installed(system="win32", browser=False):
    values = [Distribution("Sample", "1.0"), Distribution("pip", "25.2")]
    if system == "win32":
        values.append(Distribution("windows_only", "3.0"))
    if browser:
        values.append(Distribution("viewer", "2.0"))
    return values


@pytest.mark.parametrize("system,profile", [(system, profile) for system in ("win32", "linux") for profile in audit.LOCKS])
def test_exact_graph_with_platform_markers_and_profile(tmp_path, system, profile):
    fixture(tmp_path)
    values = ([Distribution("auditor", "4.0"), Distribution("pip", "25.2")] if profile == "security" else installed(system, profile == "browser"))
    report = audit.audit(profile, root=tmp_path, environment=environment(system),
                         machine="AMD64" if system == "win32" else "x86_64", distributions=values, check=lambda: True)
    assert report["passed"] and report["missing"] == report["unexpected"] == report["version_mismatches"] == []
    assert report["wheel_bytes_verified_by_this_audit"] is False and report["package_security_certified"] is False
    assert report["bootstrap_tools"] == {"pip": "25.2"}


@pytest.mark.parametrize("change,key", [("missing", "missing"), ("extra", "unexpected"),
    ("version", "version_mismatches"), ("duplicate", "duplicate_distributions"), ("direct", "direct_install_distributions")])
def test_graph_drift_fails_without_serializing_direct_urls(tmp_path, change, key):
    fixture(tmp_path)
    values = installed()
    if change == "missing":
        values.pop(0)
    elif change == "extra":
        values.append(Distribution("unreviewed", "1.0"))
    elif change == "version":
        values[0].version = "1.1"
    elif change == "duplicate":
        values.append(Distribution("SAMPLE", "1.0"))
    else:
        values[0].direct = '{"url":"https://PRIVATE-SECRET@example.invalid"}'
    report = audit.audit("runtime", root=tmp_path, environment=environment(), machine="AMD64", distributions=values, check=lambda: True)
    assert not report["passed"] and report[key]
    assert "PRIVATE" not in json.dumps(report)


def test_failed_dependency_check_cannot_pass_an_exact_version_set(tmp_path):
    fixture(tmp_path)
    class Check:
        def __bool__(self):
            return False
        def __call__(self):
            return False
    report = audit.audit("runtime", root=tmp_path, environment=environment(), machine="AMD64", distributions=installed(), check=Check())
    assert not report["passed"] and report["pip_check_passed"] is False


@pytest.mark.parametrize("change", ["python", "implementation", "os", "machine"])
def test_unsupported_target_cannot_claim_verification(tmp_path, change):
    env = environment()
    machine = "AMD64"
    if change == "python": env["python_version"] = "3.14"
    elif change == "implementation": env["implementation_name"] = "pypy"
    elif change == "os": env["sys_platform"] = "darwin"
    else: machine = "ARM64"
    with pytest.raises(audit.LockError, match="UNSUPPORTED"):
        audit.audit("runtime", root=tmp_path, environment=env, machine=machine, distributions=[], check=lambda: True)


@pytest.mark.parametrize("bad", [
    "sample>=1.0 --hash=sha256:" + "a" * 64,
    "sample==1.* --hash=sha256:" + "a" * 64,
    "sample[extra]==1.0 --hash=sha256:" + "a" * 64,
    "sample @ https://example.invalid/package.whl --hash=sha256:" + "a" * 64,
    "sample==1.0", "sample==1.0 --hash=md5:" + "a" * 32,
    "sample==1.0 --hash=sha256:" + "a" * 63,
    "--extra-index-url https://example.invalid", "-r external.txt", "-e ./project",
])
def test_unsafe_lock_syntax_fails_closed(bad):
    with pytest.raises(audit.LockError):
        audit.parse_lock("\n".join(sorted(audit.OPTIONS)) + "\n" + bad)


def test_overlapping_active_pins_are_rejected():
    entries = audit.parse_lock(lock(["sample==1.0", "Sample==2.0 ; python_version == '3.13'"]))
    with pytest.raises(audit.LockError, match="DUPLICATE"):
        audit.selected(entries, environment())


@pytest.mark.parametrize("path", [*audit.INPUTS, *audit.LOCKS.values()])
def test_stale_input_or_lock_is_never_silently_regenerated(tmp_path, path):
    fixture(tmp_path)
    with (tmp_path / path).open("a") as file:
        file.write("\n# changed without reviewed regeneration\n")
    with pytest.raises(audit.LockError, match="CHANGED"):
        audit.load_locks(tmp_path, environment())


@pytest.mark.parametrize("change", ["runtime_missing", "browser_mismatch", "invalid_date", "bool_schema", "path_traversal", "dev_alias"])
def test_self_consistent_manifest_cannot_hide_invalid_policy(tmp_path, change):
    manifest = fixture(tmp_path)
    if change == "runtime_missing":
        (tmp_path / "requirements.txt").write_text(lock(["other==1.0"]))
    elif change == "browser_mismatch":
        (tmp_path / "requirements-browser.txt").write_text(lock(["sample==1.1", "viewer==2.0"]))
    elif change == "invalid_date": manifest["resolver"]["cutoff"] = "2026-99-99T00:00:00Z"
    elif change == "bool_schema": manifest["schema"] = True
    elif change == "dev_alias": (tmp_path / "requirements-dev.txt").write_text("pytest\n")
    rewrite_manifest(tmp_path, manifest)
    if change == "path_traversal":
        manifest["inputs"]["../../private.key"] = "a" * 64
        (tmp_path / audit.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        audit.load_locks(tmp_path, environment())


def test_cross_platform_line_endings_do_not_change_lock_identity(tmp_path):
    fixture(tmp_path)
    for path in [*audit.INPUTS, *audit.LOCKS.values()]:
        file = tmp_path / path
        file.write_bytes(file.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    _, active = audit.load_locks(tmp_path, environment())
    assert active["runtime"]["sample"] == "1.0"


def test_committed_locks_cover_both_reviewed_targets():
    for system in ("win32", "linux"):
        _, active = audit.load_locks(environment=environment(system))
        assert active["runtime"]["httpx2"] == "2.16.0"
        assert active["browser"]["playwright"] == "1.64.0"
        assert active["security"]["pip-audit"] == "2.10.1"
        assert "httpx2-jsfetch" not in active["runtime"]


def test_real_pip_rejects_tampered_wheel_before_installing(tmp_path):
    # Valid local wheel, then corruption after the approved archive hash. A
    # network-free pip dry run must reject it before importing/executing payload.
    wheel = tmp_path / "ccs_integrity_probe-1.0-py3-none-any.whl"
    metadata = "ccs_integrity_probe-1.0.dist-info/"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(metadata + "METADATA", "Metadata-Version: 2.1\nName: ccs-integrity-probe\nVersion: 1.0\n")
        archive.writestr(metadata + "WHEEL", "Wheel-Version: 1.0\nGenerator: CCS-SIMULATED-TEST\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        archive.writestr(metadata + "RECORD", "")
    approved = hashlib.sha256(wheel.read_bytes()).hexdigest()
    with zipfile.ZipFile(wheel, "a") as archive:
        archive.writestr("ccs_integrity_probe.py", "raise RuntimeError('MUST_NOT_EXECUTE')\n")
    requirements = tmp_path / "corrupt.txt"
    requirements.write_text("ccs-integrity-probe==1.0 --hash=sha256:" + approved + "\n")
    result = subprocess.run([sys.executable, "-m", "pip", "--isolated", "--disable-pip-version-check", "install",
                             "--dry-run", "--no-index", "--find-links", str(tmp_path), "--require-hashes", "--only-binary=:all:",
                             "-r", str(requirements)], capture_output=True, text=True, timeout=30)
    assert result.returncode != 0 and "DO NOT MATCH THE HASHES" in result.stderr


def test_cli_preserves_sanitized_failure_report(tmp_path, monkeypatch, capsys):
    output = tmp_path / "environment.json"
    def unavailable(*args, **kwargs):
        raise ValueError("PRIVATE-SECRET-PAYLOAD")
    monkeypatch.setattr(audit, "audit", unavailable)
    monkeypatch.setattr(sys, "argv", ["dependency_audit", "--profile", "runtime", "--output", str(output)])
    with pytest.raises(SystemExit) as result:
        audit.main()
    assert result.value.code == 1 and json.loads(output.read_text())["passed"] is False
    assert "PRIVATE" not in output.read_text() + capsys.readouterr().out
