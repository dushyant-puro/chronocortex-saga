"""Verify committed dependency locks and installed metadata; never install or repair.

An exact metadata set is not an attestation of installed file bytes or package
trustworthiness. pip's hash-checked, wheel-only installation is a separate gate.
"""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version


ROOT = Path(__file__).resolve().parent.parent
UV_VERSION = "0.13.5"
INPUTS = ("requirements/runtime.in", "requirements/browser.in", "requirements/locking.txt", "requirements-dev.txt")
LOCKS = {"runtime": "requirements.txt", "browser": "requirements-browser.txt"}
OPTIONS = {"--require-hashes", "--only-binary :all:", "--index-url https://pypi.org/simple"}
MANIFEST = "requirements/manifest.json"
ALLOWED_TOOLS = {"pip"}  # CPython venv bootstrap; its actual version is reported.


class LockError(ValueError):
    pass


def digest(path):
    raw = Path(path).read_bytes()
    if len(raw) > 1_048_576:
        raise LockError("LOCK_CAPACITY_EXCEEDED")
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def pinned_requirement(text):
    try:
        requirement = Requirement(text)
        specifiers = list(requirement.specifier)
        if (requirement.url is not None or requirement.extras or len(specifiers) != 1 or
                specifiers[0].operator != "==" or "*" in specifiers[0].version):
            raise ValueError()
        Version(specifiers[0].version)
        return requirement
    except ValueError:
        raise LockError("LOCK_PIN_INVALID") from None


def parse_lock(text, *, require_policy=True):
    """Small strict subset of pip syntax; packaging owns PEP 508 semantics."""
    if type(text) is not str or len(text) > 1_048_576:
        raise LockError("LOCK_CAPACITY_EXCEEDED")
    entries, options, pending = [], set(), ""
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            if pending:
                raise LockError("LOCK_CONTINUATION_INVALID")
            continue
        continued = line.endswith("\\")
        pending += line[:-1].strip() + " " if continued else line
        if continued:
            continue
        logical, pending = pending.strip(), ""
        if logical.startswith("--"):
            if logical not in OPTIONS or logical in options:
                raise LockError("LOCK_OPTION_INVALID")
            options.add(logical)
            continue
        head, separator, tail = logical.partition(" --hash=")
        hashes = ("--hash=" + tail).split() if separator else []
        if (not hashes or len(set(hashes)) != len(hashes) or
                any(re.fullmatch(r"--hash=sha256:[0-9a-f]{64}", value) is None for value in hashes)):
            raise LockError("LOCK_HASH_INVALID")
        entries.append(pinned_requirement(head))
    if pending or not entries or (require_policy and options != OPTIONS):
        raise LockError("LOCK_POLICY_INCOMPLETE")
    return entries


def direct_inputs(root, profile):
    entries, includes = [], 0
    for line in (Path(root) / ("requirements/" + profile + ".in")).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line == "-r runtime.in" and profile == "browser":
            includes += 1
            continue
        entries.append(pinned_requirement(line))
    if includes != (1 if profile == "browser" else 0) or not entries:
        raise LockError("DIRECT_INPUT_INVALID")
    return entries


def selected(entries, environment):
    result = {}
    for entry in entries:
        if entry.marker is not None and not entry.marker.evaluate(environment):
            continue
        name = canonicalize_name(entry.name)
        if name in result:
            raise LockError("LOCK_ACTIVE_DUPLICATE")
        result[name] = str(Version(next(iter(entry.specifier)).version))
    return result


def load_locks(root=ROOT, environment=None):
    root = Path(root)
    data = json.loads((root / MANIFEST).read_text(encoding="utf-8"))
    if (type(data) is not dict or set(data) != {"schema", "python_series", "targets", "resolver", "inputs", "locks"} or
            type(data["schema"]) is not int or data["schema"] != 1 or data["python_series"] != "3.13" or
            data["targets"] != ["win32/AMD64", "linux/x86_64"] or
            type(data["resolver"]) is not dict or set(data["resolver"]) != {"name", "version", "index", "cutoff"} or
            data["resolver"]["name"] != "uv" or data["resolver"]["version"] != UV_VERSION or
            data["resolver"]["index"] != "https://pypi.org/simple" or
            type(data["resolver"]["cutoff"]) is not str or
            re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", data["resolver"]["cutoff"]) is None or
            type(data["inputs"]) is not dict or set(data["inputs"]) != set(INPUTS) or
            type(data["locks"]) is not dict or set(data["locks"]) != set(LOCKS.values())):
        raise LockError("LOCK_MANIFEST_INVALID")
    for path, expected in {**data["inputs"], **data["locks"]}.items():
        if type(expected) is not str or digest(root / path) != expected:
            raise LockError("LOCK_INPUT_OR_OUTPUT_CHANGED")
    datetime.strptime(data["resolver"]["cutoff"], "%Y-%m-%dT%H:%M:%SZ")
    alias = [line.strip() for line in (root / "requirements-dev.txt").read_text(encoding="utf-8").splitlines()
             if line.strip() and not line.lstrip().startswith("#")]
    if alias != ["-r requirements.txt"]:
        raise LockError("DEV_ALIAS_INVALID")
    parsed = {key: parse_lock((root / path).read_text(encoding="utf-8")) for key, path in LOCKS.items()}
    environment = default_environment() if environment is None else environment
    active = {key: selected(value, environment) for key, value in parsed.items()}
    if any(active["browser"].get(name) != version for name, version in active["runtime"].items()):
        raise LockError("LOCK_BROWSER_RUNTIME_DIVERGENCE")
    for profile in LOCKS:
        if any(active[profile].get(name) != version for name, version in
               selected(direct_inputs(root, profile), environment).items()):
            raise LockError("LOCK_DIRECT_PIN_MISSING")
    return data, active


def pip_check():
    try:
        result = subprocess.run([sys.executable, "-m", "pip", "--isolated", "--disable-pip-version-check", "check"],
                                capture_output=True, timeout=30)
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def audit(profile, *, root=ROOT, environment=None, distributions=None, check=None, machine=None):
    if profile not in LOCKS:
        raise LockError("PROFILE_INVALID")
    environment = default_environment() if environment is None else environment
    machine = platform.machine() if machine is None else machine
    supported = (environment.get("python_version") == "3.13" and environment.get("implementation_name") == "cpython" and
                 ((environment.get("sys_platform") == "win32" and machine == "AMD64") or
                  (environment.get("sys_platform") == "linux" and machine == "x86_64")))
    if not supported:
        raise LockError("UNSUPPORTED_ENVIRONMENT")
    manifest, active = load_locks(root, environment)
    expected = active[profile]
    counts, installed, direct = Counter(), {}, []
    distributions = importlib.metadata.distributions() if distributions is None else distributions
    for distribution in distributions:
        name = distribution.metadata.get("Name")
        if type(name) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", name) is None:
            raise LockError("INSTALLED_METADATA_INVALID")
        name = canonicalize_name(name)
        try:
            version = str(Version(distribution.version))
        except (ValueError, TypeError):
            raise LockError("INSTALLED_METADATA_INVALID") from None
        counts[name] += 1
        installed[name] = version
        if distribution.read_text("direct_url.json") is not None:
            direct.append(name)  # Never include URL, local path, credential or raw JSON.
    missing = sorted(set(expected) - set(installed))
    extra = sorted(set(installed) - set(expected) - ALLOWED_TOOLS)
    mismatched = sorted(name for name in expected.keys() & installed.keys() if installed[name] != expected[name])
    duplicates = sorted(name for name, count in counts.items() if count != 1)
    check = pip_check if check is None else check
    compatible = check() is True
    passed = not (missing or extra or mismatched or duplicates or direct) and compatible
    return {"schema": 1, "profile": profile, "passed": passed,
            "python": environment["python_full_version"], "platform": environment["sys_platform"], "machine": machine,
            "manifest_sha256": digest(Path(root) / MANIFEST), "lock_sha256": manifest["locks"][LOCKS[profile]],
            "missing": missing, "unexpected": extra, "version_mismatches": mismatched,
            "duplicate_distributions": duplicates, "direct_install_distributions": sorted(direct),
            "pip_check_passed": compatible, "expected_packages": len(expected), "installed": dict(sorted(installed.items())),
            "bootstrap_tools": {key: installed[key] for key in sorted(ALLOWED_TOOLS & installed.keys())},
            "wheel_bytes_verified_by_this_audit": False, "package_security_certified": False}


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
    parser.add_argument("--profile", choices=tuple(LOCKS), required=True)
    parser.add_argument("--output", default="results/environment.json")
    args = parser.parse_args()
    try:
        report = audit(args.profile)
    except Exception:
        report = {"schema": 1, "profile": args.profile, "passed": False, "failure": "ENVIRONMENT_OR_LOCK_UNAVAILABLE"}
    try:
        write_report(args.output, report)
    except (OSError, ValueError):
        parser.exit(2, "Environment audit could not preserve its report.\n")
    print(json.dumps({key: report[key] for key in ("profile", "passed")}))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
