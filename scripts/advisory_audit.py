"""Query known public advisories for every supported locked graph; never fix.

pip-audit runs in its own verified environment, with pip/resolution disabled.
Successful coverage means no known findings from that service during this run,
not package safety, publisher authenticity or a signed advisory-database snapshot.
"""
import argparse
from datetime import datetime, timezone
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
from packaging.utils import canonicalize_name
from packaging.version import Version

from scripts.dependency_audit import ROOT, LOCKS, MANIFEST, audit, digest, load_locks, parse_lock, write_report


TOOL_VERSION = "2.10.1"
MAX_REPORT_BYTES = 8_388_608
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")


class AdvisoryError(ValueError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def scanner_environment(temporary):
    # Child tools need OS/locale settings, not application/provider credentials.
    # A private empty netrc also avoids implicit authentication to the public feed.
    allowed = {"PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "LANG", "LC_ALL", "TZ"}
    env = {key: value for key, value in os.environ.items() if key.upper() in allowed}
    netrc = temporary / "empty.netrc"
    netrc.write_text("", encoding="ascii")
    env.update({"HOME": str(temporary), "USERPROFILE": str(temporary), "NETRC": str(netrc),
                "TEMP": str(temporary), "TMP": str(temporary), "XDG_CACHE_HOME": str(temporary / "cache")})
    return env


def target_environment(target):
    if target not in {"windows-amd64", "linux-x86_64"}:
        raise AdvisoryError("UNSUPPORTED_TARGET")
    windows = target == "windows-amd64"
    return {**default_environment(), "implementation_name": "cpython", "implementation_version": platform.python_version(),
            "python_version": "3.13", "python_full_version": platform.python_version(),
            "platform_python_implementation": "CPython", "sys_platform": "win32" if windows else "linux",
            "platform_system": "Windows" if windows else "Linux", "os_name": "nt" if windows else "posix",
            "platform_machine": "AMD64" if windows else "x86_64", "platform_release": "", "platform_version": ""}


def inventory(root=ROOT):
    for path in LOCKS.values():
        for entry in parse_lock((Path(root) / path).read_text(encoding="utf-8")):
            if entry.marker is not None and re.search(r"\b(platform_release|platform_version|extra|extras|dependency_groups)\b", str(entry.marker)):
                raise AdvisoryError("UNSUPPORTED_MARKER_SCOPE")
    groups, known = [], {}
    for target in ("windows-amd64", "linux-x86_64"):
        _, graphs = load_locks(root, target_environment(target))
        if graphs["security"].get("pip-audit") != TOOL_VERSION:
            raise AdvisoryError("UNSUPPORTED_TOOL_PIN")
        for profile, packages in graphs.items():
            key = tuple(sorted(packages.items()))
            if not 1 <= len(key) <= 1024:
                raise AdvisoryError("INVENTORY_CAPACITY_EXCEEDED")
            if key not in known:
                known[key] = len(groups)
                groups.append({"scopes": [], "packages": dict(key)})
            groups[known[key]]["scopes"].append({"target": target, "profile": profile})
    return groups


def _pairs(values):
    result = {}
    for key, value in values:
        if key in result:
            raise AdvisoryError("DUPLICATE_REPORT_KEY")
        result[key] = value
    return result


def _constant(_):
    raise AdvisoryError("NONFINITE_REPORT_VALUE")


def identifier(value):
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise AdvisoryError("ADVISORY_IDENTIFIER_INVALID")
    return value


def normalized_version(value):
    if type(value) is not str or not 1 <= len(value) <= 128:
        raise AdvisoryError("REPORT_VERSION_INVALID")
    return str(Version(value))


def validate_report(raw, expected, returncode):
    if type(raw) is not bytes or len(raw) > MAX_REPORT_BYTES:
        raise AdvisoryError("REPORT_CAPACITY_EXCEEDED")
    data = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
    if (type(data) is not dict or set(data) != {"dependencies", "fixes"} or data["fixes"] != [] or
            type(data["dependencies"]) is not list or len(data["dependencies"]) != len(expected)):
        raise AdvisoryError("REPORT_COVERAGE_INVALID")
    observed, findings = {}, []
    for dependency in data["dependencies"]:
        if type(dependency) is not dict or set(dependency) != {"name", "version", "vulns"}:
            raise AdvisoryError("SKIPPED_OR_INVALID_DEPENDENCY")
        name = dependency["name"]
        if type(name) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", name) is None:
            raise AdvisoryError("REPORT_NAME_INVALID")
        name = canonicalize_name(name)
        version = normalized_version(dependency["version"])
        if name in observed or expected.get(name) != version:
            raise AdvisoryError("REPORT_IDENTITY_MISMATCH")
        vulnerabilities = dependency["vulns"]
        if type(vulnerabilities) is not list or len(vulnerabilities) > 1024:
            raise AdvisoryError("ADVISORY_LIST_INVALID")
        clean = []
        for vulnerability in vulnerabilities:
            if (type(vulnerability) is not dict or not {"id", "aliases", "fix_versions"} <= vulnerability.keys() or
                    type(vulnerability["aliases"]) is not list or len(vulnerability["aliases"]) > 128 or
                    type(vulnerability["fix_versions"]) is not list or len(vulnerability["fix_versions"]) > 128):
                raise AdvisoryError("ADVISORY_EVIDENCE_INVALID")
            entry = {"id": identifier(vulnerability["id"]),
                     "aliases": sorted({identifier(value) for value in vulnerability["aliases"]}),
                     "fix_versions": sorted({normalized_version(value) for value in vulnerability["fix_versions"]})}
            if any(existing["id"] == entry["id"] for existing in clean):
                raise AdvisoryError("DUPLICATE_ADVISORY")
            clean.append(entry)
        clean.sort(key=lambda item: item["id"])
        observed[name] = {"name": name, "version": version, "advisories": clean}
        if clean:
            findings.append(observed[name])
    if observed.keys() != expected.keys() or type(returncode) is not int or returncode != (1 if findings else 0):
        raise AdvisoryError("REPORT_EXIT_OR_COVERAGE_MISMATCH")
    return {"complete": True, "dependencies": [observed[key] for key in sorted(observed)],
            "finding_packages": len(findings), "advisory_count": sum(len(row["advisories"]) for row in findings),
            "raw_report_sha256": hashlib.sha256(raw).hexdigest()}


def run(*, root=ROOT, runner=None, tool_version=None, environment_check=None):
    real = runner is None and tool_version is None and environment_check is None
    runner = subprocess.run if runner is None else runner
    tool_version = (lambda: importlib.metadata.version("pip-audit")) if tool_version is None else tool_version
    environment_check = (lambda: audit("security", root=root)) if environment_check is None else environment_check
    report = {"schema": 1, "source": "ONLINE_PYPI" if real else "SIMULATED", "started_at": utc_now(),
              "tool": {"name": "pip-audit", "version": TOOL_VERSION}, "service": "pypi",
              "coverage_complete": False, "passed": False, "disposition": "UNKNOWN", "groups": [],
              "automatic_remediation": False, "ignored_advisories": [], "package_security_certified": False}
    try:
        if platform.python_version_tuple()[:2] != ("3", "13") or tool_version() != TOOL_VERSION or environment_check().get("passed") is not True:
            raise AdvisoryError("UNVERIFIED_TOOL_ENVIRONMENT")
        groups = inventory(root)
        report["manifest_sha256"] = digest(Path(root) / MANIFEST)
        report["target_python"] = platform.python_version()
        report["inventory_sha256"] = hashlib.sha256(json.dumps(groups, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        # Empty, invocation-private HTTP cache: no prior run's clean status can
        # suppress a new query. Within-run reuse is bounded by the run lifetime.
        with tempfile.TemporaryDirectory(prefix="ccs-advisories-") as directory:
            temporary = Path(directory)
            env = scanner_environment(temporary)
            for index, group in enumerate(groups):
                requirement_file, output = temporary / f"graph-{index}.txt", temporary / f"report-{index}.json"
                requirement_file.write_text("".join(name + "==" + version + "\n" for name, version in group["packages"].items()), encoding="utf-8")
                command = [sys.executable, "-I", "-m", "pip_audit", "--disable-pip", "--no-deps", "--strict",
                           "--vulnerability-service", "pypi", "--format", "json", "--desc", "off", "--aliases", "on",
                           "--progress-spinner", "off", "--timeout", "10", "--cache-dir", str(temporary / "http-cache"),
                           "--output", str(output), "--requirement", str(requirement_file)]
                result = {"scopes": group["scopes"], "expected_packages": len(group["packages"]), "complete": False}
                try:
                    completed = runner(command, cwd=temporary, env=env, capture_output=True, timeout=180)
                    with output.open("rb") as stream:
                        raw = stream.read(MAX_REPORT_BYTES + 1)
                    result.update(validate_report(raw, group["packages"], completed.returncode))
                except subprocess.TimeoutExpired:
                    result["failure"] = "ADVISORY_QUERY_TIMEOUT"
                except (OSError, UnicodeError):
                    result["failure"] = "ADVISORY_QUERY_UNAVAILABLE"
                except Exception:
                    # No raw stderr, advisory description, URL or exception
                    # payload is an approved report field. Preserve UNKNOWN.
                    result["failure"] = "ADVISORY_REPORT_INCONSISTENT"
                report["groups"].append(result)
        # A changed bundle during network queries cannot receive a clean result
        # for the earlier inventory. No multi-file source snapshot is assumed.
        if digest(Path(root) / MANIFEST) != report["manifest_sha256"] or inventory(root) != groups:
            raise AdvisoryError("LOCK_BUNDLE_CHANGED_DURING_AUDIT")
        report["coverage_complete"] = all(group["complete"] for group in report["groups"])
        report["finding_packages"] = sum(group.get("finding_packages", 0) for group in report["groups"])
        report["advisory_count"] = sum(group.get("advisory_count", 0) for group in report["groups"])
        found = [(row["name"], row["version"], item["id"]) for group in report["groups"]
                 for row in group.get("dependencies", []) for item in row["advisories"]]
        report["unique_advisories"] = len({item[2] for item in found})
        report["unique_vulnerable_packages"] = len({item[:2] for item in found})
        report["passed"] = report["coverage_complete"] and report["advisory_count"] == 0
        report["disposition"] = ("ADVISORIES_FOUND" if report["advisory_count"] else
                                 "NO_KNOWN_ADVISORIES" if report["passed"] else "UNKNOWN")
    except Exception:
        report["failure"] = "TOOL_ENVIRONMENT_OR_LOCK_BUNDLE_UNVERIFIED"
    report["finished_at"] = utc_now()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--output", default="results/dependency-advisories.json")
    args = parser.parse_args()
    report = run()
    try:
        write_report(args.output, report)
    except (OSError, ValueError):
        parser.exit(2, "Advisory audit could not preserve its report.\n")
    print(json.dumps({key: report[key] for key in ("source", "coverage_complete", "passed", "disposition")}))
    raise SystemExit(0 if report["passed"] else 1 if report["coverage_complete"] else 2)


if __name__ == "__main__":
    main()
