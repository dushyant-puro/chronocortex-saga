"""Deterministic timeout evidence; no real shell or provider required."""
import subprocess

import pytest

from tests import windows_shell_support as support


@pytest.mark.parametrize("raw,expected", [
    (None, []), (True, []), ([], []), ("PRIVATE SECRET", []),
    ("CCS_KEY_SETUP_STAGE:PRIVATE_SECRET", []),
    (b"PRIVATE\xff\nCCS_KEY_SETUP_STAGE:VERIFY_ACL\r\n", ["VERIFY_ACL"]),
    ("CCS_KEY_SETUP_STAGE:PROCESS_STARTED\nCCS_KEY_SETUP_STAGE:FLUSH_FILE\n", ["PROCESS_STARTED", "FLUSH_FILE"]),
    ("prefix CCS_KEY_SETUP_STAGE:COMPLETE", []),
    ("CCS_KEY_SETUP_STAGE:COMPLETE PRIVATE", []),
])
def test_only_exact_bounded_stage_markers_are_retained(raw, expected):
    assert support.setup_stages(raw) == expected


def test_parser_bounds_tail_and_number_of_markers():
    assert support.setup_stages("CCS_KEY_SETUP_STAGE:FLUSH_FILE\n" + "x"*9000) == []
    stages = support.setup_stages("CCS_KEY_SETUP_STAGE:VERIFY_ACL\n"*1000)
    assert stages == ["VERIFY_ACL"]*64


@pytest.mark.parametrize("stderr,stage", [(None, "UNOBSERVED"), ("PRIVATE", "UNOBSERVED"),
    (b"CCS_KEY_SETUP_STAGE:PROCESS_STARTED\nCCS_KEY_SETUP_STAGE:READ_SECRET\nPRIVATE", "READ_SECRET"),
    ("CCS_KEY_SETUP_STAGE:COMPLETE\n", "COMPLETE")])
def test_timeout_has_closed_stage_without_command_or_partial_payload(monkeypatch, stderr, stage):
    monkeypatch.setattr(support.shutil, "which", lambda _: "powershell.exe")
    attempts = []
    def expired(args, **kwargs):
        attempts.append(1)
        assert kwargs["timeout"] == 20 and kwargs["capture_output"] and kwargs["text"]
        raise subprocess.TimeoutExpired(args, 20, output="PRIVATE OUTPUT", stderr=stderr)
    monkeypatch.setattr(support.subprocess, "run", expired)
    with pytest.raises(support.WindowsShellFailure) as error:
        support.run_shell("PRIVATE SOURCE WITH CREDENTIAL")
    assert error.value.reason == "TIMEOUT" and error.value.stage == stage
    assert "PRIVATE" not in str(error.value) + repr(error.value)
    assert not hasattr(error.value, "cmd") and not hasattr(error.value, "stdout")
    assert attempts == [1]


def test_success_preserves_return_and_output_but_omits_command_source(monkeypatch):
    monkeypatch.setattr(support.shutil, "which", lambda _: "powershell.exe")
    monkeypatch.setenv("PsModulePath", "PRIVATE INCOMPATIBLE MODULES")
    def completed(args, **kwargs):
        assert not any(k.upper()=="PSMODULEPATH" for k in kwargs["env"])
        assert args[:3] == ["powershell.exe", "-NoProfile", "-NonInteractive"]
        assert "PROCESS_STARTED" in args[-1]
        return subprocess.CompletedProcess(args, 7, "EXPECTED OUTPUT", "EXPECTED ERROR")
    monkeypatch.setattr(support.subprocess, "run", completed)
    result = support.run_shell("PRIVATE SOURCE")
    assert (result.returncode, result.stdout, result.stderr) == (7, "EXPECTED OUTPUT", "EXPECTED ERROR")
    assert "PRIVATE" not in repr(result)


@pytest.mark.parametrize("missing", [True, False])
def test_launch_errors_do_not_disclose_local_paths_or_exception_text(monkeypatch, missing):
    monkeypatch.setattr(support.shutil, "which", lambda _: None if missing else "powershell.exe")
    def fail(*_, **__): raise OSError("PRIVATE PATH AND PAYLOAD")
    monkeypatch.setattr(support.subprocess, "run", fail)
    with pytest.raises(support.WindowsShellFailure) as error:
        support.run_shell("PRIVATE SOURCE")
    assert error.value.reason == ("UNAVAILABLE" if missing else "LAUNCH_FAILED")
    assert error.value.stage == "UNOBSERVED" and "PRIVATE" not in str(error.value)
