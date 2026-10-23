"""Exercise real Windows ACLs with SIMULATED keys; never contact Stripe."""
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from runtime.credential_input import read_stripe_key_file
from test_stripe_sandbox import KEY


pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows ACL and masked PowerShell key setup")
SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "save_stripe_sandbox_key.ps1"


def shell(source):
    # A PowerShell 7 parent can export its incompatible modules into Windows
    # PowerShell 5.1. Let this subprocess discover its own standard modules.
    environment = {name: value for name, value in os.environ.items() if name.upper() != "PSMODULEPATH"}
    return subprocess.run([shutil.which("powershell.exe"), "-NoProfile", "-NonInteractive", "-Command", source],
                          capture_output=True, text=True, timeout=20, env=environment)


def quoted(value):
    return "'" + str(value).replace("'", "''") + "'"


def invoke(directory, key=KEY, *, input_body=None, prefix=""):
    # Only SIMULATED data is in this test command. Real users enter a key at the
    # masked prompt; the script exposes no parameter or environment-key fallback.
    body = input_body or (
        "if (-not $AsSecureString) { throw 'unmasked input' }; "
        f"$acl = Get-Acl -LiteralPath {quoted(directory)}; "
        "if (-not $acl.AreAccessRulesProtected) { throw 'prompt before protection' }; "
        f"ConvertTo-SecureString -String {quoted(key)} -AsPlainText -Force"
    )
    source = (prefix + "function Read-Host { param([string]$Prompt, [switch]$AsSecureString); " + body + " }; "
              f"& {quoted(SCRIPT)} -Directory {quoted(directory)}; exit $LASTEXITCODE")
    return shell(source)


def test_masked_input_protected_before_prompt_and_exact_ascii_file_is_readable(tmp_path):
    directory = tmp_path / "new-private-folder"
    result = invoke(directory)
    assert result.returncode == 0, result.stderr
    path = directory / "stripe-test.key"
    assert path.read_bytes() == KEY.encode("ascii")
    assert read_stripe_key_file(path) == KEY
    assert KEY not in result.stdout + result.stderr
    assert str(path) in result.stdout
    # The file inherits only the owner/SYSTEM grants from the protected folder.
    source = (f"$acl = Get-Acl -LiteralPath {quoted(path)}; "
              "$rules = @($acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier])); "
              "$owner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value; "
              "if ($rules.Count -ne 2) { exit 1 }; "
              "foreach ($rule in $rules) { if ($rule.IdentityReference.Value -notin @($owner, 'S-1-5-18') -or "
              "-not $rule.IsInherited -or $rule.AccessControlType -ne 'Allow' -or "
              "$rule.FileSystemRights -ne [System.Security.AccessControl.FileSystemRights]::FullControl) { exit 2 } }")
    verified = shell(source)
    assert verified.returncode == 0, verified.stderr


@pytest.mark.parametrize("key", ["rk_live_" + "x" * 32, "sk_test_" + "x" * 32, "sk_live_" + "x" * 32,
                                "pk_test_" + "x" * 32, KEY + "\n", KEY + " ", "rk_test_short", "x" * 300])
def test_unsafe_key_is_not_saved_or_echoed(tmp_path, key):
    directory = tmp_path / "private"
    result = invoke(directory, key)
    assert result.returncode == 1 and "KEY_INPUT_INVALID" in result.stderr
    assert key not in result.stdout + result.stderr
    assert not (directory / "stripe-test.key").exists()


def test_existing_directory_is_never_repermissioned_or_overwritten(tmp_path):
    directory = tmp_path / "existing"
    directory.mkdir()
    path = directory / "stripe-test.key"
    path.write_text("SIMULATED-original", encoding="ascii")
    before = shell(f"(Get-Acl -LiteralPath {quoted(directory)}).Sddl").stdout
    result = invoke(directory, input_body="throw 'must not prompt'")
    assert result.returncode == 1 and "DIRECTORY_EXISTS" in result.stderr
    assert path.read_text(encoding="ascii") == "SIMULATED-original"
    assert shell(f"(Get-Acl -LiteralPath {quoted(directory)}).Sddl").stdout == before


def test_existing_file_is_not_replaced_by_a_directory(tmp_path):
    target = tmp_path / "existing-file"
    target.write_text("SIMULATED-original", encoding="ascii")
    result = invoke(target, input_body="throw 'must not prompt'")
    assert result.returncode == 1 and "DIRECTORY_EXISTS" in result.stderr
    assert target.read_text(encoding="ascii") == "SIMULATED-original"


def test_failed_acl_verification_stops_before_secret_prompt(tmp_path):
    directory = tmp_path / "private"
    # Simulate an OS/administration failure leaving the inherited ACL intact.
    result = invoke(directory, input_body="throw 'must not prompt'",
                    prefix="function Set-Acl { param($LiteralPath, $AclObject) }; ")
    assert result.returncode == 1 and "ACL_NOT_VERIFIED" in result.stderr
    assert not (directory / "stripe-test.key").exists()


def test_file_created_during_input_is_not_overwritten(tmp_path):
    directory = tmp_path / "private"
    path = directory / "stripe-test.key"
    body = (f"[System.IO.File]::WriteAllText({quoted(path)}, 'SIMULATED-original'); "
            f"ConvertTo-SecureString -String {quoted(KEY)} -AsPlainText -Force")
    result = invoke(directory, input_body=body)
    assert result.returncode == 1 and "KEY_SAVE_FAILED" in result.stderr
    assert path.read_text(encoding="ascii") == "SIMULATED-original"
    assert KEY not in result.stdout + result.stderr


@pytest.mark.parametrize("target", ["relative-folder", "C:drive-relative-folder", "\\\\server\\share\\ccs", "missing-parent"])
def test_unsafe_paths_are_rejected_before_prompt_or_creation(tmp_path, target):
    if target == "missing-parent":
        target = tmp_path / "absent" / "nested"
    result = invoke(target, input_body="throw 'must not prompt'")
    assert result.returncode == 1 and "UNSAFE_DIRECTORY" in result.stderr
    if isinstance(target, Path):
        assert not target.exists() and not target.parent.exists()


@pytest.mark.parametrize("marker_type", ["directory", "worktree-file"])
def test_git_ancestor_is_rejected_before_prompt_even_for_nested_directory(tmp_path, marker_type):
    checkout = tmp_path / "checkout"
    nested = checkout / "nested"
    nested.mkdir(parents=True)
    marker = checkout / ".git"
    if marker_type == "directory":
        marker.mkdir()
    else:
        marker.write_text("gitdir: SIMULATED-worktree", encoding="ascii")
    directory = nested / "private"
    result = invoke(directory, input_body="throw 'must not prompt'")
    assert result.returncode == 1 and "UNSAFE_DIRECTORY" in result.stderr
    assert not directory.exists()


def test_junction_ancestor_is_rejected_before_prompt_or_creation(tmp_path):
    target, junction = tmp_path / "target", tmp_path / "junction"
    target.mkdir()
    source = f"New-Item -ItemType Junction -Path {quoted(junction)} -Target {quoted(target)} | Out-Null"
    created = shell(source)
    assert created.returncode == 0, created.stderr
    result = invoke(junction / "private", input_body="throw 'must not prompt'")
    assert result.returncode == 1 and "UNSAFE_DIRECTORY" in result.stderr
    assert list(target.iterdir()) == []


def test_prompt_exception_is_sanitized_and_leaves_no_key(tmp_path):
    directory = tmp_path / "private"
    result = invoke(directory, input_body=f"throw {quoted(KEY)}")
    assert result.returncode == 1 and "KEY_INPUT_INVALID" in result.stderr
    assert KEY not in result.stdout + result.stderr
    assert not (directory / "stripe-test.key").exists()
