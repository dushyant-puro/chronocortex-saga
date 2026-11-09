"""Native ACL protection before launching the SIMULATED walkthrough."""
import json
import os
from pathlib import Path

import pytest

from tests.windows_shell_support import run_shell, setup_stages

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Native Windows acceptance launcher ACLs")
SCRIPT = Path(__file__).resolve().parents[1] / "scripts/start_local_acceptance.ps1"


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


@pytest.mark.parametrize('review', [False, True])
def test_launch_directory_is_unique_protected_and_outside_checkout_before_python(tmp_path, review):
    stub = tmp_path / "python-stub.ps1"
    stub.write_text("""
$i = [Array]::IndexOf($args, '--directory')
if ($i -lt 0) { exit 3 }
$folder = $args[$i + 1]
$acl = Get-Acl -LiteralPath $folder
$rules = $acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier])
@{ module = $args[1]; protected = $acl.AreAccessRulesProtected;
   inherited = @($rules | Where-Object { $_.IsInherited }).Count;
   sids = @($rules | ForEach-Object { $_.IdentityReference.Value });
   owner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $folder 'proof.json') -Encoding UTF8
exit 0
""", encoding="ascii")
    for _ in range(2):
        entry = SCRIPT.parent / 'start_review_walkthrough.ps1' if review else SCRIPT
        result = run_shell(f"& {quote(entry)} -Diagnostics -Python {quote(stub)} -StorageParent {quote(tmp_path)}; exit $LASTEXITCODE")
        assert result.returncode == 0, result.stderr
        assert setup_stages(result.stderr) == ["PROCESS_STARTED", "VALIDATE_PATH", "CREATE_DIRECTORY",
            "APPLY_ACL", "VERIFY_ACL", "LAUNCH_RUNTIME", "RUNTIME_EXIT"]
    folders = list(tmp_path.glob("CCS-Acceptance-*"))
    assert len(folders) == 2
    for folder in folders:
        proof = json.loads((folder / "proof.json").read_text(encoding="utf-8-sig"))
        assert proof["protected"] and proof["inherited"] == 0
        assert proof['module'] == ('acceptance.review' if review else 'acceptance.local')
        assert set(proof["sids"]) == {proof["owner"], "S-1-5-18"}


def test_invalid_port_never_creates_an_acceptance_directory(tmp_path):
    result = run_shell(f"& {quote(SCRIPT)} -Port 80 -StorageParent {quote(tmp_path)}; if (-not $?) {{ exit 2 }}")
    assert result.returncode != 0 and not list(tmp_path.iterdir())


@pytest.mark.parametrize("damage", ["unprotected", "wrong_owner", "extra_rule", "deny", "inheritance", "rights", "propagation"])
def test_acl_readback_mismatch_never_starts_runtime(tmp_path, damage):
    stub = tmp_path / "python-stub.ps1"
    marker = tmp_path / "runtime-started"
    stub.write_text(f"Set-Content -LiteralPath {quote(marker)} -Value started\nexit 0", encoding="utf-8")
    # Change only the returned security descriptor. The native Set-Acl still runs.
    mutations = {
        "unprotected": "$acl.SetAccessRuleProtection($false, $true)",
        "wrong_owner": "$acl.SetOwner([System.Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))",
        "extra_rule": "$acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new('S-1-1-0','Read','Allow'))",
        "deny": "$acl.AddAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new($sid,'Read','Deny'))",
        "inheritance": "$acl.SetAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','None','None','Allow'))",
        "rights": "$acl.SetAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new($sid,'Read','ContainerInherit,ObjectInherit','None','Allow'))",
        "propagation": "$acl.SetAccessRule([System.Security.AccessControl.FileSystemAccessRule]::new($sid,'FullControl','ContainerInherit,ObjectInherit','InheritOnly','Allow'))",
    }
    source = ("function Get-Acl { param($LiteralPath); "
              "$acl = Microsoft.PowerShell.Security\\Get-Acl -LiteralPath $LiteralPath; "
              "$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User; "
              + mutations[damage] + "; return $acl }; "
              + f"try {{ & {quote(SCRIPT)} -Diagnostics -Python {quote(stub)} -StorageParent {quote(tmp_path)}; exit 0 }} catch {{ exit 9 }}")
    result = run_shell(source)
    assert result.returncode == 9 and not marker.exists()
    assert setup_stages(result.stderr) == ["PROCESS_STARTED", "VALIDATE_PATH", "CREATE_DIRECTORY", "APPLY_ACL", "VERIFY_ACL"]
    assert len(list(tmp_path.glob("CCS-Acceptance-*"))) == 1  # Preserve for inspection.
    assert str(tmp_path) not in result.stderr  # Stage trace has no paths or arguments.


def test_default_trace_is_off_and_nonzero_runtime_is_not_retried(tmp_path):
    stub = tmp_path / "python-stub.ps1"
    calls = tmp_path / "calls.txt"
    stub.write_text(f"Add-Content -LiteralPath {quote(calls)} -Value called\nexit 7", encoding="utf-8")
    result = run_shell(f"try {{ & {quote(SCRIPT)} -Python {quote(stub)} -StorageParent {quote(tmp_path)}; exit 0 }} catch {{ exit 9 }}")
    assert result.returncode == 9 and calls.read_text().splitlines() == ["called"]
    assert setup_stages(result.stderr) == ["PROCESS_STARTED"]
    assert len(list(tmp_path.glob("CCS-Acceptance-*"))) == 1


def test_diagnostic_writer_failure_does_not_block_verified_launch(tmp_path):
    stub = tmp_path / "python-stub.ps1"
    marker = tmp_path / "runtime-started"
    stub.write_text(f"Set-Content -LiteralPath {quote(marker)} -Value started\nexit 0", encoding="utf-8")
    source = ("$closed = [System.IO.StreamWriter]::new([System.IO.MemoryStream]::new()); "
              "$closed.Dispose(); [Console]::SetError($closed); "
              + f"& {quote(SCRIPT)} -Diagnostics -Python {quote(stub)} -StorageParent {quote(tmp_path)}; exit $LASTEXITCODE")
    result = run_shell(source)
    assert result.returncode == 0 and marker.exists()
