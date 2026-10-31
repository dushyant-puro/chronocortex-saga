"""Native ACL protection before launching the SIMULATED walkthrough."""
import json
import os
from pathlib import Path

import pytest

from tests.windows_shell_support import run_shell

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Native Windows acceptance launcher ACLs")
SCRIPT = Path(__file__).resolve().parents[1] / "scripts/start_local_acceptance.ps1"


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def test_launch_directory_is_unique_protected_and_outside_checkout_before_python(tmp_path):
    stub = tmp_path / "python-stub.ps1"
    stub.write_text("""
$i = [Array]::IndexOf($args, '--directory')
if ($i -lt 0) { exit 3 }
$folder = $args[$i + 1]
$acl = Get-Acl -LiteralPath $folder
$rules = $acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier])
@{ protected = $acl.AreAccessRulesProtected;
   inherited = @($rules | Where-Object { $_.IsInherited }).Count;
   sids = @($rules | ForEach-Object { $_.IdentityReference.Value });
   owner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $folder 'proof.json') -Encoding UTF8
exit 0
""", encoding="ascii")
    for _ in range(2):
        result = run_shell(f"& {quote(SCRIPT)} -Python {quote(stub)} -StorageParent {quote(tmp_path)}; exit $LASTEXITCODE")
        assert result.returncode == 0, result.stderr
    folders = list(tmp_path.glob("CCS-Acceptance-*"))
    assert len(folders) == 2
    for folder in folders:
        proof = json.loads((folder / "proof.json").read_text(encoding="utf-8-sig"))
        assert proof["protected"] and proof["inherited"] == 0
        assert set(proof["sids"]) == {proof["owner"], "S-1-5-18"}


def test_invalid_port_never_creates_an_acceptance_directory(tmp_path):
    result = run_shell(f"& {quote(SCRIPT)} -Port 80 -StorageParent {quote(tmp_path)}; if (-not $?) {{ exit 2 }}")
    assert result.returncode != 0 and not list(tmp_path.iterdir())
