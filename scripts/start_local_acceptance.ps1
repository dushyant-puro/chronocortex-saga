param(
    [ValidateRange(1024, 65535)][int]$Port = 8877,
    [string]$Python = (Join-Path (Split-Path -Parent $PSScriptRoot) '.venv\Scripts\python.exe'),
    [string]$StorageParent = $env:LOCALAPPDATA
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw 'Set up the locked .venv first, or pass -Python with its qualified python.exe path.'
}
if (-not (Test-Path -LiteralPath $StorageParent -PathType Container)) {
    throw 'An existing local storage parent is required.'
}
$acceptancePython = (Resolve-Path -LiteralPath $Python).ProviderPath
$acceptanceRoot = Join-Path $StorageParent ('CCS-Acceptance-' + [guid]::NewGuid().ToString('N'))
$acceptanceRoot = (New-Item -ItemType Directory -Path $acceptanceRoot).FullName
$acceptanceOwner = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acceptanceSystem = [System.Security.Principal.SecurityIdentifier]::new('S-1-5-18')
$acceptanceAcl = [System.Security.AccessControl.DirectorySecurity]::new()
$acceptanceAcl.SetOwner($acceptanceOwner)
$acceptanceAcl.SetAccessRuleProtection($true, $false)
foreach ($acceptanceIdentity in @($acceptanceOwner, $acceptanceSystem)) {
    $acceptanceRule = [System.Security.AccessControl.FileSystemAccessRule]::new(
        $acceptanceIdentity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
    $acceptanceAcl.AddAccessRule($acceptanceRule)
}
Set-Acl -LiteralPath $acceptanceRoot -AclObject $acceptanceAcl
Write-Host 'SIMULATED LOCAL ACCEPTANCE: no real payments or live voice.'
Write-Host ('Private run directory: ' + $acceptanceRoot)
Write-Host ('Console: http://127.0.0.1:' + $Port + '  |  Executor: SIMULATED-local-worker')
Write-Host 'Use operator.secret in that directory to connect. Ctrl+C stops the worker; evidence is retained.'
Push-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
try {
    & $acceptancePython -m acceptance.local --directory $acceptanceRoot --port $Port
    $acceptanceExit = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($acceptanceExit -ne 0) {
    throw 'Acceptance exited unsuccessfully. Preserve this directory; do not rerun or delete its evidence.'
}
