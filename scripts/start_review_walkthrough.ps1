param(
    [ValidateRange(1024, 65535)][int]$Port = 8877,
    [string]$Python = (Join-Path (Split-Path -Parent $PSScriptRoot) '.venv\Scripts\python.exe'),
    [string]$StorageParent = $env:LOCALAPPDATA,
    [switch]$Diagnostics
)
$ErrorActionPreference = 'Stop'
# Reuse the verified native ACL boundary; no credential or journal is placed
# in the checkout, and an existing run is never resumed or overwritten.
& (Join-Path $PSScriptRoot 'start_local_acceptance.ps1') -Review -Port $Port -Python $Python -StorageParent $StorageParent -Diagnostics:$Diagnostics
