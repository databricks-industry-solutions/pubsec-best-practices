<#
.SYNOPSIS
  Wipe demo-generated output so you can re-run from a clean slate (Windows/PowerShell).
.DESCRIPTION
  Default (safe): removes the generated plane tree, plan scratch, logs, pycache. KEEPS
  the captured/ export snapshots and the fetched provider binary, so an offline re-run
  (.\run.ps1 -Offline -Collapse) still works with no creds.
.EXAMPLE
  .\reset.ps1                 # generated/, .plan/, logs/, *.log, __pycache__
.EXAMPLE
  .\reset.ps1 -Provider       # ...also .provider-bootstrap/ (re-fetched by 00_prereqs.ps1)
.EXAMPLE
  .\reset.ps1 -Captured       # ...also captured/ snapshots (forces a fresh LIVE export)
.EXAMPLE
  .\reset.ps1 -All            # everything above
#>
param(
  [switch]$Provider,
  [switch]$Captured,
  [switch]$All
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
if ($All) { $Provider = $true; $Captured = $true }

function Zap($p) { if (Test-Path $p) { Write-Host "  removing $p"; Remove-Item -Recurse -Force $p } }

Write-Host "Resetting demo output in $(Get-Location)"
Zap "generated"
Zap ".plan"
Zap "logs"
Zap "__pycache__"
Get-ChildItem -Path . -Filter *.log -File -ErrorAction SilentlyContinue | ForEach-Object { Zap $_.Name }

if ($Provider) { Zap ".provider-bootstrap" }

if ($Captured) {
  Write-Host "  ! removing captured/ snapshots — next run needs a fresh live export"
  Zap "captured"
  New-Item -ItemType Directory -Force -Path "captured" | Out-Null
}

Write-Host "Done. Re-run:  .\run.ps1 -Offline -Collapse   (or .\run.ps1 -Collapse for a fresh live export)"
