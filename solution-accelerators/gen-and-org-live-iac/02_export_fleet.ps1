<#
.SYNOPSIS
  Chunked, fan-out export across a whole account + fleet of workspaces (Windows/PowerShell).
.DESCRIPTION
  Driven by a manifest (fleet.conf). Each line is one INDEPENDENT export chunk written
  to its own captured/<name>/ directory, so a stall or rate-limit on one chunk never
  loses the others, completed chunks are skipped on re-run (resumable), chunks can be
  distributed across machines with -Only, and plane_transform.py recombines every chunk
  from the parent captured/ dir.

  Manifest format (whitespace-separated; # comments and blank lines ignored):
      <name>   <profile>   <scope>   [services]
  See fleet.example.conf for a worked example.
.EXAMPLE
  .\02_export_fleet.ps1                         # export every not-yet-done chunk
.EXAMPLE
  .\02_export_fleet.ps1 -Only ws-prod -Force    # (re-)export just one chunk
.EXAMPLE
  .\02_export_fleet.ps1 -DryRun                 # print the plan, export nothing
#>
param(
  [string]$Conf = "fleet.conf",
  [string]$Only,
  [switch]$Force,
  [switch]$DryRun,
  [switch]$FailFast
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

if (-not (Test-Path $Conf)) {
  Write-Error "manifest '$Conf' not found. Copy the template: Copy-Item fleet.example.conf fleet.conf"
  exit 1
}
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

# Ensure the provider binary is available (01_export.ps1 needs it).
if (-not $env:TF_PROVIDER_BIN -and -not (Test-Path ".provider-bootstrap/.provider-bin-path")) {
  Write-Host "== fetching provider binary (00_prereqs.ps1) =="
  & (Join-Path $PSScriptRoot "00_prereqs.ps1")
}

$ok = @(); $skip = @(); $fail = @()

foreach ($line in Get-Content $Conf) {
  if ($line -match '^\s*#') { continue }
  if ($line -match '^\s*$') { continue }
  $cols = ($line.Trim() -split '\s+')
  $name    = $cols[0]
  $profile = if ($cols.Count -ge 2) { $cols[1] } else { '' }
  $scope   = if ($cols.Count -ge 3) { $cols[2] } else { '' }
  $svc     = if ($cols.Count -ge 4 -and $cols[3] -ne '-') { $cols[3] } else { '' }

  if (-not $name) { continue }
  if ($Only -and $name -ne $Only) { continue }

  $out = "captured/$name"

  if ($DryRun) {
    "{0,-16} profile={1,-22} scope={2,-10} services={3} -> {4}" -f `
      $name, $profile, $scope, $(if ($svc) { $svc } else { '<preset>' }), $out | Write-Host
    continue
  }

  # Resumable: skip a chunk that already produced .tf, unless -Force.
  $already = Test-Path $out -PathType Container
  if ($already) {
    $already = @(Get-ChildItem -Path $out -Recurse -File -Filter *.tf -ErrorAction SilentlyContinue).Count -gt 0
  }
  if (-not $Force -and $already) {
    Write-Host "== skip $name (already captured; -Force to re-export) =="
    $skip += $name; continue
  }

  Write-Host "================ export chunk: $name ($scope via $profile) ================"
  $log = "logs/export-$name.log"
  $rc = 0
  # Continue (not Stop) so a failing chunk surfaces as a non-zero exit we record, rather
  # than a terminating error (pwsh 7.4+ throws on native non-zero under Stop). rc is truth.
  $prevEAP = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
  try {
    $callArgs = @{ OutDir = $out; ProfileName = $profile; Scope = $scope }
    if ($svc) { $callArgs['Services'] = $svc }
    & (Join-Path $PSScriptRoot "01_export.ps1") @callArgs 2>&1 | Tee-Object -FilePath $log
    $rc = $LASTEXITCODE
    if ($null -eq $rc) { $rc = 0 }
  } catch {
    ($_ | Out-String) | Tee-Object -FilePath $log -Append | Write-Host
    $rc = 1
  } finally {
    $ErrorActionPreference = $prevEAP
  }

  if ($rc -eq 0) {
    $ok += $name
  } else {
    Write-Warning "chunk '$name' failed (exit $rc) — see $log"
    $fail += $name
    if ($FailFast) { Write-Warning "-FailFast: stopping."; break }
  }
}

if ($DryRun) { exit 0 }

Write-Host "`n──────── fleet export summary ────────"
Write-Host ("  ok:      {0}   {1}" -f $ok.Count,   ($ok   -join ' '))
Write-Host ("  skipped: {0}   {1}" -f $skip.Count, ($skip -join ' '))
Write-Host ("  failed:  {0}   {1}" -f $fail.Count, ($fail -join ' '))
Write-Host ""
if ($fail.Count -gt 0) {
  Write-Host ("Re-run failed chunks individually, e.g.:  .\02_export_fleet.ps1 -Only {0} -Force" -f $fail[0])
  exit 1
}
Write-Host "Next: .\run.ps1 -Offline -Collapse   (recombines every captured/ chunk into the plane tree)"
exit 0
