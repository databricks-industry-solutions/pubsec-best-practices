<#
.SYNOPSIS
  End-to-end: prereqs -> live export -> plane transform -> fmt (Windows/PowerShell).
.DESCRIPTION
  The transform reads the PARENT captured/ dir, so every export chunk (an account pass,
  a metastore pass, N per-workspace passes) is merged into one plane tree.
.EXAMPLE
  .\run.ps1                    # full pipeline, single-pass export
.EXAMPLE
  .\run.ps1 -Fleet             # chunked fan-out export via fleet.conf
.EXAMPLE
  .\run.ps1 -Offline -Collapse # no creds: rebuild from committed captured/ snapshots
#>
param(
  [switch]$Offline,
  [switch]$Collapse,
  [switch]$Fleet
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$capturedDir = "captured"
$outDir = "generated"

# Pick a python that has PyYAML. Candidates as (exe, prefix-args) pairs; the unary
# comma keeps each pair a single list element under +=.
function Find-Python {
  $cands = @()
  $cands += ,@('python',  @())
  $cands += ,@('python3', @())
  $cands += ,@('py',      @('-3'))
  foreach ($cand in $cands) {
    $exe = $cand[0]; $pre = $cand[1]
    if (Get-Command $exe -ErrorAction SilentlyContinue) {
      & $exe @pre -c "import yaml" 2>$null
      if ($LASTEXITCODE -eq 0) { return @{ Exe = $exe; Pre = $pre } }
    }
  }
  return $null
}
$py = Find-Python
if (-not $py) { Write-Error "no python with PyYAML found — pip install pyyaml"; exit 1 }

if (-not $Offline) {
  Write-Host "== 0/3 prereqs =="
  & (Join-Path $PSScriptRoot "00_prereqs.ps1")
  if ($Fleet) {
    Write-Host "== 1/3 chunked fleet export =="
    & (Join-Path $PSScriptRoot "02_export_fleet.ps1")
  } else {
    Write-Host "== 1/3 live export (single pass) =="
    & (Join-Path $PSScriptRoot "01_export.ps1") -OutDir "captured/exported"
  }
} else {
  Write-Host "== offline: using committed snapshots under $capturedDir/ =="
  if (-not (@(Get-ChildItem -Path $capturedDir -Recurse -File -Filter *.tf -ErrorAction SilentlyContinue).Count -gt 0)) {
    Write-Error "no snapshot under $capturedDir — run once live first"; exit 1
  }
}

Write-Host "== 2/3 transform to planes =="
& $py.Exe @($py.Pre) plane_transform.py --exported-dir $capturedDir --out-dir $outDir
if ($LASTEXITCODE -ne 0) { Write-Error "plane_transform.py failed"; exit 1 }

if ($Collapse) {
  Write-Host "== 2b: collapse repeated resources into for_each maps =="
  & $py.Exe @($py.Pre) collapse_foreach.py --tree "$outDir/databricks-terraform"
  if ($LASTEXITCODE -ne 0) { Write-Error "collapse_foreach.py failed"; exit 1 }
}

Write-Host "== 3/3 terraform fmt =="
if (Get-Command terraform -ErrorAction SilentlyContinue) {
  & terraform fmt -recursive "$outDir/databricks-terraform" | Out-Null
  Write-Host "formatted."
} else {
  Write-Host "terraform not on PATH; skipped fmt."
}

Write-Host "`nDone. Plane tree: $outDir/databricks-terraform/"
Write-Host "Next: .\04_plan.ps1 -Root <plane-root> -ProfileName <profile>  (speculative, zero-destroy)"
