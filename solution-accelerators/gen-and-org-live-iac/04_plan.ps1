<#
.SYNOPSIS
  Speculative-plan one plane root against a LIVE workspace/account (Windows/PowerShell).
.DESCRIPTION
  Copies the generated root into .plan/<root>/, swaps in a profile-based provider (the
  scaffolded providers.tf uses TFE-style vars), drops the remote_state data.tf, then
  init + plan. A correct adoption result is "N to import ... 0 to destroy".
  SPECULATIVE ONLY — nothing is applied.
.EXAMPLE
  .\04_plan.ps1 -Root uc-governance-default -ProfileName my-workspace
.EXAMPLE
  .\04_plan.ps1 -Root identity -ProfileName my-account-sp
#>
param(
  [Parameter(Mandatory=$true)][string]$Root,
  [string]$ProfileName = "my-workspace",
  [string]$ProviderVersion = $(if ($env:PROVIDER_VERSION) { $env:PROVIDER_VERSION } else { '1.130.0' })
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$src = "generated/databricks-terraform/environments/$Root"
if (-not (Test-Path $src)) { Write-Error "no such root: $src — run .\run.ps1 -Offline -Collapse first"; exit 1 }
if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) { Write-Error "terraform not on PATH"; exit 1 }

$work = ".plan/$Root"
if (Test-Path $work) { Remove-Item -Recurse -Force $work }
New-Item -ItemType Directory -Force -Path $work | Out-Null
Get-ChildItem -Path $src -Filter *.tf -File | Copy-Item -Destination $work
Remove-Item -Force (Join-Path $work "data.tf") -ErrorAction SilentlyContinue  # remote_state stubs → TFE ws that don't exist in a demo

$providersTf = Join-Path $work "providers.tf"
'provider "databricks" {}' | Set-Content -Path $providersTf -Encoding ascii

# Account-level roots reference var.databricks_account_id; the bare provider swap dropped
# the scaffolded declaration, so re-declare it and feed the value from the profile.
$needsAcct = @(Select-String -Path (Join-Path $work "*.tf") -Pattern 'var\.databricks_account_id' -ErrorAction SilentlyContinue).Count -gt 0
if ($needsAcct) {
  Add-Content -Path $providersTf -Value "`nvariable `"databricks_account_id`" { type = string }"
  $cfg = Join-Path $HOME ".databrickscfg"
  if (Test-Path $cfg) {
    $inSection = $false
    foreach ($l in Get-Content $cfg) {
      if ($l -match '^\s*\[(.+)\]\s*$') { $inSection = ($Matches[1] -eq $ProfileName); continue }
      if ($inSection -and $l -match '^\s*account_id\s*=\s*(.+?)\s*$') { $env:TF_VAR_databricks_account_id = $Matches[1]; break }
    }
  }
}

@"
terraform {
  required_version = ">= 1.5"
  required_providers {
    databricks = { source = "databricks/databricks", version = "$ProviderVersion" }
  }
}
"@ | Set-Content -Path (Join-Path $work "versions.tf") -Encoding ascii

$env:DATABRICKS_CONFIG_PROFILE = $ProfileName
# Continue around terraform so a non-zero exit doesn't throw before we capture it and
# print the gate verdict (pwsh 7.4+ throws on native non-zero under Stop). Exit codes rule.
$prevEAP = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
try {
  Write-Host "== init ($Root) =="
  & terraform "-chdir=$work" init -input=false | Out-Null
  if ($LASTEXITCODE -ne 0) { Write-Error "terraform init failed in $work"; exit 1 }

  Write-Host "== plan ($Root via profile $ProfileName) =="
  $planOut = Join-Path $work "plan.out"
  # Capture BOTH streams; the plan's exit code is the source of truth (not a grep).
  & terraform "-chdir=$work" plan -input=false -no-color 2>&1 | Out-File -FilePath $planOut -Encoding ascii
  $rc = $LASTEXITCODE
} finally {
  $ErrorActionPreference = $prevEAP
}

$planText = Get-Content $planOut
$planText | Select-String -Pattern 'will be (imported|created|destroyed|replaced)|must be replaced|^Plan:|No changes|^Error:|encountered a problem' |
  Select-Object -Last 60 | ForEach-Object { $_.Line }

Write-Host "`n──────── summary ────────"
$planText | Select-String -Pattern '^Plan:|No changes' | ForEach-Object { $_.Line }
# Destroy count from the authoritative "Plan:" line so REPLACEMENTS ("must be replaced") count.
$destroy = 0
$dm = $planText | Select-String -Pattern '(\d+) to destroy' | Select-Object -Last 1
if ($dm -and $dm.Line -match '(\d+) to destroy') { $destroy = [int]$Matches[1] }
$errs = @($planText | Select-String -Pattern '^Error:').Count

if ($rc -ne 0 -or $errs -gt 0) {
  Write-Host "plan: ERROR (exit $rc, $errs error block(s)) — see $planOut"
} elseif ($destroy -eq 0) {
  Write-Host "zero-destroy gate: PASS (0 to destroy)"
} else {
  Write-Host "zero-destroy gate: REVIEW — $destroy to destroy/replace; triage before apply"
}
Write-Host "full plan: $planOut"
