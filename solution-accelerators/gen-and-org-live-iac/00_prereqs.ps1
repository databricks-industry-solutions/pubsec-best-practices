<#
.SYNOPSIS
  Ensure terraform + the databricks provider binary are present (Windows/PowerShell).
.DESCRIPTION
  The experimental resource exporter is a SUBCOMMAND of the provider binary, not a
  separate download. The reliable way to get the right binary for your OS/arch is to
  `terraform init` a tiny bootstrap config that requires the provider, then read the
  binary out of the plugin cache. Writes the binary path to
  .provider-bootstrap/.provider-bin-path so 01_export.ps1 can invoke it.

  EXPORTER binary version: 1.58.0 is reliable here; the 1.130 exporter stalls during
  compute library-listing on large workspaces. This is ONLY the exporter binary — the
  generated code and speculative plans pin a newer provider (see plane_transform.py
  PROVIDER_VERSION and 04_plan.ps1). Exporter and plan/apply versions are independent.
#>
param(
  [string]$ProviderVersion = $(if ($env:PROVIDER_VERSION) { $env:PROVIDER_VERSION } else { '1.58.0' })
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) {
  Write-Error "terraform not found. Install it (winget install HashiCorp.Terraform, or scoop/choco)."
  exit 1
}
Write-Host ("terraform: " + ((terraform version) | Select-Object -First 1))

$boot = ".provider-bootstrap"
New-Item -ItemType Directory -Force -Path $boot | Out-Null
@"
terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "$ProviderVersion"
    }
  }
}
"@ | Set-Content -Path (Join-Path $boot "versions.tf") -Encoding ascii

Write-Host "Fetching databricks provider $ProviderVersion via terraform init ..."
& terraform "-chdir=$boot" init -input=false -upgrade | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Error "terraform init failed in $boot"; exit 1 }

# Filter by the pinned version — the plugin cache can hold several versions and
# `Select -First 1` would otherwise pick by sort order (e.g. "1.130.0" sorts before
# "1.58.0"), silently choosing the exporter build this script pins AWAY from.
$bin = Get-ChildItem -Path (Join-Path $boot ".terraform/providers") -Recurse -File `
        -Filter "terraform-provider-databricks*" -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -match ("[\\/]" + [regex]::Escape($ProviderVersion) + "[\\/]") } |
        Select-Object -First 1
if (-not $bin) { Write-Error "could not locate provider binary $ProviderVersion under $boot/.terraform"; exit 1 }

$binPath = $bin.FullName
Write-Host "provider binary: $binPath"
Set-Content -Path (Join-Path $boot ".provider-bin-path") -Value $binPath -Encoding ascii
$env:TF_PROVIDER_BIN = $binPath   # convenience for the current session
