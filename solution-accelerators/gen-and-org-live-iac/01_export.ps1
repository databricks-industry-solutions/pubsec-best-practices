<#
.SYNOPSIS
  Run the Databricks experimental exporter against ONE live scope (Windows/PowerShell).
.DESCRIPTION
  The single-chunk primitive. Writes a flat ground-truth dump into a chunk directory
  under captured/. For a multi-workspace fleet, drive it from 02_export_fleet.ps1.
  plane_transform.py later reads the PARENT captured/ dir, so every chunk recombines.
.EXAMPLE
  .\01_export.ps1 -ProfileName acct-sp -Scope account   -OutDir captured/account
.EXAMPLE
  .\01_export.ps1 -ProfileName ws-dev  -Scope workspace -OutDir captured/ws-dev
.EXAMPLE
  .\01_export.ps1 -ProfileName ws-dev  -Services clusters,jobs -OutDir captured/ws-dev
#>
param(
  [string]$OutDir = "captured/exported",
  # -ProfileName, not -Profile: $Profile is a PowerShell automatic variable.
  [string]$ProfileName,
  [ValidateSet('account','metastore','workspace','')]
  [string]$Scope = '',
  # [string[]] not [string]: `-Services a,b` on the PS CLI binds as an ARRAY; a plain
  # [string] would coerce it to the space-joined 'a b' and feed the exporter a bad list.
  [string[]]$Services,
  [string[]]$Listing,
  [string]$Match,
  [string]$MatchRegex,
  [string]$ExcludeRegex,
  [string]$UpdatedSince,   # ISO8601 → turns on -incremental -updated-since
  [string]$LastActiveDays
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

# Normalize array-bound args back to the exporter's comma form. A splatted string
# ("a,b") binds as a 1-element array and rejoins to "a,b" unchanged.
$servicesArg = if ($Services) { $Services -join ',' } else { '' }
$listingArg  = if ($Listing)  { $Listing  -join ',' } else { '' }

# ── auth ──────────────────────────────────────────────────────────────────────
# .env supplies DATABRICKS_CONFIG_PROFILE (or host + account-admin SP creds) and any
# EXPORTER_* parallelism vars. -ProfileName overrides the profile per chunk.
$envFile = ".env"
if (Test-Path $envFile) {
  foreach ($line in Get-Content $envFile) {
    if ($line -match '^\s*#') { continue }
    if ($line -match '^\s*([^=\s]+)\s*=\s*(.*)$') {
      $k = $Matches[1]; $v = $Matches[2].Trim()
      if ($v -match '^"(.*)"$') { $v = $Matches[1] } elseif ($v -match "^'(.*)'$") { $v = $Matches[1] }
      Set-Item -Path ("Env:" + $k) -Value $v
    }
  }
}
if ($ProfileName) { $env:DATABRICKS_CONFIG_PROFILE = $ProfileName }
if ($env:DATABRICKS_CONFIG_PROFILE) {
  Write-Host "auth: CLI profile '$($env:DATABRICKS_CONFIG_PROFILE)'"
} else {
  foreach ($req in 'DATABRICKS_HOST','DATABRICKS_ACCOUNT_ID','DATABRICKS_CLIENT_ID','DATABRICKS_CLIENT_SECRET') {
    # Non-empty, not just present: env.example ships these blank.
    if ([string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($req))) {
      Write-Error "set -ProfileName, or $req in .env (account-admin SP for account scope)"; exit 2
    }
  }
  Write-Host "auth: SP client_id on $($env:DATABRICKS_HOST)"
}

# ── provider binary ─────────────────────────────────────────────────────────
$providerBin = $env:TF_PROVIDER_BIN
$binPathFile = ".provider-bootstrap/.provider-bin-path"
if (-not $providerBin -and (Test-Path $binPathFile)) {
  $providerBin = (Get-Content $binPathFile | Select-Object -First 1)
}
if (-not $providerBin) { Write-Error "run .\00_prereqs.ps1 first (sets the provider binary path)"; exit 1 }

# ── what to enumerate ─────────────────────────────────────────────────────────
# 1. explicit -Listing/-Services win (Listing follows Services if only Services set)
# 2. else -Scope preset  3. else legacy account+UC default (back-compat)
# The presets export `groups` but NOT `users`: individual users come from the IdP via
# SCIM and are dropped by the transform anyway (skip_resource_types in plane_rules.yaml),
# so pulling them is wasted work and the per-member user expansion is slow on large
# accounts. Groups + service principals + their memberships are kept; user->group
# memberships are cascade-dropped downstream. (Pass -Services ...,users if you need them.)
switch ($Scope) {
  'account'   { $presetListing = 'mws,groups'
                $presetServices = 'mws,groups,uc-metastores,uc-storage-credentials,uc-external-locations' }
  'metastore' { $presetListing = 'uc-catalogs,uc-schemas,uc-grants,uc-storage-credentials,uc-external-locations'
                $presetServices = 'uc-catalogs,uc-schemas,uc-grants,uc-storage-credentials,uc-external-locations,uc-connections' }
  'workspace' { $presetListing = 'compute,sql-endpoints,pools,policies'
                $presetServices = 'compute,sql-endpoints,pools,policies' }
  default     { $presetListing = 'mws,groups,uc-catalogs,uc-schemas,uc-grants,uc-external-locations,uc-storage-credentials'
                $presetServices = 'mws,groups,uc-catalogs,uc-schemas,uc-grants,uc-metastores,uc-external-locations,uc-storage-credentials,uc-connections' }
}
$svc = if ($servicesArg) { $servicesArg } else { $presetServices }
if     ($listingArg)  { $lst = $listingArg }
elseif ($servicesArg) { $lst = $servicesArg }   # narrowed services but not listing → list what we import
else                  { $lst = $presetListing }

# ── granular filters (only appended when set) ─────────────────────────────────
$extra = @()
if ($Match)          { $extra += @('-match', $Match) }
if ($MatchRegex)     { $extra += @('-matchRegex', $MatchRegex) }
if ($ExcludeRegex)   { $extra += @('-excludeRegex', $ExcludeRegex) }
if ($LastActiveDays) { $extra += @('-last-active-days', $LastActiveDays) }
if ($UpdatedSince)   { $extra += @('-incremental', '-updated-since', $UpdatedSince) }

if (Test-Path $OutDir) { Remove-Item -Recurse -Force $OutDir }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
Write-Host "Exporting scope='$(if ($Scope) { $Scope } else { 'default' })'  ->  $OutDir"
Write-Host "  listing:  $lst"
Write-Host "  services: $svc"
if ($extra.Count -gt 0) { Write-Host ("  filters:  " + ($extra -join ' ')) }

$allArgs = @('exporter','-skip-interactive','-directory',$OutDir,'-native-import','-listing',$lst,'-services',$svc)
$allArgs += $extra
# Continue (not Stop) around the native call so a non-zero exit doesn't throw before we
# read the code (PSNativeCommandUseErrorActionPreference is on by default in pwsh 7.4+).
# $LASTEXITCODE is the source of truth.
$prevEAP = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
& $providerBin @allArgs
$rc = $LASTEXITCODE
$ErrorActionPreference = $prevEAP
if ($rc -ne 0) { Write-Error "exporter failed (exit $rc)"; exit $rc }

Write-Host "`nExported files:"
Get-ChildItem -Path $OutDir -Recurse -File -Filter *.tf | Sort-Object FullName | ForEach-Object { $_.FullName }
exit 0
