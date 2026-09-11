#!/usr/bin/env bash
# 01_export.sh — run the Databricks experimental exporter against ONE live scope
# (an account, or a single workspace) and write a flat ground-truth dump into a
# chunk directory under captured/.
#
# This is the single-chunk primitive. For a multi-workspace fleet, drive it from
# 02_export_fleet.sh (manifest-driven fan-out) — each chunk calls this script with
# a different PROFILE / SCOPE / OUT_DIR. plane_transform.py later reads the PARENT
# captured/ dir, so every chunk recombines into one plane tree.
#
#   ./01_export.sh                                 # legacy default: account + UC
#   PROFILE=acct-sp   SCOPE=account   ./01_export.sh captured/account
#   PROFILE=ws-dev    SCOPE=workspace ./01_export.sh captured/ws-dev
#   PROFILE=ws-dev    SCOPE=metastore ./01_export.sh captured/uc-primary
#   SERVICES=compute,sql-endpoints    ./01_export.sh captured/ws-dev   # explicit override
set -euo pipefail
cd "$(dirname "$0")"

OUT_DIR="${1:-captured/exported}"

# ── auth ──────────────────────────────────────────────────────────────────────
# Two supported modes (the exporter honors both):
#   A) DATABRICKS_CONFIG_PROFILE=<profile>  — reuse a CLI profile (user OAuth or SP)
#   B) DATABRICKS_HOST + DATABRICKS_ACCOUNT_ID + CLIENT_ID/CLIENT_SECRET (account-admin SP)
# Either can come from .env or the surrounding shell environment.
# `set -a` also EXPORTS any EXPORTER_* parallelism vars from .env to the exporter.
if [ -f .env ]; then set -a; . ./.env; set +a; fi
# PROFILE=<name> overrides .env (e.g. account pass vs per-workspace pass use different profiles).
if [ -n "${PROFILE:-}" ]; then export DATABRICKS_CONFIG_PROFILE="$PROFILE"; fi
if [ -n "${DATABRICKS_CONFIG_PROFILE:-}" ]; then
  echo "auth: CLI profile '$DATABRICKS_CONFIG_PROFILE'"
else
  : "${DATABRICKS_HOST:?set DATABRICKS_CONFIG_PROFILE, or DATABRICKS_HOST in .env}"
  : "${DATABRICKS_ACCOUNT_ID:?set DATABRICKS_ACCOUNT_ID in .env}"
  : "${DATABRICKS_CLIENT_ID:?set DATABRICKS_CLIENT_ID (account-admin SP) in .env}"
  : "${DATABRICKS_CLIENT_SECRET:?set DATABRICKS_CLIENT_SECRET in .env}"
  echo "auth: SP client_id on $DATABRICKS_HOST"
fi

# ── provider binary ─────────────────────────────────────────────────────────
if [ -z "${TF_PROVIDER_BIN:-}" ] && [ -f .provider-bootstrap/.provider-bin-path ]; then
  TF_PROVIDER_BIN="$(cat .provider-bootstrap/.provider-bin-path)"
fi
: "${TF_PROVIDER_BIN:?run ./00_prereqs.sh first (sets the provider binary path)}"

# ── what to enumerate ─────────────────────────────────────────────────────────
# Resolution order:
#   1. Explicit LISTING/SERVICES env win (LISTING falls back to SERVICES if unset).
#   2. Else SCOPE=<account|workspace|metastore> picks a sensible preset.
#   3. Else the legacy account+UC default (back-compat with earlier snapshots).
# Chunk by SCOPE so a large fleet is exported as independent, resumable passes:
#   • account  — MWS + identity + the account-level metastore object, once per account
#                (groups are account-level under identity federation, so they do NOT
#                multiply per workspace).
#   • metastore— UC catalogs/schemas/grants + storage creds/ext locations/connections,
#                once per metastore (run against any workspace attached to that metastore).
#   • workspace— compute/SQL/pools/policies, once per workspace (the ×N multiplier).
#                Deliberately excludes uc-* (captured once at metastore scope) and
#                uc-tables (usually far too many for a governance refactor).
#
# LISTING vs SERVICES — get this right or resources silently vanish. The exporter
# LISTS (enumerates) only the services in -listing; -services merely FILTERS which
# transitive dependencies of those are also imported. So a service in SERVICES but
# NOT in LISTING is exported ONLY if something listed happens to reference it.
# Invariant: every service you want captured for a scope must be in that scope's
# LISTING (each preset below keeps LISTING ⊇ the roots it means to export).
#
# The presets deliberately export `groups` but NOT `users`: individual users are
# provisioned by the IdP via SCIM and are dropped by the transform anyway
# (skip_resource_types in plane_rules.yaml), so pulling them is wasted work and the
# per-member user expansion is slow on large accounts. Groups + service principals +
# their memberships are kept; user->group memberships are cascade-dropped downstream.
# (Set SERVICES=...,users explicitly if you ever do need them.)
#
# Capture what the caller set explicitly BEFORE applying a preset, so we can tell
# "user narrowed SERVICES" from "SERVICES came from the preset".
USER_LISTING="${LISTING:-}"
USER_SERVICES="${SERVICES:-}"
case "${SCOPE:-}" in
  account)   PRESET_LISTING="mws,groups,uc-metastores"
             PRESET_SERVICES="mws,groups,uc-metastores" ;;
  metastore) PRESET_LISTING="uc-catalogs,uc-schemas,uc-grants,uc-storage-credentials,uc-external-locations,uc-connections"
             PRESET_SERVICES="uc-catalogs,uc-schemas,uc-grants,uc-storage-credentials,uc-external-locations,uc-connections" ;;
  workspace) PRESET_LISTING="compute,sql-endpoints,pools,policies"
             PRESET_SERVICES="compute,sql-endpoints,pools,policies" ;;
  "")        PRESET_LISTING="mws,groups,uc-metastores,uc-catalogs,uc-schemas,uc-grants,uc-external-locations,uc-storage-credentials,uc-connections"
             PRESET_SERVICES="mws,groups,uc-metastores,uc-catalogs,uc-schemas,uc-grants,uc-external-locations,uc-storage-credentials,uc-connections" ;;
  *)         echo "unknown SCOPE='$SCOPE' (use account|metastore|workspace, or set LISTING/SERVICES)" >&2; exit 2 ;;
esac
SERVICES="${USER_SERVICES:-$PRESET_SERVICES}"
if   [ -n "$USER_LISTING" ];  then LISTING="$USER_LISTING"
elif [ -n "$USER_SERVICES" ]; then LISTING="$USER_SERVICES"   # narrowed services but not listing → list what we import
else LISTING="$PRESET_LISTING"; fi

# ── granular filters (all optional; only appended when set) ───────────────────
# MATCH / MATCH_REGEX / EXCLUDE_REGEX chunk WITHIN a service by resource name.
# UPDATED_SINCE (ISO8601) turns on incremental export for re-sync passes.
# LAST_ACTIVE_DAYS trims stale/abandoned objects. EXPORTER_* parallelism vars are
# read straight from the environment (see env.example) — no flag needed.
EXTRA=()
[ -n "${MATCH:-}" ]           && EXTRA+=(-match "$MATCH")
[ -n "${MATCH_REGEX:-}" ]     && EXTRA+=(-matchRegex "$MATCH_REGEX")
[ -n "${EXCLUDE_REGEX:-}" ]   && EXTRA+=(-excludeRegex "$EXCLUDE_REGEX")
[ -n "${LAST_ACTIVE_DAYS:-}" ] && EXTRA+=(-last-active-days "$LAST_ACTIVE_DAYS")
if [ -n "${UPDATED_SINCE:-}" ]; then EXTRA+=(-incremental -updated-since "$UPDATED_SINCE"); fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
echo "Exporting scope='${SCOPE:-default}'  ${DATABRICKS_ACCOUNT_ID:+account ${DATABRICKS_ACCOUNT_ID} }->  $OUT_DIR"
echo "  listing:  $LISTING"
echo "  services: $SERVICES"
[ ${#EXTRA[@]} -gt 0 ] && echo "  filters:  ${EXTRA[*]}"

# NOTE: ${EXTRA[@]+...} idiom — a bare "${EXTRA[@]}" on an empty array trips
# `set -u` on macOS's default bash 3.2.
"$TF_PROVIDER_BIN" exporter \
  -skip-interactive \
  -directory "$OUT_DIR" \
  -native-import \
  -listing "$LISTING" \
  -services "$SERVICES" \
  ${EXTRA[@]+"${EXTRA[@]}"}

echo
echo "Exported files:"
find "$OUT_DIR" -name '*.tf' | sort
