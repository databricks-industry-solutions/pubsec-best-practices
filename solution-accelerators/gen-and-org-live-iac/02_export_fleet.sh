#!/usr/bin/env bash
# 02_export_fleet.sh — chunked, fan-out export across a whole account + fleet of
# workspaces, driven by a manifest (fleet.conf). Each line is one INDEPENDENT
# export chunk written to its own captured/<name>/ directory, so:
#   • a stall or rate-limit on one chunk never loses the others,
#   • completed chunks are skipped on re-run (resumable),
#   • chunks can be distributed across machines/runners with --only,
#   • plane_transform.py recombines every chunk from the parent captured/ dir.
#
# Manifest format (whitespace-separated; # comments and blank lines ignored):
#     <name>   <profile>   <scope>   [services]
#   name      chunk id → captured/<name>/            (identity plane, ws-dev, uc-primary, …)
#   profile   .databrickscfg profile for this scope  (account SP vs workspace)
#   scope     account | metastore | workspace        (picks the service preset in 01_export.sh)
#   services  OPTIONAL comma list overriding the scope preset ('-' = use preset)
# See fleet.example.conf for a worked example.
#
#   ./02_export_fleet.sh                       # export every not-yet-done chunk in fleet.conf
#   ./02_export_fleet.sh --only ws-prod        # (re-)export just one chunk
#   ./02_export_fleet.sh --force               # re-export chunks even if already captured
#   ./02_export_fleet.sh --dry-run             # print the plan, export nothing
#   ./02_export_fleet.sh --fail-fast           # stop at the first chunk that errors
#   ./02_export_fleet.sh --conf fleet.prod.conf
set -euo pipefail
cd "$(dirname "$0")"

CONF="fleet.conf"
ONLY=""; FORCE=0; DRY=0; FAIL_FAST=0
while [ $# -gt 0 ]; do
  case "$1" in
    --conf)       CONF="$2"; shift 2 ;;
    --only)       ONLY="$2"; shift 2 ;;
    --force)      FORCE=1; shift ;;
    --dry-run)    DRY=1; shift ;;
    --fail-fast)  FAIL_FAST=1; shift ;;
    -h|--help)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $1 (try --help)" >&2; exit 1 ;;
  esac
done

if [ ! -f "$CONF" ]; then
  echo "manifest '$CONF' not found." >&2
  echo "Copy the template and edit it:  cp fleet.example.conf fleet.conf" >&2
  exit 1
fi

mkdir -p logs

# Ensure the provider binary is available (01_export.sh needs TF_PROVIDER_BIN).
if [ -z "${TF_PROVIDER_BIN:-}" ] && [ ! -f .provider-bootstrap/.provider-bin-path ]; then
  echo "== fetching provider binary (00_prereqs.sh) =="
  eval "$(./00_prereqs.sh | grep '^export TF_PROVIDER_BIN=')"
fi

OK=(); SKIP=(); FAIL=()

while read -r name profile scope services _rest; do
  [ -z "${name:-}" ] && continue
  case "$name" in \#*) continue ;; esac
  [ -n "$ONLY" ] && [ "$name" != "$ONLY" ] && continue

  out="captured/$name"
  svc=""; [ -n "${services:-}" ] && [ "$services" != "-" ] && svc="$services"

  if [ "$DRY" -eq 1 ]; then
    printf 'plan: %-16s profile=%-22s scope=%-10s services=%s -> %s\n' \
      "$name" "$profile" "$scope" "${svc:-<preset>}" "$out"
    continue
  fi

  # Resumable: skip a chunk that already produced .tf, unless --force.
  if [ "$FORCE" -eq 0 ] && [ -n "$(find "$out" -name '*.tf' 2>/dev/null | head -1)" ]; then
    echo "== skip $name (already captured; --force to re-export) =="
    SKIP+=("$name"); continue
  fi

  echo "================ export chunk: $name ($scope via $profile) ================"
  log="logs/export-$name.log"
  set +e
  # Per-chunk subshell: export the chunk's env explicitly (a ${svc:+VAR=..} inline
  # prefix is NOT re-parsed as an assignment after expansion) and isolate it from
  # the next chunk.
  (
    export PROFILE="$profile" SCOPE="$scope"
    [ -n "$svc" ] && export SERVICES="$svc"
    ./01_export.sh "$out"
  ) 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  set -e

  if [ "$rc" -eq 0 ]; then
    OK+=("$name")
  else
    echo "!! chunk '$name' failed (exit $rc) — see $log" >&2
    FAIL+=("$name")
    [ "$FAIL_FAST" -eq 1 ] && { echo "--fail-fast: stopping." >&2; break; }
  fi
done < "$CONF"

[ "$DRY" -eq 1 ] && exit 0

echo
echo "──────── fleet export summary ────────"
echo "  ok:      ${#OK[@]}   ${OK[*]:-}"
echo "  skipped: ${#SKIP[@]}   ${SKIP[*]:-}"
echo "  failed:  ${#FAIL[@]}   ${FAIL[*]:-}"
echo
if [ "${#FAIL[@]}" -gt 0 ]; then
  echo "Re-run failed chunks individually, e.g.:  ./02_export_fleet.sh --only ${FAIL[0]} --force"
  exit 1
fi
echo "Next: ./run.sh --offline --collapse   (recombines every captured/ chunk into the plane tree)"
