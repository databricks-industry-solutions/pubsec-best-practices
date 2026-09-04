#!/usr/bin/env bash
# reset.sh — wipe demo-generated output so you can re-run from a clean slate.
#
# Default (safe): removes the generated plane tree, plan scratch, logs, pycache.
# KEEPS the captured/ export snapshots and the fetched provider binary, so an
# offline re-run (./run.sh --offline --collapse) still works with no creds.
#
#   ./reset.sh              # generated/, .plan/, *.log, __pycache__
#   ./reset.sh --provider   # …also .provider-bootstrap/ (re-fetched by 00_prereqs.sh)
#   ./reset.sh --captured   # …also captured/ snapshots (forces a fresh LIVE export)
#   ./reset.sh --all        # everything above
set -euo pipefail
cd "$(dirname "$0")"

PROV=0; CAP=0
for a in "$@"; do
  case "$a" in
    --provider) PROV=1 ;;
    --captured) CAP=1 ;;
    --all)      PROV=1; CAP=1 ;;
    -h|--help)  grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown flag: $a (try --help)" >&2; exit 1 ;;
  esac
done

zap() { if [ -e "$1" ]; then echo "  removing $1"; rm -rf "$1"; fi; }

echo "Resetting demo output in $(pwd)"
zap generated
zap .plan
zap logs
zap __pycache__
for f in *.log; do zap "$f"; done

[ "$PROV" -eq 1 ] && zap .provider-bootstrap

if [ "$CAP" -eq 1 ]; then
  echo "  ! removing captured/ snapshots — next run needs a fresh live export"
  zap captured
  mkdir -p captured
fi

echo "Done. Re-run:  ./run.sh --offline --collapse   (or ./run.sh --collapse for a fresh live export)"
