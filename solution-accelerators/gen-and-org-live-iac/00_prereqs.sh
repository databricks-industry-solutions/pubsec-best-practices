#!/usr/bin/env bash
# 00_prereqs.sh — ensure terraform + the databricks provider binary are present.
#
# The experimental resource exporter is a SUBCOMMAND of the provider binary, not
# a separate download. The reliable way to get the right binary for your OS/arch
# is to `terraform init` a tiny bootstrap config that requires the provider, then
# read the binary out of the plugin cache. This script prints TF_PROVIDER_BIN so
# 01_export.sh (and you) can invoke `"$TF_PROVIDER_BIN" exporter ...`.
set -euo pipefail
cd "$(dirname "$0")"

# EXPORTER binary version. 1.58.0 is reliable here; the 1.130 exporter stalls
# during compute library-listing on large workspaces. This is ONLY the exporter
# binary — the generated code and speculative plans pin a newer provider that
# fixes the SQL-warehouse read bug (see plane_transform.py PROVIDER_VERSION and
# 04_plan.sh). Exporter version and plan/apply provider version are independent.
PROVIDER_VERSION="${PROVIDER_VERSION:-1.58.0}"

command -v terraform >/dev/null 2>&1 || {
  echo "terraform not found. Install it (macOS: brew install hashicorp/tap/terraform)." >&2
  exit 1
}
echo "terraform: $(terraform version | head -1)"

BOOT=".provider-bootstrap"
mkdir -p "$BOOT"
cat > "$BOOT/versions.tf" <<EOF
terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "${PROVIDER_VERSION}"
    }
  }
}
EOF

echo "Fetching databricks provider ${PROVIDER_VERSION} via terraform init ..."
terraform -chdir="$BOOT" init -input=false -upgrade >/dev/null

# Filter by the pinned version — the plugin cache can hold several versions, and a bare
# `head -1` would pick by traversal order ("1.130.0" sorts before "1.58.0"), silently
# choosing the exporter build this script pins AWAY from.
BIN="$(find "$BOOT/.terraform/providers" -type f -path "*/${PROVIDER_VERSION}/*" -name 'terraform-provider-databricks*' | head -1)"
[ -n "$BIN" ] || { echo "could not locate provider binary ${PROVIDER_VERSION} under $BOOT/.terraform" >&2; exit 1; }
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"   # absolutize

echo "provider binary: $BIN"
echo "$BIN" > "$BOOT/.provider-bin-path"
echo
echo "export TF_PROVIDER_BIN=\"$BIN\""
