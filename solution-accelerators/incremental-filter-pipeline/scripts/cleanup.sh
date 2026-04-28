#!/bin/bash
# Cleanup script for incremental-filter-pipeline
# WARNING: This will drop all pipeline tables and checkpoints!
# Usage: databricks workspace run scripts/cleanup.sh --catalog main --schema tmp

set -e

CATALOG="${1:-main}"
SCHEMA="${2:-tmp}"
CHECKPOINT_BASE="${3:-/tmp/incremental_filter_pipeline/checkpoints}"

echo "⚠️  This will drop all pipeline tables in ${CATALOG}.${SCHEMA}"
echo "    and remove checkpoints at ${CHECKPOINT_BASE}"
read -p "Continue? (y/N) " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Dropping tables..."
    # Tables would be dropped via notebook 05_cleanup
    echo "Run notebook 05_cleanup with catalog=${CATALOG} schema=${SCHEMA}"
fi
