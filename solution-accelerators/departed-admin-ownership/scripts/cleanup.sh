#!/bin/bash
# Cleanup script for departed-admin-ownership
set -e

echo "Cleaning up departed-admin-ownership..."

# Remove Python cache
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Remove pytest cache
rm -rf .pytest_cache

# Remove build artifacts
rm -rf dist/ build/ *.egg-info/

echo "Cleanup complete."
