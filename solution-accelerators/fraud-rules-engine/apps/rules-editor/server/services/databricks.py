"""Databricks SDK service — shared client for all routes."""

import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient


# Workspace-specific values. Set via `app.yaml` or environment variables when
# deploying to your own workspace; the placeholders here are not valid for any
# real workspace.
CATALOG = os.environ.get('UC_CATALOG', 'main')
SCHEMA = os.environ.get('UC_SCHEMA', 'irs_rrp')
VOLUME = os.environ.get('UC_VOLUME', 'dmn_rules')
VOLUME_PATH = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}'
WAREHOUSE_ID = os.environ.get('DATABRICKS_WAREHOUSE_ID', '')
CLUSTER_ID = os.environ.get('DATABRICKS_CLUSTER_ID', '')
def _int_env(name: str, default: int = 0) -> int:
  """Read an int env var, tolerating empty strings and unsubstituted placeholders."""
  raw = (os.environ.get(name) or '').strip()
  try:
    return int(raw)
  except (TypeError, ValueError):
    return default


JOB_ID = _int_env('SCORING_JOB_ID')
NOTEBOOK_PATH = os.environ.get(
  'SCORING_NOTEBOOK_PATH',
  '/Workspace/Shared/fraud_rules_engine/04_batch_scoring',
)


@lru_cache()
def get_workspace_client() -> WorkspaceClient:
  """Get or create the Databricks workspace client."""
  return WorkspaceClient()
