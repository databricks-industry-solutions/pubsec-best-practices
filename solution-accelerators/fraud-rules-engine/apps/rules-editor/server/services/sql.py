"""SQL execution helper."""

from typing import Optional

from fastapi import HTTPException
from databricks.sdk.service.sql import StatementState, StatementParameterListItem

from server.services.databricks import get_workspace_client, WAREHOUSE_ID


def run_sql(statement: str, parameters: Optional[list[dict]] = None) -> list[dict]:
  """Execute SQL on the warehouse and return rows as dicts.

  Args:
    statement: SQL statement, optionally with :param_name placeholders.
    parameters: Optional list of dicts with 'name', 'value', 'type' keys.
  """
  w = get_workspace_client()

  kwargs: dict = {
    'warehouse_id': WAREHOUSE_ID,
    'statement': statement,
    'wait_timeout': '30s',
  }
  if parameters:
    # SDK requires StatementParameterListItem objects, not plain dicts
    kwargs['parameters'] = [
      StatementParameterListItem(name=p['name'], value=p['value'], type=p.get('type', 'STRING'))
      for p in parameters
    ]

  resp = w.statement_execution.execute_statement(**kwargs)
  if resp.status.state != StatementState.SUCCEEDED:
    raise HTTPException(500, f'SQL failed: {resp.status.error}')
  cols = [c.name for c in resp.manifest.schema.columns]
  return [dict(zip(cols, row)) for row in (resp.result.data_array or [])]
