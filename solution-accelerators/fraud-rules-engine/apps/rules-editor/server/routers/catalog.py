"""Unity Catalog browser routes — explore catalogs, schemas, tables, columns."""

from fastapi import APIRouter, HTTPException

from server.services.databricks import get_workspace_client

router = APIRouter()


@router.get('/catalogs', operation_id='listCatalogs')
async def list_catalogs():
  """List accessible Unity Catalog catalogs."""
  w = get_workspace_client()
  try:
    catalogs = [c.name for c in w.catalogs.list() if c.name]
    return {'catalogs': sorted(catalogs)}
  except Exception as e:
    raise HTTPException(500, f'Failed to list catalogs: {e}')


@router.get('/schemas', operation_id='listSchemas')
async def list_schemas(catalog: str):
  """List schemas in a catalog."""
  w = get_workspace_client()
  try:
    schemas = [s.name for s in w.schemas.list(catalog_name=catalog) if s.name]
    return {'schemas': sorted(schemas)}
  except Exception as e:
    raise HTTPException(500, f'Failed to list schemas: {e}')


@router.get('/tables', operation_id='listTables')
async def list_tables(catalog: str, schema: str):
  """List tables in a catalog.schema."""
  w = get_workspace_client()
  try:
    tables = []
    for t in w.tables.list(catalog_name=catalog, schema_name=schema):
      if t.name:
        tables.append({
          'name': t.name,
          'full_name': f'{catalog}.{schema}.{t.name}',
          'table_type': str(t.table_type.value) if t.table_type else 'UNKNOWN',
          'comment': t.comment or '',
        })
    return {'tables': sorted(tables, key=lambda x: x['name'])}
  except Exception as e:
    raise HTTPException(500, f'Failed to list tables: {e}')


@router.get('/columns', operation_id='listColumns')
async def list_columns(catalog: str, schema: str, table: str):
  """List columns for a table with name and type info."""
  w = get_workspace_client()
  try:
    info = w.tables.get(f'{catalog}.{schema}.{table}')
    columns = []
    for col in (info.columns or []):
      columns.append({
        'name': col.name,
        'type': str(col.type_name.value) if col.type_name else 'STRING',
        'comment': col.comment or '',
        'nullable': col.nullable if col.nullable is not None else True,
      })
    return {'columns': columns, 'full_name': f'{catalog}.{schema}.{table}'}
  except Exception as e:
    raise HTTPException(500, f'Failed to list columns: {e}')
