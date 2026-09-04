# EXAMPLE FIXTURE — fabricated data. See identity.tf for the disclaimer.
# catalog / schema + a catalog-level grant (routes to uc-governance).

resource "databricks_catalog" "sales" {
  name         = "sales"
  metastore_id = "00000000-0000-0000-0000-000000000000"
}

resource "databricks_schema" "sales_core" {
  name         = "core"
  catalog_name = databricks_catalog.sales.name
}

resource "databricks_grants" "sales_catalog" {
  catalog = databricks_catalog.sales.name
  grant {
    principal  = "analysts"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}
