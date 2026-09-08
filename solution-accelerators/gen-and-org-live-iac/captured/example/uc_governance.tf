# EXAMPLE FIXTURE — fabricated data. See identity.tf for the disclaimer.
# catalog / schema + a catalog-level grant (routes to uc-governance). Includes an
# OPEN catalog (`sales`, shared) and an ISOLATED one (`dev_sandbox`, bound to the
# dev workspace) to exercise the workspace-binding domain routing.

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

# An ISOLATED catalog reachable only from the dev workspace, plus its schema,
# grant, and the workspace binding that pins it. With catalog_env_from_bindings
# on (plane_rules.yaml), this catalog and its subtree route to uc-governance-dev
# instead of the shared uc-governance-default, while `sales` (OPEN) stays shared.
resource "databricks_catalog" "dev_sandbox" {
  name           = "dev_sandbox"
  metastore_id   = "00000000-0000-0000-0000-000000000000"
  isolation_mode = "ISOLATED"
}

resource "databricks_schema" "dev_sandbox_scratch" {
  name         = "scratch"
  catalog_name = databricks_catalog.dev_sandbox.name
}

resource "databricks_grants" "dev_sandbox_catalog" {
  catalog = databricks_catalog.dev_sandbox.name
  grant {
    principal  = "data-platform-admins"
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_workspace_binding" "dev_sandbox" {
  workspace_id   = 1111111111111111
  securable_name = databricks_catalog.dev_sandbox.name
  binding_type   = "BINDING_TYPE_READ_WRITE"
}
