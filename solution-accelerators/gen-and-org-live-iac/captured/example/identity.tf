# EXAMPLE FIXTURE — fabricated data, not from any real account. Mimics the flat
# output of the Databricks provider's experimental exporter so `run.sh --offline`
# has something to transform. Safe to publish; replace with your own live export.

resource "databricks_group" "data_platform_admins" {
  force        = true
  display_name = "data-platform-admins"
}

resource "databricks_group" "analysts" {
  force        = true
  display_name = "analysts"
}

resource "databricks_service_principal" "app_ingest" {
  display_name   = "app-ingest"
  application_id = "11111111-1111-1111-1111-111111111111"
}

resource "databricks_group_member" "data_platform_admins_app_ingest" {
  group_id  = databricks_group.data_platform_admins.id
  member_id = databricks_service_principal.app_ingest.id
}
