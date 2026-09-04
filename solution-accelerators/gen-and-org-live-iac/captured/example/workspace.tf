# EXAMPLE FIXTURE — fabricated data. See identity.tf for the disclaimer.
# workspace-local compute (routes to workspace-<env>).

resource "databricks_cluster" "shared_adhoc" {
  cluster_name            = "shared-adhoc"
  spark_version           = "15.4.x-scala2.12"
  node_type_id            = "m5d.large"
  autotermination_minutes = 30
  num_workers             = 2
}

resource "databricks_sql_endpoint" "bi_serving" {
  name             = "bi-serving"
  cluster_size     = "Small"
  max_num_clusters = 1
}
