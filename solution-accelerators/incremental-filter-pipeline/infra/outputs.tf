output "schema_name" {
  description = "Fully qualified schema name for pipeline metadata"
  value       = "${var.catalog}.${var.schema}"
}

output "config_table" {
  description = "Fully qualified name of the pipeline config table"
  value       = "${var.catalog}.${var.schema}.incremental_filter__table_config"
}

output "control_table" {
  description = "Fully qualified name of the sensitive records control table"
  value       = "${var.catalog}.${var.schema}.incremental_filter__sensitive_records"
}
