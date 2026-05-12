variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "catalog" {
  description = "Unity Catalog catalog for pipeline metadata"
  type        = string
  default     = "main"
}

variable "schema" {
  description = "Schema for pipeline metadata and target tables"
  type        = string
  default     = "tmp"
}

variable "checkpoint_base" {
  description = "Base path for streaming checkpoints"
  type        = string
  default     = "/tmp/incremental_filter_pipeline/checkpoints"
}
