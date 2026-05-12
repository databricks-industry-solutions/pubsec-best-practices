terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    azurerm = {
      source = "hashicorp/azurerm"
    }
  }
}

provider "databricks" {
  host = var.databricks_host
}

provider "azurerm" {
  features {}
}

# Unity Catalog schema for pipeline metadata
resource "databricks_schema" "pipeline_schema" {
  catalog_name = var.catalog
  name         = var.schema
  comment      = "Schema for incremental filter pipeline metadata and target tables"
}
