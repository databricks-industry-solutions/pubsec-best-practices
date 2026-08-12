terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }

  # Uncomment and configure for remote state
  # backend "azurerm" {
  #   resource_group_name  = "tfstate-rg"
  #   storage_account_name = "tfstate"
  #   container_name       = "tfstate"
  #   key                  = "departed-admin-ownership.tfstate"
  # }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  # Configure via environment variables or Azure CLI
}

# This accelerator operates on an EXISTING Databricks account and provisions no
# cloud infrastructure of its own. The only prerequisites are account-level (an
# account-admin service principal added to each workspace as metastore admin, and an
# account-level target group) — create them here if you want them managed as code,
# otherwise this module can stay empty and you can deploy the bundle directly.
