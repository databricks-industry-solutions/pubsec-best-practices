# EXAMPLE FIXTURE — fabricated import ids. See identity.tf for the disclaimer.
# Native import blocks: plane_transform.py carries each into its plane so a first
# `terraform plan` reads as adopt (import), never create.

import {
  id = "1001"
  to = databricks_group.data_platform_admins
}

import {
  id = "1002"
  to = databricks_group.analysts
}

import {
  id = "11111111-1111-1111-1111-111111111111"
  to = databricks_service_principal.app_ingest
}

import {
  id = "1001|11111111-1111-1111-1111-111111111111"
  to = databricks_group_member.data_platform_admins_app_ingest
}

import {
  id = "lakehouse-root"
  to = databricks_storage_credential.lakehouse_root
}

import {
  id = "raw-zone"
  to = databricks_external_location.raw_zone
}

import {
  id = "metastore/00000000-0000-0000-0000-000000000000"
  to = databricks_grants.metastore_admins
}

import {
  id = "sales"
  to = databricks_catalog.sales
}

import {
  id = "sales.core"
  to = databricks_schema.sales_core
}

import {
  id = "catalog/sales"
  to = databricks_grants.sales_catalog
}

import {
  id = "0101-000000-abcd1234"
  to = databricks_cluster.shared_adhoc
}

import {
  id = "abcd1234efgh5678"
  to = databricks_sql_endpoint.bi_serving
}
