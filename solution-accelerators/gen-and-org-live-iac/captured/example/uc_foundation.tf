# EXAMPLE FIXTURE — fabricated data. See identity.tf for the disclaimer.
# metastore-scoped UC plumbing + a metastore-level grant (routes to uc-foundation).

resource "databricks_storage_credential" "lakehouse_root" {
  name = "lakehouse-root"
  aws_iam_role {
    role_arn = "arn:aws:iam::000000000000:role/example-uc-access"
  }
}

resource "databricks_external_location" "raw_zone" {
  name            = "raw-zone"
  url             = "s3://example-bucket/raw"
  credential_name = databricks_storage_credential.lakehouse_root.name
}

resource "databricks_grants" "metastore_admins" {
  metastore = "00000000-0000-0000-0000-000000000000"
  grant {
    principal  = "data-platform-admins"
    privileges = ["CREATE_CATALOG", "CREATE_EXTERNAL_LOCATION"]
  }
}
