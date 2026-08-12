# Notebook: transfer_ownership

Runs the departed-admin ownership sweep inside Databricks. Same two-phase,
dry-run-first design as the bundle job, driven by widgets.

## Import

- **Repos / Git folder:** open `notebooks/transfer_ownership.py` — it's a Databricks
  `# Databricks notebook source` file and imports as a notebook.
- **Manual:** Workspace ▸ Import ▸ File ▸ `transfer_ownership.py`.

## Cluster

- Unity Catalog–enabled cluster, DBR 13.3 LTS+.
- No SQL warehouse needed — the UC crawl runs through Spark
  (`spark.sql` over `system.information_schema`).

## Widgets

| Widget | Meaning |
|--------|---------|
| `secret_scope` | Secret scope holding the account SP creds (blank = UC-only, single-workspace mode) |
| `secret_key_client_id` / `secret_key_client_secret` | Secret keys for the SP OAuth creds |
| `account_host` | Account console URL (AWS `accounts.cloud.databricks.com`, Azure `accounts.azuredatabricks.net`, GCP `accounts.gcp.databricks.com`) |
| `account_id` | Databricks account UUID |
| `departed_admins` | Comma-separated departed-admin emails |
| `target_group` | Account-level group to receive ownership |
| `phase` | `inventory` (read-only) or `transfer` |
| `execute` | `false` = dry-run (default), `true` = apply (transfer phase only) |
| `scope_uc` / `scope_ws` / `scope_wsfs` | Which domains to crawl |
| `wsfs_max_depth` | Max dir depth below a home root for the file walk (0 = unlimited) |
| `wsfs_workers` | Concurrent directory listers for the file walk |
| `workspace_ids` | Limit to specific workspace IDs (blank = all) |
| `skip_catalogs` | Catalogs to skip |
| `output_table` | Delta table for the inventory (`catalog.schema.table`) |

## Run modes

| Mode | Needs | Sees |
|------|-------|------|
| **Account-wide** (recommended) | Account-admin SP in a secret scope | UC (metastore-wide) **plus** workspace objects/files in every workspace |
| **UC-only / single workspace** | Nothing extra (notebook identity) | All UC objects; set `scope_ws=false` and `scope_wsfs=false` |

## Workflow

1. `phase = inventory` → **Run All**. Writes the Delta table and shows a summary
   (including any files flagged as active-job dependencies).
2. Review: `SELECT * FROM <output_table>`. The `current_owner` column records the
   prior owner in case you need to revert.
3. `phase = transfer`, `execute = false` → dry-run; inspect the `result` column.
4. `phase = transfer`, `execute = true` → apply.

## Notes

- **Target group must be an account-level group** synced into the metastore; the
  workspace-local `admins` group can't own UC securables.
- The account SP must be **provisioned into each workspace** with metastore-admin
  rights, or workspace/UC calls return `401 Unauthorized`.
- Some workspace object types treat `IS_OWNER` as immutable; those rows log an error
  in the `result` column rather than aborting the run.
