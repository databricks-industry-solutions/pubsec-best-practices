# Notebooks: transfer_ownership

Runs the departed-admin ownership sweep inside Databricks. Same two-phase,
dry-run-first design as the bundle job, driven by widgets. Two variants:

- **`transfer_ownership.py`** — account-based. One workspace sweeps **all** workspaces
  via an account SP (needs account-console reachability). Widgets below.
- **`transfer_ownership_local.py`** — **workspace-local**. Uses only the ambient
  `WorkspaceClient`, so it sees only what *this* workspace can see. Deploy into each
  workspace. Use when workspace compute is network-restricted from the account console.
  See [its widget differences](#workspace-local-notebook-transfer_ownership_localpy).

## Import

- **Repos / Git folder:** open the notebook — each is a Databricks
  `# Databricks notebook source` file and imports as a notebook.
- **Manual:** Workspace ▸ Import ▸ File ▸ the `.py` file.

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
| `scope_run_as` | Reassign job `run_as` from departed admins to a per-workspace SP |
| `run_as_sp_map` | JSON `{"<workspace_id>": "<sp_application_id>"}` — required when `scope_run_as=true` |
| `grant_run_as_sp_perms` | Grant the target SP `CAN_MANAGE` on reassigned jobs during execute (default false; a job can only run_as an SP that can manage it) |
| `wsfs_max_depth` | Max dir depth below a home root for the file walk (0 = unlimited) |
| `wsfs_workers` | Concurrent directory listers for the file walk |
| `workspace_ids` | Workspaces to sweep, comma-separated IDs (blank = all). Applies to objects, files, and run_as |
| `skip_catalogs` | Catalogs to skip |
| `output_table` | Delta table for the inventory (`catalog.schema.table`) |

## Run modes

| Mode | Needs | Sees |
|------|-------|------|
| **Account-wide** (recommended) | Account-admin SP in a secret scope | UC (metastore-wide) **plus** workspace objects/files in every workspace |
| **UC-only / single workspace** | Nothing extra (notebook identity) | All UC objects; set `scope_ws=false` and `scope_wsfs=false` |

## Workspace-local notebook (`transfer_ownership_local.py`)

Same phases and workflow, but it runs against **only the current workspace** using the
ambient `WorkspaceClient` — no account SP, no account-console traffic. Widget
differences from the account-based notebook:

| Change | Detail |
|--------|--------|
| **Removed** | `secret_scope`, `secret_key_client_id`, `secret_key_client_secret`, `account_host`, `account_id` — no account client, so none needed |
| **Removed** | `workspace_ids` — it always acts on the one workspace it runs in |
| **Changed** | `run_as_sp_map` (JSON map) → **`run_as_sp`** (a single SP application id for this workspace) |
| `target_group` | Must exist **in this workspace** (account group synced in, or a workspace-local group) |

**Run identity:** the crawl only sees objects the run identity can read, so run the
notebook (or its bundle job) as a **workspace admin** — the bundle pins `run_as` to a
per-workspace admin SP (`run_identity_sp`) for this reason. **Restricted catalogs** are
handled automatically: `information_schema` only exposes catalogs bound to this
workspace. Deploy and run the job in **each** workspace to cover them all.

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
- **`run_as` grant covers only the job ACL.** A reassigned job also needs the target
  SP to have the compute, Unity Catalog data, SQL warehouse, and secret/storage
  access its tasks use — this notebook does not check or grant those. Grant them to
  the SP (ideally via a group) before/with the change, then validate with a manual
  **Run now** per workspace. See the run_as caveat in the accelerator `README.md`.
