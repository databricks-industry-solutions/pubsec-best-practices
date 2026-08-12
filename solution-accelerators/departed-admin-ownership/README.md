# Departed Admin Ownership Transfer

Find every **Unity Catalog** and **workspace** object owned by departed
administrators across all workspaces in a Databricks account, and — as a separate,
reviewed step — transfer ownership to an admin group. Built for offboarding: when an
admin leaves, their owned catalogs, tables, jobs, clusters, models, and home-tree
files need a new owner before their account is deactivated.

## Why

When a Databricks admin leaves, objects they own can become orphaned. Unity Catalog
securables owned by a deactivated user block `ALTER`/`GRANT` operations; jobs and
files they own may keep running but can't be managed. This accelerator inventories
everything a departed admin owns and reassigns it to a durable admin **group**.

## How it works

Two phases, **dry-run first**:

1. **`inventory`** — read-only. Crawls the account and every workspace, writing a
   Delta table of every object owned by the departed admins. Mutates nothing.
2. **`transfer`** — reads the reviewed table and reassigns ownership to the target
   group. Defaults to **dry-run**; set `execute = true` to apply. Idempotent.

## What gets covered

| Domain | Objects | Owner source | Transfer |
|--------|---------|--------------|----------|
| Unity Catalog | catalogs, schemas, tables/views, volumes, functions | `information_schema.*_owner` | `ALTER … OWNER TO` |
| Unity Catalog | external locations, storage credentials, connections, shares, recipients, registered models | REST `owner` | owner-only PATCH |
| Workspace | jobs, pipelines, clusters, SQL warehouses, serving endpoints, experiments, MLflow models, Lakeview dashboards | `IS_OWNER` via permissions API | set group `IS_OWNER` |
| Workspace files | notebooks, files, dirs, repos, dashboards in `/Users/<email>/` + `/Repos/<email>/` | home-tree location (WSFS has **no owner**) | grant group **CAN_MANAGE** (additive) |
| Job `run_as` | jobs whose **effective** run identity is a departed admin | `run_as_user_name` (via `jobs.get`) | set `run_as` to a **per-workspace SP** |

### Job `run_as` reassignment

`run_as` is who a job *executes as* — a different axis from ownership. When it's a
departed admin's identity, the job keeps running as a user who no longer exists.
Enable `scope_run_as` to reassign it to a **service principal that varies by
workspace**, supplied as a JSON map (`run_as_sp_map`):

```json
{"1234567890123456": "sp-app-id-ws-a", "6543210987654321": "sp-app-id-ws-b"}
```

Matching uses the **effective** identity (`run_as_user_name`), so it catches both
jobs with an explicit `run_as` *and* jobs with none set that still run as their
departed-admin creator. The transfer is a **partial** `jobs.update` that replaces
only `run_as` — tasks, schedule, and clusters are untouched. Workspaces without an
entry in the map are skipped (logged), so you never accidentally repoint a job to the
wrong workspace's SP.

**Preflight + optional grant.** A job can only `run_as` a service principal that can
manage it — otherwise it fails at run time. During inventory, each reassigned job is
flagged in the `extra` column as `SP_HAS_ACCESS` or `SP_NEEDS_GRANT` (does the target
SP already hold `CAN_MANAGE`/`IS_OWNER`?), and the run prints a preflight summary of
how many need a grant. To fix them as part of the transfer, set
`grant_run_as_sp_perms = true`: during the execute step it grants the SP
`CAN_MANAGE` (additive — other principals' ACLs are preserved) on each job before
reassigning `run_as`. Left `false` (the default), the transfer still reassigns
`run_as` but **warns** on jobs where the SP lacks access, so you can grant it
out-of-band instead.

### Workspace files: the active-job flag

Workspace files/notebooks/repos have no owner — their ACL model is
CAN_READ/RUN/EDIT/MANAGE. So "owned by a departed admin" means the object lives in
that admin's home tree, and "transfer" grants the target group **CAN_MANAGE**
(access survives the user's deletion). Any path referenced by an **active, recurring
job** (unpaused schedule / continuous / trigger) is flagged in the `extra` column
(`ACTIVE_JOB_DEPENDENCY: <job names>`) and in the summary — re-home those *before*
offboarding, or the job breaks. Build/dependency noise (`.venv`, `.git`,
`node_modules`, …) is pruned. The walk is concurrent (`wsfs_workers`, default 8) and
depth-limited (`wsfs_max_depth`, default 0 = unlimited) for large home trees.

## Requirements

- A Unity Catalog–enabled cluster (for the `system.information_schema` crawl).
- An **account-admin service principal** whose `client_id`/`client_secret` live in a
  **Databricks secret scope** (for the cross-workspace sweep). The SP must be
  **provisioned into each workspace** and granted **metastore admin** — an account
  admin that is not added to a workspace gets `401 Unauthorized` on workspace calls.
- The **target group must be an account-level group** synced into the metastore; the
  workspace-local `admins` group cannot own UC securables.

Cloud-neutral: workspace hosts are resolved from the account API
(`get_workspace_client`), so it works on AWS, Azure, and GCP. Set `account_host` to
your account console (AWS `accounts.cloud.databricks.com`, Azure
`accounts.azuredatabricks.net`, GCP `accounts.gcp.databricks.com`).

## Quick start (notebook)

1. Import `notebooks/transfer_ownership.py` into your workspace.
2. Store the account SP creds in a secret scope:
   ```bash
   databricks secrets create-scope departed_admin_ownership
   databricks secrets put-secret departed_admin_ownership account_sp_client_id
   databricks secrets put-secret departed_admin_ownership account_sp_client_secret
   ```
3. Set the widgets: `secret_scope`, `account_id`, `departed_admins` (comma-separated
   emails), `target_group`, `output_table` (e.g. `main.admin.ownership_inventory`).
4. `phase = inventory` → **Run All**. Review the Delta table.
5. `phase = transfer`, `execute = false` → dry-run; inspect the `result` column.
6. `phase = transfer`, `execute = true` → apply.

### Scoping which workspaces are swept (1..n)

`workspace_ids` selects which workspaces the run touches — a comma-separated list
given at runtime. Leave it **blank to sweep every workspace** in the account, or set
e.g. `1234567890,9876543210` to limit the run to those workspaces. This applies to
all workspace-level scopes (objects, files, and `run_as`).

### Enabling `run_as` reassignment

Set `scope_run_as = true` and provide `run_as_sp_map` — a JSON object mapping each
in-scope workspace ID to the service principal that job `run_as` should point to:

```json
{"1234567890": "sp-app-id-ws-a", "9876543210": "sp-app-id-ws-b"}
```

Only workspaces present in the map are touched for `run_as`; others are logged and
skipped. Pair it with `workspace_ids` to run a controlled subset at a time.

## Deploy as a job (Asset Bundle)

The bundle in `asset-bundles/` deploys the notebook as a **two-task job**
(`inventory` → `transfer`, dry-run). Running it end to end never mutates.

```bash
cd asset-bundles
databricks bundle deploy -t dev

# inventory + dry-run preview
databricks bundle run ownership_transfer -t dev

# review, then apply — re-run ONLY the transfer task with execute=true
databricks bundle run ownership_transfer -t dev --only transfer -- \
  --notebook-params execute=true
```

See `notebooks/README.md` for the widget reference and `docs/` for the offboarding
runbook.

## Safety

- `inventory` performs **no writes**.
- `transfer` is **dry-run by default**; `execute=true` is required to mutate.
- Idempotent — objects already owned by the target group are skipped.
- Each object is applied independently; one failure does not abort the run.
- Ownership changes are hard to reverse in bulk — the `current_owner` column is your
  record of the prior owner if you need to revert.

## License

See [LICENSE](LICENSE).
