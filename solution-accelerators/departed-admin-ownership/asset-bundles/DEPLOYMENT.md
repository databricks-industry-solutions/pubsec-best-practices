# Deploying the Asset Bundle

Step-by-step guide to configuring and deploying the Databricks Asset Bundle (DAB) for
both jobs it ships:

- **`ownership_transfer`** — the **account-based** notebook (`transfer_ownership.py`).
  One workspace sweeps **all** workspaces via an account service principal. Requires
  the workspace to reach the **account console**.
- **`ownership_transfer_local`** — the **workspace-local** notebook
  (`transfer_ownership_local.py`). Uses only the ambient workspace identity and sees
  only **this** workspace. Deploy it into **each** workspace. Use when workspace
  compute is network-restricted from the account console.

Both are two-task jobs (`inventory` → `transfer`) that are **dry-run by default** —
running end to end never mutates anything until you pass `execute=true`.

> Not sure which to use? See the run-mode comparison in the top-level
> [`README.md`](../README.md#choosing-a-run-mode). If you can't run Databricks compute
> at all against the account console but have a laptop/CI host that can, use the CLI in
> [`../cli/`](../cli/) instead of a bundle.

---

## 0. Prerequisites

- **Databricks CLI v0.218+** (the one with bundle support): `databricks --version`.
- Authenticated to the target workspace. Either a profile in `~/.databrickscfg`, or
  the `DATABRICKS_HOST` / `DATABRICKS_TOKEN` environment variables the bundle targets
  read (see [`../env.example`](../env.example)).
- A **Unity Catalog–enabled** workspace and permission to create jobs + a job cluster.
- The **target group exists** and can own UC securables:
  - Account-based: an **account-level** group synced into the metastore (the
    workspace-local `admins` group cannot own UC securables).
  - Workspace-local: a group present **in that workspace** (synced account group or a
    workspace-local group).

```bash
# from the repo root
cd solution-accelerators/departed-admin-ownership/asset-bundles
databricks bundle validate -t dev     # sanity check before configuring
```

---

## 1. Understand the variables

All variables live in [`databricks.yml`](databricks.yml) with defaults. You override
them three ways (later wins): the `default:` in `databricks.yml`, a per-target
`variables:` block, or `--var key=value` at deploy/run time. The safe non-mutating
defaults (`execute`, all `scope_*` gates off except UC/WS) live in the notebook and
job resources — you rarely change those at deploy time.

### Shared by both jobs

| Variable | Required | Meaning |
|----------|----------|---------|
| `databricks_host` | ✅ | Workspace URL the bundle deploys into. The `dev`/`staging`/`prod` targets read it from `${DATABRICKS_HOST}`. |
| `departed_admins` | ✅ | Comma-separated departed-admin emails, e.g. `a@corp.com,b@corp.com`. |
| `target_group` | ✅ | Group that receives ownership. |
| `output_table` | ✅ | Delta table for the inventory, `catalog.schema.table`. |
| `scope_uc` / `scope_ws` | | Crawl Unity Catalog / workspace objects (default `true`). |
| `scope_wsfs` | | Crawl workspace files in departed-admin home trees (default `false`). |
| `scope_run_as` | | Reassign job `run_as` (default `false`). |
| `grant_run_as_sp_perms` | | Grant the target SP `CAN_MANAGE` on reassigned jobs during execute (default `false`). |
| `wsfs_max_depth` / `wsfs_workers` | | WSFS walk depth (0 = unlimited) and concurrency. |
| `skip_catalogs` | | Comma-separated catalogs to skip (default `__databricks_internal,system`). |
| `spark_version` / `node_type_id` | | Job-cluster DBR + node type. `node_type_id` defaults to an **AWS** type (`m5d.large`) — override on Azure/GCP (e.g. `Standard_DS3_v2`, `n1-standard-4`). |

### Account-based job only (`ownership_transfer`)

| Variable | Required | Meaning |
|----------|----------|---------|
| `secret_scope` | ✅ | Secret scope holding the account SP creds. |
| `secret_key_client_id` / `secret_key_client_secret` | | Secret keys (default `account_sp_client_id` / `account_sp_client_secret`). |
| `account_host` | ✅ | Account console URL (AWS `accounts.cloud.databricks.com`, Azure `accounts.azuredatabricks.net`, GCP `accounts.gcp.databricks.com`). |
| `account_id` | ✅ | Databricks account UUID. |
| `workspace_ids` | | Comma-separated workspace IDs to limit the sweep to (blank = all). |
| `run_as_sp_map` | when `scope_run_as` | JSON `{"<workspace_id>":"<sp_app_id>"}` — the `run_as` target per workspace. |

### Workspace-local job only (`ownership_transfer_local`)

| Variable | Required | Meaning |
|----------|----------|---------|
| `run_identity_sp` | ✅ | Workspace-admin SP the job **runs as**, so the crawl sees every owner's objects. |
| `run_as_sp` | when `scope_run_as` | Single SP application id that jobs in this workspace should `run_as`. |

The local job ignores `secret_scope`, `account_*`, `run_as_sp_map`, and
`workspace_ids` — it never talks to the account console and only ever acts on the one
workspace it runs in.

---

## 2A. Deploy the account-based job (`ownership_transfer`)

**Use when:** the workspace you deploy into can reach the **account console**, and you
want one job to sweep the whole account.

### Step 1 — Provision the account SP

1. Create (or reuse) an **account admin** service principal and generate an OAuth
   secret (`client_id` + `client_secret`).
2. **Add the SP to every workspace** you want swept and grant it **metastore admin**.
   An account admin that is *not* added to a workspace gets `401 Unauthorized` on that
   workspace's calls — this is the most common failure.

### Step 2 — Store the SP creds in a secret scope

```bash
databricks secrets create-scope departed_admin_ownership
databricks secrets put-secret departed_admin_ownership account_sp_client_id
databricks secrets put-secret departed_admin_ownership account_sp_client_secret
```

### Step 3 — Configure the bundle

Set the workspace host, then either edit the `dev` target's `variables:` block in
`databricks.yml` or pass `--var` at deploy time:

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com   # or use a profile

databricks bundle deploy -t dev \
  --var secret_scope=departed_admin_ownership \
  --var account_id=0d26daa6-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  --var account_host=https://accounts.cloud.databricks.com \
  --var departed_admins=former.admin1@corp.com,former.admin2@corp.com \
  --var target_group=data-platform-admins \
  --var output_table=main.admin.ownership_inventory \
  --var run_identity_sp=placeholder-unused-by-this-job
```

> **Why `run_identity_sp` here?** Both jobs ship in one bundle, and the workspace-local
> job makes that variable **required** (an empty value would render an invalid
> `run_as`). The account-based job never reads it, so any placeholder satisfies the
> deploy. If you only ever run the account job, you can delete
> `resources/jobs/ownership_transfer_local.yml` and the `run_identity_sp` variable.

> **Pin the run identity (recommended for prod).** So the sweep runs as the account SP
> and not as an individual, uncomment the `run_as:` block in the `prod` target of
> `databricks.yml` and set it to the account SP application id.

### Step 4 — Run inventory + dry-run

```bash
databricks bundle run ownership_transfer -t dev
```

This runs `inventory` (writes `output_table`) then `transfer` in **dry-run**. Inspect
the run output and the `result` column of the inventory table.

### Step 5 — Review, then apply

```sql
-- Inventory appends one row-set per run (tagged run_id / run_timestamp), so scope to
-- the latest run for review. The transfer task applies only this latest run.
SELECT * FROM main.admin.ownership_inventory
WHERE run_id = (SELECT max(run_id) FROM main.admin.ownership_inventory)
ORDER BY domain, object_type;

-- See the run history:
SELECT run_id, run_timestamp, count(*) n
FROM main.admin.ownership_inventory GROUP BY 1, 2 ORDER BY run_timestamp DESC;
```

When satisfied, re-run **only** the transfer task with `execute=true`:

```bash
databricks bundle run ownership_transfer -t dev --only transfer -- \
  --notebook-params execute=true
```

### Optional — scope the sweep and reassign `run_as`

- **Limit workspaces:** `--var workspace_ids=1234567890,9876543210` (blank = all).
- **Reassign `run_as`:** `--var scope_run_as=true` and provide the per-workspace map:
  ```bash
  --var scope_run_as=true \
  --var 'run_as_sp_map={"1234567890":"sp-app-id-ws-a","9876543210":"sp-app-id-ws-b"}' \
  --var grant_run_as_sp_perms=true    # grant SP CAN_MANAGE as part of execute
  ```

---

## 2B. Deploy the workspace-local job (`ownership_transfer_local`)

**Use when:** workspace compute is **network-restricted** from the account console.
You deploy and run this **once per workspace**; each run only sees its own workspace.

### Step 1 — Prepare a run-identity SP per workspace

The crawl only sees objects its run identity can read, so the job runs as a
**workspace admin** SP (`run_identity_sp`). In each workspace:

1. Create/reuse a service principal and add it to the workspace as a **workspace
   admin** (and **metastore admin** for full UC coverage).
2. No secret scope is needed — the bundle's `run_as` runs the job *as* this SP; you
   don't pass its credentials.

> Restricted catalogs need no special handling: `system.information_schema` only
> exposes catalogs bound to the current workspace, so each workspace's run naturally
> covers exactly its own catalogs.

### Step 2 — Deploy into a workspace

Repeat per workspace, pointing `DATABRICKS_HOST` (or your profile) at each one and
giving each its own local `output_table`:

```bash
export DATABRICKS_HOST=https://workspace-a.cloud.databricks.com

databricks bundle deploy -t dev \
  --var departed_admins=former.admin1@corp.com,former.admin2@corp.com \
  --var target_group=data-platform-admins \
  --var output_table=main.admin.ownership_inventory \
  --var run_identity_sp=<workspace-a-admin-sp-app-id>
```

### Step 3 — Run inventory + dry-run, review, apply

```bash
databricks bundle run ownership_transfer_local -t dev            # inventory + dry-run

# review main.admin.ownership_inventory in THIS workspace, then:
databricks bundle run ownership_transfer_local -t dev --only transfer -- \
  --notebook-params execute=true
```

### Optional — reassign `run_as` (single SP)

The local job uses one SP for the whole workspace (not a map):

```bash
--var scope_run_as=true \
--var run_as_sp=<this-workspace-sp-app-id> \
--var grant_run_as_sp_perms=true
```

### Repeat for every workspace

Re-run steps 2–3 for each workspace, changing `DATABRICKS_HOST`, `run_identity_sp`,
and (optionally) `output_table`. Consider scripting the loop over your workspace list.

---

## 3. Environments (dev / staging / prod)

`databricks.yml` defines three targets; `dev` is the default. `prod` uses
`mode: production` (stricter deploy semantics). Select with `-t`:

```bash
databricks bundle deploy -t prod
databricks bundle run ownership_transfer -t prod
```

Keep per-environment values (hosts, account IDs, target groups) in each target's
`variables:` block so a plain `deploy -t <env>` needs no long `--var` list. See
[`../variables.yml`](../variables.yml) for the documented value set.

---

## 4. Verify, then clean up

```bash
databricks bundle validate -t dev        # config parses and resolves
databricks bundle summary  -t dev        # what will be deployed
```

To tear down the deployed job(s) (leaves your inventory tables intact):

```bash
databricks bundle destroy -t dev
```

---

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `no value assigned to required variable run_identity_sp` | Both jobs share the bundle and the local job requires this SP. Pass `--var run_identity_sp=...` (a placeholder is fine for the account-only job). |
| `401 Unauthorized` on workspace/UC calls (account job) | The account SP isn't provisioned into that workspace, or lacks metastore admin. Add it and grant metastore admin. |
| `Interpolation is not supported for the field workspace.host` on local `validate` | Expected — `workspace.host` reads `${DATABRICKS_HOST}` at deploy time. Set the env var (or a profile) and it resolves. |
| Inventory is empty / missing objects (local job) | The `run_identity_sp` isn't a workspace/metastore admin, so it can't see others' objects. Elevate it. |
| Target group "NOT FOUND" | Account job: the group must be an **account-level** group synced into the metastore. Local job: the group must exist **in that workspace**. |
| Reassigned jobs fail after `run_as` change | `CAN_MANAGE` on the job is necessary but not sufficient — the SP also needs the compute / UC data / SQL warehouse / secret access its tasks use. See the run_as caveat in [`../README.md`](../README.md). |
| Node type error on Azure/GCP | `node_type_id` defaults to an AWS type. Override it, e.g. `--var node_type_id=Standard_DS3_v2`. |

See [`../notebooks/README.md`](../notebooks/README.md) for the full widget reference
behind these variables.
