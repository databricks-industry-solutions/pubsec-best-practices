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

## Companion: revoke account-level access (`scripts/offboard_account_access.py`)

Ownership transfer re-homes what the admin *owns*; it doesn't remove the admin's own
*access*. Once ownership is transferred, run the standalone script to strip a departed
admin of account-level access:

- membership in every account-level **group**,
- **direct** SCIM **roles** (e.g. a directly-granted `account_admin`),
- **direct** account **entitlements**,
- per-workspace **access assignments** across every workspace.

Roles/entitlements inherited *via a group* (e.g. `account_admin` from the `admins` group)
clear automatically when the group memberships are removed. With `--deactivate` it also
sets each account to inactive (`active=false`) as a **final step**, after access is
removed (reversible; does not delete the account). Dry-run by default:

```bash
# preview
python scripts/offboard_account_access.py --profile <account-profile> \
    --admins former1@corp.com,former2@corp.com
# apply, and deactivate the accounts as the final step
python scripts/offboard_account_access.py --profile <account-profile> \
    --admins former1@corp.com,former2@corp.com --deactivate --execute
```

**Recommended offboarding order:** (1) transfer ownership (this toolkit) → (2) run
`offboard_account_access.py --deactivate` → (3) later, delete the account if desired.
Doing it in this order means nothing the admin owned is orphaned and their access is
fully revoked before the account is disabled.

## What gets covered

| Domain | Objects | Owner source | Transfer |
|--------|---------|--------------|----------|
| Unity Catalog | catalogs, schemas, tables/views, volumes, functions | `information_schema.*_owner` | `ALTER … OWNER TO` |
| Unity Catalog | external locations, storage credentials, connections, shares, recipients, registered models | REST `owner` | owner-only PATCH |
| Workspace | jobs, pipelines, SQL warehouses, serving endpoints, experiments, MLflow models, Lakeview dashboards | `IS_OWNER` via permissions API | reassign `IS_OWNER` to the **run_as SP** (see below) |
| Workspace | all-purpose clusters where the admin holds an explicit grant (`CAN_ATTACH_TO`/`CAN_RESTART`/`CAN_MANAGE`) | non-inherited ACL entry via permissions API | **revoke** the admin's entitlement (no group grant) |
| Workspace files | notebooks, files, dirs, repos, dashboards in `/Users/<email>/` + `/Repos/<email>/` | home-tree location (WSFS has **no owner**) | grant group **CAN_MANAGE** (additive) |
| Job `run_as` | jobs whose **effective** run identity is a departed admin | `run_as_user_name` (via `jobs.get`) | set `run_as` to a **per-workspace SP** |

### Workspace-object ownership goes to a service principal, not the group

A **group cannot own** a workspace object: jobs reject it outright (`Groups cannot be
owners`), and although a warehouse will accept a group owner, the owner cannot be changed
with the additive PATCH the rest of the tool uses (warehouses reject an owner PATCH; a
PATCH that adds an `IS_OWNER` to a job leaves *two* owners — `must have exactly one
owner`). So workspace-object ownership is reassigned to **this workspace's run_as service
principal** — the same SP configured for `run_as` (`run_as_sp_map` / `run_as_sp`), reused
as the durable owner. The transfer does a full-ACL `set` (PUT) with exactly one
`IS_OWNER` (the SP), preserving every other principal's direct grant and dropping the
departed admin. **If no run_as SP is configured for a workspace, those rows are inventoried
but the transfer skips them** (there is no valid group target). Unity Catalog securables
are unaffected — a group *can* own those, so they still transfer to the target group.

All of jobs, pipelines, warehouses, serving endpoints, experiments, MLflow models, and
Lakeview dashboards are crawled, but only object types that actually expose an `IS_OWNER`
(jobs, pipelines, warehouses, dashboards) ever match — the others have no owner concept
(their ACLs are CAN_VIEW/CAN_MANAGE/…), so they never produce an ownership row.

### All-purpose clusters: grant revocation

Clusters have **no owner** — the permissions API exposes only `CAN_ATTACH_TO` /
`CAN_RESTART` / `CAN_MANAGE`, with no `IS_OWNER` level (unlike jobs or warehouses). A
departed admin's tie to a cluster is therefore always an explicit ACL grant, never
ownership, so clusters are never transferred to the group — they are revoked.

Only all-purpose clusters (`cluster_source` = `UI`/`API`) are considered — job/pipeline/
model-serving clusters are ephemeral and managed via their parent resource. Every such
cluster on which the admin holds an explicit, **non-inherited** grant is inventoried as
`object_type=cluster_acl`, `transfer_method=cluster_revoke`, with the strongest held
level noted in `extra`. The execute step **removes just that user's grant** from the
cluster ACL. Because the permissions API PATCH cannot delete a principal, the ACL is
re-`set` from every other principal's direct grant (one entry per principal); inherited
or group-derived access is left untouched.

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

> **Caveat — job-level CAN_MANAGE is necessary but not sufficient.** The preflight
> and grant cover only the **job ACL**. A job reassigned to a service principal also
> needs that SP to hold the access its tasks actually use, which this accelerator
> does **not** check or grant:
> - **Compute** — `CAN_ATTACH_TO`/`CAN_MANAGE` on any all-purpose cluster the job
>   uses, or instance-pool/policy access for job clusters.
> - **Unity Catalog data** — `USE CATALOG`/`USE SCHEMA` + `SELECT`/`MODIFY` (and
>   `EXECUTE` for functions, `READ VOLUME`/`WRITE VOLUME` for volumes) on every
>   securable the tasks read or write.
> - **SQL warehouses** — `CAN_USE` on any warehouse a SQL task targets.
> - **External resources** — secret-scope `READ`, storage credentials/external
>   locations, and any git/repo credentials the job relies on.
>
> Grant these to the target SP (ideally via a group the SP belongs to) before or
> alongside the `run_as` change, or the job will fail at run time even though its
> `run_as` and job ACL are correct. Validate with a manual **Run now** on a
> representative job per workspace after the transfer.

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
4. `phase = inventory` → **Run All**. Appends this run's rows (stamped with `run_id` /
   `run_timestamp`) to the Delta table; review them. The table retains prior runs as
   history.
5. `phase = transfer`, `execute = false` → dry-run over the **latest run**; inspect the
   `result` column.
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

## Choosing a run mode

There are three ways to run this, differing only in **how they reach across
workspaces**. The crawl/transfer logic is the same everywhere.

| Mode | Where it runs | Reaches the account console? | Use when |
|------|---------------|------------------------------|----------|
| **Account notebook** (`transfer_ownership.py`) | Databricks compute in one workspace | **Yes** — sweeps all workspaces via an account SP | Workspace compute can reach the account console |
| **Workspace-local notebook** (`transfer_ownership_local.py`) | Databricks compute, **deployed into each workspace** | **No** — only what that workspace can see | Workspace compute is network-restricted from the account console |
| **CLI** (`cli/`) | A host outside Databricks (laptop / jump host / CI) | **Yes** — from the CLI host, not from workspace compute | You have a host that can reach the account console but workspace compute cannot |

The two notebooks and the CLI all produce the same inventory schema and use the same
transfer methods; pick by what your network allows.

## Deploy as a job (Asset Bundle)

The bundle in `asset-bundles/` ships **both** notebooks as two-task jobs
(`inventory` → `transfer`, dry-run). Running either end to end never mutates.

- `ownership_transfer` — the **account** notebook (cross-workspace sweep).
- `ownership_transfer_local` — the **workspace-local** notebook; deploy it into each
  workspace and run it there.

```bash
cd asset-bundles
databricks bundle deploy -t dev

# --- account-based (one workspace sweeps the account) ---
databricks bundle run ownership_transfer -t dev            # inventory + dry-run preview
databricks bundle run ownership_transfer -t dev --only transfer -- \
  --notebook-params execute=true                           # review, then apply

# --- workspace-local (deploy + run in EACH workspace) ---
databricks bundle run ownership_transfer_local -t dev      # inventory + dry-run preview
databricks bundle run ownership_transfer_local -t dev --only transfer -- \
  --notebook-params execute=true                           # review, then apply
```

**For a full step-by-step walkthrough** — provisioning the SP, secret scope, per-target
variables, per-workspace deploy loop, and troubleshooting — see
[`asset-bundles/DEPLOYMENT.md`](asset-bundles/DEPLOYMENT.md). See `notebooks/README.md`
for the widget reference.

### Workspace-local mode (network-restricted from the account console)

If workspace compute **cannot reach the account console**, the account notebook's
cross-workspace sweep won't work. `transfer_ownership_local.py` runs entirely against
the **ambient `WorkspaceClient`** — no `AccountClient`, no account SP, no
account-console traffic — so it inventories and transfers only what *this* workspace
can see. Deploy the bundle into **each** workspace (set `databricks_host` /
`output_table` per workspace) and run `ownership_transfer_local` there.

- **Run identity matters.** The crawl only sees objects the run identity can read, so
  the job's `run_as` is pinned to a per-workspace **admin service principal**
  (`run_identity_sp`). Make it a workspace admin (and metastore admin for full UC
  coverage).
- **Restricted catalogs are handled naturally.** `system.information_schema` only
  exposes the catalogs bound to this workspace, so catalogs restricted to other
  workspaces simply don't appear — no special handling. Running the job in each
  workspace covers each workspace's catalogs.
- **`run_as` reassignment** targets a single `run_as_sp` (this workspace's SP), not the
  JSON per-workspace map the account notebook uses.
- **Per-workspace output.** Each job writes its inventory to a workspace-local
  `output_table`; review and transfer happen within each workspace.

## Run from the CLI (network-restricted workspaces)

The notebook runs on workspace compute and calls the account console directly
(SCIM, `workspaces.list`, per-workspace token exchange). **If workspace compute is
network-restricted from the account console, use the CLI in `cli/` instead** — run
it from a host that *can* reach the account console (a laptop, jump host, or CI
runner), while it still reaches each workspace's API.

Auth bootstrap (no account-console traffic from workspace compute):

1. The CLI authenticates to a **bootstrap workspace** it can reach, via a normal
   Databricks CLI profile.
2. It reads the account SP `client_id`/`client_secret` from a **secret scope** in
   that workspace.
3. It builds the `AccountClient` **from the CLI host** and uses
   `get_workspace_client()` for the sweep — so all account-console traffic and token
   exchange happen from the CLI host, not from workspace compute.

```bash
cd cli
pip install databricks-sdk PyYAML          # or: uv run --with databricks-sdk,PyYAML python main.py ...
cp config.example.yaml config.yaml         # fill in bootstrap, admins, target group, scopes

python main.py -c config.yaml whoami                 # sanity: account, principals, workspaces
python main.py -c config.yaml inventory              # read-only crawl -> out/inventory_<ts>.csv
# review the CSV, then:
python main.py -c config.yaml transfer --from out/inventory_<ts>.csv            # dry-run
python main.py -c config.yaml transfer --from out/inventory_<ts>.csv --execute  # apply
```

Feature parity with the notebook: UC (SQL warehouse per workspace via
`sql_warehouse_ids`, with REST fallback), workspace objects, workspace files, and
job `run_as` (per-workspace SP + preflight + optional grant). `workspace_ids` selects
the **1..n** workspaces to sweep (empty = all). `--only` and `--limit` scope the
transfer. See `cli/config.example.yaml` for all options.

Set `workspace_workers` > 1 to crawl and transfer workspaces **in parallel** — one
thread per workspace, each with its own client and warehouse, so a large fleet sweeps
much faster. Per-workspace clients are minted serially up front (the account token
exchange isn't safe to call concurrently) and then handed to the worker threads. Rows
are assembled in workspace order, so the CSV is deterministic and diffable across
runs; metastore-global UC securables (catalogs/schemas/tables and account-level
securables) are de-duplicated so they appear — and transfer — once, not once per
workspace. Per-workspace failures are isolated. Within a workspace, the WSFS walk
keeps using `wsfs_workers`.

Total in-flight API calls scale as `workspace_workers` × `wsfs_workers`, so raising
both on a large account drives real load against the account console and per-workspace
APIs. The Databricks SDK retries throttling (429/503) with backoff, but if you see
truncated inventories, dial the workers back. Start conservative (e.g. 4 × 8) and
raise as the account tolerates.

## Safety

- `inventory` performs **no writes**.
- `transfer` is **dry-run by default**; `execute=true` is required to mutate.
- Idempotent — objects already owned by the target group are skipped.
- Each object is applied independently; one failure does not abort the run.
- Ownership changes are hard to reverse in bulk — the `current_owner` column is your
  record of the prior owner if you need to revert.

## Known limitations

- **UC identifiers containing a literal dot.** A catalog/schema/table/volume/function
  whose backtick-quoted name contains a `.` (e.g. a table named `` `weird.name` ``) is
  stored as a dot-joined `full_name` and re-split on `.` at transfer time, so the
  generated `ALTER … OWNER` targets the wrong (nonexistent) object and that row fails
  rather than reassigning. Such rows show an error in the `result` column; reassign
  those objects manually. All other identifiers are backtick-escaped and safe.

## License

See [LICENSE](LICENSE).
