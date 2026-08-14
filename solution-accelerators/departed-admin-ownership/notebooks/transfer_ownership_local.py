# Databricks notebook source
# MAGIC %md
# MAGIC # Transfer Ownership from Departed Admins — workspace-local
# MAGIC
# MAGIC A **workspace-local** variant of `transfer_ownership.py` for environments where
# MAGIC workspace compute **cannot reach the account console**. It uses only the
# MAGIC ambient `WorkspaceClient` (no `AccountClient`, no account SP), so everything it
# MAGIC touches is what **this** workspace can see. Deploy the bundle into **each**
# MAGIC workspace and run it there.
# MAGIC
# MAGIC **Two phases, dry-run first:**
# MAGIC 1. `inventory` — read-only. Writes a Delta table of everything in this workspace
# MAGIC    owned by the departed admins. Touches nothing.
# MAGIC 2. `transfer` — reads the reviewed table and reassigns ownership. Defaults to
# MAGIC    **dry-run**; set `execute = true` to apply.
# MAGIC
# MAGIC **Requirements**
# MAGIC - Run on a **Unity Catalog-enabled** cluster (for the `information_schema` crawl).
# MAGIC - The run identity must be a **workspace admin** (and metastore admin for full UC
# MAGIC   coverage) — otherwise the crawl only sees objects that identity can read. The
# MAGIC   bundle pins `run_as` to a per-workspace admin service principal for this reason.
# MAGIC - The **target group** must exist in this workspace (account group synced in, or a
# MAGIC   workspace-local group).
# MAGIC
# MAGIC **Restricted catalogs:** `information_schema` only exposes catalogs bound to this
# MAGIC workspace, so a catalog restricted to other workspaces simply never appears here —
# MAGIC no special handling needed. Run the job in each workspace to cover its catalogs.

# COMMAND ----------

# MAGIC %pip install --quiet 'databricks-sdk>=0.44.0'
# dbutils.library.restartPython()  # uncomment if the runtime ships an older SDK

# COMMAND ----------

dbutils.widgets.text("departed_admins", "", "Departed admins (comma-separated emails)")
dbutils.widgets.text("target_group", "", "Target group (in this workspace)")

dbutils.widgets.dropdown("phase", "inventory", ["inventory", "transfer"], "Phase")
dbutils.widgets.dropdown("execute", "false", ["false", "true"], "Execute (transfer only)")

dbutils.widgets.dropdown("scope_uc", "true", ["true", "false"], "Scope: Unity Catalog")
dbutils.widgets.dropdown("scope_ws", "true", ["true", "false"], "Scope: Workspace objects")
dbutils.widgets.dropdown(
    "scope_wsfs", "false", ["true", "false"], "Scope: Workspace files (home trees)"
)
dbutils.widgets.dropdown(
    "scope_run_as", "false", ["true", "false"], "Scope: Job run_as reassignment"
)
dbutils.widgets.text(
    "run_as_sp", "", "run_as target: service principal application id (this workspace)"
)
dbutils.widgets.dropdown(
    "grant_run_as_sp_perms",
    "false",
    ["true", "false"],
    "Grant SP CAN_MANAGE on reassigned jobs (execute step)",
)
dbutils.widgets.text("wsfs_max_depth", "0", "WSFS max depth (0 = unlimited)")
dbutils.widgets.text("wsfs_workers", "8", "WSFS concurrent listers")
dbutils.widgets.text(
    "skip_catalogs", "__databricks_internal,system", "Catalogs to skip (comma-sep)"
)

dbutils.widgets.text("output_table", "", "Inventory Delta table (catalog.schema.table)")

# COMMAND ----------

import json
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("transfer_ownership_local")

G = lambda k: dbutils.widgets.get(k).strip()

departed_admins = sorted({e.strip().lower() for e in G("departed_admins").split(",") if e.strip()})
target_group = G("target_group")
phase = G("phase")
execute = G("execute") == "true"
scope_uc = G("scope_uc") == "true"
scope_ws = G("scope_ws") == "true"
scope_wsfs = G("scope_wsfs") == "true"
scope_run_as = G("scope_run_as") == "true"
run_as_sp = G("run_as_sp")
grant_run_as_sp_perms = G("grant_run_as_sp_perms") == "true"
wsfs_max_depth = int(G("wsfs_max_depth") or 0)
wsfs_workers = max(1, int(G("wsfs_workers") or 8))
skip_catalogs = {c.strip().lower() for c in G("skip_catalogs").split(",") if c.strip()}
output_table = G("output_table")

errors = []
if not departed_admins:
    errors.append("departed_admins is empty")
if not target_group:
    errors.append("target_group is empty")
if not output_table:
    errors.append("output_table is empty")
if not (scope_uc or scope_ws or scope_wsfs or scope_run_as):
    errors.append("all scopes disabled")
if scope_run_as and not run_as_sp:
    errors.append(
        "scope_run_as is on but run_as_sp is empty — provide the service principal "
        "application id that jobs in this workspace should run as"
    )
if errors:
    raise ValueError("Config errors:\n  - " + "\n  - ".join(errors))

print(
    "Phase          :",
    phase,
    "(EXECUTE)"
    if (phase == "transfer" and execute)
    else "(dry-run)"
    if phase == "transfer"
    else "",
)
print("Departed admins:", departed_admins)
print("Target group   :", target_group)
print(
    "Scope          :",
    "UC " if scope_uc else "",
    "WS " if scope_ws else "",
    "WSFS " if scope_wsfs else "",
    "RUN_AS" if scope_run_as else "",
)
if scope_run_as:
    print("run_as SP      :", run_as_sp)
print("Output table   :", output_table)

# COMMAND ----------

# MAGIC %md ## Client: this workspace only (ambient auth)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Ambient notebook auth — no AccountClient, no account SP, no account-console traffic.
w = WorkspaceClient()
try:
    this_ws_id = w.get_workspace_id()
except Exception as e:  # noqa: BLE001
    log.warning("get_workspace_id failed (%s); continuing without a workspace id", e)
    this_ws_id = None
this_host = w.config.host  # cloud-neutral
print(f"Workspace: {this_host} (id={this_ws_id})")

# COMMAND ----------

# MAGIC %md ## Resolve principals + verify target group (workspace-local SCIM)

# COMMAND ----------

admin_set = set(departed_admins)
by_key = {e: e for e in admin_set}  # any SCIM identifier -> canonical email
scim_resolved = set()

# Workspace-local SCIM: only identities provisioned into THIS workspace. That's exactly
# what we want — we only act on this workspace's objects.
for u in w.users.list(attributes="id,userName,displayName,emails"):
    uname = (u.user_name or "").lower()
    if uname in admin_set:
        scim_resolved.add(uname)
        emails = [e.value.lower() for e in (u.emails or []) if e.value]
        for k in {uname, str(u.id).lower(), (u.display_name or "").lower(), *emails}:
            if k:
                by_key[k] = uname

target_group_present = any(
    (g.display_name or "").lower() == target_group.lower()
    for g in w.groups.list(attributes="id,displayName")
)
print(
    "Target group '%s': %s"
    % (
        target_group,
        "FOUND in this workspace" if target_group_present else "NOT FOUND — fix before transfer",
    )
)
for e in sorted(admin_set):
    print(f"  {e:40s} {'✓ in workspace SCIM' if e in scim_resolved else '⚠ not provisioned here'}")
if phase == "transfer" and execute and not target_group_present:
    raise ValueError(f"Target group '{target_group}' not found in this workspace. Aborting.")


def match_owner(owner):
    if not owner:
        return None
    return by_key.get(owner.strip().lower())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 — Inventory (this workspace)
# MAGIC UC objects come from `system.information_schema` (metastore-local via Spark — only
# MAGIC the catalogs bound to this workspace). Account-level UC securables and workspace
# MAGIC objects come from the workspace SDK.

# COMMAND ----------

INVENTORY_COLUMNS = [
    "domain",
    "workspace_id",
    "workspace_host",
    "object_type",
    "securable_type",
    "full_name",
    "object_id",
    "current_owner",
    "matched_admin",
    "proposed_new_owner",
    "transfer_method",
    "extra",
]

rows = []


def emit(**kw):
    kw.setdefault("workspace_id", this_ws_id)
    kw.setdefault("workspace_host", this_host)
    kw.setdefault("object_id", "")
    kw.setdefault("proposed_new_owner", target_group)
    kw.setdefault("extra", "")
    # Coerce all values to strings so Spark infers a stable, all-string schema.
    rows.append({c: ("" if kw.get(c) is None else str(kw.get(c))) for c in INVENTORY_COLUMNS})


# COMMAND ----------

# ---- UC via Spark information_schema (this workspace's metastore view) ----
IS_QUERIES = [
    ("catalog", "system.information_schema.catalogs", ["catalog_name"], "catalog_owner"),
    (
        "schema",
        "system.information_schema.schemata",
        ["catalog_name", "schema_name"],
        "schema_owner",
    ),
    (
        "table",
        "system.information_schema.tables",
        ["table_catalog", "table_schema", "table_name"],
        "table_owner",
    ),
    (
        "volume",
        "system.information_schema.volumes",
        ["volume_catalog", "volume_schema", "volume_name"],
        "volume_owner",
    ),
    (
        "routine",
        "system.information_schema.routines",
        ["routine_catalog", "routine_schema", "routine_name"],
        "routine_owner",
    ),
]

if phase == "inventory" and scope_uc:
    admin_sql = ", ".join("'%s'" % a.replace("'", "''") for a in departed_admins)
    for label, view, name_cols, owner_col in IS_QUERIES:
        cols = ", ".join(name_cols)
        q = f"SELECT {cols}, {owner_col} AS owner FROM {view} WHERE lower({owner_col}) IN ({admin_sql})"
        try:
            df = spark.sql(q)
        except Exception as e:  # noqa: BLE001
            log.warning("information_schema %s failed: %s", label, e)
            continue
        for r in df.collect():
            parts = [r[c] for c in name_cols]
            catalog = parts[0] if parts else ""
            if catalog and catalog.lower() in skip_catalogs:
                continue
            matched = match_owner(r["owner"])
            if not matched:
                continue
            emit(
                domain="unity_catalog",
                object_type=label,
                securable_type=label.upper(),
                full_name=".".join(p for p in parts if p),
                current_owner=r["owner"],
                matched_admin=matched,
                transfer_method="sql_alter",
            )
    log.info("UC information_schema crawl: %d matched", len(rows))

# COMMAND ----------

# ---- Account-level UC securables via workspace SDK REST ----
# These are metastore-scoped but reachable from the workspace client. Only the ones
# this workspace/metastore can see are returned.
if phase == "inventory" and scope_uc:
    uc_rest_crawlers = [
        ("external_location", lambda: w.external_locations.list(), "name"),
        ("storage_credential", lambda: w.storage_credentials.list(), "name"),
        ("connection", lambda: w.connections.list(), "name"),
        ("share", lambda: w.shares.list_shares(), "name"),
        ("recipient", lambda: w.recipients.list(), "name"),
        ("registered_model", lambda: w.registered_models.list(), "full_name"),
    ]
    for kind, lister, name_attr in uc_rest_crawlers:
        try:
            objs = list(lister())
        except Exception as e:  # noqa: BLE001
            log.debug("UC REST %s skipped: %s", kind, e)
            continue
        for obj in objs:
            matched = match_owner(getattr(obj, "owner", None))
            if not matched:
                continue
            name = getattr(obj, name_attr, None) or getattr(obj, "name", "")
            emit(
                domain="unity_catalog",
                object_type=kind,
                securable_type=kind.upper(),
                full_name=name,
                current_owner=obj.owner,
                matched_admin=matched,
                transfer_method="uc_rest",
            )

# COMMAND ----------


# ---- Workspace objects via permissions API (this workspace) ----
def owner_from_permissions(request_object_type, request_object_id):
    try:
        pl = w.permissions.get(
            request_object_type=request_object_type, request_object_id=str(request_object_id)
        )
    except Exception as e:  # noqa: BLE001
        log.debug("permissions.get(%s/%s) failed: %s", request_object_type, request_object_id, e)
        return None
    for acl in pl.access_control_list or []:
        principal = acl.user_name or acl.group_name or acl.service_principal_name
        for perm in acl.all_permissions or []:
            if perm.permission_level and "IS_OWNER" in str(perm.permission_level):
                return principal
    return None


WS_SPECS = [
    (
        "job",
        "jobs",
        lambda: w.jobs.list(expand_tasks=False),
        "job_id",
        lambda o: o.settings.name if o.settings else str(o.job_id),
    ),
    (
        "pipeline",
        "pipelines",
        lambda: w.pipelines.list_pipelines(),
        "pipeline_id",
        lambda o: o.name or o.pipeline_id,
    ),
    (
        "cluster",
        "clusters",
        lambda: w.clusters.list(),
        "cluster_id",
        lambda o: o.cluster_name or o.cluster_id,
    ),
    ("warehouse", "sql/warehouses", lambda: w.warehouses.list(), "id", lambda o: o.name or o.id),
    (
        "serving_endpoint",
        "serving-endpoints",
        lambda: w.serving_endpoints.list(),
        "id",
        lambda o: o.name,
    ),
    (
        "experiment",
        "experiments",
        lambda: w.experiments.list_experiments(),
        "experiment_id",
        lambda o: o.name or o.experiment_id,
    ),
    (
        "registered_model_wsfs",
        "registered-models",
        lambda: w.model_registry.list_models(),
        "id",
        lambda o: o.name,
    ),
]

if phase == "inventory" and scope_ws:
    for label, api_type, lister, id_attr, name_fn in WS_SPECS:
        try:
            objs = list(lister())
        except Exception as e:  # noqa: BLE001
            log.debug("%s list failed: %s", label, e)
            continue
        for o in objs:
            oid = getattr(o, id_attr, None)
            if oid is None:
                continue
            owner = owner_from_permissions(api_type, oid)
            matched = match_owner(owner)
            if not matched:
                continue
            emit(
                domain="workspace",
                object_type=label,
                securable_type=api_type,
                full_name=str(name_fn(o)),
                object_id=str(oid),
                current_owner=owner,
                matched_admin=matched,
                transfer_method="permissions_api",
            )
    # Lakeview dashboards (separate API)
    try:
        for d in w.lakeview.list():
            owner = owner_from_permissions("dashboards", d.dashboard_id)
            matched = match_owner(owner)
            if matched:
                emit(
                    domain="workspace",
                    object_type="lakeview_dashboard",
                    securable_type="dashboards",
                    full_name=d.display_name or d.dashboard_id,
                    object_id=d.dashboard_id,
                    current_owner=owner,
                    matched_admin=matched,
                    transfer_method="permissions_api",
                )
    except Exception as e:  # noqa: BLE001
        log.debug("lakeview list failed: %s", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job `run_as` reassignment
# MAGIC `run_as` is who a job *executes as* — distinct from ownership. We match the
# MAGIC **effective** identity (`run_as_user_name`), which also catches jobs with no
# MAGIC explicit `run_as` set that still run as their departed-admin creator. The
# MAGIC transfer sets `run_as` to `run_as_sp` — the service principal for **this**
# MAGIC workspace. Requires `jobs.get()` per job for the full settings.

# COMMAND ----------

# A job can only run_as an SP that can manage it. This helper reads whether a given SP
# already holds a run-capable permission (CAN_MANAGE or IS_OWNER) on a job — used both
# for the inventory preflight flag and to skip redundant grants at execute time.
SP_RUN_CAPABLE = {"CAN_MANAGE", "IS_OWNER"}


def sp_job_permission_levels(job_id, sp_app_id):
    """Return the SP's permission levels (bare names) on a job, or [] if none."""
    try:
        pl = w.permissions.get(request_object_type="jobs", request_object_id=str(job_id))
    except Exception as e:  # noqa: BLE001
        log.debug("permissions.get(jobs/%s) failed: %s", job_id, e)
        return []
    for acl in pl.access_control_list or []:
        if acl.service_principal_name == sp_app_id:
            return [
                str(p.permission_level).replace("PermissionLevel.", "")
                for p in (acl.all_permissions or [])
            ]
    return []


if phase == "inventory" and scope_run_as:
    try:
        base_jobs = list(w.jobs.list(expand_tasks=False))
    except Exception as e:  # noqa: BLE001
        log.warning("jobs.list failed: %s", e)
        base_jobs = []
    for bj in base_jobs:
        try:
            job = w.jobs.get(bj.job_id)  # full Job carries run_as + run_as_user_name
        except Exception as e:  # noqa: BLE001
            log.debug("jobs.get(%s) failed: %s", bj.job_id, e)
            continue
        # Effective identity: catches explicit run_as AND creator-default.
        effective = job.run_as_user_name
        matched = match_owner(effective)
        if not matched:
            continue
        name = job.settings.name if job.settings else str(bj.job_id)
        # Preflight: does the target SP already have a run-capable permission?
        sp_levels = sp_job_permission_levels(bj.job_id, run_as_sp)
        sp_can_manage = bool(set(sp_levels) & SP_RUN_CAPABLE)
        extra = "SP_HAS_ACCESS" if sp_can_manage else "SP_NEEDS_GRANT"
        emit(
            domain="job_run_as",
            object_type="job_run_as",
            securable_type="jobs",
            full_name=str(name),
            object_id=str(bj.job_id),
            current_owner=effective,  # current effective run_as identity
            matched_admin=matched,
            proposed_new_owner=run_as_sp,  # this workspace's SP
            transfer_method="jobs_update_run_as",
            extra=extra,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workspace files / folders (home trees)
# MAGIC Files/notebooks/dirs/repos have **no owner** — "owned by a departed admin" means
# MAGIC the object lives under `/Users/<email>/` (or `/Repos/<email>/`). "Transfer"
# MAGIC grants the group **CAN_MANAGE** (additive). Paths used by an **active, recurring**
# MAGIC job are flagged in `extra` so you re-home them first.

# COMMAND ----------

from databricks.sdk.service.jobs import PauseStatus

WSFS_PERM_TYPE = {
    "NOTEBOOK": "notebooks",
    "DIRECTORY": "directories",
    "FILE": "files",
    "REPO": "repos",
    "DASHBOARD": "dashboards",
}
WSFS_NOISE_SEGMENTS = {
    ".venv",
    "venv",
    ".git",
    "__pycache__",
    "node_modules",
    ".ipynb_checkpoints",
    ".mypy_cache",
    ".pytest_cache",
    "site-packages",
}


def is_noise_path(path):
    return any(seg in WSFS_NOISE_SEGMENTS for seg in (path or "").split("/"))


def walk_wsfs(roots, max_depth, workers):
    """BFS walk of workspace paths, listing directories concurrently. Replaces the
    SDK's server-side recursive=True so we can cap depth (0 = unlimited; levels below a
    root) and fan out listings. Repos are leaves. Yields ObjectInfo."""
    from concurrent.futures import ThreadPoolExecutor

    def list_dir(path):
        try:
            return list(w.workspace.list(path))
        except Exception as e:  # noqa: BLE001
            log.debug("workspace.list(%s) failed: %s", path, e)
            return []

    frontier = [(r, 0) for r in roots]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        while frontier:
            results = list(pool.map(list_dir, [p for p, _ in frontier]))
            nxt = []
            for (_parent, depth), objs in zip(frontier, results, strict=False):
                for o in objs:
                    if is_noise_path(o.path or ""):
                        continue
                    yield o
                    otype = o.object_type.value if o.object_type else ""
                    if otype == "DIRECTORY":
                        if max_depth and depth + 1 >= max_depth:
                            continue
                        nxt.append((o.path, depth + 1))
            frontier = nxt


def norm_path(p):
    if not p:
        return ""
    p = p.strip()
    if p.startswith("/Workspace/"):
        p = p[len("/Workspace") :]
    elif p == "/Workspace":
        p = "/"
    return p.rstrip("/") or "/"


def task_paths(t):
    if getattr(t, "notebook_task", None) and t.notebook_task.notebook_path:
        yield t.notebook_task.notebook_path
    if getattr(t, "spark_python_task", None) and t.spark_python_task.python_file:
        yield t.spark_python_task.python_file
    sql = getattr(t, "sql_task", None)
    if sql and getattr(sql, "file", None) and getattr(sql.file, "path", None):
        yield sql.file.path
    dbt = getattr(t, "dbt_task", None)
    if dbt and getattr(dbt, "project_directory", None):
        yield dbt.project_directory


def active_recurring_job_paths():
    def unpaused(o):
        return o is not None and getattr(o, "pause_status", None) != PauseStatus.PAUSED

    out = {}
    try:
        job_list = list(w.jobs.list(expand_tasks=True))
    except Exception as e:  # noqa: BLE001
        log.warning("jobs.list for active-path map failed: %s", e)
        return out
    for j in job_list:
        js = j.settings
        if not js:
            continue
        if not (unpaused(js.schedule) or unpaused(js.continuous) or unpaused(js.trigger)):
            continue
        name = js.name or str(j.job_id)
        for t in js.tasks or []:
            for p in task_paths(t):
                out.setdefault(norm_path(p), []).append(name)
    return out


def path_is_active(norm, active_map):
    hits = []
    for used, names in active_map.items():
        if norm == used or used.startswith(norm + "/"):
            hits.extend(names)
    return sorted(set(hits))


if phase == "inventory" and scope_wsfs:
    active_map = active_recurring_job_paths()
    log.info("active-recurring-job path map: %d referenced path(s)", len(active_map))
    for email in departed_admins:
        roots = []
        for root in (f"/Users/{email}", f"/Repos/{email}"):
            try:
                w.workspace.get_status(root)
                roots.append(root)
            except Exception:  # noqa: BLE001
                continue
        if not roots:
            continue
        for o in walk_wsfs(roots, wsfs_max_depth, wsfs_workers):
            otype = o.object_type.value if o.object_type else ""
            if otype in ("LIBRARY", ""):
                continue
            active_jobs = path_is_active(norm_path(o.path), active_map)
            extra = ""
            if active_jobs:
                extra = "ACTIVE_JOB_DEPENDENCY: " + "; ".join(active_jobs[:5])
                if len(active_jobs) > 5:
                    extra += f" (+{len(active_jobs) - 5} more)"
            emit(
                domain="workspace_files",
                object_type=otype.lower(),
                securable_type=WSFS_PERM_TYPE.get(otype, ""),
                full_name=o.path,
                object_id=str(o.object_id or ""),
                current_owner=email,
                matched_admin=email,
                transfer_method="wsfs_permissions",
                extra=extra,
            )

# COMMAND ----------

# MAGIC %md ### Write / show inventory

# COMMAND ----------

if phase == "inventory":
    from pyspark.sql import Row

    if rows:
        inv_df = spark.createDataFrame([Row(**r) for r in rows])
    else:
        empty_schema = ", ".join(f"{c} string" for c in INVENTORY_COLUMNS)
        inv_df = spark.createDataFrame([], empty_schema)
    inv_df = inv_df.select(*INVENTORY_COLUMNS)
    (inv_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table))
    print(f"Wrote {inv_df.count()} rows to {output_table}")
    display(
        spark.sql(
            f"SELECT domain, object_type, matched_admin, count(*) n "
            f"FROM {output_table} GROUP BY 1,2,3 ORDER BY 1,2,3"
        )
    )

    # run_as preflight summary: how many reassigned jobs need an SP grant.
    if scope_run_as:
        needs = sum(
            1
            for r in rows
            if r["transfer_method"] == "jobs_update_run_as" and r["extra"] == "SP_NEEDS_GRANT"
        )
        has = sum(
            1
            for r in rows
            if r["transfer_method"] == "jobs_update_run_as" and r["extra"] == "SP_HAS_ACCESS"
        )
        print(
            f"\nrun_as preflight: {has} job(s) already grantable by their target SP, "
            f"{needs} need a CAN_MANAGE grant."
        )
        if needs:
            print(
                "  -> Set grant_run_as_sp_perms=true (with execute=true) to grant "
                "CAN_MANAGE as part of the transfer, or grant it out-of-band first. "
                "Without the grant, those jobs will FAIL at run time after reassignment."
            )

# COMMAND ----------

if phase == "inventory":
    displayHTML(
        "<b>Review the table above.</b> "
        "When ready, set <code>phase = transfer</code> "
        "(and <code>execute = true</code> to apply) and re-run."
    )
    dbutils.notebook.exit(
        json.dumps({"phase": "inventory", "rows": len(rows), "table": output_table})
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2 — Transfer
# MAGIC Reads the reviewed inventory table and reassigns ownership in **this** workspace.
# MAGIC Dry-run unless `execute = true`. Idempotent: rows already owned by the target
# MAGIC group are skipped.

# COMMAND ----------

from databricks.sdk.service import iam, jobs

UC_ALTER = {
    "catalog": "ALTER CATALOG {name} OWNER TO `{grp}`",
    "schema": "ALTER SCHEMA {name} OWNER TO `{grp}`",
    "table": "ALTER TABLE {name} OWNER TO `{grp}`",
    "volume": "ALTER VOLUME {name} OWNER TO `{grp}`",
    "routine": "ALTER FUNCTION {name} OWNER TO `{grp}`",
}
UC_REST_PATH = {
    "external_location": "external-locations",
    "storage_credential": "storage-credentials",
    "connection": "connections",
    "share": "shares",
    "recipient": "recipients",
    "registered_model": "models",
}


def _bt(identifier):
    """Backtick-quote an identifier, doubling any embedded backtick."""
    return "`{}`".format(str(identifier).replace("`", "``"))


def do_transfer(row):
    grp = row["proposed_new_owner"] or target_group
    method = row["transfer_method"]
    # Idempotency skip does not apply to WSFS (current_owner is the home-tree admin, not
    # an ACL principal) or run_as (target is a per-workspace SP, not a group).
    if (
        method not in ("wsfs_permissions", "jobs_update_run_as")
        and (row["current_owner"] or "").lower() == grp.lower()
    ):
        return f"SKIP already owned by {grp}"

    if method == "sql_alter":
        tmpl = UC_ALTER.get(row["object_type"])
        if not tmpl:
            return f"SKIP (no SQL template for {row['object_type']})"
        qualified = ".".join(_bt(p) for p in row["full_name"].split("."))
        sql = tmpl.format(name=qualified, grp=grp.replace("`", "``"))
        if not execute:
            return f"DRY-RUN {sql}"
        spark.sql(sql)
        return f"OK {sql}"

    if method == "uc_rest":
        seg = UC_REST_PATH.get(row["object_type"])
        if seg is None:
            return f"SKIP (no REST path for {row['object_type']})"
        import urllib.parse

        name = urllib.parse.quote(row["full_name"], safe="")
        path = f"/api/2.1/unity-catalog/{seg}/{name}"
        if not execute:
            return f"DRY-RUN PATCH {path} owner -> {grp}"
        w.api_client.do("PATCH", path, body={"owner": grp})
        return f"OK PATCH {path} owner -> {grp}"

    if method == "permissions_api":
        if not execute:
            return f"DRY-RUN set {grp} IS_OWNER on {row['securable_type']}/{row['object_id']}"
        acl = iam.AccessControlRequest(
            group_name=grp, permission_level=iam.PermissionLevel.IS_OWNER
        )
        w.permissions.update(
            request_object_type=row["securable_type"],
            request_object_id=str(row["object_id"]),
            access_control_list=[acl],
        )
        return f"OK set {grp} IS_OWNER on {row['securable_type']}/{row['object_id']}"

    if method == "wsfs_permissions":
        # No owner on WSFS — grant the group CAN_MANAGE (additive) so access survives
        # the user's deletion. Directories cascade to their contents.
        api_type, oid = row["securable_type"], row["object_id"]
        if not api_type or not oid:
            return f"SKIP (no permissions target for {row['object_type']} {row['full_name']})"
        active = str(row.get("extra", "")).startswith("ACTIVE_JOB_DEPENDENCY")
        warn = " [ACTIVE JOB DEP — verify before deleting user]" if active else ""
        if not execute:
            return f"DRY-RUN grant {grp} CAN_MANAGE on {api_type}/{oid} ({row['full_name']}){warn}"
        acl = iam.AccessControlRequest(
            group_name=grp, permission_level=iam.PermissionLevel.CAN_MANAGE
        )
        w.permissions.update(
            request_object_type=api_type, request_object_id=str(oid), access_control_list=[acl]
        )
        return f"OK grant {grp} CAN_MANAGE on {api_type}/{oid}{warn}"

    if method == "jobs_update_run_as":
        # Reassign run_as to this workspace's SP (grp holds the SP application id).
        # jobs.update is a partial update: only run_as is replaced; tasks/schedule are
        # left intact. A job can only run_as an SP that can manage it, so optionally
        # grant CAN_MANAGE first (gated by grant_run_as_sp_perms).
        job_id = int(row["object_id"])
        needs_grant = row.get("extra") == "SP_NEEDS_GRANT"
        will_grant = grant_run_as_sp_perms and needs_grant
        if not execute:
            grant_note = (
                " + grant CAN_MANAGE"
                if will_grant
                else (
                    " [SP LACKS ACCESS — will fail at run time; set grant_run_as_sp_perms=true]"
                    if needs_grant
                    else ""
                )
            )
            return (
                f"DRY-RUN set run_as -> SP {grp} on job {job_id} "
                f"({row['full_name']}) [was {row['current_owner']}]{grant_note}"
            )
        granted = ""
        if will_grant:
            # Additive grant — preserves other principals' ACLs on the job.
            w.permissions.update(
                request_object_type="jobs",
                request_object_id=str(job_id),
                access_control_list=[
                    iam.AccessControlRequest(
                        service_principal_name=grp,
                        permission_level=iam.PermissionLevel.CAN_MANAGE,
                    )
                ],
            )
            granted = " (granted CAN_MANAGE)"
        w.jobs.update(
            job_id=job_id,
            new_settings=jobs.JobSettings(run_as=jobs.JobRunAs(service_principal_name=grp)),
        )
        warn = (
            " [WARN: SP lacked access and grant_run_as_sp_perms=false — job may fail]"
            if (needs_grant and not will_grant)
            else ""
        )
        return f"OK set run_as -> SP {grp} on job {job_id}{granted}{warn}"

    return f"SKIP (unknown method {method})"


# COMMAND ----------

if phase == "transfer":
    all_rows = [r.asDict() for r in spark.table(output_table).collect()]
    log.info(
        "%s %d row(s) from %s",
        "EXECUTING" if execute else "DRY-RUN over",
        len(all_rows),
        output_table,
    )

    results = []
    ok = skip = err = 0
    for r in all_rows:
        try:
            res = do_transfer(r)
            if res.startswith(("OK", "DRY-RUN")):
                ok += 1
            else:
                skip += 1
        except Exception as e:  # noqa: BLE001
            res = f"ERR {e}"
            err += 1
        results.append({**r, "result": res})
        log.info("%s | %s %s", res, r["object_type"], r["full_name"])

    # Guard the empty case: spark.createDataFrame([]) can't infer a schema and raises.
    # An empty inventory just means "nothing to transfer" — report it cleanly.
    if results:
        display(
            spark.createDataFrame(results).select(
                "result", "domain", "object_type", "full_name", "current_owner",
                "proposed_new_owner",
            )
        )
    else:
        print("Inventory table is empty — nothing to transfer.")
    print(f"Done. {'(dry-run) ' if not execute else ''}ok/dry={ok} skip={skip} err={err}")
    if not execute:
        print("Set execute = true and re-run to apply.")
    dbutils.notebook.exit(
        json.dumps({"phase": "transfer", "execute": execute, "ok": ok, "skip": skip, "err": err})
    )
