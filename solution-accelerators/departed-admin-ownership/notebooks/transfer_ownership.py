# Databricks notebook source
# MAGIC %md
# MAGIC # Transfer Ownership from Departed Admins
# MAGIC
# MAGIC Finds every **Unity Catalog** and **workspace** object owned by a list of
# MAGIC departed admins across **all workspaces in the account**, and (as a separate,
# MAGIC reviewed step) transfers ownership to an admin group.
# MAGIC
# MAGIC **Two phases, dry-run first:**
# MAGIC 1. `inventory` — read-only. Writes a Delta table of everything owned by the
# MAGIC    departed admins. Touches nothing.
# MAGIC 2. `transfer` — reads the reviewed table and reassigns ownership. Defaults to
# MAGIC    **dry-run**; set `execute = true` to apply.
# MAGIC
# MAGIC **Requirements**
# MAGIC - Run on a cluster with **Unity Catalog** enabled (for the `system.information_schema` crawl).
# MAGIC - An **account-admin service principal** whose `client_id` / `client_secret`
# MAGIC   live in a **Databricks secret scope** (for the cross-workspace sweep).
# MAGIC - The **target group must be an account-level group** synced into the metastore.

# COMMAND ----------

# MAGIC %pip install --quiet 'databricks-sdk>=0.44.0'
# dbutils.library.restartPython()  # uncomment if the runtime ships an older SDK

# COMMAND ----------

dbutils.widgets.text("secret_scope", "", "Secret scope (account SP)")
dbutils.widgets.text("secret_key_client_id", "account_sp_client_id", "Secret key: client_id")
dbutils.widgets.text(
    "secret_key_client_secret", "account_sp_client_secret", "Secret key: client_secret"
)
dbutils.widgets.text(
    "account_host",
    "https://accounts.cloud.databricks.com",
    "Account console host (AWS default; Azure: accounts.azuredatabricks.net, GCP: accounts.gcp.databricks.com)",
)
dbutils.widgets.text("account_id", "", "Account ID (UUID)")

dbutils.widgets.text("departed_admins", "", "Departed admins (comma-separated emails)")
dbutils.widgets.text("target_group", "", "Target account group")

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
dbutils.widgets.dropdown(
    "grant_run_as_sp_perms",
    "false",
    ["true", "false"],
    "Grant SP CAN_MANAGE on reassigned jobs (execute step)",
)
dbutils.widgets.text("wsfs_max_depth", "0", "WSFS max depth (0 = unlimited)")
dbutils.widgets.text("wsfs_workers", "8", "WSFS concurrent listers")
dbutils.widgets.text("workspace_ids", "", "Workspace IDs to sweep (comma-sep, blank=all)")
dbutils.widgets.text(
    "run_as_sp_map",
    "",
    'Per-workspace run_as SP, JSON: {"<workspace_id>": "<sp_application_id>"}',
)
dbutils.widgets.text(
    "skip_catalogs", "__databricks_internal,system", "Catalogs to skip (comma-sep)"
)

dbutils.widgets.text("output_table", "", "Inventory Delta table (catalog.schema.table)")

# COMMAND ----------

import json
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("transfer_ownership")

G = lambda k: dbutils.widgets.get(k).strip()

secret_scope = G("secret_scope")
account_host = G("account_host")
account_id = G("account_id")
departed_admins = sorted({e.strip().lower() for e in G("departed_admins").split(",") if e.strip()})
target_group = G("target_group")
phase = G("phase")
execute = G("execute") == "true"
scope_uc = G("scope_uc") == "true"
scope_ws = G("scope_ws") == "true"
scope_wsfs = G("scope_wsfs") == "true"
scope_run_as = G("scope_run_as") == "true"
grant_run_as_sp_perms = G("grant_run_as_sp_perms") == "true"
wsfs_max_depth = int(G("wsfs_max_depth") or 0)
wsfs_workers = max(1, int(G("wsfs_workers") or 8))
workspace_ids = {int(x) for x in G("workspace_ids").split(",") if x.strip()}
skip_catalogs = {c.strip().lower() for c in G("skip_catalogs").split(",") if c.strip()}
output_table = G("output_table")

# Per-workspace run_as SP map: {workspace_id -> service principal application id}.
# Keys are normalized to str so JSON int/str keys both work.
import json as _json  # noqa: E402  (also imported below for the rest of the notebook)

run_as_sp_map = {}
_raw_sp_map = G("run_as_sp_map")
if _raw_sp_map:
    try:
        run_as_sp_map = {str(k): str(v).strip() for k, v in _json.loads(_raw_sp_map).items()}
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"run_as_sp_map is not valid JSON: {e}")

# Stamp every row of this run with a shared identity so the inventory table can retain
# the history of many runs (append, not overwrite). RUN_ID is sortable + unique, so the
# transfer phase can pick the latest run with max(run_id); RUN_TIMESTAMP is the readable
# UTC instant. Both are derived from the same moment.
import uuid  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

_run_dt = datetime.now(timezone.utc)
RUN_TIMESTAMP = _run_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{_run_dt.microsecond // 1000:03d}Z"
RUN_ID = _run_dt.strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]

errors = []
if not departed_admins:
    errors.append("departed_admins is empty")
if not target_group:
    errors.append("target_group is empty")
if not output_table:
    errors.append("output_table is empty")
if not (scope_uc or scope_ws or scope_wsfs or scope_run_as):
    errors.append("all scopes disabled")
if (scope_ws or scope_wsfs or scope_run_as) and not secret_scope:
    errors.append(
        "workspace object/file/run_as scope needs an account SP (set secret_scope), "
        "or disable those scopes to crawl UC only"
    )
if scope_run_as and not run_as_sp_map:
    errors.append(
        "scope_run_as is on but run_as_sp_map is empty — provide a per-workspace SP "
        'map, e.g. {"1234567890": "sp-app-id"}'
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
    print("run_as SP map  :", {k: v for k, v in run_as_sp_map.items()})
print("Output table   :", output_table)
print("Run id         :", RUN_ID, "(", RUN_TIMESTAMP, ")")

# COMMAND ----------

# MAGIC %md ## Clients: account SP + per-workspace

# COMMAND ----------

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.provisioning import WorkspaceStatus

account_client = None
if secret_scope:
    client_id = dbutils.secrets.get(secret_scope, G("secret_key_client_id"))
    client_secret = dbutils.secrets.get(secret_scope, G("secret_key_client_secret"))
    account_client = AccountClient(
        host=account_host,
        account_id=account_id or None,
        client_id=client_id,
        client_secret=client_secret,
    )
    print("AccountClient ready for account", account_client.config.account_id)
else:
    print("No account SP configured — UC-only mode using the notebook's identity.")

# Current-workspace client (ambient notebook auth) — used for UC REST + Spark host.
w_here = WorkspaceClient()


def iter_workspaces():
    """Yield (workspace_id, Workspace) for in-scope RUNNING workspaces.
    Requires an account client; otherwise yields nothing (UC-only mode)."""
    if account_client is None:
        return
    for ws in account_client.workspaces.list():
        if workspace_ids and ws.workspace_id not in workspace_ids:
            continue
        if ws.workspace_status not in (WorkspaceStatus.RUNNING, None):
            log.info("skip workspace %s (status=%s)", ws.workspace_id, ws.workspace_status)
            continue
        if not ws.deployment_name:
            log.warning("workspace %s has no deployment_name; skipping", ws.workspace_id)
            continue
        yield ws.workspace_id, ws


def ws_client(ws):
    return account_client.get_workspace_client(ws)


# COMMAND ----------

# MAGIC %md ## Resolve principals + verify target group

# COMMAND ----------

admin_set = set(departed_admins)
by_key = {e: e for e in admin_set}  # any SCIM identifier -> canonical email
target_group_present = False

if account_client is not None:
    for u in account_client.users.list(attributes="id,userName,displayName,emails"):
        uname = (u.user_name or "").lower()
        if uname in admin_set:
            emails = [e.value.lower() for e in (u.emails or []) if e.value]
            for k in {uname, str(u.id).lower(), (u.display_name or "").lower(), *emails}:
                if k:
                    by_key[k] = uname
    for g in account_client.groups.list(attributes="id,displayName"):
        if (g.display_name or "").lower() == target_group.lower():
            target_group_present = True
            break
    print(
        "Target group '%s': %s"
        % (
            target_group,
            "FOUND at account level" if target_group_present else "NOT FOUND — fix before transfer",
        )
    )
    if phase == "transfer" and execute and not target_group_present:
        raise ValueError(f"Target group '{target_group}' not found at account level. Aborting.")
else:
    # UC-only mode: trust the provided emails as-is.
    print("Skipping SCIM resolution (no account client).")


def match_owner(owner):
    if not owner:
        return None
    return by_key.get(owner.strip().lower())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 — Inventory
# MAGIC UC objects come from `system.information_schema` (fast, metastore-wide via Spark).
# MAGIC Account-level UC securables and workspace objects come from the SDK.

# COMMAND ----------

INVENTORY_COLUMNS = [
    "run_id",
    "run_timestamp",
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
    # Stamp this run's identity on every row (append-mode table retains run history).
    kw.setdefault("run_id", RUN_ID)
    kw.setdefault("run_timestamp", RUN_TIMESTAMP)
    kw.setdefault("workspace_id", None)
    kw.setdefault("workspace_host", "")
    kw.setdefault("object_id", "")
    kw.setdefault("proposed_new_owner", target_group)
    kw.setdefault("extra", "")
    # Coerce all values to strings so Spark infers a stable, all-string schema
    # regardless of which columns are populated per row (e.g. workspace_id is
    # None for UC rows, int for workspace rows).
    rows.append({c: ("" if kw.get(c) is None else str(kw.get(c))) for c in INVENTORY_COLUMNS})


# COMMAND ----------

# ---- UC via Spark information_schema (metastore-wide) ----
# (label, information_schema view, name columns, owner column, transfer method)
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

# ---- Account-level UC securables via SDK REST (once) ----
if phase == "inventory" and scope_uc:
    uc_rest_crawlers = [
        ("external_location", lambda w: w.external_locations.list(), "name"),
        ("storage_credential", lambda w: w.storage_credentials.list(), "name"),
        ("connection", lambda w: w.connections.list(), "name"),
        ("share", lambda w: w.shares.list_shares(), "name"),
        ("recipient", lambda w: w.recipients.list(), "name"),
        ("registered_model", lambda w: w.registered_models.list(), "full_name"),
    ]
    for kind, lister, name_attr in uc_rest_crawlers:
        try:
            objs = list(lister(w_here))
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


# ---- Workspace objects per workspace via permissions API ----
def owner_from_permissions(w, request_object_type, request_object_id):
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
        lambda w: w.jobs.list(expand_tasks=False),
        "job_id",
        lambda o: o.settings.name if o.settings else str(o.job_id),
    ),
    (
        "pipeline",
        "pipelines",
        lambda w: w.pipelines.list_pipelines(),
        "pipeline_id",
        lambda o: o.name or o.pipeline_id,
    ),
    # clusters handled separately below — all-purpose only, capturing explicit
    # non-owner ACL grants in addition to IS_OWNER.
    ("warehouse", "sql/warehouses", lambda w: w.warehouses.list(), "id", lambda o: o.name or o.id),
    (
        "serving_endpoint",
        "serving-endpoints",
        lambda w: w.serving_endpoints.list(),
        "id",
        lambda o: o.name,
    ),
    (
        "experiment",
        "experiments",
        lambda w: w.experiments.list_experiments(),
        "experiment_id",
        lambda o: o.name or o.experiment_id,
    ),
    (
        "registered_model_wsfs",
        "registered-models",
        lambda w: w.model_registry.list_models(),
        "id",
        lambda o: o.name,
    ),
]

# --- All-purpose cluster helpers (used inside the workspace loop below) ---
# Clusters have NO owner — the permissions API exposes only CAN_ATTACH_TO / CAN_RESTART /
# CAN_MANAGE (no IS_OWNER level, unlike jobs or warehouses). A departed admin's tie to a
# cluster is therefore always an explicit ACL grant; we inventory every all-purpose
# cluster (cluster_source UI/API) on which the admin holds an explicit, non-inherited
# grant and the transfer phase simply revokes it (never handed to the group).
CLUSTER_PERM_RANK = {"CAN_MANAGE": 3, "CAN_RESTART": 2, "CAN_ATTACH_TO": 1}
ALL_PURPOSE_CLUSTER_SOURCES = {"UI", "API"}


def perm_value(level):
    """Normalize a PermissionLevel enum (or str) to its bare value, e.g. CAN_MANAGE."""
    return str(getattr(level, "value", level) or "")


def strongest_cluster_level(levels):
    """Highest-privilege level among a principal's direct grants (one effective grant
    per cluster; collapse any list to a single level for the PUT)."""
    return max(levels, key=lambda lvl: CLUSTER_PERM_RANK.get(perm_value(lvl), 0))


def acl_direct_grants(w, cluster_id):
    """principal -> list of that principal's DIRECT (non-inherited) permission levels
    on the cluster. Inherited/group-derived grants are excluded."""
    try:
        pl = w.permissions.get(request_object_type="clusters", request_object_id=str(cluster_id))
    except Exception as e:  # noqa: BLE001
        log.debug("permissions.get(clusters/%s) failed: %s", cluster_id, e)
        return {}
    grants = {}
    for acl in pl.access_control_list or []:
        principal = acl.user_name or acl.group_name or acl.service_principal_name
        if not principal:
            continue
        direct = [
            p.permission_level
            for p in (acl.all_permissions or [])
            if p.permission_level and not p.inherited
        ]
        if direct:
            grants[principal] = direct
    return grants


def emit_cluster_grants(w, ws_id, host):
    """Inventory all-purpose clusters in this workspace using the already-built client."""
    try:
        clusters = list(w.clusters.list())
    except Exception as e:  # noqa: BLE001
        log.debug("clusters.list failed on %s: %s", ws_id, e)
        return
    for o in clusters:
        if perm_value(getattr(o, "cluster_source", None)) not in ALL_PURPOSE_CLUSTER_SOURCES:
            continue
        cid = getattr(o, "cluster_id", None)
        if not cid:
            continue
        name = o.cluster_name or cid
        for principal, levels in acl_direct_grants(w, cid).items():
            matched = match_owner(principal)
            if not matched:
                continue
            held = perm_value(strongest_cluster_level(levels))
            emit(
                domain="workspace",
                workspace_id=ws_id,
                workspace_host=host,
                object_type="cluster_acl",
                securable_type="clusters",
                full_name=str(name),
                object_id=str(cid),
                current_owner=principal,
                matched_admin=matched,
                # Clusters are revoked, never transferred — no "new owner".
                proposed_new_owner="",
                transfer_method="cluster_revoke",
                extra=f"EXPLICIT_PERMISSION={held}",
            )


if phase == "inventory" and scope_ws:
    if account_client is None:
        log.warning("scope_ws requested but no account client — skipping workspace objects.")
    for ws_id, ws in iter_workspaces():
        try:
            w = ws_client(ws)
        except Exception as e:  # noqa: BLE001
            log.warning("cannot connect to workspace %s: %s", ws_id, e)
            continue
        # Cloud-neutral: take the host from the resolved client, not a hardcoded
        # AWS suffix — works on AWS / Azure / GCP.
        host = w.config.host
        log.info("Workspace crawl: %s (%s)", ws_id, host)
        for label, api_type, lister, id_attr, name_fn in WS_SPECS:
            try:
                objs = list(lister(w))
            except Exception as e:  # noqa: BLE001
                log.debug("%s list failed on %s: %s", label, ws_id, e)
                continue
            for o in objs:
                oid = getattr(o, id_attr, None)
                if oid is None:
                    continue
                owner = owner_from_permissions(w, api_type, oid)
                matched = match_owner(owner)
                if not matched:
                    continue
                emit(
                    domain="workspace",
                    workspace_id=ws_id,
                    workspace_host=host,
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
                owner = owner_from_permissions(w, "dashboards", d.dashboard_id)
                matched = match_owner(owner)
                if matched:
                    emit(
                        domain="workspace",
                        workspace_id=ws_id,
                        workspace_host=host,
                        object_type="lakeview_dashboard",
                        securable_type="dashboards",
                        full_name=d.display_name or d.dashboard_id,
                        object_id=d.dashboard_id,
                        current_owner=owner,
                        matched_admin=matched,
                        transfer_method="permissions_api",
                    )
        except Exception as e:  # noqa: BLE001
            log.debug("lakeview list failed on %s: %s", ws_id, e)

        # All-purpose clusters — reuse this workspace's client (no second crawl).
        emit_cluster_grants(w, ws_id, host)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job `run_as` reassignment
# MAGIC `run_as` is who a job *executes as* — distinct from ownership. We match the
# MAGIC **effective** identity (`run_as_user_name`), which also catches jobs with no
# MAGIC explicit `run_as` set that still run as their departed-admin creator. The
# MAGIC transfer sets `run_as` to the **per-workspace service principal** from
# MAGIC `run_as_sp_map`. Requires `jobs.get()` per job for the full settings.

# COMMAND ----------

# A job can only run_as an SP that can manage it. This helper reads whether a given
# SP already holds a run-capable permission (CAN_MANAGE or IS_OWNER) on a job — used
# both for the inventory preflight flag and to skip redundant grants at execute time.
SP_RUN_CAPABLE = {"CAN_MANAGE", "IS_OWNER"}


def sp_job_permission_levels(w, job_id, sp_app_id):
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
    if account_client is None:
        log.warning("scope_run_as requested but no account client — skipping.")
    for ws_id, ws in iter_workspaces():
        target_sp = run_as_sp_map.get(str(ws_id))
        if not target_sp:
            log.warning("workspace %s has no entry in run_as_sp_map — skipping run_as here", ws_id)
            continue
        try:
            w = ws_client(ws)
        except Exception as e:  # noqa: BLE001
            log.warning("cannot connect to workspace %s: %s", ws_id, e)
            continue
        host = w.config.host
        log.info("run_as crawl: %s (%s) -> SP %s", ws_id, host, target_sp)
        try:
            base_jobs = list(w.jobs.list(expand_tasks=False))
        except Exception as e:  # noqa: BLE001
            log.warning("jobs.list failed on %s: %s", ws_id, e)
            continue
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
            sp_levels = sp_job_permission_levels(w, bj.job_id, target_sp)
            sp_can_manage = bool(set(sp_levels) & SP_RUN_CAPABLE)
            extra = "SP_HAS_ACCESS" if sp_can_manage else "SP_NEEDS_GRANT"
            emit(
                domain="job_run_as",
                workspace_id=ws_id,
                workspace_host=host,
                object_type="job_run_as",
                securable_type="jobs",
                full_name=str(name),
                object_id=str(bj.job_id),
                current_owner=effective,  # current effective run_as identity
                matched_admin=matched,
                proposed_new_owner=target_sp,  # per-workspace SP
                transfer_method="jobs_update_run_as",
                extra=extra,
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workspace files / folders (home trees)
# MAGIC Files/notebooks/dirs/repos have **no owner** — "owned by a departed admin"
# MAGIC means the object lives under `/Users/<email>/` (or `/Repos/<email>/`).
# MAGIC "Transfer" grants the group **CAN_MANAGE** (additive). Paths used by an
# MAGIC **active, recurring** job are flagged in `extra` so you re-home them first.

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


def walk_wsfs(w, roots, max_depth, workers):
    """BFS walk of workspace paths, listing directories concurrently. Replaces the
    SDK's server-side recursive=True so we can cap depth (0 = unlimited; levels below
    a root) and fan out listings. Repos are leaves. Yields ObjectInfo."""
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


def active_recurring_job_paths(w):
    def unpaused(o):
        return o is not None and getattr(o, "pause_status", None) != PauseStatus.PAUSED

    out = {}
    try:
        jobs = list(w.jobs.list(expand_tasks=True))
    except Exception as e:  # noqa: BLE001
        log.warning("jobs.list for active-path map failed: %s", e)
        return out
    for j in jobs:
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
    if account_client is None:
        log.warning("scope_wsfs requested but no account client — skipping.")
    for ws_id, ws in iter_workspaces():
        try:
            w = ws_client(ws)
        except Exception as e:  # noqa: BLE001
            log.warning("cannot connect to workspace %s: %s", ws_id, e)
            continue
        host = w.config.host  # cloud-neutral
        log.info("Workspace files crawl (home trees): %s (%s)", ws_id, host)
        active_map = active_recurring_job_paths(w)
        log.info("  active-recurring-job path map: %d referenced path(s)", len(active_map))
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
            for o in walk_wsfs(w, roots, wsfs_max_depth, wsfs_workers):
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
                    workspace_id=ws_id,
                    workspace_host=host,
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
    # Append (not overwrite): the table accumulates the history of every run, each
    # tagged with run_id/run_timestamp. mergeSchema lets an older table pick up the new
    # run columns on first append after this upgrade. The transfer phase reads only the
    # latest run (max(run_id)), so accumulated history never affects a transfer.
    (
        inv_df.write.mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(output_table)
    )
    print(f"Wrote {inv_df.count()} rows to {output_table} (run_id={RUN_ID})")
    display(
        spark.sql(
            f"SELECT domain, object_type, matched_admin, count(*) n "
            f"FROM {output_table} WHERE run_id = '{RUN_ID}' GROUP BY 1,2,3 ORDER BY 1,2,3"
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
# MAGIC Reads the reviewed inventory table and reassigns ownership. Dry-run unless
# MAGIC `execute = true`. Idempotent: rows already owned by the target group are skipped.

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


def do_transfer(row, ws_client_cache):
    grp = row["proposed_new_owner"] or target_group
    method = row["transfer_method"]
    # Idempotency skip does not apply where current_owner is not an ownership
    # principal: WSFS (home-tree admin), run_as (per-workspace SP), or cluster_revoke
    # (the departed admin whose explicit grant we're removing).
    if (
        method not in ("wsfs_permissions", "jobs_update_run_as", "cluster_revoke")
        and (row["current_owner"] or "").lower() == grp.lower()
    ):
        return f"SKIP already owned by {grp}"

    if method == "cluster_revoke":
        # Remove the departed admin's explicit entitlement on an all-purpose cluster.
        # PATCH (update) is additive and can't delete a principal, so read the ACL,
        # drop the departed admin, and PUT (set) the remaining DIRECT grants back
        # (inherited entries aren't settable and persist on their own). Preserves the
        # owner and everyone else's explicit access.
        api_type, oid = row["securable_type"], row["object_id"]
        if not oid:
            return f"SKIP (no cluster id for {row['full_name']})"
        if not execute:
            held = str(row.get("extra", "")).replace("EXPLICIT_PERMISSION=", "") or "grant"
            return f"DRY-RUN revoke {row['matched_admin']} ({held}) from {api_type}/{oid} ({row['full_name']})"
        ws_id = int(row["workspace_id"])
        w = ws_client_cache.get(ws_id)
        if w is None:
            return f"SKIP (workspace {ws_id} not in scope)"
        try:
            pl = w.permissions.get(request_object_type=api_type, request_object_id=str(oid))
        except Exception as e:  # noqa: BLE001
            return f"SKIP (cannot read ACL for {api_type}/{oid}: {e})"
        target = (row["current_owner"] or row["matched_admin"] or "").strip().lower()
        keep, removed = [], []
        for acl in pl.access_control_list or []:
            principal = acl.user_name or acl.group_name or acl.service_principal_name
            direct = [
                p.permission_level
                for p in (acl.all_permissions or [])
                if p.permission_level and not p.inherited
            ]
            if not direct:
                continue
            if principal and principal.strip().lower() == target:
                removed += [str(getattr(lvl, "value", lvl)) for lvl in direct]
                continue
            # One request per principal (PUT expects a single level per principal); if a
            # principal somehow has several direct levels, keep the strongest.
            lvl = strongest_cluster_level(direct)
            if acl.user_name:
                keep.append(iam.AccessControlRequest(user_name=acl.user_name, permission_level=lvl))
            elif acl.group_name:
                keep.append(iam.AccessControlRequest(group_name=acl.group_name, permission_level=lvl))
            elif acl.service_principal_name:
                keep.append(
                    iam.AccessControlRequest(
                        service_principal_name=acl.service_principal_name, permission_level=lvl
                    )
                )
        if not removed:
            return f"SKIP (no explicit grant for {row['matched_admin']} on {api_type}/{oid})"
        # `keep` may be empty if the admin was the only direct grantee — intended end
        # state (only inherited access remains), so we still PUT it.
        w.permissions.set(request_object_type=api_type, request_object_id=str(oid), access_control_list=keep)
        return f"OK revoked {row['matched_admin']} ({','.join(removed)}) from {api_type}/{oid}"

    if method == "sql_alter":
        tmpl = UC_ALTER.get(row["object_type"])
        if not tmpl:
            return f"SKIP (no SQL template for {row['object_type']})"
        qualified = ".".join(f"`{p}`" for p in row["full_name"].split("."))
        sql = tmpl.format(name=qualified, grp=grp)
        if not execute:
            return f"DRY-RUN {sql}"
        spark.sql(sql)
        return f"OK {sql}"

    if method == "uc_rest":
        seg = UC_REST_PATH.get(row["object_type"])
        if seg is None:
            return f"SKIP (no REST path for {row['object_type']})"
        path = f"/api/2.1/unity-catalog/{seg}/{row['full_name']}"
        if not execute:
            return f"DRY-RUN PATCH {path} owner -> {grp}"
        w_here.api_client.do("PATCH", path, body={"owner": grp})
        return f"OK PATCH {path} owner -> {grp}"

    if method == "permissions_api":
        if not execute:
            return f"DRY-RUN set {grp} IS_OWNER on {row['securable_type']}/{row['object_id']}"
        ws_id = int(row["workspace_id"])
        w = ws_client_cache.get(ws_id)
        if w is None:
            return f"SKIP (workspace {ws_id} not in scope)"
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
        # No owner on WSFS — grant the group CAN_MANAGE (additive) so access
        # survives the user's deletion. Directories cascade to their contents.
        api_type, oid = row["securable_type"], row["object_id"]
        if not api_type or not oid:
            return f"SKIP (no permissions target for {row['object_type']} {row['full_name']})"
        active = str(row.get("extra", "")).startswith("ACTIVE_JOB_DEPENDENCY")
        warn = " [ACTIVE JOB DEP — verify before deleting user]" if active else ""
        if not execute:
            return f"DRY-RUN grant {grp} CAN_MANAGE on {api_type}/{oid} ({row['full_name']}){warn}"
        ws_id = int(row["workspace_id"])
        w = ws_client_cache.get(ws_id)
        if w is None:
            return f"SKIP (workspace {ws_id} not in scope)"
        acl = iam.AccessControlRequest(
            group_name=grp, permission_level=iam.PermissionLevel.CAN_MANAGE
        )
        w.permissions.update(
            request_object_type=api_type, request_object_id=str(oid), access_control_list=[acl]
        )
        return f"OK grant {grp} CAN_MANAGE on {api_type}/{oid}{warn}"

    if method == "jobs_update_run_as":
        # Reassign run_as to the per-workspace SP (grp holds the SP application id).
        # jobs.update is a partial update: only run_as is replaced; tasks/schedule
        # are left intact. A job can only run_as an SP that can manage it, so
        # optionally grant CAN_MANAGE first (gated by grant_run_as_sp_perms).
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
        ws_id = int(row["workspace_id"])
        w = ws_client_cache.get(ws_id)
        if w is None:
            return f"SKIP (workspace {ws_id} not in scope)"
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
    from pyspark.sql import Row

    # The table retains every run's rows (append). Transfer only the most recent run,
    # selected by max(run_id) — run_id is lexicographically sortable (UTC-time prefix).
    latest_run = spark.sql(f"SELECT max(run_id) AS r FROM {output_table}").collect()[0]["r"]
    all_rows = [
        r.asDict()
        for r in spark.table(output_table).where(f"run_id = '{latest_run}'").collect()
    ]
    log.info(
        "%s %d row(s) from %s (latest run_id=%s)",
        "EXECUTING" if execute else "DRY-RUN over",
        len(all_rows),
        output_table,
        latest_run,
    )

    ws_client_cache = {}
    if execute:
        for ws_id, ws in iter_workspaces():
            try:
                ws_client_cache[ws_id] = ws_client(ws)
            except Exception as e:  # noqa: BLE001
                log.warning("cannot connect to workspace %s: %s", ws_id, e)

    _t_dt = datetime.now(timezone.utc)
    transfer_timestamp = _t_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{_t_dt.microsecond // 1000:03d}Z"

    results = []
    ok = skip = err = 0
    for r in all_rows:
        try:
            res = do_transfer(r, ws_client_cache)
            if res.startswith(("OK", "DRY-RUN")):
                ok += 1
            else:
                skip += 1
        except Exception as e:  # noqa: BLE001
            # Capture the full exception (with type) so the failure reason is retained
            # verbatim in the results table and the printed summary below.
            res = f"ERR {type(e).__name__}: {e}"
            err += 1
        results.append(
            {
                **r,
                "result": res,
                "executed": str(bool(execute)).lower(),
                "transfer_run_id": latest_run,
                "transfer_timestamp": transfer_timestamp,
            }
        )
        log.info("%s | %s %s", res, r["object_type"], r["full_name"])

    # Persist every per-row outcome to a durable, queryable table (append = history).
    # This is the source of truth for WHY a row did not transfer: the full error text is
    # retained here even when the notebook's display() output is not rendered (e.g. a
    # job run) or is truncated. Named alongside the inventory table.
    results_table = f"{output_table}_transfer_results"
    RESULT_COLUMNS = INVENTORY_COLUMNS + [
        "result",
        "executed",
        "transfer_run_id",
        "transfer_timestamp",
    ]
    if results:
        res_df = spark.createDataFrame(
            [Row(**{c: ("" if row.get(c) is None else str(row.get(c))) for c in RESULT_COLUMNS}) for row in results]
        ).select(*RESULT_COLUMNS)
        (
            res_df.write.mode("append").option("mergeSchema", "true").saveAsTable(results_table)
        )
        print(f"Wrote {len(results)} result row(s) to {results_table} (transfer_run_id={latest_run})")

    # Show the outcomes. Guard the empty case: spark.createDataFrame([]) can't infer a
    # schema and raises — an empty inventory just means "nothing to transfer".
    if results:
        display(
            spark.createDataFrame(results).select(
                "result", "domain", "object_type", "full_name", "current_owner",
                "matched_admin", "transfer_method",
            )
        )
    else:
        print("Inventory table is empty — nothing to transfer.")

    # Explicitly surface every non-success row in plain text — not truncated (unlike the
    # display grid) and visible in job logs. A row is a "problem" if it neither applied
    # (OK) nor is a dry-run preview (DRY-RUN): i.e. it was SKIPped or it ERRored.
    problems = [x for x in results if not x["result"].startswith(("OK", "DRY-RUN"))]
    if problems:
        print(f"\n⚠ {len(problems)} row(s) did NOT transfer (SKIP/ERR):")
        for x in problems:
            print(
                f"  [{x['result'].split()[0]}] {x['object_type']} '{x['full_name']}' "
                f"({x['transfer_method']}) — {x['result']}"
            )

    print(f"\nDone. {'(dry-run) ' if not execute else ''}ok/dry={ok} skip={skip} err={err}")
    if err:
        print(f"⚠ {err} row(s) ERRORED — full detail above and in table {results_table}.")
    if not execute:
        print("Set execute = true and re-run to apply.")

# COMMAND ----------

if phase == "transfer":
    # Exit is in its OWN cell: dbutils.notebook.exit() stops the notebook immediately and
    # would suppress a display() queued in the same cell — which is why the results grid
    # previously never rendered. Keeping it separate lets the cell above render fully.
    dbutils.notebook.exit(
        json.dumps(
            {
                "phase": "transfer",
                "execute": execute,
                "ok": ok,
                "skip": skip,
                "err": err,
                "results_table": results_table,
            }
        )
    )
