"""Core crawl + transfer logic for the departed-admin-ownership CLI.

Cloud-neutral: workspace hosts come from AccountClient.get_workspace_client() /
w.config.host, never a hardcoded suffix. Feature parity with the notebook: UC (SQL
warehouse with REST fallback), workspace objects (IS_OWNER), workspace files
(CAN_MANAGE + active-job flag), and job run_as (per-workspace SP + preflight + grant).
"""

from __future__ import annotations

import dataclasses
import logging
import urllib.parse
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from config import Config
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service import iam, jobs
from databricks.sdk.service.provisioning import WorkspaceStatus
from databricks.sdk.service.sql import StatementState

log = logging.getLogger("dao.core")


# =============================================================================
# Principal resolution
# =============================================================================
@dataclasses.dataclass
class Principals:
    by_key: dict[str, str]
    emails: set[str]
    target_group: str
    target_group_present: bool
    scim_resolved: set[str]  # configured admins actually found in account SCIM

    def matches(self, owner: str | None) -> str | None:
        if not owner:
            return None
        return self.by_key.get(owner.strip().lower())


def resolve_principals(cfg: Config, ac: AccountClient) -> Principals:
    by_key = {e: e for e in cfg.departed_admins}
    wanted = set(cfg.departed_admins)
    resolved: set[str] = set()
    log.info("Resolving %d departed admin(s) via account SCIM…", len(wanted))
    for u in ac.users.list(attributes="id,userName,displayName,emails"):
        uname = (u.user_name or "").lower()
        if uname in wanted:
            resolved.add(uname)
            emails = [e.value.lower() for e in (u.emails or []) if e.value]
            for k in {uname, str(u.id).lower(), (u.display_name or "").lower(), *emails}:
                if k:
                    by_key[k] = uname
    found = any(
        (g.display_name or "").lower() == cfg.target_group.lower()
        for g in ac.groups.list(attributes="id,displayName")
    )
    return Principals(by_key, wanted, cfg.target_group, found, resolved)


# =============================================================================
# Workspace iteration (cloud-neutral)
# =============================================================================
def iter_workspaces(cfg: Config, ac: AccountClient) -> Iterator[tuple[int, Any]]:
    wanted = set(cfg.workspace_ids)
    for ws in ac.workspaces.list():
        if wanted and ws.workspace_id not in wanted:
            continue
        if ws.workspace_status not in (WorkspaceStatus.RUNNING, None):
            log.info("skip workspace %s (status=%s)", ws.workspace_id, ws.workspace_status)
            continue
        yield ws.workspace_id, ws


def ws_client(ac: AccountClient, ws) -> WorkspaceClient:
    """Cloud-neutral workspace client via account token exchange (runs on CLI host)."""
    return ac.get_workspace_client(ws)


# =============================================================================
# Inventory row
# =============================================================================
INVENTORY_FIELDS = [
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


def _row(**kw) -> dict[str, Any]:
    kw.setdefault("workspace_id", "")
    kw.setdefault("workspace_host", "")
    kw.setdefault("object_id", "")
    kw.setdefault("extra", "")
    return {c: ("" if kw.get(c) is None else str(kw.get(c))) for c in INVENTORY_FIELDS}


# =============================================================================
# UC — SQL fast path (Statement Execution API) with REST fallback
# =============================================================================
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


def _exec_sql(w: WorkspaceClient, warehouse_id: str, sql: str) -> Iterator[list]:
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s"
    )
    sid = resp.statement_id
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        resp = w.statement_execution.get_statement(sid)
    if not resp.status or resp.status.state != StatementState.SUCCEEDED:
        state = resp.status.state if resp.status else "UNKNOWN"
        msg = resp.status.error.message if resp.status and resp.status.error else ""
        raise RuntimeError(f"SQL failed ({state}): {msg}")
    result = resp.result
    while result:
        yield from result.data_array or []
        nxt = getattr(result, "next_chunk_index", None)
        if nxt is None:
            break
        result = w.statement_execution.get_statement_result_chunk_n(sid, nxt)


def inventory_uc_sql(cfg, prin, w, warehouse_id, ws_id, host) -> Iterator[dict]:
    admin_list = ",".join("'{}'".format(a.replace("'", "''")) for a in prin.emails)
    for label, view, name_cols, owner_col in IS_QUERIES:
        cols = ", ".join(name_cols)
        sql = f"SELECT {cols}, {owner_col} FROM {view} WHERE lower({owner_col}) IN ({admin_list})"
        try:
            rows = list(_exec_sql(w, warehouse_id, sql))
        except Exception as e:  # noqa: BLE001
            log.warning("information_schema %s query failed: %s", label, e)
            continue
        for row in rows:
            *name_parts, owner = row
            catalog = name_parts[0] if name_parts else ""
            if catalog and catalog.lower() in cfg.skip_catalogs:
                continue
            matched = prin.matches(owner)
            if not matched:
                continue
            yield _row(
                domain="unity_catalog",
                workspace_id=ws_id,
                workspace_host=host,
                object_type=label,
                securable_type=label.upper(),
                full_name=".".join(p for p in name_parts if p),
                current_owner=owner,
                matched_admin=matched,
                proposed_new_owner=prin.target_group,
                transfer_method="sql_alter",
            )


def inventory_uc_rest_account(cfg, prin, w, ws_id, host) -> Iterator[dict]:
    crawlers = [
        ("external_location", lambda: w.external_locations.list(), "name"),
        ("storage_credential", lambda: w.storage_credentials.list(), "name"),
        ("connection", lambda: w.connections.list(), "name"),
        ("share", lambda: w.shares.list_shares(), "name"),
        ("recipient", lambda: w.recipients.list(), "name"),
        ("registered_model", lambda: w.registered_models.list(), "full_name"),
    ]
    for kind, lister, name_attr in crawlers:
        try:
            objs = list(lister())
        except Exception as e:  # noqa: BLE001
            log.debug("UC REST %s crawl skipped: %s", kind, e)
            continue
        for obj in objs:
            matched = prin.matches(getattr(obj, "owner", None))
            if not matched:
                continue
            name = getattr(obj, name_attr, None) or getattr(obj, "name", "")
            yield _row(
                domain="unity_catalog",
                workspace_id=ws_id,
                workspace_host=host,
                object_type=kind,
                securable_type=kind.upper(),
                full_name=name,
                current_owner=obj.owner,
                matched_admin=matched,
                proposed_new_owner=prin.target_group,
                transfer_method="uc_rest",
            )


# =============================================================================
# Workspace objects — IS_OWNER via permissions API
# =============================================================================
def owner_from_permissions(w, request_object_type, request_object_id) -> str | None:
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
    # clusters handled separately (inventory_clusters) — all-purpose only, and we
    # capture explicit non-owner ACL grants in addition to IS_OWNER.
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


def inventory_workspace(cfg, prin, w, ws_id, host) -> Iterator[dict]:
    # Workspace-object ownership (IS_OWNER) must be a user or service principal — a
    # group cannot own a job/pipeline/warehouse/dashboard. We reassign to this
    # workspace's run_as service principal (reused as the durable owner). If none is
    # configured, the row still lists the object but the transfer will skip it.
    owner_sp = cfg.run_as_sp_map.get(ws_id, "")
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
            matched = prin.matches(owner)
            if not matched:
                continue
            yield _row(
                domain="workspace",
                workspace_id=ws_id,
                workspace_host=host,
                object_type=label,
                securable_type=api_type,
                full_name=str(name_fn(o)),
                object_id=str(oid),
                current_owner=owner,
                matched_admin=matched,
                proposed_new_owner=owner_sp,
                transfer_method="permissions_api",
            )
    try:
        for d in w.lakeview.list():
            owner = owner_from_permissions(w, "dashboards", d.dashboard_id)
            matched = prin.matches(owner)
            if matched:
                yield _row(
                    domain="workspace",
                    workspace_id=ws_id,
                    workspace_host=host,
                    object_type="lakeview_dashboard",
                    securable_type="dashboards",
                    full_name=d.display_name or d.dashboard_id,
                    object_id=d.dashboard_id,
                    current_owner=owner,
                    matched_admin=matched,
                    proposed_new_owner=owner_sp,
                    transfer_method="permissions_api",
                )
    except Exception as e:  # noqa: BLE001
        log.debug("lakeview list failed on %s: %s", ws_id, e)

    yield from inventory_clusters(cfg, prin, w, ws_id, host)


# =============================================================================
# All-purpose clusters
#
# Clusters have NO owner: the permissions API exposes only CAN_ATTACH_TO /
# CAN_RESTART / CAN_MANAGE (there is no IS_OWNER level for clusters, unlike jobs or
# warehouses). So a departed admin's relationship to a cluster is always an explicit
# ACL grant, never ownership. We inventory every all-purpose cluster on which the
# admin holds an explicit, NON-inherited grant and, at transfer time, simply revoke
# that grant (transfer_method=cluster_revoke) — the access is never handed to the
# group. The strongest held level is recorded in `extra`.
#
# Only all-purpose clusters (cluster_source UI/API) are considered — job/pipeline/
# model-serving clusters are ephemeral and managed via their parent resource.
# =============================================================================
CLUSTER_PERM_RANK = {"CAN_MANAGE": 3, "CAN_RESTART": 2, "CAN_ATTACH_TO": 1}
ALL_PURPOSE_CLUSTER_SOURCES = {"UI", "API"}


def perm_value(level: Any) -> str:
    """Normalize a PermissionLevel enum (or str) to its bare value, e.g. CAN_MANAGE."""
    return str(getattr(level, "value", level) or "")


def _strongest_cluster_level(levels: list[Any]) -> Any:
    """The highest-privilege level among a principal's direct grants (a principal has
    one effective grant per cluster; collapse any list to a single level for the PUT)."""
    return max(levels, key=lambda lvl: CLUSTER_PERM_RANK.get(perm_value(lvl), 0))


def acl_direct_grants(w, cluster_id) -> dict[str, list[Any]]:
    """principal -> list of that principal's DIRECT (non-inherited) permission levels
    on the cluster. Inherited/group-derived grants are excluded — we only act on
    explicit entitlements."""
    try:
        pl = w.permissions.get(request_object_type="clusters", request_object_id=str(cluster_id))
    except Exception as e:  # noqa: BLE001
        log.debug("permissions.get(clusters/%s) failed: %s", cluster_id, e)
        return {}
    grants: dict[str, list[Any]] = {}
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


def inventory_clusters(cfg, prin, w, ws_id, host) -> Iterator[dict]:
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
            matched = prin.matches(principal)
            if not matched:
                continue
            held = perm_value(_strongest_cluster_level(levels))
            yield _row(
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


# =============================================================================
# Job run_as — per-workspace SP + preflight
# =============================================================================
SP_RUN_CAPABLE = {"CAN_MANAGE", "IS_OWNER"}


def sp_job_permission_levels(w, job_id, sp_app_id) -> list[str]:
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


def classify_sp_access(sp_levels: list[str]) -> str:
    return "SP_HAS_ACCESS" if set(sp_levels) & SP_RUN_CAPABLE else "SP_NEEDS_GRANT"


def inventory_run_as(cfg, prin, w, ws_id, host, target_sp) -> Iterator[dict]:
    try:
        base_jobs = list(w.jobs.list(expand_tasks=False))
    except Exception as e:  # noqa: BLE001
        log.warning("jobs.list failed on %s: %s", ws_id, e)
        return
    for bj in base_jobs:
        try:
            job = w.jobs.get(bj.job_id)
        except Exception as e:  # noqa: BLE001
            log.debug("jobs.get(%s) failed: %s", bj.job_id, e)
            continue
        effective = job.run_as_user_name  # catches explicit run_as AND creator-default
        matched = prin.matches(effective)
        if not matched:
            continue
        name = job.settings.name if job.settings else str(bj.job_id)
        extra = classify_sp_access(sp_job_permission_levels(w, bj.job_id, target_sp))
        yield _row(
            domain="job_run_as",
            workspace_id=ws_id,
            workspace_host=host,
            object_type="job_run_as",
            securable_type="jobs",
            full_name=str(name),
            object_id=str(bj.job_id),
            current_owner=effective,
            matched_admin=matched,
            proposed_new_owner=target_sp,
            transfer_method="jobs_update_run_as",
            extra=extra,
        )


# =============================================================================
# Workspace files (WSFS) — concurrent, depth-limited walk
# =============================================================================
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


def is_noise_path(path: str) -> bool:
    return any(seg in WSFS_NOISE_SEGMENTS for seg in (path or "").split("/"))


def norm_path(p: str | None) -> str:
    if not p:
        return ""
    p = p.strip()
    if p.startswith("/Workspace/"):
        p = p[len("/Workspace") :]
    elif p == "/Workspace":
        p = "/"
    return p.rstrip("/") or "/"


def path_is_active(norm: str, active_map: dict[str, list[str]]) -> list[str]:
    hits: list[str] = []
    for used, names in active_map.items():
        if norm == used or used.startswith(norm + "/"):
            hits.extend(names)
    return sorted(set(hits))


def _task_paths(task) -> Iterator[str]:
    if getattr(task, "notebook_task", None) and task.notebook_task.notebook_path:
        yield task.notebook_task.notebook_path
    if getattr(task, "spark_python_task", None) and task.spark_python_task.python_file:
        yield task.spark_python_task.python_file
    sql = getattr(task, "sql_task", None)
    if sql and getattr(sql, "file", None) and getattr(sql.file, "path", None):
        yield sql.file.path
    dbt = getattr(task, "dbt_task", None)
    if dbt and getattr(dbt, "project_directory", None):
        yield dbt.project_directory


def active_recurring_job_paths(w) -> dict[str, list[str]]:
    def unpaused(obj) -> bool:
        return obj is not None and getattr(obj, "pause_status", None) != jobs.PauseStatus.PAUSED

    result: dict[str, list[str]] = {}
    try:
        job_list = list(w.jobs.list(expand_tasks=True))
    except Exception as e:  # noqa: BLE001
        log.warning("jobs.list for active-path map failed: %s", e)
        return result
    for j in job_list:
        js = j.settings
        if not js or not (unpaused(js.schedule) or unpaused(js.continuous) or unpaused(js.trigger)):
            continue
        name = js.name or str(j.job_id)
        for t in js.tasks or []:
            for p in _task_paths(t):
                result.setdefault(norm_path(p), []).append(name)
    return result


def _walk_wsfs(w, roots, max_depth, workers) -> Iterator[Any]:
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
            for (_parent, depth), objs in zip(frontier, results, strict=True):
                for o in objs:
                    if is_noise_path(o.path or ""):
                        continue
                    yield o
                    if (o.object_type.value if o.object_type else "") == "DIRECTORY":
                        if max_depth and depth + 1 >= max_depth:
                            continue
                        nxt.append((o.path, depth + 1))
            frontier = nxt


def inventory_workspace_files(cfg, prin, w, ws_id, host) -> Iterator[dict]:
    active_map = active_recurring_job_paths(w)
    log.info("active-recurring-job path map: %d referenced path(s)", len(active_map))
    for email in sorted(prin.emails):
        roots = []
        for root in (f"/Users/{email}", f"/Repos/{email}"):
            try:
                w.workspace.get_status(root)
                roots.append(root)
            except Exception:  # noqa: BLE001
                continue
        if not roots:
            continue
        for o in _walk_wsfs(w, roots, cfg.wsfs_max_depth, cfg.wsfs_workers):
            otype = o.object_type.value if o.object_type else ""
            if otype == "LIBRARY":
                continue
            active_jobs = path_is_active(norm_path(o.path), active_map)
            extra = ""
            if active_jobs:
                extra = "ACTIVE_JOB_DEPENDENCY: " + "; ".join(active_jobs[:5])
                if len(active_jobs) > 5:
                    extra += f" (+{len(active_jobs) - 5} more)"
            yield _row(
                domain="workspace_files",
                workspace_id=ws_id,
                workspace_host=host,
                object_type=otype.lower() or "unknown",
                securable_type=WSFS_PERM_TYPE.get(otype, ""),
                full_name=o.path,
                object_id=str(o.object_id or ""),
                current_owner=email,
                matched_admin=email,
                proposed_new_owner=prin.target_group,
                transfer_method="wsfs_permissions",
                extra=extra,
            )


# =============================================================================
# Transfer
# =============================================================================
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


def _reassign_owner_to_sp(row: dict, w: WorkspaceClient, dry_run: bool) -> str:
    """Reassign IS_OWNER of a workspace object (job/pipeline/warehouse/dashboard) to a
    service principal.

    A group cannot own these objects ("Groups cannot be owners"), and the owner cannot
    be changed with a PATCH: warehouses reject it outright, and a PATCH that adds a new
    IS_OWNER leaves the old one ("must have exactly one owner"). So we PUT the full ACL
    with exactly one IS_OWNER (the SP), preserving every other principal's direct grant
    and dropping the departed admin. proposed_new_owner carries this workspace's run_as
    SP application id (set at inventory)."""
    api_type, oid = row["securable_type"], row["object_id"]
    owner_sp = (row.get("proposed_new_owner") or "").strip()
    if not owner_sp:
        return (
            f"SKIP (no run_as SP for this workspace — cannot reassign owner of "
            f"{api_type}/{oid}; set run_as_sp_map)"
        )
    if dry_run:
        return (
            f"DRY-RUN set SP {owner_sp} IS_OWNER on {api_type}/{oid} "
            f"({row['full_name']}) [was {row['current_owner']}]"
        )
    departed = (row.get("current_owner") or row.get("matched_admin") or "").strip().lower()
    try:
        pl = w.permissions.get(request_object_type=api_type, request_object_id=str(oid))
    except Exception as e:  # noqa: BLE001
        return f"SKIP (cannot read ACL for {api_type}/{oid}: {e})"
    # Rebuild the ACL: SP as the sole owner, other principals' direct grants preserved,
    # the departed admin dropped. Never re-send a second IS_OWNER or the SP twice.
    keep = [
        iam.AccessControlRequest(
            service_principal_name=owner_sp, permission_level=iam.PermissionLevel.IS_OWNER
        )
    ]
    for acl in pl.access_control_list or []:
        principal = acl.user_name or acl.group_name or acl.service_principal_name
        if not principal or principal.strip().lower() == departed:
            continue
        if principal.strip().lower() == owner_sp.lower():  # added above as the owner
            continue
        for p in acl.all_permissions or []:
            if not p.permission_level or p.inherited:
                continue
            if "IS_OWNER" in perm_value(p.permission_level):  # only the SP owns
                continue
            if acl.user_name:
                keep.append(iam.AccessControlRequest(user_name=acl.user_name, permission_level=p.permission_level))
            elif acl.group_name:
                keep.append(iam.AccessControlRequest(group_name=acl.group_name, permission_level=p.permission_level))
            elif acl.service_principal_name:
                keep.append(
                    iam.AccessControlRequest(
                        service_principal_name=acl.service_principal_name, permission_level=p.permission_level
                    )
                )
    w.permissions.set(request_object_type=api_type, request_object_id=str(oid), access_control_list=keep)
    return f"OK set SP {owner_sp} IS_OWNER on {api_type}/{oid} (was {row['current_owner']})"


def _revoke_cluster_grant(row: dict, w: WorkspaceClient, dry_run: bool) -> str:
    """Remove the departed admin's explicit entitlement on an all-purpose cluster.

    The permissions API PATCH (update) is additive and cannot delete a principal, so
    we read the current ACL, drop the departed admin's entry, and PUT (set) the
    remaining DIRECT grants back. Only non-inherited entries are re-sent (inherited
    grants aren't settable and persist on their own), which preserves the owner and
    everyone else's explicit access while removing just this user."""
    api_type, oid = row["securable_type"], row["object_id"]
    if not oid:
        return f"SKIP (no cluster id for {row['full_name']})"
    # Dry-run must not touch the workspace client — cmd_transfer builds clients only for a
    # real run (w is None in dry-run). Preview from the level recorded in `extra`.
    if dry_run:
        held = (row.get("extra") or "").replace("EXPLICIT_PERMISSION=", "") or "grant"
        return (
            f"DRY-RUN revoke {row['matched_admin']} ({held}) from "
            f"{api_type}/{oid} ({row['full_name']})"
        )
    target = (row.get("current_owner") or row.get("matched_admin") or "").strip().lower()
    try:
        pl = w.permissions.get(request_object_type=api_type, request_object_id=str(oid))
    except Exception as e:  # noqa: BLE001
        return f"SKIP (cannot read ACL for {api_type}/{oid}: {e})"

    keep: list[iam.AccessControlRequest] = []
    removed: list[str] = []
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
            removed += [perm_value(lvl) for lvl in direct]
            continue  # drop the departed admin's grant(s)
        # One request per principal (PUT expects a single level per principal); if a
        # principal somehow has several direct levels, keep the strongest.
        lvl = _strongest_cluster_level(direct)
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
    # `keep` may be empty if the departed admin was the only direct grantee — that is
    # the intended end state (only inherited access remains), so we still PUT it.
    w.permissions.set(request_object_type=api_type, request_object_id=str(oid), access_control_list=keep)
    return f"OK revoked {row['matched_admin']} ({','.join(removed)}) from {api_type}/{oid}"


def transfer_row(
    cfg: Config, row: dict, w: WorkspaceClient, warehouse_id: str, dry_run: bool
) -> str:
    grp = row["proposed_new_owner"] or cfg.target_group
    method = row["transfer_method"]
    # "Already owned" idempotency applies only to methods that transfer to a group and
    # whose current_owner is a comparable ownership principal. permissions_api reassigns
    # to an SP (its own skip logic lives in _reassign_owner_to_sp); wsfs/run_as/
    # cluster_revoke don't have a group owner to compare.
    if (
        method not in ("wsfs_permissions", "jobs_update_run_as", "cluster_revoke", "permissions_api")
        and (row.get("current_owner") or "").lower() == grp.lower()
    ):
        return f"SKIP already owned by {grp}"

    if method == "cluster_revoke":
        return _revoke_cluster_grant(row, w, dry_run)

    if method == "sql_alter":
        tmpl = UC_ALTER.get(row["object_type"])
        if not tmpl:
            return f"SKIP (no SQL template for {row['object_type']})"
        if not warehouse_id:
            return "SKIP (sql_alter needs a warehouse; none configured for this workspace)"
        # Backtick-quote each identifier part, doubling any embedded backtick so a
        # name like `weird`col` can't break out of the quoting. grp is likewise quoted
        # in the ALTER template.
        qualified = ".".join("`{}`".format(p.replace("`", "``")) for p in row["full_name"].split("."))
        sql = tmpl.format(name=qualified, grp=grp.replace("`", "``"))
        if dry_run:
            return f"DRY-RUN {sql}"
        list(_exec_sql(w, warehouse_id, sql))
        return f"OK {sql}"

    if method == "uc_rest":
        seg = UC_REST_PATH.get(row["object_type"])
        if seg is None:
            return f"SKIP (no REST path for {row['object_type']})"
        # URL-encode the name so spaces / slashes / reserved chars don't corrupt the
        # path (safe="" also encodes '/', keeping a slash in the name in-segment).
        name = urllib.parse.quote(row["full_name"], safe="")
        path = f"/api/2.1/unity-catalog/{seg}/{name}"
        if dry_run:
            return f"DRY-RUN PATCH {path} owner -> {grp}"
        w.api_client.do("PATCH", path, body={"owner": grp})
        return f"OK PATCH {path} owner -> {grp}"

    if method == "permissions_api":
        return _reassign_owner_to_sp(row, w, dry_run)

    if method == "wsfs_permissions":
        api_type, oid = row["securable_type"], row["object_id"]
        if not api_type or not oid:
            return f"SKIP (no permissions target for {row['object_type']} {row['full_name']})"
        active = str(row.get("extra", "")).startswith("ACTIVE_JOB_DEPENDENCY")
        warn = " [ACTIVE JOB DEP — verify before deleting user]" if active else ""
        if dry_run:
            return f"DRY-RUN grant {grp} CAN_MANAGE on {api_type}/{oid} ({row['full_name']}){warn}"
        w.permissions.update(
            request_object_type=api_type,
            request_object_id=str(oid),
            access_control_list=[
                iam.AccessControlRequest(
                    group_name=grp, permission_level=iam.PermissionLevel.CAN_MANAGE
                )
            ],
        )
        return f"OK grant {grp} CAN_MANAGE on {api_type}/{oid}{warn}"

    if method == "jobs_update_run_as":
        job_id = int(row["object_id"])
        needs_grant = row.get("extra") == "SP_NEEDS_GRANT"
        will_grant = cfg.grant_run_as_sp_perms and needs_grant
        if dry_run:
            note = (
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
                f"({row['full_name']}) [was {row['current_owner']}]{note}"
            )
        granted = ""
        if will_grant:
            w.permissions.update(
                request_object_type="jobs",
                request_object_id=str(job_id),
                access_control_list=[
                    iam.AccessControlRequest(
                        service_principal_name=grp, permission_level=iam.PermissionLevel.CAN_MANAGE
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
