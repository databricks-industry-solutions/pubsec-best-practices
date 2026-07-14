"""Staged resolution pipeline (spec §2 data path, §3.3, §3.4).

Turns an input id (parent run / task run / job / cluster, or a manual
cluster-id) into a structured tree of app-id groups -> executors -> log files,
each carrying an opaque signed ``file_ref``. Emits a ``reason_code`` from the
§3.4 set describing the outcome.

TWO-IDENTITY DISCIPLINE (spec §2, §3.3) — the whole point of this module:

  - Every JOBS / CLUSTERS call goes through the **SP** side (``runs_client`` +
    ``ClusterMetaClient``). Metadata only. The SP is NOT the authorization
    oracle.
  - Every FILE LISTING call goes through the **user OBO** side
    (``FileLister``). Unity Catalog enforces per-user access on these calls: a
    ``PermissionDenied`` here -> ``FILES_FORBIDDEN`` and the run is hidden.
    This is the ONLY per-user access check; the SP could see the metadata but
    the user still cannot see the logs.

Wiring these two seams to the wrong identity is a security bug. The unit tests
assert the split: jobs/clusters go to the SP mock, listing goes to the user
mock, and a test fails if they are crossed.

Staged / lazy (spec §2 step 6, §3.2): we list app-ids then executors then
files, but we do NOT read file *content* here (that is ``logs.read_log`` via a
minted ``file_ref``).
"""

from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass, field
from typing import Iterable, Optional, Protocol

import filerefs
import runs as runs_mod
from logfiles import classify_log_file as _classify_log_file  # single source (MEDIUM #4)
from paths import is_under_root, join_under_root, normalize_path

CLD_ROOT_ALLOWLIST_ENV = "CLD_ROOT_ALLOWLIST"

# Recognized log-file classification (spec §2, §3.1) lives in ``logfiles`` and
# is shared verbatim with the file-ref layer so the resolver's "what is a log
# file" definition and the mint/verify guard can never drift apart (MEDIUM #4).


# --------------------------------------------------------------------------- #
# Identity seams                                                               #
# --------------------------------------------------------------------------- #
class ClusterMetaClient(Protocol):
    """[SP] Clusters metadata seam. Implemented by ``SdkClusterMetaClient``."""

    def get_cluster(self, cluster_id: str):
        """Return the SDK ``ClusterDetails`` (with ``cluster_log_conf`` /
        ``cluster_source``), or raise if unavailable / aged out."""


class SdkClusterMetaClient:
    """Real [SP] cluster metadata reader. ISOLATED (like ``SdkRunsClient``).

    Confirmed SDK name (databricks-sdk 0.44.0):
    ``clusters.get(cluster_id) -> ClusterDetails``.
    """

    def __init__(self, sp_client):
        self._sp = sp_client

    def get_cluster(self, cluster_id: str):
        return self._sp.clusters.get(cluster_id)


class FileLister(Protocol):
    """[USER OBO] directory-listing seam. UC enforces per-user access here.

    A UC permission denial MUST surface as ``PermissionDenied`` (the SDK's
    exception) so the resolver can map it to ``FILES_FORBIDDEN``. Not-found is
    signalled by returning an empty list (or raising ``NotFound``).
    """

    def list_dir(self, path: str) -> Iterable:
        """Return an iterator of entries with ``.name`` / ``.path`` /
        ``.is_directory`` / ``.file_size`` / ``.last_modified``."""


class SdkFileLister:
    """Real [USER OBO] lister backed by the user's ``WorkspaceClient``.

    Confirmed SDK name (databricks-sdk 0.44.0):
    ``files.list_directory_contents(path) -> Iterator[DirectoryEntry]``.
    """

    def __init__(self, user_client):
        self._client = user_client

    def list_dir(self, path: str):
        return self._client.files.list_directory_contents(path)


# --------------------------------------------------------------------------- #
# Result shapes                                                                #
# --------------------------------------------------------------------------- #
@dataclass
class LogFileNode:
    name: str
    file_kind: str
    size: Optional[int]
    modified: Optional[int]
    file_ref: str  # opaque signed ref (never the raw path)


@dataclass
class ExecutorNode:
    executor_id: str
    files: list = field(default_factory=list)  # list[LogFileNode]


@dataclass
class AppGroupNode:
    app_id: str
    executors: list = field(default_factory=list)  # list[ExecutorNode]


@dataclass
class LogTree:
    """The resolved tree (spec §3.2 ``tree``). Driver files (if present) are a
    sibling group so the UI's Driver tab can render them (spec §5)."""

    cluster_id: str
    cld_root: str
    app_groups: list = field(default_factory=list)  # list[AppGroupNode]
    driver_files: list = field(default_factory=list)  # list[LogFileNode]


@dataclass
class ResolveResult:
    """Structured outcome (spec §3.2). ``reason_code`` from §3.4."""

    outcome: str  # "OK" | "ERROR" (coarse; reason_code is the detail)
    reason_code: str
    run_meta: Optional[dict] = None
    tasks: Optional[list] = None  # list[dict] for a task selector
    tree: Optional[dict] = None  # LogTree as dict
    detail: Optional[str] = None  # safe, no sensitive paths


def _ok(reason_code: str, **kw) -> ResolveResult:
    return ResolveResult(outcome="OK", reason_code=reason_code, **kw)


def _err(reason_code: str, **kw) -> ResolveResult:
    return ResolveResult(outcome="ERROR", reason_code=reason_code, **kw)


# --------------------------------------------------------------------------- #
# Config                                                                       #
# --------------------------------------------------------------------------- #
def load_allowlist(raw: Optional[str] = None) -> list[str]:
    """Parse the comma-separated ``CLD_ROOT_ALLOWLIST`` into normalized roots."""
    raw = raw if raw is not None else os.environ.get(CLD_ROOT_ALLOWLIST_ENV, "")
    roots = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            roots.append(normalize_path(part))
    return roots


# --------------------------------------------------------------------------- #
# Input classification                                                         #
# --------------------------------------------------------------------------- #
# Databricks cluster ids look like "0710-123456-abcd1234" / "0710-abc". Run and
# job ids are pure integers.
_CLUSTER_ID_RE = re.compile(r"^\d{4}-\d{4,}-[a-z0-9]+$|^\d{4}-[a-z0-9]+$")


def classify_input(value: str) -> str:
    """Classify a raw lookup string as ``cluster`` or ``run`` (spec §2 step 1).

    Job vs run vs task cannot be told apart from the id alone (all ints) — the
    resolver treats any integer id as a run and lets ``get_run`` sort out
    parent-vs-task. A dashed non-numeric id is a cluster id (manual mode).
    """
    v = (value or "").strip()
    if not v:
        return "unknown"
    if _CLUSTER_ID_RE.match(v):
        return "cluster"
    if v.isdigit():
        return "run"
    return "unknown"


# --------------------------------------------------------------------------- #
# Cluster classification + CLD resolution [SP]                                 #
# --------------------------------------------------------------------------- #
def _is_job_cluster(cluster_details) -> bool:
    """True iff the cluster was created for a job (executor CLD is only verified
    for job clusters — spec §1 verified-facts #1)."""
    source = getattr(cluster_details, "cluster_source", None)
    val = getattr(source, "value", None) or (str(source) if source is not None else "")
    return str(val).upper().endswith("JOB")


def _cld_destination(cluster_details) -> Optional[str]:
    """Extract the UC-Volume CLD destination, or None if not a Volume dest."""
    conf = getattr(cluster_details, "cluster_log_conf", None)
    if conf is None:
        return None
    volumes = getattr(conf, "volumes", None)
    if volumes is None:
        return None
    return getattr(volumes, "destination", None)


def _match_allowlisted_root(cld_dest: str, allowlist: list[str]) -> Optional[str]:
    """Return the allowlisted root the CLD destination maps to, else None.

    The configured CLD destination (from ``clusters.get``) must be equal to, or
    nested under, one of the allowlisted team roots (spec §3.3). A destination
    that merely *contains* an allowlisted root (i.e. is broader than allowed) is
    rejected — we never widen access beyond the configured team roots.
    """
    if not cld_dest:
        return None
    ndest = normalize_path(cld_dest)
    for root in allowlist:
        if is_under_root(ndest, root):
            return root
    return None


def _find_root_by_probe(
    cluster_id: str, allowlist: list[str], lister: FileLister
) -> Optional[str]:
    """Fallback when cluster metadata aged out (spec §3.3): probe each
    allowlisted root for an existing ``<root>/<cluster-id>/``.

    Uses the USER OBO lister (per-user access still enforced). A denial on one
    root is not fatal to the probe — we try the next.
    """
    for root in allowlist:
        candidate = join_under_root(root, cluster_id)
        try:
            entries = list(lister.list_dir(candidate))
        except Exception:  # noqa: BLE001 - not-found / denied on this root; skip
            continue
        if entries:
            return root
    return None


# --------------------------------------------------------------------------- #
# File-tree listing [USER OBO]                                                 #
# --------------------------------------------------------------------------- #
def _is_permission_denied(exc: Exception) -> bool:
    """Recognize a UC per-user access denial across SDK versions.

    We match on the exception class name so we don't hard-depend on the SDK's
    ``errors`` module in unit tests (the fakes raise a same-named class).
    """
    name = type(exc).__name__
    return name in ("PermissionDenied", "Forbidden") or "PermissionDenied" in name


def _list_names(lister: FileLister, path: str):
    """List a directory, returning entries. Raises on permission denied so the
    caller maps it to FILES_FORBIDDEN; returns [] on not-found."""
    try:
        return list(lister.list_dir(path))
    except Exception as exc:  # noqa: BLE001
        if _is_permission_denied(exc):
            raise
        # NotFound / empty dir — treat as absent.
        if type(exc).__name__ in ("NotFound", "ResourceDoesNotExist", "FileNotFoundError"):
            return []
        # Unknown listing error: treat as absent rather than crash the resolve.
        return []


def _collect_log_files(
    lister: FileLister,
    dir_path: str,
    *,
    root: str,
    base_dir: str,
    run_id: str,
    cluster_id: str,
    user: str,
) -> list[LogFileNode]:
    """List a leaf dir and mint a file_ref for each recognized log file.

    ``base_dir`` is the resolved ``<cld_dest>/<cluster-id>`` directory; every
    minted ref is bound to it (MEDIUM #6). The entry path is always constructed
    with ``join_under_root(dir_path, name)`` rather than trusting a listing's
    ``entry.path`` verbatim, so a malformed/spoofed listing result can never
    escape the directory actually being listed.
    """
    nodes: list[LogFileNode] = []
    for entry in _list_names(lister, dir_path):
        if getattr(entry, "is_directory", False):
            continue
        name = getattr(entry, "name", None)
        if not name:
            continue
        kind = _classify_log_file(name)
        if kind is None:
            continue  # not a recognized log file — never minted, never exposed
        # Construct the path under the listed dir (never trust entry.path): this
        # guarantees containment under dir_path (and thus base_dir/root).
        entry_path = join_under_root(dir_path, name)
        ref = filerefs.mint_file_ref(
            path=entry_path,
            run_id=run_id,
            cluster_id=cluster_id,
            file_kind=kind,
            user=user,
            root=root,
            base_dir=base_dir,
        )
        nodes.append(
            LogFileNode(
                name=name,
                file_kind=kind,
                size=getattr(entry, "file_size", None),
                modified=getattr(entry, "last_modified", None),
                file_ref=ref,
            )
        )
    return nodes


def build_tree(
    lister: FileLister,
    *,
    root: str,
    cluster_id: str,
    run_id: str,
    user: str,
    cld_dest: Optional[str] = None,
) -> ResolveResult:
    """[USER OBO] List ``<base>/<cluster-id>/executor/<app>/<exec>/`` and the
    sibling ``driver/`` into a structured tree (spec §2 step 6).

    ``root`` is the allowlisted team root used for file-ref containment binding.
    ``cld_dest`` is the actual configured CLD destination to list under (equal
    to or nested under ``root``); it defaults to ``root`` for manual mode /
    probe fallback where the two coincide.

    Reason codes emitted here: ``FILES_FORBIDDEN`` (UC denial),
    ``DELIVERY_PENDING`` (cluster dir absent — recently terminated),
    ``NO_EXECUTOR_DIR`` (dir present, no executor/ — e.g. all-purpose; driver
    still surfaced), ``NO_LOG_FILES`` (dirs present but no recognized files),
    or ``OK``.
    """
    base = cld_dest or root
    cluster_dir = join_under_root(base, cluster_id)
    # MEDIUM #6: every ref minted below is bound to this cluster directory, so a
    # ref can only ever read a file under THIS cluster's dir (not a sibling's).
    base_dir = cluster_dir

    try:
        top = _list_names(lister, cluster_dir)
    except Exception as exc:  # noqa: BLE001
        if _is_permission_denied(exc):
            return _err("FILES_FORBIDDEN", detail="no READ on this Volume")
        raise

    if not top:
        # Cluster dir absent: CLD hasn't landed yet (lag) or aged out.
        return _err(
            "DELIVERY_PENDING",
            detail="logs not delivered yet; retry shortly",
            run_meta={"cluster_id": cluster_id},
        )

    top_names = {getattr(e, "name", ""): e for e in top}
    tree = LogTree(cluster_id=cluster_id, cld_root=root)

    # Driver files (sibling of executor/). Surfaced whenever present (spec §5).
    if "driver" in top_names:
        driver_dir = join_under_root(cluster_dir, "driver")
        tree.driver_files = _collect_log_files(
            lister, driver_dir, root=root, base_dir=base_dir, run_id=run_id,
            cluster_id=cluster_id, user=user,
        )

    # Executor tree: executor/<app-id>/<exec-id>/<files>.
    if "executor" in top_names:
        exec_dir = join_under_root(cluster_dir, "executor")
        for app_entry in _list_names(lister, exec_dir):
            if not getattr(app_entry, "is_directory", False):
                continue
            app_id = getattr(app_entry, "name", None)
            if not app_id:
                continue
            app_dir = join_under_root(exec_dir, app_id)
            group = AppGroupNode(app_id=app_id)
            for exec_entry in _list_names(lister, app_dir):
                if not getattr(exec_entry, "is_directory", False):
                    continue
                exec_id = getattr(exec_entry, "name", None)
                if not exec_id:
                    continue
                leaf = join_under_root(app_dir, exec_id)
                files = _collect_log_files(
                    lister, leaf, root=root, base_dir=base_dir, run_id=run_id,
                    cluster_id=cluster_id, user=user,
                )
                group.executors.append(ExecutorNode(executor_id=exec_id, files=files))
            tree.app_groups.append(group)

    has_exec_files = any(
        ex.files for grp in tree.app_groups for ex in grp.executors
    )

    if not tree.app_groups and "executor" not in top_names:
        # No executor dir at all (e.g. all-purpose cluster). Driver may exist.
        return _err(
            "NO_EXECUTOR_DIR",
            detail="no executor logs; showing driver if present",
            tree=asdict(tree),
            run_meta={"cluster_id": cluster_id},
        )

    if not has_exec_files and not tree.driver_files:
        return _err(
            "NO_LOG_FILES",
            detail="delivered dirs contain no recognized log files",
            tree=asdict(tree),
            run_meta={"cluster_id": cluster_id},
        )

    return _ok("OK", tree=asdict(tree), run_meta={"cluster_id": cluster_id})


# --------------------------------------------------------------------------- #
# Cluster -> tree ([SP] metadata + CLD resolve, then [USER OBO] list)          #
# --------------------------------------------------------------------------- #
def resolve_cluster(
    cluster_id: str,
    *,
    cluster_meta: ClusterMetaClient,
    lister: FileLister,
    allowlist: list[str],
    user: str,
    run_id: str = "",
    require_job_cluster: bool = True,
) -> ResolveResult:
    """Resolve a known cluster-id to a tree (spec §2 steps 4-6, §3.3).

    [SP] ``clusters.get`` -> classify job/all-purpose -> read CLD dest ->
    assert allowlisted. If metadata aged out, fall back to probing allowlisted
    roots with the [USER OBO] lister. Then [USER OBO] build the tree.
    """
    details = None
    try:
        details = cluster_meta.get_cluster(cluster_id)
    except Exception as exc:  # noqa: BLE001 - metadata aged out / not found
        # Metadata unavailable: fall back to probing allowlisted roots (§3.3).
        root = _find_root_by_probe(cluster_id, allowlist, lister)
        if root is None:
            return _err(
                "CLUSTER_METADATA_UNAVAILABLE",
                detail="cluster metadata unavailable and no allowlisted root "
                "contains this cluster",
            )
        return build_tree(
            lister, root=root, cluster_id=cluster_id, run_id=run_id, user=user
        )

    if require_job_cluster and not _is_job_cluster(details):
        # Not a job cluster: executor CLD is not delivered (verified-fact #1).
        # Still attempt to surface driver logs if a CLD dest exists.
        cld = _cld_destination(details)
        root = _match_allowlisted_root(cld, allowlist) if cld else None
        if root is None:
            return _err(
                "NOT_JOB_CLUSTER",
                detail="not a job cluster; executor logs are not delivered",
            )
        result = build_tree(
            lister, root=root, cluster_id=cluster_id, run_id=run_id, user=user,
            cld_dest=cld,
        )
        # Preserve the "not a job cluster" reason unless files were forbidden.
        if result.reason_code in ("OK", "NO_EXECUTOR_DIR", "NO_LOG_FILES"):
            return _err("NOT_JOB_CLUSTER", detail="not a job cluster",
                        tree=result.tree, run_meta=result.run_meta)
        return result

    cld = _cld_destination(details)
    if not cld:
        # A TERMINATED cluster is still gettable via clusters.get, but the API
        # frequently drops ``cluster_log_conf`` once it terminates. Before
        # declaring NO_CLD, probe the allowlisted roots for an existing
        # ``<root>/<cluster-id>/`` — the same fallback the metadata-aged-out
        # (exception) path uses. If the logs are sitting in an allowlisted
        # Volume, use it (this is exactly the browse->click case).
        root = _find_root_by_probe(cluster_id, allowlist, lister)
        if root is not None:
            return build_tree(
                lister, root=root, cluster_id=cluster_id, run_id=run_id, user=user
            )
        return _err("NO_CLD", detail="cluster has no cluster-log-delivery configured")

    root = _match_allowlisted_root(cld, allowlist)
    if root is None:
        # CLD dest is set but not one of the configured team roots (§3.3).
        return _err(
            "CLD_ROOT_NOT_FOUND",
            detail="log destination is not an allowlisted team root",
        )

    return build_tree(
        lister, root=root, cluster_id=cluster_id, run_id=run_id, user=user,
        cld_dest=cld,
    )


# --------------------------------------------------------------------------- #
# Top-level resolve (input -> tree), full staged pipeline                      #
# --------------------------------------------------------------------------- #
def _run_meta_from(run_obj) -> dict:
    state = getattr(run_obj, "state", None)
    return {
        "run_id": getattr(run_obj, "run_id", None),
        "job_id": getattr(run_obj, "job_id", None),
        "run_name": getattr(run_obj, "run_name", None),
        "state": runs_mod._life_cycle_state(state),
        "result_state": runs_mod._result_state(state),
        "run_page_url": getattr(run_obj, "run_page_url", None),
    }


def _is_terminated(run_obj) -> bool:
    lcs = runs_mod._life_cycle_state(getattr(run_obj, "state", None))
    return lcs == "TERMINATED"


def resolve(
    value: str,
    *,
    runs_client: runs_mod.RunsClient,
    cluster_meta: ClusterMetaClient,
    lister: FileLister,
    allowlist: list[str],
    user: str,
    task_run_id: Optional[int] = None,
) -> ResolveResult:
    """Full staged resolution from a raw lookup value (spec §2 data path).

    - Manual cluster-id -> ``resolve_cluster`` directly.
    - Run id -> [SP] ``get_run``; classify parent-vs-task:
        * multiple tasks and no ``task_run_id`` chosen ->
          ``PARENT_RUN_HAS_MULTIPLE_TASKS`` + task list.
        * running -> ``RUNNING``. not found -> ``RUN_NOT_FOUND``.
        * resolve the task's cluster instance -> ``resolve_cluster``.
    """
    kind = classify_input(value)

    if kind == "unknown":
        return _err("INPUT_AMBIGUOUS", detail="could not classify the input id")

    if kind == "cluster":
        # Manual mode (spec §3.3 / plan 2.3): user supplies a cluster-id.
        return resolve_cluster(
            value.strip(),
            cluster_meta=cluster_meta,
            lister=lister,
            allowlist=allowlist,
            user=user,
            run_id=value.strip(),
        )

    # kind == "run" — but a bare int could also be a JOB id (indistinguishable
    # by shape). Try it as a run first; if that 404s, fall back to treating it
    # as a job id and resolving the job's latest run that has a cluster.
    run_id = int(value.strip())

    def _as_job_fallback():
        try:
            latest = runs_mod.latest_run_id_for_job(runs_client, run_id)
        except Exception:  # noqa: BLE001
            return None
        if latest is None:
            return None
        try:
            return runs_mod.get_run_tasks(runs_client, latest)
        except Exception:  # noqa: BLE001
            return None

    run_obj = None
    tasks = []
    try:
        run_obj, tasks = runs_mod.get_run_tasks(runs_client, run_id)
    except Exception as exc:  # noqa: BLE001
        if _is_permission_denied(exc):
            return _err("RUN_NO_ACCESS", detail="no access to this run")
        if type(exc).__name__ in ("NotFound", "ResourceDoesNotExist"):
            fb = _as_job_fallback()
            if fb is not None:
                run_obj, tasks = fb
            else:
                return _err("RUN_NOT_FOUND",
                            detail="no run or job found for that identifier")
        else:
            return _err("RUN_NOT_FOUND", detail="run not found")

    if run_obj is None:
        return _err("RUN_NOT_FOUND",
                    detail="no run or job found for that identifier")

    run_meta = _run_meta_from(run_obj)

    # Running runs: do not attempt the Volume (CLD lags) — link out (spec §5).
    if not _is_terminated(run_obj) and run_meta.get("state") not in (None, "TERMINATED"):
        if run_meta.get("state") in ("RUNNING", "PENDING", "QUEUED", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"):
            return _err("RUNNING", run_meta=run_meta,
                        detail="run in progress; view the live Spark UI")

    # Multi-task parent: need a task selector (spec §2 step 2, §3.2).
    if len(tasks) > 1 and task_run_id is None:
        return _err(
            "PARENT_RUN_HAS_MULTIPLE_TASKS",
            run_meta=run_meta,
            tasks=[asdict(t) for t in tasks],
            detail="parent run has multiple tasks; choose one",
        )

    # Determine which (task) run carries the cluster instance.
    target_task_run_id: Optional[int]
    if task_run_id is not None:
        target_task_run_id = task_run_id
    elif len(tasks) == 1:
        target_task_run_id = tasks[0].run_id or run_id
    else:
        # No tasks listed (a submit run / single-node); use the run itself.
        target_task_run_id = run_id

    cluster_id = runs_mod.get_task_cluster_instance(runs_client, target_task_run_id)
    if not cluster_id:
        # A run with no cluster instance never ran on a classic Spark cluster —
        # e.g. a serverless task, a pipeline/DLT task, or a non-compute task
        # type. There is no executor CLD for these; say so plainly rather than
        # implying the run wasn't found.
        return _err("NO_CLUSTER_INSTANCE", run_meta=run_meta,
                    detail="This run didn't use a classic Spark cluster "
                           "(e.g. serverless or a pipeline/DLT task), so it has "
                           "no executor logs to show.")

    result = resolve_cluster(
        cluster_id,
        cluster_meta=cluster_meta,
        lister=lister,
        allowlist=allowlist,
        user=user,
        run_id=str(run_id),
    )
    # Attach run metadata for the UI header if the cluster stage didn't set it.
    if result.run_meta is None:
        result.run_meta = run_meta
    else:
        merged = dict(run_meta)
        merged.update(result.run_meta)
        result.run_meta = merged
    return result
