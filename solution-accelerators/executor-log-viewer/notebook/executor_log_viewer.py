# Databricks notebook source
# MAGIC %md
# MAGIC # Executor Log Viewer (notebook edition)
# MAGIC
# MAGIC Read Spark **executor** / **driver** `stdout`/`stderr` logs for a
# MAGIC **terminated job cluster** — the case where the Spark History Server
# MAGIC "Logs" link 404s because the cluster is gone.
# MAGIC
# MAGIC This is the notebook counterpart to the Executor Log Viewer app. It does the
# MAGIC same job with far less setup — **no app deploy, no service principal, no
# MAGIC OBO scope, no signing secret.** Attach it to any cluster and run.
# MAGIC
# MAGIC ### How it works
# MAGIC - Everything runs **as you** (the notebook's executing identity). Unity
# MAGIC   Catalog enforces exactly which log Volumes you can read — no second
# MAGIC   identity, no signed file refs needed. You either have `READ VOLUME` on the
# MAGIC   log Volume or you don't.
# MAGIC - `clusters.list` / `clusters.get` provide cluster **metadata** (which
# MAGIC   cluster, where it delivered logs). The Files API reads log **content**.
# MAGIC   Both under your token.
# MAGIC - Cluster Log Delivery (CLD) writes executor logs — **only for job
# MAGIC   clusters** — to:
# MAGIC   `<cld-root>/<cluster-id>/executor/<application-id>/<executor-id>/{stdout,stderr}`
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Your job clusters already deliver logs to a **UC Volume** (cluster/job →
# MAGIC   *Advanced → Logging → Destination = Volume*). This notebook reads what CLD
# MAGIC   delivered; it does not configure delivery.
# MAGIC - You have `USE CATALOG` / `USE SCHEMA` / `READ VOLUME` on that Volume.
# MAGIC - `databricks-sdk` is installed (it ships in every Databricks Runtime).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configure — set your CLD log root(s)
# MAGIC
# MAGIC Comma-separate multiple roots. These are the UC Volume paths your job
# MAGIC clusters deliver logs to. Only clusters/paths **under** one of these roots
# MAGIC are surfaced or read (an allowlist — the same containment rule the app uses).

# COMMAND ----------

dbutils.widgets.text("cld_root_allowlist", "/Volumes/<catalog>/<schema>/<cld_volume>", "CLD log root(s), comma-separated")
dbutils.widgets.text("lookup", "", "Cluster ID or run/job ID (optional)")
dbutils.widgets.dropdown("mode", "tail", ["tail", "full"], "Read mode")
dbutils.widgets.dropdown("recent_limit", "25", ["10", "25", "50", "100"], "Recent clusters to list")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Library — resolution + read logic
# MAGIC
# MAGIC Self-contained (no imports from the app package) so the notebook is a
# MAGIC single portable file. The path/CLD/log-file rules mirror the app's
# MAGIC `resolver.py`, `logfiles.py`, and `clusters_source.py` exactly.

# COMMAND ----------

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass, field
from typing import Optional

from databricks.sdk import WorkspaceClient

# One client, the NOTEBOOK's own identity (no SP, no OBO). UC enforces access.
w = WorkspaceClient()


# --- path containment (mirrors app/backend/paths.py) ------------------------ #
def normalize_path(path: str) -> str:
    """Collapse '..'/'.'/duplicate slashes to an absolute normalized path."""
    if not path:
        return path
    p = posixpath.normpath(path)
    return p


def _segments(path: str) -> list[str]:
    return [s for s in normalize_path(path).split("/") if s]


def is_under_root(path: str, root: str) -> bool:
    """Segment-aware containment: True iff ``path`` == ``root`` or is nested
    under it. Segment-aware so ``/Volumes/a/root-evil`` is NOT under
    ``/Volumes/a/root`` (a plain prefix check would wrongly accept it)."""
    ps, rs = _segments(path), _segments(root)
    if len(ps) < len(rs):
        return False
    return ps[: len(rs)] == rs


def join_under_root(root: str, *parts: str) -> str:
    return normalize_path(posixpath.join(root, *parts))


# --- recognized-log-file boundary (mirrors app/backend/logfiles.py) --------- #
_ROTATED_RE = re.compile(r"^std(?:out|err)(?:--.+)?$")


def classify_log_file(name_or_path: str) -> Optional[str]:
    """Return 'stdout'/'stderr'/'log' for a recognized log basename, else None.
    Rejects e.g. 'stderr_credentials.json' (no '--' and not '*.log')."""
    base = name_or_path.rstrip("/").rsplit("/", 1)[-1]
    if _ROTATED_RE.match(base):
        return "stdout" if base.startswith("stdout") else "stderr"
    if base.endswith(".log"):
        return "log"
    return None


# --- config ----------------------------------------------------------------- #
def load_allowlist(raw: str) -> list[str]:
    return [normalize_path(p.strip()) for p in (raw or "").split(",") if p.strip()]


# --- CLD metadata extraction (mirrors app/backend/resolver.py) -------------- #
def _is_job_cluster(details) -> bool:
    source = getattr(details, "cluster_source", None)
    val = getattr(source, "value", None) or (str(source) if source is not None else "")
    return str(val).upper().endswith("JOB")


def _cld_destination(details) -> Optional[str]:
    conf = getattr(details, "cluster_log_conf", None)
    if conf is None:
        return None
    volumes = getattr(conf, "volumes", None)
    if volumes is None:
        return None
    return getattr(volumes, "destination", None)


def _match_allowlisted_root(cld_dest: str, allowlist: list[str]) -> Optional[str]:
    if not cld_dest:
        return None
    ndest = normalize_path(cld_dest)
    for root in allowlist:
        if is_under_root(ndest, root):
            return root
    return None


def _stringify_enum(value) -> Optional[str]:
    if value is None:
        return None
    val = getattr(value, "value", None)
    if val is not None:
        return str(val)
    s = str(value)
    return s.rsplit(".", 1)[-1] if "." in s else s


# --- input classification (mirrors app/backend/resolver.py) ----------------- #
_CLUSTER_ID_RE = re.compile(r"^\d{4}-\d{4,}-[a-z0-9]+$|^\d{4}-[a-z0-9]+$")


def classify_input(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return "unknown"
    if _CLUSTER_ID_RE.match(v):
        return "cluster"
    if v.isdigit():
        return "run"
    return "unknown"


# --- listing helpers (Files API, as the notebook user) ---------------------- #
def _list_dir(path: str):
    """List a Volume directory. Returns [] on not-found; re-raises a real
    permission denial so the caller can report it honestly."""
    try:
        return list(w.files.list_directory_contents(path))
    except Exception as exc:  # noqa: BLE001
        name = type(exc).__name__
        if "PermissionDenied" in name or name in ("Forbidden",):
            raise
        if name in ("NotFound", "ResourceDoesNotExist", "FileNotFoundError"):
            return []
        return []


# --- result shapes ---------------------------------------------------------- #
@dataclass
class LogFileNode:
    name: str
    file_kind: str
    path: str            # full Volume path (notebook is trusted — no signed ref)
    size: Optional[int]
    modified: Optional[int]


@dataclass
class ExecutorNode:
    app_id: str
    executor_id: str
    files: list = field(default_factory=list)


@dataclass
class LogTree:
    cluster_id: str
    cld_root: str
    reason_code: str
    executors: list = field(default_factory=list)   # list[ExecutorNode]
    driver_files: list = field(default_factory=list)  # list[LogFileNode]
    detail: Optional[str] = None


def _collect_log_files(dir_path: str) -> list[LogFileNode]:
    nodes: list[LogFileNode] = []
    for entry in _list_dir(dir_path):
        if getattr(entry, "is_directory", False):
            continue
        name = getattr(entry, "name", None)
        if not name:
            continue
        kind = classify_log_file(name)
        if kind is None:
            continue  # not a recognized log file
        nodes.append(
            LogFileNode(
                name=name,
                file_kind=kind,
                path=join_under_root(dir_path, name),
                size=getattr(entry, "file_size", None),
                modified=getattr(entry, "last_modified", None),
            )
        )
    return nodes


def build_tree(cld_dest: str, root: str, cluster_id: str) -> LogTree:
    """List <cld_dest>/<cluster-id>/{driver, executor/<app>/<exec>/} as the
    notebook user. Reason codes mirror the app's resolver."""
    cluster_dir = join_under_root(cld_dest, cluster_id)
    tree = LogTree(cluster_id=cluster_id, cld_root=root, reason_code="OK")

    try:
        top = _list_dir(cluster_dir)
    except Exception:  # permission denied
        tree.reason_code = "FILES_FORBIDDEN"
        tree.detail = f"No READ on this Volume for {cluster_dir}"
        return tree

    if not top:
        tree.reason_code = "DELIVERY_PENDING"
        tree.detail = "Cluster dir not present — logs not delivered yet (lag) or aged out."
        return tree

    top_names = {getattr(e, "name", ""): e for e in top}

    if "driver" in top_names:
        tree.driver_files = _collect_log_files(join_under_root(cluster_dir, "driver"))

    if "executor" in top_names:
        exec_dir = join_under_root(cluster_dir, "executor")
        for app_entry in _list_dir(exec_dir):
            if not getattr(app_entry, "is_directory", False):
                continue
            app_id = getattr(app_entry, "name", None)
            if not app_id:
                continue
            app_dir = join_under_root(exec_dir, app_id)
            for exec_entry in _list_dir(app_dir):
                if not getattr(exec_entry, "is_directory", False):
                    continue
                exec_id = getattr(exec_entry, "name", None)
                if not exec_id:
                    continue
                files = _collect_log_files(join_under_root(app_dir, exec_id))
                tree.executors.append(ExecutorNode(app_id=app_id, executor_id=exec_id, files=files))

    has_exec_files = any(ex.files for ex in tree.executors)
    if not tree.executors and "executor" not in top_names:
        tree.reason_code = "NO_EXECUTOR_DIR"
        tree.detail = "No executor/ dir (e.g. an all-purpose cluster). Driver logs shown if present."
    elif not has_exec_files and not tree.driver_files:
        tree.reason_code = "NO_LOG_FILES"
        tree.detail = "Delivered dirs contain no recognized log files."
    return tree


def resolve_cluster(cluster_id: str, allowlist: list[str]) -> LogTree:
    """clusters.get -> classify job cluster -> CLD dest -> allowlist -> tree.
    Falls back to probing allowlisted roots if metadata has aged out."""
    try:
        details = w.clusters.get(cluster_id)
    except Exception:  # noqa: BLE001 - metadata aged out / not found
        for root in allowlist:
            if _list_dir(join_under_root(root, cluster_id)):
                return build_tree(root, root, cluster_id)
        t = LogTree(cluster_id=cluster_id, cld_root="", reason_code="CLUSTER_METADATA_UNAVAILABLE")
        t.detail = "Cluster metadata unavailable and no allowlisted root contains this cluster."
        return t

    cld = _cld_destination(details)
    if not cld:
        t = LogTree(cluster_id=cluster_id, cld_root="", reason_code="NO_CLD")
        t.detail = "Cluster has no cluster-log-delivery configured."
        return t

    root = _match_allowlisted_root(cld, allowlist)
    if root is None:
        t = LogTree(cluster_id=cluster_id, cld_root="", reason_code="CLD_ROOT_NOT_FOUND")
        t.detail = f"Log destination {cld} is not under an allowlisted root."
        return t

    tree = build_tree(cld, root, cluster_id)
    if not _is_job_cluster(details) and tree.reason_code in ("OK", "NO_EXECUTOR_DIR", "NO_LOG_FILES"):
        # Non-job cluster: executor CLD isn't delivered. Driver may still be shown.
        tree.detail = ("Not a job cluster — executor logs are not delivered. "
                       + (tree.detail or ""))
    return tree


def resolve(value: str, allowlist: list[str]) -> LogTree:
    """A cluster id resolves directly; a numeric id is treated as a run (then a
    job) and its task's cluster instance is resolved."""
    kind = classify_input(value)
    if kind == "cluster":
        return resolve_cluster(value.strip(), allowlist)
    if kind != "run":
        t = LogTree(cluster_id="", cld_root="", reason_code="INPUT_AMBIGUOUS")
        t.detail = f"Could not classify {value!r} as a cluster id or a numeric run/job id."
        return t

    run_id = int(value.strip())
    cluster_id = _cluster_id_for_run_or_job(run_id)
    if not cluster_id:
        t = LogTree(cluster_id="", cld_root="", reason_code="NO_CLUSTER_INSTANCE")
        t.detail = ("This run/job didn't use a classic Spark cluster (e.g. serverless "
                    "or a pipeline/DLT task), so it has no executor logs.")
        return t
    return resolve_cluster(cluster_id, allowlist)


def _cluster_id_from_run_obj(run) -> Optional[str]:
    ci = getattr(run, "cluster_instance", None)
    if ci is not None and getattr(ci, "cluster_id", None):
        return ci.cluster_id
    for t in getattr(run, "tasks", None) or []:
        tci = getattr(t, "cluster_instance", None)
        if tci is not None and getattr(tci, "cluster_id", None):
            return tci.cluster_id
    return None


def _cluster_id_for_run_or_job(run_id: int) -> Optional[str]:
    """Try the id as a run; if that 404s, treat it as a job id and use its
    latest run that bound a cluster."""
    try:
        run = w.jobs.get_run(run_id=run_id)
        cid = _cluster_id_from_run_obj(run)
        if cid:
            return cid
    except Exception:  # noqa: BLE001 - maybe it's a job id
        pass
    try:
        for base_run in w.jobs.list_runs(job_id=run_id, limit=25, expand_tasks=True):
            cid = _cluster_id_from_run_obj(base_run)
            if cid:
                return cid
    except Exception:  # noqa: BLE001
        return None
    return None


# --- content read (mirrors app/backend/logs.py tail/full) ------------------- #
TAIL_BYTES = 256 * 1024
FULL_CAP_BYTES = 10 * 1024 * 1024


def read_log(path: str, mode: str = "tail") -> tuple[str, dict]:
    """Return (text, meta) for a log file under the user's token. 'tail' = last
    256 KB; 'full' = whole file up to a 10 MB cap."""
    resp = w.files.download(path)
    contents = getattr(resp, "contents", resp)
    data = contents.read() if hasattr(contents, "read") else (
        contents.encode("utf-8") if isinstance(contents, str) else bytes(contents))
    total = len(data)
    if mode == "full":
        if total > FULL_CAP_BYTES:
            return "", {"outcome": "FILE_TOO_LARGE", "total_size": total}
        window = data
        start = 0
    else:
        start = max(0, total - TAIL_BYTES)
        window = data[start:]
    return window.decode("utf-8", "replace"), {
        "outcome": "OK", "total_size": total, "range_start": start,
        "range_end": total, "earlier_hidden": start,
    }


print("Library loaded. Identity:", w.current_user.me().user_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Recent clusters with executor logs
# MAGIC
# MAGIC Enumerates clusters you can see via `clusters.list` and keeps the ones
# MAGIC whose CLD destination is under your allowlisted root(s) — the same
# MAGIC "clusters that have logs" list the app's left rail shows. Most-recent first.

# COMMAND ----------

ALLOWLIST = load_allowlist(dbutils.widgets.get("cld_root_allowlist"))
RECENT_LIMIT = int(dbutils.widgets.get("recent_limit"))

if not ALLOWLIST or ALLOWLIST == ["/Volumes/<catalog>/<schema>/<cld_volume>"]:
    print("⚠️  Set the 'cld_root_allowlist' widget to your CLD Volume path(s) first.")
    recent_rows = []
else:
    recent_rows = []
    for cluster in w.clusters.list():
        cid = getattr(cluster, "cluster_id", None)
        if not cid:
            continue
        cld = _cld_destination(cluster)
        if not cld:
            continue
        root = _match_allowlisted_root(cld, ALLOWLIST)
        if root is None:
            continue
        recent_rows.append({
            "cluster_id": cid,
            "cluster_name": getattr(cluster, "cluster_name", None),
            "state": _stringify_enum(getattr(cluster, "state", None)),
            "source": _stringify_enum(getattr(cluster, "cluster_source", None)),
            "cld_dest": cld,
            "terminated_at": getattr(cluster, "terminated_time", None),
            "started_at": getattr(cluster, "start_time", None),
        })
    # most-recent first (terminated_time, else start_time)
    recent_rows.sort(
        key=lambda r: -((r["terminated_at"] or r["started_at"] or 0))
    )
    recent_rows = recent_rows[:RECENT_LIMIT]

if recent_rows:
    display(spark.createDataFrame(recent_rows))  # noqa: F821 (spark is provided)
else:
    print("No clusters with CLD under an allowlisted root were found "
          "(or the allowlist isn't set). You can still paste an ID in step 4.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resolve — cluster ID, run ID, or job ID
# MAGIC
# MAGIC Put an ID in the `lookup` widget (a dashed cluster id like
# MAGIC `0710-123456-abc123`, or a numeric run/job id) and run this cell. Leaving it
# MAGIC blank uses the most-recent cluster from step 3.

# COMMAND ----------

lookup = dbutils.widgets.get("lookup").strip()
if not lookup and recent_rows:
    lookup = recent_rows[0]["cluster_id"]
    print(f"No lookup provided — using most-recent cluster: {lookup}")

if not lookup:
    print("Provide a cluster/run/job id in the 'lookup' widget, or populate step 3 first.")
    tree = None
else:
    tree = resolve(lookup, ALLOWLIST)
    print(f"cluster_id : {tree.cluster_id or '(unresolved)'}")
    print(f"cld_root   : {tree.cld_root or '(n/a)'}")
    print(f"reason     : {tree.reason_code}")
    if tree.detail:
        print(f"detail     : {tree.detail}")
    print(f"executors  : {len(tree.executors)} ({sum(len(e.files) for e in tree.executors)} files)")
    print(f"driver     : {len(tree.driver_files)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Browse the resolved log files
# MAGIC
# MAGIC Every executor + driver log file found, with its full path and size. Copy a
# MAGIC `path` from here into step 6 to read it.

# COMMAND ----------

if tree:
    file_rows = []
    for ex in tree.executors:
        for f in ex.files:
            file_rows.append({
                "scope": f"executor {ex.executor_id}", "app_id": ex.app_id,
                "kind": f.file_kind, "name": f.name, "size": f.size, "path": f.path,
            })
    for f in tree.driver_files:
        file_rows.append({
            "scope": "driver", "app_id": "-", "kind": f.file_kind,
            "name": f.name, "size": f.size, "path": f.path,
        })
    if file_rows:
        display(spark.createDataFrame(file_rows))  # noqa: F821
    else:
        print(f"No log files to show (reason: {tree.reason_code}).")
else:
    print("Resolve a cluster/run in step 4 first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Read a log file
# MAGIC
# MAGIC Set `READ_PATH` to a `path` from step 5 (or leave it `None` to auto-pick the
# MAGIC first executor `stderr`, the usual place to look). Mode comes from the
# MAGIC `mode` widget (`tail` = last 256 KB, `full` = whole file up to 10 MB).

# COMMAND ----------

READ_PATH = None  # e.g. "/Volumes/<catalog>/<schema>/<vol>/<cluster>/executor/<app>/0/stderr"
MODE = dbutils.widgets.get("mode")

if READ_PATH is None and tree:
    # Auto-pick: first executor stderr, else first executor file, else driver.
    for ex in tree.executors:
        for f in ex.files:
            if f.file_kind == "stderr":
                READ_PATH = f.path
                break
        if READ_PATH:
            break
    if READ_PATH is None:
        for ex in tree.executors:
            if ex.files:
                READ_PATH = ex.files[0].path
                break
    if READ_PATH is None and tree.driver_files:
        READ_PATH = tree.driver_files[0].path

if not READ_PATH:
    print("No READ_PATH set and nothing to auto-pick. Set READ_PATH to a path from step 5.")
else:
    print(f"Reading ({MODE}): {READ_PATH}\n")
    try:
        text, meta = read_log(READ_PATH, mode=MODE)
        print(f"--- {meta} ---\n")
        if meta["outcome"] == "FILE_TOO_LARGE":
            print(f"File is {meta['total_size']:,} bytes — over the {FULL_CAP_BYTES:,} "
                  f"byte 'full' cap. Switch the mode widget to 'tail'.")
        else:
            print(text)
    except Exception as exc:  # noqa: BLE001
        name = type(exc).__name__
        if "PermissionDenied" in name or name == "Forbidden":
            print(f"403 — you don't have READ on this Volume/file. "
                  f"Ask for READ VOLUME on {'/'.join(READ_PATH.split('/')[:5])}.")
        else:
            print(f"Read failed ({name}): {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes / troubleshooting
# MAGIC
# MAGIC | Reason code | Meaning | What to do |
# MAGIC |---|---|---|
# MAGIC | `DELIVERY_PENDING` | Cluster dir not in the Volume yet | CLD lags termination by a bit — retry shortly |
# MAGIC | `NO_EXECUTOR_DIR` | No `executor/` (e.g. all-purpose cluster) | Executor CLD is only delivered for **job** clusters; driver may still show |
# MAGIC | `NO_CLD` | Cluster has no log delivery configured | Configure CLD → Volume on the cluster/job |
# MAGIC | `CLD_ROOT_NOT_FOUND` | Logs deliver somewhere not in your allowlist | Add that Volume to the `cld_root_allowlist` widget |
# MAGIC | `NO_CLUSTER_INSTANCE` | Serverless / pipeline / DLT run | No classic Spark cluster → no executor logs |
# MAGIC | `FILES_FORBIDDEN` / 403 | You lack `READ VOLUME` | Get `READ VOLUME` (+ `USE CATALOG`/`USE SCHEMA`) on the Volume |
# MAGIC
# MAGIC This notebook reads only recognized log files (`stdout`, `stderr`, `*.log`,
# MAGIC rotated variants) — the same boundary as the app. It runs entirely as you,
# MAGIC so Unity Catalog is the access control; there is no separate service
# MAGIC principal or signed file ref to manage.
