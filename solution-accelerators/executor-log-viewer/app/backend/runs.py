"""Recent parent runs + parent/task/cluster helpers (spec §3.2, §3.3, runs.py).

**Identity: the app SERVICE PRINCIPAL, always.** Every call in this module goes
through the SP client (Jobs API). The OBO scope catalog on govcloud has no
``jobs`` scope, so the user token could not make these calls even if we tried
(§0 results). Log *content* is read elsewhere, via the user OBO token — never
here.

Responsibilities:
  - ``list_recent_runs``: paginated recent **parent** runs visible to the SP
    (``limit`` / ``page_token`` / ``terminated_only`` / ``job_id``). It does
    NOT pre-resolve each run's CLD root (too slow — §3.2); resolution happens
    lazily in ``resolver.py`` on selection.
  - ``get_run_tasks``: resolve a parent run -> its task runs (a pasted run ID
    is often a parent; cluster instances live on task runs — §2 step 2).
  - ``get_task_cluster_instance``: a (task) run -> its
    ``cluster_instance.cluster_id`` (§2 step 3).

The real Jobs SDK calls are ISOLATED behind ``RunsClient`` / ``SdkRunsClient``
(same pattern as ``logs.SdkFileReader``) so the exact SDK surface lives in one
place and tests inject a fake. Confirmed SDK names (databricks-sdk 0.44.0):
``jobs.list_runs(...) -> Iterator[BaseRun]`` and
``jobs.get_run(run_id) -> Run``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional, Protocol


# --------------------------------------------------------------------------- #
# Plain data shapes returned to the endpoint layer (SDK-object-free so the     #
# API response shaping and tests never depend on SDK internals).              #
# --------------------------------------------------------------------------- #
@dataclass
class RunSummary:
    """One row in the recent-runs list (spec §3.2 response)."""

    run_id: int
    job_id: Optional[int]
    job_name: Optional[str]
    state: Optional[str]  # life-cycle state, e.g. "TERMINATED" / "RUNNING"
    result_state: Optional[str]  # e.g. "SUCCESS" / "FAILED" (may be None)
    started_at: Optional[int]  # unix epoch millis
    run_page_url: Optional[str]
    cluster_id: Optional[str] = None  # resolved from tasks[].cluster_instance
    has_cluster: bool = False  # False => serverless/pipeline; no executor logs
    has_logs: bool = False  # cluster has CLD->Volume (executor logs deliverable)


@dataclass
class TaskSummary:
    """One task within a parent run (spec §2 step 2, §3.2 ``tasks``)."""

    run_id: int  # the task run id (used to resolve the cluster instance)
    task_key: Optional[str]
    state: Optional[str]
    cluster_id: Optional[str]  # from cluster_instance, if present


@dataclass
class RunsPage:
    runs: list = field(default_factory=list)  # list[RunSummary]
    next_page_token: Optional[str] = None


class RunsClient(Protocol):
    """Isolation seam over the SP Jobs API. Implemented by ``SdkRunsClient``."""

    def list_runs(
        self,
        *,
        limit: Optional[int],
        page_token: Optional[str],
        completed_only: Optional[bool],
        job_id: Optional[int],
    ) -> Iterable:
        """Return an iterator of SDK ``BaseRun`` objects (parent runs)."""

    def get_run(self, run_id: int):
        """Return the SDK ``Run`` object (with ``tasks`` / ``cluster_instance``)."""


class SdkRunsClient:
    """Real ``RunsClient`` backed by the app-SP ``WorkspaceClient`` Jobs API.

    ISOLATED so the exact SDK method names live in exactly one place. If a
    future SDK renames these, only this class changes.
    """

    def __init__(self, sp_client):
        self._sp = sp_client

    def list_runs(self, *, limit, page_token, completed_only, job_id):
        # jobs.list_runs is an iterator; we page manually (see list_recent_runs).
        # expand_tasks=True so each row carries tasks[].cluster_instance — this
        # lets us classify cluster vs serverless/pipeline WITHOUT a get_run per
        # row (still one API call), so we can filter clusterless runs (which
        # have no executor logs) out of the browsable list by default.
        return self._sp.jobs.list_runs(
            limit=limit,
            page_token=page_token,
            completed_only=completed_only,
            job_id=job_id,
            expand_tasks=True,
        )

    def get_run(self, run_id: int):
        return self._sp.jobs.get_run(run_id=run_id)


def _life_cycle_state(state_obj) -> Optional[str]:
    """Extract a string life-cycle state from an SDK RunState (enum or str)."""
    if state_obj is None:
        return None
    lcs = getattr(state_obj, "life_cycle_state", None)
    if lcs is None:
        return None
    return getattr(lcs, "value", None) or str(lcs)


def _result_state(state_obj) -> Optional[str]:
    if state_obj is None:
        return None
    rs = getattr(state_obj, "result_state", None)
    if rs is None:
        return None
    return getattr(rs, "value", None) or str(rs)


def _cluster_id_from_run(base_run) -> Optional[str]:
    """Extract a run's cluster id from its tasks[].cluster_instance (requires
    expand_tasks). Returns None for serverless / pipeline / non-compute runs
    that never bound a classic Spark cluster (and thus have no executor logs).
    """
    ci = getattr(base_run, "cluster_instance", None)
    if ci is not None and getattr(ci, "cluster_id", None):
        return ci.cluster_id
    for t in getattr(base_run, "tasks", None) or []:
        tci = getattr(t, "cluster_instance", None)
        if tci is not None and getattr(tci, "cluster_id", None):
            return tci.cluster_id
    return None


def list_recent_runs(
    runs_client: RunsClient,
    *,
    limit: int = 25,
    page_token: Optional[str] = None,
    terminated_only: bool = False,
    job_id: Optional[int] = None,
    include_clusterless: bool = False,
    cluster_meta=None,  # optional ClusterMetaClient: enables CLD-based filtering
    has_logs_fn=None,  # (cluster_details) -> bool; injected to avoid import cycle
) -> RunsPage:
    """List recent **parent** runs (spec §3.2). Shapes SDK objects into
    ``RunSummary`` rows and surfaces the next page token.

    Cluster classification is free (tasks[].cluster_instance via expand_tasks).
    "Has executor logs" is NOT free — it needs the cluster's ``cluster_log_conf``
    (a ``clusters.get``). When ``cluster_meta`` + ``has_logs_fn`` are supplied,
    each DISTINCT cluster is fetched once (cached within the page) and, by
    default, only runs whose cluster delivers logs to a Volume are shown — so
    the browsable list is "runs you can actually open." ``include_clusterless``
    turns off ALL filtering (shows serverless/pipeline + no-CLD runs too, each
    carrying ``run_page_url`` to open in Databricks).

    Without ``cluster_meta`` (e.g. unit tests, or a fast path), it falls back to
    the cheap cluster-only filter: hide only clusterless runs.
    """
    # jobs/runs/list caps limit at 25, but the SDK returns an auto-paginating
    # iterator — so we KEEP CONSUMING past the first page until we've collected
    # `limit` qualifying rows, bounded by a scan cap so a long tail of
    # non-qualifying runs can't loop unboundedly.
    page_limit = min(limit, 25)
    scan_cap = limit if include_clusterless else max(limit * 8, 200)
    iterator = runs_client.list_runs(
        limit=page_limit,
        page_token=page_token,
        completed_only=terminated_only or None,
        job_id=job_id,
    )

    do_cld_filter = cluster_meta is not None and has_logs_fn is not None
    # Tri-state per cluster: True (confirmed CLD->Volume), False (confirmed no
    # CLD), or None (COULDN'T CHECK — cluster metadata gone or the app SP lacks
    # cluster read). We only HIDE confirmed-False; "unknown" stays visible so a
    # missing SP cluster grant or an aged-out cluster never nukes the whole list
    # (the resolve path checks for real on click).
    cld_cache: dict = {}

    def _cluster_has_logs(cid: str):  # -> Optional[bool]
        if cid in cld_cache:
            return cld_cache[cid]
        try:
            details = cluster_meta.get_cluster(cid)
            result = bool(has_logs_fn(details))
        except Exception:  # noqa: BLE001 - can't confirm either way
            result = None  # unknown
        cld_cache[cid] = result
        return result

    rows: list[RunSummary] = []
    next_token: Optional[str] = None
    scanned = 0
    for base_run in iterator:
        if len(rows) >= limit or scanned >= scan_cap:
            break
        scanned += 1
        cluster_id = _cluster_id_from_run(base_run)
        has_cluster = cluster_id is not None

        # THE ONLY DEFAULT FILTER: hide runs with no Spark cluster at all
        # (serverless / pipeline) — those can never have executor logs. Every
        # cluster-bearing run STAYS in the list; the click resolves whether it
        # actually has logs. We never gate the list on a per-cluster CLD read
        # (that needs SP cluster access we may not have, and made the list go
        # empty). CLD is a best-effort BADGE only (has_logs), never a filter.
        if not include_clusterless and not has_cluster:
            continue

        cld_state = None  # None => unknown / not checked (badge only)
        if has_cluster and do_cld_filter:
            cld_state = _cluster_has_logs(cluster_id)
        has_logs = cld_state is True  # confirmed CLD->Volume; else False/unknown

        rows.append(
            RunSummary(
                run_id=int(getattr(base_run, "run_id", 0) or 0),
                job_id=getattr(base_run, "job_id", None),
                job_name=getattr(base_run, "run_name", None),
                state=_life_cycle_state(getattr(base_run, "state", None)),
                result_state=_result_state(getattr(base_run, "state", None)),
                started_at=getattr(base_run, "start_time", None),
                run_page_url=getattr(base_run, "run_page_url", None),
                cluster_id=cluster_id,
                has_cluster=has_cluster,
                has_logs=has_logs,
            )
        )
        token = getattr(base_run, "next_page_token", None)
        if token:
            next_token = token

    return RunsPage(runs=rows, next_page_token=next_token)


def latest_run_id_for_job(runs_client: RunsClient, job_id: int) -> Optional[int]:
    """Return the most recent run id for a job, preferring a run that bound a
    cluster (so a pasted JOB id resolves to a run that actually has logs).

    Job IDs and run IDs are indistinguishable by shape; the resolver tries a
    numeric input as a run first and falls back here when that 404s.
    """
    first_run_id: Optional[int] = None
    for base_run in runs_client.list_runs(
        limit=25, page_token=None, completed_only=None, job_id=job_id
    ):
        rid = int(getattr(base_run, "run_id", 0) or 0)
        if first_run_id is None:
            first_run_id = rid  # newest overall, as a fallback
        if _cluster_id_from_run(base_run) is not None:
            return rid  # newest run that actually has a cluster
    return first_run_id


def get_run_tasks(runs_client: RunsClient, run_id: int) -> tuple:
    """Resolve a run to ``(run_obj, [TaskSummary, ...])``.

    A pasted run ID is often a *parent* run whose tasks each carry their own
    ``cluster_instance`` (§2 step 2). Returns the raw run object (so the caller
    can read state / job_id / run_name) plus its tasks as plain summaries.
    Returns ``(None, [])`` if the run object is falsy.
    """
    run = runs_client.get_run(run_id)
    if run is None:
        return None, []

    tasks: list[TaskSummary] = []
    for t in getattr(run, "tasks", None) or []:
        ci = getattr(t, "cluster_instance", None)
        tasks.append(
            TaskSummary(
                run_id=int(getattr(t, "run_id", 0) or 0),
                task_key=getattr(t, "task_key", None),
                state=_life_cycle_state(getattr(t, "state", None)),
                cluster_id=getattr(ci, "cluster_id", None) if ci else None,
            )
        )
    return run, tasks


def get_task_cluster_instance(runs_client: RunsClient, task_run_id: int) -> Optional[str]:
    """Return a (task) run's ``cluster_instance.cluster_id`` or None (§2 step 3).

    Fetches the run fresh so it works whether the caller has a task-run id in
    hand or re-resolves one from ``get_run_tasks``.
    """
    run = runs_client.get_run(task_run_id)
    if run is None:
        return None
    ci = getattr(run, "cluster_instance", None)
    if ci is not None and getattr(ci, "cluster_id", None):
        return ci.cluster_id
    # A parent run's cluster instance can live on its single task instead.
    tasks = getattr(run, "tasks", None) or []
    if len(tasks) == 1:
        tci = getattr(tasks[0], "cluster_instance", None)
        if tci is not None:
            return getattr(tci, "cluster_id", None)
    return None
