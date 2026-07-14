"""Enrich Volume-discovered clusters with job/run identity + friendly job name.

The primary cluster-discovery source (``/api/clusters``) now lists cluster-id
directories straight from the CLD Volume, so it is self-maintaining and needs no
per-cluster grants. But a Volume dir only knows the **cluster id** — no job name.

This module adds best-effort enrichment using the [APP SP]:

  1. ``clusters.get(cluster_id)`` -> ``cluster_name``. For a JOB cluster this is
     the synthetic name ``job-<jobId>-run-<runId>``, which we parse to recover
     the job id and run id (free, no jobs.get needed).
  2. ``jobs.get(jobId)`` -> ``settings.name`` -> the FRIENDLY job name.

Every step degrades gracefully and is NEVER fatal to the list:
  - ``clusters.get`` fails / not a job-cluster name -> leave job fields null.
  - ``jobs.get`` fails (e.g. the job was DELETED -> ResourceDoesNotExist) ->
    keep the parsed job_id / run_id, friendly name stays null.

A per-call cache (``jobs.get`` keyed by job id) means N clusters from the same
job cost ONE ``jobs.get``.

Identity: this is METADATA ONLY via the SP (job/run ids, job name). It reads no
log content. Content stays per-user UC-gated on click.
"""

from __future__ import annotations

import re
from typing import Optional, Protocol

# Databricks names a job cluster ``job-<jobId>-run-<runId>``.
_JOB_CLUSTER_NAME_RE = re.compile(r"^job-(\d+)-run-(\d+)$")


class ClusterNameMetaClient(Protocol):
    """[SP] seam for cluster + job metadata. Impl by ``SdkClusterNameMetaClient``."""

    def cluster_name(self, cluster_id: str) -> Optional[str]:
        """Return the cluster's ``cluster_name`` (or None if unavailable)."""

    def job_name(self, job_id: int) -> Optional[str]:
        """Return the job's friendly name (or None if unavailable/deleted)."""


class SdkClusterNameMetaClient:
    """Real [SP] impl backed by a WorkspaceClient. ISOLATED for testing."""

    def __init__(self, sp_client):
        self._sp = sp_client

    def cluster_name(self, cluster_id: str) -> Optional[str]:
        try:
            d = self._sp.clusters.get(cluster_id)
        except Exception:  # noqa: BLE001 - aged out / not visible -> unknown
            return None
        return getattr(d, "cluster_name", None) or None

    def job_name(self, job_id: int) -> Optional[str]:
        try:
            j = self._sp.jobs.get(job_id=job_id)
        except Exception:  # noqa: BLE001 - deleted job -> ResourceDoesNotExist
            return None
        settings = getattr(j, "settings", None)
        return getattr(settings, "name", None) if settings else None


def parse_job_run(cluster_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Parse ``job-<jobId>-run-<runId>`` -> (job_id, run_id) as strings, else
    (None, None) for non-job / all-purpose cluster names."""
    if not cluster_name:
        return (None, None)
    m = _JOB_CLUSTER_NAME_RE.match(cluster_name.strip())
    if not m:
        return (None, None)
    return (m.group(1), m.group(2))


def enrich_clusters(
    clusters: list[dict],
    meta: ClusterNameMetaClient,
    *,
    max_lookups: int = 100,
) -> list[dict]:
    """Add ``job_id`` / ``run_id`` / ``job_name`` to each cluster dict in place.

    ``clusters`` are the Volume-discovered rows (must have ``cluster_id``). We
    look up cluster metadata for each (capped at ``max_lookups`` to bound cost on
    a huge root), cache ``jobs.get`` by job id, and never raise: a lookup failure
    just leaves the corresponding fields null. Returns the same list for
    convenience.
    """
    job_name_cache: dict[str, Optional[str]] = {}
    looked_up = 0

    for c in clusters:
        c.setdefault("job_id", None)
        c.setdefault("run_id", None)
        c.setdefault("job_name", None)

        cid = c.get("cluster_id")
        if not cid or looked_up >= max_lookups:
            continue
        looked_up += 1

        cname = meta.cluster_name(cid)
        job_id, run_id = parse_job_run(cname)
        if job_id is None:
            # Not a job cluster (or name unavailable). Fall back to any name we
            # got so the row isn't blank.
            c["job_name"] = cname
            continue

        c["job_id"] = job_id
        c["run_id"] = run_id

        if job_id in job_name_cache:
            c["job_name"] = job_name_cache[job_id]
        else:
            try:
                name = meta.job_name(int(job_id))
            except (TypeError, ValueError):
                name = None
            job_name_cache[job_id] = name
            c["job_name"] = name

    return clusters
