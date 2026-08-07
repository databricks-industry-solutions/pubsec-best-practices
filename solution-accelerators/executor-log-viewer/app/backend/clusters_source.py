"""Legacy SP-sourced "recent clusters with logs" source — CURRENTLY UNUSED.

NOTE: ``/api/clusters`` no longer uses this module. The recent-clusters list is
now built from the CLD **Volume itself**, listed with the viewing user's OBO
token (see ``app.get_clusters`` -> ``browse.browse_root``), then enriched with
friendly job names via ``cluster_names``. That is self-maintaining and needs no
SP or per-cluster grants to populate. This module is kept for reference as an
alternative source; it is not imported by the app.

WHY THE SWITCH: the SP's ``clusters.list`` only returns clusters the SP can
*view*, so freshly-created job clusters (owned by users' jobs, not the SP) never
appeared without per-job grants — not self-maintaining. Listing the Volume the
user can read solves that: any cluster that *delivered* logs shows up, UC-gated
per user.

WHAT THIS MODULE DID: the app SP's ``clusters.list`` returns clusters (incl.
TERMINATED) without any per-cluster grant, and each carries ``cluster_log_conf``
-> the CLD Volume path. It enumerated clusters via the SP, kept the ones whose
CLD destination is an allowlisted Volume, and returned a "clusters that have
executor logs" list.

IDENTITY (spec §2, §3.3) — this is METADATA ONLY:
  - The SP ``clusters.list`` is used to enumerate cluster metadata + CLD PATHS.
    It NEVER reads log file CONTENT.
  - Log CONTENT stays gated per-user by the OBO Files read on click: clicking a
    cluster goes through ``resolver.resolve`` (dashed id -> ``resolve_cluster``),
    which lists + reads under the USER's OBO token. This module returns paths,
    not content, so no user token is needed to build the list.

Isolation seam (mirrors ``resolver.SdkClusterMetaClient`` /
``runs.SdkRunsClient``): the real SDK ``clusters.list()`` call lives in exactly
one place (``SdkClustersLister``) behind a thin ``ClustersLister`` Protocol so
tests inject a fake without the SDK.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Optional, Protocol

import resolver


# --------------------------------------------------------------------------- #
# Identity seam — [SP] clusters.list enumeration                               #
# --------------------------------------------------------------------------- #
class ClustersLister(Protocol):
    """[SP] cluster enumeration seam. Implemented by ``SdkClustersLister``."""

    def list(self) -> Iterable:
        """Return an iterator of SDK ``ClusterDetails`` (incl. TERMINATED),
        each carrying ``cluster_log_conf`` / ``cluster_source`` / ``state``."""


class SdkClustersLister:
    """Real [SP] cluster lister. ISOLATED (like ``SdkClusterMetaClient``).

    Confirmed SDK name (databricks-sdk 0.44.0):
    ``clusters.list() -> Iterator[ClusterDetails]``.
    """

    def __init__(self, sp_client):
        self._sp = sp_client

    def list(self):
        return self._sp.clusters.list()


# --------------------------------------------------------------------------- #
# Result shape                                                                 #
# --------------------------------------------------------------------------- #
@dataclass
class ClusterEntry:
    """One row in the "recent clusters with logs" list.

    Metadata only — ``cld_dest`` is a PATH, never file content. Clicking reuses
    the resolve path keyed on ``cluster_id``.
    """

    cluster_id: str
    cluster_name: Optional[str]
    state: Optional[str]  # stringified enum, e.g. "TERMINATED" / "RUNNING"
    cluster_source: Optional[str]  # stringified, e.g. "JOB" / "UI" / "API"
    cld_dest: str  # the allowlisted CLD Volume destination
    started_at: Optional[int]  # unix epoch millis (start_time), for sorting
    terminated_at: Optional[int]  # unix epoch millis (terminated_time)


def _stringify_enum(value) -> Optional[str]:
    """Stringify an SDK enum (or plain str) to its bare name.

    SDK enums expose ``.value`` (e.g. ``ClusterSource.JOB.value == "JOB"`` or an
    enum ``State.TERMINATED``). We prefer ``.value``; else ``str()`` and strip
    any ``ClassName.`` prefix so we return e.g. "TERMINATED", not
    "State.TERMINATED".
    """
    if value is None:
        return None
    val = getattr(value, "value", None)
    if val is not None:
        return str(val)
    s = str(value)
    # Strip an "EnumClass.MEMBER" prefix if str() gave us the repr form.
    if "." in s:
        s = s.rsplit(".", 1)[-1]
    return s


def _sort_key(entry: ClusterEntry) -> tuple:
    """Most-recent-first. Prefer terminated_at, else started_at; unknown last.

    Returns a tuple sortable ascending where "more recent" sorts EARLIER:
    (has_no_timestamp, -timestamp, cluster_id). Entries with no known timestamp
    sort after all timestamped ones.
    """
    ts = entry.terminated_at if entry.terminated_at is not None else entry.started_at
    return (ts is None, -(ts or 0), entry.cluster_id)


def list_clusters_with_logs(
    sp_clusters_client: ClustersLister,
    *,
    allowlist: list[str],
    limit: int = 50,
) -> list[ClusterEntry]:
    """Enumerate SP clusters, keep those whose CLD delivers to an allowlisted
    Volume, and return them most-recent-first (capped at ``limit``).

    Pure + testable: iterate the ``ClustersLister`` seam, read each cluster's
    ``cluster_id`` / ``state`` / ``cluster_source`` / ``cluster_name`` /
    timestamps, and its CLD destination via ``resolver._cld_destination``
    (reused — no duplicated CLD-extraction logic). A cluster is KEPT only if its
    CLD dest resolves (equal-or-nested-under) an allowlisted root via
    ``resolver._match_allowlisted_root``. Others are dropped (no logs this app
    can open).
    """
    entries: list[ClusterEntry] = []
    for cluster in sp_clusters_client.list():
        cluster_id = getattr(cluster, "cluster_id", None)
        if not cluster_id:
            continue

        cld_dest = resolver._cld_destination(cluster)
        if not cld_dest:
            continue  # no CLD->Volume => no executor logs this app can open

        root = resolver._match_allowlisted_root(cld_dest, allowlist)
        if root is None:
            continue  # CLD dest is not an allowlisted team root => dropped

        entries.append(
            ClusterEntry(
                cluster_id=cluster_id,
                cluster_name=getattr(cluster, "cluster_name", None) or None,
                state=_stringify_enum(getattr(cluster, "state", None)),
                cluster_source=_stringify_enum(
                    getattr(cluster, "cluster_source", None)
                ),
                cld_dest=cld_dest,
                started_at=getattr(cluster, "start_time", None),
                terminated_at=getattr(cluster, "terminated_time", None),
            )
        )

    entries.sort(key=_sort_key)
    return entries[:limit]


# Fields kept internal and NEVER serialized to the browser (MEDIUM #8):
# ``cld_dest`` is a team Volume PATH used only for internal resolution. The
# browser never needs it (clicking resolves on ``cluster_id``), and shipping
# raw Volume paths for allowlisted roots the user may not be able to read is a
# metadata over-disclosure. The click path stays per-user UC-gated on content.
_PUBLIC_OMIT = {"cld_dest"}


def entries_as_dicts(entries: list[ClusterEntry]) -> list[dict]:
    """Serialize entries for the JSON endpoint, WITHOUT internal-only fields.

    MEDIUM #8: ``cld_dest`` (a raw Volume path) is dropped from the response.
    It remains on the ``ClusterEntry`` dataclass for internal use, but the
    browser only ever sees cluster metadata (id / name / state / source /
    timestamps) — never Volume paths and never log content.
    """
    return [
        {k: v for k, v in asdict(e).items() if k not in _PUBLIC_OMIT}
        for e in entries
    ]
