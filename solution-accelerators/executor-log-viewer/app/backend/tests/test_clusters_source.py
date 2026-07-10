"""clusters_source.py tests — the PRIMARY "recent clusters with logs" source.

Discipline (spec §2, §3.3): enumeration goes through the [SP] ``ClustersLister``
seam (a fake here). This module returns cluster METADATA + CLD *paths* only —
never a file_ref, never file content. Tests assert:
  - clusters whose CLD resolves to an allowlisted root are KEPT
  - non-allowlisted / None CLD clusters are DROPPED
  - sort is most-recent-first (terminated_at preferred, else start_time; None last)
  - state / cluster_source are stringified to bare names
  - empty list, and limit respected
"""

from dataclasses import asdict

import clusters_source
from conftest import Obj

ALLOW = ["/Volumes/cat/logs/team_a"]


class FakeClustersLister:
    """[SP] cluster lister. Records that ``list()`` was called; returns a
    preloaded list of SDK-shaped cluster objects."""

    identity = "sp"

    def __init__(self, clusters):
        self._clusters = clusters
        self.list_calls = 0

    def list(self):
        self.list_calls += 1
        return iter(self._clusters)


def _cluster(
    cluster_id,
    *,
    volume_dest=None,
    state="TERMINATED",
    source="JOB",
    name=None,
    start_time=None,
    terminated_time=None,
):
    """SDK-shaped ClusterDetails. ``volume_dest`` None => no cluster_log_conf."""
    log_conf = None
    if volume_dest is not None:
        log_conf = Obj(volumes=Obj(destination=volume_dest), dbfs=None, s3=None)
    return Obj(
        cluster_id=cluster_id,
        cluster_name=name,
        state=state,
        cluster_source=source,
        cluster_log_conf=log_conf,
        start_time=start_time,
        terminated_time=terminated_time,
    )


# --------------------------------------------------------------------------- #
# Keep / drop by allowlisted CLD                                               #
# --------------------------------------------------------------------------- #
def test_keeps_only_allowlisted_cld_clusters():
    lister = FakeClustersLister([
        # KEPT: CLD nested under the allowlisted root.
        _cluster("0710-aaa", volume_dest="/Volumes/cat/logs/team_a/0710-aaa"),
        # KEPT: CLD equal to the allowlisted root.
        _cluster("0710-bbb", volume_dest="/Volumes/cat/logs/team_a"),
        # DROPPED: CLD points at a different (non-allowlisted) Volume.
        _cluster("0710-ccc", volume_dest="/Volumes/cat/logs/team_b"),
        # DROPPED: no CLD at all.
        _cluster("0710-ddd", volume_dest=None),
    ])

    out = clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW)

    assert lister.list_calls == 1
    kept = {e.cluster_id for e in out}
    assert kept == {"0710-aaa", "0710-bbb"}
    # cld_dest is the raw destination path (never content, never a ref).
    by_id = {e.cluster_id: e for e in out}
    assert by_id["0710-aaa"].cld_dest == "/Volumes/cat/logs/team_a/0710-aaa"


def test_drops_broader_than_allowlist():
    # A dest that CONTAINS the allowlisted root (broader) is rejected — we never
    # widen access beyond the configured team roots (matches resolver semantics).
    lister = FakeClustersLister([
        _cluster("0710-wide", volume_dest="/Volumes/cat/logs"),
    ])
    out = clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW)
    assert out == []


def test_skips_clusters_without_id():
    lister = FakeClustersLister([
        _cluster(None, volume_dest="/Volumes/cat/logs/team_a"),
        _cluster("0710-ok", volume_dest="/Volumes/cat/logs/team_a"),
    ])
    out = clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW)
    assert [e.cluster_id for e in out] == ["0710-ok"]


# --------------------------------------------------------------------------- #
# Sorting — most-recent-first                                                  #
# --------------------------------------------------------------------------- #
def test_sorts_most_recent_first_terminated_then_started_then_unknown():
    lister = FakeClustersLister([
        _cluster("old-term", volume_dest="/Volumes/cat/logs/team_a", terminated_time=100),
        _cluster("new-term", volume_dest="/Volumes/cat/logs/team_a", terminated_time=500),
        # No terminated_time -> falls back to start_time for the sort key.
        _cluster("running", volume_dest="/Volumes/cat/logs/team_a", start_time=300, terminated_time=None),
        # No timestamps at all -> sorts last.
        _cluster("unknown", volume_dest="/Volumes/cat/logs/team_a"),
    ])
    out = clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW)
    assert [e.cluster_id for e in out] == ["new-term", "running", "old-term", "unknown"]


# --------------------------------------------------------------------------- #
# State / source stringification                                              #
# --------------------------------------------------------------------------- #
def test_stringifies_enum_state_and_source():
    # SDK enums expose .value; a bare enum repr like "State.TERMINATED" is
    # stripped to "TERMINATED".
    lister = FakeClustersLister([
        _cluster(
            "0710-enumval",
            volume_dest="/Volumes/cat/logs/team_a",
            state=Obj(value="TERMINATED"),
            source=Obj(value="JOB"),
        ),
        _cluster(
            "0710-enumrepr",
            volume_dest="/Volumes/cat/logs/team_a",
            state=_FakeEnum("State", "RUNNING"),
            source=_FakeEnum("ClusterSource", "UI"),
        ),
    ])
    out = clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW)
    by_id = {e.cluster_id: e for e in out}
    assert by_id["0710-enumval"].state == "TERMINATED"
    assert by_id["0710-enumval"].cluster_source == "JOB"
    assert by_id["0710-enumrepr"].state == "RUNNING"
    assert by_id["0710-enumrepr"].cluster_source == "UI"


class _FakeEnum:
    """Mimics an SDK enum whose str() is 'ClassName.MEMBER' and has no .value."""

    def __init__(self, cls, member):
        self._repr = f"{cls}.{member}"

    def __str__(self):
        return self._repr


def test_plain_string_state_passes_through():
    lister = FakeClustersLister([
        _cluster("0710-str", volume_dest="/Volumes/cat/logs/team_a", state="TERMINATED", source="JOB"),
    ])
    out = clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW)
    assert out[0].state == "TERMINATED"
    assert out[0].cluster_source == "JOB"


# --------------------------------------------------------------------------- #
# Empty + limit                                                               #
# --------------------------------------------------------------------------- #
def test_empty_cluster_list():
    lister = FakeClustersLister([])
    assert clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW) == []


def test_all_dropped_yields_empty():
    lister = FakeClustersLister([
        _cluster("0710-x", volume_dest=None),
        _cluster("0710-y", volume_dest="/Volumes/other"),
    ])
    assert clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW) == []


def test_limit_respected():
    clusters = [
        _cluster(f"0710-{i:03d}", volume_dest="/Volumes/cat/logs/team_a", terminated_time=i)
        for i in range(20)
    ]
    lister = FakeClustersLister(clusters)
    out = clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW, limit=5)
    assert len(out) == 5
    # Cap AFTER sort => the 5 most recent (highest terminated_time).
    assert [e.cluster_id for e in out] == [
        "0710-019", "0710-018", "0710-017", "0710-016", "0710-015",
    ]


# --------------------------------------------------------------------------- #
# Serialization shape (endpoint contract)                                     #
# --------------------------------------------------------------------------- #
def test_entries_as_dicts_shape():
    lister = FakeClustersLister([
        _cluster("0710-aaa", volume_dest="/Volumes/cat/logs/team_a", name="my-job-cluster",
                 state="TERMINATED", source="JOB", start_time=1, terminated_time=2),
    ])
    out = clusters_source.list_clusters_with_logs(lister, allowlist=ALLOW)
    dicts = clusters_source.entries_as_dicts(out)
    d = dicts[0]
    # MEDIUM #8: cld_dest (a raw Volume path) is DROPPED from the serialized
    # response; it stays internal on the dataclass but is never sent to the UI.
    assert set(d.keys()) == {
        "cluster_id", "cluster_name", "state", "cluster_source",
        "started_at", "terminated_at",
    }
    assert "cld_dest" not in d
    # cld_dest is still present internally on the entry for resolution.
    assert out[0].cld_dest == "/Volumes/cat/logs/team_a"
    # Never leaks a file_ref or content.
    assert "file_ref" not in d
    assert d["cluster_name"] == "my-job-cluster"
