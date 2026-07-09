"""runs.py tests — [SP] pagination shaping, parent->task, task->cluster.

All calls go through the SP fake (``FakeRunsClient``). There is deliberately no
user client here: runs.py must NEVER touch the user OBO identity (spec §2).
"""

import runs as runs_mod
from conftest import (
    FakeRunsClient,
    make_cluster_instance,
    make_run,
    make_task,
    Obj,
)


# --------------------------------------------------------------------------- #
# list_recent_runs — pagination shaping                                        #
# --------------------------------------------------------------------------- #
def test_list_recent_runs_shapes_rows_and_passes_filters():
    base_runs = [
        make_run(101, run_name="nightly-etl", life_cycle="TERMINATED", result="SUCCESS", cluster_id="0710-a"),
        make_run(102, run_name="hourly-agg", life_cycle="RUNNING", result=None, cluster_id="0710-b"),
    ]
    client = FakeRunsClient(list_result=base_runs)

    page = runs_mod.list_recent_runs(
        client, limit=25, page_token="tok", terminated_only=True, job_id=7
    )

    assert [r.run_id for r in page.runs] == [101, 102]
    assert page.runs[0].job_name == "nightly-etl"
    assert page.runs[0].state == "TERMINATED"
    assert page.runs[0].result_state == "SUCCESS"
    assert page.runs[1].state == "RUNNING"

    # Filters forwarded to the SP Jobs API. terminated_only -> completed_only.
    # The SDK iterator auto-paginates, so we request a page-sized limit (capped
    # at the API max of 25) and keep consuming until we've collected `limit`
    # cluster-bearing rows.
    call = client.list_runs_calls[0]
    assert call["limit"] == 25
    assert call["page_token"] == "tok"
    assert call["completed_only"] is True
    assert call["job_id"] == 7


def test_list_recent_runs_terminated_only_false_sends_none():
    client = FakeRunsClient(list_result=[make_run(1)])
    runs_mod.list_recent_runs(client, terminated_only=False)
    assert client.list_runs_calls[0]["completed_only"] is None


def test_list_recent_runs_respects_limit():
    many = [make_run(i, cluster_id=f"0710-{i}") for i in range(50)]
    client = FakeRunsClient(list_result=many)
    page = runs_mod.list_recent_runs(client, limit=10)
    assert len(page.runs) == 10


def test_list_recent_runs_captures_next_page_token():
    r = make_run(1, cluster_id="0710-a")
    r.next_page_token = "next-123"
    client = FakeRunsClient(list_result=[r])
    page = runs_mod.list_recent_runs(client, limit=5)
    assert page.next_page_token == "next-123"


# --------------------------------------------------------------------------- #
# list_recent_runs — clusterless filter (serverless/pipeline runs)             #
# --------------------------------------------------------------------------- #
def test_clusterless_runs_hidden_by_default():
    runs = [
        make_run(1, cluster_id="0710-a"),      # has cluster -> shown
        make_run(2, cluster_id=None),           # serverless   -> hidden
        make_run(3, cluster_id="0710-c"),      # has cluster -> shown
    ]
    client = FakeRunsClient(list_result=runs)
    page = runs_mod.list_recent_runs(client, limit=25)
    assert [r.run_id for r in page.runs] == [1, 3]
    assert all(r.has_cluster and r.cluster_id for r in page.runs)


def test_clusterless_runs_shown_when_requested():
    runs = [
        make_run(1, cluster_id="0710-a"),
        make_run(2, cluster_id=None),
    ]
    client = FakeRunsClient(list_result=runs)
    page = runs_mod.list_recent_runs(client, limit=25, include_clusterless=True)
    ids = [r.run_id for r in page.runs]
    assert ids == [1, 2]
    by_id = {r.run_id: r for r in page.runs}
    assert by_id[1].has_cluster is True and by_id[1].cluster_id == "0710-a"
    assert by_id[2].has_cluster is False and by_id[2].cluster_id is None


def test_clusterless_cluster_id_read_from_single_task():
    # A run with no run-level cluster_instance but a task that carries one
    # counts as having a cluster (this is the real job-cluster shape).
    task = make_task(999, "g", cluster_id="0710-task")
    r = make_run(5, cluster_id=None, tasks=[task])
    client = FakeRunsClient(list_result=[r])
    page = runs_mod.list_recent_runs(client, limit=25)
    assert [r.run_id for r in page.runs] == [5]
    assert page.runs[0].cluster_id == "0710-task"
    assert page.runs[0].has_cluster is True


# --------------------------------------------------------------------------- #
# list_recent_runs — CLD-based filter (has_logs, per-cluster cached)           #
# --------------------------------------------------------------------------- #
class _FakeClusterMeta:
    """Returns a details obj whose truthiness of `cld` we control per cluster.
    Records get_cluster calls so we can assert per-cluster caching."""

    def __init__(self, cld_by_cluster):
        self._cld = cld_by_cluster  # cluster_id -> destination str or None
        self.calls = []

    def get_cluster(self, cluster_id):
        self.calls.append(cluster_id)
        return type("D", (), {"cld": self._cld.get(cluster_id)})()


def _has_logs(details):
    return getattr(details, "cld", None) is not None


def test_default_shows_all_cluster_runs_cld_is_badge_only():
    """Default view hides ONLY clusterless (serverless/pipeline) runs. Every
    cluster-bearing run stays — CLD status is a best-effort badge (has_logs),
    never a filter (so a cluster-without-CLD or an unreadable cluster can't
    make the list go empty; the click resolves the real answer)."""
    runs = [
        make_run(1, cluster_id="c-logs"),    # cluster + CLD  -> shown, has_logs=True
        make_run(2, cluster_id="c-nolog"),   # cluster, no CLD -> STILL shown, has_logs=False
        make_run(3, cluster_id=None),         # serverless      -> hidden
        make_run(4, cluster_id="c-logs2"),   # cluster + CLD  -> shown, has_logs=True
    ]
    client = FakeRunsClient(list_result=runs)
    meta = _FakeClusterMeta({"c-logs": "/Volumes/a", "c-logs2": "/Volumes/b", "c-nolog": None})
    page = runs_mod.list_recent_runs(
        client, limit=25, cluster_meta=meta, has_logs_fn=_has_logs
    )
    assert [r.run_id for r in page.runs] == [1, 2, 4]  # 3 (serverless) hidden; 2 kept
    by_id = {r.run_id: r for r in page.runs}
    assert by_id[1].has_logs is True
    assert by_id[2].has_logs is False   # cluster w/o CLD: visible, badge False
    assert by_id[4].has_logs is True


def test_cld_lookup_cached_per_cluster():
    # Two runs on the SAME cluster => get_cluster called once, not twice.
    runs = [make_run(1, cluster_id="c1"), make_run(2, cluster_id="c1")]
    client = FakeRunsClient(list_result=runs)
    meta = _FakeClusterMeta({"c1": "/Volumes/x"})
    page = runs_mod.list_recent_runs(
        client, limit=25, cluster_meta=meta, has_logs_fn=_has_logs
    )
    assert [r.run_id for r in page.runs] == [1, 2]
    assert meta.calls == ["c1"]  # cached: only one lookup


def test_unknown_cld_run_stays_visible():
    """If the cluster can't be read (SP lacks cluster access, or it aged out),
    CLD is UNKNOWN — the run must stay visible (has_logs=False but not hidden),
    so a missing SP cluster grant never nukes the whole list."""

    class _RaisingMeta:
        def get_cluster(self, cluster_id):
            from databricks.sdk.errors import PermissionDenied  # type: ignore
            raise PermissionDenied("SP has no access to this cluster")

    runs = [make_run(1, cluster_id="c-unknown")]
    client = FakeRunsClient(list_result=runs)
    page = runs_mod.list_recent_runs(
        client, limit=25, cluster_meta=_RaisingMeta(), has_logs_fn=_has_logs
    )
    assert [r.run_id for r in page.runs] == [1]  # NOT hidden
    assert page.runs[0].has_cluster is True
    assert page.runs[0].has_logs is False  # can't promise logs, but visible


def test_include_clusterless_shows_nolog_and_serverless_with_flags():
    runs = [
        make_run(1, cluster_id="c-logs"),
        make_run(2, cluster_id="c-nolog"),
        make_run(3, cluster_id=None),
    ]
    client = FakeRunsClient(list_result=runs)
    meta = _FakeClusterMeta({"c-logs": "/Volumes/a", "c-nolog": None})
    page = runs_mod.list_recent_runs(
        client, limit=25, include_clusterless=True, cluster_meta=meta, has_logs_fn=_has_logs
    )
    by_id = {r.run_id: r for r in page.runs}
    assert set(by_id) == {1, 2, 3}
    assert by_id[1].has_cluster and by_id[1].has_logs
    assert by_id[2].has_cluster and not by_id[2].has_logs   # cluster, no CLD
    assert not by_id[3].has_cluster and not by_id[3].has_logs  # serverless


# --------------------------------------------------------------------------- #
# get_run_tasks — parent -> tasks                                              #
# --------------------------------------------------------------------------- #
def test_get_run_tasks_multi_task():
    parent = make_run(
        200,
        tasks=[
            make_task(2001, "extract", cluster_id="0710-aaa"),
            make_task(2002, "load", cluster_id="0710-bbb"),
        ],
    )
    client = FakeRunsClient(runs_by_id={200: parent})

    run_obj, tasks = runs_mod.get_run_tasks(client, 200)
    assert run_obj is parent
    assert [t.task_key for t in tasks] == ["extract", "load"]
    assert [t.run_id for t in tasks] == [2001, 2002]
    assert tasks[0].cluster_id == "0710-aaa"
    assert client.get_run_calls == [200]


def test_get_run_tasks_missing_run():
    client = FakeRunsClient(runs_by_id={})
    run_obj, tasks = runs_mod.get_run_tasks(client, 999)
    assert run_obj is None
    assert tasks == []


# --------------------------------------------------------------------------- #
# get_task_cluster_instance — task -> cluster                                  #
# --------------------------------------------------------------------------- #
def test_task_cluster_instance_from_run_level():
    run = make_run(300, cluster_id="0710-run-level")
    client = FakeRunsClient(runs_by_id={300: run})
    assert runs_mod.get_task_cluster_instance(client, 300) == "0710-run-level"


def test_task_cluster_instance_from_single_task():
    run = make_run(301, tasks=[make_task(3011, "only", cluster_id="0710-task")])
    # No run-level cluster_instance; falls back to the single task's.
    run.cluster_instance = None
    client = FakeRunsClient(runs_by_id={301: run})
    assert runs_mod.get_task_cluster_instance(client, 301) == "0710-task"


def test_task_cluster_instance_none_when_absent():
    run = make_run(302)
    run.cluster_instance = None
    client = FakeRunsClient(runs_by_id={302: run})
    assert runs_mod.get_task_cluster_instance(client, 302) is None
