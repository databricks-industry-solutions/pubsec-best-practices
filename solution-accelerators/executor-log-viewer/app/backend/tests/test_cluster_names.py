"""Tests for Volume-cluster -> job-name enrichment (cluster_names.py)."""
import cluster_names as cn


class FakeMeta:
    """Fake [SP] metadata client. cluster_names maps cid->cluster_name;
    job_names maps job_id(str)->friendly name. Missing -> None (deleted/aged)."""
    def __init__(self, cluster_names_map, job_names, *, fail_cluster=(), fail_job=()):
        self._c = cluster_names_map
        self._j = job_names
        self._fail_cluster = set(fail_cluster)
        self._fail_job = set(fail_job)
        self.cluster_calls = []
        self.job_calls = []

    def cluster_name(self, cluster_id):
        self.cluster_calls.append(cluster_id)
        if cluster_id in self._fail_cluster:
            return None
        return self._c.get(cluster_id)

    def job_name(self, job_id):
        self.job_calls.append(job_id)
        if str(job_id) in self._fail_job:
            return None
        return self._j.get(str(job_id))


def test_parse_job_run():
    assert cn.parse_job_run("job-123-run-456") == ("123", "456")
    assert cn.parse_job_run("shs-exec-log-test") == (None, None)
    assert cn.parse_job_run(None) == (None, None)
    assert cn.parse_job_run("") == (None, None)


def test_enrich_happy_path_friendly_name():
    clusters = [{"cluster_id": "0714-abc"}]
    meta = FakeMeta({"0714-abc": "job-265-run-730"}, {"265": "executor-log-test-SHARED"})
    out = cn.enrich_clusters(clusters, meta)
    assert out[0]["job_id"] == "265"
    assert out[0]["run_id"] == "730"
    assert out[0]["job_name"] == "executor-log-test-SHARED"


def test_enrich_deleted_job_keeps_ids_null_name():
    clusters = [{"cluster_id": "0709-xyz"}]
    # cluster_name resolves (so we get ids) but jobs.get fails (deleted job).
    meta = FakeMeta({"0709-xyz": "job-514-run-849"}, {}, fail_job=["514"])
    out = cn.enrich_clusters(clusters, meta)
    assert out[0]["job_id"] == "514"
    assert out[0]["run_id"] == "849"
    assert out[0]["job_name"] is None  # graceful: ids kept, name unknown


def test_enrich_cluster_get_fails_leaves_fields_null():
    clusters = [{"cluster_id": "0700-gone"}]
    meta = FakeMeta({}, {}, fail_cluster=["0700-gone"])
    out = cn.enrich_clusters(clusters, meta)
    assert out[0]["job_id"] is None
    assert out[0]["run_id"] is None
    assert out[0]["job_name"] is None


def test_enrich_caches_jobs_get_per_job():
    # Two clusters from the SAME job -> jobs.get called ONCE.
    clusters = [{"cluster_id": "c1"}, {"cluster_id": "c2"}]
    meta = FakeMeta(
        {"c1": "job-9-run-1", "c2": "job-9-run-2"}, {"9": "shared-job"}
    )
    cn.enrich_clusters(clusters, meta)
    assert meta.job_calls == [9]  # one call, cached for the second cluster


def test_enrich_non_job_cluster_uses_raw_name():
    clusters = [{"cluster_id": "0708-ui"}]
    meta = FakeMeta({"0708-ui": "my-interactive-cluster"}, {})
    out = cn.enrich_clusters(clusters, meta)
    assert out[0]["job_id"] is None
    assert out[0]["job_name"] == "my-interactive-cluster"  # fall back to any name
