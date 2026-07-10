"""Shared test fixtures. No real Databricks workspace is ever contacted."""

from __future__ import annotations

import pytest

import filerefs

TEST_SECRET = "unit-test-signing-secret"


@pytest.fixture(autouse=True)
def _signing_secret(monkeypatch):
    """Every test gets a deterministic signing secret."""
    monkeypatch.setenv(filerefs.SIGNING_SECRET_ENV, TEST_SECRET)


class FakeFileReader:
    """In-memory FileReader for logs tests. Maps path -> bytes."""

    def __init__(self, files: dict[str, bytes]):
        self._files = files

    def stat_size(self, path: str) -> int:
        if path not in self._files:
            raise FileNotFoundError(path)
        return len(self._files[path])

    def read_range(self, path: str, start: int, end: int) -> bytes:
        if path not in self._files:
            raise FileNotFoundError(path)
        return self._files[path][start:end]


@pytest.fixture
def make_reader():
    return lambda files: FakeFileReader(files)


# --------------------------------------------------------------------------- #
# Phase 2 fakes — enforce the two-identity split (spec §2, §3.3).             #
#                                                                             #
# The whole security model is: Jobs/Clusters go to the SP; file listing goes  #
# to the USER OBO client. These fakes are deliberately SEPARATE objects so a  #
# test fails loudly if resolver code ever calls Jobs/Clusters on the user     #
# client or listing on the SP client.                                        #
# --------------------------------------------------------------------------- #
class FakeDirEntry:
    """Mimics an SDK DirectoryEntry."""

    def __init__(self, name, *, is_directory=False, path=None, file_size=None,
                 last_modified=None):
        self.name = name
        self.is_directory = is_directory
        self.path = path
        self.file_size = file_size
        self.last_modified = last_modified


class PermissionDenied(Exception):
    """Same class NAME as the SDK's; resolver matches on name, not import."""


class NotFound(Exception):
    pass


class FakeUserLister:
    """[USER OBO] file lister. Maps dir path -> list[FakeDirEntry].

    Records every path it was asked to list so tests can assert that FILE
    listing went through the USER identity (never the SP). A path in
    ``denied`` raises PermissionDenied (the UC per-user access oracle).
    """

    identity = "user"

    def __init__(self, dirs=None, denied=None):
        self._dirs = dirs or {}
        self._denied = set(denied or [])
        self.listed_paths = []

    def list_dir(self, path):
        self.listed_paths.append(path)
        if path in self._denied:
            raise PermissionDenied(f"UC denied READ on {path}")
        if path not in self._dirs:
            raise NotFound(path)
        return list(self._dirs[path])


class SpForbiddenLister:
    """Stand-in placed where a USER lister belongs, to PROVE listing never runs
    on the SP. If resolver code ever lists via the SP, this raises."""

    identity = "sp-should-not-list"

    def list_dir(self, path):  # pragma: no cover - must never be called
        raise AssertionError(
            "FILE listing was routed through the SP identity — security bug"
        )


class FakeClusterMeta:
    """[SP] cluster metadata. Records cluster_ids requested. Raising factory
    supported to simulate aged-out metadata."""

    identity = "sp"

    def __init__(self, clusters=None, raise_for=None):
        self._clusters = clusters or {}
        self._raise_for = set(raise_for or [])
        self.requested = []

    def get_cluster(self, cluster_id):
        self.requested.append(cluster_id)
        if cluster_id in self._raise_for:
            raise NotFound(f"cluster {cluster_id} metadata unavailable")
        return self._clusters[cluster_id]


class FakeRunsClient:
    """[SP] runs client. Records calls; returns preloaded run objects."""

    identity = "sp"

    def __init__(self, runs_by_id=None, list_result=None, raise_for=None):
        self._runs = runs_by_id or {}
        self._list = list_result or []
        self._raise_for = set(raise_for or [])
        self.get_run_calls = []
        self.list_runs_calls = []

    def list_runs(self, *, limit, page_token, completed_only, job_id):
        self.list_runs_calls.append(
            dict(limit=limit, page_token=page_token,
                 completed_only=completed_only, job_id=job_id)
        )
        return iter(self._list)

    def get_run(self, run_id):
        self.get_run_calls.append(run_id)
        if run_id in self._raise_for:
            raise NotFound(f"run {run_id} not found")
        return self._runs.get(run_id)


# --- SDK-shaped value objects (attribute access, like the real dataclasses) --
class Obj:
    """Simple attribute bag standing in for SDK dataclasses."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


def make_run_state(life_cycle="TERMINATED", result="SUCCESS"):
    return Obj(life_cycle_state=life_cycle, result_state=result)


def make_cluster_instance(cluster_id):
    return Obj(cluster_id=cluster_id, spark_context_id="ctx")


def make_task(run_id, task_key, cluster_id=None, life_cycle="TERMINATED"):
    return Obj(
        run_id=run_id,
        task_key=task_key,
        state=make_run_state(life_cycle),
        cluster_instance=make_cluster_instance(cluster_id) if cluster_id else None,
    )


def make_run(run_id, *, job_id=1, run_name="job-A", tasks=None,
             cluster_id=None, life_cycle="TERMINATED", result="SUCCESS",
             run_page_url="https://ws/run"):
    return Obj(
        run_id=run_id,
        job_id=job_id,
        run_name=run_name,
        state=make_run_state(life_cycle, result),
        start_time=1_700_000_000_000,
        run_page_url=run_page_url,
        next_page_token=None,
        tasks=tasks or [],
        cluster_instance=make_cluster_instance(cluster_id) if cluster_id else None,
    )


def make_cluster_details(*, cluster_id="0710-abc", source="JOB", volume_dest=None):
    log_conf = None
    if volume_dest is not None:
        log_conf = Obj(volumes=Obj(destination=volume_dest), dbfs=None, s3=None)
    return Obj(
        cluster_id=cluster_id,
        cluster_source=source,
        cluster_log_conf=log_conf,
    )
