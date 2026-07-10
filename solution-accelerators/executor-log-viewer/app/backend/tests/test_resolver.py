"""resolver.py tests — every reason_code + plan 2.7 edge cases + identity split.

Identity discipline (spec §2, §3.3), asserted throughout:
  - Jobs/Clusters metadata -> the SP fakes (FakeRunsClient / FakeClusterMeta).
  - File listing -> the USER fake (FakeUserLister).
The fakes are separate objects; ``SpForbiddenLister`` proves listing never
runs on the SP, and ``FakeClusterMeta.requested`` / ``FakeRunsClient.*_calls``
prove Jobs/Clusters never run on the user identity.
"""

import pytest

import filerefs
import resolver
from conftest import (
    FakeClusterMeta,
    FakeDirEntry,
    FakeRunsClient,
    FakeUserLister,
    make_cluster_details,
    make_run,
    make_task,
)

USER = "dev@example.com"
ROOT = "/Volumes/cat/logs/team_a"
ALLOW = [ROOT, "/Volumes/cat/logs/team_b"]


# --------------------------------------------------------------------------- #
# Directory-tree builders                                                      #
# --------------------------------------------------------------------------- #
def _entry(name, is_dir=False, size=None):
    return FakeDirEntry(name, is_directory=is_dir, size=size, path=None)


def dirs_full_executor_tree(cluster_id, *, app_ids=("app-1",), execs=("0", "1"),
                            files=("stderr", "stdout"), with_driver=True):
    """Build a dir map for <root>/<cluster>/executor/<app>/<exec>/<files>."""
    base = f"{ROOT}/{cluster_id}"
    d = {}
    top = [FakeDirEntry("executor", is_directory=True)]
    if with_driver:
        top.append(FakeDirEntry("driver", is_directory=True))
    d[base] = top
    d[f"{base}/executor"] = [FakeDirEntry(a, is_directory=True) for a in app_ids]
    for a in app_ids:
        d[f"{base}/executor/{a}"] = [FakeDirEntry(e, is_directory=True) for e in execs]
        for e in execs:
            leaf = f"{base}/executor/{a}/{e}"
            d[leaf] = [
                FakeDirEntry(f, is_directory=False, file_size=100,
                             path=f"{leaf}/{f}")
                for f in files
            ]
    if with_driver:
        d[f"{base}/driver"] = [
            FakeDirEntry("stderr", is_directory=False, file_size=50,
                         path=f"{base}/driver/stderr"),
            FakeDirEntry("stdout", is_directory=False, file_size=50,
                         path=f"{base}/driver/stdout"),
        ]
    return d


# --------------------------------------------------------------------------- #
# classify_input                                                               #
# --------------------------------------------------------------------------- #
def test_classify_input():
    assert resolver.classify_input("0710-123456-abcd1234") == "cluster"
    assert resolver.classify_input("0710-abc") == "cluster"
    assert resolver.classify_input("123456789") == "run"
    assert resolver.classify_input("") == "unknown"
    assert resolver.classify_input("not-an-id!!") == "unknown"


def test_load_allowlist():
    got = resolver.load_allowlist(" /Volumes/a/x , /Volumes/a/y/ ")
    assert got == ["/Volumes/a/x", "/Volumes/a/y"]
    assert resolver.load_allowlist("") == []


# --------------------------------------------------------------------------- #
# OK — happy path (manual cluster mode)                                        #
# --------------------------------------------------------------------------- #
def test_ok_full_tree_manual_cluster():
    cid = "0710-abc"
    lister = FakeUserLister(dirs_full_executor_tree(cid))
    meta = FakeClusterMeta(
        {cid: make_cluster_details(cluster_id=cid, source="JOB", volume_dest=ROOT)}
    )

    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )

    assert res.outcome == "OK"
    assert res.reason_code == "OK"
    tree = res.tree
    assert tree["cluster_id"] == cid
    assert tree["cld_root"] == ROOT
    assert len(tree["app_groups"]) == 1
    grp = tree["app_groups"][0]
    assert grp["app_id"] == "app-1"
    assert len(grp["executors"]) == 2
    # Files carry opaque, verifiable refs (not raw paths).
    ref = grp["executors"][0]["files"][0]["file_ref"]
    payload = filerefs.verify_file_ref(ref)
    assert payload.root == ROOT
    assert payload.path.startswith(ROOT)
    # Driver siblings surfaced.
    assert len(tree["driver_files"]) == 2

    # IDENTITY: cluster metadata went to the SP; listing went to the user.
    assert meta.requested == [cid]
    assert all(p.startswith(ROOT) for p in lister.listed_paths)


def test_resolver_binds_refs_to_cluster_base_dir():
    """MEDIUM #6: every ref the resolver mints is bound to the resolved
    <cld_dest>/<cluster-id> directory, so it can't read a sibling cluster's log
    even under the same team root."""
    cid = "0710-abc"
    lister = FakeUserLister(dirs_full_executor_tree(cid))
    meta = FakeClusterMeta(
        {cid: make_cluster_details(cluster_id=cid, source="JOB", volume_dest=ROOT)}
    )
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    ref = res.tree["app_groups"][0]["executors"][0]["files"][0]["file_ref"]
    payload = filerefs.verify_file_ref(ref)
    expected_base = f"{ROOT}/{cid}"
    assert payload.base_dir == expected_base
    assert payload.path.startswith(expected_base + "/")
    # A validly-signed ref pointing at a SIBLING cluster (same root, different
    # base_dir) is rejected at verify.
    import base64, hashlib, hmac, json, os, time
    sibling = {
        "path": f"{ROOT}/0710-OTHER/executor/app-1/0/stderr",
        "run_id": cid, "cluster_id": cid, "file_kind": "stderr",
        "user": USER, "root": ROOT, "base_dir": expected_base,
        "expiry": int(time.time()) + 600,
    }
    pj = json.dumps(sibling, sort_keys=True, separators=(",", ":"))
    b64 = base64.urlsafe_b64encode(pj.encode()).decode().rstrip("=")
    secret = os.environ[filerefs.SIGNING_SECRET_ENV].encode()
    sig = base64.urlsafe_b64encode(
        hmac.new(secret, b64.encode(), hashlib.sha256).digest()
    ).decode().rstrip("=")
    with pytest.raises(filerefs.FileRefPathEscape):
        filerefs.verify_file_ref(f"{b64}.{sig}")


def test_ok_rotated_files_recognized():
    cid = "0710-rot"
    files = ("stderr", "stdout", "stderr--2026-07-08", "stdout--2026-07-08", "gc.log")
    lister = FakeUserLister(
        dirs_full_executor_tree(cid, files=files, with_driver=False)
    )
    meta = FakeClusterMeta(
        {cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)}
    )
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "OK"
    names = {f["name"] for f in res.tree["app_groups"][0]["executors"][0]["files"]}
    assert names == set(files)  # all 5 recognized (incl. rotated + .log)
    kinds = {f["name"]: f["file_kind"] for f in res.tree["app_groups"][0]["executors"][0]["files"]}
    assert kinds["stderr--2026-07-08"] == "stderr"
    assert kinds["gc.log"] == "log"


def test_non_log_files_never_minted():
    cid = "0710-junk"
    d = dirs_full_executor_tree(cid, files=("stderr", "secrets.txt", "data.parquet"),
                                with_driver=False)
    lister = FakeUserLister(d)
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    names = {f["name"] for f in res.tree["app_groups"][0]["executors"][0]["files"]}
    assert names == {"stderr"}  # non-log files excluded, never given a ref


def test_cld_dest_nested_under_allowlist_root():
    """CLD destination may sit UNDER an allowlisted root (e.g. a per-catalog
    subdir). We list under the real dest but bind refs to the allowlist root."""
    cid = "0710-nested"
    dest = f"{ROOT}/sub"  # nested under the allowlisted ROOT
    base = f"{dest}/{cid}"
    dirs = {
        base: [FakeDirEntry("executor", is_directory=True)],
        f"{base}/executor": [FakeDirEntry("app-1", is_directory=True)],
        f"{base}/executor/app-1": [FakeDirEntry("0", is_directory=True)],
        f"{base}/executor/app-1/0": [
            FakeDirEntry("stderr", is_directory=False, file_size=5,
                         path=f"{base}/executor/app-1/0/stderr"),
        ],
    }
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=dest)})
    lister = FakeUserLister(dirs)
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "OK"
    assert res.tree["cld_root"] == ROOT  # ref containment bound to allowlist root
    ref = res.tree["app_groups"][0]["executors"][0]["files"][0]["file_ref"]
    payload = filerefs.verify_file_ref(ref)
    assert payload.root == ROOT
    assert payload.path.startswith(dest)


def test_multiple_app_ids_grouped():
    cid = "0710-multi"
    lister = FakeUserLister(
        dirs_full_executor_tree(cid, app_ids=("app-1", "app-2"), with_driver=False)
    )
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "OK"
    assert {g["app_id"] for g in res.tree["app_groups"]} == {"app-1", "app-2"}


def test_large_executor_count():
    cid = "0710-big"
    execs = tuple(str(i) for i in range(200))
    lister = FakeUserLister(
        dirs_full_executor_tree(cid, execs=execs, with_driver=False)
    )
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "OK"
    assert len(res.tree["app_groups"][0]["executors"]) == 200


# --------------------------------------------------------------------------- #
# NOT_JOB_CLUSTER (existing all-purpose cluster)                               #
# --------------------------------------------------------------------------- #
def test_not_job_cluster_no_cld():
    cid = "0710-ui"
    meta = FakeClusterMeta(
        {cid: make_cluster_details(cluster_id=cid, source="UI", volume_dest=None)}
    )
    # Listing must NOT be reached; use the forbidden lister to prove it.
    from conftest import SpForbiddenLister
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=SpForbiddenLister(),
        allowlist=ALLOW, user=USER,
    )
    assert res.reason_code == "NOT_JOB_CLUSTER"


# --------------------------------------------------------------------------- #
# NO_CLD                                                                        #
# --------------------------------------------------------------------------- #
def test_no_cld():
    cid = "0710-nocld"
    meta = FakeClusterMeta(
        {cid: make_cluster_details(cluster_id=cid, source="JOB", volume_dest=None)}
    )
    from conftest import SpForbiddenLister
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=SpForbiddenLister(),
        allowlist=ALLOW, user=USER,
    )
    assert res.reason_code == "NO_CLD"


# --------------------------------------------------------------------------- #
# CLD_ROOT_NOT_FOUND (CLD dest not in allowlist)                               #
# --------------------------------------------------------------------------- #
def test_cld_root_not_in_allowlist():
    cid = "0710-rogue"
    meta = FakeClusterMeta(
        {cid: make_cluster_details(cluster_id=cid, source="JOB",
                                   volume_dest="/Volumes/cat/logs/UNLISTED")}
    )
    from conftest import SpForbiddenLister
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=SpForbiddenLister(),
        allowlist=ALLOW, user=USER,
    )
    assert res.reason_code == "CLD_ROOT_NOT_FOUND"


# --------------------------------------------------------------------------- #
# FILES_FORBIDDEN (per-user UC denial on the user's list call)                 #
# --------------------------------------------------------------------------- #
def test_files_forbidden_is_the_per_user_check():
    cid = "0710-secret"
    cluster_dir = f"{ROOT}/{cid}"
    # SP CAN see the metadata (job cluster, allowlisted CLD)...
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    # ...but the USER is denied READ on the Volume path.
    lister = FakeUserLister(dirs={}, denied=[cluster_dir])

    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "FILES_FORBIDDEN"
    # SP still saw metadata (proves the SP is not the access oracle).
    assert meta.requested == [cid]
    # The denial came from the USER identity's list call.
    assert cluster_dir in lister.listed_paths


# --------------------------------------------------------------------------- #
# DELIVERY_PENDING (cluster dir absent — recently terminated)                  #
# --------------------------------------------------------------------------- #
def test_delivery_pending_when_cluster_dir_absent():
    cid = "0710-pending"
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    lister = FakeUserLister(dirs={})  # nothing under the cluster dir yet
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "DELIVERY_PENDING"


# --------------------------------------------------------------------------- #
# NO_EXECUTOR_DIR (driver present, no executor/) — all-purpose delivery shape  #
# --------------------------------------------------------------------------- #
def test_no_executor_dir_but_driver_present():
    cid = "0710-driveronly"
    base = f"{ROOT}/{cid}"
    dirs = {
        base: [FakeDirEntry("driver", is_directory=True),
               FakeDirEntry("eventlog", is_directory=True)],
        f"{base}/driver": [
            FakeDirEntry("stderr", is_directory=False, file_size=10,
                         path=f"{base}/driver/stderr"),
        ],
    }
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    lister = FakeUserLister(dirs)
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "NO_EXECUTOR_DIR"
    # Driver still surfaced automatically (spec §5).
    assert len(res.tree["driver_files"]) == 1


# --------------------------------------------------------------------------- #
# NO_LOG_FILES (dirs exist but no recognized files)                            #
# --------------------------------------------------------------------------- #
def test_no_log_files():
    cid = "0710-emptyexec"
    base = f"{ROOT}/{cid}"
    dirs = {
        base: [FakeDirEntry("executor", is_directory=True)],
        f"{base}/executor": [FakeDirEntry("app-1", is_directory=True)],
        f"{base}/executor/app-1": [FakeDirEntry("0", is_directory=True)],
        f"{base}/executor/app-1/0": [
            FakeDirEntry("notes.txt", is_directory=False, path=f"{base}/executor/app-1/0/notes.txt"),
        ],
    }
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    lister = FakeUserLister(dirs)
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "NO_LOG_FILES"


# --------------------------------------------------------------------------- #
# CLUSTER_METADATA_UNAVAILABLE + probe fallback (retention/aged out)           #
# --------------------------------------------------------------------------- #
def test_cluster_metadata_unavailable_probe_finds_root():
    cid = "0710-aged"
    # clusters.get raises (aged out) -> probe the allowlisted roots.
    meta = FakeClusterMeta(clusters={}, raise_for=[cid])
    lister = FakeUserLister(dirs_full_executor_tree(cid, with_driver=False))
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    # Probe found <root>/<cid>/ and built the tree.
    assert res.reason_code == "OK"
    assert res.tree["cld_root"] == ROOT
    assert cid in meta.requested  # SP was still tried first


def test_cluster_metadata_unavailable_probe_finds_nothing():
    cid = "0710-gone"
    meta = FakeClusterMeta(clusters={}, raise_for=[cid])
    lister = FakeUserLister(dirs={})  # not under any allowlisted root
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "CLUSTER_METADATA_UNAVAILABLE"


# --------------------------------------------------------------------------- #
# Full pipeline: resolve(value) — run classification                          #
# --------------------------------------------------------------------------- #
def _wire(runs_client, meta, lister):
    return dict(runs_client=runs_client, cluster_meta=meta, lister=lister,
                allowlist=ALLOW, user=USER)


def test_resolve_input_ambiguous():
    res = resolver.resolve(
        "!!!", runs_client=FakeRunsClient(), cluster_meta=FakeClusterMeta(),
        lister=FakeUserLister(), allowlist=ALLOW, user=USER,
    )
    assert res.reason_code == "INPUT_AMBIGUOUS"


def test_resolve_run_not_found():
    runs_client = FakeRunsClient(runs_by_id={}, raise_for=[404])
    res = resolver.resolve("404", **_wire(runs_client, FakeClusterMeta(), FakeUserLister()))
    assert res.reason_code == "RUN_NOT_FOUND"


def test_resolve_run_no_access():
    from conftest import PermissionDenied

    class DenyingRuns(FakeRunsClient):
        def get_run(self, run_id):
            self.get_run_calls.append(run_id)
            raise PermissionDenied("no access")

    res = resolver.resolve("500", **_wire(DenyingRuns(), FakeClusterMeta(), FakeUserLister()))
    assert res.reason_code == "RUN_NO_ACCESS"


def test_resolve_running_run_links_out():
    run = make_run(600, life_cycle="RUNNING", result=None,
                   tasks=[make_task(6001, "t", cluster_id="0710-x")])
    runs_client = FakeRunsClient(runs_by_id={600: run})
    res = resolver.resolve("600", **_wire(runs_client, FakeClusterMeta(), FakeUserLister()))
    assert res.reason_code == "RUNNING"
    assert res.run_meta["run_page_url"] == "https://ws/run"


def test_resolve_parent_run_has_multiple_tasks():
    parent = make_run(
        700,
        tasks=[make_task(7001, "extract", cluster_id="0710-a"),
               make_task(7002, "load", cluster_id="0710-b")],
    )
    runs_client = FakeRunsClient(runs_by_id={700: parent})
    res = resolver.resolve("700", **_wire(runs_client, FakeClusterMeta(), FakeUserLister()))
    assert res.reason_code == "PARENT_RUN_HAS_MULTIPLE_TASKS"
    assert [t["task_key"] for t in res.tasks] == ["extract", "load"]


def test_resolve_single_task_run_end_to_end():
    cid = "0710-single"
    task = make_task(8001, "only", cluster_id=cid)
    parent = make_run(800, tasks=[task])
    parent.cluster_instance = None
    # get_run(800) -> parent(with task); get_run(8001) -> task run w/ instance
    task_run = make_run(8001, tasks=[], cluster_id=cid)
    runs_client = FakeRunsClient(runs_by_id={800: parent, 8001: task_run})
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    lister = FakeUserLister(dirs_full_executor_tree(cid, with_driver=False))

    res = resolver.resolve("800", **_wire(runs_client, meta, lister))
    assert res.reason_code == "OK"
    assert res.run_meta["run_id"] == 800
    assert res.tree["cluster_id"] == cid
    # IDENTITY: jobs via SP, cluster meta via SP, listing via user.
    assert 800 in runs_client.get_run_calls
    assert meta.requested == [cid]


def test_resolve_multi_task_with_selection():
    cid = "0710-chosen"
    parent = make_run(
        900,
        tasks=[make_task(9001, "extract", cluster_id="0710-a"),
               make_task(9002, "load", cluster_id=cid)],
    )
    task_run = make_run(9002, cluster_id=cid)
    runs_client = FakeRunsClient(runs_by_id={900: parent, 9002: task_run})
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    lister = FakeUserLister(dirs_full_executor_tree(cid, with_driver=False))

    res = resolver.resolve("900", task_run_id=9002, **_wire(runs_client, meta, lister))
    assert res.reason_code == "OK"
    assert res.tree["cluster_id"] == cid


def test_resolve_retry_repair_run():
    """A repaired run resolves like any terminated run: its (latest) task run
    still points at a cluster instance."""
    cid = "0710-repair"
    task = make_task(9101, "main", cluster_id=cid)
    run = make_run(910, tasks=[task])
    run.cluster_instance = None
    task_run = make_run(9101, cluster_id=cid)
    runs_client = FakeRunsClient(runs_by_id={910: run, 9101: task_run})
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    lister = FakeUserLister(dirs_full_executor_tree(cid, with_driver=False))
    res = resolver.resolve("910", **_wire(runs_client, meta, lister))
    assert res.reason_code == "OK"


def test_resolve_no_cluster_instance():
    run = make_run(920, tasks=[make_task(9201, "t", cluster_id=None)])
    run.cluster_instance = None
    task_run = make_run(9201)
    task_run.cluster_instance = None
    runs_client = FakeRunsClient(runs_by_id={920: run, 9201: task_run})
    res = resolver.resolve("920", **_wire(runs_client, FakeClusterMeta(), FakeUserLister()))
    assert res.reason_code == "NO_CLUSTER_INSTANCE"


# --------------------------------------------------------------------------- #
# IDENTITY SPLIT — negative control                                            #
# --------------------------------------------------------------------------- #
def test_listing_never_routed_through_sp():
    """If resolver code ever lists files through the SP identity, the
    SpForbiddenLister raises AssertionError. Here we drive a full OK resolve
    with the user lister and confirm the SP fakes were used ONLY for metadata
    (they have no list_dir method at all)."""
    cid = "0710-split"
    meta = FakeClusterMeta({cid: make_cluster_details(cluster_id=cid, volume_dest=ROOT)})
    lister = FakeUserLister(dirs_full_executor_tree(cid, with_driver=False))
    res = resolver.resolve_cluster(
        cid, cluster_meta=meta, lister=lister, allowlist=ALLOW, user=USER
    )
    assert res.reason_code == "OK"
    # SP metadata client has no list capability — structural proof of the split.
    assert not hasattr(meta, "list_dir")
    # User lister has no jobs/clusters capability.
    assert not hasattr(lister, "get_cluster")
    assert not hasattr(lister, "get_run")
