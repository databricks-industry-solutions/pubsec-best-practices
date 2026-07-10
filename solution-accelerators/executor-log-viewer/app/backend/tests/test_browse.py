"""browse.py tests — user-scoped Volume browsing (discovery path).

Discipline (spec §2, §3.3): ALL listing goes through the USER OBO lister
(``FakeUserLister``). These tests assert the returned candidate shape, the
modified-desc sort, the cheap executor/driver peek, and the error mapping
(NOT_VOLUME / FILES_FORBIDDEN / CLD_ROOT_NOT_FOUND). Browsing lists directory
NAMES only — no file_ref is ever minted here.
"""

import pytest

import browse as browse_mod
from conftest import FakeDirEntry, FakeUserLister

ROOT = "/Volumes/cat/logs/team_a"


def _cluster_dirs(*names, modified=None):
    """Top-level listing of a root: each name is a cluster subdirectory."""
    entries = []
    for i, n in enumerate(names):
        mod = None
        if modified is not None:
            mod = modified[i] if isinstance(modified, (list, tuple)) else modified
        entries.append(
            FakeDirEntry(n, is_directory=True, path=f"{ROOT}/{n}", last_modified=mod)
        )
    return entries


# --------------------------------------------------------------------------- #
# Happy path — clusters returned, sorted by modified desc, peeked for flags    #
# --------------------------------------------------------------------------- #
def test_browse_returns_clusters_sorted_desc_with_flags():
    lister = FakeUserLister(
        dirs={
            ROOT: _cluster_dirs("0710-aaa", "0710-bbb", modified=[100, 300]),
            # 0710-aaa has both executor/ and driver/; 0710-bbb has only driver/.
            f"{ROOT}/0710-aaa": [
                FakeDirEntry("executor", is_directory=True),
                FakeDirEntry("driver", is_directory=True),
            ],
            f"{ROOT}/0710-bbb": [
                FakeDirEntry("driver", is_directory=True),
            ],
        }
    )

    out = browse_mod.browse_root(lister, ROOT)

    assert out["path"] == ROOT
    clusters = out["clusters"]
    assert [c["cluster_id"] for c in clusters] == ["0710-bbb", "0710-aaa"]  # 300 > 100

    bbb, aaa = clusters
    assert bbb["modified"] == 300
    assert bbb["has_executor"] is False
    assert bbb["has_driver"] is True
    assert bbb["path"] == f"{ROOT}/0710-bbb"

    assert aaa["has_executor"] is True
    assert aaa["has_driver"] is True

    # NAMES only — never a file_ref, never file content.
    for c in clusters:
        assert "file_ref" not in c
        assert set(c.keys()) == {
            "cluster_id", "path", "modified", "has_executor", "has_driver",
        }


def test_browse_ignores_non_directories():
    lister = FakeUserLister(
        dirs={
            ROOT: [
                FakeDirEntry("0710-aaa", is_directory=True, path=f"{ROOT}/0710-aaa"),
                FakeDirEntry("README.txt", is_directory=False, path=f"{ROOT}/README.txt"),
            ],
            f"{ROOT}/0710-aaa": [FakeDirEntry("executor", is_directory=True)],
        }
    )
    out = browse_mod.browse_root(lister, ROOT)
    assert [c["cluster_id"] for c in out["clusters"]] == ["0710-aaa"]


def test_browse_empty_root_returns_no_clusters():
    lister = FakeUserLister(dirs={ROOT: []})
    out = browse_mod.browse_root(lister, ROOT)
    assert out == {"path": ROOT, "clusters": []}


# --------------------------------------------------------------------------- #
# Peek threshold — large roots skip the per-cluster peek (flags unknown)       #
# --------------------------------------------------------------------------- #
def test_browse_skips_peek_when_many_clusters():
    names = [f"0710-{i:04d}" for i in range(browse_mod._PEEK_LIMIT + 1)]
    lister = FakeUserLister(dirs={ROOT: _cluster_dirs(*names)})
    out = browse_mod.browse_root(lister, ROOT)
    assert len(out["clusters"]) == len(names)
    # Flags left unknown (null) — only the root was listed, no child peeks.
    for c in out["clusters"]:
        assert c["has_executor"] is None
        assert c["has_driver"] is None
    assert lister.listed_paths == [ROOT]


def test_browse_peek_failure_yields_unknown_flags():
    # Root lists fine, but the child dir listing fails -> flags unknown, not error.
    lister = FakeUserLister(
        dirs={ROOT: _cluster_dirs("0710-aaa")},  # child dir absent -> NotFound on peek
    )
    out = browse_mod.browse_root(lister, ROOT)
    assert len(out["clusters"]) == 1
    c = out["clusters"][0]
    assert c["has_executor"] is None
    assert c["has_driver"] is None


# --------------------------------------------------------------------------- #
# Errors                                                                       #
# --------------------------------------------------------------------------- #
def test_browse_permission_denied_maps_to_403():
    lister = FakeUserLister(dirs={ROOT: []}, denied=[ROOT])
    with pytest.raises(browse_mod.BrowseError) as ei:
        browse_mod.browse_root(lister, ROOT)
    assert ei.value.status_code == 403
    assert ei.value.reason_code == "FILES_FORBIDDEN"


def test_browse_not_found_maps_to_404():
    # ROOT not in dirs -> FakeUserLister raises NotFound.
    lister = FakeUserLister(dirs={"/Volumes/cat/logs/other": []})
    with pytest.raises(browse_mod.BrowseError) as ei:
        browse_mod.browse_root(lister, ROOT)
    assert ei.value.status_code == 404
    assert ei.value.reason_code == "CLD_ROOT_NOT_FOUND"


def test_browse_non_volume_path_rejected():
    lister = FakeUserLister(dirs={})
    for bad in ("/dbfs/foo", "/tmp/x", "not-a-path", ""):
        with pytest.raises(browse_mod.BrowseError) as ei:
            browse_mod.browse_root(lister, bad)
        assert ei.value.status_code == 400
        assert ei.value.reason_code == "NOT_VOLUME"
    # The non-Volumes rejects happen BEFORE any listing (never touch the lister).
    assert lister.listed_paths == []


def test_browse_normalizes_path():
    lister = FakeUserLister(dirs={ROOT: _cluster_dirs("0710-aaa")})
    # Trailing slash + dot segments are normalized to the canonical root.
    out = browse_mod.browse_root(lister, f"{ROOT}/./")
    assert out["path"] == ROOT


# --------------------------------------------------------------------------- #
# HIGH #1 — CLD_ROOT_ALLOWLIST enforcement                                     #
# --------------------------------------------------------------------------- #
ALLOWLIST = [ROOT]  # /Volumes/cat/logs/team_a


def test_browse_allowlisted_root_ok():
    """A path equal to an allowlisted root is accepted and listed."""
    lister = FakeUserLister(dirs={ROOT: _cluster_dirs("0710-aaa")})
    out = browse_mod.browse_root(lister, ROOT, allowlist=ALLOWLIST)
    assert out["path"] == ROOT
    assert [c["cluster_id"] for c in out["clusters"]] == ["0710-aaa"]


def test_browse_nested_under_allowlisted_ok():
    """A path NESTED under an allowlisted root is accepted."""
    nested = f"{ROOT}/0710-aaa"
    lister = FakeUserLister(dirs={nested: _cluster_dirs("executor")})
    out = browse_mod.browse_root(lister, nested, allowlist=ALLOWLIST)
    assert out["path"] == nested


def test_browse_non_allowlisted_path_rejected_before_listing():
    """A valid /Volumes path that is NOT under any allowlisted root is rejected
    with 403 CLD_ROOT_NOT_FOUND — BEFORE any listing (no existence oracle)."""
    other = "/Volumes/cat/logs/team_b/secrets"
    lister = FakeUserLister(dirs={other: _cluster_dirs("x")})
    with pytest.raises(browse_mod.BrowseError) as ei:
        browse_mod.browse_root(lister, other, allowlist=ALLOWLIST)
    assert ei.value.status_code == 403
    assert ei.value.reason_code == "CLD_ROOT_NOT_FOUND"
    assert ei.value.detail == "path is not an allowlisted log root"
    # Rejected before the lister was ever touched (no confirmation of existence).
    assert lister.listed_paths == []


def test_browse_sibling_prefix_not_treated_as_allowlisted():
    """/Volumes/cat/logs/team_alpha must NOT be considered under team_a."""
    sibling = "/Volumes/cat/logs/team_alpha"
    lister = FakeUserLister(dirs={sibling: _cluster_dirs("x")})
    with pytest.raises(browse_mod.BrowseError) as ei:
        browse_mod.browse_root(lister, sibling, allowlist=ALLOWLIST)
    assert ei.value.status_code == 403
    assert ei.value.reason_code == "CLD_ROOT_NOT_FOUND"
