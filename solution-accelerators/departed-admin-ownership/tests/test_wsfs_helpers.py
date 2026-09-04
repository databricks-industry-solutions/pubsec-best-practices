"""Unit tests for the WSFS pure helpers (path normalization, active-job flagging,
noise filtering) extracted from the notebook."""


def test_norm_path_strips_workspace_prefix(helpers):
    norm = helpers["norm_path"]
    assert norm("/Workspace/Applications/SAT/x") == "/Applications/SAT/x"
    assert norm("/Users/a@x.com/nb/") == "/Users/a@x.com/nb"
    assert norm("/Workspace") == "/"
    assert norm("") == ""


def test_is_noise_path(helpers):
    noise = helpers["is_noise_path"]
    assert noise("/Users/a@x.com/repo/.venv/lib/foo.py") is True
    assert noise("/Users/a@x.com/proj/node_modules/x") is True
    assert noise("/Users/a@x.com/repo/.git/config") is True
    assert noise("/Users/a@x.com/repo/src/main.py") is False
    assert noise("/Users/a@x.com/nb") is False


def test_path_is_active_exact_and_parent(helpers):
    active = helpers["path_is_active"]
    amap = {
        "/Users/dep@x.com/etl/main": ["Nightly ETL"],
        "/Users/dep@x.com/shared": ["Hourly Sync", "Model Refresh"],
    }
    # exact notebook match
    assert active("/Users/dep@x.com/etl/main", amap) == ["Nightly ETL"]
    # a directory whose subtree contains an active path
    assert active("/Users/dep@x.com/etl", amap) == ["Nightly ETL"]
    # directory that maps to two active jobs
    assert active("/Users/dep@x.com/shared", amap) == ["Hourly Sync", "Model Refresh"]
    # unrelated path
    assert active("/Users/dep@x.com/unrelated", amap) == []


def test_path_is_active_does_not_match_deeper_sibling(helpers):
    active = helpers["path_is_active"]
    amap = {"/Users/dep@x.com/shared": ["Hourly Sync"]}
    # a child of the active path is not itself flagged as the parent
    assert active("/Users/dep@x.com/shared/sub/deep", amap) == []
