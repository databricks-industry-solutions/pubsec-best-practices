"""Tests for the CLI core module (cli/core.py).

Unlike the notebook (which has top-level dbutils/spark calls), the CLI modules are
importable, so we test their pure logic directly. No live workspace needed.
"""

import sys
from pathlib import Path

import pytest

CLI_DIR = Path(__file__).parent.parent / "cli"
sys.path.insert(0, str(CLI_DIR))

core = pytest.importorskip("core", reason="requires databricks-sdk (installed via uv sync)")


# --- WSFS pure helpers ----------------------------------------------------------
def test_norm_path_strips_workspace_prefix():
    assert core.norm_path("/Workspace/Applications/SAT/x") == "/Applications/SAT/x"
    assert core.norm_path("/Users/a@x.com/nb/") == "/Users/a@x.com/nb"
    assert core.norm_path("/Workspace") == "/"
    assert core.norm_path("") == ""


def test_is_noise_path():
    assert core.is_noise_path("/Users/a@x.com/repo/.venv/lib/foo.py") is True
    assert core.is_noise_path("/Users/a@x.com/proj/node_modules/x") is True
    assert core.is_noise_path("/Users/a@x.com/repo/src/main.py") is False


def test_path_is_active():
    amap = {
        "/Users/d@x.com/etl/main": ["Nightly ETL"],
        "/Users/d@x.com/shared": ["Hourly Sync", "Model Refresh"],
    }
    assert core.path_is_active("/Users/d@x.com/etl/main", amap) == ["Nightly ETL"]
    assert core.path_is_active("/Users/d@x.com/etl", amap) == ["Nightly ETL"]  # parent dir
    assert core.path_is_active("/Users/d@x.com/shared", amap) == ["Hourly Sync", "Model Refresh"]
    assert core.path_is_active("/Users/d@x.com/none", amap) == []
    # a child of an active path is not itself flagged as the parent
    assert core.path_is_active("/Users/d@x.com/shared/deep", amap) == []


# --- run_as preflight classification -------------------------------------------
@pytest.mark.parametrize(
    "levels,expected",
    [
        (["CAN_MANAGE"], "SP_HAS_ACCESS"),
        (["IS_OWNER"], "SP_HAS_ACCESS"),
        (["CAN_VIEW", "CAN_MANAGE"], "SP_HAS_ACCESS"),
        (["CAN_MANAGE_RUN"], "SP_NEEDS_GRANT"),  # run-only is not enough to run_as
        (["CAN_VIEW"], "SP_NEEDS_GRANT"),
        ([], "SP_NEEDS_GRANT"),
    ],
)
def test_classify_sp_access(levels, expected):
    assert core.classify_sp_access(levels) == expected


# --- transfer dry-run strings (no client needed) --------------------------------
def _cfg(**over):
    from config import Config

    base = dict(
        bootstrap_profile="p",
        secret_scope="s",
        secret_key_client_id="",
        secret_key_client_secret="",
        account_host="",
        account_id="",
        departed_admins=["a@x.com"],
        target_group="admins",
        scope_uc=False,
        scope_ws=False,
        scope_wsfs=False,
        scope_run_as=True,
        workspace_ids=[],
        skip_catalogs=[],
        sql_warehouse_ids={},
        wsfs_max_depth=0,
        wsfs_workers=8,
        run_as_sp_map={},
        grant_run_as_sp_perms=False,
        output_dir=Path("/tmp"),
        raw={},
    )
    base.update(over)
    return Config(**base)


def _run_as_row(extra):
    return {
        "proposed_new_owner": "sp-123",
        "transfer_method": "jobs_update_run_as",
        "current_owner": "a@x.com",
        "object_id": "999",
        "full_name": "etl",
        "extra": extra,
        "securable_type": "jobs",
    }


def test_run_as_dry_run_warns_when_grant_off():
    msg = core.transfer_row(_cfg(), _run_as_row("SP_NEEDS_GRANT"), None, "", dry_run=True)
    assert "SP LACKS ACCESS" in msg and "grant_run_as_sp_perms=true" in msg


def test_run_as_dry_run_grants_when_opted_in():
    msg = core.transfer_row(
        _cfg(grant_run_as_sp_perms=True), _run_as_row("SP_NEEDS_GRANT"), None, "", dry_run=True
    )
    assert "+ grant CAN_MANAGE" in msg


def test_run_as_dry_run_clean_when_sp_has_access():
    msg = core.transfer_row(
        _cfg(grant_run_as_sp_perms=True), _run_as_row("SP_HAS_ACCESS"), None, "", dry_run=True
    )
    assert "grant CAN_MANAGE" not in msg and "LACKS ACCESS" not in msg


def test_uc_sql_dry_run_backticks_name():
    row = {
        "proposed_new_owner": "admins",
        "transfer_method": "sql_alter",
        "current_owner": "a@x.com",
        "object_type": "table",
        "full_name": "cat.sch.tbl",
        "securable_type": "TABLE",
    }
    msg = core.transfer_row(_cfg(), row, None, "wh1", dry_run=True)
    assert "ALTER TABLE `cat`.`sch`.`tbl` OWNER TO `admins`" in msg
