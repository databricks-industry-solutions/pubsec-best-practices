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
        workspace_workers=1,
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


def test_uc_sql_escapes_embedded_backtick():
    # An identifier containing a backtick must have it doubled so it can't break the
    # quoting (SQL-injection-resistant ALTER).
    row = {
        "proposed_new_owner": "adm`ins",
        "transfer_method": "sql_alter",
        "current_owner": "a@x.com",
        "object_type": "table",
        "full_name": "cat.sch.we`ird",
        "securable_type": "TABLE",
    }
    msg = core.transfer_row(_cfg(), row, None, "wh1", dry_run=True)
    assert "`we``ird`" in msg
    assert "`adm``ins`" in msg


def test_uc_rest_url_encodes_name():
    # A securable name with a space / slash must be URL-encoded in the PATCH path.
    row = {
        "proposed_new_owner": "admins",
        "transfer_method": "uc_rest",
        "current_owner": "a@x.com",
        "object_type": "external_location",
        "full_name": "my loc/prod",
        "securable_type": "EXTERNAL_LOCATION",
    }
    msg = core.transfer_row(_cfg(), row, None, "", dry_run=True)
    assert "my%20loc%2Fprod" in msg


# --- all-purpose cluster inventory + grant revocation ---------------------------
import types  # noqa: E402
from unittest.mock import MagicMock  # noqa: E402


def _perm(level, inherited=False):
    return types.SimpleNamespace(
        permission_level=types.SimpleNamespace(value=level), inherited=inherited
    )


def _acl(user, perms):
    return types.SimpleNamespace(
        user_name=user, group_name=None, service_principal_name=None, all_permissions=perms
    )


def _cluster(cid, name, source):
    return types.SimpleNamespace(
        cluster_id=cid, cluster_name=name, cluster_source=types.SimpleNamespace(value=source)
    )


def _prin():
    return core.Principals(
        by_key={"gone@x.com": "gone@x.com"},
        emails={"gone@x.com"},
        target_group="grp",
        target_group_present=True,
        scim_resolved={"gone@x.com"},
    )


def _cluster_w():
    """Mock WorkspaceClient: two all-purpose clusters and one job cluster. Clusters have
    NO IS_OWNER level — only CAN_ATTACH_TO/CAN_RESTART/CAN_MANAGE. The departed admin
    holds CAN_MANAGE on c1 and CAN_ATTACH_TO+CAN_RESTART on c2. c2 also has another
    principal (peer@) with two direct levels — exercising per-principal collapse — and
    an inherited admins grant that must be ignored."""
    w = MagicMock()
    w.clusters.list.return_value = [
        _cluster("c1", "owned", "UI"),
        _cluster("c2", "shared", "API"),
        _cluster("cj", "jobclust", "JOB"),
    ]

    def perms_get(request_object_type, request_object_id):
        if request_object_id == "c1":
            acls = [
                _acl("gone@x.com", [_perm("CAN_MANAGE")]),
                _acl("other@x.com", [_perm("CAN_ATTACH_TO")]),
            ]
        elif request_object_id == "c2":
            acls = [
                _acl("peer@x.com", [_perm("CAN_RESTART"), _perm("CAN_MANAGE")]),
                _acl("gone@x.com", [_perm("CAN_ATTACH_TO"), _perm("CAN_RESTART")]),
                _acl("admins", [_perm("CAN_MANAGE", inherited=True)]),
            ]
        else:
            acls = []
        return types.SimpleNamespace(access_control_list=acls)

    w.permissions.get.side_effect = perms_get
    return w


def test_inventory_clusters_always_revoke_never_transfer():
    rows = list(core.inventory_clusters(_cfg(), _prin(), _cluster_w(), 111, "https://h"))
    # job cluster excluded; both all-purpose clusters emit a revoke row (clusters have
    # no owner, so nothing is ever transferred).
    assert len(rows) == 2
    assert all(r["object_type"] == "cluster_acl" for r in rows)
    assert all(r["transfer_method"] == "cluster_revoke" for r in rows)
    by_name = {r["full_name"]: r for r in rows}
    assert by_name["owned"]["extra"] == "EXPLICIT_PERMISSION=CAN_MANAGE"
    # strongest of the admin's grants on c2 (CAN_ATTACH_TO, CAN_RESTART)
    assert by_name["shared"]["extra"] == "EXPLICIT_PERMISSION=CAN_RESTART"


def test_cluster_revoke_removes_only_departed_admin_and_dedupes_principals():
    w = _cluster_w()
    captured = {}
    w.permissions.set.side_effect = lambda request_object_type, request_object_id, access_control_list: captured.update(
        acl=access_control_list
    )
    row = {
        "securable_type": "clusters",
        "object_id": "c2",
        "full_name": "shared",
        "current_owner": "gone@x.com",
        "matched_admin": "gone@x.com",
        "transfer_method": "cluster_revoke",
        "proposed_new_owner": "grp",
    }
    msg = core.transfer_row(_cfg(), row, w, "", dry_run=False)
    assert msg.startswith("OK revoked gone@x.com")
    kept = [(a.user_name, a.permission_level.value) for a in captured["acl"]]
    assert not any(u == "gone@x.com" for u, _ in kept)  # departed admin removed
    assert not any(u == "admins" for u, _ in kept)  # inherited grant not re-sent
    # peer@ preserved as exactly ONE entry, collapsed to its strongest level
    peer = [lvl for u, lvl in kept if u == "peer@x.com"]
    assert peer == ["CAN_MANAGE"]


def test_cluster_revoke_dry_run_needs_no_client():
    # Regression: cmd_transfer builds no workspace client in dry-run mode, so a
    # cluster_revoke dry-run must not touch the client (w is None). It should preview
    # from row['extra'] rather than reading the live ACL.
    row = {
        "securable_type": "clusters",
        "object_id": "c2",
        "full_name": "shared",
        "current_owner": "gone@x.com",
        "matched_admin": "gone@x.com",
        "transfer_method": "cluster_revoke",
        "proposed_new_owner": "",
        "extra": "EXPLICIT_PERMISSION=CAN_RESTART",
    }
    msg = core.transfer_row(_cfg(), row, None, "", dry_run=True)  # w is None
    assert msg.startswith("DRY-RUN revoke gone@x.com")
    assert "CAN_RESTART" in msg


# --- workspace-object ownership reassignment (to an SP, never a group) ----------
def _acl_sp(sp, perms):
    return types.SimpleNamespace(
        user_name=None, group_name=None, service_principal_name=sp, all_permissions=perms
    )


def _perms_row(**over):
    row = {
        "securable_type": "jobs",
        "object_id": "J1",
        "full_name": "etl",
        "current_owner": "gone@x.com",
        "matched_admin": "gone@x.com",
        "proposed_new_owner": "sp-owner-app",
        "transfer_method": "permissions_api",
    }
    row.update(over)
    return row


def test_reassign_owner_dry_run_needs_no_client():
    # Dry-run must not touch the client (cmd_transfer builds none in dry-run).
    msg = core.transfer_row(_cfg(), _perms_row(), None, "", dry_run=True)
    assert msg.startswith("DRY-RUN set SP sp-owner-app IS_OWNER")
    assert "was gone@x.com" in msg


def test_reassign_owner_skips_without_sp():
    # No run_as SP configured for this workspace -> cannot reassign (group can't own).
    msg = core.transfer_row(_cfg(), _perms_row(proposed_new_owner=""), None, "", dry_run=True)
    assert msg.startswith("SKIP") and "run_as_sp_map" in msg


def test_reassign_owner_puts_sp_as_sole_owner():
    w = MagicMock()
    w.permissions.get.side_effect = lambda request_object_type, request_object_id: types.SimpleNamespace(
        access_control_list=[
            _acl("gone@x.com", [_perm("IS_OWNER")]),  # departed admin (old owner)
            _acl_sp("ci-sp-app", [_perm("CAN_VIEW")]),  # preserve
            _acl("admins", [_perm("CAN_MANAGE", inherited=True)]),  # inherited -> drop
        ]
    )
    captured = {}
    w.permissions.set.side_effect = lambda request_object_type, request_object_id, access_control_list: captured.update(
        acl=access_control_list
    )
    msg = core.transfer_row(_cfg(), _perms_row(), w, "", dry_run=False)
    assert msg.startswith("OK set SP sp-owner-app IS_OWNER")
    owners = [a for a in captured["acl"] if a.permission_level == core.iam.PermissionLevel.IS_OWNER]
    assert len(owners) == 1 and owners[0].service_principal_name == "sp-owner-app"  # exactly one owner = SP
    assert not any(getattr(a, "user_name", None) == "gone@x.com" for a in captured["acl"])  # admin dropped
    assert not any(getattr(a, "group_name", None) == "admins" for a in captured["acl"])  # inherited not re-sent
    # the other principal's direct grant is preserved
    assert any(a.service_principal_name == "ci-sp-app" for a in captured["acl"])


def test_reassign_owner_dedupes_existing_sp_grant_case_insensitive():
    # The SP may already hold a non-owner grant, possibly in different case than
    # run_as_sp_map stores. It must appear exactly once (as IS_OWNER), never duplicated.
    w = MagicMock()
    w.permissions.get.side_effect = lambda request_object_type, request_object_id: types.SimpleNamespace(
        access_control_list=[
            _acl("gone@x.com", [_perm("IS_OWNER")]),
            _acl_sp("SP-Owner-App", [_perm("CAN_MANAGE")]),  # same SP, different case
        ]
    )
    captured = {}
    w.permissions.set.side_effect = lambda request_object_type, request_object_id, access_control_list: captured.update(
        acl=access_control_list
    )
    core.transfer_row(_cfg(), _perms_row(proposed_new_owner="sp-owner-app"), w, "", dry_run=False)
    sp_entries = [a for a in captured["acl"] if (a.service_principal_name or "").lower() == "sp-owner-app"]
    assert len(sp_entries) == 1
    assert sp_entries[0].permission_level == core.iam.PermissionLevel.IS_OWNER


def test_scope_ws_without_sp_is_warning_not_error():
    # Regression: enabling scope_ws without a run_as_sp_map must NOT be a fatal config
    # error (inventory should still run); ownership rows just get skipped at transfer.
    cfg = _cfg(scope_ws=True, scope_run_as=False, run_as_sp_map={}, account_host="https://acct")
    cfg.validate()  # must not raise SystemExit


def test_dedupe_uc_drops_repeat_metastore_rows_keeps_per_workspace():
    from main import _dedupe_uc

    uc = {"domain": "unity_catalog", "object_type": "table", "full_name": "cat.sch.tbl"}
    ws = {"domain": "workspace", "object_type": "job", "full_name": "etl"}
    seen: set = set()
    first = _dedupe_uc([dict(uc), dict(ws)], seen)
    second = _dedupe_uc([dict(uc), dict(ws)], seen)  # same UC row from another workspace
    assert len(first) == 2  # UC + workspace both kept the first time
    assert len(second) == 1  # UC row deduped, per-workspace job row kept
    assert second[0]["domain"] == "workspace"
