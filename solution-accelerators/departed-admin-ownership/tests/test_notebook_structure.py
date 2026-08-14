"""Structural checks on the notebook — verify it parses, has both phases, and keeps
the safe dry-run defaults. These run without a live workspace."""

import ast


def test_notebook_parses(nb_source):
    ast.parse(nb_source)  # raises on syntax error


def test_is_databricks_notebook(nb_source):
    assert nb_source.startswith("# Databricks notebook source")


def test_defines_expected_helpers(nb_source):
    tree = ast.parse(nb_source)
    funcs = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    for expected in (
        "iter_workspaces",
        "ws_client",
        "walk_wsfs",
        "active_recurring_job_paths",
        "do_transfer",
    ):
        assert expected in funcs, f"missing helper: {expected}"


def test_has_both_phases(nb_source):
    assert 'phase == "inventory"' in nb_source
    assert 'phase == "transfer"' in nb_source


def test_transfer_defaults_to_dry_run(nb_source):
    # The execute widget must default to "false" so a plain run never mutates.
    assert 'dbutils.widgets.dropdown("execute", "false"' in nb_source


def test_cloud_neutral_host(nb_source):
    # Host must come from the resolved client, not a hardcoded AWS suffix, in the
    # crawl loops. The only allowed literal is the account-console widget default.
    assert "w.config.host" in nb_source
    assert nb_source.count(".cloud.databricks.com") == 1  # widget default only


def test_wsfs_grants_can_manage_not_owner(nb_source):
    # WSFS has no owner — the transfer must grant CAN_MANAGE, not IS_OWNER.
    assert "CAN_MANAGE" in nb_source


def test_run_as_matches_effective_identity(nb_source):
    # run_as must match the EFFECTIVE identity (run_as_user_name), which also catches
    # creator-default jobs — not just explicitly-set run_as.
    assert "run_as_user_name" in nb_source


def test_run_as_uses_partial_job_update(nb_source):
    # Reassignment must set run_as via a partial jobs.update with JobRunAs, so tasks
    # and schedule are left intact.
    assert "jobs_update_run_as" in nb_source
    assert "JobRunAs(service_principal_name=" in nb_source


def test_run_as_requires_sp_map(nb_source):
    # scope_run_as must fail fast if no per-workspace SP map is provided.
    assert "run_as_sp_map" in nb_source
    assert "scope_run_as and not run_as_sp_map" in nb_source


def test_run_as_preflight_flags_grant_need(nb_source):
    # Inventory must classify each reassigned job as SP_HAS_ACCESS / SP_NEEDS_GRANT.
    assert "SP_NEEDS_GRANT" in nb_source
    assert "SP_HAS_ACCESS" in nb_source
    assert "sp_job_permission_levels" in nb_source


def test_run_as_grant_is_opt_in(nb_source):
    # Granting CAN_MANAGE to the SP must be gated behind grant_run_as_sp_perms.
    assert "grant_run_as_sp_perms" in nb_source
    assert "will_grant = grant_run_as_sp_perms and needs_grant" in nb_source


def test_run_as_grant_uses_service_principal_acl(nb_source):
    # The grant must target the SP via service_principal_name with CAN_MANAGE.
    assert "service_principal_name=grp" in nb_source


def test_transfer_guards_empty_inventory(nb_source):
    # spark.createDataFrame([]) can't infer a schema and raises — the transfer phase
    # must guard the empty case so an empty inventory reports cleanly, not crashes.
    assert "if results:" in nb_source
    assert "nothing to transfer" in nb_source
