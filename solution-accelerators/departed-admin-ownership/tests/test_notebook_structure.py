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
