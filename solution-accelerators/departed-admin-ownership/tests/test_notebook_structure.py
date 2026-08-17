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


def test_inventory_has_run_identity_columns(nb_source):
    # Every row is stamped with a run identity so the table can retain many runs.
    assert '"run_id",' in nb_source
    assert '"run_timestamp",' in nb_source
    assert "RUN_ID = " in nb_source
    assert "RUN_TIMESTAMP = " in nb_source


def test_inventory_appends_for_history(nb_source):
    # Inventory must append (retain history), not overwrite the table each run.
    assert 'mode("append")' in nb_source
    assert 'mode("overwrite")' not in nb_source
    assert 'option("mergeSchema", "true")' in nb_source


def test_transfer_reads_latest_run_only(nb_source):
    # With history retained, the transfer phase must operate on the latest run only.
    assert "max(run_id)" in nb_source
    assert "run_id = '{latest_run}'" in nb_source


def _code_only(cell: str) -> str:
    """Drop comment lines so substring checks see real code, not prose in comments."""
    return "\n".join(ln for ln in cell.splitlines() if not ln.lstrip().startswith("#"))


def test_display_and_exit_never_in_same_cell(nb_source):
    # dbutils.notebook.exit() stops the notebook and suppresses a display() queued in the
    # SAME cell — so no cell may contain both. Regression guard: the transfer results grid
    # previously failed to render because display() and exit() shared a cell.
    cells = [_code_only(c) for c in nb_source.split("# COMMAND ----------")]
    offenders = [
        i for i, c in enumerate(cells) if "display(" in c and "dbutils.notebook.exit(" in c
    ]
    assert not offenders, f"cells containing both display() and exit(): {offenders}"


def test_transfer_persists_results_and_surfaces_failures(nb_source):
    # Per-row outcomes are written to a durable results table (so failures survive even
    # when display output isn't rendered), and non-success rows are printed explicitly.
    assert "_transfer_results" in nb_source
    assert "did NOT transfer" in nb_source


def test_clusters_scoped_to_all_purpose(nb_source):
    # Only all-purpose clusters (cluster_source UI/API) are inventoried — job/pipeline
    # clusters are excluded.
    assert "ALL_PURPOSE_CLUSTER_SOURCES" in nb_source
    assert "cluster_source" in nb_source


def test_cluster_explicit_grant_is_revoked_not_transferred(nb_source):
    # Explicit non-owner cluster grants are revoked (permissions.set), not handed to
    # the group. Only non-inherited grants are matched.
    assert "cluster_revoke" in nb_source
    assert "object_type=\"cluster_acl\"" in nb_source
    assert "not p.inherited" in nb_source
    assert "permissions.set(" in nb_source
