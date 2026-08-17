"""Structural checks on the workspace-local notebook (transfer_ownership_local.py).

The whole point of this variant is that it stays workspace-local: no AccountClient, no
account SP, no cross-workspace sweep. These tests lock that contract in, plus the safe
dry-run defaults and feature parity with the account-based notebook. No live workspace.
"""

import ast


def test_local_notebook_parses(nb_local_source):
    ast.parse(nb_local_source)


def test_local_is_databricks_notebook(nb_local_source):
    assert nb_local_source.startswith("# Databricks notebook source")


def _code_names(src: str) -> set:
    """All identifier names that appear in the notebook's *code* (not comments/strings)
    — imports, attributes, calls, assignments. Lets us assert on real code, ignoring
    prose in the markdown/docstring cells that legitimately name AccountClient etc."""
    tree = ast.parse(src)
    names: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.alias):
            names.add((node.asname or node.name).split(".")[0])
            names.add(node.name)
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                names.add(a.name)
    return names


def test_local_uses_no_account_client(nb_local_source):
    # The defining property: never import or construct an AccountClient, and never do a
    # cross-workspace sweep — this must run entirely against the ambient workspace. We
    # check the CODE (AST), so the "no AccountClient" prose in the docs cell is ignored.
    names = _code_names(nb_local_source)
    assert "AccountClient" not in names
    assert "get_workspace_client" not in names
    assert "iter_workspaces" not in names
    assert "secrets" not in names  # no account-SP secret bootstrap (dbutils.secrets)


def test_local_uses_ambient_workspace_client(nb_local_source):
    # Ambient auth: a bare WorkspaceClient(), and a cloud-neutral host from its config.
    assert "w = WorkspaceClient()" in nb_local_source
    assert "w.config.host" in nb_local_source


def test_local_resolves_principals_from_workspace_scim(nb_local_source):
    # Principal resolution + target-group check must use workspace-local SCIM.
    assert "w.users.list(" in nb_local_source
    assert "w.groups.list(" in nb_local_source


def test_local_has_both_phases(nb_local_source):
    assert 'phase == "inventory"' in nb_local_source
    assert 'phase == "transfer"' in nb_local_source


def test_local_transfer_defaults_to_dry_run(nb_local_source):
    assert 'dbutils.widgets.dropdown("execute", "false"' in nb_local_source


def test_local_wsfs_grants_can_manage(nb_local_source):
    assert "CAN_MANAGE" in nb_local_source


def test_local_run_as_uses_single_sp_not_map(nb_local_source):
    # One workspace per job, so run_as targets a single run_as_sp, not a JSON map.
    assert "run_as_sp" in nb_local_source
    assert "run_as_sp_map" not in nb_local_source
    assert "scope_run_as and not run_as_sp" in nb_local_source


def test_local_run_as_matches_effective_identity(nb_local_source):
    assert "run_as_user_name" in nb_local_source


def test_local_run_as_uses_partial_job_update(nb_local_source):
    assert "jobs_update_run_as" in nb_local_source
    assert "JobRunAs(service_principal_name=" in nb_local_source


def test_local_run_as_preflight_flags_grant_need(nb_local_source):
    assert "SP_NEEDS_GRANT" in nb_local_source
    assert "SP_HAS_ACCESS" in nb_local_source
    assert "sp_job_permission_levels" in nb_local_source


def test_local_run_as_grant_is_opt_in(nb_local_source):
    assert "will_grant = grant_run_as_sp_perms and needs_grant" in nb_local_source


def test_local_uc_alter_escapes_backticks(nb_local_source):
    # ALTER OWNER identifiers must double embedded backticks (injection-resistant).
    assert 'replace("`", "``")' in nb_local_source


def test_local_transfer_guards_empty_inventory(nb_local_source):
    # spark.createDataFrame([]) can't infer a schema and raises — the transfer phase
    # must guard the empty case so an empty inventory reports cleanly, not crashes.
    assert "if results:" in nb_local_source
    assert "nothing to transfer" in nb_local_source


def test_local_inventory_has_run_identity_columns(nb_local_source):
    # Every row is stamped with a run identity so the table can retain many runs.
    assert '"run_id",' in nb_local_source
    assert '"run_timestamp",' in nb_local_source
    assert "RUN_ID = " in nb_local_source
    assert "RUN_TIMESTAMP = " in nb_local_source


def test_local_inventory_appends_for_history(nb_local_source):
    # Inventory must append (retain history), not overwrite the table each run.
    assert 'mode("append")' in nb_local_source
    assert 'mode("overwrite")' not in nb_local_source
    assert 'option("mergeSchema", "true")' in nb_local_source


def test_local_transfer_reads_latest_run_only(nb_local_source):
    # With history retained, the transfer phase must operate on the latest run only.
    assert "max(run_id)" in nb_local_source
    assert "run_id = '{latest_run}'" in nb_local_source


def _code_only(cell: str) -> str:
    """Drop comment lines so substring checks see real code, not prose in comments."""
    return "\n".join(ln for ln in cell.splitlines() if not ln.lstrip().startswith("#"))


def test_local_display_and_exit_never_in_same_cell(nb_local_source):
    # dbutils.notebook.exit() stops the notebook and suppresses a display() queued in the
    # SAME cell — so no cell may contain both. Regression guard: the transfer results grid
    # previously failed to render because display() and exit() shared a cell.
    cells = [_code_only(c) for c in nb_local_source.split("# COMMAND ----------")]
    offenders = [
        i for i, c in enumerate(cells) if "display(" in c and "dbutils.notebook.exit(" in c
    ]
    assert not offenders, f"cells containing both display() and exit(): {offenders}"


def test_local_transfer_persists_results_and_surfaces_failures(nb_local_source):
    # Per-row outcomes are written to a durable results table (so failures survive even
    # when display output isn't rendered), and non-success rows are printed explicitly.
    assert "_transfer_results" in nb_local_source
    assert "did NOT transfer" in nb_local_source


def test_local_clusters_scoped_to_all_purpose(nb_local_source):
    assert "ALL_PURPOSE_CLUSTER_SOURCES" in nb_local_source
    assert "cluster_source" in nb_local_source


def test_local_cluster_explicit_grant_is_revoked_not_transferred(nb_local_source):
    assert "cluster_revoke" in nb_local_source
    assert "object_type=\"cluster_acl\"" in nb_local_source
    assert "not p.inherited" in nb_local_source
    assert "permissions.set(" in nb_local_source
