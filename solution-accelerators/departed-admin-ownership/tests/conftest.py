"""Pytest fixtures for departed-admin-ownership.

The logic lives in the Databricks notebook (single source of truth). Because the
notebook has top-level dbutils/spark calls, we can't import it directly — instead we
extract the self-contained pure helper functions by AST and exec them into an
isolated namespace so they can be unit-tested without a live workspace.
"""

import ast
from pathlib import Path

import pytest

NOTEBOOK = Path(__file__).parent.parent / "notebooks" / "transfer_ownership.py"

# Pure helpers with no dependency on notebook globals (dbutils/spark/log).
_PURE_FUNCS = {"is_noise_path", "norm_path", "path_is_active", "task_paths"}
_PURE_ASSIGNS = {"WSFS_NOISE_SEGMENTS", "WSFS_PERM_TYPE"}


@pytest.fixture(scope="session")
def nb_source() -> str:
    return NOTEBOOK.read_text()


@pytest.fixture(scope="session")
def helpers(nb_source):
    """Namespace containing the notebook's pure helper functions."""
    tree = ast.parse(nb_source)
    keep: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in _PURE_FUNCS:
            keep.append(node)
        elif isinstance(node, ast.Assign):
            targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
            if targets & _PURE_ASSIGNS:
                keep.append(node)
    ns: dict = {}
    module = ast.Module(body=keep, type_ignores=[])
    exec(compile(module, filename=str(NOTEBOOK), mode="exec"), ns)  # noqa: S102
    return ns
