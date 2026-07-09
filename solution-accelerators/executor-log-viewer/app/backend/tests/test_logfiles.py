"""MEDIUM #4 — the single strict log-basename classifier.

One shared predicate (``logfiles``) is used by BOTH the resolver and the
file-ref mint/verify. It must be STRICT: exact ``stdout`` / ``stderr``,
rotated ``stdout--<suffix>`` / ``stderr--<suffix>``, or ``*.log`` — and must
REJECT lookalikes such as ``stderr_credentials.json`` that the old loose
``filerefs.is_log_basename`` used to accept.
"""

import filerefs
import logfiles
import resolver


ACCEPTED = [
    "stdout",
    "stderr",
    "stderr--2026-07-08--18-00",
    "stdout--1",
    "foo.log",
    "app.log",
    "/Volumes/cat/logs/team_a/0710/executor/app-1/0/stderr",
]

REJECTED = [
    "stderr_credentials.json",
    "stdout.secret",
    "stderr.parquet",
    "stdoutx",           # no "--" separator
    "stderrfoo",         # no "--" separator
    "credentials.json",
    "data.csv",
    "log",               # not ".log"
    "stdout.log.enc",    # not a .log suffix
]


def test_accepted_basenames():
    for name in ACCEPTED:
        assert logfiles.is_log_basename(name) is True, name
        assert logfiles.classify_log_file(name) is not None, name


def test_rejected_basenames():
    for name in REJECTED:
        assert logfiles.is_log_basename(name) is False, name
        assert logfiles.classify_log_file(name) is None, name


def test_classify_returns_expected_kinds():
    assert logfiles.classify_log_file("stdout") == "stdout"
    assert logfiles.classify_log_file("stderr") == "stderr"
    assert logfiles.classify_log_file("stdout--x") == "stdout"
    assert logfiles.classify_log_file("stderr--x") == "stderr"
    assert logfiles.classify_log_file("foo.log") == "log"


def test_resolver_and_filerefs_share_one_predicate():
    """Both modules import the SAME classifier — no drift is possible."""
    assert resolver._classify_log_file is logfiles.classify_log_file
    assert filerefs.is_log_basename is logfiles.is_log_basename


def test_filerefs_refuses_to_mint_lookalike(monkeypatch):
    """The former loose predicate would have minted stderr_credentials.json;
    the shared strict one refuses it."""
    root = "/Volumes/cat/logs/team_a"
    path = f"{root}/0710/executor/app-1/0/stderr_credentials.json"
    import pytest
    with pytest.raises(filerefs.FileRefNotLog):
        filerefs.mint_file_ref(
            path=path, run_id="1", cluster_id="c", file_kind="log",
            user="u", root=root, base_dir=root,
        )
