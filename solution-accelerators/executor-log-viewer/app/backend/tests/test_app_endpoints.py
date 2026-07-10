"""Endpoint-level (FastAPI TestClient) tests for the security fixes.

Covers:
  - HIGH #2: /probe is absent (404) when ENABLE_DEBUG_PROBE is unset.
  - MEDIUM #5: a UC PermissionDenied on the content read maps to 403
    FILES_FORBIDDEN with no path leaked in the body.
  - HIGH #3 (endpoint side): a ref bound to a different user is rejected 403;
    a matching user is served.

These import ``app`` fresh with the debug flag controlled per-test so route
registration reflects the env at import time.
"""

from __future__ import annotations

import importlib
import sys

import pytest
from fastapi.testclient import TestClient

import filerefs
import logs

ROOT = "/Volumes/cat/logs/team_a"
BASE_DIR = f"{ROOT}/0710-abc"
STDERR = f"{BASE_DIR}/executor/app-1/0/stderr"
USER = "dev@example.com"
TOKEN_HEADER = {"x-forwarded-access-token": "fake-user-token"}


def _load_app(monkeypatch, *, enable_probe: bool):
    """(Re)import app.py with ENABLE_DEBUG_PROBE set as requested."""
    if enable_probe:
        monkeypatch.setenv("ENABLE_DEBUG_PROBE", "1")
    else:
        monkeypatch.delenv("ENABLE_DEBUG_PROBE", raising=False)
    monkeypatch.setenv(filerefs.SIGNING_SECRET_ENV, "endpoint-test-secret")
    sys.modules.pop("app", None)
    return importlib.import_module("app")


class _FakeMe:
    def __init__(self, user_name):
        self.user_name = user_name


class _FakeCurrentUser:
    def __init__(self, user_name):
        self._u = user_name

    def me(self):
        return _FakeMe(self._u)


class _FakeUserClient:
    """Stands in for the OBO WorkspaceClient. Exposes current_user only."""

    def __init__(self, user_name):
        self.current_user = _FakeCurrentUser(user_name)


# --------------------------------------------------------------------------- #
# HIGH #2 — /probe is 404 when the debug flag is unset                         #
# --------------------------------------------------------------------------- #
def test_probe_absent_when_flag_unset(monkeypatch):
    app_mod = _load_app(monkeypatch, enable_probe=False)
    client = TestClient(app_mod.app)
    resp = client.get("/probe?path=/Volumes/anything")
    assert resp.status_code == 404
    # And the fallback root HTML must not contain the probe button/script.
    root = client.get("/")
    assert "Run OBO capability probe" not in root.text
    assert "/probe?path=" not in root.text


def test_probe_present_when_flag_set(monkeypatch):
    app_mod = _load_app(monkeypatch, enable_probe=True)
    # We don't exercise the probe body (needs a real client); just prove the
    # route exists (not a 404). Without a token it returns a JSON error body.
    client = TestClient(app_mod.app)
    resp = client.get("/probe")
    assert resp.status_code != 404


# --------------------------------------------------------------------------- #
# MEDIUM #5 — UC PermissionDenied on content read -> 403, no path leaked       #
# --------------------------------------------------------------------------- #
def _mint(user=USER):
    return filerefs.mint_file_ref(
        path=STDERR, run_id="1", cluster_id="0710-abc", file_kind="stderr",
        user=user, root=ROOT, base_dir=BASE_DIR,
    )


def test_content_permission_denied_maps_to_403_no_path(monkeypatch):
    app_mod = _load_app(monkeypatch, enable_probe=False)

    monkeypatch.setattr(
        app_mod.auth, "build_user_client",
        lambda headers: _FakeUserClient(USER),
    )

    class ForbiddenReader:
        def __init__(self, client):
            pass

        def stat_size(self, path):
            class PermissionDenied(Exception):
                pass
            raise PermissionDenied(f"user lacks READ on {path}")

        def read_range(self, path, start, end):
            raise AssertionError("must not read after a denied stat")

    monkeypatch.setattr(app_mod.logs, "SdkFileReader", ForbiddenReader)

    client = TestClient(app_mod.app)
    resp = client.get(f"/api/log-files/{_mint()}?mode=tail", headers=TOKEN_HEADER)
    assert resp.status_code == 403
    body = resp.json()
    assert body["reason_code"] == "FILES_FORBIDDEN"
    # The SDK message (which may contain the raw path) must NOT leak.
    assert STDERR not in resp.text
    assert "/Volumes/" not in resp.text
    assert resp.headers["Cache-Control"] == "no-store"


# --------------------------------------------------------------------------- #
# HIGH #3 (endpoint) — ref bound to a different user rejected; match served    #
# --------------------------------------------------------------------------- #
class _OkReader:
    def __init__(self, client):
        pass

    def stat_size(self, path):
        return 10

    def read_range(self, path, start, end):
        return b"log bytes\n"


def test_ref_for_other_user_rejected_403(monkeypatch):
    app_mod = _load_app(monkeypatch, enable_probe=False)
    # The request user is someone ELSE than the ref's bound user.
    monkeypatch.setattr(
        app_mod.auth, "build_user_client",
        lambda headers: _FakeUserClient("attacker@example.com"),
    )
    monkeypatch.setattr(app_mod.logs, "SdkFileReader", _OkReader)

    client = TestClient(app_mod.app)
    resp = client.get(
        f"/api/log-files/{_mint(user=USER)}?mode=tail", headers=TOKEN_HEADER
    )
    assert resp.status_code == 403
    assert resp.json()["reason_code"] == "FILES_FORBIDDEN"


def test_ref_for_matching_user_served(monkeypatch):
    app_mod = _load_app(monkeypatch, enable_probe=False)
    monkeypatch.setattr(
        app_mod.auth, "build_user_client",
        lambda headers: _FakeUserClient(USER),
    )
    monkeypatch.setattr(app_mod.logs, "SdkFileReader", _OkReader)

    client = TestClient(app_mod.app)
    resp = client.get(
        f"/api/log-files/{_mint(user=USER)}?mode=tail", headers=TOKEN_HEADER
    )
    assert resp.status_code == 200
    assert resp.content == b"log bytes\n"
    assert resp.headers["Cache-Control"] == "no-store"


def test_unresolvable_user_fails_closed(monkeypatch):
    """If the OBO user cannot be resolved (empty user_name), the read is
    rejected (HIGH #3 fail-closed), not served."""
    app_mod = _load_app(monkeypatch, enable_probe=False)
    monkeypatch.setattr(
        app_mod.auth, "build_user_client",
        lambda headers: _FakeUserClient(""),  # no resolvable subject
    )
    monkeypatch.setattr(app_mod.logs, "SdkFileReader", _OkReader)

    client = TestClient(app_mod.app)
    resp = client.get(f"/api/log-files/{_mint()}?mode=tail", headers=TOKEN_HEADER)
    assert resp.status_code == 403
    assert resp.json()["reason_code"] == "FILES_FORBIDDEN"
