"""Tests for the account-access offboarding script's pure logic + arg validation.

No live account needed — we exercise the direct/indirect classification, the scope
validation, and the safe dry-run default.
"""

import sys
import types
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

offb = pytest.importorskip("offboard_account_access", reason="requires databricks-sdk")


def _cv(type_=None):
    """Minimal stand-in for a SCIM ComplexValue (role/entitlement)."""
    return types.SimpleNamespace(type=type_, value="account_admin")


def test_is_direct_classification():
    assert offb._is_direct(_cv(type_=None)) is True          # untyped -> direct
    assert offb._is_direct(_cv(type_="direct")) is True
    assert offb._is_direct(_cv(type_="indirect")) is False    # from a group
    assert offb._is_direct(_cv(type_="INDIRECT")) is False     # case-insensitive


def test_all_scopes_value():
    assert set(offb.ALL_SCOPES) == {"groups", "roles", "entitlements", "workspace_assignments"}


def test_unknown_scope_exits():
    with pytest.raises(SystemExit):
        offb.main(["--profile", "p", "--admins", "a@x.com", "--scope", "bogus"])


def _ns(**kw):
    return types.SimpleNamespace(**kw)


def _dry_run_safety_ac(mutation_guard):
    """A fake AccountClient with a resolvable user who has a group membership, a direct
    role/entitlement, and a workspace assignment — every mutating method calls
    mutation_guard() so a test can assert none fire in dry-run."""

    class _Users:
        def list(self, filter=None):
            return [_ns(id="123", user_name="gone@x.com")]

        def get(self, id):
            return _ns(id="123", user_name="gone@x.com", active=True,
                       roles=[_ns(type=None, value="account_admin")],
                       entitlements=[_ns(type=None, value="allow-cluster-create")])

        def patch(self, *a, **k):
            mutation_guard("users.patch")

    class _Groups:
        def list(self, attributes=None):
            return [_ns(id="g1", display_name="grp")]

        def get(self, gid):
            return _ns(id="g1", display_name="grp", members=[_ns(value="123")])

        def patch(self, *a, **k):
            mutation_guard("groups.patch")

    class _WSAssign:
        def list(self, workspace_id):
            return [_ns(principal=_ns(principal_id=123, user_name="gone@x.com"),
                        permissions=["WorkspacePermission.ADMIN"])]

        def delete(self, *a, **k):
            mutation_guard("workspace_assignment.delete")

    class _API:
        def do(self, *a, **k):
            mutation_guard("api_client.do")

    class _AC:
        def __init__(self, *a, **k):
            self.config = _ns(account_id="acct")
            self.users = _Users()
            self.groups = _Groups()
            self.workspace_assignment = _WSAssign()
            self.api_client = _API()
            self.workspaces = _ns(list=lambda: [_ns(workspace_id=1)])

    return _AC


def test_dry_run_performs_no_mutations(monkeypatch):
    # The safety-critical guarantee: without --execute, NO mutating call is made across
    # any scope (groups/roles/entitlements/workspace assignments) or --deactivate.
    fired = []
    monkeypatch.setattr(offb, "AccountClient", _dry_run_safety_ac(fired.append))
    rc = offb.main(["--profile", "p", "--admins", "gone@x.com", "--deactivate"])  # no --execute
    assert rc == 0
    assert fired == [], f"dry-run made mutating calls: {fired}"


def _fake_ac_with(recorded, active=True):
    """A fake AccountClient that resolves one user and records raw api_client.do calls."""
    class _Users:
        def list(self, filter=None):
            return [types.SimpleNamespace(id="123", user_name="gone@x.com")]

        def get(self, id):
            return types.SimpleNamespace(id="123", user_name="gone@x.com", active=active)

    class _API:
        def do(self, method, path, body=None):
            recorded.append({"method": method, "path": path, "body": body})

    class _AC:
        def __init__(self, *a, **k):
            self.config = types.SimpleNamespace(account_id="acct")
            self.users = _Users()
            self.api_client = _API()

    return _AC


def test_deactivate_execute_sends_raw_active_false(monkeypatch):
    # The deactivate final step must PATCH the account SCIM with an explicit
    # value=False body (the SDK's iam.Patch drops boolean False).
    recorded: list = []
    monkeypatch.setattr(offb, "AccountClient", _fake_ac_with(recorded))
    rc = offb.main(["--profile", "p", "--admins", "gone@x.com", "--scope", "", "--deactivate", "--execute"])
    assert rc == 0
    assert len(recorded) == 1
    call = recorded[0]
    assert call["method"] == "PATCH" and call["path"].endswith("/scim/v2/Users/123")
    assert call["body"]["Operations"][0] == {"op": "replace", "path": "active", "value": False}


def test_deactivate_dry_run_makes_no_call(monkeypatch):
    recorded: list = []
    monkeypatch.setattr(offb, "AccountClient", _fake_ac_with(recorded))
    rc = offb.main(["--profile", "p", "--admins", "gone@x.com", "--scope", "", "--deactivate"])  # no --execute
    assert rc == 0
    assert recorded == []  # dry-run mutates nothing


def test_deactivate_skips_already_inactive(monkeypatch):
    recorded: list = []
    monkeypatch.setattr(offb, "AccountClient", _fake_ac_with(recorded, active=False))
    offb.main(["--profile", "p", "--admins", "gone@x.com", "--scope", "", "--deactivate", "--execute"])
    assert recorded == []  # already inactive -> no PATCH
