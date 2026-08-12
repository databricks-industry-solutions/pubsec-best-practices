"""Tests for the per-workspace run_as SP mapping logic.

The parsing/lookup is small and self-contained; we replicate it here to lock in the
contract (JSON parse, str-normalized keys, per-workspace lookup, skip when absent)
that the notebook relies on.
"""

import json

import pytest


def parse_sp_map(raw: str) -> dict:
    """Mirror of the notebook's run_as_sp_map parsing (str-normalized keys/values)."""
    if not raw:
        return {}
    return {str(k): str(v).strip() for k, v in json.loads(raw).items()}


def test_parse_sp_map_normalizes_keys():
    # JSON object keys are strings; workspace_id lookups must match as strings.
    m = parse_sp_map('{"1234567890": "sp-abc", "9876543210": "sp-def"}')
    assert m == {"1234567890": "sp-abc", "9876543210": "sp-def"}


def test_parse_sp_map_empty():
    assert parse_sp_map("") == {}


def test_parse_sp_map_invalid_json_raises():
    with pytest.raises(json.JSONDecodeError):
        parse_sp_map("{not json}")


def test_per_workspace_lookup():
    m = parse_sp_map('{"111": "sp-one", "222": "sp-two"}')
    # A workspace with an entry gets its own SP.
    assert m.get(str(111)) == "sp-one"
    assert m.get(str(222)) == "sp-two"
    # A workspace with no entry is skipped (None) — the notebook logs and moves on.
    assert m.get(str(333)) is None


def test_values_are_stripped():
    m = parse_sp_map('{"111": "  sp-one  "}')
    assert m["111"] == "sp-one"


# --- Preflight SP-permission classification -------------------------------------

SP_RUN_CAPABLE = {"CAN_MANAGE", "IS_OWNER"}


def classify(sp_levels: list) -> str:
    """Mirror of the notebook's preflight flag from an SP's job permission levels."""
    return "SP_HAS_ACCESS" if set(sp_levels) & SP_RUN_CAPABLE else "SP_NEEDS_GRANT"


@pytest.mark.parametrize(
    "levels,expected",
    [
        (["CAN_MANAGE"], "SP_HAS_ACCESS"),
        (["IS_OWNER"], "SP_HAS_ACCESS"),
        (["CAN_VIEW", "CAN_MANAGE"], "SP_HAS_ACCESS"),
        (["CAN_MANAGE_RUN"], "SP_NEEDS_GRANT"),  # run-only is not enough to run_as
        (["CAN_VIEW"], "SP_NEEDS_GRANT"),
        ([], "SP_NEEDS_GRANT"),  # SP not on the ACL at all
    ],
)
def test_preflight_classification(levels, expected):
    assert classify(levels) == expected


def test_grant_decision_is_opt_in():
    # will_grant only when both the opt-in flag is set AND the job needs a grant.
    def will_grant(flag, extra):
        return flag and extra == "SP_NEEDS_GRANT"

    assert will_grant(True, "SP_NEEDS_GRANT") is True
    assert will_grant(False, "SP_NEEDS_GRANT") is False  # opt-in off -> no grant
    assert will_grant(True, "SP_HAS_ACCESS") is False  # already has access -> skip
