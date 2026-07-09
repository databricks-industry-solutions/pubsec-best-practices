"""Security release-blocker tests (spec §7, Codex review #1 + governance).

These two properties GATE release:

  (A) A user cannot use the app to read a NON-LOG Volume file, even one they
      have UC access to. The app is a log viewer, not a generic file reader.
  (B) A user without READ on the Volume is denied — never served another
      user's log content. UC enforces this on the user's OBO Files call, and
      the app must surface it as a permission error, not leak bytes.

Both are exercised against the real modules with fakes injected — no live
workspace, no real SDK.
"""

from __future__ import annotations

import time

import pytest

import filerefs
import logs

SECRET = "release-blocker-secret"
ROOT = "/Volumes/cat/logs/team_a"
CLUSTER_DIR = f"{ROOT}/0709-005241-3ta2ot8g"
EXEC_STDERR = f"{CLUSTER_DIR}/executor/app-1/0/stderr"
NON_LOG = f"{CLUSTER_DIR}/../../secrets/customer_pii.csv"  # escapes / non-log
SIBLING_NON_LOG = f"{ROOT}/0709-005241-3ta2ot8g/executor/app-1/0/credentials.json"


@pytest.fixture(autouse=True)
def _secret(monkeypatch):
    monkeypatch.setenv(filerefs.SIGNING_SECRET_ENV, SECRET)


# --------------------------------------------------------------------------- #
# BLOCKER A — no non-log Volume file can ever be read through the app          #
# --------------------------------------------------------------------------- #
def test_cannot_mint_ref_for_non_log_file():
    """The resolver-facing mint API refuses non-log basenames outright."""
    with pytest.raises(filerefs.FileRefNotLog):
        filerefs.mint_file_ref(
            path=SIBLING_NON_LOG,  # under the root, but not a log file
            run_id="1",
            cluster_id="c",
            file_kind="log",
            user="u",
            root=ROOT,
        )


def test_cannot_mint_ref_that_escapes_root():
    """A path that escapes the bound root is refused at mint (path traversal)."""
    with pytest.raises(filerefs.FileRefPathEscape):
        filerefs.mint_file_ref(
            path=NON_LOG,
            run_id="1",
            cluster_id="c",
            file_kind="log",
            user="u",
            root=ROOT,
        )


def test_verify_rejects_forged_non_log_ref_even_if_signed():
    """Defense in depth: if a non-log ref were ever produced (future bug, or a
    valid signing key), verify STILL rejects it at read time — so no code path
    can turn a signed ref into a generic file read."""
    # Hand-build a *validly signed* payload that points at a non-log file by
    # bypassing mint's guard, to prove verify independently blocks it.
    import base64, json, hmac, hashlib

    payload = {
        "path": SIBLING_NON_LOG,
        "run_id": "1",
        "cluster_id": "c",
        "file_kind": "log",
        "user": "u",
        "root": ROOT,
        "base_dir": ROOT,
        "expiry": int(time.time()) + 600,
    }
    pj = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    b64 = base64.urlsafe_b64encode(pj.encode()).decode().rstrip("=")
    mac = hmac.new(SECRET.encode(), b64.encode(), hashlib.sha256).digest()
    sig = base64.urlsafe_b64encode(mac).decode().rstrip("=")
    forged = f"{b64}.{sig}"

    # Signature is VALID (not a tamper) — the rejection is specifically because
    # the bound path is not a recognized log file.
    with pytest.raises(filerefs.FileRefNotLog):
        filerefs.verify_file_ref(forged)


def test_read_log_never_reads_non_log_via_endpoint_path():
    """read_log (the endpoint's read call) refuses a non-log ref before it ever
    touches the reader — the reader is a tripwire that must NOT be called."""

    class TripwireReader:
        def stat_size(self, path):
            raise AssertionError(f"reader must never be called for {path}")

        def read_range(self, path, start, end):
            raise AssertionError(f"reader must never be called for {path}")

    import base64, json, hmac, hashlib

    payload = {
        "path": SIBLING_NON_LOG, "run_id": "1", "cluster_id": "c",
        "file_kind": "log", "user": "u", "root": ROOT, "base_dir": ROOT,
        "expiry": int(time.time()) + 600,
    }
    pj = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    b64 = base64.urlsafe_b64encode(pj.encode()).decode().rstrip("=")
    sig = base64.urlsafe_b64encode(
        hmac.new(SECRET.encode(), b64.encode(), hashlib.sha256).digest()
    ).decode().rstrip("=")
    forged = f"{b64}.{sig}"

    with pytest.raises(filerefs.FileRefNotLog):
        logs.read_log(forged, reader=TripwireReader(), mode="tail")


# --------------------------------------------------------------------------- #
# BLOCKER B — a valid log ref still reads via the USER's client; a UC denial   #
# on that read surfaces as an error, never as leaked bytes.                    #
# --------------------------------------------------------------------------- #
def test_forbidden_user_read_surfaces_as_error_not_bytes():
    """When the user's OBO Files read is denied by UC (PermissionDenied), the
    app raises — it does NOT fall back to any other identity or return bytes."""

    class ForbiddenReader:
        def stat_size(self, path):
            from databricks.sdk.errors import PermissionDenied  # type: ignore
            raise PermissionDenied("user lacks READ on this Volume")

        def read_range(self, path, start, end):
            raise AssertionError("must not read after a denied stat")

    ref = filerefs.mint_file_ref(
        path=EXEC_STDERR, run_id="1", cluster_id="c",
        file_kind="stderr", user="u", root=ROOT, base_dir=CLUSTER_DIR,
    )
    # MEDIUM #5: read_log now maps the UC PermissionDenied to a dedicated
    # ``logs.ContentForbidden`` (which the endpoint turns into 403 FILES_FORBIDDEN
    # with no path leaked). Crucially: no bytes are returned and the read never
    # advances past the denied stat.
    with pytest.raises(logs.ContentForbidden):
        logs.read_log(ref, reader=ForbiddenReader(), mode="tail")


def test_valid_log_ref_reads_only_its_own_bound_path():
    """A valid stderr ref reads exactly its bound path and nothing else — the
    reader is only ever asked for the path baked into the signed ref."""
    seen = {}

    class RecordingReader:
        def stat_size(self, path):
            seen["stat"] = path
            return 10

        def read_range(self, path, start, end):
            seen["read"] = path
            return b"log bytes\n"

    ref = filerefs.mint_file_ref(
        path=EXEC_STDERR, run_id="1", cluster_id="c",
        file_kind="stderr", user="u", root=ROOT,
    )
    result = logs.read_log(ref, reader=RecordingReader(), mode="tail")
    assert result.outcome == "OK"
    assert seen["stat"] == EXEC_STDERR
    assert seen["read"] == EXEC_STDERR
    assert result.headers["Cache-Control"] == "no-store"
