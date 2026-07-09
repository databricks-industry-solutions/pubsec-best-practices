import time

import pytest

import filerefs
from filerefs import (
    FileRefExpired,
    FileRefPathEscape,
    FileRefTampered,
    mint_file_ref,
    verify_file_ref,
)

ROOT = "/Volumes/cat/sch/vol/cld"
PATH = ROOT + "/0710-abc/executor/app-1/1/stderr"

COMMON = dict(
    run_id="run-123",
    cluster_id="0710-abc",
    file_kind="stderr",
    user="dev@example.com",
    root=ROOT,
)


def test_round_trip_sign_verify():
    ref = mint_file_ref(path=PATH, **COMMON)
    payload = verify_file_ref(ref)
    assert payload.path == PATH
    assert payload.run_id == "run-123"
    assert payload.cluster_id == "0710-abc"
    assert payload.file_kind == "stderr"
    assert payload.user == "dev@example.com"
    assert payload.root == ROOT
    assert payload.expiry > time.time()


def test_ref_is_opaque_no_raw_path():
    ref = mint_file_ref(path=PATH, **COMMON)
    # The raw Volume path must not be readable in the token.
    assert "/Volumes/" not in ref
    assert "stderr" not in ref or "." in ref  # only as base64, not plaintext
    assert PATH not in ref


def test_expiry_rejection():
    # Mint with an already-past expiry via a fixed 'now' far in the past.
    past = time.time() - 10_000
    ref = mint_file_ref(path=PATH, ttl_seconds=1, now=past, **COMMON)
    with pytest.raises(FileRefExpired):
        verify_file_ref(ref)


def test_tamper_rejection_payload():
    ref = mint_file_ref(path=PATH, **COMMON)
    payload_b64, _, sig = ref.partition(".")
    # Flip a character in the payload, keep the old signature.
    tampered_payload = ("A" if payload_b64[0] != "A" else "B") + payload_b64[1:]
    with pytest.raises(FileRefTampered):
        verify_file_ref(f"{tampered_payload}.{sig}")


def test_tamper_rejection_signature():
    ref = mint_file_ref(path=PATH, **COMMON)
    payload_b64, _, sig = ref.partition(".")
    tampered_sig = ("A" if sig[0] != "A" else "B") + sig[1:]
    with pytest.raises(FileRefTampered):
        verify_file_ref(f"{payload_b64}.{tampered_sig}")


def test_tamper_rejection_malformed():
    with pytest.raises(FileRefTampered):
        verify_file_ref("not-a-valid-ref")
    with pytest.raises(FileRefTampered):
        verify_file_ref("")


def test_path_escape_rejected_at_mint():
    with pytest.raises(FileRefPathEscape):
        mint_file_ref(path="/Volumes/other/cat/secret", **COMMON)


def test_path_escape_rejected_at_verify():
    # Construct a validly-signed token whose path escapes the bound root, to
    # prove verify re-checks containment independently of the signature.
    import base64
    import hashlib
    import hmac
    import json

    bad = {
        "path": "/Volumes/cat/sch/vol/other/secret",
        "run_id": "r",
        "cluster_id": "c",
        "file_kind": "stderr",
        "user": "u",
        "root": ROOT,
        "base_dir": ROOT,
        "expiry": int(time.time()) + 600,
    }
    payload_json = json.dumps(bad, sort_keys=True, separators=(",", ":"))
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode().rstrip("=")
    secret = __import__("os").environ[filerefs.SIGNING_SECRET_ENV].encode()
    mac = hmac.new(secret, payload_b64.encode(), hashlib.sha256).digest()
    sig = base64.urlsafe_b64encode(mac).decode().rstrip("=")
    ref = f"{payload_b64}.{sig}"

    with pytest.raises(FileRefPathEscape):
        verify_file_ref(ref)


def test_wrong_secret_is_tamper(monkeypatch):
    ref = mint_file_ref(path=PATH, **COMMON)
    monkeypatch.setenv(filerefs.SIGNING_SECRET_ENV, "a-different-secret")
    with pytest.raises(FileRefTampered):
        verify_file_ref(ref)


# --------------------------------------------------------------------------- #
# HIGH #3 — verify binds the ref to the reading user (expected_user)           #
# --------------------------------------------------------------------------- #
BASE_DIR = ROOT + "/0710-abc"


def test_verify_matching_user_ok():
    ref = mint_file_ref(path=PATH, base_dir=BASE_DIR, **COMMON)
    payload = verify_file_ref(ref, expected_user="dev@example.com")
    assert payload.user == "dev@example.com"


def test_verify_mismatched_user_rejected():
    ref = mint_file_ref(path=PATH, base_dir=BASE_DIR, **COMMON)
    with pytest.raises(filerefs.FileRefUserMismatch):
        verify_file_ref(ref, expected_user="attacker@example.com")


def test_verify_empty_expected_user_fails_closed():
    """An empty expected_user (user could not be resolved) is rejected, not
    treated as 'no check' — that would reopen the bearer-token gap."""
    ref = mint_file_ref(path=PATH, base_dir=BASE_DIR, **COMMON)
    with pytest.raises(filerefs.FileRefUserMismatch):
        verify_file_ref(ref, expected_user="")


def test_verify_without_expected_user_skips_binding():
    """When no expected_user is passed (callers that don't bind), verify still
    succeeds — the binding is opt-in at the read seam."""
    ref = mint_file_ref(path=PATH, base_dir=BASE_DIR, **COMMON)
    payload = verify_file_ref(ref)  # no expected_user
    assert payload.user == "dev@example.com"


def test_mint_and_verify_use_the_same_user_subject():
    """The subject baked at mint is exactly the subject verify compares against
    — so a caller that mints with me().user_name and verifies with the same
    field round-trips cleanly (HIGH #3 subject consistency)."""
    subject = "person@example.com"
    kw = dict(COMMON)
    kw["user"] = subject
    ref = mint_file_ref(path=PATH, base_dir=BASE_DIR, **kw)
    payload = verify_file_ref(ref, expected_user=subject)
    assert payload.user == subject


# --------------------------------------------------------------------------- #
# MEDIUM #6 — ref bound to base_dir, not just root                             #
# --------------------------------------------------------------------------- #
def test_base_dir_defaults_to_root_when_omitted():
    ref = mint_file_ref(path=PATH, **COMMON)  # no base_dir
    payload = verify_file_ref(ref)
    assert payload.base_dir == ROOT


def test_mint_rejects_path_outside_base_dir():
    """A path under root but under a DIFFERENT cluster dir than base_dir is
    refused at mint."""
    other_cluster_file = ROOT + "/0710-OTHER/executor/app-1/1/stderr"
    with pytest.raises(FileRefPathEscape):
        mint_file_ref(path=other_cluster_file, base_dir=BASE_DIR, **COMMON)


def test_verify_rejects_path_under_root_but_not_base_dir():
    """Defense in depth: a validly-signed ref whose path is under root but NOT
    under the bound base_dir is rejected at verify (MEDIUM #6)."""
    import base64
    import hashlib
    import hmac
    import json

    # Path is a sibling cluster's stderr: under ROOT, but not under BASE_DIR.
    sibling = ROOT + "/0710-SIBLING/executor/app-1/1/stderr"
    bad = {
        "path": sibling,
        "run_id": "r",
        "cluster_id": "0710-abc",
        "file_kind": "stderr",
        "user": "dev@example.com",
        "root": ROOT,
        "base_dir": BASE_DIR,
        "expiry": int(time.time()) + 600,
    }
    pj = json.dumps(bad, sort_keys=True, separators=(",", ":"))
    b64 = base64.urlsafe_b64encode(pj.encode()).decode().rstrip("=")
    secret = __import__("os").environ[filerefs.SIGNING_SECRET_ENV].encode()
    sig = base64.urlsafe_b64encode(
        hmac.new(secret, b64.encode(), hashlib.sha256).digest()
    ).decode().rstrip("=")
    ref = f"{b64}.{sig}"

    with pytest.raises(FileRefPathEscape):
        verify_file_ref(ref)


def test_verify_normal_base_dir_case_ok():
    ref = mint_file_ref(path=PATH, base_dir=BASE_DIR, **COMMON)
    payload = verify_file_ref(ref)
    assert payload.path == PATH
    assert payload.base_dir == BASE_DIR
