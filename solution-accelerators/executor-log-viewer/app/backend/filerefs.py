"""Opaque, HMAC-signed file references (spec §3.1).

The browser NEVER supplies a raw Volume path. The resolver mints an opaque
``file_ref`` for each discovered log file, binding::

    {path, run_id, cluster_id, file_kind, user, expiry, root, base_dir}

On every read the backend verifies the ref and re-checks, independently, that
the bound path is still under BOTH the bound CLD ``root`` and the resolved
``base_dir`` (the ``<cld_dest>/<cluster-id>`` cluster directory the file was
listed under). Defense in depth — the signature guarantees the payload wasn't
tampered, the root check guarantees the app can't be turned into a generic
Volume reader even if a signing key were ever misused, and the ``base_dir``
check (MEDIUM #6) guarantees a ref can only ever read a file under the exact
cluster directory it was discovered in, not a sibling cluster under the same
team root.

The ref is a URL-safe base64 token: ``base64(payload_json).base64(hmac)``.
The signing secret comes from the ``FILEREF_SIGNING_SECRET`` env var.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import asdict, dataclass

from logfiles import is_log_basename  # single strict classifier (MEDIUM #4)
from paths import is_under_root, normalize_path

SIGNING_SECRET_ENV = "FILEREF_SIGNING_SECRET"
DEFAULT_TTL_SECONDS = 15 * 60  # refs are short-lived; minted per view.

# ``is_log_basename`` is intentionally imported (not redefined) from
# ``logfiles`` so the resolver and the file-ref layer share ONE strict
# definition of "recognized log file" (MEDIUM #4: the old local predicate here
# was looser — it accepted ``stderr_credentials.json`` etc.).


class FileRefError(Exception):
    """Base class for file-ref failures. Messages never contain the token."""


class FileRefExpired(FileRefError):
    pass


class FileRefTampered(FileRefError):
    pass


class FileRefPathEscape(FileRefError):
    pass


class FileRefNotLog(FileRefError):
    """The bound path is not a recognized log file (security boundary §3.1)."""


class FileRefUserMismatch(FileRefError):
    """The signed ``user`` subject does not match the reading request's user
    (HIGH #3). Raised with the SAME generic response as tamper/escape so it is
    never a user-existence oracle."""


@dataclass(frozen=True)
class FileRefPayload:
    path: str
    run_id: str
    cluster_id: str
    file_kind: str
    user: str
    root: str
    base_dir: str  # resolved <cld_dest>/<cluster-id> dir the file was listed in
    expiry: int  # unix epoch seconds


def _secret() -> bytes:
    secret = os.environ.get(SIGNING_SECRET_ENV)
    if not secret:
        raise FileRefError(
            f"{SIGNING_SECRET_ENV} is not set; cannot sign/verify file refs"
        )
    return secret.encode("utf-8")


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64d(txt: str) -> bytes:
    pad = "=" * (-len(txt) % 4)
    return base64.urlsafe_b64decode(txt + pad)


def _sign(payload_b64: str) -> str:
    mac = hmac.new(_secret(), payload_b64.encode("ascii"), hashlib.sha256)
    return _b64e(mac.digest())


def mint_file_ref(
    *,
    path: str,
    run_id: str,
    cluster_id: str,
    file_kind: str,
    user: str,
    root: str,
    base_dir: str | None = None,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    now: float | None = None,
) -> str:
    """Create a signed, opaque file ref. ``path`` is normalized and must be
    under ``root`` at mint time (fail fast if the resolver hands us garbage).

    ``base_dir`` (MEDIUM #6) is the resolved ``<cld_dest>/<cluster-id>``
    directory the file was listed under. The path must be under it too, and it
    is baked into the signed payload so verify can re-check containment against
    the specific cluster directory — not just the (broader) team root. Defaults
    to ``root`` when a caller has no narrower directory (manual/probe paths).
    """
    npath = normalize_path(path)
    nroot = normalize_path(root)
    nbase = normalize_path(base_dir) if base_dir is not None else nroot
    if not is_under_root(npath, nroot):
        raise FileRefPathEscape("path is not under root at mint time")
    if not is_under_root(npath, nbase):
        raise FileRefPathEscape("path is not under base_dir at mint time")
    if not is_log_basename(npath):
        raise FileRefNotLog("refusing to mint a ref for a non-log file")

    now = time.time() if now is None else now
    payload = FileRefPayload(
        path=npath,
        run_id=run_id,
        cluster_id=cluster_id,
        file_kind=file_kind,
        user=user,
        root=nroot,
        base_dir=nbase,
        expiry=int(now) + int(ttl_seconds),
    )
    payload_json = json.dumps(asdict(payload), sort_keys=True, separators=(",", ":"))
    payload_b64 = _b64e(payload_json.encode("utf-8"))
    sig = _sign(payload_b64)
    return f"{payload_b64}.{sig}"


def verify_file_ref(
    file_ref: str,
    *,
    expected_user: str | None = None,
    now: float | None = None,
) -> FileRefPayload:
    """Verify a file ref and return its payload.

    Rejects: malformed tokens, tampered payloads (bad signature), expired refs,
    payloads whose bound path escapes the bound root OR the bound ``base_dir``
    after normalization (MEDIUM #6), and — when ``expected_user`` is supplied
    (HIGH #3) — payloads whose signed ``user`` subject does not match the
    reading request's user. Never includes the raw token or secret in raised
    messages, and the user-mismatch rejection is the SAME generic response as a
    tamper/escape so it is never a user-existence oracle.
    """
    if not file_ref or "." not in file_ref:
        raise FileRefTampered("malformed file ref")

    payload_b64, _, sig = file_ref.partition(".")
    if not payload_b64 or not sig:
        raise FileRefTampered("malformed file ref")

    expected = _sign(payload_b64)
    if not hmac.compare_digest(expected, sig):
        raise FileRefTampered("signature mismatch")

    try:
        data = json.loads(_b64d(payload_b64).decode("utf-8"))
        payload = FileRefPayload(**data)
    except (ValueError, TypeError) as exc:  # bad base64/json/shape
        raise FileRefTampered("undecodable payload") from exc

    # Re-check containment independently of the signature (defense in depth).
    if not is_under_root(payload.path, payload.root):
        raise FileRefPathEscape("bound path escapes bound root")

    # MEDIUM #6: also re-check containment under the resolved cluster/base dir,
    # so a ref can only read a file under the exact directory it was discovered
    # in — never a sibling cluster's log under the same team root.
    if not is_under_root(payload.path, payload.base_dir):
        raise FileRefPathEscape("bound path escapes bound base_dir")

    # Re-check the log-file boundary at read time too: even a hypothetically
    # mis-minted or future-code-path ref can never read a non-log Volume file.
    if not is_log_basename(payload.path):
        raise FileRefNotLog("bound path is not a recognized log file")

    # HIGH #3: bind the ref to the user. A ref is otherwise a bearer token until
    # expiry; verifying the signed subject against the current OBO user closes
    # that gap. Fail closed if the caller could not resolve a user subject.
    if expected_user is not None:
        if not expected_user or not payload.user or expected_user != payload.user:
            raise FileRefUserMismatch("file ref is bound to a different user")

    now = time.time() if now is None else now
    if now > payload.expiry:
        raise FileRefExpired("file ref expired")

    return payload
