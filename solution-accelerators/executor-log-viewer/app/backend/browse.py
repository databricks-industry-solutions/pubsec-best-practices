"""User-scoped Volume browsing (discovery path).

The recent-runs list is sourced from the app SP's Jobs API visibility, which
needs per-job grants and does NOT reflect the viewing user. This module adds a
USER-SCOPED discovery path: list the CLD log Volume directly with the USER's
OBO ``files.files`` token. Because Unity Catalog enforces per-user access on the
Volume listing, "list the CLD root" == "the log directories THIS user can see"
— zero SP grants, self-maintaining, survives cluster aging-out.

SECURITY (spec §3.1): this module only LISTS directory NAMES. It never reads
file content and never mints a ``file_ref``. Content reads stay exclusively
through ``logs.read_log`` via a signed ref minted by the resolver. Do NOT add a
content-read capability here.

Identity: all listing goes through the USER OBO ``FileLister`` seam (reused
from ``resolver.SdkFileLister``). A UC per-user denial surfaces as
``PermissionDenied`` -> ``FILES_FORBIDDEN``; a missing path -> ``CLD_ROOT_NOT_FOUND``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional

from paths import is_under_root, join_under_root, normalize_path
from resolver import FileLister, _is_permission_denied

# Peeking each cluster dir for executor/driver presence is one extra cheap
# listing per cluster. Only do it when the directory count is small so a big
# root stays fast; above this the flags are left unknown (null) and the
# resolve-on-click determines them.
_PEEK_LIMIT = 50


class BrowseError(Exception):
    """Structured browse failure carrying an HTTP status + reason_code.

    Messages are safe to surface to the client and never leak internals.
    """

    def __init__(self, status_code: int, reason_code: str, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.reason_code = reason_code
        self.detail = detail


@dataclass
class ClusterCandidate:
    cluster_id: str  # the dir basename
    path: str  # the normalized /Volumes/... dir path (names only; not a ref)
    modified: Optional[int]
    has_executor: Optional[bool]  # None => unknown (not peeked)
    has_driver: Optional[bool]  # None => unknown (not peeked)


def _not_found(exc: Exception) -> bool:
    return type(exc).__name__ in (
        "NotFound",
        "ResourceDoesNotExist",
        "FileNotFoundError",
    )


def _peek_flags(lister: FileLister, cluster_dir: str) -> tuple[Optional[bool], Optional[bool]]:
    """One cheap listing of a cluster dir to detect executor/ and driver/.

    Returns ``(has_executor, has_driver)``. Any listing failure (denied on the
    child, not-found, transient) yields ``(None, None)`` — unknown, not an
    error: the row is still browsable and resolve-on-click gives the real answer.
    """
    try:
        entries = list(lister.list_dir(cluster_dir))
    except Exception:  # noqa: BLE001 - unknown flags are fine; never fatal here
        return (None, None)
    names = {
        getattr(e, "name", "")
        for e in entries
        if getattr(e, "is_directory", False)
    }
    return ("executor" in names, "driver" in names)


def browse_root(
    lister: FileLister, path: str, *, allowlist: Optional[list[str]] = None
) -> dict:
    """List the immediate cluster-level subdirectories under a CLD root, AS THE
    USER (spec §3.3 discovery path).

    Parameters
    ----------
    lister : FileLister
        The USER OBO lister (``resolver.SdkFileLister(user_client)``). UC
        enforces per-user access on every listing here.
    path : str
        A ``/Volumes/...`` directory path to browse. HIGH #1: this MUST be
        equal to, or nested under, one of the allowlisted CLD roots — the app is
        an executor-log viewer, not a generic Volume browser.
    allowlist : list[str], optional
        The configured CLD roots (``resolver.load_allowlist()``). When provided
        (always, in production), a ``path`` that is not equal-to / nested-under
        an allowlisted root is rejected with 403 ``CLD_ROOT_NOT_FOUND`` BEFORE
        any listing — the raw path is never echoed in a way that confirms
        existence. ``None`` (tests only) skips the check.

    Returns
    -------
    dict
        ``{"path": <normalized>, "clusters": [ClusterCandidate-as-dict, ...]}``.
        Sorted by ``modified`` descending (unknown modified sorts last).

    Raises
    ------
    BrowseError
        - non-``/Volumes/`` path -> 400 ``NOT_VOLUME``
        - path not under any allowlisted root -> 403 ``CLD_ROOT_NOT_FOUND``
        - UC per-user denial -> 403 ``FILES_FORBIDDEN``
        - missing path -> 404 ``CLD_ROOT_NOT_FOUND``
    """
    if not path or not path.strip():
        raise BrowseError(400, "NOT_VOLUME", "a /Volumes path is required")

    npath = normalize_path(path)
    if not (npath == "/Volumes" or npath.startswith("/Volumes/")):
        raise BrowseError(
            400, "NOT_VOLUME", "browse path must be a /Volumes/... path"
        )

    # HIGH #1: enforce the CLD_ROOT_ALLOWLIST. The browsed path must be equal to
    # or nested under an allowlisted root; otherwise 403 without confirming
    # whether the path exists (no raw-path echo, no existence oracle).
    if allowlist is not None:
        if not any(is_under_root(npath, root) for root in allowlist):
            raise BrowseError(
                403, "CLD_ROOT_NOT_FOUND", "path is not an allowlisted log root"
            )

    try:
        entries = list(lister.list_dir(npath))
    except Exception as exc:  # noqa: BLE001
        if _is_permission_denied(exc):
            raise BrowseError(
                403, "FILES_FORBIDDEN", "you don't have READ on this Volume path"
            ) from None
        if _not_found(exc):
            raise BrowseError(
                404, "CLD_ROOT_NOT_FOUND", "path not found"
            ) from None
        # Never leak other errors' internals.
        raise BrowseError(
            404, "CLD_ROOT_NOT_FOUND", "path not found or not listable"
        ) from None

    subdirs = [
        e
        for e in entries
        if getattr(e, "is_directory", False) and getattr(e, "name", None)
    ]

    do_peek = len(subdirs) <= _PEEK_LIMIT
    candidates: list[ClusterCandidate] = []
    for entry in subdirs:
        name = getattr(entry, "name", None)
        if not name:
            continue
        entry_path = getattr(entry, "path", None) or join_under_root(npath, name)
        entry_path = normalize_path(entry_path)
        has_exec: Optional[bool] = None
        has_driver: Optional[bool] = None
        if do_peek:
            has_exec, has_driver = _peek_flags(lister, entry_path)
        candidates.append(
            ClusterCandidate(
                cluster_id=name,
                path=entry_path,
                modified=getattr(entry, "last_modified", None),
                has_executor=has_exec,
                has_driver=has_driver,
            )
        )

    # Sort by modified desc; unknown (None) sorts last, then by cluster_id.
    candidates.sort(
        key=lambda c: (c.modified is None, -(c.modified or 0), c.cluster_id)
    )

    return {
        "path": npath,
        "clusters": [asdict(c) for c in candidates],
    }
