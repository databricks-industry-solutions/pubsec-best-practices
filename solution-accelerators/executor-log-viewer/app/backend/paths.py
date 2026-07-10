"""Path normalization + containment helpers.

Security-critical (spec §6). The resolver and file-ref verifier depend on these
to guarantee the app never reads outside a resolved / allowlisted CLD root.

These helpers operate on *logical* UC Volume paths (POSIX-style, e.g.
``/Volumes/<catalog>/<schema>/<volume>/<cld-root>/<cluster-id>/...``). They do
not touch the local filesystem, so ``os.path.realpath`` is deliberately NOT
used (it would resolve against the app container's disk, not the Volume).
"""

from __future__ import annotations

import posixpath
from urllib.parse import unquote


def normalize_path(path: str) -> str:
    """Normalize a logical Volume path.

    Handles: URL-encoding (percent-escapes), backslashes, duplicate/trailing
    slashes, and ``.``/``..`` segments (collapsed lexically via
    ``posixpath.normpath`` — no disk access). The result is always absolute
    (leading ``/``) and has no trailing slash (except the root ``/`` itself).

    Note: URL-decoding is applied once. A caller that receives an
    already-decoded path (e.g. from FastAPI) will not be double-decoded in a
    way that changes meaning, because a second decode of a normal path is a
    no-op; but paths containing a literal ``%`` should be pre-decoded by the
    framework, not passed here raw. We decode here to defend against
    ``%2e%2e`` style escape attempts arriving in a signed ref.
    """
    if path is None:
        raise ValueError("path is None")

    # Decode percent-escapes (defends against %2e%2e%2f traversal encoding).
    decoded = unquote(path)

    # Normalize Windows-style separators to POSIX.
    decoded = decoded.replace("\\", "/")

    # Force absolute so posixpath.normpath cannot produce a relative escape.
    if not decoded.startswith("/"):
        decoded = "/" + decoded

    # Collapse duplicate slashes, '.', and resolve '..' lexically.
    normalized = posixpath.normpath(decoded)

    # posixpath.normpath("//x") -> "//x" (POSIX allows leading //). Collapse it.
    while normalized.startswith("//"):
        normalized = normalized[1:]

    return normalized


def is_under_root(path: str, root: str) -> bool:
    """True iff ``path`` is contained within ``root`` after normalization.

    Both are normalized first. Containment is checked on path *segments* so
    that ``/Volumes/a/root-evil`` is NOT considered under ``/Volumes/a/root``.
    A path equal to the root is considered under it.
    """
    npath = normalize_path(path)
    nroot = normalize_path(root)

    if npath == nroot:
        return True

    # Ensure segment boundary: root must be a proper prefix ending on a slash.
    prefix = nroot if nroot.endswith("/") else nroot + "/"
    return npath.startswith(prefix)


def join_under_root(root: str, *parts: str) -> str:
    """Join ``parts`` onto ``root`` and verify the result stays under root.

    Raises ``ValueError`` if the joined+normalized path escapes ``root``.
    Useful when the resolver builds child paths from listing results.
    """
    nroot = normalize_path(root)
    joined = posixpath.join(nroot, *[p.lstrip("/") for p in parts])
    result = normalize_path(joined)
    if not is_under_root(result, nroot):
        raise ValueError("joined path escapes root")
    return result
