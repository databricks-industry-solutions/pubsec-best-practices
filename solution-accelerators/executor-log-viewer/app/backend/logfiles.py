"""Single source of truth for the recognized-log-file boundary (spec §3.1).

The app is an executor/driver *log* viewer, not a generic Volume file reader.
Exactly ONE predicate decides "is this a log file we may read?" and it is used
by BOTH the resolver (which discovers files by listing) and ``filerefs``
(which mints/verifies signed refs). Keeping a single definition prevents the
loose-vs-strict drift found in the security cross-validation (MEDIUM #4):
``filerefs.is_log_basename`` used to accept anything starting with ``stdout`` /
``stderr`` (e.g. ``stderr_credentials.json``), which was looser than the
resolver's classifier.

Recognized log files (and ONLY these):
  - exact ``stdout`` / exact ``stderr``
  - rotated ``stdout--<suffix>`` / ``stderr--<suffix>``
  - any ``*.log`` file
"""

from __future__ import annotations

import re

# Exact stdout/stderr, or rotated stdout--<suffix> / stderr--<suffix>.
# The "--" separator is REQUIRED for a suffix, so "stderr_credentials.json" and
# "stdout.secret" do NOT match (they have neither "--" nor a ".log" extension).
_ROTATED_RE = re.compile(r"^std(?:out|err)(?:--.+)?$")


def _basename(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


def classify_log_file(name_or_path: str) -> str | None:
    """Return a ``file_kind`` for a recognized log basename, else ``None``.

    ``file_kind`` is one of ``stdout`` / ``stderr`` / ``log``. Accepts either a
    bare basename or a full path (the basename is taken). This is the single
    strict classifier shared by the resolver and the file-ref layer.
    """
    base = _basename(name_or_path)
    if _ROTATED_RE.match(base):
        return "stdout" if base.startswith("stdout") else "stderr"
    if base.endswith(".log"):
        return "log"
    return None


def is_log_basename(path: str) -> bool:
    """True iff the path's basename is a recognized log file (spec §3.1)."""
    return classify_log_file(path) is not None
