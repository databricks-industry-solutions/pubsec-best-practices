"""Read one log file by file_ref (spec §3.2, §3.3).

Two modes:
  - ``tail``: last 256 KB of the file (default view).
  - ``full``: whole file, capped at ``full_cap_bytes`` (default 10 MB). Over the
    cap -> a ``FILE_TOO_LARGE`` outcome (no bytes returned).

Metadata (total size, returned range, whether earlier bytes were hidden) is
returned via RESPONSE HEADERS, because the body is streamed text and cannot
carry JSON fields. All responses set ``Cache-Control: no-store``.

The actual Databricks Files-API read is ISOLATED behind ``FileReader`` /
``SdkFileReader`` (spec: "range-read mechanics unconfirmed until the Phase 0
spike"). Phase 0 confirms the exact SDK method + whether a native suffix/range
read exists; if it does, ``SdkFileReader.read_range`` is the single place to
swap in. Until then it falls back to a full download sliced in memory, which is
correct but not bandwidth-optimal. Tests inject a fake reader.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import os

from filerefs import FileRefPayload, verify_file_ref

TAIL_BYTES = 256 * 1024  # 256 KB
DEFAULT_FULL_CAP_BYTES = 10 * 1024 * 1024  # 10 MB

# MEDIUM #7: when only a whole-object download is available (no native range
# read), cap how much we will ever pull into memory for the download-and-slice
# fallback. Prevents a 256 KB tail request against a multi-GB object from
# OOM-ing / saturating bandwidth. Configurable via env for ops.
MAX_FALLBACK_DOWNLOAD_BYTES = int(
    os.environ.get("MAX_FALLBACK_DOWNLOAD_BYTES", str(64 * 1024 * 1024))  # 64 MB
)


def _is_permission_denied(exc: Exception) -> bool:
    """Recognize a Databricks UC per-user access denial on a content read.

    Matches on the exception CLASS NAME (not an import) so it works across SDK
    versions and with test fakes — consistent with ``resolver._is_permission_denied``.
    """
    name = type(exc).__name__
    return (
        name in ("PermissionDenied", "Forbidden")
        or "PermissionDenied" in name
        or "Forbidden" in name
    )


class ContentForbidden(Exception):
    """UC denied the user's content read (stat/read). Mapped by the endpoint to
    403 ``FILES_FORBIDDEN`` WITHOUT leaking the SDK message (MEDIUM #5)."""

# Response headers carrying metadata (spec §3.2).
HDR_TOTAL_SIZE = "X-Log-Total-Size"
HDR_RANGE_START = "X-Log-Range-Start"
HDR_RANGE_END = "X-Log-Range-End"
HDR_EARLIER_HIDDEN = "X-Log-Earlier-Bytes-Hidden"
HDR_MODE = "X-Log-Mode"
HDR_OUTCOME = "X-Log-Outcome"
CACHE_CONTROL_VALUE = "no-store"


class FileReader(Protocol):
    """Isolation seam for the Files API. Phase 0 confirms the real mechanics."""

    def stat_size(self, path: str) -> int:
        """Return total file size in bytes."""

    def read_range(self, path: str, start: int, end: int) -> bytes:
        """Return bytes for the half-open range [start, end). ``end`` may exceed
        the file size; the reader must clamp and return what exists."""


class SdkFileReader:
    """Real reader backed by a Databricks SDK ``WorkspaceClient``.

    ISOLATED so Phase 0 can confirm/replace the exact call. The current
    implementation assumes only ``files.download`` (full buffered download) is
    guaranteed; native range/suffix reads are a Phase-0 unknown. If Phase 0
    proves a range read, replace ``read_range`` with it and delete the slice.
    """

    def __init__(self, client):
        self._client = client

    def stat_size(self, path: str) -> int:
        # PHASE-0 TODO: confirm the metadata call. Candidates:
        #   client.files.get_metadata(path).content_length
        #   or a HEAD-style call. Falls back to len(download) if unavailable.
        meta = self._client.files.get_metadata(path)
        size = getattr(meta, "content_length", None)
        if size is None:
            return len(self._download_all(path))
        return int(size)

    def read_range(self, path: str, start: int, end: int) -> bytes:
        # MEDIUM #7: prefer a bounded read. We stream the response handle and
        # stop after ``end`` bytes so we never buffer more than we need (a tail
        # of a multi-GB object reads at most ``end`` bytes, not the whole file),
        # and we hard-cap the total we will ever pull via
        # ``MAX_FALLBACK_DOWNLOAD_BYTES``. If a future SDK gains a native
        # range/suffix read, swap it in here.
        data = self._download_bounded(path, limit=end)
        return data[start:end]

    def _download_bounded(self, path: str, *, limit: int) -> bytes:
        """Download at most ``limit`` bytes (capped at MAX_FALLBACK_DOWNLOAD_BYTES).

        Streams from the SDK handle so a huge object is not fully buffered; the
        caller slices the returned prefix. ``limit`` is the highest offset the
        caller needs (``end``); for a tail read that is the file size, which
        ``read_log`` bounds via the size guard before calling us.
        """
        hard_cap = min(int(limit), MAX_FALLBACK_DOWNLOAD_BYTES)
        resp = self._client.files.download(path)
        contents = getattr(resp, "contents", resp)
        if hasattr(contents, "read"):
            # Bounded read from the streaming handle — never pull the whole file.
            return contents.read(hard_cap)
        if isinstance(contents, str):
            return contents.encode("utf-8")[:hard_cap]
        return bytes(contents)[:hard_cap]

    def _download_all(self, path: str) -> bytes:
        # Only used by ``stat_size`` as a last-resort size probe when the Files
        # metadata call has no ``content_length``; still bounded by the cap.
        return self._download_bounded(path, limit=MAX_FALLBACK_DOWNLOAD_BYTES)


@dataclass
class LogRead:
    """Result of a log read. ``outcome`` is a spec §3.4 reason code."""

    outcome: str  # "OK" | "FILE_TOO_LARGE" | "FILE_NOT_FOUND"
    body: bytes  # empty on non-OK outcomes
    headers: dict  # includes Cache-Control + X-Log-* metadata


def _base_headers(mode: str, outcome: str) -> dict:
    return {
        "Cache-Control": CACHE_CONTROL_VALUE,
        HDR_MODE: mode,
        HDR_OUTCOME: outcome,
    }


def read_log(
    file_ref: str,
    *,
    reader: FileReader,
    mode: str = "tail",
    full_cap_bytes: int = DEFAULT_FULL_CAP_BYTES,
    tail_bytes: int = TAIL_BYTES,
    expected_user: str | None = None,
    max_fallback_bytes: int = MAX_FALLBACK_DOWNLOAD_BYTES,
    now: float | None = None,
) -> LogRead:
    """Verify ``file_ref`` and read the bound file in ``tail`` or ``full`` mode.

    Raises the ``filerefs`` errors (expired/tampered/escape/user-mismatch)
    unchanged so the endpoint layer can map them to 4xx. ``expected_user``
    (HIGH #3) is threaded into ``verify_file_ref`` to bind the ref to the
    reading user. A UC permission denial on the content read raises
    ``ContentForbidden`` (MEDIUM #5). Returns a ``LogRead`` for read-level
    outcomes (``OK`` / ``FILE_TOO_LARGE`` / ``FILE_NOT_FOUND``).
    """
    if mode not in ("tail", "full"):
        raise ValueError(f"unknown mode: {mode!r}")

    payload: FileRefPayload = verify_file_ref(
        file_ref, expected_user=expected_user, now=now
    )
    path = payload.path

    try:
        total = reader.stat_size(path)
    except FileNotFoundError:
        return LogRead("FILE_NOT_FOUND", b"", _base_headers(mode, "FILE_NOT_FOUND"))
    except Exception as exc:  # noqa: BLE001
        if _is_permission_denied(exc):
            raise ContentForbidden("no READ on this file") from None
        raise

    if mode == "full":
        if total > full_cap_bytes:
            headers = _base_headers(mode, "FILE_TOO_LARGE")
            headers[HDR_TOTAL_SIZE] = str(total)
            return LogRead("FILE_TOO_LARGE", b"", headers)
        start, end = 0, total
    else:  # tail
        start = max(0, total - tail_bytes)
        end = total
        # MEDIUM #7: a tail wants the SUFFIX, but the download-and-slice fallback
        # can only read a bounded PREFIX. If the file is larger than the fallback
        # cap AND the tail window doesn't start within the readable prefix, we
        # cannot serve the true tail without native range reads — return a
        # controlled FILE_TOO_LARGE rather than OOM-ing or lying about content.
        if total > max_fallback_bytes and start >= max_fallback_bytes:
            headers = _base_headers(mode, "FILE_TOO_LARGE")
            headers[HDR_TOTAL_SIZE] = str(total)
            return LogRead("FILE_TOO_LARGE", b"", headers)

    try:
        body = reader.read_range(path, start, end)
    except FileNotFoundError:
        return LogRead("FILE_NOT_FOUND", b"", _base_headers(mode, "FILE_NOT_FOUND"))
    except Exception as exc:  # noqa: BLE001
        if _is_permission_denied(exc):
            raise ContentForbidden("no READ on this file") from None
        raise

    # The file may have changed between stat and read; clamp reported range to
    # what we actually got so the metadata is never a lie.
    actual_end = start + len(body)
    earlier_hidden = start  # bytes before the returned window

    headers = _base_headers(mode, "OK")
    headers[HDR_TOTAL_SIZE] = str(total)
    headers[HDR_RANGE_START] = str(start)
    headers[HDR_RANGE_END] = str(actual_end)
    headers[HDR_EARLIER_HIDDEN] = str(earlier_hidden)

    return LogRead("OK", body, headers)
