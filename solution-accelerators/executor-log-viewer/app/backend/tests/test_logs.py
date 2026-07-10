import pytest

import logs
from filerefs import mint_file_ref
from logs import (
    CACHE_CONTROL_VALUE,
    DEFAULT_FULL_CAP_BYTES,
    HDR_EARLIER_HIDDEN,
    HDR_RANGE_END,
    HDR_RANGE_START,
    HDR_TOTAL_SIZE,
    TAIL_BYTES,
    read_log,
)

ROOT = "/Volumes/cat/sch/vol/cld"
PATH = ROOT + "/0710-abc/executor/app-1/1/stderr"

REF_KW = dict(
    run_id="r",
    cluster_id="0710-abc",
    file_kind="stderr",
    user="dev@example.com",
    root=ROOT,
)


def _ref():
    return mint_file_ref(path=PATH, **REF_KW)


def test_small_file_tail(make_reader):
    data = b"hello world\n" * 3
    reader = make_reader({PATH: data})
    res = read_log(_ref(), reader=reader, mode="tail")

    assert res.outcome == "OK"
    assert res.body == data  # whole file fits in the tail window
    assert res.headers["Cache-Control"] == CACHE_CONTROL_VALUE
    assert res.headers[HDR_TOTAL_SIZE] == str(len(data))
    assert res.headers[HDR_RANGE_START] == "0"
    assert res.headers[HDR_RANGE_END] == str(len(data))
    assert res.headers[HDR_EARLIER_HIDDEN] == "0"


def test_exactly_256kb_tail(make_reader):
    data = b"a" * TAIL_BYTES  # exactly the tail window
    reader = make_reader({PATH: data})
    res = read_log(_ref(), reader=reader, mode="tail")

    assert res.outcome == "OK"
    assert len(res.body) == TAIL_BYTES
    assert res.headers[HDR_RANGE_START] == "0"
    assert res.headers[HDR_EARLIER_HIDDEN] == "0"  # nothing hidden at exactly 256K
    assert res.headers[HDR_RANGE_END] == str(TAIL_BYTES)


def test_large_file_tail_truncation_and_hidden_math(make_reader):
    total = TAIL_BYTES + 1000  # 1000 bytes beyond the window
    data = b"x" * total
    reader = make_reader({PATH: data})
    res = read_log(_ref(), reader=reader, mode="tail")

    assert res.outcome == "OK"
    assert len(res.body) == TAIL_BYTES  # only the last 256 KB
    assert res.headers[HDR_TOTAL_SIZE] == str(total)
    assert res.headers[HDR_RANGE_START] == "1000"  # start = total - 256K
    assert res.headers[HDR_RANGE_END] == str(total)
    assert res.headers[HDR_EARLIER_HIDDEN] == "1000"  # earlier bytes hidden


def test_empty_file_tail(make_reader):
    reader = make_reader({PATH: b""})
    res = read_log(_ref(), reader=reader, mode="tail")

    assert res.outcome == "OK"
    assert res.body == b""
    assert res.headers[HDR_TOTAL_SIZE] == "0"
    assert res.headers[HDR_RANGE_START] == "0"
    assert res.headers[HDR_RANGE_END] == "0"
    assert res.headers[HDR_EARLIER_HIDDEN] == "0"


def test_full_mode_under_cap(make_reader):
    data = b"line\n" * 100
    reader = make_reader({PATH: data})
    res = read_log(_ref(), reader=reader, mode="full")

    assert res.outcome == "OK"
    assert res.body == data
    assert res.headers[HDR_RANGE_START] == "0"
    assert res.headers[HDR_RANGE_END] == str(len(data))
    assert res.headers[HDR_EARLIER_HIDDEN] == "0"


def test_full_mode_over_cap_returns_file_too_large(make_reader):
    reader = make_reader({PATH: b"z" * (DEFAULT_FULL_CAP_BYTES + 1)})
    res = read_log(_ref(), reader=reader, mode="full")

    assert res.outcome == "FILE_TOO_LARGE"
    assert res.body == b""  # no bytes returned over cap
    assert res.headers["Cache-Control"] == CACHE_CONTROL_VALUE
    assert res.headers[HDR_TOTAL_SIZE] == str(DEFAULT_FULL_CAP_BYTES + 1)


def test_full_mode_exactly_at_cap_is_ok(make_reader):
    reader = make_reader({PATH: b"z" * DEFAULT_FULL_CAP_BYTES})
    res = read_log(_ref(), reader=reader, mode="full")
    assert res.outcome == "OK"
    assert len(res.body) == DEFAULT_FULL_CAP_BYTES


def test_file_not_found(make_reader):
    reader = make_reader({})  # PATH absent
    res = read_log(_ref(), reader=reader, mode="tail")
    assert res.outcome == "FILE_NOT_FOUND"
    assert res.body == b""
    assert res.headers["Cache-Control"] == CACHE_CONTROL_VALUE


def test_changed_between_stat_and_read_clamps_range(make_reader):
    # stat reports N bytes, but the read returns fewer (file rotated/truncated).
    class ShrinkingReader:
        def stat_size(self, path):
            return 5000

        def read_range(self, path, start, end):
            return b"short"  # only 5 bytes actually returned

    res = read_log(_ref(), reader=ShrinkingReader(), mode="tail")
    assert res.outcome == "OK"
    assert res.body == b"short"
    # range-end reflects what we actually got (start + len), not the stale stat.
    assert res.headers[HDR_RANGE_END] == str(0 + len(b"short"))


def test_invalid_mode_raises(make_reader):
    reader = make_reader({PATH: b"x"})
    with pytest.raises(ValueError):
        read_log(_ref(), reader=reader, mode="sideways")


def test_cache_control_no_store_on_every_outcome(make_reader):
    for data, mode in [(b"ok", "tail"), (b"z" * (DEFAULT_FULL_CAP_BYTES + 1), "full")]:
        reader = make_reader({PATH: data})
        res = read_log(_ref(), reader=reader, mode=mode)
        assert res.headers["Cache-Control"] == "no-store"


# --------------------------------------------------------------------------- #
# HIGH #3 — read_log threads expected_user into verify                         #
# --------------------------------------------------------------------------- #
def test_read_log_expected_user_match(make_reader):
    reader = make_reader({PATH: b"ok"})
    res = read_log(_ref(), reader=reader, mode="tail", expected_user="dev@example.com")
    assert res.outcome == "OK"


def test_read_log_expected_user_mismatch_raises(make_reader):
    import filerefs
    reader = make_reader({PATH: b"ok"})
    with pytest.raises(filerefs.FileRefUserMismatch):
        read_log(_ref(), reader=reader, mode="tail", expected_user="someone-else")


# --------------------------------------------------------------------------- #
# MEDIUM #5 — a UC PermissionDenied on stat/read -> ContentForbidden           #
# --------------------------------------------------------------------------- #
def test_permission_denied_on_stat_maps_to_content_forbidden():
    class DeniedOnStat:
        def stat_size(self, path):
            class PermissionDenied(Exception):
                pass
            raise PermissionDenied(f"no READ on {path}")

        def read_range(self, path, start, end):
            raise AssertionError("must not read after denied stat")

    with pytest.raises(logs.ContentForbidden):
        read_log(_ref(), reader=DeniedOnStat(), mode="tail")


def test_permission_denied_on_read_maps_to_content_forbidden():
    class DeniedOnRead:
        def stat_size(self, path):
            return 100

        def read_range(self, path, start, end):
            class Forbidden(Exception):
                pass
            raise Forbidden(f"no READ on {path}")

    with pytest.raises(logs.ContentForbidden):
        read_log(_ref(), reader=DeniedOnRead(), mode="tail")


def test_content_forbidden_message_does_not_leak_path():
    class Denied:
        def stat_size(self, path):
            class PermissionDenied(Exception):
                pass
            raise PermissionDenied(f"denied on {path}")

        def read_range(self, path, start, end):
            raise AssertionError

    try:
        read_log(_ref(), reader=Denied(), mode="tail")
    except logs.ContentForbidden as exc:
        assert PATH not in str(exc)
        assert "/Volumes/" not in str(exc)


# --------------------------------------------------------------------------- #
# MEDIUM #7 — tail download is bounded / oversized tail is a controlled error  #
# --------------------------------------------------------------------------- #
def test_tail_on_oversized_file_returns_file_too_large():
    """A tail whose window starts beyond the fallback cap can't be served by the
    prefix-only download fallback -> controlled FILE_TOO_LARGE, not OOM."""
    cap = 1024  # tiny cap for the test
    huge = 10 * cap  # 10x the cap

    class HugeStatReader:
        def stat_size(self, path):
            return huge

        def read_range(self, path, start, end):
            raise AssertionError("must not attempt to read an oversized tail")

    res = read_log(
        _ref(), reader=HugeStatReader(), mode="tail",
        tail_bytes=256, max_fallback_bytes=cap,
    )
    assert res.outcome == "FILE_TOO_LARGE"
    assert res.body == b""
    assert res.headers[HDR_TOTAL_SIZE] == str(huge)


def test_tail_within_cap_still_served(make_reader):
    """A small file's tail is unaffected by the cap (correctness preserved)."""
    data = b"small file\n"
    reader = make_reader({PATH: data})
    res = read_log(
        _ref(), reader=reader, mode="tail", max_fallback_bytes=64 * 1024 * 1024
    )
    assert res.outcome == "OK"
    assert res.body == data


def test_sdk_reader_download_is_bounded():
    """SdkFileReader.read_range never buffers more than the requested end / cap:
    it calls .read(hard_cap) on the streaming handle, not an unbounded read."""

    class Handle:
        def __init__(self, size):
            self._data = b"y" * size
            self.read_calls = []

        def read(self, n=-1):
            self.read_calls.append(n)
            if n is None or n < 0:
                return self._data
            return self._data[:n]

    class FakeFiles:
        def __init__(self, handle):
            self._handle = handle

        def download(self, path):
            class Resp:
                pass
            r = Resp()
            r.contents = self._handle
            return r

    class FakeClient:
        def __init__(self, handle):
            self.files = FakeFiles(handle)

    handle = Handle(size=1000)
    reader = logs.SdkFileReader(FakeClient(handle))
    out = reader.read_range(PATH, 0, 100)
    assert out == b"y" * 100
    # A BOUNDED read was issued (not read(-1)/read(None)).
    assert handle.read_calls and all(
        c is not None and c >= 0 for c in handle.read_calls
    )
