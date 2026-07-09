import pytest

from paths import is_under_root, join_under_root, normalize_path

ROOT = "/Volumes/cat/sch/vol/cld"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("/Volumes/cat/sch/vol/cld/", "/Volumes/cat/sch/vol/cld"),
        ("/Volumes//cat///sch/vol/cld", "/Volumes/cat/sch/vol/cld"),
        ("/Volumes/cat/sch/vol/cld/./x", "/Volumes/cat/sch/vol/cld/x"),
        ("Volumes/cat/sch/vol/cld", "/Volumes/cat/sch/vol/cld"),  # forced absolute
        ("/Volumes/cat/sch/vol\\cld", "/Volumes/cat/sch/vol/cld"),  # backslashes
        ("/Volumes/cat/sch/vol/cld/a%20b", "/Volumes/cat/sch/vol/cld/a b"),  # url space
        # percent-encoded traversal is decoded then collapsed
        ("/Volumes/cat/sch/vol/cld/%2e%2e/x", "/Volumes/cat/sch/vol/x"),
    ],
)
def test_normalize_path(raw, expected):
    assert normalize_path(raw) == expected


def test_is_under_root_true_cases():
    assert is_under_root(ROOT, ROOT)  # equal counts as under
    assert is_under_root(ROOT + "/0710-abc/executor/app-1/1/stderr", ROOT)
    assert is_under_root(ROOT + "//0710-abc//stdout", ROOT)  # dup slashes


def test_is_under_root_rejects_sibling_prefix():
    # /cld-evil must NOT be considered under /cld (segment boundary check).
    assert not is_under_root("/Volumes/cat/sch/vol/cld-evil/x", ROOT)


def test_is_under_root_rejects_traversal_escape():
    escape = ROOT + "/../../../../etc/passwd"
    assert not is_under_root(escape, ROOT)


def test_is_under_root_rejects_encoded_traversal_escape():
    escape = ROOT + "/%2e%2e/%2e%2e/%2e%2e/secret"
    assert not is_under_root(escape, ROOT)


def test_join_under_root_ok():
    p = join_under_root(ROOT, "0710-abc", "executor", "app-1", "1", "stderr")
    assert p == ROOT + "/0710-abc/executor/app-1/1/stderr"


def test_join_under_root_rejects_escape():
    with pytest.raises(ValueError):
        join_under_root(ROOT, "..", "..", "..", "etc", "passwd")
