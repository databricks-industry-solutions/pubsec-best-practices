"""
Tests for pattern-generation fidelity in generate_pattern_samples().

rstr.xeger can over-generate for XSD patterns that combine alternation with
quantifiers. The real IRS ZIP type is "[0-9]{5}|[0-9]{9}" (5-digit ZIP or
9-digit ZIP+4); unanchored xeger sometimes emitted longer runs (e.g. a
12-digit string). generate_pattern_samples now anchors the pattern and
validates every sample with re.fullmatch, resampling invalid ones, so output
always satisfies the original pattern.
"""
import re
import pytest


@pytest.fixture
def gps(notebook_classes):
    fn = notebook_classes.get("generate_pattern_samples")
    if fn is None:
        pytest.skip("generate_pattern_samples not available in notebook_classes")
    return fn


@pytest.mark.parametrize("pattern", [
    r"[0-9]{5}|[0-9]{9}",      # ZIP or ZIP+4 (the original offender)
    r"[0-9]{9}",               # EIN/SSN
    r"P[0-9]{8}",              # PTIN
    r"[A-Z]{2}|[A-Z]{3}",      # alternation with quantifiers
    r"[0-9]{5}(-[0-9]{4})?",   # optional group
])
def test_all_samples_match_pattern(gps, pattern):
    samples = gps(pattern, count=50)
    assert samples, f"no samples generated for {pattern!r}"
    invalid = [s for s in samples if not re.fullmatch(pattern, s)]
    assert not invalid, f"{len(invalid)} samples violate {pattern!r}: {invalid[:5]}"


def test_zip_lengths_are_only_5_or_9(gps):
    """Regression: ZIP must never produce 12-digit (or other-length) strings."""
    samples = gps(r"[0-9]{5}|[0-9]{9}", count=80)
    lengths = {len(s) for s in samples}
    assert lengths <= {5, 9}, f"unexpected ZIP lengths: {sorted(lengths)}"


def test_invalid_pattern_returns_empty(gps):
    """A malformed regex should return [] (caller falls back), not raise."""
    assert gps(r"[0-9", count=10) == []


def test_empty_pattern_returns_empty(gps):
    assert gps("", count=10) == []
