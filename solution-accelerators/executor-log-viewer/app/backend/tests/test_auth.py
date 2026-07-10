import logging

import pytest

import auth
from auth import AuthError, build_user_client, has_forwarded_token

TOKEN = "super-secret-user-token-DO-NOT-LEAK"
HEADER = auth.FORWARDED_TOKEN_HEADER


class DictHeaders(dict):
    """Plain dict headers (case-sensitive) to exercise the dict fallback."""


class CaseInsensitiveHeaders:
    """Mimics Starlette Headers.get (case-insensitive)."""

    def __init__(self, data):
        self._data = {k.lower(): v for k, v in data.items()}

    def get(self, key, default=None):
        return self._data.get(key.lower(), default)


def _capturing_factory(sink):
    def factory(**kwargs):
        sink.update(kwargs)  # captures host, token, auth_type, ...
        return object()  # stand-in client

    return factory


def test_has_forwarded_token_true():
    assert has_forwarded_token(CaseInsensitiveHeaders({HEADER: TOKEN})) is True


def test_has_forwarded_token_false_when_missing():
    assert has_forwarded_token(CaseInsensitiveHeaders({})) is False
    assert has_forwarded_token(DictHeaders({})) is False
    assert has_forwarded_token(None) is False


def test_has_forwarded_token_empty_is_false():
    assert has_forwarded_token(CaseInsensitiveHeaders({HEADER: ""})) is False


def test_build_client_from_header(monkeypatch):
    monkeypatch.setenv(auth.HOST_ENV, "https://example.cloud.databricks.com")
    sink = {}
    client = build_user_client(
        CaseInsensitiveHeaders({HEADER: TOKEN}),
        client_factory=_capturing_factory(sink),
    )
    assert client is not None
    assert sink["token"] == TOKEN
    assert sink["host"] == "https://example.cloud.databricks.com"
    # OBO fix: must pin PAT auth so ambient app OAuth env vars don't conflict.
    assert sink["auth_type"] == "pat"


def test_build_client_dict_headers_case_insensitive():
    sink = {}
    build_user_client(
        DictHeaders({"X-Forwarded-Access-Token": TOKEN}),
        host="h",
        client_factory=_capturing_factory(sink),
    )
    assert sink["token"] == TOKEN


def test_missing_header_raises_401():
    with pytest.raises(AuthError) as exc:
        build_user_client(CaseInsensitiveHeaders({}), client_factory=lambda **k: None)
    assert exc.value.status_code == 401
    # message must not contain any token
    assert TOKEN not in str(exc.value)


def test_token_never_in_error_when_factory_raises():
    def boom(*, host, token):
        raise RuntimeError(f"connect failed with token={token}")  # SDK might leak

    with pytest.raises(AuthError) as exc:
        build_user_client(
            CaseInsensitiveHeaders({HEADER: TOKEN}),
            host="h",
            client_factory=boom,
        )
    # Our re-wrap must drop the underlying message so the token can't escape.
    assert TOKEN not in str(exc.value)
    assert exc.value.__cause__ is None  # raised `from None`


def test_token_never_logged(caplog):
    sink = {}
    with caplog.at_level(logging.DEBUG):
        build_user_client(
            CaseInsensitiveHeaders({HEADER: TOKEN}),
            host="h",
            client_factory=_capturing_factory(sink),
        )
    assert TOKEN not in caplog.text
