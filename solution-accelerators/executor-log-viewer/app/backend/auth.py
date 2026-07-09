"""Per-request on-behalf-of-user auth (spec §4).

Databricks Apps inject the viewing user's token as the
``x-forwarded-access-token`` request header. We build a FRESH
``WorkspaceClient`` bound to that identity on every request and NEVER cache the
token or the client (tokens expire; caching would also leak identity across
users).

Hard rules (spec §3.1, §6):
  - Never log the token value.
  - Never put the token in an exception message.
  - Missing header -> 401-style AuthError (the UI shows "reload the app").
"""

from __future__ import annotations

import os

FORWARDED_TOKEN_HEADER = "x-forwarded-access-token"
HOST_ENV = "DATABRICKS_HOST"


class AuthError(Exception):
    """Raised when the request cannot be authenticated. HTTP status 401.

    Messages are safe to surface to the client and NEVER contain the token.
    """

    status_code = 401


def has_forwarded_token(headers) -> bool:
    """True iff the forwarded-access-token header is present and non-empty.

    ``headers`` is any case-insensitive mapping (e.g. Starlette ``Headers``) or
    a plain dict. We only report presence — never the value.
    """
    token = _extract_token(headers)
    return bool(token)


def _extract_token(headers) -> str | None:
    # Starlette Headers are case-insensitive; plain dicts are not, so try both.
    if headers is None:
        return None
    token = None
    try:
        token = headers.get(FORWARDED_TOKEN_HEADER)
    except AttributeError:
        token = None
    if token is None and isinstance(headers, dict):
        for k, v in headers.items():
            if k.lower() == FORWARDED_TOKEN_HEADER:
                token = v
                break
    return token or None


def build_user_client(headers, *, host: str | None = None, client_factory=None):
    """Build a fresh WorkspaceClient bound to the forwarded user identity.

    Parameters
    ----------
    headers : mapping
        Request headers (case-insensitive mapping or dict).
    host : str, optional
        Workspace host. Defaults to the ``DATABRICKS_HOST`` env var. In a
        deployed Databricks App the host is available from the environment.
    client_factory : callable, optional
        Factory ``(host=..., token=...) -> client``. Defaults to the real
        ``databricks.sdk.WorkspaceClient``. Injected in tests so we never import
        or hit a real workspace.

    Raises
    ------
    AuthError
        If the forwarded-access-token header is missing/empty.
    """
    token = _extract_token(headers)
    if not token:
        # NB: do not include header contents in the message.
        raise AuthError("missing x-forwarded-access-token; reload the app")

    resolved_host = host or os.environ.get(HOST_ENV)

    if client_factory is None:
        # Imported lazily so unit tests need not have the SDK installed / the
        # module import cannot accidentally establish a connection.
        from databricks.sdk import WorkspaceClient  # noqa: WPS433

        client_factory = WorkspaceClient

    try:
        # Force PAT-only auth. In the Databricks Apps runtime the platform also
        # injects the app service principal's OAuth env vars
        # (DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET); without pinning the
        # auth type the SDK sees BOTH the ambient OAuth creds and the forwarded
        # user PAT and refuses ("more than one authorization method configured").
        # auth_type="pat" makes it use exactly the forwarded user token (OBO).
        return client_factory(host=resolved_host, token=token, auth_type="pat")
    except Exception as exc:  # noqa: BLE001 - re-wrap so token can't leak
        # Surface a SANITIZED diagnostic: keep the exception type + message but
        # scrub the token value (and any long bearer-ish substrings) so it can
        # never leak. Also report whether the host resolved.
        msg = _scrub(str(exc), token)
        err = AuthError(
            f"failed to build workspace client "
            f"(host_set={bool(resolved_host)}, err={type(exc).__name__}: {msg})"
        )
        raise err from None


def _scrub(text: str, token: str | None) -> str:
    """Remove the token and any long token-like substrings from a message."""
    import re

    out = text
    if token:
        out = out.replace(token, "<redacted>")
    # redact any long opaque run of token-ish chars (e.g. dapi..., JWT segments)
    out = re.sub(r"[A-Za-z0-9._\-]{24,}", "<redacted>", out)
    return out[:300]
