"""App service-principal WorkspaceClient (spec §2, §3, §4).

The **app service principal (SP)** is a SEPARATE identity from the viewing
user. It authenticates via the default M2M OAuth env the Databricks Apps
runtime injects (``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET`` /
``DATABRICKS_HOST``). Unlike the user's OBO token, the SP is NOT limited by the
OBO scope catalog, so it *can* call the Jobs and Clusters APIs on GovCloud
(confirmed live during testing).

Identity discipline (spec §2, §3.3) — CRITICAL:
  - The SP client is used ONLY for Jobs/Clusters METADATA (run -> task ->
    cluster -> CLD root). It MUST NEVER be used to list or read log FILE
    content — that is the user OBO client's job, and it is what enforces
    per-user access via Unity Catalog.

Because the SP is the app's own identity (not per-user), the client may be
cached module-level. ``build_sp_client`` provides a factory seam so tests can
inject a mock SP without importing the real SDK or hitting a workspace.
"""

from __future__ import annotations

from typing import Callable, Optional

# Module-level cache: the SP is the app identity, safe to reuse across requests
# (unlike the user OBO client, which must be per-request).
_cached_client = None


def build_sp_client(client_factory: Optional[Callable[[], object]] = None):
    """Return the app service-principal ``WorkspaceClient`` (cached).

    Parameters
    ----------
    client_factory : callable, optional
        Zero-arg factory returning a client. Defaults to the real
        ``databricks.sdk.WorkspaceClient()`` with default env (M2M OAuth) auth.
        Injected in tests so no real SDK/workspace is contacted.

    Notes
    -----
    Built with NO explicit ``token``/``auth_type`` so the SDK uses the ambient
    app-SP OAuth env vars (the opposite of ``auth.build_user_client``, which
    pins ``auth_type="pat"`` to the forwarded user token). This is the concrete
    embodiment of the two-identity split.
    """
    global _cached_client
    if _cached_client is not None and client_factory is None:
        return _cached_client

    if client_factory is None:
        # Lazy import so unit tests need not have the SDK installed and module
        # import can never establish a connection.
        from databricks.sdk import WorkspaceClient  # noqa: WPS433

        client_factory = WorkspaceClient

    client = client_factory()

    # Only cache the real default-auth client; an injected test factory is not
    # cached so tests stay isolated from one another.
    if client_factory is not None and _cached_client is None:
        # Detect the "real" path: the default factory is WorkspaceClient itself.
        try:
            from databricks.sdk import WorkspaceClient  # noqa: WPS433

            if client_factory is WorkspaceClient:
                _cached_client = client
        except Exception:  # noqa: BLE001 - SDK not installed in tests; skip cache
            pass

    return client


def reset_cache() -> None:
    """Clear the module-level cache. Test helper; not used at runtime."""
    global _cached_client
    _cached_client = None
