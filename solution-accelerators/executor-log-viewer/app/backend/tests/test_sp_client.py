"""sp_client tests — app service-principal client build + caching.

No real SDK / workspace is contacted; a factory is injected.
"""

import sp_client


def teardown_function():
    sp_client.reset_cache()


def test_build_with_injected_factory_not_cached():
    calls = []

    def factory():
        calls.append(1)
        return object()

    c1 = sp_client.build_sp_client(client_factory=factory)
    c2 = sp_client.build_sp_client(client_factory=factory)
    assert c1 is not None and c2 is not None
    # An injected (test) factory is never cached — each call builds fresh.
    assert len(calls) == 2


def test_reset_cache_is_safe_when_empty():
    sp_client.reset_cache()  # no error


def test_factory_receives_no_token_or_auth_type():
    """The SP uses ambient M2M OAuth — the opposite of the user client which
    pins auth_type='pat'. Our factory is zero-arg, proving we pass nothing that
    would force PAT/user auth."""
    seen = {}

    def factory():
        seen["called"] = True
        return object()

    sp_client.build_sp_client(client_factory=factory)
    assert seen["called"] is True
