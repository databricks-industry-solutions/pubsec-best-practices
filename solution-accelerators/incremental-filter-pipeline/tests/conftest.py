"""Shared test fixtures for incremental filter pipeline tests."""

import pytest


@pytest.fixture(scope="session")
def catalog():
    """Default catalog for testing."""
    return "main"


@pytest.fixture(scope="session")
def schema():
    """Default schema for testing."""
    return "tmp"


@pytest.fixture(scope="session")
def config_table(catalog, schema):
    """Fully qualified config table name."""
    return f"{catalog}.{schema}.incremental_filter__table_config"


@pytest.fixture(scope="session")
def control_table(catalog, schema):
    """Fully qualified control table name."""
    return f"{catalog}.{schema}.incremental_filter__sensitive_records"
