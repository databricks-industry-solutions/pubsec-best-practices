"""XSDLoader include-resolution tests.

No Spark required — these exercise schema loading only.

Covers the three things the resolution rewrite is responsible for:
  1. native resolution works with NO configuration when the package layout is intact
  2. filename-keyed overrides rescue a RELOCATED schema, whose relative include path
     no longer matches anything on disk (the `Common/`-rooted pattern list could not)
  3. the include report makes silent degradation visible — xmlschema only WARNS on a
     failed include, so a schema can look healthy while missing most of its types
"""

import os
import shutil
import tempfile

import pytest

# Reuse the notebook-class loader that conftest.py provides.
pytestmark = pytest.mark.usefixtures()

MEF_ENV = "LOCAL_XSD_PATH"


def _mef_root():
    root = os.environ.get(MEF_ENV)
    if not root or not os.path.isdir(root):
        pytest.skip(f"set {MEF_ENV} to an IRS MeF schema package to run these")
    return root


def _paths(root):
    form = os.path.join(root, "TEGE", "TEGE990T", "IRS990T", "IRS990T.xsd")
    types = os.path.join(root, "Common", "efileTypes.xsd")
    if not (os.path.exists(form) and os.path.exists(types)):
        pytest.skip("IRS990T.xsd / efileTypes.xsd not found under the schema root")
    return form, types


@pytest.fixture
def loader(notebook_classes):
    XSDLoader = notebook_classes.get("XSDLoader")
    if XSDLoader is None:
        pytest.skip("XSDLoader not available in notebook_classes")
    return XSDLoader()


def test_native_resolution_needs_no_configuration(loader):
    """An in-place package resolves its own relative includes with no args."""
    form, _ = _paths(_mef_root())
    schema = loader.load(form)
    report = loader.get_include_report()

    assert report["unresolved"] == [], f"unresolved includes: {report['unresolved']}"
    assert report["resolved"] == report["declared"] >= 1
    # efileTypes.xsd carries the shared MeF simple types; without it we'd see a
    # handful of types instead of well over a hundred.
    assert "USAmountType" in schema.types
    assert len(schema.types) > 100


def test_include_paths_still_supported(loader):
    """The original API keeps working unchanged."""
    form, types = _paths(_mef_root())
    schema = loader.load(form, include_paths=[types])

    assert loader.get_include_report()["unresolved"] == []
    assert "USAmountType" in schema.types


def test_relocated_schema_reports_unresolved_includes(loader):
    """A moved schema cannot resolve its include — and must SAY so, not fail silently.

    This is the dangerous case: xmlschema emits only a warning, so without the report
    a caller sees a schema object and assumes validation is in force when the shared
    types (and therefore their facets) are missing.
    """
    form, types = _paths(_mef_root())
    tmp = tempfile.mkdtemp()
    try:
        shutil.copy(form, os.path.join(tmp, "IRS990T.xsd"))
        loader.load(os.path.join(tmp, "IRS990T.xsd"))
        report = loader.get_include_report()

        assert report["unresolved"], "a relocated schema should report unresolved includes"
        assert report["resolved"] < report["declared"]
        assert report["global_types"] < 100, "shared types should be absent here"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_filename_key_resolves_a_non_common_layout(loader):
    """Overrides are keyed by FILENAME, so any relative shape redirects.

    The include reads '../../../Common/efileTypes.xsd', but here the file lives in
    'elsewhere/'. A pattern list of Common/-rooted shapes cannot match that; a
    filename index can.
    """
    form, types = _paths(_mef_root())
    tmp = tempfile.mkdtemp()
    try:
        shutil.copy(form, os.path.join(tmp, "IRS990T.xsd"))
        moved = os.path.join(tmp, "elsewhere")
        os.makedirs(moved, exist_ok=True)
        shutil.copy(types, os.path.join(moved, "efileTypes.xsd"))

        schema = loader.load(
            os.path.join(tmp, "IRS990T.xsd"),
            include_paths=[os.path.join(moved, "efileTypes.xsd")],
        )
        assert loader.get_include_report()["unresolved"] == []
        assert "USAmountType" in schema.types
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_include_dirs_scans_a_package_by_filename(loader):
    """include_dirs indexes a whole tree, so callers needn't enumerate files.

    Also guards the self-reference hazard: the walk must not map the MAIN schema
    onto one of its own include targets, which sends xmlschema into an unbounded
    include cycle (it hangs in C-level parsing, uninterruptible by a signal).
    """
    form, types = _paths(_mef_root())
    tmp = tempfile.mkdtemp()
    try:
        shutil.copy(form, os.path.join(tmp, "IRS990T.xsd"))
        moved = os.path.join(tmp, "elsewhere")
        os.makedirs(moved, exist_ok=True)
        shutil.copy(types, os.path.join(moved, "efileTypes.xsd"))

        schema = loader.load(os.path.join(tmp, "IRS990T.xsd"), include_dirs=[tmp])
        assert loader.get_include_report()["unresolved"] == []
        assert "USAmountType" in schema.types
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
