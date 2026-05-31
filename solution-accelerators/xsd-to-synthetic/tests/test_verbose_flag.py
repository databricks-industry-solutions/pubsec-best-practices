"""
Regression test for the verbose flag in xsd_to_synthetic().

Previously, calling xsd_to_synthetic(..., verbose=False) raised
UnboundLocalError because `info = loader.get_schema_info(schema)` was assigned
only inside an `if verbose:` block, but the value was then referenced
unconditionally by a `_logger.info(...)` call. This test guards both the
verbose=False and verbose=True paths.
"""
import pytest


SIMPLE_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <xsd:element name="Person">
    <xsd:complexType>
      <xsd:sequence>
        <xsd:element name="RecordId" type="xsd:string"/>
        <xsd:element name="PersonAge" type="xsd:integer"/>
      </xsd:sequence>
    </xsd:complexType>
  </xsd:element>
</xsd:schema>
"""


@pytest.mark.parametrize("verbose", [False, True])
def test_xsd_to_synthetic_respects_verbose_flag(spark, temp_xsd, notebook_classes, verbose):
    """xsd_to_synthetic must work for both verbose values (regression: verbose=False
    used to raise UnboundLocalError on the unconditional _logger.info(info[...])."""
    xsd_to_synthetic = notebook_classes.get("xsd_to_synthetic")
    if xsd_to_synthetic is None:
        pytest.skip("xsd_to_synthetic not available in notebook_classes")

    xsd_path = temp_xsd(SIMPLE_XSD)

    # The key assertion: this call must not raise (verbose=False was the broken path).
    df = xsd_to_synthetic(
        spark,
        xsd_path=xsd_path,
        root_element="Person",
        num_rows=5,
        verbose=verbose,
    )

    assert df is not None
    assert df.count() == 5
    assert "RecordId" in df.columns and "PersonAge" in df.columns
