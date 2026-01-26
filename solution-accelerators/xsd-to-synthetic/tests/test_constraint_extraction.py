"""
Test constraint extraction from XSD types.

Based on test_all_constraints.py but converted to proper pytest format.
"""

import pytest
import xmlschema


class TestConstraintExtraction:
    """Test ConstraintExtractor functionality."""

    def test_pattern_extraction_from_id_types(self, efile_types_schema, notebook_classes):
        """Test that patterns are correctly extracted from ID types."""
        ConstraintExtractor = notebook_classes['ConstraintExtractor']

        test_cases = [
            ('EINType', r'[0-9]{9}'),
            ('SSNType', r'[0-9]{9}'),
        ]

        for type_name, expected_pattern in test_cases:
            # efileTypes.xsd types don't have namespace prefix in the key
            xsd_type = efile_types_schema.types.get(type_name)
            assert xsd_type is not None, f"Type {type_name} not found in schema"

            constraints = ConstraintExtractor.extract(xsd_type)

            assert constraints.pattern is not None, \
                f"{type_name} should have a pattern constraint"
            assert constraints.pattern == expected_pattern, \
                f"{type_name} pattern mismatch: expected {expected_pattern}, got {constraints.pattern}"

    def test_enumeration_extraction(self, temp_xsd, notebook_classes):
        """Test that enumerations are correctly extracted."""
        ConstraintExtractor = notebook_classes['ConstraintExtractor']

        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsd:simpleType name="StateCodeType">
        <xsd:restriction base="xsd:string">
            <xsd:enumeration value="CA"/>
            <xsd:enumeration value="NY"/>
            <xsd:enumeration value="TX"/>
        </xsd:restriction>
    </xsd:simpleType>
</xsd:schema>'''

        xsd_path = temp_xsd(xsd_content)
        schema = xmlschema.XMLSchema(xsd_path)

        xsd_type = schema.types['StateCodeType']
        constraints = ConstraintExtractor.extract(xsd_type)

        assert constraints.enumeration is not None, "Should have enumeration"
        assert set(constraints.enumeration) == {'CA', 'NY', 'TX'}, \
            "Enumeration values don't match"

    def test_min_max_extraction(self, temp_xsd, notebook_classes):
        """Test that min/max values are correctly extracted."""
        ConstraintExtractor = notebook_classes['ConstraintExtractor']

        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsd:simpleType name="LatitudeType">
        <xsd:restriction base="xsd:decimal">
            <xsd:minInclusive value="-90"/>
            <xsd:maxInclusive value="90"/>
            <xsd:totalDigits value="8"/>
            <xsd:fractionDigits value="6"/>
        </xsd:restriction>
    </xsd:simpleType>
</xsd:schema>'''

        xsd_path = temp_xsd(xsd_content)
        schema = xmlschema.XMLSchema(xsd_path)

        xsd_type = schema.types['LatitudeType']
        constraints = ConstraintExtractor.extract(xsd_type)

        # Use the correct attribute names: min_value, max_value
        assert constraints.min_value == -90, "min_value should be -90"
        assert constraints.max_value == 90, "max_value should be 90"
        assert constraints.total_digits == 8, "total_digits should be 8"
        assert constraints.fraction_digits == 6, "fraction_digits should be 6"

    def test_length_constraints(self, temp_xsd, notebook_classes):
        """Test that length constraints are correctly extracted."""
        ConstraintExtractor = notebook_classes['ConstraintExtractor']

        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsd:simpleType name="PhoneType">
        <xsd:restriction base="xsd:string">
            <xsd:minLength value="10"/>
            <xsd:maxLength value="15"/>
        </xsd:restriction>
    </xsd:simpleType>
</xsd:schema>'''

        xsd_path = temp_xsd(xsd_content)
        schema = xmlschema.XMLSchema(xsd_path)

        xsd_type = schema.types['PhoneType']
        constraints = ConstraintExtractor.extract(xsd_type)

        assert constraints.min_length == 10, "min_length should be 10"
        assert constraints.max_length == 15, "max_length should be 15"

    def test_multiple_patterns_warning(self, temp_xsd, notebook_classes):
        """Test that multiple patterns generate a warning and use first pattern."""
        ConstraintExtractor = notebook_classes['ConstraintExtractor']

        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsd:simpleType name="MultiPatternType">
        <xsd:restriction base="xsd:string">
            <xsd:pattern value="[A-Z]{3}"/>
            <xsd:pattern value="[0-9]{3}"/>
        </xsd:restriction>
    </xsd:simpleType>
</xsd:schema>'''

        xsd_path = temp_xsd(xsd_content)
        schema = xmlschema.XMLSchema(xsd_path)

        xsd_type = schema.types['MultiPatternType']

        # This should log a warning but still extract the first pattern
        constraints = ConstraintExtractor.extract(xsd_type)

        assert constraints.pattern is not None, "Should extract first pattern"
        assert constraints.pattern == '[A-Z]{3}', "Should use first pattern"

    def test_comprehensive_type_coverage(self, efile_types_schema, notebook_classes):
        """Test constraint extraction across many different types."""
        ConstraintExtractor = notebook_classes['ConstraintExtractor']

        # Test various type families that exist in efileTypes.xsd
        type_families = {
            'IDs': ['EINType', 'SSNType'],
            'Amounts': ['USAmountType', 'USAmountNNType'],
            'Text': ['PersonNameType', 'BusinessNameLine1Type'],
            'Codes': ['StateType', 'CountryType'],
        }

        stats = {'tested': 0, 'with_constraints': 0}

        for family, types in type_families.items():
            for type_name in types:
                # Types in efileTypes.xsd don't have namespace prefix in key
                xsd_type = efile_types_schema.types.get(type_name)
                if xsd_type is None:
                    continue

                stats['tested'] += 1
                constraints = ConstraintExtractor.extract(xsd_type)

                # Check if any constraint was extracted
                has_constraint = any([
                    constraints.pattern,
                    constraints.enumeration,
                    constraints.min_value is not None,
                    constraints.max_value is not None,
                    constraints.min_length is not None,
                    constraints.max_length is not None,
                ])

                if has_constraint:
                    stats['with_constraints'] += 1

        # Assert that we tested types and found constraints
        assert stats['tested'] > 0, "Should have tested some types"

        # At least 50% should have extractable constraints
        coverage = stats['with_constraints'] / stats['tested']
        assert coverage >= 0.5, \
            f"Low constraint coverage: {stats['with_constraints']}/{stats['tested']} = {coverage:.1%}"
