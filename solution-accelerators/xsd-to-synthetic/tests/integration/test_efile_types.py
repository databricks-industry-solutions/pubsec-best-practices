"""
Comprehensive type mapping validation for all efileTypes.xsd types.

This module tests that the TypeMapper correctly maps XSD types to Spark types,
and that the ConstraintExtractor correctly extracts facet constraints.
"""
import pytest
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DecimalType,
    BooleanType, DateType, TimestampType, ArrayType, DataType
)
from decimal import Decimal

from tests.fixtures.expected_type_mappings import (
    EXPECTED_TYPE_MAPPINGS,
    TYPE_CATEGORIES,
    CRITICAL_AMOUNT_TYPES,
    CRITICAL_DATE_TYPES,
    CRITICAL_BOOLEAN_TYPES,
)


def _get_type_from_schema(schema, type_name):
    """Get a type from schema, trying with and without namespace prefix."""
    # Try without namespace first (simpler schemas)
    xsd_type = schema.types.get(type_name)
    if xsd_type is not None:
        return xsd_type

    # Try with namespace prefix
    ns = '{http://www.irs.gov/efile}'
    xsd_type = schema.types.get(ns + type_name)
    return xsd_type


def _get_all_type_names(schema):
    """Get all type names from schema, stripping namespace if present."""
    ns = '{http://www.irs.gov/efile}'
    names = []
    for key in schema.types.keys():
        if isinstance(key, str):
            if key.startswith(ns):
                names.append(key.replace(ns, ''))
            else:
                names.append(key)
    return names


class TestEfileTypesMapping:
    """Validate type mapping for types in efileTypes.xsd."""

    # =========================================================================
    # CRITICAL TYPE TESTS - These must pass!
    # =========================================================================

    @pytest.mark.parametrize("type_name", CRITICAL_AMOUNT_TYPES)
    def test_critical_amount_types_not_string(
        self, efile_types_schema, notebook_classes, type_name
    ):
        """Test that amount types are NOT mapped to StringType."""
        TypeMapper = notebook_classes['TypeMapper']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        spark_type = TypeMapper.get_spark_type(xsd_type)

        assert not isinstance(spark_type, StringType), (
            f"CRITICAL: {type_name} mapped to StringType! "
            f"Amount types must map to numeric types (LongType or DecimalType)."
        )

    @pytest.mark.parametrize("type_name", CRITICAL_DATE_TYPES)
    def test_critical_date_types_not_string(
        self, efile_types_schema, notebook_classes, type_name
    ):
        """Test that date types are NOT mapped to StringType."""
        TypeMapper = notebook_classes['TypeMapper']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        spark_type = TypeMapper.get_spark_type(xsd_type)

        assert not isinstance(spark_type, StringType), (
            f"CRITICAL: {type_name} mapped to StringType! "
            f"Date types should map to DateType or TimestampType."
        )

    @pytest.mark.parametrize("type_name", CRITICAL_BOOLEAN_TYPES)
    def test_critical_boolean_types(
        self, efile_types_schema, notebook_classes, type_name
    ):
        """Test that boolean types map correctly."""
        TypeMapper = notebook_classes['TypeMapper']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        spark_type = TypeMapper.get_spark_type(xsd_type)

        assert isinstance(spark_type, BooleanType), (
            f"CRITICAL: {type_name} should map to BooleanType, got {type(spark_type).__name__}"
        )

    # =========================================================================
    # SPARK TYPE MAPPING TESTS
    # =========================================================================

    @pytest.mark.parametrize("type_name,expected", EXPECTED_TYPE_MAPPINGS.items())
    def test_spark_type_mapping(
        self, efile_types_schema, notebook_classes, type_name, expected
    ):
        """Test that each XSD type maps to expected Spark type class."""
        TypeMapper = notebook_classes['TypeMapper']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        spark_type = TypeMapper.get_spark_type(xsd_type)
        expected_spark = expected['spark_type']

        # Compare type classes
        assert type(spark_type) == type(expected_spark), (
            f"{type_name}: Expected {type(expected_spark).__name__}, "
            f"got {type(spark_type).__name__}"
        )

    @pytest.mark.parametrize("type_name,expected", [
        (name, exp) for name, exp in EXPECTED_TYPE_MAPPINGS.items()
        if isinstance(exp['spark_type'], DecimalType)
    ])
    def test_decimal_precision_and_scale(
        self, efile_types_schema, notebook_classes, type_name, expected
    ):
        """Test DecimalType precision and scale are correct after constraint extraction.
        
        Note: TypeMapper returns DecimalType(38,18) for all decimal types.
        Precision refinement happens in ConstraintExtractor._refine_spark_type().
        """
        TypeMapper = notebook_classes['TypeMapper']
        ConstraintExtractor = notebook_classes['ConstraintExtractor']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        # Use ConstraintExtractor which refines precision based on XSD facets
        constraints = ConstraintExtractor.extract(xsd_type, TypeMapper)
        spark_type = constraints.spark_type
        
        # Verify it's a DecimalType
        assert isinstance(spark_type, DecimalType), (
            f"{type_name}: Expected DecimalType, got {type(spark_type).__name__}"
        )
        
        # Verify precision/scale come from XSD facets (totalDigits, fractionDigits)
        expected_constraints = expected.get('constraints', {})
        if expected_constraints.get('total_digits'):
            expected_precision = min(expected_constraints['total_digits'], 38)
            expected_scale = min(expected_constraints.get('fraction_digits', 0), expected_precision)
            assert spark_type.precision == expected_precision, (
                f"{type_name}: Expected precision {expected_precision} from XSD totalDigits, "
                f"got {spark_type.precision}"
            )
            assert spark_type.scale == expected_scale, (
                f"{type_name}: Expected scale {expected_scale} from XSD fractionDigits, "
                f"got {spark_type.scale}"
            )

    # =========================================================================
    # CONSTRAINT EXTRACTION TESTS
    # =========================================================================

    @pytest.mark.parametrize("type_name,expected", [
        (name, exp) for name, exp in EXPECTED_TYPE_MAPPINGS.items()
        if exp.get('constraints', {}).get('pattern')
    ])
    def test_pattern_constraint_extracted(
        self, efile_types_schema, notebook_classes, type_name, expected
    ):
        """Test that pattern constraints are correctly extracted."""
        TypeMapper = notebook_classes['TypeMapper']
        ConstraintExtractor = notebook_classes['ConstraintExtractor']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        constraints = ConstraintExtractor.extract(xsd_type, TypeMapper)

        assert constraints.pattern is not None, (
            f"{type_name}: Expected pattern constraint, got None"
        )

    @pytest.mark.parametrize("type_name,expected", [
        (name, exp) for name, exp in EXPECTED_TYPE_MAPPINGS.items()
        if exp.get('constraints', {}).get('enumeration')
    ])
    def test_enumeration_constraint_extracted(
        self, efile_types_schema, notebook_classes, type_name, expected
    ):
        """Test that enumeration constraints are correctly extracted."""
        TypeMapper = notebook_classes['TypeMapper']
        ConstraintExtractor = notebook_classes['ConstraintExtractor']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        constraints = ConstraintExtractor.extract(xsd_type, TypeMapper)
        expected_enum = expected['constraints']['enumeration']

        assert constraints.enumeration is not None, (
            f"{type_name}: Expected enumeration constraint, got None"
        )
        # Check that expected values are subset of actual (some may have more)
        assert set(expected_enum).issubset(set(constraints.enumeration)), (
            f"{type_name}: Expected enumerations {expected_enum[:5]}... "
            f"not found in {constraints.enumeration[:5]}..."
        )

    @pytest.mark.parametrize("type_name,expected", [
        (name, exp) for name, exp in EXPECTED_TYPE_MAPPINGS.items()
        if exp.get('constraints', {}).get('max_length')
    ])
    def test_max_length_constraint_extracted(
        self, efile_types_schema, notebook_classes, type_name, expected
    ):
        """Test that max_length constraints are correctly extracted."""
        TypeMapper = notebook_classes['TypeMapper']
        ConstraintExtractor = notebook_classes['ConstraintExtractor']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        constraints = ConstraintExtractor.extract(xsd_type, TypeMapper)
        expected_max = expected['constraints']['max_length']

        assert constraints.max_length is not None, (
            f"{type_name}: Expected max_length constraint, got None"
        )
        assert constraints.max_length == expected_max, (
            f"{type_name}: Expected max_length {expected_max}, "
            f"got {constraints.max_length}"
        )

    @pytest.mark.parametrize("type_name,expected", [
        (name, exp) for name, exp in EXPECTED_TYPE_MAPPINGS.items()
        if exp.get('constraints', {}).get('total_digits')
    ])
    def test_total_digits_constraint_extracted(
        self, efile_types_schema, notebook_classes, type_name, expected
    ):
        """Test that total_digits constraints are correctly extracted."""
        TypeMapper = notebook_classes['TypeMapper']
        ConstraintExtractor = notebook_classes['ConstraintExtractor']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found in schema")

        constraints = ConstraintExtractor.extract(xsd_type, TypeMapper)
        expected_digits = expected['constraints']['total_digits']

        assert constraints.total_digits is not None, (
            f"{type_name}: Expected total_digits constraint, got None"
        )
        assert constraints.total_digits == expected_digits, (
            f"{type_name}: Expected total_digits {expected_digits}, "
            f"got {constraints.total_digits}"
        )

    # =========================================================================
    # CATEGORY COVERAGE TESTS
    # =========================================================================

    @pytest.mark.parametrize("category,types", TYPE_CATEGORIES.items())
    def test_type_category_all_found(
        self, efile_types_schema, category, types
    ):
        """Test that all types in each category exist in the schema."""
        missing = []
        for type_name in types:
            xsd_type = _get_type_from_schema(efile_types_schema, type_name)
            if xsd_type is None:
                missing.append(type_name)

        assert len(missing) == 0, (
            f"Category '{category}' has missing types: {missing}"
        )

    @pytest.mark.parametrize("category,types", TYPE_CATEGORIES.items())
    def test_type_category_all_mappable(
        self, efile_types_schema, notebook_classes, category, types
    ):
        """Test that all types in each category can be mapped."""
        TypeMapper = notebook_classes['TypeMapper']

        failures = []
        for type_name in types:
            xsd_type = _get_type_from_schema(efile_types_schema, type_name)
            if xsd_type is None:
                continue

            try:
                spark_type = TypeMapper.get_spark_type(xsd_type)
                if spark_type is None:
                    failures.append(f"{type_name}: returned None")
            except Exception as e:
                failures.append(f"{type_name}: {e}")

        assert len(failures) == 0, (
            f"Category '{category}' has mapping failures:\n" +
            "\n".join(failures)
        )

    # =========================================================================
    # COVERAGE SUMMARY TEST
    # =========================================================================

    def test_complete_type_coverage(self, efile_types_schema, notebook_classes):
        """Test and report coverage of all types in efileTypes.xsd."""
        TypeMapper = notebook_classes['TypeMapper']

        all_types = _get_all_type_names(efile_types_schema)

        results = {
            'total': len(all_types),
            'mapped': 0,
            'string_fallback': 0,
            'numeric': 0,
            'date': 0,
            'boolean': 0,
            'failed': 0,
            'failures': []
        }

        for type_name in all_types:
            xsd_type = _get_type_from_schema(efile_types_schema, type_name)
            try:
                spark_type = TypeMapper.get_spark_type(xsd_type)
                if spark_type is not None:
                    results['mapped'] += 1
                    if isinstance(spark_type, StringType):
                        results['string_fallback'] += 1
                    elif isinstance(spark_type, (LongType, IntegerType, DecimalType)):
                        results['numeric'] += 1
                    elif isinstance(spark_type, (DateType, TimestampType)):
                        results['date'] += 1
                    elif isinstance(spark_type, BooleanType):
                        results['boolean'] += 1
                else:
                    results['failed'] += 1
                    results['failures'].append((type_name, 'Returned None'))
            except Exception as e:
                results['failed'] += 1
                results['failures'].append((type_name, str(e)))

        # Print summary
        coverage = results['mapped'] / results['total'] * 100 if results['total'] > 0 else 0
        print(f"\n{'='*60}")
        print(f"TYPE MAPPING COVERAGE REPORT")
        print(f"{'='*60}")
        print(f"Total types:      {results['total']}")
        print(f"Mapped:           {results['mapped']} ({coverage:.1f}%)")
        print(f"  - StringType:   {results['string_fallback']}")
        print(f"  - Numeric:      {results['numeric']}")
        print(f"  - Date/Time:    {results['date']}")
        print(f"  - Boolean:      {results['boolean']}")
        print(f"Failed:           {results['failed']}")

        if results['failures']:
            print(f"\nFailed types:")
            for name, error in results['failures'][:10]:
                print(f"  - {name}: {error}")
            if len(results['failures']) > 10:
                print(f"  ... and {len(results['failures']) - 10} more")

        # Assert minimum coverage
        assert coverage >= 90.0, f"Type coverage below 90%: {coverage:.1f}%"


class TestEfileTypesAmount:
    """Focused tests for amount type mappings."""

    def test_us_amount_types_are_long(self, efile_types_schema, notebook_classes):
        """Test that USAmount types map to LongType."""
        TypeMapper = notebook_classes['TypeMapper']

        us_amount_types = [
            'USAmountType', 'USAmountNNType', 'USAmountPosType',
            'USAmountNegType', 'USAmountNonPosType'
        ]

        for type_name in us_amount_types:
            xsd_type = _get_type_from_schema(efile_types_schema, type_name)
            if xsd_type is None:
                continue

            spark_type = TypeMapper.get_spark_type(xsd_type)
            assert isinstance(spark_type, LongType), (
                f"{type_name} should be LongType, got {type(spark_type).__name__}"
            )

    def test_us_decimal_amount_types_are_decimal(
        self, efile_types_schema, notebook_classes
    ):
        """Test that USDecimalAmount types map to DecimalType."""
        TypeMapper = notebook_classes['TypeMapper']

        decimal_types = [
            'USDecimalAmountType', 'USDecimalAmountNNType',
            'USDecimalAmountPosType', 'USLargeDecimalAmountType'
        ]

        for type_name in decimal_types:
            xsd_type = _get_type_from_schema(efile_types_schema, type_name)
            if xsd_type is None:
                continue

            spark_type = TypeMapper.get_spark_type(xsd_type)
            assert isinstance(spark_type, DecimalType), (
                f"{type_name} should be DecimalType, got {type(spark_type).__name__}"
            )

    def test_foreign_amount_types_are_long(
        self, efile_types_schema, notebook_classes
    ):
        """Test that ForeignAmount types map to LongType."""
        TypeMapper = notebook_classes['TypeMapper']

        foreign_types = ['ForeignAmountType', 'ForeignAmountNNType']

        for type_name in foreign_types:
            xsd_type = _get_type_from_schema(efile_types_schema, type_name)
            if xsd_type is None:
                continue

            spark_type = TypeMapper.get_spark_type(xsd_type)
            assert isinstance(spark_type, LongType), (
                f"{type_name} should be LongType, got {type(spark_type).__name__}"
            )


class TestEfileTypesIdentifiers:
    """Focused tests for identifier type mappings."""

    @pytest.mark.parametrize("type_name,expected_pattern", [
        ('SSNType', r'[0-9]{9}'),
        ('EINType', r'[0-9]{9}'),
        ('PTINType', r'P[0-9]{8}'),
        ('EFINType', r'[0-9]{6}'),
        ('PINType', r'[0-9]{5}'),
        ('ETINType', r'[0-9]{5}'),
    ])
    def test_id_type_patterns(
        self, efile_types_schema, notebook_classes, type_name, expected_pattern
    ):
        """Test that ID types have correct pattern constraints."""
        TypeMapper = notebook_classes['TypeMapper']
        ConstraintExtractor = notebook_classes['ConstraintExtractor']
        xsd_type = _get_type_from_schema(efile_types_schema, type_name)

        if xsd_type is None:
            pytest.skip(f"Type {type_name} not found")

        constraints = ConstraintExtractor.extract(xsd_type, TypeMapper)

        assert constraints.pattern is not None, f"{type_name} should have pattern"
        assert constraints.pattern == expected_pattern, (
            f"{type_name}: Expected pattern '{expected_pattern}', got '{constraints.pattern}'"
        )
