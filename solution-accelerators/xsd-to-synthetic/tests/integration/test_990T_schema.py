"""
Full 990T schema parsing and field type validation.

Tests the complete IRS 990-T schema parsing with all includes,
Spark schema building, and data generation.

These tests require IRS XSD schemas to be downloaded separately.
Set the XSD_SCHEMA_PATH environment variable or place schemas in ~/Downloads.
"""
import os
from pathlib import Path
import pytest
from pyspark.sql.types import (
    StructType, StructField, DataType, StringType, LongType,
    DecimalType, BooleanType, DateType, TimestampType, ArrayType
)


def _get_schema_base():
    """Get schema base path from environment or default location."""
    # Check environment variable first
    env_path = os.environ.get('XSD_SCHEMA_PATH')
    if env_path and Path(env_path).exists():
        return env_path
    
    # Fall back to common download locations
    default_paths = [
        Path.home() / "Downloads" / "990T Schema 2024 v5.0" / "2024v5.0",
        Path.home() / "Downloads" / "irs-schemas" / "2024v5.0",
        Path("schemas") / "2024v5.0",
    ]
    
    for path in default_paths:
        if path.exists():
            return str(path)
    
    return None


# Schema paths - tests will skip if not available
SCHEMA_990T_V5_BASE = _get_schema_base()
EFILE_TYPES_V5_PATH = f'{SCHEMA_990T_V5_BASE}/Common/efileTypes.xsd' if SCHEMA_990T_V5_BASE else None
RETURN_990T_V5_PATH = f'{SCHEMA_990T_V5_BASE}/TEGE/TEGE990T/Return990T.xsd' if SCHEMA_990T_V5_BASE else None


class Test990TSchemaLoading:
    """Test 990T schema loading and basic structure."""

    def test_schema_loads_successfully(self, schema_990T_v5):
        """Test that the 990T v5.0 schema loads without errors."""
        assert schema_990T_v5 is not None
        assert len(schema_990T_v5.types) > 0
        print(f"\nLoaded schema with {len(schema_990T_v5.types)} types")
        print(f"Elements: {len(schema_990T_v5.elements)}")

    def test_irs990t_element_exists(self, schema_990T_v5):
        """Test that the IRS990T root element exists."""
        ns = '{http://www.irs.gov/efile}'

        # Check both with and without namespace
        root_elements = list(schema_990T_v5.elements.keys())

        irs990t_found = any('IRS990T' in el for el in root_elements)
        assert irs990t_found, (
            f"IRS990T element not found. Available elements: "
            f"{[e for e in root_elements if 'IRS' in e or '990' in e][:10]}"
        )

    def test_irs990t_type_has_elements(self, schema_990T_v5):
        """Test that IRS990T has child elements defined."""
        ns = '{http://www.irs.gov/efile}'
        irs990t_type = schema_990T_v5.types.get(ns + 'IRS990TType')

        if irs990t_type is None:
            pytest.skip("IRS990TType not found")

        # Check it has content model
        assert hasattr(irs990t_type, 'content') or hasattr(irs990t_type, 'content_type'), (
            "IRS990TType should have content model"
        )

    def test_efile_types_included(self, schema_990T_v5):
        """Test that efileTypes base types are included."""
        # Note: xmlschema stores type keys without namespace prefix
        expected_types = ['USAmountType', 'DateType', 'EINType', 'StateType']
        missing = []

        for type_name in expected_types:
            # Try both with and without namespace prefix
            xsd_type = schema_990T_v5.types.get(type_name)
            if xsd_type is None:
                ns = '{http://www.irs.gov/efile}'
                xsd_type = schema_990T_v5.types.get(ns + type_name)
            if xsd_type is None:
                missing.append(type_name)

        assert len(missing) == 0, f"Missing base types from efileTypes: {missing}"


class Test990TTypeResolution:
    """Test that all types in 990T schema resolve correctly."""

    def test_all_types_resolve(self, schema_990T_v5, notebook_classes):
        """Test that all types in the schema can be resolved."""
        TypeMapper = notebook_classes['TypeMapper']

        # Note: xmlschema stores type keys without namespace prefix
        # Filter out XSD built-in types (they start with xs: or xsd:)
        all_types = [
            name for name in schema_990T_v5.types.keys()
            if isinstance(name, str) and not name.startswith('{http://www.w3.org/')
        ]

        unresolved = []
        resolved = 0

        for type_name in all_types:
            xsd_type = schema_990T_v5.types.get(type_name)
            try:
                spark_type = TypeMapper.get_spark_type(xsd_type)
                if spark_type is not None:
                    resolved += 1
                else:
                    unresolved.append(type_name)
            except Exception as e:
                unresolved.append(f"{type_name}: {e}")

        total = len(all_types)
        resolution_rate = resolved / total * 100 if total > 0 else 0

        print(f"\n990T Type Resolution:")
        print(f"  Total types: {total}")
        print(f"  Resolved: {resolved} ({resolution_rate:.1f}%)")
        print(f"  Unresolved: {len(unresolved)}")

        if unresolved and len(unresolved) <= 10:
            print(f"\n  Unresolved types:")
            for t in unresolved[:10]:
                print(f"    - {t}")

        # Allow some unresolved (complex types without simple content)
        assert resolution_rate >= 80, f"Type resolution below 80%: {resolution_rate:.1f}%"

    def test_amount_types_resolve_to_numeric(
        self, schema_990T_v5, notebook_classes
    ):
        """Test that amount fields in 990T resolve to numeric types."""
        TypeMapper = notebook_classes['TypeMapper']

        # Common amount type names that should be numeric
        # Note: xmlschema stores type keys without namespace prefix
        amount_types = [
            'USAmountType', 'USAmountNNType', 'USDecimalAmountType',
            'ForeignAmountType', 'USAmountPosType'
        ]

        string_mapped = []

        for type_name in amount_types:
            xsd_type = schema_990T_v5.types.get(type_name)
            if xsd_type is None:
                continue

            spark_type = TypeMapper.get_spark_type(xsd_type)
            if isinstance(spark_type, StringType):
                string_mapped.append(type_name)

        assert len(string_mapped) == 0, (
            f"Amount types incorrectly mapped to StringType: {string_mapped}"
        )


class Test990TSparkSchemaBuilder:
    """Test SparkSchemaBuilder with 990T schema."""

    def test_spark_schema_builds_successfully(
        self, schema_990T_v5, notebook_classes
    ):
        """Test that SparkSchemaBuilder can build from 990T schema."""
        SparkSchemaBuilder = notebook_classes['SparkSchemaBuilder']
        FlattenMode = notebook_classes['FlattenMode']

        builder = SparkSchemaBuilder(
            flatten_mode=FlattenMode.FULL,
            max_depth=10
        )

        spark_schema, constraints_dict = builder.build(
            schema_990T_v5,
            root_element='IRS990T'
        )

        assert spark_schema is not None, "Spark schema should not be None"
        assert isinstance(spark_schema, StructType), "Should return StructType"
        assert len(spark_schema.fields) > 0, "Schema should have fields"

        print(f"\nBuilt Spark schema with {len(spark_schema.fields)} fields")
        print(f"Constraints dict has {len(constraints_dict)} entries")

    def test_spark_schema_field_types_valid(
        self, schema_990T_v5, notebook_classes
    ):
        """Test that all field types in Spark schema are valid."""
        SparkSchemaBuilder = notebook_classes['SparkSchemaBuilder']
        FlattenMode = notebook_classes['FlattenMode']

        builder = SparkSchemaBuilder(
            flatten_mode=FlattenMode.FULL,
            max_depth=10
        )

        spark_schema, _ = builder.build(
            schema_990T_v5,
            root_element='IRS990T'
        )

        def validate_field_types(schema, path=""):
            """Recursively validate all field types."""
            invalid = []
            for field in schema.fields:
                field_path = f"{path}.{field.name}" if path else field.name
                if not isinstance(field.dataType, DataType):
                    invalid.append(f"{field_path}: {type(field.dataType)}")
                elif isinstance(field.dataType, StructType):
                    invalid.extend(validate_field_types(field.dataType, field_path))
                elif isinstance(field.dataType, ArrayType):
                    if isinstance(field.dataType.elementType, StructType):
                        invalid.extend(
                            validate_field_types(
                                field.dataType.elementType,
                                f"{field_path}[]"
                            )
                        )
            return invalid

        invalid_fields = validate_field_types(spark_schema)
        assert len(invalid_fields) == 0, f"Invalid field types: {invalid_fields}"

    def test_spark_schema_type_distribution(
        self, schema_990T_v5, notebook_classes
    ):
        """Test and report type distribution in the Spark schema."""
        SparkSchemaBuilder = notebook_classes['SparkSchemaBuilder']
        FlattenMode = notebook_classes['FlattenMode']

        builder = SparkSchemaBuilder(
            flatten_mode=FlattenMode.FULL,
            max_depth=10
        )

        spark_schema, _ = builder.build(
            schema_990T_v5,
            root_element='IRS990T'
        )

        def count_types(schema, counts=None):
            """Count field types recursively."""
            if counts is None:
                counts = {}
            for field in schema.fields:
                type_name = type(field.dataType).__name__
                counts[type_name] = counts.get(type_name, 0) + 1
                if isinstance(field.dataType, StructType):
                    count_types(field.dataType, counts)
                elif isinstance(field.dataType, ArrayType):
                    if isinstance(field.dataType.elementType, StructType):
                        count_types(field.dataType.elementType, counts)
            return counts

        type_counts = count_types(spark_schema)

        print("\n990T Spark Schema Type Distribution:")
        for type_name, count in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"  {type_name}: {count}")

        # Verify we have numeric types (amounts shouldn't all be strings)
        numeric_count = type_counts.get('LongType', 0) + type_counts.get('DecimalType', 0)
        string_count = type_counts.get('StringType', 0)

        print(f"\nNumeric vs String ratio: {numeric_count}:{string_count}")

        # We expect some numeric types for amounts
        assert numeric_count > 0, "Expected some LongType or DecimalType fields for amounts"


class Test990TDataGeneration:
    """Test synthetic data generation from 990T schema."""

    def test_data_generation_completes(
        self, spark, schema_990T_v5, notebook_classes
    ):
        """Test that data generation completes without error."""
        SparkSchemaBuilder = notebook_classes['SparkSchemaBuilder']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']
        FlattenMode = notebook_classes['FlattenMode']

        # Build schema
        builder = SparkSchemaBuilder(
            flatten_mode=FlattenMode.FULL,
            max_depth=10
        )
        spark_schema, constraints_dict = builder.build(
            schema_990T_v5,
            root_element='IRS990T'
        )

        # Generate data
        generator = SyntheticDataGenerator(spark, schema=schema_990T_v5)

        # Use a small row count for testing
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=10,
        )

        assert df is not None, "Generated DataFrame should not be None"
        assert df.count() == 10, f"Expected 10 rows, got {df.count()}"

        print(f"\nGenerated DataFrame with {df.count()} rows")
        print(f"Schema has {len(df.schema.fields)} fields")

    def test_data_generation_schema_matches(
        self, spark, schema_990T_v5, notebook_classes
    ):
        """Test that generated data schema matches expected schema."""
        SparkSchemaBuilder = notebook_classes['SparkSchemaBuilder']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']
        FlattenMode = notebook_classes['FlattenMode']

        # Build schema
        builder = SparkSchemaBuilder(
            flatten_mode=FlattenMode.FULL,
            max_depth=10
        )
        spark_schema, constraints_dict = builder.build(
            schema_990T_v5,
            root_element='IRS990T'
        )

        # Generate data
        generator = SyntheticDataGenerator(spark, schema=schema_990T_v5)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=5,
        )

        # Compare field names
        expected_fields = set(f.name for f in spark_schema.fields)
        actual_fields = set(f.name for f in df.schema.fields)

        missing = expected_fields - actual_fields
        extra = actual_fields - expected_fields

        assert len(missing) == 0, f"Missing fields in generated data: {missing}"
        assert len(extra) == 0, f"Extra fields in generated data: {extra}"

    def test_generated_data_has_values(
        self, spark, schema_990T_v5, notebook_classes
    ):
        """Test that generated data has non-null values."""
        SparkSchemaBuilder = notebook_classes['SparkSchemaBuilder']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']
        FlattenMode = notebook_classes['FlattenMode']

        # Build schema
        builder = SparkSchemaBuilder(
            flatten_mode=FlattenMode.FULL,
            max_depth=10
        )
        spark_schema, constraints_dict = builder.build(
            schema_990T_v5,
            root_element='IRS990T'
        )

        # Generate data with low null probability
        generator = SyntheticDataGenerator(spark, schema=schema_990T_v5)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.1,
        )

        # Check that we have some non-null values
        first_row = df.first()
        non_null_count = sum(1 for v in first_row.asDict().values() if v is not None)

        print(f"\nFirst row has {non_null_count} non-null values out of {len(first_row)}")

        assert non_null_count > 0, "Generated data should have some non-null values"


class TestXSDIncludeResolution:
    """Test that XSD includes are properly resolved using real 990T v5.0 schema."""

    @pytest.fixture
    def schema_v5_paths_exist(self):
        """Skip tests if schema files don't exist."""
        if SCHEMA_990T_V5_BASE is None:
            pytest.skip("XSD schemas not found. Set XSD_SCHEMA_PATH or download IRS schemas.")
        if not os.path.exists(SCHEMA_990T_V5_BASE):
            pytest.skip(f"Schema base not found at {SCHEMA_990T_V5_BASE}")
        if EFILE_TYPES_V5_PATH and not os.path.exists(EFILE_TYPES_V5_PATH):
            pytest.skip(f"efileTypes.xsd not found at {EFILE_TYPES_V5_PATH}")
        if RETURN_990T_V5_PATH and not os.path.exists(RETURN_990T_V5_PATH):
            pytest.skip(f"Return990T.xsd not found at {RETURN_990T_V5_PATH}")
        return True

    def test_include_paths_resolves_irs_types(self, notebook_classes, schema_v5_paths_exist):
        """Test that include_paths properly redirects XSD includes."""
        XSDLoader = notebook_classes.get('XSDLoader')
        if not XSDLoader:
            pytest.skip("XSDLoader not available in notebook_classes")

        loader = XSDLoader()
        schema = loader.load(
            RETURN_990T_V5_PATH,
            include_paths=[EFILE_TYPES_V5_PATH]
        )

        # Verify IRS types are loaded (USAmountType should exist)
        type_names = [str(t) for t in schema.types.keys()]
        has_us_amount = any('USAmountType' in t for t in type_names)

        print(f"\nLoaded schema with {len(type_names)} types")
        if not has_us_amount:
            # Show what types we got
            irs_types = [t for t in type_names if 'http://www.irs.gov' in t][:10]
            print(f"Sample IRS types found: {irs_types}")

        assert has_us_amount, \
            f"USAmountType should be loaded from efileTypes.xsd. Found {len(type_names)} types."

    def test_amount_field_not_string(self, notebook_classes, schema_v5_paths_exist):
        """Test that amount fields resolve to numeric types, not StringType."""
        XSDLoader = notebook_classes.get('XSDLoader')
        TypeMapper = notebook_classes['TypeMapper']

        if not XSDLoader:
            pytest.skip("XSDLoader not available in notebook_classes")

        loader = XSDLoader()
        schema = loader.load(
            RETURN_990T_V5_PATH,
            include_paths=[EFILE_TYPES_V5_PATH]
        )

        # Find USAmountType and verify it maps to LongType (not StringType)
        us_amount_type = None
        for type_name, xsd_type in schema.types.items():
            type_str = str(type_name)
            if 'USAmountType' in type_str and 'Decimal' not in type_str and 'NN' not in type_str:
                us_amount_type = xsd_type
                spark_type = TypeMapper.get_spark_type(xsd_type)
                print(f"\nUSAmountType ({type_name}) maps to: {spark_type}")

                assert 'StringType' not in str(spark_type), \
                    f"USAmountType should NOT map to StringType, got {spark_type}"
                assert 'LongType' in str(spark_type) or 'IntegerType' in str(spark_type), \
                    f"USAmountType should map to LongType or IntegerType, got {spark_type}"
                break

        assert us_amount_type is not None, "USAmountType should be found in schema"

    def test_schema_has_expected_type_count(self, notebook_classes, schema_v5_paths_exist):
        """Test that schema loads with sufficient types (includes resolved)."""
        XSDLoader = notebook_classes.get('XSDLoader')

        if not XSDLoader:
            pytest.skip("XSDLoader not available in notebook_classes")

        loader = XSDLoader()

        # Load WITH include_paths
        schema = loader.load(
            RETURN_990T_V5_PATH,
            include_paths=[EFILE_TYPES_V5_PATH]
        )
        type_count = len(schema.types)

        print(f"\nLoaded {type_count} types from Return990T.xsd with includes")

        # We expect at least 100+ types when includes are properly resolved
        # efileTypes.xsd alone has ~140 types
        assert type_count >= 100, \
            f"Schema should have at least 100 types when includes are resolved, got {type_count}"

        # Verify specific IRS types are present
        type_names = [str(t) for t in schema.types.keys()]
        expected_types = ['USAmountType', 'DateType', 'EINType', 'StateType', 'CheckboxType']
        found_types = [t for t in expected_types if any(t in name for name in type_names)]

        print(f"Found expected types: {found_types}")

        assert len(found_types) >= 3, \
            f"Expected to find at least 3 of {expected_types}, found {found_types}"
