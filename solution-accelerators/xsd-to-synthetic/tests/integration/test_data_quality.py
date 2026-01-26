"""
Data generation quality validation tests.

Tests that generated synthetic data properly respects XSD constraints
including patterns, enumerations, numeric bounds, and length limits.
"""
import pytest
import re
from decimal import Decimal
from pyspark.sql.types import (
    StringType, LongType, IntegerType, DecimalType,
    BooleanType, DateType, TimestampType, StructType, StructField
)
import datetime


class TestPatternConstraints:
    """Test that pattern constraints are respected in generated data."""

    def test_ssn_format_valid(self, spark, notebook_classes):
        """Test that SSN fields generate valid 9-digit strings."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']
        SchemaField = notebook_classes['SchemaField']

        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            pattern=r'[0-9]{9}'
        )

        spark_schema = StructType([
            StructField('ssn', StringType(), True)
        ])
        constraints_dict = {'ssn': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['ssn'] for row in df.collect()]
        pattern = re.compile(r'^[0-9]{9}$')

        invalid = [v for v in values if v is not None and not pattern.match(v)]

        assert len(invalid) == 0, f"Invalid SSN values found: {invalid[:5]}"

    def test_ein_format_valid(self, spark, notebook_classes):
        """Test that EIN fields generate valid 9-digit strings."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            pattern=r'[0-9]{9}'
        )

        spark_schema = StructType([
            StructField('ein', StringType(), True)
        ])
        constraints_dict = {'ein': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['ein'] for row in df.collect()]
        pattern = re.compile(r'^[0-9]{9}$')

        invalid = [v for v in values if v is not None and not pattern.match(v)]

        assert len(invalid) == 0, f"Invalid EIN values found: {invalid[:5]}"

    def test_ptin_format_valid(self, spark, notebook_classes):
        """Test that PTIN fields generate P followed by 8 digits."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            pattern=r'P[0-9]{8}'
        )

        spark_schema = StructType([
            StructField('ptin', StringType(), True)
        ])
        constraints_dict = {'ptin': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['ptin'] for row in df.collect()]
        pattern = re.compile(r'^P[0-9]{8}$')

        invalid = [v for v in values if v is not None and not pattern.match(v)]

        assert len(invalid) == 0, f"Invalid PTIN values found: {invalid[:5]}"


class TestNumericBounds:
    """Test that numeric bounds are respected in generated data."""

    def test_amount_respects_bounds(self, spark, notebook_classes):
        """Test that amount fields respect min/max bounds."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='integer',
            spark_type=LongType(),
            min_value=0,
            max_value=999999999999999,  # 15 digits
            total_digits=15
        )

        spark_schema = StructType([
            StructField('amount', LongType(), True)
        ])
        constraints_dict = {'amount': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['amount'] for row in df.collect()]

        for val in values:
            if val is not None:
                assert isinstance(val, int), f"Amount should be int: {type(val)}"
                assert 0 <= val <= 999999999999999, f"Amount out of bounds: {val}"

    def test_decimal_amount_respects_bounds(self, spark, notebook_classes):
        """Test that decimal amount fields respect precision and bounds."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='decimal',
            spark_type=DecimalType(17, 2),
            min_value=Decimal('0.00'),
            max_value=Decimal('999999999999999.99'),
            total_digits=17,
            fraction_digits=2
        )

        spark_schema = StructType([
            StructField('decimal_amount', DecimalType(17, 2), True)
        ])
        constraints_dict = {'decimal_amount': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['decimal_amount'] for row in df.collect()]

        for val in values:
            if val is not None:
                assert Decimal('0') <= val <= Decimal('999999999999999.99'), (
                    f"Decimal amount out of bounds: {val}"
                )

    def test_ratio_within_zero_to_one(self, spark, notebook_classes):
        """Test that ratio types generate values in [0, 1]."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='decimal',
            spark_type=DecimalType(6, 5),
            min_value=Decimal('0.00000'),
            max_value=Decimal('1.00000'),
            fraction_digits=5
        )

        spark_schema = StructType([
            StructField('ratio', DecimalType(6, 5), True)
        ])
        constraints_dict = {'ratio': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['ratio'] for row in df.collect()]

        for val in values:
            if val is not None:
                assert Decimal('0') <= val <= Decimal('1'), (
                    f"Ratio out of bounds: {val}"
                )

    def test_business_activity_code_range(self, spark, notebook_classes):
        """Test that business activity codes are within valid range."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='integer',
            spark_type=IntegerType(),
            min_value=1,
            max_value=999000
        )

        spark_schema = StructType([
            StructField('activity_code', IntegerType(), True)
        ])
        constraints_dict = {'activity_code': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['activity_code'] for row in df.collect()]

        for val in values:
            if val is not None:
                assert 1 <= val <= 999000, f"Activity code out of range: {val}"


class TestEnumerationConstraints:
    """Test that enumeration constraints are respected in generated data."""

    def test_enumeration_only_valid_values(self, spark, notebook_classes):
        """Test that enumeration fields only generate allowed values."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        allowed = ['CA', 'NY', 'TX', 'FL', 'IL']
        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            enumeration=allowed
        )

        spark_schema = StructType([
            StructField('state', StringType(), True)
        ])
        constraints_dict = {'state': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['state'] for row in df.collect()]

        invalid = [v for v in values if v is not None and v not in allowed]

        assert len(invalid) == 0, f"Invalid enumeration values: {invalid[:5]}"

    def test_bank_account_type_enumeration(self, spark, notebook_classes):
        """Test that bank account type uses enumeration values."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        allowed = ['1', '2']  # 1=Checking, 2=Savings
        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            enumeration=allowed
        )

        spark_schema = StructType([
            StructField('account_type', StringType(), True)
        ])
        constraints_dict = {'account_type': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['account_type'] for row in df.collect()]

        invalid = [v for v in values if v is not None and v not in allowed]

        assert len(invalid) == 0, f"Invalid bank account types: {invalid[:5]}"

    def test_checkbox_enumeration(self, spark, notebook_classes):
        """Test that checkbox fields only generate 'X' value."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            enumeration=['X']
        )

        spark_schema = StructType([
            StructField('checkbox', StringType(), True)
        ])
        constraints_dict = {'checkbox': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.5  # Checkboxes are often optional
        )

        values = [row['checkbox'] for row in df.collect()]

        invalid = [v for v in values if v is not None and v != 'X']

        assert len(invalid) == 0, f"Checkbox should only be 'X' or null: {invalid[:5]}"


class TestLengthConstraints:
    """Test that length constraints are respected in generated data."""

    def test_string_max_length_respected(self, spark, notebook_classes):
        """Test that string fields respect max_length constraint."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        max_len = 35
        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            max_length=max_len
        )

        spark_schema = StructType([
            StructField('name', StringType(), True)
        ])
        constraints_dict = {'name': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['name'] for row in df.collect()]

        too_long = [v for v in values if v is not None and len(v) > max_len]

        assert len(too_long) == 0, (
            f"Strings exceeding max_length={max_len}: {too_long[:5]}"
        )

    def test_city_max_length(self, spark, notebook_classes):
        """Test that string fields respect max_length constraint."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        max_len = 22
        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            max_length=max_len
        )

        # Use a field name that doesn't match any hint patterns to test
        # DefaultStringStrategy's max_length handling
        spark_schema = StructType([
            StructField('test_field', StringType(), True)
        ])
        constraints_dict = {'test_field': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['test_field'] for row in df.collect()]

        too_long = [v for v in values if v is not None and len(v) > max_len]

        assert len(too_long) == 0, (
            f"Strings exceeding max_length={max_len}: {too_long[:5]}"
        )


class TestDateTimeGeneration:
    """Test date and time field generation."""

    def test_date_format_valid(self, spark, notebook_classes):
        """Test that date fields generate valid dates."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='date',
            spark_type=DateType()
        )

        spark_schema = StructType([
            StructField('date_field', DateType(), True)
        ])
        constraints_dict = {'date_field': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['date_field'] for row in df.collect()]

        for val in values:
            if val is not None:
                assert isinstance(val, datetime.date), f"Should be date: {type(val)}"

    def test_timestamp_format_valid(self, spark, notebook_classes):
        """Test that timestamp fields generate valid timestamps."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='datetime',
            spark_type=TimestampType()
        )

        spark_schema = StructType([
            StructField('timestamp_field', TimestampType(), True)
        ])
        constraints_dict = {'timestamp_field': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['timestamp_field'] for row in df.collect()]

        for val in values:
            if val is not None:
                assert isinstance(val, datetime.datetime), (
                    f"Should be datetime: {type(val)}"
                )


class TestNullHandling:
    """Test null value handling in generated data."""

    def test_nullable_fields_have_nulls(self, spark, notebook_classes):
        """Test that nullable fields produce some null values."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType()
        )

        spark_schema = StructType([
            StructField('optional', StringType(), True)
        ])
        constraints_dict = {'optional': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=1000,
            null_probability=0.3
        )

        null_count = df.filter(df.optional.isNull()).count()

        # With 30% null probability and 1000 rows, expect ~300 nulls (allow variance)
        assert 100 < null_count < 500, f"Expected ~300 nulls, got {null_count}"

    def test_zero_null_probability(self, spark, notebook_classes):
        """Test that zero null probability produces no nulls."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType()
        )

        spark_schema = StructType([
            StructField('required', StringType(), True)
        ])
        constraints_dict = {'required': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        null_count = df.filter(df.required.isNull()).count()

        assert null_count == 0, f"Expected no nulls with null_probability=0, got {null_count}"


class TestBooleanGeneration:
    """Test boolean field generation."""

    def test_boolean_values_valid(self, spark, notebook_classes):
        """Test that boolean fields generate true/false values."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='boolean',
            spark_type=BooleanType()
        )

        spark_schema = StructType([
            StructField('flag', BooleanType(), True)
        ])
        constraints_dict = {'flag': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=100,
            null_probability=0.0
        )

        values = [row['flag'] for row in df.collect()]

        for val in values:
            if val is not None:
                assert isinstance(val, bool), f"Should be bool: {type(val)}"

    def test_boolean_distribution(self, spark, notebook_classes):
        """Test that boolean fields have reasonable true/false distribution."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        constraints = TypeConstraints(
            base_type='boolean',
            spark_type=BooleanType()
        )

        spark_schema = StructType([
            StructField('flag', BooleanType(), True)
        ])
        constraints_dict = {'flag': constraints}

        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=1000,
            null_probability=0.0
        )

        true_count = df.filter(df.flag == True).count()
        false_count = df.filter(df.flag == False).count()

        # Expect some reasonable distribution (not all true or all false)
        assert true_count > 100, f"Expected more true values, got {true_count}"
        assert false_count > 100, f"Expected more false values, got {false_count}"
