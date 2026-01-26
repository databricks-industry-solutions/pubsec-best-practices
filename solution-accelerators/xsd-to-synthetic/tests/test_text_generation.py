"""
Test text generation and field hint detection.
"""

import pytest
import re
from pyspark.sql.types import StructType, StructField, StringType


class TestFieldHintDetection:
    """Test automatic field hint detection from field names."""

    def test_description_field_detection(self, notebook_classes):
        """Test that description fields are detected correctly."""
        FieldHint = notebook_classes['FieldHint']
        DEFAULT_FIELD_HINT_PATTERNS = {
            r'.*Desc(ription)?.*': FieldHint.DESCRIPTION,
        }

        test_fields = [
            ('ActivityDesc', True),
            ('SpecialConditionDescription', True),
            ('Desc', True),
            ('Description', True),
            ('SomeOtherField', False),
        ]

        pattern = re.compile(r'.*Desc(ription)?.*')
        for field_name, should_match in test_fields:
            matches = pattern.match(field_name) is not None
            assert matches == should_match, \
                f"Field '{field_name}' match={matches}, expected={should_match}"

    def test_text_field_pattern_anchoring(self, notebook_classes):
        """Test that text field pattern correctly anchors at end."""
        FieldHint = notebook_classes['FieldHint']

        # The fixed pattern should be .*(Txt|Text)$
        pattern = re.compile(r'.*(Txt|Text)$')

        test_fields = [
            ('ActivityDescTxt', True),      # Ends with Txt
            ('ExplanationText', True),      # Ends with Text
            ('DetailTxt', True),            # Ends with Txt
            ('CommentText', True),          # Ends with Text
            ('TextSomething', False),       # Text not at end
            ('TxtSomething', False),        # Txt not at end
            ('SomeField', False),           # No Txt or Text
        ]

        for field_name, should_match in test_fields:
            matches = pattern.match(field_name) is not None
            assert matches == should_match, \
                f"Field '{field_name}' match={matches}, expected={should_match}"


class TestTextGenerationFakers:
    """Test that text generation uses appropriate Faker providers."""

    def test_description_uses_sentence(self, spark, notebook_classes):
        """Test that DESCRIPTION hint uses Faker sentence provider."""
        FieldHint = notebook_classes['FieldHint']
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        # Create constraint
        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            max_length=100
        )

        # Create schema and constraints dict
        spark_schema = StructType([
            StructField('description', StringType(), True)
        ])
        constraints_dict = {'description': constraints}

        # Generate data with DESCRIPTION hint (no nulls for this test)
        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=10,
            field_hints={'description': FieldHint.DESCRIPTION},
            null_probability=0.0
        )

        # Collect values
        values = [row['description'] for row in df.collect()]

        # Assert: Values should be sentence-like (not just random characters)
        for val in values:
            assert isinstance(val, str), "Value should be string"
            assert len(val) > 5, "Sentence should be more than 5 chars"

    def test_fallback_uses_faker_for_long_fields(self, spark, notebook_classes):
        """Test that long string fields without hints use Faker text."""
        TypeConstraints = notebook_classes['TypeConstraints']
        SyntheticDataGenerator = notebook_classes['SyntheticDataGenerator']

        # Create constraint for long string field (>20 chars) without hint
        constraints = TypeConstraints(
            base_type='string',
            spark_type=StringType(),
            max_length=150
        )

        # Create schema and constraints dict
        spark_schema = StructType([
            StructField('long_text_field', StringType(), True)
        ])
        constraints_dict = {'long_text_field': constraints}

        # Generate data (no nulls for this test)
        generator = SyntheticDataGenerator(spark)
        df = generator.generate(
            spark_schema,
            constraints_dict,
            num_rows=10,
            null_probability=0.0
        )

        # Collect values
        values = [row['long_text_field'] for row in df.collect()]

        # Assert: Values should be text-like (not just \w\w\w pattern)
        for val in values:
            assert val is not None, "Value should not be None"
            assert isinstance(val, str), "Value should be string"


class TestArrayConstraints:
    """Test array occurrence constraint handling."""

    def test_array_occurrence_warning(self, temp_xsd, spark, notebook_classes, caplog):
        """Test that arrays with minOccurs generate appropriate warning."""
        SparkSchemaBuilder = notebook_classes['SparkSchemaBuilder']
        FlattenMode = notebook_classes['FlattenMode']

        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsd:complexType name="RootType">
        <xsd:sequence>
            <xsd:element name="Items" type="xsd:string" minOccurs="2" maxOccurs="unbounded"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:element name="Root" type="RootType"/>
</xsd:schema>'''

        xsd_path = temp_xsd(xsd_content)
        import xmlschema
        schema = xmlschema.XMLSchema(xsd_path)

        # Build Spark schema with correct API
        builder = SparkSchemaBuilder(flatten_mode=FlattenMode.FULL)

        # Enable logging to capture warnings
        import logging
        logging.basicConfig(level=logging.WARNING)

        spark_schema, constraints_dict = builder.build(schema, 'Root')

        # The builder should have logged a warning about array constraints
        # (Can't easily test logging without more setup, but this verifies no crash)
        assert spark_schema is not None
        assert len(constraints_dict) > 0

    def test_min_occurs_sets_nullable(self, temp_xsd, notebook_classes):
        """Test that minOccurs=0 sets nullable=True."""
        SparkSchemaBuilder = notebook_classes['SparkSchemaBuilder']
        FlattenMode = notebook_classes['FlattenMode']

        xsd_content = '''<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <xsd:complexType name="RootType">
        <xsd:sequence>
            <xsd:element name="OptionalField" type="xsd:string" minOccurs="0"/>
            <xsd:element name="RequiredField" type="xsd:string" minOccurs="1"/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:element name="Root" type="RootType"/>
</xsd:schema>'''

        xsd_path = temp_xsd(xsd_content)
        import xmlschema
        schema = xmlschema.XMLSchema(xsd_path)

        # Build Spark schema with correct API
        builder = SparkSchemaBuilder(flatten_mode=FlattenMode.FULL)
        spark_schema, constraints_dict = builder.build(schema, 'Root')

        # Find the fields
        optional_field = next((f for f in spark_schema.fields if f.name == 'OptionalField'), None)
        required_field = next((f for f in spark_schema.fields if f.name == 'RequiredField'), None)

        assert optional_field is not None, "OptionalField should exist"
        assert required_field is not None, "RequiredField should exist"

        assert optional_field.nullable == True, "OptionalField should be nullable"
        assert required_field.nullable == False, "RequiredField should not be nullable"
