# XSD to Synthetic Data Framework

A robust, production-ready library for generating realistic synthetic data from any XSD schema using Apache Spark.

## Features

- **Automatic Type Inference**: Maps XSD types to Spark types using recursive base type resolution
- **Constraint-Aware Generation**: Respects XSD enumerations, ranges, patterns, and length constraints
- **Pattern-Based Generation**: Generates data matching XSD regex patterns (e.g., EIN format `[0-9]{2}-[0-9]{7}`)
- **Realistic Data**: Uses Faker for realistic names, addresses, and descriptions
- **Flexible Schema Handling**: Choose between flat or nested output structures
- **Case-Insensitive Duplicate Handling**: Automatically renames duplicate fields (e.g., `Section529aInd` vs `Section529AInd`)
- **Production-Ready**: Comprehensive error handling, logging, and validation
- **Well-Tested**: 239 tests passing, validated against IRS Form 990T schema

## Quick Start

```python
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("XSD to Synthetic").getOrCreate()

# Generate 1000 rows of synthetic data from an XSD schema
df = xsd_to_synthetic(
    spark,
    xsd_path="IRS990.xsd",
    root_element="IRS990",
    include_paths=["efileTypes.xsd"],
    num_rows=1000,
    flatten_mode="full"
)

# Use the data
df.show()
df.write.parquet("synthetic_data.parquet")
```

## Installation

### Requirements

```bash
pip install pyspark xmlschema dbldatagen faker
```

### XSD Schema Setup

This framework requires XSD schema files to define the data structure. You must provide your own XSD files.

**For IRS Forms:**
1. Download IRS e-file schemas from [IRS MeF Schema Downloads](https://www.irs.gov/e-file-providers/modernized-e-file-schemas-and-business-rules)
2. Extract to a directory (e.g., `~/Downloads/irs-schemas/`)

**Configuration:**

Set the schema path via environment variable:
```bash
# For local development
export LOCAL_XSD_PATH=/path/to/your/schemas

# For tests
export XSD_SCHEMA_PATH=/path/to/your/schemas
```

Or configure in the notebook directly via the `volume_base` parameter for Databricks.

## Usage Examples

### Basic Usage
```python
# Simple one-liner
df = xsd_to_synthetic(spark, "schema.xsd", root_element="MyElement", num_rows=1000)
```

### Advanced Usage with Customization
```python
# Step-by-step for full control
loader = XSDLoader()
schema = loader.load("schema.xsd", include_paths=["types.xsd"])

builder = SparkSchemaBuilder(flatten_mode=FlattenMode.FULL)
spark_schema, constraints = builder.build(schema, "MyElement")

generator = SyntheticDataGenerator(spark)
df = generator.generate(
    spark_schema,
    constraints,
    num_rows=10000,
    null_probability=0.2,  # 20% null values
    field_hints={
        "CompanyName": FieldHint.BUSINESS_NAME,
        "CEO": FieldHint.PERSON_NAME
    }
)
```

### Nested vs Flat Schemas
```python
# Preserve nested structure
df_nested = xsd_to_synthetic(
    spark, "schema.xsd", "MyRoot",
    flatten_mode="none"
)

# Flatten everything to top level
df_flat = xsd_to_synthetic(
    spark, "schema.xsd", "MyRoot",
    flatten_mode="full"
)

# Flatten up to depth 3
df_partial = xsd_to_synthetic(
    spark, "schema.xsd", "MyRoot",
    flatten_mode="depth:3"
)
```

## Realistic Data Generation

The framework uses Faker to generate realistic text for free-form fields. XSD patterns and enumerations are respected directly from the schema.

| Field Pattern | Generated Data Example |
|---------------|----------------------|
| `*Email*` | `john.doe@example.org` |
| `*PersonNm` | `Jane Doe` |
| `*BusinessName` | `Community Foundation` |
| `*CityNm` | `San Francisco` |
| `*Address*` | `123 Main Street` |
| `*Desc*` | `Charitable activities for community benefit` |
| `*Explanation*` | `Detailed paragraph of explanation text...` |

*Note: Structured fields (EIN, ZIP, State codes) are generated from XSD patterns/enumerations.*

## Architecture

The framework consists of well-designed classes organized into three layers:

### Core Data Classes
1. **TypeConstraints**: Holds XSD facet restrictions (min, max, pattern, enumeration, etc.)
2. **SchemaField**: Represents a field with metadata and constraints
3. **FieldHint**: Semantic hints for realistic text (EMAIL, CITY, PERSON_NAME, etc.)
4. **FlattenMode**: Enum for schema flattening strategy

### Processing Classes
5. **TypeMapper**: Maps XSD types to Spark types via recursive base type resolution
6. **ConstraintExtractor**: Extracts XSD facets and restrictions
7. **SparkSchemaBuilder**: Builds Spark schema from XSD with duplicate field handling
8. **XSDLoader**: Loads XSD schemas using native xmlschema include resolution

### Data Generation (Strategy Pattern)
9. **SyntheticDataGenerator**: Orchestrates data generation using pluggable strategies:
   - **EnumerationStrategy**: Picks from enumerated values
   - **PatternStrategy**: Generates data matching XSD regex patterns
   - **XMLSchemaSampleStrategy**: Uses xmlschema's native sample generation
   - **NumericBoundsStrategy**: Respects min/max constraints
   - **DateTimeStrategy**: Generates dates within specified ranges
   - **HintBasedStrategy**: Uses field name patterns for realistic data
   - **DefaultStringStrategy**: Fallback with length constraints

## Test Results

Comprehensive test suite with **239 tests passing**:

| Test Category | Tests | Coverage |
|--------------|-------|----------|
| Type Mapping (efileTypes.xsd) | 180 | All 173 IRS types validated |
| Data Quality | 18 | Pattern, enum, length, numeric constraints |
| Schema Loading (990T) | 11 | Full schema parsing and generation |
| Constraint Extraction | 6 | Facet extraction from XSD |
| Text Generation | 6 | Field hints and realistic data |
| Type Priority | 4 | Constraint precedence logic |

## Configuration Options

### Flatten Modes
- **NONE**: Preserve nested StructTypes (good for hierarchical data)
- **FULL**: Flatten all levels with prefixed names (good for analytics)
- **DEPTH**: Flatten up to N levels (balance between structure and usability)

### Field Hints

Field hints add realism to free-form text fields. XSD handles structured data (patterns, enumerations) directly.

```python
from enum import Enum

class FieldHint(Enum):
    # Contact/Location
    EMAIL = "email"
    CITY = "city"
    ADDRESS = "address"
    URL = "url"
    # Names
    PERSON_NAME = "person_name"
    BUSINESS_NAME = "business_name"
    # Text content
    DESCRIPTION = "description"
    EXPLANATION = "explanation"
    TEXT = "text"
```

## Performance

| Rows | Generation Time (estimate) |
|------|----------------|
| 1,000 | ~5-10 seconds |
| 10,000 | ~20-30 seconds |
| 100,000 | ~2-3 minutes |

*Performance depends on schema complexity and Spark cluster configuration*

## Error Handling

The framework includes comprehensive error handling:
- Graceful degradation for malformed schemas
- Warning messages for duplicate fields (with automatic renaming)
- Fallback to string type for unparseable elements
- Type cache management to prevent cross-contamination
- Comprehensive logging (DEBUG/INFO/WARNING/ERROR levels)

## Example Output

For IRS Form 990:
```
+----------------+-------------+------------+------------------+
|EIN             |BusinessName |CityNm      |StateAbbreviationCd|
+----------------+-------------+------------+------------------+
|12-3456789      |Arts Council |Chicago     |IL                |
|24-5678901      |Food Bank    |Seattle     |WA                |
|35-4567890      |Museum Assoc |Boston      |MA                |
+----------------+-------------+------------+------------------+
```

## Security

- No code injection (uses xmlschema library safely)
- No SQL injection (uses DataFrame API)
- Only reads specified XSD files
- Generates synthetic data only (no real PII)

## Documentation

All classes and methods include comprehensive docstrings:

```python
help(SyntheticDataGenerator)
help(xsd_to_synthetic)
```

See also:
- `notebooks/README.md` - Databricks usage guide
- `notebooks/TECHNICAL_SUMMARY.md` - Detailed technical documentation
- `tests/README.md` - Test suite documentation

## Databricks Usage

### Setup

1. Upload the notebooks from `notebooks/` to your Databricks workspace
2. Upload your XSD schemas to a Unity Catalog volume
3. Configure the widgets in `XSD to Synthetic.ipynb`:
   - **Catalog**: Target Unity Catalog name
   - **Schema**: Target schema name  
   - **Table**: Output table name
   - **Form Type**: IRS form (990, 990T, 1120, or Custom)
   - **Volume Path**: Path to XSD files in Unity Catalog

### Running

1. Open `XSD to Synthetic.ipynb`
2. Run the library cell: `%run "./XSD to Synthetic - Library"`
3. Configure widgets and run all cells
4. Data is written to your specified Unity Catalog table

## Use Cases

This framework is suitable for:
- Generating test data for XSD-based systems
- Creating sample datasets for IRS forms
- Demonstrating XSD-to-Spark conversion
- Load testing with realistic synthetic data
- Educational and prototyping purposes

## License

See LICENSE file for details.

## Acknowledgments

Built with:
- [Apache Spark](https://spark.apache.org/) - Distributed data processing
- [xmlschema](https://pypi.org/project/xmlschema/) - XSD parsing
- [dbldatagen](https://databrickslabs.github.io/dbldatagen/) - Synthetic data generation
- [Faker](https://faker.readthedocs.io/) - Realistic fake data

---

**Status**: Production-Ready | **Last Updated**: 2026-01-20 | **Version**: 3.0
