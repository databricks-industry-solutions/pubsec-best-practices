# XSD-to-Synthetic Test Suite

Comprehensive pytest-based test suite for the XSD-to-Synthetic framework. **239 tests passing**.

## Running Tests

### Prerequisites

```bash
# Ensure Java is installed (required for PySpark)
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

# Install dependencies
pip install pytest pyspark xmlschema dbldatagen faker jmespath numpy pandas pyparsing
```

### Run all tests
```bash
pytest
```

### Run specific test file
```bash
pytest tests/integration/test_efile_types.py
```

### Run specific test
```bash
pytest tests/integration/test_data_quality.py::TestDataQuality::test_ein_pattern_validation -v
```

### Run with verbose output
```bash
pytest -v
```

### Run with output capture disabled (see print statements)
```bash
pytest -s
```

## Test Structure

```
tests/
├── conftest.py                    # Shared fixtures
├── fixtures/
│   └── expected_type_mappings.py  # Expected XSD → Spark type mappings
├── integration/
│   ├── test_990T_schema.py        # Full schema loading & generation (11 tests)
│   ├── test_data_quality.py       # Data constraint validation (18 tests)
│   └── test_efile_types.py        # Type mapping validation (180 tests)
├── test_constraint_extraction.py  # Constraint extraction (6 tests)
├── test_local_smoke.py            # Quick sanity checks
├── test_text_generation.py        # Text/hint generation (6 tests)
└── test_type_aware_priority.py    # Type priority logic (4 tests)
```

## Test Categories

### Integration Tests (`integration/`)

#### `test_efile_types.py` (180 tests)
Validates type mapping and constraint extraction for all 173 types in `efileTypes.xsd`:
- Amount types → LongType/DecimalType
- Date/Time types → DateType/TimestampType
- Boolean/Checkbox types → BooleanType
- String types with patterns/enumerations
- Constraint extraction (patterns, enums, min/max, lengths)

#### `test_990T_schema.py` (11 tests)
Full IRS 990T schema parsing and data generation:
- Schema loading with includes
- Type resolution (>80% success rate)
- Spark schema building
- Synthetic data generation
- DataFrame validation

#### `test_data_quality.py` (18 tests)
Validates that generated data respects XSD constraints:
- Pattern matching (EIN format, ZIP codes)
- Numeric bounds (min/max values)
- Enumeration values
- String length constraints
- Date ranges
- Boolean generation
- Nullability

### Unit Tests

#### `test_constraint_extraction.py` (6 tests)
Tests for the `ConstraintExtractor` class:
- Pattern extraction from ID types
- Enumeration extraction from state codes
- Min/max value extraction
- Length constraint extraction

#### `test_type_aware_priority.py` (4 tests)
Tests for constraint priority logic:
- Numeric types prioritize min/max over pattern
- Enumeration always wins (highest priority)
- Real-world examples (latitude, percentage)

#### `test_text_generation.py` (6 tests)
Tests for text generation and field hints:
- Field hint pattern matching (EIN, email, phone)
- Faker provider usage for realistic text
- Max length constraint handling

## Fixtures

Common fixtures defined in `conftest.py`:

| Fixture | Scope | Description |
|---------|-------|-------------|
| `spark` | session | Local SparkSession for testing |
| `schema_990T_v5` | session | IRS 990T XSD schema (2024 v5.0) |
| `efile_types_schema` | session | efileTypes.xsd base types |
| `notebook_classes` | session | All classes from the library notebook |

### `notebook_classes` Fixture

Provides access to all library classes:
- `TypeConstraints`, `FieldHint`, `SchemaField`, `FlattenMode`
- `TypeMapper`, `ConstraintExtractor`, `SparkSchemaBuilder`
- `SyntheticDataGenerator`, `XSDLoader`
- Strategy classes: `EnumerationStrategy`, `PatternStrategy`, `NumericBoundsStrategy`, etc.

## Test Data

Tests use:
1. **Real schemas**: IRS 990T and efileTypes.xsd in `xsds/` directory
2. **Expected mappings**: `fixtures/expected_type_mappings.py` defines expected type conversions
3. **Generated data**: PySpark DataFrames validated against constraints

## Coverage Summary

| Category | Status | Tests |
|----------|--------|-------|
| Type mapping (all IRS types) | Complete | 180 |
| Constraint extraction | Complete | 6 |
| Data quality validation | Complete | 18 |
| Schema loading/building | Complete | 11 |
| Text generation/hints | Complete | 6 |
| Type priority logic | Complete | 4 |
| **Total** | | **239** |

## Adding New Tests

1. Place unit tests in `tests/`
2. Place integration tests in `tests/integration/`
3. Use fixtures from `conftest.py`
4. Follow naming convention: `test_*.py`

Example:
```python
def test_new_feature(spark, notebook_classes):
    """Test description."""
    TypeMapper = notebook_classes['TypeMapper']
    # ... test implementation
```

---

**Last Updated**: 2026-01-20
