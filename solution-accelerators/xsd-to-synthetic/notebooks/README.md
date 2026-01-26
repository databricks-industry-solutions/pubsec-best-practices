# XSD to Synthetic Data Framework

Generate realistic synthetic data from XSD schemas for IRS tax forms.

## Quick Start

### 1. Import the Library

In your Databricks notebook:

```python
%run "./XSD to Synthetic - Library"
```

### 2. Configure Widgets

The framework provides widgets for easy configuration:
- **Catalog**: Output catalog (default: `edp`)
- **Schema**: Output schema (default: `sandbox_bronze`)
- **Table**: Output table name
- **Form Type**: IRS form (990, 1040, 1120, Custom)
- **Number of Rows**: How many synthetic records to generate

### 3. Run the Notebook

Click "Run All" - the framework will:
1. Load the XSD schema
2. Build the Spark schema
3. Generate synthetic data
4. Write to Unity Catalog

## Features

### Comprehensive XSD Support

- [DONE] **Type Resolution**: Multi-layer strategy with IRS-specific types, recursive base_type resolution, and elementpath datetime support
- [DONE] **Amount Fields**: IRS amount types (USAmountType, etc.) correctly map to BIGINT
- [DONE] **Arrays**: Repeated elements (maxOccurs > 1) become `array<type>`
- [DONE] **Attributes**: XML attributes included as fields (documentId, softwareId, etc.)
- [DONE] **Constraints**: Pattern, enumeration, min/max values for realistic data
- [DONE] **Nullability**: Based on minOccurs (0 = nullable)
- [DONE] **Logging**: Comprehensive logging infrastructure for debugging type conversions and XSD traversal

### Data Generation

- **dbldatagen**: Generates realistic synthetic data
- **Faker Integration**: Names, addresses, emails, phone numbers
- **Constraint-Based**: Respects XSD facets (patterns, ranges, enumerations)
- **Configurable**: Amount ranges, date ranges, null probability

### Supported Forms

- IRS 990 (Return of Organization Exempt From Income Tax)
- IRS 990-EZ (Short Form)
- IRS 990-T (Exempt Organization Business Income Tax Return)
- IRS 1040 (U.S. Individual Income Tax Return)
- IRS 1120 (U.S. Corporation Income Tax Return)
- Custom XSD schemas

## Configuration Examples

### Generate 10,000 rows for Form 990

```python
dbutils.widgets.dropdown('form_type', '990', ['990', '1040', '1120', 'Custom'])
dbutils.widgets.text('num_rows', '10000')
```

### Custom Amount Ranges

```python
amount_ranges = {
    'Revenue|GrossReceipt': (1000000, 100000000),
    'Expense': (500000, 50000000),
    'default': (0, 1000000)
}
```

### Custom Date Range

```python
date_range = ('2023-01-01', '2023-12-31')  # Tax year 2023
```

### Enable Debug Logging

```python
# Enable detailed logging for troubleshooting
enable_debug_logging()

# Or log to a file
enable_debug_logging(log_file='xsd_processing.log')

# Disable logging
disable_logging()
```

The logging system provides:
- **WARNING**: Type conversion fallbacks and traversal issues (default)
- **ERROR**: Failed operations
- **INFO**: Summary statistics and pipeline progress
- **DEBUG**: Detailed trace of type mapping and schema building

## Architecture

### Library Notebook (Reusable Classes)

- **TypeMapper**: XSD → Spark type mapping (5-layer strategy)
- **ConstraintExtractor**: Extract XSD facets/constraints
- **SparkSchemaBuilder**: Build Spark schema from XSD (with duplicate field handling)
- **SyntheticDataGenerator**: Generate realistic data using strategy pattern
- **XSDLoader**: Load XSD files with native include resolution

### Data Generation Strategies

The `SyntheticDataGenerator` uses pluggable strategies:
- **EnumerationStrategy**: Picks from enumerated values
- **PatternStrategy**: Generates data matching XSD regex patterns
- **XMLSchemaSampleStrategy**: Uses xmlschema's native sample generation
- **NumericBoundsStrategy**: Respects min/max constraints
- **DateTimeStrategy**: Generates dates within specified ranges
- **HintBasedStrategy**: Uses field name patterns for realistic data
- **DefaultStringStrategy**: Fallback with length constraints

### Main Notebook (Configuration & Execution)

- Widget-based configuration
- Form presets (990, 1040, 1120)
- One-click execution
- Unity Catalog integration

## Technical Details

### Type Resolution (5-Layer Strategy)

1. **IRS Amount Types (Layer 1)**: Fast-path explicit mapping (USAmountType → LongType)
2. **IRS Common Types (Layer 1.5)**: IRS_TYPE_MAPPINGS for DateType, CheckboxType, DecimalType variants, YearType, TimeType, etc.
3. **Recursive base_type Resolution (Layer 2)**: Walks up to 5 levels of base_type chain for deep type hierarchies
4. **Direct python_type (Layer 3)**: For standard unrestricted types, includes elementpath datetime types (Date10, DateTime10, Time)
5. **Name-based Fallback (Layer 4)**: Safety net (fields with "Amt" → LongType)

### Why This Matters

IRS schemas have complex type hierarchies:

**Example 1 - Restricted Amount Type:**
```xml
<simpleType name="USAmountType">
  <restriction base="xs:integer">
    <totalDigits value="15"/>
  </restriction>
</simpleType>
```
- xmlschema's direct `python_type` returns `str` (misleading!)
- `base_type.python_type` correctly returns `int` → Spark's `LongType` (BIGINT)

**Example 2 - Deep Type Chain:**
```
USDecimalAmountType → DecimalType → xs:decimal
```
- Layer 2 recursively walks the chain to find the correct base type
- Maps to `DecimalType(17, 2)` via IRS_TYPE_MAPPINGS

**Example 3 - Date/Time Types:**
- efileTypes.xsd DateType uses `elementpath.Date10` internally
- Layer 1.5 maps directly to Spark's `DateType()`
- Layer 3 handles elementpath datetime types (Date10, DateTime10, Time)

## Expected Output Schema

For IRS 990-T, you'll see:

```
root
 |-- documentId: string (nullable = false)          # Attribute
 |-- softwareId: string (nullable = true)           # Attribute
 |-- softwareVersionNum: string (nullable = true)   # Attribute
 |-- BookValueAssetsEOYAmt: long (nullable = true)  # Amount field
 |-- TotalUBTIComputedAmt: long (nullable = true)   # Amount field
 |-- Contributors: array<struct<...>>               # Repeated element
 |-- ...
```

## Requirements

- **Databricks Runtime**: 13.3 LTS or higher
- **Python**: 3.10+
- **Libraries**:
  - `xmlschema` (XSD parsing)
  - `dbldatagen` (synthetic data generation)
  - `faker` (realistic fake data)

Install in Databricks:
```python
%pip install xmlschema dbldatagen faker
```

## Troubleshooting

### Amount fields showing as STRING instead of BIGINT

[DONE] **Fixed in this version!** Uses `base_type.python_type` for correct type resolution.

### Missing fields in schema

- Check if fields are XML attributes (now supported!)
- Check if elements are in choice groups (all options will appear)

### Schema validation errors

- Ensure XSD include paths are correct
- Check Volume path configuration
- Verify root element name matches XSD

## Support

For issues or questions, contact the EDP team.

## Version

**Version**: 2.9 (2026-01-20)

**Version 2.2 Improvements**:
- PatternStrategy for generating data matching XSD regex patterns
- Case-insensitive duplicate field handling in SparkSchemaBuilder
- DefaultStringStrategy improvements for max_length constraints
- 239 tests passing (comprehensive test suite)

**Version 2.1 Improvements** (2025-12-31):
- Comprehensive logging infrastructure (WARNING, ERROR, INFO, DEBUG levels)
- Enhanced 5-layer type resolution with IRS_TYPE_MAPPINGS
- Recursive base_type chain resolution for deep type hierarchies
- elementpath datetime type support (Date10, DateTime10, Time)
- Native xmlschema include handling (simplified XSDLoader)
- Correct amount type detection (base_type.python_type)
- Array support for repeated elements
- Attribute support (documentId, softwareId, etc.)
- Handles anonymous types and complex type chains
