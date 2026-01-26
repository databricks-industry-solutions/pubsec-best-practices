"""
Pytest configuration and shared fixtures for xsd-to-synthetic tests.
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import xmlschema
from pyspark.sql import SparkSession
from pyspark.sql.types import *


# =============================================================================
# Schema Paths Configuration
# =============================================================================

def _get_schema_base():
    """Get schema base path from environment or default location."""
    import os
    
    # Check environment variable first
    env_path = os.environ.get('XSD_SCHEMA_PATH')
    if env_path and Path(env_path).exists():
        return Path(env_path)
    
    # Fall back to common download locations
    default_paths = [
        Path.home() / "Downloads" / "990T Schema 2024 v5.0" / "2024v5.0",
        Path.home() / "Downloads" / "irs-schemas" / "2024v5.0",
        Path("schemas") / "2024v5.0",
    ]
    
    for path in default_paths:
        if path.exists():
            return path
    
    return None

# Path to XSD schemas - tests skip gracefully if not available
SCHEMA_990T_2024_V5_BASE = _get_schema_base()


# =============================================================================
# Spark Session Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("XSDSyntheticTests") \
        .master("local[*]") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Suppress Spark logging
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()


# =============================================================================
# XSD Schema Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def schema_990T_v5():
    """Load the IRS 990T XSD schema from the 2024 v5.0 download."""
    if SCHEMA_990T_2024_V5_BASE is None:
        pytest.skip("XSD schemas not found. Set XSD_SCHEMA_PATH environment variable.")
    
    schema_path = SCHEMA_990T_2024_V5_BASE / "TEGE" / "TEGE990T" / "IRS990T" / "IRS990T.xsd"

    if not schema_path.exists():
        pytest.skip(f"990T v5.0 schema not found at {schema_path}")

    schema = xmlschema.XMLSchema(
        str(schema_path),
        validation='skip'
    )

    return schema


@pytest.fixture(scope="session")
def efile_types_schema():
    """Load the efileTypes.xsd base types schema."""
    if SCHEMA_990T_2024_V5_BASE is None:
        pytest.skip("XSD schemas not found. Set XSD_SCHEMA_PATH environment variable.")
    
    schema_path = SCHEMA_990T_2024_V5_BASE / "Common" / "efileTypes.xsd"

    if not schema_path.exists():
        pytest.skip(f"efileTypes.xsd not found at {schema_path}")

    schema = xmlschema.XMLSchema(
        str(schema_path),
        validation='skip'
    )

    return schema


@pytest.fixture(scope="session")
def schema_990T():
    """Load the IRS 990T XSD schema (alias for schema_990T_v5)."""
    if SCHEMA_990T_2024_V5_BASE is None:
        pytest.skip("XSD schemas not found. Set XSD_SCHEMA_PATH environment variable.")
    
    schema_path = SCHEMA_990T_2024_V5_BASE / "TEGE" / "TEGE990T" / "IRS990T" / "IRS990T.xsd"

    if not schema_path.exists():
        pytest.skip(f"990T schema not found at {schema_path}")

    schema = xmlschema.XMLSchema(
        str(schema_path),
        validation='skip'
    )

    return schema


# =============================================================================
# Utility Fixtures
# =============================================================================

@pytest.fixture
def temp_xsd(tmp_path):
    """Create a temporary XSD file for testing."""
    def _create_xsd(xsd_content: str) -> str:
        """Create a temporary XSD file with the given content."""
        xsd_file = tmp_path / "test.xsd"
        xsd_file.write_text(xsd_content)
        return str(xsd_file)

    return _create_xsd


# =============================================================================
# Notebook Classes Fixtures
# =============================================================================

def _is_executable_code(source: str) -> bool:
    """Check if code cell is executable Python (not Jupyter magic)."""
    stripped = source.strip()
    # Skip cells that start with Jupyter magics
    if stripped.startswith('%') or stripped.startswith('!'):
        return False
    # Skip cells that are primarily magic commands
    if stripped.startswith('# %') or stripped.startswith('#%'):
        return False
    # Skip cells that contain display() or show() as main action (demo cells)
    lines = [l.strip() for l in stripped.split('\n') if l.strip() and not l.strip().startswith('#')]
    if lines and all(l.startswith('display(') or l.startswith('df.show(') or l.startswith('print(') for l in lines):
        return False
    return True


def _clean_notebook_code(source: str) -> str:
    """Clean notebook code by removing or commenting out non-Python syntax."""
    lines = source.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        # Skip magic commands
        if stripped.startswith('%') or stripped.startswith('!'):
            continue
        # Skip dbutils commands (Databricks-specific)
        if 'dbutils.' in stripped:
            continue
        cleaned_lines.append(line)
    return '\n'.join(cleaned_lines)


@pytest.fixture(scope="session")
def notebook_classes():
    """
    Import classes from the notebook.

    This is a temporary solution. In production, these should be extracted
    to a proper Python package.
    """
    import json

    notebook_path = "notebooks/XSD to Synthetic - Library.ipynb"

    with open(notebook_path, 'r') as f:
        notebook = json.load(f)

    # Extract code cells, filtering out magic commands and demo cells
    code_cells = []
    for cell in notebook['cells']:
        if cell.get('cell_type') == 'code':
            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
            if _is_executable_code(source):
                cleaned = _clean_notebook_code(source)
                if cleaned.strip():
                    code_cells.append(cleaned)

    # Execute the code in a namespace
    namespace = {}
    full_code = '\n\n'.join(code_cells)

    # Execute the notebook code
    exec(full_code, namespace)

    # Return all the classes we need including strategy pattern classes
    return {
        # Core data classes
        'TypeConstraints': namespace['TypeConstraints'],
        'FieldHint': namespace['FieldHint'],
        'SchemaField': namespace['SchemaField'],
        'FlattenMode': namespace['FlattenMode'],
        # Core processing classes
        'TypeMapper': namespace['TypeMapper'],
        'ConstraintExtractor': namespace['ConstraintExtractor'],
        'SparkSchemaBuilder': namespace['SparkSchemaBuilder'],
        'SyntheticDataGenerator': namespace['SyntheticDataGenerator'],
        # XSD loading
        'XSDLoader': namespace.get('XSDLoader'),
        # Strategy pattern classes (if available)
        'FieldConfigurationStrategy': namespace.get('FieldConfigurationStrategy'),
        'StrategyContext': namespace.get('StrategyContext'),
        'EnumerationStrategy': namespace.get('EnumerationStrategy'),
        'XMLSchemaSampleStrategy': namespace.get('XMLSchemaSampleStrategy'),
        'NumericBoundsStrategy': namespace.get('NumericBoundsStrategy'),
        'DateTimeStrategy': namespace.get('DateTimeStrategy'),
        'HintBasedStrategy': namespace.get('HintBasedStrategy'),
        'PatternStrategy': namespace.get('PatternStrategy'),
        'DefaultStringStrategy': namespace.get('DefaultStringStrategy'),
        # Databricks integration (if available)
        'DatabricksRuntime': namespace.get('DatabricksRuntime'),
        'UnityCatalogWriter': namespace.get('UnityCatalogWriter'),
        # Configuration class (if available)
        'XSDSyntheticConfig': namespace.get('XSDSyntheticConfig'),
    }


@pytest.fixture(scope="session")
def type_mapper(notebook_classes):
    """Get the TypeMapper class from the notebook."""
    return notebook_classes['TypeMapper']


@pytest.fixture(scope="session")
def constraint_extractor(notebook_classes):
    """Get the ConstraintExtractor class from the notebook."""
    return notebook_classes['ConstraintExtractor']


@pytest.fixture
def sample_constraints(notebook_classes):
    """Factory fixture for creating TypeConstraints with defaults."""
    TypeConstraints = notebook_classes['TypeConstraints']

    def _create_constraints(**kwargs):
        defaults = {
            'base_type': 'string',
            'spark_type': StringType(),
        }
        defaults.update(kwargs)
        return TypeConstraints(**defaults)

    return _create_constraints
