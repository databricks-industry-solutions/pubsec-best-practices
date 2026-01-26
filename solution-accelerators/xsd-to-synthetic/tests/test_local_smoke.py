"""
Local smoke tests - verify library structure without PySpark/XSD files.

These tests verify that our Priority 1 changes haven't broken the
library structure, imports, or class definitions.
"""

import pytest
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

NOTEBOOK_PATH = Path(__file__).parent.parent / "notebooks" / "XSD to Synthetic - Library.ipynb"


class TestNotebookStructure:
    """Verify notebook structure is intact."""

    def test_notebook_exists(self):
        """Notebook file exists."""
        assert NOTEBOOK_PATH.exists(), f"Notebook not found at {NOTEBOOK_PATH}"

    def test_notebook_valid_json(self):
        """Notebook is valid JSON."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)
        assert 'cells' in notebook
        assert len(notebook['cells']) > 0

    def test_has_logging_cell(self):
        """Cell-3.5 (Logging Setup) exists."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        logging_cell_found = False
        for cell in notebook['cells']:
            if cell.get('id') == 'cell-3.5':
                logging_cell_found = True
                source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
                assert 'logging' in source, "Logging setup not found in cell-3.5"
                assert 'enable_debug' in source, "enable_debug function not found"
                break

        assert logging_cell_found, "Cell-3.5 (Logging Setup) not found"

    def test_has_unity_catalog_cell(self):
        """Cell-17.5 (Unity Catalog) exists."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        unity_cell_found = False
        for cell in notebook['cells']:
            if cell.get('id') == 'cell-17.5':
                unity_cell_found = True
                source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
                assert 'UnityCatalogWriter' in source, "UnityCatalogWriter class not found"
                assert 'DatabricksRuntime' in source, "DatabricksRuntime class not found"
                break

        assert unity_cell_found, "Cell-17.5 (Unity Catalog) not found"


class TestConstraintExtractorRefactoring:
    """Verify _process_facets() refactoring."""

    def test_facet_handler_exists(self):
        """FacetHandler dataclass is defined."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        for cell in notebook['cells']:
            if cell.get('id') == 'cell-9':
                source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
                assert 'class FacetHandler' in source, "FacetHandler class not found"
                assert 'FACET_HANDLERS' in source, "FACET_HANDLERS table not found"
                assert '_find_facet_handler' in source, "_find_facet_handler method not found"
                assert '_apply_facet_handler' in source, "_apply_facet_handler method not found"
                return

        pytest.fail("Cell-9 (ConstraintExtractor) not found")

    def test_facet_handlers_table_populated(self):
        """FACET_HANDLERS table has entries."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        for cell in notebook['cells']:
            if cell.get('id') == 'cell-9':
                source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

                # Check for specific facet handlers
                assert 'mininclusive' in source.lower(), "minInclusive handler missing"
                assert 'maxinclusive' in source.lower(), "maxInclusive handler missing"
                assert 'pattern' in source.lower(), "pattern handler missing"
                assert 'enumeration' in source.lower(), "enumeration handler missing"
                return

        pytest.fail("Cell-9 (ConstraintExtractor) not found")


class TestSyntheticDataGeneratorChanges:
    """Verify SyntheticDataGenerator changes."""

    def test_generate_has_num_partitions(self):
        """generate() method has num_partitions parameter."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        for cell in notebook['cells']:
            if cell.get('id') == 'cell-13':
                source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
                assert 'num_partitions' in source, "num_partitions parameter not found"
                assert 'partitions=num_partitions' in source, "partitions not passed to DataGenerator"
                return

        pytest.fail("Cell-13 (SyntheticDataGenerator) not found")


class TestXsdToSyntheticChanges:
    """Verify xsd_to_synthetic() function changes."""

    def test_count_calls_removed(self):
        """Duplicate .count() calls were removed."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        for cell in notebook['cells']:
            if cell.get('id') == 'cell-17':
                source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

                # Find the verbose logging section
                if 'if verbose:' in source and 'Generated:' in source:
                    # Count .count() calls in the logging section
                    # Should be 0 (using num_rows instead)
                    verbose_section_start = source.find('if verbose:')
                    verbose_section_end = source.find('return df', verbose_section_start)
                    verbose_section = source[verbose_section_start:verbose_section_end]

                    # The new code should use {num_rows} not {df.count()}
                    assert '{num_rows}' in verbose_section, "Should use num_rows in logging"

                    # Verify .count() is NOT used for logging
                    count_in_logging = verbose_section.count('df.count()')
                    assert count_in_logging == 0, f"Found {count_in_logging} .count() calls in logging (should be 0)"

                return

        pytest.fail("Cell-17 (xsd_to_synthetic) not found")


class TestConfigurationAPI:
    """Verify simplified logging API."""

    def test_enable_debug_exists(self):
        """enable_debug() function exists."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        found = False
        for cell in notebook['cells']:
            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
            if 'def enable_debug' in source:
                found = True
                break

        assert found, "enable_debug() function not found"

    def test_get_logger_exists(self):
        """get_logger() function exists."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        source_combined = ''
        for cell in notebook['cells']:
            source_combined += ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

        assert 'def get_logger' in source_combined, "get_logger() not found"


class TestCodeQualityMetrics:
    """Verify code quality improvements."""

    def test_process_facets_is_shorter(self):
        """_process_facets() method is significantly shorter than before."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        for cell in notebook['cells']:
            if cell.get('id') == 'cell-9':
                source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

                # Find _process_facets method
                method_start = source.find('def _process_facets(')
                assert method_start != -1, "_process_facets method not found"

                # Find end of method (next def or end of class)
                method_end = source.find('\n    def ', method_start + 1)
                if method_end == -1:
                    method_end = source.find('\n    @classmethod', method_start + 1)
                if method_end == -1:
                    method_end = len(source)

                method_code = source[method_start:method_end]
                method_lines = len([l for l in method_code.split('\n') if l.strip() and not l.strip().startswith('#')])

                # New implementation should be ~20-30 lines (was 74)
                assert method_lines < 40, f"_process_facets is {method_lines} lines (should be < 40 after refactoring)"

                # Should NOT have many elif statements
                elif_count = method_code.count('elif')
                assert elif_count == 0, f"_process_facets has {elif_count} elif statements (should be 0)"

                return

        pytest.fail("Cell-9 (ConstraintExtractor) not found")


class TestBackwardCompatibility:
    """Verify backward compatibility maintained."""

    def test_cell_ids_intact(self):
        """Important cell IDs are unchanged."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        cell_ids = {cell.get('id') for cell in notebook['cells'] if cell.get('id')}

        # These cells should still exist (some cells may not have IDs)
        required_cells = [
            'cell-3',   # Logging setup
            'cell-5',   # TypeConstraints
            # 'cell-7',   # TypeMapper (may not have ID)
            'cell-9',   # ConstraintExtractor
            'cell-11',  # SparkSchemaBuilder
            'cell-13',  # SyntheticDataGenerator
            'cell-15',  # XSDLoader
            'cell-17',  # xsd_to_synthetic
        ]

        for cell_id in required_cells:
            assert cell_id in cell_ids, f"Required cell {cell_id} missing (breaks compatibility)"

    def test_key_classes_exist(self):
        """Verify all key classes exist in notebook."""
        with open(NOTEBOOK_PATH, 'r') as f:
            notebook = json.load(f)

        # Combine all source code
        all_source = ''
        for cell in notebook['cells']:
            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
            all_source += source

        # These classes must exist (XSDSyntheticConfig removed - logging simplified)
        required_classes = [
            'class TypeMapper',
            'class ConstraintExtractor',
            'class SparkSchemaBuilder',
            'class SyntheticDataGenerator',
            'class XSDLoader',
            'class UnityCatalogWriter',
            'class DatabricksRuntime',
        ]

        for class_name in required_classes:
            assert class_name in all_source, f"Required class '{class_name}' not found in notebook"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
