# Contributing to XSD to Synthetic

Thank you for your interest in contributing to XSD to Synthetic!

## Getting Started

### Prerequisites

- Python 3.10+
- Java 8+ (for PySpark)
- XSD schema files for testing

### Development Setup

```bash
# Clone the repository
git clone https://github.com/databricks/xsd-to-synthetic.git
cd xsd-to-synthetic

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"
```

### Running Tests

Tests require XSD schema files. Set the environment variable to your schema location:

```bash
export XSD_SCHEMA_PATH=/path/to/your/xsd/schemas

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_constraint_extraction.py -v
```

Tests will skip gracefully if schema files are not available.

## Making Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests to ensure nothing is broken
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Code Style

- Follow PEP 8 guidelines
- Use type hints where applicable
- Include docstrings for public functions/classes
- Keep functions focused and single-purpose

## Reporting Issues

When reporting issues, please include:

- Python version
- PySpark version
- XSD schema details (if applicable)
- Full error traceback
- Steps to reproduce

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
