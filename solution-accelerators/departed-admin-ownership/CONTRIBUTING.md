# Contributing to Departed Admin Ownership Transfer

Thank you for your interest in contributing!

## Getting Started

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) for dependency management
- A Databricks account with an account-admin service principal for end-to-end testing

### Development Setup

```bash
cd solution-accelerators/departed-admin-ownership
uv sync --all-extras
```

### Running Tests

```bash
uv run pytest -v
```

The unit tests validate the notebook structure and the pure helper logic (path
normalization, active-job flagging, depth limiting) without needing a live workspace.

## Making Changes

1. Create a branch: `departed-admin-ownership/<feature>` (see the repo's
   `docs/branching-strategy.md`).
2. Make your changes within this project directory.
3. Run tests and `pre-commit run --all-files`.
4. Open a PR.

## Code Style

- Follow PEP 8; use type hints where practical.
- Keep the notebook and any library code in sync — the tests check both.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
