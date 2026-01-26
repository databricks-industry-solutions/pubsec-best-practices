# Getting Started

This mono-repo contains solution accelerators and demos for the team. Each project is self-contained with its own dependencies, tests, and deployment configuration.

## Prerequisites

- Python 3.12+ (managed via `.python-version`)
- [uv](https://docs.astral.sh/uv/) for Python dependency management
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) for asset bundle deployment
- [Terraform](https://www.terraform.io/) for infrastructure (solution accelerators only)
- [pre-commit](https://pre-commit.com/) for code quality hooks

## Setup

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd <repo-name>
   ```

2. Install pre-commit hooks:
   ```bash
   pip install pre-commit
   pre-commit install
   ```

3. Navigate to a project and set up its environment:
   ```bash
   cd solution-accelerators/xsd-to-synthetic
   uv sync
   ```

4. Run tests:
   ```bash
   uv run pytest
   ```

## Creating a New Project

### Solution Accelerator

1. Create the directory structure:
   ```bash
   mkdir -p solution-accelerators/<project-name>/{infra,asset-bundles/resources/{jobs,notebooks},src/<package_name>,tests,notebooks}
   ```

2. Copy configuration from an existing project or use the structure in `docs/project-structure.md`

3. Create a workflow file at `.github/workflows/solution-accelerators/<project-name>.yml`

### Demo

1. Create the directory structure:
   ```bash
   mkdir -p demos/<demo-name>/{asset-bundles/resources/{jobs,notebooks},src/<package_name>,tests,notebooks}
   ```

2. Use `demos/_template/` as a reference

3. Create a workflow file at `.github/workflows/demos/<demo-name>.yml`

## Working on a Project

Each project uses the branching convention `<project-name>/<feature>`:

```bash
git checkout -b xsd-to-synthetic/add-validation
# ... make changes ...
git push -u origin xsd-to-synthetic/add-validation
```

See `docs/branching-strategy.md` for full details.
