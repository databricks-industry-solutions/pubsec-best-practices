# Project Structure

## Repository Layout

```
repo/
├── .github/workflows/          # CI/CD workflows
│   ├── _templates/             # Reusable workflow templates
│   ├── solution-accelerators/  # Per-project workflows
│   └── demos/                  # Per-demo workflows
│
├── solution-accelerators/      # Production-ready accelerators
│   └── <project>/
│
├── demos/                      # Lightweight demos
│   ├── _template/              # Template for new demos
│   └── <demo>/
│
├── shared/                     # Shared code across projects
│   └── python/
│
├── docs/                       # Mono-repo documentation
│
├── .python-version             # Default Python version
├── .pre-commit-config.yaml     # Code quality hooks
├── .gitignore
├── pyproject.toml              # Shared tool configuration
└── README.md
```

## Solution Accelerator Structure

Solution accelerators are production-ready projects with full infrastructure:

```
solution-accelerators/<project>/
├── infra/                      # Terraform infrastructure
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── environments/           # Per-environment tfvars
│       ├── dev.tfvars
│       ├── staging.tfvars
│       └── prod.tfvars
│
├── asset-bundles/              # Databricks Asset Bundles
│   ├── databricks.yml          # Bundle configuration
│   ├── resources/
│   │   ├── jobs/               # Job definitions
│   │   └── notebooks/          # Notebook references
│   └── variables.yml           # Environment variables
│
├── apps/                       # Databricks Apps
│
├── dashboards/                 # Lakeview dashboards (.lvdash.json)
│
├── src/
│   └── <package_name>/         # Python package
│       ├── __init__.py
│       └── ...
│
├── tests/                      # Unit and integration tests
│   ├── __init__.py
│   ├── conftest.py
│   └── test_*.py
│
├── notebooks/                  # Jupyter notebooks
│
├── scripts/                    # Utility scripts (cleanup, etc.)
│
├── pyproject.toml              # Project dependencies
├── env.example                 # Environment variables template
├── .python-version             # Override Python version (optional)
└── README.md                   # Project documentation
```

## Demo Structure

Demos are lighter-weight, without infrastructure management:

```
demos/<demo>/
├── asset-bundles/              # Databricks Asset Bundles
│   ├── databricks.yml
│   └── resources/
│       ├── jobs/
│       └── notebooks/
│
├── apps/                       # Databricks Apps
│
├── dashboards/                 # Lakeview dashboards
│
├── src/
│   └── <package_name>/
│
├── tests/
│
├── notebooks/
│
├── scripts/                    # Utility scripts
│
├── pyproject.toml
├── env.example
└── README.md
```

## Key Files

### `pyproject.toml`

Each project has its own `pyproject.toml` for dependencies. The root `pyproject.toml` only contains shared tool configuration (ruff, pytest defaults).

### `databricks.yml`

Defines Databricks Asset Bundle configuration:

```yaml
bundle:
  name: <project-name>

workspace:
  host: ${var.databricks_host}

variables:
  databricks_host:
    description: Databricks workspace URL

targets:
  dev:
    default: true
    variables:
      databricks_host: https://adb-xxx.azuredatabricks.net
```

### `.python-version`

The root file sets the default (3.12). Projects can override by creating their own `.python-version`.
