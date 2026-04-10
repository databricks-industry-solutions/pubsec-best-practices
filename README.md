# Public Sector Best Practices

[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Serverless](https://img.shields.io/badge/Serverless-Compute-00C851?style=for-the-badge)](https://docs.databricks.com/en/compute/serverless.html)

A mono-repo for public sector solution accelerators and demos built on Databricks.

## Projects

### Solution Accelerators

| Project | Description |
|---------|-------------|
| [xsd-to-synthetic](solution-accelerators/xsd-to-synthetic/) | Generate synthetic data from XSD schemas using PySpark |

### Demos

| Project | Description |
|---------|-------------|
| [r-airgap-install](demos/r-airgap-install/) | Install R geospatial packages (`zipcodeR`, `sf`, `terra`, etc.) on an air-gapped Databricks cluster via UC Volume |

## Installation

1. Clone the project into your Databricks Workspace

2. Open the Asset Bundle Editor in the Databricks UI

3. Click "Deploy"

4. Navigate to the Deployments tab and click "Run" on the available job

See [docs/getting-started.md](docs/getting-started.md) for detailed setup instructions.

## Repository Structure

```
.
├── solution-accelerators/   # Production-ready accelerators (with infra)
├── demos/                   # Lightweight demos (no infra)
├── shared/                  # Shared code across projects
├── docs/                    # Repository documentation
└── .github/workflows/       # CI/CD with path-filtered triggers
```

## Documentation

- [Getting Started](docs/getting-started.md) - Setup and creating new projects
- [Project Structure](docs/project-structure.md) - Directory layout and conventions
- [Branching Strategy](docs/branching-strategy.md) - `projectname/feature` workflow

## Contributing

1. **git clone** this project locally
2. Create a branch: `<project-name>/<feature>`
3. Utilize the Databricks CLI to test your changes
4. Submit PRs with peer review from a capable teammate

See [docs/branching-strategy.md](docs/branching-strategy.md) for details.

## Third-Party Package Licenses

&copy; 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| Package | License | Copyright |
|---------|---------|-----------|
| Apache Spark | Apache 2.0 | Apache Software Foundation |
| xmlschema | MIT | Davide Brunato |
| dbldatagen | Databricks | Databricks, Inc. |
| Faker | MIT | Daniele Faraglia |
| ruff | MIT | Astral Software Inc. |
| pytest | MIT | Holger Krekel and others |
