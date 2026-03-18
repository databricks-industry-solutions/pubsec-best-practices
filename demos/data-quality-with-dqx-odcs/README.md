# Data Quality Contracts Demo

A PySpark demo that shows how **[Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/)** data contracts and **[Databricks Labs DQX](https://github.com/databrickslabs/dqx)** can work together to automate data quality validation in a multi-task Databricks workflow.

## What It Demonstrates

| Concept | How it's shown |
|---|---|
| **Data Contracts (ODCS v3)** | A YAML contract defines schema, column-level quality rules, SLAs, and ownership for the `curated_orders` table |
| **Contract-driven DQX checks** | Quality rules are parsed from the contract at runtime and converted to DQX checks automatically — no hardcoded rules |
| **Data quarantine** | DQX splits data into `curated_orders_valid` (clean) and `dq_quarantine` (violations) |
| **SLA enforcement** | The pipeline raises an exception if the overall pass rate drops below the contract's 95% completeness threshold |
| **Databricks Asset Bundles** | The full demo — workflow, notebooks, and dashboard — is packaged as a DAB for repeatable deployment |
| **Lakeview dashboard** | A 3-page AI/BI dashboard visualizes the quality scorecard, per-check results, and run history |

## Tools & References

- **[Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/)** — open specification for defining data contracts in YAML (v3.0.0)
- **[Databricks Labs DQX](https://github.com/databrickslabs/dqx)** — open-source library for applying metadata-driven data quality checks in PySpark
- **[Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)** — DAB packages workflows, notebooks, and dashboards for CI/CD-style deployment
- **[Databricks Lakeview Dashboards](https://docs.databricks.com/en/dashboards/index.html)** — AI/BI dashboards for visualizing results

## Project Structure

```
data_quality_contracts/
├── databricks.yml                  # DAB bundle config — targets: dev, irs-demo
├── contracts/
│   └── orders_contract.yaml        # ODCS v3 data contract for curated_orders
├── notebooks/
│   ├── 01_generate_data.py         # Synthetic e-commerce data with injected quality issues
│   ├── 02_transform_data.py        # Join raw tables → curated_orders
│   ├── 03_validate_contract.py     # Parse ODCS contract → contract_rules table
│   ├── 04_dqx_quality_checks.py    # Run DQX checks derived from contract
│   └── 05_quality_report.py        # Scorecard, SLA check, per-region breakdowns
├── resources/
│   ├── dq_demo_job.yml             # DAB job resource (5-task workflow, serverless)
│   └── dq_dashboard.yml            # DAB Lakeview dashboard resource
└── dashboards/
    └── dq_report.lvdash.json       # Dashboard definition (3 pages)
```

## Data Flow

```
raw_customers ─┐
raw_products  ─┼──► curated_orders ──► DQX checks ──► curated_orders_valid
raw_orders    ─┘         ▲                   │
                         │                   └──► dq_quarantine
                  ODCS contract                        │
                  (orders_contract.yaml)         dq_results
                         │                            │
                  contract_rules table          dq_summary
                                                       │
                                               Lakeview dashboard
```

## Workflow Tasks

| Task | Description |
|---|---|
| `01_generate_data` | Creates ~1,000 synthetic orders with 12 intentional quality violations (nulls, duplicates, out-of-range values, invalid statuses) |
| `02_transform_data` | LEFT-joins raw tables into `curated_orders`, preserving quality issues |
| `03_validate_contract` | Reads the ODCS YAML contract (embedded in the notebook), extracts column-level rules, writes `contract_rules` table |
| `04_dqx_quality_checks` | Converts ODCS rules to DQX check metadata, runs `apply_checks_by_metadata_and_split`, writes valid/quarantine/results tables |
| `05_quality_report` | Generates a quality scorecard, checks SLA compliance (≥95% threshold), raises if SLA is breached |

## Tables Written

| Table | Description |
|---|---|
| `raw_customers` | Synthetic customer master data |
| `raw_products` | Synthetic product catalog |
| `raw_orders` | Synthetic order transactions (with injected quality issues) |
| `curated_orders` | Enriched orders joined with customer and product dimensions |
| `contract_rules` | Quality rules extracted from the ODCS contract |
| `curated_orders_valid` | Rows that passed all error-level DQX checks |
| `dq_quarantine` | Rows that failed at least one error-level check, with DQX `_error_*` annotation columns |
| `dq_results` | Per-check pass/fail counts and pass rates |
| `dq_summary` | Run-level scorecard written after each execution |

## ODCS → DQX Rule Mapping

The `03_validate_contract` notebook parses these ODCS rule types and `04_dqx_quality_checks` maps them to DQX functions:

| ODCS rule | DQX function | Key arguments |
|---|---|---|
| `not_null` | `is_not_null` | `column` |
| `not_null_and_not_empty` | `is_not_null_and_not_empty` | `column` |
| `unique` | `is_unique` | `columns` (list) |
| `is_in_range` | `is_in_range` | `column`, `min_limit`, `max_limit` |
| `value_in_set` | `is_in_list` | `column`, `allowed` |
| `regex_match` | `regex_match` | `column`, `regex` |

## Dashboard

The Lakeview dashboard has three pages:

| Page | Content |
|---|---|
| **Overview** | 6 KPI counters: Total Rows, Valid Rows, Quarantined Rows, Pass Rate %, SLA Met, Failing Checks |
| **Check Results** | Detail table of all checks + bar chart of pass rate per check (green = PASS, red = FAIL) |
| **Trends** | Historical table of all `dq_summary` runs |

## Deployment

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) v0.200+
- Authentication configured for the target workspace

### Deploy

```bash
# Authenticate
databricks auth login <workspace-url> --profile <profile-name>

# Validate the bundle
databricks bundle validate --target irs-demo

# Deploy (creates/updates the workflow and dashboard)
databricks bundle deploy --target irs-demo
```

### Run the workflow

```bash
databricks bundle run dq_demo_job --target irs-demo
```

Or trigger it from the Databricks UI.

### Add a new deployment target

Add a new target block to `databricks.yml`:

```yaml
targets:
  my-workspace:
    workspace:
      host: https://<your-workspace>.azuredatabricks.net
      profile: <your-profile>
    variables:
      catalog: <your-catalog>
      schema: dq_demo
      warehouse_id: <your-warehouse-id>
```

Then update the catalog/schema references in `dashboards/dq_report.lvdash.json` (DAB does not substitute variables inside `.lvdash.json` files).

## Design Notes

- **Self-contained contract** — the ODCS YAML is embedded as a Python string in `03_validate_contract.py` and written to `/tmp/orders_contract.yaml` at runtime. No DBFS or external file dependencies.
- **Serverless compute** — all tasks run on serverless; no cluster configuration required.
- **Protobuf pin** — DQX's transitive dependency on `protobuf` is pinned to `>=5.26.1,<6.0.0` to avoid conflicts with other pre-installed packages on the Databricks runtime.
