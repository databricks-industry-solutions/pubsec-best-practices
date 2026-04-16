# Monorepo Best Practices for Data Pipeline Teams

A guide for organizing shared code in a single GitHub repository (monorepo) for teams building data pipelines on Databricks using R, Python, SQL, and shell scripts.

---

## Why Not a Folder Per Person?

It's natural to think "my folder, my code" — but it creates problems fast:

- **Duplication** — Two people write the same utility function in their own folders and neither knows about the other
- **No shared context** — Looking at the repo, you can't tell what the pipeline does, only who works here
- **Handoff problems** — When someone leaves or changes teams, their folder becomes a black box
- **No clear ownership of pipelines** — A pipeline that spans multiple people's folders has no single home

A monorepo should be organized around **what the code does**, not **who wrote it**. Git already tracks who wrote what via commit history — you don't need folders for that.

---

## Recommended Structure

Organize by pipeline first, then by medallion layer within each pipeline. This keeps each pipeline self-contained so teams or individuals can own a pipeline without stepping on each other.

```
my-project/
├── README.md                        # What this repo is, how to get started
├── databricks.yml                   # DABs main bundle config
├── .github/
│   └── CODEOWNERS                   # Who reviews what (replaces "folder per person")
│
├── resources/                       # DABs resource definitions (jobs, pipelines)
│   ├── pipeline_a_job.yml
│   ├── pipeline_b_job.yml
│   └── pipeline_c_job.yml
│
├── pipelines/                       # Data transformation pipelines
│   ├── pipeline_a/                  # Self-contained pipeline
│   │   ├── bronze/
│   │   │   ├── notebooks/
│   │   │   └── sql/
│   │   ├── silver/
│   │   │   ├── notebooks/
│   │   │   └── sql/
│   │   ├── gold/
│   │   │   ├── notebooks/
│   │   │   └── sql/
│   │   └── README.md
│   ├── pipeline_b/
│   │   ├── bronze/
│   │   ├── silver/
│   │   ├── gold/
│   │   └── README.md
│   └── pipeline_c/
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       └── README.md
│
├── shared/                          # Reusable code across pipelines
│   ├── utils/                       # Common functions (R, Python)
│   ├── sql_templates/               # Reusable SQL patterns
│   └── init_scripts/                # Cluster init scripts
│
├── config/                          # Environment and pipeline config
│   ├── dev.yml
│   ├── staging.yml
│   └── prod.yml
│
└── tests/                           # Tests organized to mirror pipelines
    ├── pipeline_a/
    ├── pipeline_b/
    └── pipeline_c/
```

### Why this layout works

- **Pipeline-first** — Each pipeline is self-contained. A team of two can own pipeline_a without worrying about pipeline_c
- **New team members** can look at the folder names and understand what exists without reading any code
- **Shared code has a home** — Common functions go in `shared/`, not copy-pasted across pipelines
- **DABs-ready** — Resource definitions in `resources/` point at notebook paths in `pipelines/`, and `databricks.yml` ties it all together for deployment
- **Scales naturally** — Adding a new pipeline is just adding a new folder under `pipelines/`

### DABs Integration

Each pipeline's job is defined in `resources/` and references the notebook paths:

```yaml
# resources/pipeline_a_job.yml
resources:
  jobs:
    pipeline_a:
      name: "Pipeline A"
      tasks:
        - task_key: bronze
          notebook_task:
            notebook_path: pipelines/pipeline_a/bronze/notebooks/01_ingest.r
        - task_key: silver
          depends_on:
            - task_key: bronze
          notebook_task:
            notebook_path: pipelines/pipeline_a/silver/notebooks/01_transform.r
        - task_key: gold
          depends_on:
            - task_key: silver
          notebook_task:
            notebook_path: pipelines/pipeline_a/gold/notebooks/01_aggregate.r
```

DABs handles deployment across environments (dev/staging/prod) using the config files, so the same pipeline code can target different clusters or tables per environment.

---

## Key Concepts for Teams New to Git

### Branches, Not Folders, Separate Work

In a "folder per person" model, you avoid stepping on each other by staying in your own folder. In Git, you avoid stepping on each other by working on **branches**.

```
main              ← The stable, reviewed version of everything
  └── feature/add-data-parser       ← Sarah's work in progress
  └── feature/update-bronze-schema  ← Bob's work in progress
```

Each person works on their own branch, then merges into `main` through a pull request (PR) that gets reviewed. This is how every modern software team collaborates.

### CODEOWNERS Replaces Folder Ownership

GitHub has a built-in feature called CODEOWNERS that assigns reviewers based on file paths. This gives you ownership without personal folders:

```
# .github/CODEOWNERS

# Sarah and Bob own pipelines A and B
/pipelines/pipeline_a/    @sarah @bob
/pipelines/pipeline_b/    @sarah @bob

# Lisa owns pipeline C
/pipelines/pipeline_c/    @lisa

# The whole team reviews shared utilities
/shared/                  @team
```

When someone opens a PR that touches files in `/pipelines/pipeline_a/`, GitHub automatically adds Sarah and Bob as reviewers. Ownership is clear, but the code is organized by pipeline, not by person.

### Commit Messages Matter

Git tracks every change with a commit message. Good messages make the repo self-documenting:

```
Bad:  "updated code"
Bad:  "fixed stuff"
Good: "Add data parser for bronze ingestion"
Good: "Fix model threshold to reduce false positives"
```

A simple convention: start with a verb (Add, Fix, Update, Remove) and say what changed and why.

---

## Naming Conventions

Consistency matters more than any specific convention. Pick one and stick with it.

### Files

```
# Notebooks — descriptive, prefixed with order number if sequential
01_load_raw_data.r
02_parse_and_validate.r
03_enrich_and_classify.r

# SQL — match the table or transformation they produce
create_bronze_raw_events.sql
transform_silver_sessions.sql
aggregate_gold_summary.sql

# Shell scripts — describe what they do
init_cluster_packages.sh
export_results_to_s3.sh
```

### Branches

```
feature/add-data-parser
fix/bronze-timestamp-parsing
update/model-v2
```

---

## Working with Databricks

### Syncing Notebooks

Databricks notebooks can be synced to/from GitHub using Repos or the CLI. The key decisions:

**Databricks Repos (Git integration)** — Clones the repo directly into the Databricks workspace. Users edit in the workspace and commit/push from there. Good for teams that prefer working in the Databricks UI.

**CLI deployment** — Notebooks are developed locally or in Databricks, and deployed via the Databricks CLI. Good for CI/CD pipelines where you want controlled deployments.

```bash
# Deploy a notebook from the repo to the workspace
databricks workspace import \
  /Workspace/Users/team/my-project/pipelines/pipeline_a/bronze/01_ingest.r \
  --file pipelines/pipeline_a/bronze/notebooks/01_ingest.r \
  --language R --format SOURCE --overwrite
```

### Notebook Format

Databricks source format notebooks use `# COMMAND ----------` as cell separators and `# MAGIC %md` for markdown cells. These are plain text files that work well in Git — you get meaningful diffs and code review on PRs.

### What Goes in the Repo vs. What Stays in Databricks

| In the repo | In Databricks only |
|-------------|-------------------|
| Notebook source code | Cluster configurations |
| SQL scripts | Job schedules |
| Init scripts | Secrets and credentials |
| Config files | Ephemeral scratch notebooks |
| Shared utilities | Personal exploratory work |

Personal scratch work and exploration notebooks don't need to be in the repo. The repo is for code that's part of the pipeline and needs to be reviewed, versioned, and shared.

---

## Pull Request Workflow

This is the core workflow for collaborating on shared code:

1. **Create a branch** from `main` for your work
   ```bash
   git checkout -b feature/add-data-parser
   ```

2. **Make your changes** — edit notebooks, add SQL, update scripts

3. **Commit your changes** with a clear message
   ```bash
   git add pipelines/pipeline_a/bronze/notebooks/01_parse_data.r
   git commit -m "Add data parser for bronze ingestion"
   ```

4. **Push your branch** to GitHub
   ```bash
   git push -u origin feature/add-data-parser
   ```

5. **Open a pull request** on GitHub — describe what you changed and why

6. **Get a review** — CODEOWNERS are automatically assigned. They review the code, leave comments, and approve.

7. **Merge to main** — Once approved, merge the PR. The code is now part of the stable pipeline.

This process ensures no one's changes break someone else's work, and every change is reviewed before it goes into the shared codebase.

---

## Common Mistakes to Avoid

**Committing data files** — Don't put CSVs, Parquet files, or large datasets in the repo. Data lives in the lakehouse (Delta tables, volumes), not in Git. Add a `.gitignore` to prevent accidental commits:

```
# .gitignore
*.csv
*.parquet
*.xlsx
*.rds
.Rhistory
.RData
__pycache__/
.DS_Store
```

**Committing secrets** — Never commit passwords, tokens, or connection strings. Use Databricks secrets or environment variables instead.

**Long-lived branches** — Branches should be short-lived (days, not weeks). The longer a branch lives, the harder it is to merge back. Small, frequent PRs are easier to review and less likely to conflict.

**Skipping code review** — Even if you're confident in the change, having a second pair of eyes catches mistakes and spreads knowledge across the team. It also means more than one person understands each part of the pipeline.
