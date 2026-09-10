# ---
# jupyter: jupytext (percent format). Open in VS Code / Jupyter and run cells,
# or read top-to-bottom as the demo script. Runs LOCAL tooling (terraform + the
# databricks provider exporter), not on a Databricks cluster.
# ---

# %% [markdown]
# # From a live Databricks account to plane-based Terraform
#
# **The problem.** IaC that is applied by hand drifts, and a lift-and-shift migration
# creates one remote Terraform workspace per resource group only to collapse them
# later. We want to see reality and land it directly in the target design — the five
# lifecycle planes (see README.md):
#
# | plane | what lives here | churn | blast radius |
# |---|---|---|---|
# | account-infra | MWS + metastore | very low | catastrophic |
# | identity | account groups / SPs | low | high (applies first) |
# | uc-foundation | storage creds, ext locations, metastore grants | low | high |
# | workspace-\<env\> | clusters, policies, warehouses | medium | workspace-local |
# | uc-governance-\<domain\> | catalogs, schemas, volumes, grants | **high** | data exposure |
#
# **The move.** The exporter tells us *what is really out there*. `plane_transform.py`
# decides *where each piece belongs* — and keeps every resource's native `import {}`
# block, so the first plan is **adopt, never create/destroy.**

# %%
import subprocess, sys, pathlib
HERE = pathlib.Path(__file__).resolve().parent

def sh(cmd, **kw):
    print(f"$ {cmd}")
    return subprocess.run(cmd, shell=True, cwd=HERE, text=True, **kw)

# %% [markdown]
# ## 1. Prereqs — terraform + the provider binary
# The exporter is a **subcommand of the databricks provider binary**, so we fetch
# the provider via a throwaway `terraform init` and read the binary out of the
# plugin cache.

# %%
sh("./00_prereqs.sh")

# %% [markdown]
# ## 2. Export the live account
# Reads account-admin SP creds from `.env`. Emits a flat dump of `resource` +
# native `import {}` blocks — the exporter's own addresses, not our module layout.
# (Offline demo? Skip this cell; a captured snapshot lives in `captured/exported/`.)

# %%
sh("./01_export.sh captured/exported")

# %% [markdown]
# ### What the exporter gives us: a flat pile
# Correct and complete, but everything at one level — grants next to clusters next
# to metastores. No lifecycle separation, no module structure.

# %%
sh("ls -1 captured/exported/*.tf 2>/dev/null | head; "
   "echo '--- sample ---'; head -40 captured/exported/*.tf 2>/dev/null | head -40")

# %% [markdown]
# ## 3. Classify + route into planes
# `plane_rules.yaml` maps each resource type to a plane (grants routed by the
# securable they target). `--dry-run` first: just the classification report.

# %%
sh("python3 plane_transform.py --exported-dir captured/exported "
   "--out-dir generated --dry-run")

# %% [markdown]
# ### Now write the tree

# %%
sh("python3 plane_transform.py --exported-dir captured/exported --out-dir generated")
sh("terraform fmt -recursive generated/databricks-terraform >/dev/null; echo fmt ok")

# %% [markdown]
# ### The result: reality, reorganized by lifecycle
# Each `environments/<root>/` is one remote Terraform workspace / one state, wired
# in the run-trigger order and carrying its import blocks.

# %%
sh("find generated/databricks-terraform -maxdepth 2 -type d | sort")
sh("echo '--- a plane root ---'; ls generated/databricks-terraform/environments/uc-governance-default/ 2>/dev/null")

# %% [markdown]
# ## 3b. (optional) Collapse to `for_each`
# Fold the flat one-block-per-resource output into idiomatic `for_each` maps —
# 138 catalog blocks become one `databricks_catalog.this` + a 138-entry map,
# with nested blocks handled via `dynamic`. The collapser rewrites both import
# targets and cross-references to the new addresses.
#
# **Footgun it encodes:** to adopt a `for_each` resource you need ONE import block
# with its own `for_each` (`to = ...this[each.key]`); N separate per-instance
# import blocks make Terraform honor only the first and *create* the rest.

# %%
sh("python3 collapse_foreach.py --tree generated/databricks-terraform")
sh("terraform fmt -recursive generated/databricks-terraform >/dev/null; echo fmt ok")
sh("sed -n '1,20p' generated/databricks-terraform/environments/uc-governance-default/databricks_catalog.tf")

# %% [markdown]
# ## 4. Prove it's adopt, not create
# In a real run you'd `terraform init` a plane root and speculative-plan it through
# your VCS-driven Terraform workflow. With the `import {}` blocks present, a correct
# plan shows **N to import, 0 to add, 0 to change, 0 to destroy** — the zero-destroy
# gate in `04_plan.sh` enforces exactly that.
#
# **Refinement pass (manual, labeled):** collapse repeated resources into
# `for_each` maps behind `module.*` and resolve groups by display name. Until then
# the layout is already plane-correct and the plan is clean.
