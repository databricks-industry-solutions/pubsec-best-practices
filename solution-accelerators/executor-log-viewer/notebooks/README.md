# Executor Log Viewer — notebook edition

A single Databricks notebook that reads Spark **executor**/**driver**
`stdout`/`stderr` logs from **terminated job clusters**, delivered by Cluster
Log Delivery (CLD) to a Unity Catalog Volume.

This is the lightweight counterpart to the [app](../app/). Same job, far less
setup: **no app deploy, no service principal, no OBO scope, no signing secret.**
Import it, attach it to any cluster, and run.

## When to use the notebook vs. the app

| | Notebook | App ([../app/](../app/)) |
|---|---|---|
| Setup | Import + attach | Deploy app, SP, secret, OBO scope, allowlist |
| Identity | Runs **as you** — UC enforces access directly | Two-identity (SP metadata + user OBO content) |
| Best for | Ad-hoc / one-off debugging by an engineer | Shared, always-on team tool with a UI |
| Access control | Your own `READ VOLUME` grants | Per-user via OBO token |

Both use the **same** CLD-path rules, the same recognized-log-file boundary
(`stdout` / `stderr` / `*.log` / rotated variants — nothing else), and the same
outcome/reason codes.

## Prerequisites

- Your job clusters already deliver logs to a **UC Volume** (cluster/job →
  *Advanced → Logging → Destination = Volume*). This notebook reads what CLD
  delivered; it does not configure delivery.
- You have `USE CATALOG` / `USE SCHEMA` / `READ VOLUME` on that Volume.
- `databricks-sdk` — ships in every Databricks Runtime; nothing to install.

## Use it

1. **Import** `executor_log_viewer.py` into your workspace
   (*Workspace → Import → File*). It's a Databricks source-format notebook, so
   it imports as a runnable notebook, not a plain script.
2. **Attach** to any cluster (classic or serverless).
3. Set the **`cld_root_allowlist`** widget to your CLD Volume path(s),
   comma-separated — e.g. `/Volumes/<catalog>/<schema>/<cld_volume>`.
4. **Run All.** Then:
   - **Step 3** lists clusters you can see whose CLD delivers under your
     allowlisted root(s).
   - **Step 4** resolves a cluster / run / job ID (from the `lookup` widget, or
     the most-recent cluster from step 3).
   - **Step 5** shows the resolved log files with full paths and sizes.
   - **Step 6** reads one (tail = last 256 KB, or full up to 10 MB).

## Reason codes

`DELIVERY_PENDING` (CLD lag) · `NO_EXECUTOR_DIR` (all-purpose cluster) ·
`NO_CLD` (delivery not configured) · `CLD_ROOT_NOT_FOUND` (not in your
allowlist) · `NO_CLUSTER_INSTANCE` (serverless/pipeline — no executor logs) ·
`FILES_FORBIDDEN` (you lack `READ VOLUME`). See the last cell for the full
troubleshooting table.
