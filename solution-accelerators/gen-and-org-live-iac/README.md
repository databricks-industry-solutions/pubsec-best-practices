# Generate & Organize Live IaC

Turn a **live Databricks estate** into **plane-organized Terraform** — and adopt it
without creating or destroying anything.

The Databricks provider's experimental resource exporter answers *"what is really
deployed?"* as a flat HCL dump. This accelerator's `plane_transform.py` answers *"where
does each resource belong in a maintainable design?"* — routing every block into five
lifecycle planes and carrying its native `import {}` block, so the **first `terraform
plan` reads as adopt (import), never create/destroy**. It scales to large estates via a
manifest-driven, resumable chunked export, and ships bash **and** PowerShell 7 tooling.

```
live account + workspaces ──exporter──▶ flat HCL dump ──plane_transform──▶ 5 plane roots ──terraform plan──▶ N to import, 0 to destroy
                                                        └─collapse_foreach─▶ for_each maps
```

## Why planes

A flat export isn't a maintainable repo — one giant state is slow, risky to change, and
couples unrelated resources. This tool splits state on **lifecycle cadence × blast
radius** into five planes:

| Plane | What lives here | Churn | Blast radius |
|---|---|---|---|
| `account-infra` | Workspaces, networks, credentials, metastore wiring (MWS) | Very low | Catastrophic (keep `restrict_destroy` armed) |
| `identity` | Account groups, service principals, SCIM memberships | Low | High (shared everywhere); applies first |
| `uc-foundation` | Storage credentials, external locations, metastore-level grants | Low | High; one state per metastore |
| `workspace-<env>` | Clusters, pools, policies, warehouses, secrets | Medium | Workspace-local; one state per workspace |
| `uc-governance-<domain>` | Catalogs, schemas, volumes and their grants | High | Data-exposure; one state per data domain |

Classification lives entirely in [`plane_rules.yaml`](plane_rules.yaml) (resource type →
plane; `databricks_grants` routed by the securable it targets). A type not listed there is
never guessed — it lands in `environments/_unclassified/` for a human to place.

## Quick start

Prerequisites: `terraform` 1.5+, a Python 3.9+ with `PyYAML` (`pip install pyyaml`), and
either `bash` or PowerShell 7+ (pwsh).

```bash
# 1. No creds — rebuild the plane tree from the bundled example fixture:
./run.sh --offline --collapse
ls generated/databricks-terraform/environments/
#   → identity  uc-foundation  uc-governance-default  workspace-dev

# 2. Live, single scope — export one account (or workspace) and transform:
cp env.example .env         # set an account-admin SP (or a CLI profile)
./run.sh --collapse

# 3. Live, whole fleet — chunked, resumable fan-out (see RUNBOOK.md):
cp fleet.example.conf fleet.conf
./02_export_fleet.sh         # one chunk per line → captured/<name>/
./run.sh --offline --collapse
```

The bundled `captured/example/` is a **fabricated** fixture (no real data) so the offline
demo works out of the box. It transforms into 12 resources across four planes.

### Windows / PowerShell

Every orchestration script has a `.ps1` twin (the Python is shared and cross-platform), so
the whole workflow runs natively on **PowerShell 7+ (pwsh)** without WSL or git-bash.
Env-var knobs on bash become parameters on PowerShell:

| bash | PowerShell |
|------|------------|
| `./run.sh --offline --collapse` | `.\run.ps1 -Offline -Collapse` |
| `PROFILE=p SCOPE=workspace ./01_export.sh captured/ws` | `.\01_export.ps1 -ProfileName p -Scope workspace -OutDir captured/ws` |
| `./02_export_fleet.sh --only ws-prod --force` | `.\02_export_fleet.ps1 -Only ws-prod -Force` |
| `./04_plan.sh identity my-account-sp` | `.\04_plan.ps1 -Root identity -ProfileName my-account-sp` |

First run may need `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`.

## What's here

| File | Role |
|------|------|
| `00_prereqs.{sh,ps1}` | Fetch `terraform` + the Databricks provider binary (the exporter is a provider subcommand) |
| `01_export.{sh,ps1}` | Single-scope export primitive; `SCOPE=account\|metastore\|workspace` presets + `MATCH`/`EXCLUDE_REGEX`/`UPDATED_SINCE` filters → `captured/<name>/` |
| `02_export_fleet.{sh,ps1}` | Manifest-driven fan-out across a whole fleet — one resumable chunk per line of `fleet.conf`, per-chunk logs, continue-on-error |
| `fleet.example.conf` | The fleet manifest template (`<name> <profile> <scope> [services]`) |
| `plane_rules.yaml` | Resource-type → plane map (grants routed by securable) — the whole classification |
| `plane_transform.py` | Split flat HCL → classify → emit the plane tree + provider/backend/remote-state scaffolding; reads the parent `captured/` so all chunks recombine |
| `collapse_foreach.py` | Fold repeated resources into `for_each` maps (rewrites import targets + cross-refs) |
| `04_plan.{sh,ps1}` | Per-plane speculative plan + zero-destroy gate against a live workspace/account |
| `run.{sh,ps1}` | Orchestrator: `--fleet` drives the chunked export, `--offline` transforms committed snapshots |
| `reset.{sh,ps1}` | Wipe generated output (keeps `captured/`) |
| `walkthrough.py` | Narrated (jupytext) version for presenting the flow |
| `RUNBOOK.md` | **Operational step-by-step** for running this against a real estate at fleet scale |

## Scale model

Not everything multiplies by workspace count: under identity federation, users/groups are
**account-level (pulled once)**; catalogs/schemas/grants are **metastore-level (once per
metastore)**; only compute/SQL/pools/policies are the per-workspace multiplier. Chunk on
four axes — scope, workspace, service, and name (`MATCH`/`EXCLUDE_REGEX`) — plus incremental
re-syncs (`UPDATED_SINCE`). See [`RUNBOOK.md`](RUNBOOK.md) for the full procedure and
rate-limit tuning (`EXPORTER_WS_LIST_PARALLELISM`, `EXPORTER_PARALLELISM_<SVC>`).

## Safety

- **Adopt, never create/destroy.** A correct plan is `N to import, 0 to destroy`. The
  zero-destroy gate in `04_plan.*` is the tripwire; a nonzero destroy/replace count means
  triage-before-apply, never auto-proceed.
- **Read-only + speculative only.** This tool authors code and runs exports + *speculative*
  plans. Every `apply` / real `import` runs through your VCS-driven Terraform workflow
  (e.g. Terraform Cloud/Enterprise). Never run a local apply against shared state.
- **No real data is committed.** Only the fabricated `captured/example/` fixture ships;
  live export output is gitignored. Sanitize before sharing anything you export.

## License

[MIT](LICENSE) © 2026 Databricks. Provided as-is; not an officially supported Databricks product.
