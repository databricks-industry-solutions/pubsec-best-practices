# Operational Runbook — Generate & Organize Live IaC

Adopt a **live Databricks estate** into plane-organized Terraform, at fleet scale, without
creating or destroying anything. This is the operational procedure; see [`README.md`](README.md)
for the concept and the five-plane model.

**Windows / PowerShell.** Every step has a `.ps1` twin with the same behavior (the Python is
shared). Commands below show bash; on PowerShell 7+ substitute the `.ps1` form — env-var knobs
(`PROFILE=`, `SCOPE=`, `MATCH=`, …) become parameters (`-ProfileName`, `-Scope`, `-Match`, …).
First run may need `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`.

**Hard boundary.** This tool authors code and runs **read-only exports and *speculative* plans
only**. Every `apply`, `state mv`, and real `import` runs through your VCS-driven Terraform
workflow (e.g. Terraform Cloud/Enterprise). Never run a local apply against shared state.

**Safety property.** A clean adoption plan is `N to import, 0 to destroy`. The zero-destroy gate
in `04_plan.*` is the tripwire; a nonzero destroy/replace count is triage-before-apply.

---

## Scale model — where the volume actually is

Not everything multiplies by workspace count. Size the export against this:

| Resource class | Scope | Chunk | Scales with |
|---|---|---|---|
| Users, groups, SPs | Account (under identity federation) | `account` | Account size — pulled **once**, not ×workspaces |
| MWS (networks, credentials, storage, workspaces) | Account | `account` | Small |
| Catalogs, schemas, grants | Metastore | `metastore` (one per metastore) | Metastore size — **once per metastore** |
| `uc-tables` | Metastore | *(excluded by default)* | Usually far too many; add only if needed |
| Clusters, jobs, pools, policies, warehouses, secrets | Workspace | `workspace` (one per ws) | **The ×N multiplier** |

> **Verify identity federation first.** With account-level SCIM (identity federation),
> thousands of users/groups are a one-time `account` pull. If workspaces still use
> workspace-local SCIM, identity multiplies per workspace — move `groups`/`users` into each
> `workspace` chunk instead.

**Four chunking axes** (all first-class): **scope** (`SCOPE=account|metastore|workspace`
presets), **workspace** (one chunk per workspace — the primary unit for many workspaces),
**service** (the `services` column narrows `-listing`/`-services`), and **name**
(`MATCH`/`MATCH_REGEX`/`EXCLUDE_REGEX`). Plus **incremental** re-syncs via `UPDATED_SINCE`
(ISO8601 → `-incremental -updated-since`).

---

## Step 0 — Prerequisites

```bash
cp env.example .env            # auth for the account-scope pass (see below)
./00_prereqs.sh                 # fetch terraform + the provider exporter binary
databricks auth profiles        # confirm every profile you'll use is Valid: YES
```

- **`terraform`** 1.5+ on PATH, and a **`python3` with PyYAML** (`pip install pyyaml`;
  `run.sh` auto-selects a capable interpreter).
- **Auth per scope.** `account` chunks need an **account-admin service principal** (OAuth
  M2M) or an account-admin CLI profile. `metastore`/`workspace` chunks use a profile whose
  host is that workspace, with read access to the objects being exported.
- **Provider version split (deliberate).** The **exporter binary** is pinned in
  `00_prereqs.sh` (a version proven stable for listing large workspaces). The **generated
  code + plans** pin a newer provider (`plane_transform.py` `PROVIDER_VERSION`, `04_plan.sh`).
  Exporter and plan/apply versions are independent — set them to whatever your estate needs.

---

## Step 1 — Inventory the fleet → `fleet.conf`

```bash
cp fleet.example.conf fleet.conf
$EDITOR fleet.conf
```

One line per chunk: `<name> <profile> <scope> [services]`. Build it as **one `account`
chunk** (MWS + identity), **one `metastore` chunk per metastore**, and **one `workspace`
chunk per workspace**. `uc-tables` is excluded by the presets on purpose; add a
`MATCH_REGEX`/`EXCLUDE_REGEX` to drop scratch/temp noise.

```bash
./02_export_fleet.sh --dry-run     # print the plan (chunks, profiles, output dirs); exports nothing
```

---

## Step 2 — Chunked export (fan-out)

Each chunk is independent and written to `captured/<name>/`, so a stall or rate-limit on one
never loses the others, and completed chunks are skipped on re-run.

```bash
./02_export_fleet.sh                          # export every not-yet-captured chunk
./02_export_fleet.sh --only ws-prod --force   # (re-)export one chunk
```

Per-chunk output is teed to `logs/export-<name>.log`; the summary lists `ok / skipped /
failed` and the script exits nonzero if any chunk failed.

- **Distribute across runners.** Chunks are independent — run `--only <name>` on separate
  machines/runners for real parallelism (safer than in-process parallelism, which multiplies
  API load).
- **Rate limits (429s).** Turn exporter parallelism down for the whole fleet:
  ```bash
  EXPORTER_WS_LIST_PARALLELISM=2 ./02_export_fleet.sh          # default 5
  EXPORTER_PARALLELISM_SCIM=1    ./02_export_fleet.sh --only account
  ```
- **Incremental re-sync** (after the initial full pull):
  ```bash
  UPDATED_SINCE=2026-01-01T00:00:00Z ./02_export_fleet.sh --force
  ```
- **A chunk stalled or errored?** Read its `logs/export-<name>.log`, then re-run just it with
  `--only <name> --force`.

---

## Step 3 — Recombine → transform to planes

`plane_transform.py` reads the **parent** `captured/` dir, so **every chunk recombines into
one plane tree** (resources and imports are de-duplicated by address, so overlapping passes
are safe).

```bash
./run.sh --offline --collapse      # transform all captured/ chunks → collapse to for_each → fmt
```

- Output: `generated/databricks-terraform/environments/<plane>/`.
- **`environments/_unclassified/`** holds any resource type absent from `plane_rules.yaml` —
  never guessed. Triage: add the type to `plane_rules.yaml` and re-run, or place it by hand.
- **Identity scoping:** individual `databricks_user` resources are dropped (users come from
  the IdP via SCIM, not Terraform); groups + SPs + memberships are kept. See
  `skip_resource_types` in `plane_rules.yaml`.

To do it live in one shot (small estate): `./run.sh --fleet --collapse`.

---

## Step 4 — Speculative plan + zero-destroy gate (per plane)

Prove the adopt-not-create property against the live estate, one plane at a time.
`04_plan.sh` copies a plane root to `.plan/<root>/`, swaps in profile-based auth, drops the
remote-state stubs, then `init` + `plan` and prints the verdict. **Speculative only — nothing
is applied.**

```bash
./04_plan.sh account-infra          my-account-sp   # account-level planes → account SP
./04_plan.sh identity               my-account-sp
./04_plan.sh uc-foundation          my-account-sp
./04_plan.sh workspace-dev          my-workspace    # workspace-level planes → ws profile
./04_plan.sh uc-governance-default  my-workspace
```

Read the authoritative `Plan:` line:

| Bucket | Meaning | Action |
|---|---|---|
| `N to import` | Adoption — the whole point | Expected; good |
| `to add` (pure create) | Should be **0** for adoption | Investigate any nonzero |
| `to change` (in place) | Optional attrs the exporter omits + authoritative-grant reconciliation | Triage; usually benign |
| `to destroy` / `must be replaced` | **The gate trips** | **Stop.** Triage before any apply |

A common tripped-gate cause is a name collision in source data (e.g. two catalogs differing
only by `-` vs `_` that the exporter cross-links); disambiguate, then re-plan. Inspect the
full plan at `.plan/<root>/plan.out`.

---

## Step 5 — Hand off to your Terraform workflow (adoption)

The generated tree + `import {}` blocks are the artifact. Adoption runs on your remote
Terraform backend, per env, **via VCS** — never a local apply:

1. PR the plane roots into the VCS-connected repo.
2. A **speculative plan** on the PR must show `import … 0 destroy` (Step 4 rehearsed this).
3. Merge → the backend's apply performs the import. Roll env by env, Dev → Prod.
4. Put `restrict_destroy` (or your backend's equivalent) on `account-infra` / `uc-foundation`
   before any apply touches them.
5. After cutover, remove the `import {}` blocks and enforce a VCS-only workflow.

---

## Troubleshooting

- **A provider read bug on a resource type** (e.g. an intermittent SQL-warehouse listing
  error on some provider versions): plans/applies pin a newer provider than the exporter
  (see the version split in Step 0). Re-run the affected chunk (`--only <name> --force`).
- **`captured/` contains real data:** exported blocks carry real user names/emails, SP ids,
  and catalog names. `captured/*` (except the `example/` fixture) and `fleet.conf` are
  gitignored; sanitize before sharing anything outside your team.
- **PowerShell:** the `.ps1` scripts target pwsh 7+. `$LASTEXITCODE` is the source of truth
  for each step (the scripts scope `$ErrorActionPreference` around native calls so a non-zero
  exit is reported, not thrown, under `PSNativeCommandUseErrorActionPreference`).

---

## Command reference

```bash
./00_prereqs.sh                              # terraform + provider exporter binary
cp fleet.example.conf fleet.conf             # define the fleet
./02_export_fleet.sh --dry-run               # preview the export plan
./02_export_fleet.sh                         # chunked fan-out export → captured/<name>/
./02_export_fleet.sh --only <name> --force   # re-export one chunk
./run.sh --offline --collapse                # recombine all chunks → plane tree
./04_plan.sh <plane-root> <profile>          # speculative plan + zero-destroy gate
./reset.sh                                   # wipe generated/ + .plan/ + logs/ (keeps captured/)

# single-scope primitive (what the fleet calls under the hood):
PROFILE=<p> SCOPE=account|metastore|workspace ./01_export.sh captured/<name>
# granular knobs (env): SERVICES= LISTING= MATCH= MATCH_REGEX= EXCLUDE_REGEX= UPDATED_SINCE= LAST_ACTIVE_DAYS=
```
