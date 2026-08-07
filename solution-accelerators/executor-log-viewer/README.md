# Executor Log Viewer — Databricks App

Read Spark **executor** (and driver) `stdout`/`stderr` logs from **terminated
job clusters** — the case where the Spark History Server "Logs" link 404s
because the cluster no longer exists.

The app reads logs that Databricks [Cluster Log Delivery
(CLD)](https://docs.databricks.com/compute/configure.html#cluster-log-delivery)
has written to a Unity Catalog Volume, and serves them through a small
FastAPI + React app that runs **on-behalf-of the viewing user** so Unity
Catalog enforces exactly what each person is allowed to read.

Built for restricted / **air-gapped** environments (e.g. GovCloud): the deploy
pulls **nothing** from PyPI or npm — all runtime dependencies ship in the
Databricks Apps base image and the frontend is prebuilt. See
[`docs/air-gapped-deploy.md`](docs/air-gapped-deploy.md).

> **Deploying it?** Follow the step-by-step runbook in **[`DEPLOY.md`](DEPLOY.md)**.

## Why this exists

Spark executor logs are only retained for a running cluster. Once a **job**
cluster terminates, the History Server can render the event-timeline UI but its
per-executor "Logs" links break — the log files lived on the (now gone) nodes.
Cluster Log Delivery solves the retention half by copying `stdout`/`stderr` to a
Volume as the cluster runs, but there's no built-in UI to *browse* those files
per user. This app is that UI.

Executor logs are delivered by CLD **only for job clusters**, to:

```
<cld-root>/<cluster-id>/executor/<application-id>/<executor-id>/{stdout,stderr}
```

## How it works — two-identity model

| Identity | Used for | Why |
|---|---|---|
| **App service principal (SP)** | Jobs & Clusters **metadata** (run → task → cluster → CLD path) | These APIs aren't in the on-behalf-of-user (OBO) scope catalog on some shards; the SP can call them. The SP never reads log *content*. |
| **Viewing user (OBO token)** | Listing & reading log **file content** in the Volume | Unity Catalog enforces per-user access. The user only sees files they're granted. |

The browser never handles raw Volume paths — the backend hands out opaque,
HMAC-signed `file_ref`s that are re-verified (path containment, file-kind, and
the requesting user) on every read. The user's forwarded token is used
per-request and never cached, logged, or echoed.

## Finding logs (three low-friction sources)

Discovering *which* logs exist is the hard part when jobs age out. The app
offers three, in order of friction. Crucially, discovery is driven by the CLD
**Volume itself** (listed with the user's OBO token), **not** by the service
principal's `clusters.list` — so any cluster that *delivered* logs shows up
with **zero** SP or per-cluster grants, and it's self-maintaining.

1. **Recent clusters with logs** (primary) — lists the `<cluster-id>`
   directories under the allowlisted CLD root(s) with the **viewing user's OBO
   token** (so Unity Catalog gates which clusters each user sees), aggregated
   across roots. Each row is then enriched **best-effort** via the SP
   (`clusters.get` → `jobs.get`) to show the friendly **job name + run ID**;
   if the job was deleted, the row falls back to `Job <id>` — the list never
   breaks and needs no SP grant to populate.
2. **Browse a log root** (secondary) — the same user-OBO Volume listing,
   scoped to a single root you pick; useful for drilling into one team's
   Volume.
3. **Look up by run / job / cluster ID** — paste an ID; the SP resolves it to a
   CLD path (`clusters.get`/`jobs.get` on a specific ID — no `clusters.list`
   enumeration needed).

## Structure

```
executor-log-viewer/
├── README.md                 # this file
├── DEPLOY.md                 # step-by-step deployment runbook (start here to deploy)
├── docs/
│   └── air-gapped-deploy.md  # no-PyPI / no-npm deployment details + wheel-vendoring
└── app/                      # the Databricks App (deploy this folder)
    ├── app.yaml              # Apps config (uvicorn command, env, secret + allowlist)
    ├── requirements.txt      # INTENTIONALLY EMPTY — deps are in the base image
    ├── requirements-dev.txt  # local/CI pins (mirror the base-image versions)
    ├── backend/              # FastAPI backend (flat modules + pytest suite)
    └── frontend/             # React/Vite app; prebuilt dist/ is committed & served
```

## Security invariants

- The browser never supplies a raw Volume path — only opaque HMAC-signed
  `file_ref`s, re-verified (path containment + file-kind + user) on every read.
- Reads are restricted to recognized log files (`stdout`, `stderr`, `*.log`,
  rotated variants) — this is **not** a general Volume file reader.
- The user's `x-forwarded-access-token` is read per-request and never cached,
  logged, or echoed in errors. Log responses set `Cache-Control: no-store`.
- A Unity Catalog denial surfaces as a clean `403`, never a leaked path.

## Local development

```bash
cd app/backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r ../requirements-dev.txt
python -m pytest -q          # mocked SDK — no live workspace needed
```

To run the server locally: `export FILEREF_SIGNING_SECRET=dev-only && uvicorn app:app --reload --app-dir .`

---

*Databricks Field Engineering — pubsec-best-practices. Provided as-is, no
warranty; validate against your own security requirements before production use.*
