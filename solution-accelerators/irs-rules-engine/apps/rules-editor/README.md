# IRS Rules Editor

Rules editor for authoring, testing, and governing the DMN rules that drive the
IRS Return Review Program POC. A React frontend wraps the Apache KIE DMN Editor;
a FastAPI backend reads and writes DMN XML to a Unity Catalog Volume, tracks
`DRAFT` / `ACTIVE` / `ARCHIVED` versions in a Delta governance table, and
triggers Drools-powered batch scoring via a saved Databricks job. Deployed as a
Databricks App.

## Directory layout

```
apps/rules-editor/
├── client/                # React 18 + Vite (KIE DMN Editor). dev :3333, proxies /api -> :8000
│   ├── package.json       # scripts: dev, build, preview
│   └── vite.config.js
├── server/
│   ├── app.py             # FastAPI entrypoint; mounts /api + static client/build
│   ├── routers/
│   │   ├── catalog.py     # UC browsing (catalogs, schemas, tables, columns)
│   │   ├── dmn.py         # DMN CRUD, version promotion, Python test evaluate
│   │   └── scoring.py     # Batch scoring job trigger + history/results
│   └── services/
│       ├── databricks.py  # Shared WorkspaceClient + UC/job constants
│       └── sql.py         # Statement Execution helper (parameterized)
├── dmn/                   # Sample DMN files: irs_rrp_advanced, irs_tax_review_v2, v3_irm
├── app.yaml               # Databricks App manifest (command + env)
├── deploy.sh              # Build client, stage, upload, deploy app
└── requirements.txt
```

## Prerequisites

- Node.js 18+ and npm (for the Vite build)
- Python 3.11+
- Databricks CLI authenticated to a workspace profile (set
  `DATABRICKS_CONFIG_PROFILE`; default is `DEFAULT`)
- Single-User cluster with the Drools shaded JAR attached (for test scoring via
  `04_batch_scoring`) — see `../../notebooks/README.md`
- UC resources seeded by `notebooks/01_setup_catalog.py` (default catalog
  `<your-catalog>`, schema `<your-schema>`, volume `<your-volume>`, and
  `rule_versions`)

## Local development

Install dependencies:

```bash
# From apps/rules-editor/
pip install -r requirements.txt
cd client && npm install && cd ..
```

> **Note on `package-lock.json`:** No lockfile is shipped — the upstream POC
> resolved packages from an internal npm mirror that is not reachable from
> outside the Databricks corporate network. `npm install` will generate a fresh
> lockfile against the public registry on first run. Commit the generated
> lockfile in your fork.

Run the backend (serves `/api` on port 8000):

```bash
uvicorn server.app:app --reload --port 8000
```

Run the frontend dev server (port 3333, proxies `/api` to `localhost:8000`):

```bash
cd client && npm run dev
```

### Environment variables

All workspace-specific values live in env vars. Set them in `app.yaml`
when deploying, or in your shell when running locally. The defaults
shipped in this repo are intentionally placeholders — replace them.

| Variable | Purpose |
| --- | --- |
| `DATABRICKS_WAREHOUSE_ID` | SQL warehouse used by `services/sql.py` for Delta reads/writes |
| `DATABRICKS_CLUSTER_ID` | Interactive cluster the app references (must have the Drools shaded JAR attached) |
| `SCORING_JOB_ID` | Job ID triggered by `POST /api/scoring/run` (the saved Lakeflow job that runs `04_batch_scoring.py`) |
| `SCORING_NOTEBOOK_PATH` | Informational path surfaced in the UI for the scoring notebook |
| `DATABRICKS_CONFIG_PROFILE` | CLI profile to use locally (or `DATABRICKS_HOST` + `DATABRICKS_TOKEN`) |

Unity Catalog targets are hardcoded in `server/services/databricks.py`:
catalog `<your-catalog>`, schema `<your-schema>`, volume
`/Volumes/<your-catalog>/<your-schema>/<your-volume>`. Edit those constants
if your deployment uses different names.

Authentication for the `WorkspaceClient` follows the standard Databricks SDK
chain — set `DATABRICKS_CONFIG_PROFILE` (or `DATABRICKS_HOST` +
`DATABRICKS_TOKEN`) locally.

## Deployment

`./deploy.sh` builds the React bundle, stages both client and server under
`/tmp/irs-rules-editor-deploy`, uploads to your workspace home
(`/Workspace/Users/<your-user>/apps/irs-rules-editor` by default), and runs
`databricks apps deploy` against the configured profile. The script prints
the resulting app URL and status on exit.

Override targets via env:

```bash
DATABRICKS_CONFIG_PROFILE=my-profile \
APP_NAME=my-rules-editor \
WORKSPACE_PATH=/Workspace/Shared/apps/my-rules-editor \
./deploy.sh
```

The deployed app runs under `app.yaml`:

```yaml
command: [uvicorn, server.app:app, --host, 0.0.0.0, --port, $DATABRICKS_APP_PORT]
```

The FastAPI app mounts the built React bundle at `/` when `client/build` is
present (see `server/app.py`).

## API overview

All routes are mounted under `/api`:

- `catalog` — `GET /catalog/{catalogs,schemas,tables,columns}` — Unity Catalog
  browsing used by the input-mapping UI.
- `dmn` — `GET /dmn/versions`, `GET /dmn/active`, `GET /dmn/{version_id}`,
  `POST /dmn/save` (new `DRAFT`), `POST /dmn/promote` (atomic `ACTIVE` swap +
  archive), `POST /dmn/evaluate` (pure-Python mirror of the 21 DMN rules for
  instant feedback), `GET /dmn/sample-returns`, `POST /dmn/dedup`.
- `scoring` — `POST /scoring/run` (triggers the job named in `SCORING_JOB_ID` via
  `run_now` with `proof_limit` / `version_id` notebook params),
  `GET /scoring/run/{run_id}`, `GET /scoring/history`, `GET /scoring/results`.

## Relationship to the batch pipeline

This app is for **authoring and testing** rules. It writes DMN XML to
`/Volumes/<your-catalog>/<your-schema>/<your-volume>/` and records versions in
`<your-catalog>.<your-schema>.rule_versions`. Full-fleet scoring is done by
`notebooks/04_batch_scoring.py`, which reads the same Delta table and evaluates
all 10M rows with Drools. See `../../notebooks/README.md`.
