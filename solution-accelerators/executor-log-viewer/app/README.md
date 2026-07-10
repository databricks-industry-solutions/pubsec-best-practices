# Executor Log Viewer — the app

The deployable Databricks App: a FastAPI backend serving a prebuilt React
frontend. It reads Spark **executor**/**driver** `stdout`/`stderr` logs that
Cluster Log Delivery (CLD) wrote to a UC Volume, on-behalf-of the viewing user.
Stateless — no database, no ingest.

> **Deploying?** See [`../DEPLOY.md`](../DEPLOY.md) for the step-by-step runbook.
> **Air-gapped?** See [`../docs/air-gapped-deploy.md`](../docs/air-gapped-deploy.md).

## Layout

```
app/
├── app.yaml              # Databricks Apps config (uvicorn command, env, secret, allowlist)
├── requirements.txt      # INTENTIONALLY EMPTY — runtime deps ship in the Apps base image
├── requirements-dev.txt  # local/CI pins mirroring the base-image versions
├── backend/
│   ├── app.py            # FastAPI routes: /healthz, /api/runs, /api/clusters, /api/browse,
│   │                     #   /api/log-files/{ref}, SPA catch-all
│   ├── auth.py           # per-request user WorkspaceClient from x-forwarded-access-token (OBO)
│   ├── sp_client.py      # app service-principal client (Jobs/Clusters metadata only)
│   ├── resolver.py       # run/job/cluster ID -> CLD path resolution + reason codes
│   ├── runs.py           # recent runs listing (expand_tasks -> cluster_id, has_logs)
│   ├── clusters_source.py# SP clusters.list -> clusters with CLD under an allowlisted root
│   ├── browse.py         # user-OBO Volume listing of an allowlisted log root
│   ├── filerefs.py       # opaque HMAC-signed file refs (mint/verify: path+kind+user+expiry)
│   ├── logfiles.py       # strict log-file classification (stdout/stderr/*.log/rotated)
│   ├── logs.py           # read one log file by ref: tail / full (capped)
│   ├── paths.py          # normalize + segment-aware containment (is_under_root)
│   ├── pyproject.toml    # pytest config (pythonpath=["."], testpaths=["tests"])
│   └── tests/            # pytest suite — mocked SDK, no live workspace needed
└── frontend/
    ├── src/              # React/Vite/TypeScript source
    └── dist/             # prebuilt bundle — committed and served as static files
```

## Local development

```bash
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r ../requirements-dev.txt
python -m pytest -q                     # mocked SDK — no live workspace
```

Run the server locally:

```bash
export FILEREF_SIGNING_SECRET="dev-only-secret-change-me"
uvicorn app:app --reload --app-dir .
# GET http://127.0.0.1:8000/healthz -> {"ok": true}
```

`pyproject.toml` puts the backend dir on `sys.path` (`pythonpath = ["."]`),
matching how Databricks Apps runs this flat module layout.

## Rebuilding the frontend

The committed `frontend/dist/` is what the app serves. To change the UI, rebuild
on a machine with npm registry access and commit the result:

```bash
cd frontend
npm install
npm run build          # regenerates dist/
```

The deployed (possibly air-gapped) app never runs `npm install` — it only serves
the prebuilt `dist/`.

## Security invariants

- The browser never supplies a raw Volume path — only opaque HMAC-signed
  `file_ref`s, re-verified (path containment + file-kind + requesting user) on
  every read. Reads are restricted to recognized log files — not a general
  Volume reader.
- The user's `x-forwarded-access-token` is read per-request and never cached,
  logged, or echoed. Log responses set `Cache-Control: no-store`.
- A Unity Catalog denial surfaces as a clean `403`, never a leaked path.
- The debug probe route is disabled unless `ENABLE_DEBUG_PROBE=1` — leave it
  unset in production.
