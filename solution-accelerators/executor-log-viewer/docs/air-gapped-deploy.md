# Air-Gapped / No-PyPI Deployment

**Status:** Verified on a Databricks GovCloud workspace. The app deploys and runs
with an empty runtime `requirements.txt` — no PyPI, no npm registry — because
every dependency is either pre-installed in the Databricks Apps base image or
prebuilt and committed.

## TL;DR — it just works, no vendoring needed (today)

- **Python runtime deps** (`databricks-sdk`, `fastapi`, `uvicorn[standard]`, and
  their transitive deps like `starlette`/`pydantic`) are **all pre-installed in
  the Databricks Apps base image**. So `app/requirements.txt` is intentionally
  EMPTY (comments only) → the deploy installs nothing from PyPI.
- **Frontend** is **prebuilt**: `app/frontend/dist/` is committed and served as
  static files. No `npm install` / Node registry access happens at deploy.
- Verified: the full backend suite (151 tests) passes against the exact
  base-image versions, and a live deploy with the empty requirements booted with
  **0 PyPI fetch lines** and served real `/api/runs` + `/api/log-files` requests.

## Base-image pre-installed versions (what the app runs on)

| Package | Base image | Our code verified on it |
|---|---|---|
| `databricks-sdk` | 0.33.0 | ✅ |
| `fastapi` | 0.115.0 | ✅ |
| `uvicorn[standard]` | 0.30.6 | ✅ |
| (`starlette`, `pydantic`, `anyio`, `click`, `h11` …) | transitive | ✅ |

Local/CI mirrors these via `app/requirements-dev.txt` (pinned to the base-image
versions). Do NOT bump a runtime pin past the base image without vendoring (see
below) — a pin the base image can't satisfy forces pip to reach PyPI at deploy,
which an air-gapped env cannot do.

## Deploy in an air-gapped workspace

Nothing special — the normal deploy works because it installs nothing:

```
databricks --profile <gov> sync  app/  /Workspace/Users/<you>/executor-log-viewer
databricks --profile <gov> apps deploy executor-log-viewer \
  --source-code-path /Workspace/Users/<you>/executor-log-viewer
```

Confirm air-gap safety in the build log: it must show **no** `Collecting`/
`Downloading http…` lines (i.e. no PyPI reach).

## IF a future dependency is NOT in the base image (vendor wheels)

If you add a package the base image lacks, vendor it (and ALL its transitive
deps) as wheels into a UC Volume — no PyPI at deploy:

1. On a machine WITH PyPI, download the full wheel closure:
   ```
   pip download <pkg>==<ver> -d ./wheels --only-binary=:all:
   ```
   (repeat / let it pull transitive wheels; verify every dep has a `.whl`).
2. Upload the wheels to a UC Volume the app SP can read, e.g.
   `/Volumes/<cat>/<schema>/app_wheels/`.
3. Add that Volume as an **app resource** (Apps UI → Resources → Volume, or via
   `apps update` resources JSON, same mechanism as the signing-secret resource).
4. Reference each wheel by its FULL hard-coded path in `app/requirements.txt`
   (env-var refs are NOT supported there):
   ```
   /Volumes/<cat>/<schema>/app_wheels/<pkg>-<ver>-py3-none-any.whl
   /Volumes/<cat>/<schema>/app_wheels/<transitive-dep>-<ver>-…-.whl
   ```
5. Redeploy; confirm the build log installs from the Volume paths, not PyPI.

## Frontend note

The React app must be built (`npm run build` in `app/frontend/`) on a machine
with the npm registry, and the resulting `app/frontend/dist/` committed. The
air-gapped workspace only ever serves the prebuilt static assets — it never runs
`npm install`.
