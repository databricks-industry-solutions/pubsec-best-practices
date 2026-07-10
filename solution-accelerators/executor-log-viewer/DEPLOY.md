# Deploying the Executor Log Viewer

A complete, copy-paste runbook. Written for a **restricted / air-gapped**
workspace where you may **not** have the Databricks CLI locally — every step
has a **UI path** and, where useful, a **web-terminal** (`databricks` CLI is
preinstalled in the workspace web terminal) alternative.

> The deploy installs **nothing** from PyPI/npm — all runtime deps are in the
> Apps base image and the frontend is prebuilt. See
> [`docs/air-gapped-deploy.md`](docs/air-gapped-deploy.md).

---

## 0. What you need first (prerequisites)

- **Cluster Log Delivery already configured** for the job clusters whose logs
  you want to read, pointing at a **UC Volume** path. This app *reads* delivered
  logs; it does not configure delivery. (Set it on the cluster/job:
  *Advanced options → Logging → Destination = Volume*, e.g.
  `/Volumes/<catalog>/<schema>/<cld_volume>`.)
- **Workspace admin** access (or an admin to help) for two one-time steps: the
  OBO scope toggle and creating the app.
- The **UC Volume path(s)** your clusters deliver to — you'll put these in the
  allowlist.
- Ability to upload files to the workspace (UI upload or web terminal).

The app runs as a **service principal (SP)** the platform creates for it, and
reads logs **on behalf of the signed-in user**. You will grant the SP a little,
and rely on each user's own Unity Catalog grants for what they can read.

---

## 1. Get the app code into the workspace

**Option A — Git folder (preferred).** In the workspace: **Workspace → Repos /
Git folders → Add** and clone this repo, or clone just this accelerator. The
app to deploy is the `app/` subfolder of this accelerator.

**Option B — Upload.** Zip the `app/` folder, then **Workspace → (your folder) →
Import** and upload. You want the workspace to contain an `app/` directory with
`app.yaml` at its root.

**Option C — Web terminal / CLI.** From the workspace web terminal:
```bash
databricks sync ./app /Workspace/Users/<you>/executor-log-viewer/app
```

> Whichever you pick, note the workspace path to the folder that **contains
> `app.yaml`** — call it `<APP_SOURCE_PATH>`. It will look like
> `/Workspace/Users/<you>/executor-log-viewer/app`.

---

## 2. Create the signing-secret (one time)

The app signs its opaque file references with an HMAC secret. Create a scope +
key and put a random value in it. **Never commit the value.**

**UI:** there is no secrets UI; use the web terminal.

**Web terminal / CLI:**
```bash
# scope name is your choice; this runbook uses "executor-log-viewer"
databricks secrets create-scope executor-log-viewer

# generate a strong random value and store it under key "fileref_signing_secret"
databricks secrets put-secret executor-log-viewer fileref_signing_secret \
  --string-value "$(python3 -c 'import secrets;print(secrets.token_urlsafe(48))')"
```

---

## 3. Create the app

**UI:** **Compute → Apps → Create app** (or **New → App**). Give it a name
(e.g. `executor-log-viewer`). Choose "Deploy from source" and you'll point it at
`<APP_SOURCE_PATH>` in the next step. Creating the app provisions its **service
principal** — note the SP's name/ID from the app's **Overview** page; you'll
grant it access in step 6.

**Web terminal / CLI:**
```bash
databricks apps create executor-log-viewer
```

---

## 4. Wire up the secret as an app *resource*

`app.yaml` references the secret by a **resource name** (`fileref-signing-secret`),
not by scope/key directly. Add that resource to the app so the platform injects
it as the `FILEREF_SIGNING_SECRET` env var.

**UI:** Open the app → **Edit / Configuration → Resources → Add resource →
Secret.** Set:
- **Resource key / name:** `fileref-signing-secret`  ← must match `app.yaml`'s `valueFrom`
- **Scope:** `executor-log-viewer`
- **Key:** `fileref_signing_secret`
- **Permission:** READ

**Web terminal / CLI:** add a resources block via `apps update --json` binding
resource name `fileref-signing-secret` → scope `executor-log-viewer`, key
`fileref_signing_secret`, permission `READ`.

---

## 5. Turn on on-behalf-of-user (OBO) auth  *(admin)*

The app reads logs as the signed-in user, so it needs the user token forwarded
with the `files.files` scope.

**UI:** Open the app → **Authorization / User authorization → + Add scope →**
select **`files.files`** → Save.

> On some GovCloud shards the OBO scope catalog is limited (no `jobs`/`clusters`
> scope). That's expected — this app only needs **`files.files`** for OBO; the
> Jobs/Clusters calls run as the SP (step 6), not as the user.

---

## 6. Grant the service principal what it needs

Two small grants, both cheap and self-maintaining:

1. **List clusters** — the SP calls `clusters.list` to surface recent clusters
   and their CLD paths. `clusters.list` returns clusters **without any
   per-cluster grant**, so usually **no grant is required** here. (If you want
   the SP to also resolve pasted **run/job IDs**, grant it **CAN_VIEW** on those
   specific jobs.)

2. **No SP grant on the Volume is required for user-facing reads** — logs are
   read with the **user's** token, so each user needs their own **READ VOLUME**
   (and USE CATALOG / USE SCHEMA) on the CLD Volume. Grant your log-reading
   users:
   ```sql
   GRANT USE CATALOG ON CATALOG <catalog> TO `<group-or-user>`;
   GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema> TO `<group-or-user>`;
   GRANT READ VOLUME ON VOLUME  <catalog>.<schema>.<cld_volume> TO `<group-or-user>`;
   ```

> That's the whole point of the two-identity model: you are **not** giving the
> SP broad access to everyone's logs. The SP only reads *metadata*; content
> access is each user's own UC grants.

---

## 7. Set the log-root allowlist

Edit **`app/app.yaml`** and set `CLD_ROOT_ALLOWLIST` to your CLD Volume path(s),
comma-separated. Only clusters/paths under an allowlisted root are surfaced or
readable.

```yaml
  - name: CLD_ROOT_ALLOWLIST
    value: "/Volumes/<catalog>/<schema>/<cld_volume>"
```

(For multiple teams/volumes: `"/Volumes/a/b/logs,/Volumes/c/d/logs"`.)

Leave `FILEREF_SIGNING_SECRET` as-is (`valueFrom: "fileref-signing-secret"`).
Leave the debug probe **off** (do not set `ENABLE_DEBUG_PROBE`).

---

## 8. Deploy

**UI:** Open the app → **Deploy**, pointing at `<APP_SOURCE_PATH>`.

**Web terminal / CLI:**
```bash
databricks apps deploy executor-log-viewer --source-code-path <APP_SOURCE_PATH>
```

**Confirm air-gap safety** in the deploy/build log: it should say *"Requirements
installed successfully"* and show **no** `Collecting` / `Downloading http…`
lines (nothing pulled from PyPI).

---

## 9. Verify

1. Open the app URL. As a **workspace admin/owner** you may not see the OBO
   consent screen; a **regular user's** first visit should prompt to authorize
   `files.files`.
2. The left rail should list **Recent clusters with logs** (from the SP's
   `clusters.list`). If empty, either no clusters have CLD under an allowlisted
   root, or the allowlist path is wrong.
3. Click a cluster (or paste a cluster/run ID). You should see `stdout`/`stderr`
   for the executors. A user **without** UC READ on the Volume should get a
   clean **403**, not a path leak.
4. Health check: `GET /healthz` → `{"ok": true}` (unauthenticated liveness).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Deploy log shows `Collecting`/`Downloading` | A dep pin isn't in the base image | Keep `app/requirements.txt` empty; vendor wheels per `docs/air-gapped-deploy.md` |
| App 404 / won't start | Wrong source path (no `app.yaml`) | Point deploy at the folder **containing** `app.yaml` |
| "more than one authorization method configured" | Not applicable at deploy; internal OBO client already handles this (`auth_type="pat"`) | — |
| Secret resource "error resolving resource" | `valueFrom` must match the **resource name** (`fileref-signing-secret`), not `scope/key` | Fix the resource name in step 4 |
| `files.files` scope disappears after an update | Adding resources via `apps update` can drop OBO scopes | Re-add `files.files` in the Authorization UI (send only `files.files`) |
| No recent clusters listed | No CLD under allowlisted root, or wrong allowlist | Verify cluster logging → Volume, and `CLD_ROOT_ALLOWLIST` |
| Logs 403 for a user | User lacks UC READ on the Volume | Grant `READ VOLUME` (+ USE CATALOG/SCHEMA) to that user/group |

---

## Reference: what's configured where

| Thing | Where | Value |
|---|---|---|
| ASGI command | `app.yaml` | `uvicorn app:app --host 0.0.0.0 --port 8000 --app-dir backend` |
| Signing secret env | `app.yaml` | `FILEREF_SIGNING_SECRET` ← `valueFrom: fileref-signing-secret` |
| Secret resource → scope/key | app resources | `fileref-signing-secret` → `executor-log-viewer` / `fileref_signing_secret` |
| OBO scope | app Authorization UI | `files.files` |
| Log-root allowlist | `app.yaml` | `CLD_ROOT_ALLOWLIST` (your Volume paths) |
| Debug probe | env (leave unset) | `ENABLE_DEBUG_PROBE` — must be OFF in production |
