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

## 6. Grant the users what they need (the SP needs almost nothing)

Discovery and reading are both driven by the **viewing user's** OBO token, so
the important grants go to your **users**, not the app service principal:

1. **User grants on the CLD Volume (required).** The recent-clusters list, the
   Browse view, and every log read all list/read the Volume with the *user's*
   token, so each user needs their own **READ VOLUME** (plus `USE CATALOG` /
   `USE SCHEMA`) on the CLD Volume:
   ```sql
   GRANT USE CATALOG ON CATALOG <catalog> TO `<group-or-user>`;
   GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema> TO `<group-or-user>`;
   GRANT READ VOLUME ON VOLUME  <catalog>.<schema>.<cld_volume> TO `<group-or-user>`;
   ```
   With this in place the **recent-clusters list populates on its own** — no SP
   or per-cluster grant is needed, and it stays current as new job clusters
   deliver logs.

2. **Optional SP grants (nice-to-have only).** The SP is used for two
   best-effort things that never block the list: (a) enriching each cluster row
   with a **friendly job name** (`clusters.get` → `jobs.get`), and (b) resolving
   a pasted **run/job ID** to its cluster. Both work on *specific IDs* without
   any `clusters.list` visibility. If a job's name doesn't show (e.g. the job
   was deleted, or the SP can't see it), the row still appears as `Job <id>` and
   logs still open — nothing breaks. Granting the SP **CAN_VIEW** on the jobs you
   care about simply makes friendly names appear.

> That's the two-identity model: the SP is *never* given broad access to
> everyone's logs. Log **content** is gated entirely by each user's own UC
> grants; the SP only reads job/cluster **metadata** for display, and only
> best-effort.

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
2. The left rail should list **Recent clusters with logs** (from user-OBO
   listing of the allowlisted CLD Volume root(s), enriched with job names). If
   empty, either no `<cluster-id>` directories exist under an allowlisted root,
   the signed-in user lacks **READ VOLUME** on that root, or the allowlist path
   is wrong.
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
| No recent clusters listed | No `<cluster-id>` dirs under an allowlisted root, user lacks READ VOLUME on it, or wrong allowlist | Verify cluster logging → Volume, grant the user `READ VOLUME`, and check `CLD_ROOT_ALLOWLIST` |
| Clusters show as `Job <id>` (no friendly name) | SP can't resolve the job (deleted, or no CAN_VIEW) — expected, not an error | Optionally grant the SP CAN_VIEW on the job; logs still open regardless |
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
