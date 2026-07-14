"""Executor Log Viewer — FastAPI backend.

Phase 0.1 spike routes (``/whoami``, ``/probe``) + Phase 1 log-file endpoint
(``/api/log-files/{file_ref}``). The resolver (Phase 2), runs endpoints, and
frontend (Phase 3) are intentionally NOT here yet.

Security invariants enforced here (spec §3.1, §6):
  - The browser never sends a raw Volume path; only opaque signed file refs.
  - The user's token is read per-request and never logged.
  - Log responses set ``Cache-Control: no-store``.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles

import auth
import browse as browse_mod
import cluster_names
import filerefs
import logs
import resolver
import runs as runs_mod
import sp_client

app = FastAPI(title="Executor Log Viewer", version="0.1.0")

# --------------------------------------------------------------------------- #
# Built React SPA (Phase 3). The frontend builds to app/frontend/dist/. When   #
# that build exists we serve it; otherwise we fall back to the Phase 0 spike   #
# HTML at "/". The static mount + catch-all are added at the BOTTOM of this    #
# file so they never shadow the /api/*, /whoami, /probe, /healthz routes.      #
# --------------------------------------------------------------------------- #
# backend/ is the app-dir; the built SPA is a sibling: ../frontend/dist.
_FRONTEND_DIST = (Path(__file__).resolve().parent.parent / "frontend" / "dist")
_SPA_INDEX = _FRONTEND_DIST / "index.html"
_HAS_SPA = _SPA_INDEX.is_file()


# --------------------------------------------------------------------------- #
# Root — HTML page (a real document navigation for the OBO consent flow to     #
# attach to; a bare JSON API may not trigger the browser consent redirect).    #
# --------------------------------------------------------------------------- #
# HIGH #2: the diagnostic /probe endpoint (and this page's button that called
# it) is DISABLED unless ENABLE_DEBUG_PROBE=="1". The deployed app must not
# expose arbitrary Volume listing / SCIM bodies / SP CLD paths / job ACLs to
# ordinary users. The inline probe button is only emitted when the flag is on.
ENABLE_DEBUG_PROBE = os.environ.get("ENABLE_DEBUG_PROBE") == "1"

_ROOT_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Executor Log Viewer</title></head>
<body style="font-family:system-ui;max-width:720px;margin:40px auto;padding:0 16px">
<h1>Executor Log Viewer</h1>
<p>Phase 0 spike / manual-mode backend. Token present on this request:
<b id="tok">%(tok)s</b></p>
%(probe_ui)s
</body></html>"""

# The probe button + script is ONLY included when the debug flag is enabled.
_PROBE_UI = """<p><button onclick="probe()">Run OBO capability probe</button></p>
<pre id="out" style="background:#111;color:#ddd;padding:12px;border-radius:6px;white-space:pre-wrap"></pre>
<script>
async function probe(){
  const o=document.getElementById('out'); o.textContent='running...';
  const p='/Volumes/<catalog>/<schema>/<cld_volume>';
  const r=await fetch('/probe?path='+encodeURIComponent(p));
  o.textContent=JSON.stringify(await r.json(),null,2);
}
</script>"""


@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    """Serve the built React SPA if present; else the Phase 0 spike HTML.

    Either way this is a real HTML document so the browser performs a document
    navigation — which is what the OBO consent redirect attaches to. A pure
    JSON response may not trigger the consent screen.
    """
    if _HAS_SPA:
        # no-store so the consent/redirect flow always re-fetches a fresh shell.
        return FileResponse(_SPA_INDEX, media_type="text/html", headers={"Cache-Control": "no-store"})
    tok = "yes" if auth.has_forwarded_token(request.headers) else "no"
    probe_ui = _PROBE_UI if ENABLE_DEBUG_PROBE else ""
    return HTMLResponse(_ROOT_HTML % {"tok": tok, "probe_ui": probe_ui})


@app.get("/healthz")
def healthz():
    """Unauthenticated liveness check."""
    return {"ok": True}


# --------------------------------------------------------------------------- #
# Phase 0.1 spike routes                                                       #
# --------------------------------------------------------------------------- #
@app.get("/whoami")
def whoami(request: Request):
    """Report whether the forwarded-access-token header is present.

    NEVER returns or logs the token value — only its presence. This is the
    minimal probe used to confirm OBO header injection.
    """
    return {"has_forwarded_token": auth.has_forwarded_token(request.headers)}


def probe(request: Request, path: str = Query(None, description="Optional /Volumes/... dir to list")):
    """Live Phase 0 capability probe under the user's OBO token.

    Finding: on some GovCloud shards the valid app
    user-authorization scope catalog is limited to files.files, sql.warehouses,
    serving.serving-endpoints, dashboards.genie, catalog.catalogs,
    vectorsearch.vector-search-endpoints — there is NO jobs or clusters scope.
    So this probe exercises what actually works: files.files list/read. Jobs and
    Clusters auto-resolution is not available on this shard (manual mode only).
    """
    result = {"token_present": auth.has_forwarded_token(request.headers), "checks": {}}
    try:
        client = auth.build_user_client(request.headers)
    except auth.AuthError as exc:
        result["checks"]["build_client"] = f"error: {exc}"
        return result
    result["checks"]["build_client"] = "ok"

    def _try(name, fn):
        # Does the OBO token actually work for this API, regardless of whether
        # the scope could be *registered* on the app? (govcloud test)
        try:
            fn()
            result["checks"][name] = "OK — token can call this API"
        except Exception as exc:  # noqa: BLE001
            info = {"type": type(exc).__name__, "msg": str(exc)[:200]}
            # Pull the real HTTP status + body if the SDK attached them.
            for attr in ("status_code", "response"):
                v = getattr(exc, attr, None)
                if v is not None:
                    if attr == "response":
                        info["http_status"] = getattr(v, "status_code", None)
                        try:
                            info["body"] = v.text[:300]
                        except Exception:  # noqa: BLE001
                            pass
                    else:
                        info["status_code"] = v
            # also check for a nested cause with a body
            cause = getattr(exc, "__cause__", None)
            if cause is not None and "body" not in info:
                info["cause"] = f"{type(cause).__name__}: {str(cause)[:200]}"
            result["checks"][name] = info

    # DISCRIMINATOR: current_user.me() is covered by the default
    # iam.current-user:read scope that EVERY OBO token carries. If even this
    # fails to parse, the problem is host/transport (wrong endpoint / login
    # redirect), NOT scopes/consent.
    _try("current_user_me", lambda: client.current_user.me())

    # Raw HTTP with the token, bypassing the SDK's response parsing, to capture
    # the ACTUAL status code + body bytes the govcloud endpoint returns.
    import os, urllib.request, urllib.error
    token = request.headers.get("x-forwarded-access-token")
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = "https://" + host
    result["host_seen"] = host or "(unset)"
    if token and host:
        req = urllib.request.Request(
            host.rstrip("/") + "/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": "Bearer " + token},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                body = r.read(400).decode("utf-8", "replace")
                result["raw_me"] = {"http_status": r.status, "body_head": body[:200]}
        except urllib.error.HTTPError as e:
            body = e.read(400).decode("utf-8", "replace")
            result["raw_me"] = {"http_status": e.code, "body_head": body[:300]}
        except Exception as e:  # noqa: BLE001
            result["raw_me"] = {"error": f"{type(e).__name__}: {str(e)[:200]}"}

    _try("files_list_directory",
         lambda: result.__setitem__(
             "files_sample",
             [getattr(e, "path", str(e))
              for e in list(client.files.list_directory_contents(path))[:10]])) \
        if path else result["checks"].__setitem__("files_list_directory", "skipped (no ?path=)")

    # KEY TEST: can the APP SERVICE PRINCIPAL (not the OBO token) call Jobs /
    # Clusters? The SP uses normal OAuth M2M (DATABRICKS_CLIENT_ID/SECRET in the
    # runtime), which is NOT limited by the OBO scope catalog. If this works, we
    # can do run->cluster resolution via the SP and still read log CONTENT via
    # the user's OBO token — giving job-based search without a jobs OBO scope.
    def _try_sp(name, fn):
        try:
            fn()
            result["checks"][name] = "OK — app service principal can call this"
        except Exception as exc:  # noqa: BLE001
            result["checks"][name] = {"type": type(exc).__name__, "msg": str(exc)[:220]}

    try:
        from databricks.sdk import WorkspaceClient
        sp = WorkspaceClient()  # default auth = app service principal (M2M)
        result["checks"]["sp_build_client"] = "ok"
        _try_sp("sp_jobs_list_runs", lambda: list(sp.jobs.list_runs(limit=1)))
        _try_sp("sp_current_user", lambda: sp.current_user.me())

        # Can the SP enumerate clusters + read their CLD path (non-MANAGE)? This
        # decides whether clusters.list is a viable browse source on the SP.
        try:
            cl = list(sp.clusters.list())
            withcld = []
            for c in cl:
                conf = getattr(c, "cluster_log_conf", None)
                vol = getattr(getattr(conf, "volumes", None), "destination", None) if conf else None
                if vol:
                    withcld.append({"cluster_id": getattr(c, "cluster_id", None),
                                    "state": str(getattr(c, "state", None)),
                                    "cld": vol})
            result["checks"]["sp_clusters_list"] = f"OK — {len(cl)} clusters, {len(withcld)} with CLD->Volume"
            result["sp_clusters_with_cld"] = withcld[:10]
        except Exception as exc:  # noqa: BLE001
            result["checks"]["sp_clusters_list"] = {"type": type(exc).__name__, "msg": str(exc)[:200]}

        # STRICT-CHECK linchpin: can the SP read a job's PERMISSIONS ACL? If so,
        # the app can verify the viewing user is entitled to a run before showing
        # it (per-user visibility without an OBO jobs scope). Uses the first job
        # the SP can see, if any.
        def _perms_probe():
            # Test job we granted the SP CAN_VIEW on (519866750327567).
            jid = 519866750327567
            acl = sp.jobs.get_permissions(job_id=jid)
            entries = getattr(acl, "access_control_list", []) or []
            who = []
            for a in entries:
                name = getattr(a, "service_principal_name", None) or \
                       getattr(a, "user_name", None) or getattr(a, "group_name", None)
                lvls = [getattr(p, "permission_level", None) for p in (getattr(a, "all_permissions", []) or [])]
                who.append(f"{name}:{','.join(str(x) for x in lvls)}")
            result["checks"]["sp_read_job_permissions"] = f"OK — read ACL ({len(entries)} entries)"
            result["job_acl_sample"] = who
        try:
            _perms_probe()
        except Exception as exc:  # noqa: BLE001
            result["checks"]["sp_read_job_permissions"] = {"type": type(exc).__name__, "msg": str(exc)[:200]}

        # Does OBO current_user.me() expose the user's GROUPS (for group ACL eval)?
        try:
            me = client.current_user.me()  # OBO user
            groups = [g.display for g in (getattr(me, "groups", []) or [])]
            result["checks"]["obo_me_groups"] = groups or "none listed"
        except Exception as exc:  # noqa: BLE001
            result["checks"]["obo_me_groups"] = {"type": type(exc).__name__, "msg": str(exc)[:150]}
    except Exception as exc:  # noqa: BLE001
        result["checks"]["sp_build_client"] = {"type": type(exc).__name__, "msg": str(exc)[:220]}

    result["note"] = (
        "sp_* checks confirm the app SP can call Jobs/Clusters; obo files.files "
        "reads log content as the user."
    )
    return result


# HIGH #2: register the live diagnostic /probe ONLY when explicitly enabled.
# When the flag is unset we register an EXPLICIT 404 stub instead, so /probe is
# never the diagnostic endpoint AND never silently falls through to the SPA
# catch-all (which would otherwise serve index.html for /probe). Either way, an
# ordinary user in the deployed app can never reach the diagnostic body.
if ENABLE_DEBUG_PROBE:
    app.add_api_route("/probe", probe, methods=["GET"])
else:
    @app.get("/probe")
    def probe_disabled():
        return JSONResponse(status_code=404, content={"detail": "Not Found"})


# --------------------------------------------------------------------------- #
# Phase 1 — read one log file by opaque file_ref                               #
# --------------------------------------------------------------------------- #
@app.get("/api/log-files/{file_ref}")
def get_log_file(
    file_ref: str,
    request: Request,
    mode: str = Query("tail", pattern="^(tail|full)$"),
):
    """Stream a log file's text for the given opaque, signed ``file_ref``.

    Verifies the ref (expiry/tamper/path-escape), reads via the user's client,
    and returns text with metadata in headers. ``Cache-Control: no-store``.
    """
    # Build a fresh user-bound client per request (never cached).
    try:
        client = auth.build_user_client(request.headers)
    except auth.AuthError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"reason_code": "RUN_NO_ACCESS", "detail": str(exc)},
        )

    # HIGH #3: resolve a STABLE user subject from the OBO identity and bind the
    # ref to it. The resolver mints refs with ``me().user_name`` (see
    # /api/runs/{run_id}/logs), so we MUST use the same field here or every read
    # would 403. If we cannot resolve the user, fail closed (reject the read) —
    # a ref is otherwise a bearer token until expiry.
    try:
        expected_user = getattr(client.current_user.me(), "user_name", "") or ""
    except Exception:  # noqa: BLE001
        expected_user = ""
    if not expected_user:
        return JSONResponse(
            status_code=403,
            content={"reason_code": "FILES_FORBIDDEN", "detail": "invalid file reference"},
        )

    reader = logs.SdkFileReader(client)

    try:
        result = logs.read_log(
            file_ref, reader=reader, mode=mode, expected_user=expected_user
        )
    except filerefs.FileRefExpired:
        return JSONResponse(
            status_code=401,
            content={"reason_code": "FILE_NOT_FOUND", "detail": "link expired; reopen the file"},
        )
    except (
        filerefs.FileRefTampered,
        filerefs.FileRefPathEscape,
        filerefs.FileRefNotLog,
        filerefs.FileRefUserMismatch,  # HIGH #3 — same generic response (no oracle)
    ):
        # Do not distinguish tamper vs escape vs not-a-log vs wrong-user (no oracle).
        return JSONResponse(
            status_code=403,
            content={"reason_code": "FILES_FORBIDDEN", "detail": "invalid file reference"},
        )
    except logs.ContentForbidden:
        # MEDIUM #5: UC denied the user's actual content read. Map to 403 with
        # no-store and WITHOUT surfacing the SDK message (may contain the path).
        return JSONResponse(
            status_code=403,
            content={"reason_code": "FILES_FORBIDDEN", "detail": "you don't have READ on this log file"},
            headers={"Cache-Control": "no-store"},
        )

    if result.outcome == "FILE_TOO_LARGE":
        resp = JSONResponse(
            status_code=413,
            content={
                "reason_code": "FILE_TOO_LARGE",
                "detail": "file exceeds full-load cap; use tail mode",
                "next_action": "tail",
            },
        )
        for k, v in result.headers.items():
            resp.headers[k] = v
        return resp

    if result.outcome == "FILE_NOT_FOUND":
        resp = JSONResponse(
            status_code=404,
            content={"reason_code": "FILE_NOT_FOUND", "detail": "log file not found"},
        )
        for k, v in result.headers.items():
            resp.headers[k] = v
        return resp

    # OK: stream text body with metadata headers.
    def _iter():
        yield result.body

    return StreamingResponse(
        _iter(),
        media_type="text/plain; charset=utf-8",
        headers=result.headers,
    )


# --------------------------------------------------------------------------- #
# Phase 2 — recent runs + staged resolution                                   #
#                                                                             #
# Identity split (spec §2, §3.3): Jobs/Clusters metadata is fetched with the  #
# APP SERVICE PRINCIPAL; log FILE listing/reading uses the USER OBO token.    #
# --------------------------------------------------------------------------- #
@app.get("/api/runs")
def get_runs(
    request: Request,
    limit: int = Query(25, ge=1, le=100),
    page_token: str = Query(None),
    terminated_only: bool = Query(False),
    job_id: int = Query(None),
    include_clusterless: bool = Query(False),
):
    """List recent PARENT runs (spec §3.2). [APP SP] Jobs API.

    By default hides serverless/pipeline runs that have no cluster (and thus no
    executor logs); pass ``include_clusterless=true`` to surface them. Does NOT
    pre-resolve each run's CLD root (too slow — §3.2); CLD resolution happens on
    selection via ``/api/runs/{run_id}/logs``.

    SECURITY (MEDIUM #8 — intentional decision): this endpoint returns
    workspace-scoped run METADATA sourced from the app SP (Jobs API), NOT
    filtered per viewing user. Every app user sees the same SP-visible run
    metadata (run/job ids, names, states, page URLs, cluster ids, has_logs
    badges). This is an accepted, documented SP-visible metadata disclosure;
    log CONTENT stays per-user UC-gated (the OBO Files read on click).
    ``Cache-Control: no-store`` like the other sensitive API responses.
    """
    sp = sp_client.build_sp_client()
    runs_client = runs_mod.SdkRunsClient(sp)
    cluster_meta = resolver.SdkClusterMetaClient(sp)
    # A cluster "has logs" if CLD delivers to a UC Volume (reuse the resolver's
    # extractor so list-filtering and resolve agree on what "has logs" means).
    page = runs_mod.list_recent_runs(
        runs_client,
        limit=limit,
        page_token=page_token,
        terminated_only=terminated_only,
        job_id=job_id,
        include_clusterless=include_clusterless,
        cluster_meta=cluster_meta,
        has_logs_fn=lambda details: resolver._cld_destination(details) is not None,
    )
    payload = {
        "runs": [
            {
                "run_id": r.run_id,
                "job_id": r.job_id,
                "job_name": r.job_name,
                "state": r.state,
                "result_state": r.result_state,
                "started_at": r.started_at,
                "run_page_url": r.run_page_url,
                "cluster_id": r.cluster_id,
                "has_cluster": r.has_cluster,
                "has_logs": r.has_logs,
            }
            for r in page.runs
        ],
        "next_page_token": page.next_page_token,
    }
    resp = JSONResponse(content=payload)
    resp.headers["Cache-Control"] = "no-store"  # MEDIUM #8
    return resp


@app.get("/api/runs/{run_id}/logs")
def get_run_logs(
    run_id: str,
    request: Request,
    task_run_id: int = Query(None),
):
    """Staged resolution for a run/cluster id (spec §3.2, §2 data path).

    Returns ``{outcome, reason_code, run_meta, tasks?, tree?}``. Jobs/Clusters
    metadata via the [APP SP]; the executor/driver file tree is listed with the
    [USER OBO] client so Unity Catalog enforces per-user access.
    """
    # [USER OBO] — for listing files (the per-user access oracle).
    try:
        user = auth.build_user_client(request.headers)
    except auth.AuthError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"outcome": "ERROR", "reason_code": "FILES_SCOPE_MISSING", "detail": str(exc)},
        )

    # [APP SP] — for Jobs/Clusters metadata only.
    sp = sp_client.build_sp_client()
    runs_client = runs_mod.SdkRunsClient(sp)
    cluster_meta = resolver.SdkClusterMetaClient(sp)
    lister = resolver.SdkFileLister(user)

    # Identify the viewing user for the file_ref binding (best-effort; the ref
    # is HMAC-signed regardless).
    try:
        user_name = getattr(user.current_user.me(), "user_name", "") or ""
    except Exception:  # noqa: BLE001
        user_name = ""

    result = resolver.resolve(
        run_id,
        runs_client=runs_client,
        cluster_meta=cluster_meta,
        lister=lister,
        allowlist=resolver.load_allowlist(),
        user=user_name,
        task_run_id=task_run_id,
    )

    payload = {
        "outcome": result.outcome,
        "reason_code": result.reason_code,
        "run_meta": result.run_meta,
        "tasks": result.tasks,
        "tree": result.tree,
        "detail": result.detail,
    }
    resp = JSONResponse(content=payload)
    resp.headers["Cache-Control"] = "no-store"
    return resp


# --------------------------------------------------------------------------- #
# Browse-by-Volume-path (user-scoped discovery)                               #
#                                                                             #
# DISCOVERY, not access: the recent-runs list is SP-scoped and needs per-job  #
# grants. Instead let the user list the CLD log Volume DIRECTLY with their OBO #
# files.files token — UC enforces per-user access on the listing, so "list    #
# the CLD root" == "the log dirs THIS user can see". Zero SP grants,          #
# self-maintaining. These endpoints only LIST directory names; content reads  #
# stay exclusively through /api/log-files/{file_ref}.                         #
# --------------------------------------------------------------------------- #
@app.get("/api/log-roots")
def get_log_roots():
    """Return the configured CLD roots (the allowlist) as browse shortcuts.

    No auth-sensitive data — just the allowlist strings the operator configured
    via ``CLD_ROOT_ALLOWLIST``. The actual per-user access check happens when
    the user browses a root (UC-gated in ``/api/browse``).
    """
    return {"roots": resolver.load_allowlist()}


@app.get("/api/clusters")
def get_clusters(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
):
    """List recent clusters that have executor logs — the PRIMARY browse source.

    SOURCE (changed): this is derived from the CLD **Volume itself**, listed with
    the viewing user's [USER OBO] ``files.files`` token — the same self-maintaining
    source as ``/api/browse``. We enumerate every allowlisted root, list its
    immediate ``<cluster-id>/`` subdirectories, and return them as clusters.

    WHY NOT ``clusters.list``: the app SP's ``clusters.list`` only returns
    clusters the SP can VIEW, so freshly-created job clusters (owned by users'
    jobs, not the SP) never appear without per-job grants — not self-maintaining.
    Listing the Volume instead means "any cluster that DELIVERED logs shows up,"
    with zero SP/cluster grants, and Unity Catalog enforces per-user access on
    the listing (the user only sees dirs they can read).

    SECURITY: directory NAMES only — never file content, never a ``file_ref``.
    Content stays gated per-user by the OBO Files read on click (clicking a
    cluster -> ``/api/runs/{cluster_id}/logs`` -> resolve -> file_ref -> user
    read). ``Cache-Control: no-store``.
    """
    # [USER OBO] — UC enforces per-user access on the Volume listing.
    try:
        user = auth.build_user_client(request.headers)
    except auth.AuthError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"reason_code": "FILES_SCOPE_MISSING", "detail": str(exc)},
        )

    lister = resolver.SdkFileLister(user)
    allowlist = resolver.load_allowlist()

    # Aggregate cluster dirs across every allowlisted CLD root. A root the user
    # can't read (or that doesn't exist yet) is skipped, not fatal.
    seen: set[str] = set()
    clusters: list[dict] = []
    for root in allowlist:
        try:
            result = browse_mod.browse_root(lister, root, allowlist=allowlist)
        except browse_mod.BrowseError:
            continue  # missing / forbidden root -> just skip it
        for cand in result.get("clusters", []):
            cid = cand.get("cluster_id")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            clusters.append(
                {
                    "cluster_id": cid,
                    "cluster_name": None,  # not known from the Volume alone
                    "state": None,
                    "cluster_source": None,
                    "started_at": None,
                    "terminated_at": cand.get("modified"),
                    "has_executor": cand.get("has_executor"),
                    "has_driver": cand.get("has_driver"),
                }
            )

    # Most-recent first (by dir mtime); unknown mtime sorts last.
    clusters.sort(key=lambda c: (c["terminated_at"] is None, -(c["terminated_at"] or 0), c["cluster_id"]))
    clusters = clusters[:limit]

    # Best-effort enrich with job/run ids + friendly job name via the [APP SP]
    # (metadata only; never fatal — deleted/aged-out jobs leave fields null).
    try:
        sp = sp_client.build_sp_client()
        name_meta = cluster_names.SdkClusterNameMetaClient(sp)
        cluster_names.enrich_clusters(clusters, name_meta)
    except Exception:  # noqa: BLE001 - enrichment is optional; list still works
        for c in clusters:
            c.setdefault("job_id", None)
            c.setdefault("run_id", None)
            c.setdefault("job_name", None)

    resp = JSONResponse(content={"clusters": clusters})
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/api/browse")
def browse_log_root(
    request: Request,
    path: str = Query(..., description="/Volumes/... CLD root to list"),
):
    """List the cluster-level log directories under a CLD root, AS THE USER.

    [USER OBO] listing only — UC enforces per-user access. Returns each
    immediate subdirectory as a candidate cluster (name + relative age + cheap
    executor/driver presence hints when the dir count is small). Clicking a
    cluster goes through the EXISTING resolve path (``/api/runs/{cluster_id}/logs``).

    HIGH #1: the ``path`` must be equal to, or nested under, an allowlisted CLD
    root (``CLD_ROOT_ALLOWLIST``); otherwise 403 ``CLD_ROOT_NOT_FOUND`` — this
    is NOT a generic Volume lister.

    Errors: 401 (auth), 400 NOT_VOLUME, 403 CLD_ROOT_NOT_FOUND (not allowlisted),
    403 FILES_FORBIDDEN (no UC READ), 404 CLD_ROOT_NOT_FOUND (missing). Never
    leaks other errors' internals.
    """
    try:
        user = auth.build_user_client(request.headers)
    except auth.AuthError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"reason_code": "FILES_SCOPE_MISSING", "detail": str(exc)},
        )

    lister = resolver.SdkFileLister(user)
    try:
        # HIGH #1: enforce the CLD_ROOT_ALLOWLIST so this endpoint lists only
        # allowlisted log roots, never an arbitrary user-readable Volume path.
        result = browse_mod.browse_root(
            lister, path, allowlist=resolver.load_allowlist()
        )
    except browse_mod.BrowseError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"reason_code": exc.reason_code, "detail": exc.detail},
        )

    resp = JSONResponse(content=result)
    resp.headers["Cache-Control"] = "no-store"
    return resp


# --------------------------------------------------------------------------- #
# Phase 3 — serve the built React SPA                                          #
#                                                                             #
# Registered LAST so it never shadows the API / spike routes above. Vite emits #
# hashed assets under dist/assets, which we mount read-only; every other GET   #
# (client-side routes / hash-only URLs) falls through to index.html so the SPA #
# boots and reads its selection from the URL hash.                             #
# --------------------------------------------------------------------------- #
if _HAS_SPA:
    _ASSETS_DIR = _FRONTEND_DIST / "assets"
    if _ASSETS_DIR.is_dir():
        app.mount("/assets", StaticFiles(directory=str(_ASSETS_DIR)), name="assets")

    # A couple of top-level static files Vite may emit (favicon, etc.).
    @app.get("/{filename:path}")
    def spa_catch_all(filename: str):
        """Serve a real dist file if it exists, else the SPA shell.

        Never handles ``/api/...`` etc. — those are matched by their explicit
        routes registered earlier, so FastAPI dispatches them before this
        catch-all. This only ever sees unclaimed paths.
        """
        # Guard: never serve app internals or escape the dist dir.
        if filename and not filename.startswith("api/"):
            candidate = (_FRONTEND_DIST / filename).resolve()
            try:
                candidate.relative_to(_FRONTEND_DIST.resolve())
            except ValueError:
                candidate = None
            if candidate and candidate.is_file():
                return FileResponse(candidate)
        # Fall through to the SPA shell for client-side routing.
        return FileResponse(_SPA_INDEX, media_type="text/html", headers={"Cache-Control": "no-store"})
