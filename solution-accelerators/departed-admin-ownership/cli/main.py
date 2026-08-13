#!/usr/bin/env python3
"""departed-admin-ownership CLI.

Runs the ownership + run_as sweep from a host that CAN reach the account console,
for environments where workspace compute is network-restricted from it. Same
two-phase, dry-run-first design as the notebook.

  whoami     Bootstrap the account client, resolve principals, list in-scope workspaces.
  inventory  Read-only crawl -> CSV report (+ run_as preflight summary).
  transfer   Reassign ownership / run_as from a reviewed CSV. Dry-run unless --execute.

Always run `inventory` first and review the CSV before `transfer`.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import core
from config import Config, account_client

log = logging.getLogger("dao.cli")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    logging.getLogger("databricks.sdk").setLevel(logging.WARNING)


def _warehouse_for(cfg: Config, ws_id: int) -> str:
    return cfg.sql_warehouse_ids.get(ws_id, "")


def cmd_whoami(cfg: Config) -> int:
    ac = account_client(cfg)
    prin = core.resolve_principals(cfg, ac)
    print(f"Account: {ac.config.account_id}")
    print(
        f"Target group '{prin.target_group}': "
        f"{'FOUND' if prin.target_group_present else 'NOT FOUND — fix before transfer'}"
    )
    print(f"Resolved {len(prin.emails)} departed admin(s):")
    for e in sorted(prin.emails):
        print(f"  {e:40s} {'✓ in SCIM' if e in prin.scim_resolved else '⚠ not found'}")
    print("Workspaces in scope:")
    for ws_id, _ws in core.iter_workspaces(cfg, ac):
        wh = _warehouse_for(cfg, ws_id)
        sp = cfg.run_as_sp_map.get(ws_id)
        print(f"  {ws_id}  warehouse={wh or '(none)'}  run_as_sp={sp or '(none)'}")
    return 0


def _build_clients(cfg: Config, ac, workspaces: list) -> list[tuple[int, object, object]]:
    """Mint one WorkspaceClient per workspace, serially, on the main thread.

    Token exchange goes through the shared AccountClient, whose auth state is not
    guaranteed thread-safe for concurrent get_workspace_client() calls — so we build
    every client here, before any fan-out. Each returned client is independent and
    safe to use from its own worker thread. Connect failures yield w=None so both
    inventory and transfer skip that workspace identically.
    """
    built: list[tuple[int, object, object]] = []
    for ws_id, ws in workspaces:
        try:
            w = core.ws_client(ac, ws)
        except Exception as e:  # noqa: BLE001
            log.warning("cannot connect to workspace %s: %s — skipping", ws_id, e)
            w = None
        built.append((ws_id, ws, w))
    return built


def _crawl_workspace(cfg: Config, prin, ws_id: int, w) -> list[dict]:
    """Crawl one pre-built workspace client and return its inventory rows. Runs in a
    worker thread; the client is this workspace's own, so there is no shared state."""
    host = w.config.host
    log.info("Workspace %s (%s)", ws_id, host)

    rows: list[dict] = []
    if cfg.scope_uc:
        wh = _warehouse_for(cfg, ws_id)
        if wh:
            rows.extend(core.inventory_uc_sql(cfg, prin, w, wh, ws_id, host))
        else:
            log.info("  no warehouse for ws %s — UC securables via REST only", ws_id)
        rows.extend(core.inventory_uc_rest_account(cfg, prin, w, ws_id, host))
    if cfg.scope_ws:
        rows.extend(core.inventory_workspace(cfg, prin, w, ws_id, host))
    if cfg.scope_wsfs:
        rows.extend(core.inventory_workspace_files(cfg, prin, w, ws_id, host))
    if cfg.scope_run_as:
        target_sp = cfg.run_as_sp_map.get(ws_id)
        if not target_sp:
            log.warning("  ws %s has no run_as_sp_map entry — skipping run_as", ws_id)
        else:
            rows.extend(core.inventory_run_as(cfg, prin, w, ws_id, host, target_sp))
    return rows


def _dedupe_uc(rows: list[dict], seen: set) -> list[dict]:
    """Drop metastore-global UC rows already emitted by another workspace.

    UC catalogs/schemas/tables and account-level securables live in the metastore, not
    a workspace, so the per-workspace crawl surfaces the same securable once per
    workspace that can see it. Keeping duplicates would bloat the CSV and make the
    parallel transfer fire N racing ALTER/PATCH statements at one object. Workspace,
    WSFS and run_as rows are genuinely per-workspace and are never deduped.
    """
    out = []
    for r in rows:
        if r["domain"] == "unity_catalog":
            key = (r["object_type"], r["full_name"])
            if key in seen:
                continue
            seen.add(key)
        out.append(r)
    return out


def cmd_inventory(cfg: Config) -> int:
    ac = account_client(cfg)
    prin = core.resolve_principals(cfg, ac)
    if not prin.target_group_present:
        log.warning(
            "Target group '%s' not found at account level — transfer would "
            "fail. Fix before Phase 2.",
            cfg.target_group,
        )

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = cfg.output_dir / f"inventory_{ts}.csv"
    rows: list[dict] = []
    workspaces = list(core.iter_workspaces(cfg, ac))
    clients = _build_clients(cfg, ac, workspaces)
    live = [(ws_id, w) for ws_id, _ws, w in clients if w is not None]
    workers = min(cfg.workspace_workers, len(live)) or 1
    log.info("Crawling %d workspace(s) with %d worker(s)", len(live), workers)

    # Crawl in parallel, but assemble results in workspace order so the CSV is
    # deterministic across runs (easy to diff successive inventories).
    results: dict[int, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_crawl_workspace, cfg, prin, ws_id, w): ws_id for ws_id, w in live
        }
        for fut in as_completed(futures):
            ws_id = futures[fut]
            try:
                results[ws_id] = fut.result()
            except Exception as e:  # noqa: BLE001
                log.error("workspace %s crawl failed: %s", ws_id, e)
                results[ws_id] = []

    seen_uc: set = set()
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=core.INVENTORY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for ws_id, _w in live:
            for r in _dedupe_uc(results.get(ws_id, []), seen_uc):
                rows.append(r)
                writer.writerow(r)

    _print_summary(rows, cfg)
    print(f"\nInventory CSV : {path}")
    print(f"\nReview the CSV, then run:  python main.py -c <config> transfer --from {path}")
    return 0


def _print_summary(rows: list[dict], cfg: Config) -> None:
    from collections import Counter

    counts = Counter((r["domain"], r["object_type"]) for r in rows)
    print("\n" + "=" * 60)
    print(f"Total objects: {len(rows)}")
    for (dom, typ), n in sorted(counts.items()):
        print(f"  {dom:16s} {typ:22s} {n}")

    active = [r for r in rows if str(r.get("extra", "")).startswith("ACTIVE_JOB_DEPENDENCY")]
    if active:
        print(
            f"\n⚠ {len(active)} file/folder(s) used by ACTIVE RECURRING jobs — re-home "
            "before deleting the user (see the `extra` column)."
        )
    if cfg.scope_run_as:
        need = sum(
            1
            for r in rows
            if r["transfer_method"] == "jobs_update_run_as" and r["extra"] == "SP_NEEDS_GRANT"
        )
        has = sum(
            1
            for r in rows
            if r["transfer_method"] == "jobs_update_run_as" and r["extra"] == "SP_HAS_ACCESS"
        )
        print(
            f"\nrun_as preflight: {has} job(s) already grantable by their target SP, "
            f"{need} need a CAN_MANAGE grant."
        )
        if need:
            print(
                "  -> set grant_run_as_sp_perms: true (with --execute) to grant CAN_MANAGE "
                "as part of the transfer, or grant out-of-band. Without it those jobs "
                "FAIL at run time after reassignment."
            )
    print("=" * 60)


def cmd_transfer(
    cfg: Config, from_csv: str, dry_run: bool, only: list[str] | None, limit: int | None
) -> int:
    ac = account_client(cfg)
    prin = core.resolve_principals(cfg, ac)
    if not prin.target_group_present:
        log.error("Target group '%s' not found at account level. Aborting.", cfg.target_group)
        return 2

    with open(from_csv) as fh:
        all_rows = list(csv.DictReader(fh))
    if only:
        all_rows = [r for r in all_rows if r["object_type"] in only or r["domain"] in only]
    if limit:
        all_rows = all_rows[:limit]

    log.info(
        "%s %d row(s) from %s",
        "DRY-RUN over" if dry_run else "Transferring",
        len(all_rows),
        from_csv,
    )

    # Group rows by workspace so each workspace is a self-contained unit of work:
    # one client, its own rows, applied sequentially within the thread.
    by_ws: dict[int | None, list[dict]] = {}
    for r in all_rows:
        ws_id = int(r["workspace_id"]) if r.get("workspace_id") else None
        by_ws.setdefault(ws_id, []).append(r)

    # Pre-build one client per workspace serially on the main thread (see
    # _build_clients — the shared AccountClient's token exchange is not safe to call
    # concurrently). Dry-run needs no client: transfer_row's dry-run branches never
    # touch the workspace client, so previews render regardless.
    client_by_id: dict[int | None, object] = {None: None}
    if not dry_run:
        ws_by_id = {ws_id: ws for ws_id, ws in core.iter_workspaces(cfg, ac)}
        for ws_id in by_ws:
            if ws_id is None:
                continue
            ws = ws_by_id.get(ws_id)
            if ws is None:
                client_by_id[ws_id] = None
                continue
            try:
                client_by_id[ws_id] = core.ws_client(ac, ws)
            except Exception as e:  # noqa: BLE001
                log.warning("cannot connect to workspace %s: %s — skipping", ws_id, e)
                client_by_id[ws_id] = None

    def transfer_workspace(ws_id, ws_rows) -> tuple[int, int, int]:
        ok = skip = err = 0
        w = client_by_id.get(ws_id)
        wh = _warehouse_for(cfg, ws_id) if ws_id is not None else ""
        for r in ws_rows:
            if w is None and not dry_run:
                log.warning(
                    "SKIP %s %s — workspace %s not reachable",
                    r["object_type"],
                    r["full_name"],
                    r.get("workspace_id"),
                )
                skip += 1
                continue
            try:
                res = core.transfer_row(cfg, r, w, wh, dry_run)
                log.info("%s | %s %s", res, r["object_type"], r["full_name"])
                if res.startswith(("OK", "DRY-RUN")):
                    ok += 1
                else:
                    skip += 1
            except Exception as e:  # noqa: BLE001
                log.error("ERR %s %s: %s", r["object_type"], r["full_name"], e)
                err += 1
        return ok, skip, err

    workers = min(cfg.workspace_workers, len(by_ws)) or 1
    log.info("Applying across %d workspace(s) with %d worker(s)", len(by_ws), workers)
    ok = skip = err = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(transfer_workspace, ws_id, ws_rows): ws_id
            for ws_id, ws_rows in by_ws.items()
        }
        for fut in as_completed(futures):
            ws_id = futures[fut]
            try:
                w_ok, w_skip, w_err = fut.result()
            except Exception as e:  # noqa: BLE001
                log.error("workspace %s transfer failed: %s", ws_id, e)
                err += len(by_ws[ws_id])
                continue
            ok += w_ok
            skip += w_skip
            err += w_err

    print(f"\nDone. {'(dry-run) ' if dry_run else ''}ok/dry={ok} skip={skip} err={err}")
    if dry_run:
        print("Re-run with --execute to apply.")
    return 0 if err == 0 else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("-c", "--config", default="config.yaml")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("whoami", help="bootstrap + sanity check")
    sub.add_parser("inventory", help="read-only crawl -> CSV")
    t = sub.add_parser("transfer", help="apply changes from a CSV")
    t.add_argument("--from", dest="from_csv", required=True)
    t.add_argument("--execute", action="store_true", help="apply (default is dry-run)")
    t.add_argument("--only", nargs="*", help="restrict to these object_type/domain values")
    t.add_argument("--limit", type=int)

    args = p.parse_args(argv)
    _setup_logging(args.verbose)
    cfg = Config.load(args.config)
    cfg.validate()

    if args.cmd == "whoami":
        return cmd_whoami(cfg)
    if args.cmd == "inventory":
        return cmd_inventory(cfg)
    if args.cmd == "transfer":
        return cmd_transfer(cfg, args.from_csv, not args.execute, args.only, args.limit)
    return 1


if __name__ == "__main__":
    sys.exit(main())
