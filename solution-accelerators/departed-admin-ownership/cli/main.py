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
    matched = set(prin.by_key.values())
    print(f"Resolved {len(prin.emails)} departed admin(s):")
    for e in sorted(prin.emails):
        print(f"  {e:40s} {'✓ in SCIM' if e in matched else '⚠ not found'}")
    print("Workspaces in scope:")
    for ws_id, _ws in core.iter_workspaces(cfg, ac):
        wh = _warehouse_for(cfg, ws_id)
        sp = cfg.run_as_sp_map.get(ws_id)
        print(f"  {ws_id}  warehouse={wh or '(none)'}  run_as_sp={sp or '(none)'}")
    return 0


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
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=core.INVENTORY_FIELDS, extrasaction="ignore")
        writer.writeheader()

        def emit(r):
            rows.append(r)
            writer.writerow(r)

        workspaces = list(core.iter_workspaces(cfg, ac))
        for ws_id, ws in workspaces:
            try:
                w = core.ws_client(ac, ws)
            except Exception as e:  # noqa: BLE001
                log.warning("cannot connect to workspace %s: %s", ws_id, e)
                continue
            host = w.config.host
            log.info("Workspace %s (%s)", ws_id, host)

            if cfg.scope_uc:
                wh = _warehouse_for(cfg, ws_id)
                if wh:
                    for r in core.inventory_uc_sql(cfg, prin, w, wh, ws_id, host):
                        emit(r)
                else:
                    log.info("  no warehouse for ws %s — UC securables via REST only", ws_id)
                for r in core.inventory_uc_rest_account(cfg, prin, w, ws_id, host):
                    emit(r)
            if cfg.scope_ws:
                for r in core.inventory_workspace(cfg, prin, w, ws_id, host):
                    emit(r)
            if cfg.scope_wsfs:
                for r in core.inventory_workspace_files(cfg, prin, w, ws_id, host):
                    emit(r)
            if cfg.scope_run_as:
                target_sp = cfg.run_as_sp_map.get(ws_id)
                if not target_sp:
                    log.warning("  ws %s has no run_as_sp_map entry — skipping run_as", ws_id)
                else:
                    for r in core.inventory_run_as(cfg, prin, w, ws_id, host, target_sp):
                        emit(r)

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

    ws_by_id = {ws_id: ws for ws_id, ws in core.iter_workspaces(cfg, ac)}
    clients: dict[int, object] = {}

    def client_for(ws_id):
        if ws_id not in clients:
            ws = ws_by_id.get(ws_id)
            clients[ws_id] = core.ws_client(ac, ws) if ws else None
        return clients[ws_id]

    ok = skip = err = 0
    for r in all_rows:
        try:
            ws_id = int(r["workspace_id"]) if r.get("workspace_id") else None
            w = client_for(ws_id) if ws_id is not None else None
            if w is None:
                log.warning(
                    "SKIP %s %s — workspace %s not in scope",
                    r["object_type"],
                    r["full_name"],
                    r.get("workspace_id"),
                )
                skip += 1
                continue
            wh = _warehouse_for(cfg, ws_id) if ws_id is not None else ""
            res = core.transfer_row(cfg, r, w, wh, dry_run)
            log.info("%s | %s %s", res, r["object_type"], r["full_name"])
            ok += 1 if res.startswith(("OK", "DRY-RUN")) else 0
            skip += 0 if res.startswith(("OK", "DRY-RUN")) else 1
        except Exception as e:  # noqa: BLE001
            log.error("ERR %s %s: %s", r["object_type"], r["full_name"], e)
            err += 1

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
