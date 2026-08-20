#!/usr/bin/env python3
"""Strip departed admins of account-level access (groups, roles, entitlements,
workspace assignments).

For each departed admin (identified by account userName / email) this removes:
  * membership in every account-level group,
  * direct SCIM roles on the user (e.g. a directly-granted ``account_admin``),
  * direct account entitlements (e.g. ``allow-cluster-create``),
  * per-workspace access assignments across every workspace in the account.

Roles/entitlements inherited *indirectly* (via a group) are cleared automatically when
the group memberships are removed — only directly-assigned ones need an explicit PATCH.

Optionally, with ``--deactivate``, it sets each account to inactive (active=false) as a
final step, after access has been removed. That is reversible and does NOT delete the
account.

This is a companion to the departed-admin **ownership-transfer** toolkit. Run the
ownership transfer FIRST (so nothing the admin owns is orphaned), then run this to remove
their access (and optionally deactivate).

Safe by default: DRY-RUN unless ``--execute`` is passed.

Usage:
    python offboard_account_access.py --profile <account-profile> \
        --admins former1@corp.com,former2@corp.com [--execute]
        [--scope groups,roles,entitlements,workspace_assignments]
        [--workspace-ids 123,456] [--workers 8] [-v]

The profile must point at the ACCOUNT console (host https://accounts.<cloud>...) with
account_id set, and the principal must be an account admin.
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

log = logging.getLogger("offboard")

ALL_SCOPES = ("groups", "roles", "entitlements", "workspace_assignments")
_PATCH_SCHEMA = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]


# --------------------------------------------------------------------------- #
# Principal resolution
# --------------------------------------------------------------------------- #
def resolve_users(ac: AccountClient, emails: list[str]) -> dict[str, object]:
    """email (lowercased) -> account User. Warns on any that can't be found."""
    found: dict[str, object] = {}
    for email in emails:
        matches = list(ac.users.list(filter=f'userName eq "{email}"'))
        if not matches:
            log.warning("  ⚠ no account user found for %s — skipping", email)
            continue
        # userName is unique, but guard against a filter that returns extras.
        user = next((u for u in matches if (u.user_name or "").lower() == email), matches[0])
        found[email] = user
        log.info("  resolved %s -> id=%s", email, user.id)
    return found


# --------------------------------------------------------------------------- #
# Group memberships
# --------------------------------------------------------------------------- #
def build_group_membership(ac: AccountClient, user_ids: set[str], workers: int) -> dict[str, list[tuple[str, str]]]:
    """user_id -> [(group_id, group_display_name), ...] for the target users.

    Members are read per-group via groups.get(): the inline ``members`` returned by a
    groups *list* can be truncated for very large groups, which would silently miss a
    membership. Any group whose members can't be read is warned about (so the operator
    knows that group wasn't checked) rather than skipped silently. Group members may be
    users, nested groups, or service principals — we only match our target user ids."""
    membership: dict[str, list[tuple[str, str]]] = {uid: [] for uid in user_ids}
    group_ids = [(g.id, g.display_name or g.id) for g in ac.groups.list(attributes="id,displayName")]

    def members_of(item):
        gid, gname = item
        try:
            g = ac.groups.get(gid)
            return gid, gname, [m.value for m in (g.members or [])]
        except Exception as e:  # noqa: BLE001
            log.warning("  ⚠ could not read members of group '%s' (%s) — NOT checked: %s", gname, gid, e)
            return gid, gname, None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for gid, gname, members in pool.map(members_of, group_ids):
            if members is None:
                continue
            for mv in members:
                if mv in membership:
                    membership[mv].append((gid, gname))
    return membership


def remove_group_memberships(ac, user, groups, dry_run) -> tuple[int, int]:
    ok = err = 0
    for gid, gname in groups:
        action = f"remove {user.user_name} from group '{gname}' ({gid})"
        if dry_run:
            log.info("  DRY-RUN %s", action)
            ok += 1
            continue
        try:
            ac.groups.patch(
                gid,
                operations=[iam.Patch(op=iam.PatchOp.REMOVE, path=f'members[value eq "{user.id}"]')],
                schemas=_PATCH_SCHEMA,
            )
            log.info("  OK %s", action)
            ok += 1
        except Exception as e:  # noqa: BLE001
            log.error("  ERR %s: %s", action, e)
            err += 1
    return ok, err


# --------------------------------------------------------------------------- #
# Direct roles + entitlements on the user object
# --------------------------------------------------------------------------- #
def _is_direct(complex_value) -> bool:
    """A SCIM role/entitlement is 'indirect' only when explicitly typed so (it comes from
    a group). Direct assignments carry type 'direct' OR no type at all — so we treat
    anything not explicitly 'indirect' as direct. (Observed: inherited account roles are
    returned with type='indirect', so they are correctly excluded.)"""
    return (getattr(complex_value, "type", None) or "").lower() != "indirect"


def remove_direct_roles_entitlements(ac, user, do_roles, do_entitlements, dry_run) -> tuple[int, int]:
    # Re-fetch the full object: list() does not populate roles/entitlements.
    full = ac.users.get(user.id)
    targets: list[tuple[str, str]] = []  # (scim path, human label)

    if do_roles:
        for r in full.roles or []:
            if _is_direct(r):
                targets.append((f'roles[value eq "{r.value}"]', f"role '{r.value}'"))
            elif (r.type or "").lower() == "indirect":
                log.info("  note: role '%s' is indirect (from a group) — cleared by group removal", r.value)
    if do_entitlements:
        for e in full.entitlements or []:
            if _is_direct(e):
                targets.append((f'entitlements[value eq "{e.value}"]', f"entitlement '{e.value}'"))

    # One PATCH per item (not a single batch): a rejected op — e.g. an assignment that
    # turns out not to be directly removable — must not block the others.
    ok = err = 0
    for path, label in targets:
        action = f"remove {label} from {user.user_name}"
        if dry_run:
            log.info("  DRY-RUN %s", action)
            ok += 1
            continue
        try:
            ac.users.patch(user.id, operations=[iam.Patch(op=iam.PatchOp.REMOVE, path=path)], schemas=_PATCH_SCHEMA)
            log.info("  OK %s", action)
            ok += 1
        except Exception as e:  # noqa: BLE001
            log.error("  ERR %s: %s", action, e)
            err += 1
    return ok, err


# --------------------------------------------------------------------------- #
# Per-workspace access assignments
# --------------------------------------------------------------------------- #
def remove_workspace_assignments(ac, users_by_email, dry_run, workspace_ids, workers) -> tuple[int, int]:
    # Match by principal_id (== account user id) primarily, user_name as a fallback — but
    # always DELETE with the resolved account user id, never the assignment's principal_id
    # (which can be None on some assignments and would make delete() fail).
    id_to_name = {int(u.id): (u.user_name or str(u.id)) for u in users_by_email.values() if str(u.id).isdigit()}
    name_to_id = {(u.user_name or "").lower(): int(u.id) for u in users_by_email.values() if str(u.id).isdigit()}

    workspaces = [
        ws for ws in ac.workspaces.list()
        if not workspace_ids or ws.workspace_id in workspace_ids
    ]
    log.info("  scanning %d workspace(s) for assignments…", len(workspaces))

    def hits_in(ws):
        """(status, workspace_id, payload): status 'ok' -> payload is a list of
        (workspace_id, target_user_id, label); status 'err' -> payload is the error."""
        try:
            out = []
            for pa in ac.workspace_assignment.list(ws.workspace_id):
                pr = pa.principal
                if pr is None:
                    continue
                pid = getattr(pr, "principal_id", None)
                uname = (getattr(pr, "user_name", None) or "").lower()
                target = pid if pid in id_to_name else name_to_id.get(uname)
                if target is None:
                    continue
                perms = ",".join(str(p).replace("WorkspacePermission.", "") for p in (pa.permissions or []))
                out.append((ws.workspace_id, target, f"{id_to_name.get(target, uname or target)} [{perms}]"))
            return ("ok", ws.workspace_id, out)
        except Exception as e:  # noqa: BLE001
            return ("err", ws.workspace_id, str(e))

    matches: list[tuple[int, int, str]] = []
    failed: list[tuple[int, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for status, wsid, payload in pool.map(hits_in, workspaces):
            if status == "err":
                failed.append((wsid, payload))
            else:
                matches.extend(payload)

    # Scan failures are NOT silent: a workspace we couldn't read might still hold the
    # admin's access, so we surface it and count it toward the error total.
    ok = 0
    err = len(failed)
    if failed:
        shown = ", ".join(str(w) for w, _ in failed[:10]) + (" …" if len(failed) > 10 else "")
        log.warning("  ⚠ could not scan %d workspace(s) for assignments (access may remain there): %s",
                    len(failed), shown)

    for ws_id, target_id, label in matches:
        action = f"remove workspace assignment {label} from workspace {ws_id}"
        if dry_run:
            log.info("  DRY-RUN %s", action)
            ok += 1
            continue
        try:
            ac.workspace_assignment.delete(workspace_id=ws_id, principal_id=target_id)
            log.info("  OK %s", action)
            ok += 1
        except Exception as e:  # noqa: BLE001
            log.error("  ERR %s: %s", action, e)
            err += 1
    return ok, err


# --------------------------------------------------------------------------- #
# Deactivate the user account (final step)
# --------------------------------------------------------------------------- #
def deactivate_users(ac, users_by_email, dry_run) -> tuple[int, int]:
    """Set active=false on each user (reversible; the account is not deleted). Run last,
    after access is removed. Skips users already inactive."""
    ok = err = 0
    for email, user in users_by_email.items():
        full = ac.users.get(user.id)  # re-fetch: list() may not populate `active`
        if full.active is False:
            log.info("  %s already inactive — skipping", email)
            continue
        if dry_run:
            log.info("  DRY-RUN deactivate %s (set active=false)", email)
            ok += 1
            continue
        try:
            # NOTE: use the raw SCIM PATCH rather than iam.Patch — the SDK's
            # Patch.as_dict() drops a boolean `value=False`, producing a value-less op
            # the API rejects. This explicit body is the form the account SCIM accepts.
            ac.api_client.do(
                "PATCH",
                f"/api/2.0/accounts/{ac.config.account_id}/scim/v2/Users/{user.id}",
                body={
                    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                    "Operations": [{"op": "replace", "path": "active", "value": False}],
                },
            )
            log.info("  OK deactivated %s", email)
            ok += 1
        except Exception as e:  # noqa: BLE001
            log.error("  ERR deactivate %s: %s", email, e)
            err += 1
    return ok, err


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", required=True, help="Databricks CLI profile for the ACCOUNT console")
    p.add_argument("--admins", required=True, help="comma-separated departed-admin emails/userNames")
    p.add_argument("--scope", default=",".join(ALL_SCOPES),
                   help=f"comma-separated subset of: {', '.join(ALL_SCOPES)} (default: all)")
    p.add_argument("--workspace-ids", default="", help="restrict workspace-assignment scan to these ids")
    p.add_argument("--workers", type=int, default=8, help="concurrent workspace-assignment lookups")
    p.add_argument("--deactivate", action="store_true",
                   help="final step: set the user account(s) to inactive (active=false) after "
                        "removing access. Reversible; does not delete the account.")
    p.add_argument("--execute", action="store_true", help="apply changes (default is dry-run)")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S", stream=sys.stderr,
    )
    logging.getLogger("databricks.sdk").setLevel(logging.WARNING)

    scope = {s.strip() for s in args.scope.split(",") if s.strip()}
    bad = scope - set(ALL_SCOPES)
    if bad:
        sys.exit(f"unknown scope(s): {', '.join(sorted(bad))}; valid: {', '.join(ALL_SCOPES)}")
    emails = sorted({e.strip().lower() for e in args.admins.split(",") if e.strip()})
    ws_filter = {int(x) for x in args.workspace_ids.split(",") if x.strip()}
    dry_run = not args.execute

    ac = AccountClient(profile=args.profile)
    log.info("Account: %s", ac.config.account_id)
    log.info("Mode   : %s", "DRY-RUN (no changes)" if dry_run else "EXECUTE")
    log.info("Scope  : %s", ", ".join(sorted(scope)))
    log.info("Resolving %d departed admin(s)…", len(emails))

    users_by_email = resolve_users(ac, emails)
    if not users_by_email:
        log.error("No departed admins resolved; nothing to do.")
        return 1

    totals = {"ok": 0, "err": 0}

    def tally(ok, err):
        totals["ok"] += ok
        totals["err"] += err

    if "groups" in scope:
        log.info("== Group memberships ==")
        membership = build_group_membership(ac, {u.id for u in users_by_email.values()}, args.workers)
        for email, user in users_by_email.items():
            groups = membership.get(user.id, [])
            log.info("%s: in %d account group(s)", email, len(groups))
            tally(*remove_group_memberships(ac, user, groups, dry_run))

    if "roles" in scope or "entitlements" in scope:
        log.info("== Direct roles / entitlements ==")
        for email, user in users_by_email.items():
            tally(*remove_direct_roles_entitlements(
                ac, user, "roles" in scope, "entitlements" in scope, dry_run))

    if "workspace_assignments" in scope:
        log.info("== Workspace assignments ==")
        tally(*remove_workspace_assignments(ac, users_by_email, dry_run, ws_filter, args.workers))

    # Final step: deactivate the account(s) only after access has been removed.
    if args.deactivate:
        log.info("== Deactivate user account(s) [final step] ==")
        tally(*deactivate_users(ac, users_by_email, dry_run))

    print(f"\nDone. {'(dry-run) ' if dry_run else ''}actions ok/dry={totals['ok']} err={totals['err']}")
    if dry_run:
        print("Re-run with --execute to apply.")
    return 0 if totals["err"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
