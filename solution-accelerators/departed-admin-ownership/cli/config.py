"""Configuration and client bootstrap for the departed-admin-ownership CLI.

Network model: workspace *compute* cannot reach the account console, but the host
running this CLI can. So we (1) authenticate to a reachable "bootstrap" workspace via
a normal Databricks CLI profile, (2) read the account service-principal credentials
from a secret scope in that workspace, (3) build an AccountClient from the CLI host,
and (4) use AccountClient.get_workspace_client() for the per-workspace sweep. All
account-console traffic and token exchange happens from the CLI host, not from
workspace compute.
"""

from __future__ import annotations

import base64
import dataclasses
import logging
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import AccountClient, WorkspaceClient

log = logging.getLogger("dao.config")


@dataclasses.dataclass
class Config:
    # --- bootstrap: how the CLI reaches the account SP creds ---
    bootstrap_profile: str  # Databricks CLI profile for a reachable workspace
    secret_scope: str  # scope in that workspace holding the SP creds
    secret_key_client_id: str
    secret_key_client_secret: str
    account_host: str  # e.g. https://accounts.cloud.databricks.com
    account_id: str

    # --- who / what ---
    departed_admins: list[str]
    target_group: str

    # --- scope ---
    scope_uc: bool
    scope_ws: bool
    scope_wsfs: bool
    scope_run_as: bool

    # --- selection ---
    workspace_ids: list[int]  # 1..n workspaces; empty = all in the account
    workspace_workers: int  # how many workspaces to crawl/transfer in parallel
    skip_catalogs: list[str]

    # --- UC crawl ---
    sql_warehouse_ids: dict[int, str]  # workspace_id -> warehouse_id (SQL fast path)

    # --- WSFS tuning ---
    wsfs_max_depth: int
    wsfs_workers: int

    # --- run_as ---
    run_as_sp_map: dict[int, str]  # workspace_id -> SP application id
    grant_run_as_sp_perms: bool

    output_dir: Path
    raw: dict[str, Any]

    @classmethod
    def load(cls, path: str) -> Config:
        with open(path) as fh:
            d = yaml.safe_load(fh) or {}
        scope = d.get("scope", {}) or {}
        boot = d.get("bootstrap", {}) or {}

        def _int_keyed(m):
            return {int(k): str(v).strip() for k, v in (m or {}).items()}

        return cls(
            bootstrap_profile=boot.get("profile", "DEFAULT"),
            secret_scope=str(boot.get("secret_scope") or "").strip(),
            secret_key_client_id=boot.get("secret_key_client_id", "account_sp_client_id"),
            secret_key_client_secret=boot.get(
                "secret_key_client_secret", "account_sp_client_secret"
            ),
            account_host=str(boot.get("account_host") or "").strip(),
            account_id=str(boot.get("account_id") or "").strip(),
            departed_admins=[a.strip().lower() for a in d.get("departed_admins", []) if a.strip()],
            target_group=(d.get("target_group") or "").strip(),
            scope_uc=bool(scope.get("unity_catalog", True)),
            scope_ws=bool(scope.get("workspace_objects", True)),
            scope_wsfs=bool(scope.get("workspace_files", False)),
            scope_run_as=bool(scope.get("job_run_as", False)),
            workspace_ids=[int(w) for w in d.get("workspace_ids", []) or []],
            workspace_workers=max(1, int(d.get("workspace_workers", 1) or 1)),
            skip_catalogs=[c.lower() for c in d.get("skip_catalogs", []) or []],
            sql_warehouse_ids=_int_keyed(d.get("sql_warehouse_ids")),
            wsfs_max_depth=int(d.get("wsfs_max_depth", 0) or 0),
            wsfs_workers=max(1, int(d.get("wsfs_workers", 8) or 8)),
            run_as_sp_map=_int_keyed(d.get("run_as_sp_map")),
            grant_run_as_sp_perms=bool(d.get("grant_run_as_sp_perms", False)),
            output_dir=Path(d.get("output_dir", "./out")).expanduser(),
            raw=d,
        )

    def validate(self) -> None:
        errs = []
        if not self.departed_admins:
            errs.append("departed_admins is empty")
        if not self.target_group:
            errs.append("target_group is empty")
        if not self.secret_scope:
            errs.append("bootstrap.secret_scope is empty (needed to read the account SP creds)")
        if not self.account_host:
            errs.append("bootstrap.account_host is empty")
        if not (self.scope_uc or self.scope_ws or self.scope_wsfs or self.scope_run_as):
            errs.append("all scopes disabled — nothing to do")
        if self.scope_run_as and not self.run_as_sp_map:
            errs.append(
                "scope.job_run_as is on but run_as_sp_map is empty — provide a "
                "per-workspace SP map {workspace_id: sp_application_id}"
            )
        if errs:
            raise SystemExit("Config errors:\n  - " + "\n  - ".join(errs))


def account_client(cfg: Config) -> AccountClient:
    """Bootstrap the AccountClient: read the SP creds from the bootstrap workspace's
    secret scope, then build an account-scoped client from the CLI host."""
    boot = WorkspaceClient(profile=cfg.bootstrap_profile)
    log.info("Bootstrap workspace: %s", boot.config.host)

    def _secret(key: str) -> str:
        resp = boot.secrets.get_secret(cfg.secret_scope, key)
        # Secrets API returns base64-encoded values.
        return base64.b64decode(resp.value).decode()

    client_id = _secret(cfg.secret_key_client_id)
    client_secret = _secret(cfg.secret_key_client_secret)
    ac = AccountClient(
        host=cfg.account_host,
        account_id=cfg.account_id or None,
        client_id=client_id,
        client_secret=client_secret,
    )
    log.info("AccountClient ready for account %s", ac.config.account_id)
    return ac
