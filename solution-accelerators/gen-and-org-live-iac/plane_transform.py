#!/usr/bin/env python3
"""
plane_transform.py — turn a flat Databricks exporter dump into a plane-based
repository tree of five lifecycle planes (see README.md).

The Databricks provider's experimental resource exporter emits a FLAT set of
`resource` + native `import {}` blocks with its own addresses. This script:

  1. Splits every *.tf in the export dir into top-level HCL blocks (a small
     brace/quote/comment/heredoc-aware scanner — no HCL library needed; the raw
     block text is preserved verbatim so it stays valid, complete Terraform).
  2. Classifies each resource into ONE of the five lifecycle planes
     (account-infra / identity / uc-foundation / workspace-<env> /
     uc-governance-<domain>) using plane_rules.yaml — grants by the securable
     they target, everything else by resource type.
  3. Re-emits the classified blocks into environments/<plane>/ roots, carrying
     each resource's native import block alongside it, and scaffolds
     providers/versions/backend/data(remote_state) for every populated root.

The point of the demo: the exporter answers "what is really out there"; this
script answers "where does each piece live in the target design" — with a plan
that reads as ADOPT (import), never create/destroy.

Nothing is guessed: a resource type absent from plane_rules.yaml lands in
environments/_unclassified/ for a human to place.
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("PyYAML is required: pip install pyyaml")


# ─────────────────────────────────────────────────────────────────────────────
# HCL block splitter — brace-balanced, aware of strings / comments / heredocs.
# ─────────────────────────────────────────────────────────────────────────────
_HEREDOC_RE = re.compile(r"<<-?\s*([A-Za-z_][A-Za-z0-9_]*)")


def split_top_level_blocks(text: str) -> list[str]:
    """Return the verbatim text of each top-level `{...}` block in `text`."""
    blocks: list[str] = []
    n = len(text)
    i = 0
    depth = 0
    start: int | None = None

    while i < n:
        c = text[i]

        # line comment
        if c == "#" or (c == "/" and i + 1 < n and text[i + 1] == "/"):
            nl = text.find("\n", i)
            i = n if nl == -1 else nl + 1
            continue
        # block comment
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            end = text.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        # double-quoted string (with escapes)
        if c == '"':
            i += 1
            while i < n:
                if text[i] == "\\":
                    i += 2
                    continue
                if text[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        # heredoc
        if c == "<" and i + 1 < n and text[i + 1] == "<":
            m = _HEREDOC_RE.match(text, i)
            if m:
                tag = m.group(1)
                nl = text.find("\n", i)
                i = n if nl == -1 else nl + 1
                term = re.compile(r"^[ \t]*" + re.escape(tag) + r"[ \t]*$", re.M)
                mm = term.search(text, i)
                i = mm.end() if mm else n
                continue
        # braces
        if c == "{":
            depth += 1
            i += 1
            continue
        if c == "}":
            depth -= 1
            i += 1
            if depth == 0 and start is not None:
                blocks.append(text[start:i])
                start = None
            continue
        # first significant char of a new top-level block
        if depth == 0 and start is None and not c.isspace():
            start = i
        i += 1

    return blocks


# ─────────────────────────────────────────────────────────────────────────────
# Block metadata
# ─────────────────────────────────────────────────────────────────────────────
_RESOURCE_RE = re.compile(r'^\s*resource\s+"([^"]+)"\s+"([^"]+)"')
_DATA_RE = re.compile(r'^\s*data\s+"([^"]+)"\s+"([^"]+)"')
_IMPORT_TO_RE = re.compile(r"^\s*to\s*=\s*(.+?)\s*$", re.M)
_KIND_RE = re.compile(r"^\s*([A-Za-z_]+)")
# securable attribute inside a grants/grant block, e.g.  catalog = "x"
_SECURABLE_RE = re.compile(
    r"^\s*(metastore|external_location|storage_credential|connection|share|"
    r"recipient|foreign_connection|catalog|schema|volume|table|function|model|"
    r"registered_model|materialized_view)\s*=",
    re.M,
)


class Block:
    __slots__ = ("kind", "rtype", "name", "text", "import_to")

    def __init__(self, text: str):
        self.text = text
        self.kind = None      # resource | import | data | provider | terraform | ...
        self.rtype = None     # resource/data type, e.g. databricks_cluster
        self.name = None      # local name
        self.import_to = None # for import blocks: the target address string

        km = _KIND_RE.match(text)
        self.kind = km.group(1) if km else "?"

        if self.kind == "resource":
            m = _RESOURCE_RE.match(text)
            if m:
                self.rtype, self.name = m.group(1), m.group(2)
        elif self.kind == "data":
            m = _DATA_RE.match(text)
            if m:
                self.rtype, self.name = m.group(1), m.group(2)
        elif self.kind == "import":
            m = _IMPORT_TO_RE.search(text)
            if m:
                self.import_to = m.group(1).strip()

    @property
    def address(self) -> str | None:
        if self.kind == "resource" and self.rtype and self.name:
            return f"{self.rtype}.{self.name}"
        return None

    def securable(self) -> str | None:
        m = _SECURABLE_RE.search(self.text)
        return m.group(1) if m else None


# ─────────────────────────────────────────────────────────────────────────────
# Classifier
# ─────────────────────────────────────────────────────────────────────────────
class Rules:
    def __init__(self, cfg: dict):
        self.type_to_plane: dict[str, str] = {}
        for plane, types in (cfg.get("planes") or {}).items():
            for t in types:
                self.type_to_plane[t] = plane
        self.grant_types = set(cfg.get("grant_resource_types") or [])
        self.grant_securable_planes = cfg.get("grant_securable_planes") or {}
        self.grant_default_plane = cfg.get("grant_default_plane", "uc-governance")
        self.permissions_type = cfg.get("permissions_resource_type", "databricks_permissions")
        self.permissions_plane = cfg.get("permissions_plane", "workspace")
        self.skip_types = set(cfg.get("skip_resource_types") or [])
        self.ws_env = cfg.get("default_workspace_env", "dev")
        self.gov_domain = cfg.get("default_governance_domain", "default")

    def plane_for(self, b: Block) -> tuple[str, str | None]:
        """Return (plane, reason). plane is None-ish 'unclassified' if unknown."""
        t = b.rtype
        if t in self.grant_types:
            sec = b.securable()
            plane = self.grant_securable_planes.get(sec, self.grant_default_plane)
            return plane, f"grant on {sec or '?'}"
        if t == self.permissions_type:
            return self.permissions_plane, "workspace ACL"
        if t in self.type_to_plane:
            return self.type_to_plane[t], "type"
        return "_unclassified", "unknown type"

    def env_dir(self, plane: str) -> str:
        if plane == "workspace":
            return f"workspace-{self.ws_env}"
        if plane == "uc-governance":
            return f"uc-governance-{self.gov_domain}"
        return plane


# ─────────────────────────────────────────────────────────────────────────────
# Scaffolding emitters
# ─────────────────────────────────────────────────────────────────────────────
PROVIDER_VERSION = "~> 1.130"  # pin in real use; verify against your TFE run image

_ACCOUNT_HOST = "https://accounts.cloud.databricks.com"


def versions_tf() -> str:
    return f'''terraform {{
  required_version = ">= 1.5"
  required_providers {{
    databricks = {{
      source  = "databricks/databricks"
      version = "{PROVIDER_VERSION}"
    }}
  }}
}}
'''


def backend_tf(plane_dir: str) -> str:
    return f'''# TFE/HCP remote backend — one workspace per root state.
# terraform {{
#   backend "remote" {{
#     organization = "your-org"
#     workspaces {{ name = "databricks-{plane_dir}" }}
#   }}
# }}
'''


def providers_tf(plane: str) -> str:
    account_level = plane in ("account-infra", "identity")
    if account_level:
        return f'''# Account-level plane — authenticates against the accounts console.
provider "databricks" {{
  host          = "{_ACCOUNT_HOST}"
  account_id    = var.databricks_account_id
  # auth via env: DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET (account-admin SP)
}}

variable "databricks_account_id" {{
  type = string
}}
'''
    if plane == "uc-foundation":
        return '''# UC-foundation authenticates workspace-level against a metastore-attached
# workspace (metastore-scoped securables are managed through any such workspace).
provider "databricks" {
  host = var.databricks_workspace_host
}

variable "databricks_workspace_host" {
  type = string
}
'''
    # workspace + uc-governance: workspace-scoped
    return '''provider "databricks" {
  host = var.databricks_workspace_host
}

variable "databricks_workspace_host" {
  type = string
}
'''


# remote_state wiring per the guide's TFE run-trigger DAG
_DATA_DEPS = {
    "uc-foundation": ["account-infra"],
    "workspace": ["account-infra", "identity"],
    "uc-governance": ["uc-foundation", "identity"],
}


def data_tf(plane: str) -> str:
    deps = _DATA_DEPS.get(plane)
    if not deps:
        return ""
    lines = [
        "# Cross-plane reads. Prefer resolving identity BY NAME (below) over",
        "# threading ids across remote_state — that loose coupling is what makes",
        "# the plane split cheap to operate.",
        "",
    ]
    for d in deps:
        lines.append(f'''data "terraform_remote_state" "{d.replace("-", "_")}" {{
  backend = "remote"
  config = {{
    organization = "your-org"
    workspaces = {{ name = "databricks-{d}" }}
  }}
}}
''')
    if plane in ("workspace", "uc-governance"):
        lines.append('''# Resolve groups by stable display name — no group_ids over a state boundary.
# variable "principals" { type = map(object({ display_name = string })) }
# data "databricks_group" "by_name" {
#   for_each     = var.principals
#   display_name = each.value.display_name
# }
''')
    return "\n".join(lines)


_PLANE_BLURB = {
    "account-infra": "MWS workspaces/networks/storage/credentials + metastore. Very low churn, catastrophic blast radius. Keep restrict_destroy armed.",
    "identity": "Account groups / service principals / SCIM. Applies first; referenced by name everywhere else.",
    "uc-foundation": "Metastore-scoped UC plumbing: storage credentials, external locations, connections, shares, metastore-level grants.",
    "workspace": "Workspace-local compute + config: clusters, policies, pools, SQL warehouses, secret scopes, folders, ACLs.",
    "uc-governance": "Catalogs / schemas / volumes and their grants. Highest churn (grants especially).",
}


def plane_readme(plane: str, env_dir: str, counts: dict[str, int]) -> str:
    total = sum(counts.values())
    rows = "\n".join(f"| `{t}` | {c} |" for t, c in sorted(counts.items()))
    return f'''# environments/{env_dir}

Plane: **{plane}** — {_PLANE_BLURB.get(plane, "")}

Adopted from a live export: **{total}** resources.

| resource type | count |
|---------------|-------|
{rows}

The `*.tf` here are the exporter's verbatim blocks, each paired with its native
`import {{}}` block so `terraform plan` adopts live state rather than creating it.

**Refinement not yet applied (manual, per guide):** collapse repeated resources
into `for_each` maps, move them behind `module.{plane.replace("-", "_")}`, and
resolve groups by display name. Until then a plan is clean adopt/no-op; keep the
zero-destroy gate on.
'''


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--exported-dir", required=True, help="dir of *.tf from the exporter")
    ap.add_argument("--out-dir", required=True, help="output root for the plane tree")
    ap.add_argument("--config", default=str(Path(__file__).with_name("plane_rules.yaml")))
    ap.add_argument("--dry-run", action="store_true", help="classify and report, write nothing")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    rules = Rules(cfg)

    exported = Path(args.exported_dir)
    tf_files = sorted(exported.rglob("*.tf"))
    if not tf_files:
        sys.exit(f"no *.tf found under {exported}")

    resources: list[Block] = []
    imports: list[Block] = []
    shared: list[Block] = []          # data / locals / variable / output the exporter emitted
    dropped_kinds: dict[str, int] = defaultdict(int)

    for f in tf_files:
        for raw in split_top_level_blocks(f.read_text()):
            b = Block(raw)
            if b.kind == "resource":
                resources.append(b)
            elif b.kind == "import":
                imports.append(b)
            elif b.kind in ("data", "locals", "variable", "output"):
                shared.append(b)
            else:  # provider / terraform / moved — we regenerate these
                dropped_kinds[b.kind] += 1

    # de-dupe resources by address — overlapping export passes (e.g. an account
    # pass and a workspace pass whose services both include uc-catalogs) can emit
    # the same securable twice; a duplicate `resource` block is a hard error.
    seen_res: set[str] = set()
    deduped: list[Block] = []
    for b in resources:
        a = b.address
        if a and a in seen_res:
            continue
        if a:
            seen_res.add(a)
        deduped.append(b)
    resources = deduped

    # address -> import block. The exporter can emit the SAME import twice (e.g.
    # a resource discovered via two paths); keep only the first per address, or
    # Terraform errors "Duplicate import configuration".
    imports_by_addr: dict[str, list[Block]] = defaultdict(list)
    seen_import: set[str] = set()
    for imp in imports:
        if imp.import_to:
            base = re.sub(r"\[.*\]$", "", imp.import_to)  # strip [key]
            if base in seen_import:
                continue
            seen_import.add(base)
            imports_by_addr[base].append(imp)

    # ── IAM scoping: drop skipped types (e.g. individual users), and cascade-drop
    # anything that only references one (e.g. a user's group_member entry) so no
    # reference is left dangling. SP and nested-group memberships survive.
    skipped_report: dict[str, int] = defaultdict(int)
    if rules.skip_types:
        skip_pats = [re.compile(rf"\b{re.escape(t)}\.") for t in rules.skip_types]
        kept: list[Block] = []
        for b in resources:
            if b.rtype in rules.skip_types:
                skipped_report[b.rtype] += 1
            elif any(p.search(b.text) for p in skip_pats):
                skipped_report[f"{b.rtype} (references dropped)"] += 1
            else:
                kept.append(b)
        resources = kept

    # classify
    # plane -> env_dir -> rtype -> list[(resource_text, [import_texts])]
    routed: dict[str, dict[str, dict[str, list[tuple[str, list[str]]]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    plane_type_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    unclassified: list[str] = []

    for b in resources:
        plane, _reason = rules.plane_for(b)
        env_dir = rules.env_dir(plane)
        imps = [i.text for i in imports_by_addr.get(b.address, [])]
        routed[plane][env_dir][b.rtype].append((b.text, imps))
        plane_type_counts[env_dir][b.rtype] += 1
        if plane == "_unclassified":
            unclassified.append(b.address or b.rtype)

    # ── report ──
    print(f"\nParsed {len(resources)} resources, {len(imports)} import blocks, "
          f"{len(shared)} shared (data/locals/var/output) from {len(tf_files)} files.")
    if dropped_kinds:
        print("Regenerated (dropped exporter copies): " +
              ", ".join(f"{k}x{v}" for k, v in dropped_kinds.items()))
    if skipped_report:
        print("Skipped (IAM scoping — users not Terraform-managed): " +
              ", ".join(f"{k}×{v}" for k, v in sorted(skipped_report.items())))
    print("\nPlane classification:")
    print(f"  {'environment root':<28}{'resources':>10}   types")
    for plane in ["account-infra", "identity", "uc-foundation", "workspace",
                  "uc-governance", "_unclassified"]:
        for env_dir, types in sorted(routed.get(plane, {}).items()):
            n = sum(len(v) for v in types.values())
            print(f"  {env_dir:<28}{n:>10}   {', '.join(sorted(types))}")
    if unclassified:
        print(f"\n  ⚠ {len(unclassified)} unclassified -> environments/_unclassified/ : "
              f"{', '.join(sorted(set(unclassified)))}")

    if args.dry_run:
        print("\n[dry-run] nothing written.")
        return 0

    # ── write tree ──
    # Wipe first so a resource type that is no longer produced (e.g. users once
    # IAM-scoped, or a service dropped from the export) leaves no stale *.tf behind.
    root = Path(args.out_dir) / "databricks-terraform"
    if root.exists():
        shutil.rmtree(root)
    envs = root / "environments"

    for plane, env_map in routed.items():
        for env_dir, types in env_map.items():
            d = envs / env_dir
            d.mkdir(parents=True, exist_ok=True)
            res_text_all = ""
            for rtype, items in types.items():
                chunks = []
                for res_text, imp_texts in items:
                    chunks.extend(imp_texts)   # import block first, then its resource
                    chunks.append(res_text)
                    res_text_all += res_text + "\n"
                (d / f"{rtype}.tf").write_text("\n\n".join(chunks) + "\n")
            if plane != "_unclassified":
                (d / "versions.tf").write_text(versions_tf())
                (d / "providers.tf").write_text(providers_tf(plane))
                (d / "backend.tf").write_text(backend_tf(env_dir))
                dt = data_tf(plane)
                if dt:
                    (d / "data.tf").write_text(dt)
                (d / "README.md").write_text(
                    plane_readme(plane, env_dir, dict(plane_type_counts[env_dir])))
            # Carry over exporter `data` blocks ONLY where actually referenced in
            # this root — a blindly-copied data source (e.g. an account group that
            # doesn't exist in a workspace) aborts the whole plan. `variable` and
            # `locals` are deliberately NOT carried: the scaffolding declares the
            # variables each root needs, and a re-emitted exporter variable (e.g.
            # databricks_account_id) would duplicate the scaffolded one.
            inc = [b.text for b in shared
                   if b.kind == "data" and b.rtype and b.name
                   and f"data.{b.rtype}.{b.name}" in res_text_all]
            if inc:
                (d / "_exporter_shared.tf").write_text(
                    "# exporter data sources referenced by this root.\n\n"
                    + "\n\n".join(inc) + "\n")

    # module placeholders + top README
    for plane in _PLANE_BLURB:
        md = root / "modules" / plane.replace("-", "_")
        md.mkdir(parents=True, exist_ok=True)
        (md / "README.md").write_text(
            f"# modules/{plane.replace('-', '_')}\n\n"
            f"Refinement target for the **{plane}** plane. The adopted resources currently\n"
            f"live flat under `environments/`; fold them behind this module as `for_each`\n"
            f"maps as the second refactor pass. {_PLANE_BLURB[plane]}\n")

    (root / "README.md").write_text(_top_readme(routed, plane_type_counts))
    print(f"\n✅ wrote plane tree to {root}")
    return 0


def _top_readme(routed, counts) -> str:
    lines = [
        "# databricks-terraform (generated from a live export)",
        "",
        "Built by `plane_transform.py` from the Databricks provider exporter's flat",
        "output, organized into the five lifecycle planes (see README.md). Each",
        "`environments/<root>/` is one remote Terraform workspace / one state.",
        "",
        "## Adopted inventory",
        "",
        "| environment root | resources |",
        "|------------------|-----------|",
    ]
    for plane in ["account-infra", "identity", "uc-foundation", "workspace",
                  "uc-governance", "_unclassified"]:
        for env_dir, types in sorted(routed.get(plane, {}).items()):
            n = sum(len(v) for v in types.values())
            lines.append(f"| `environments/{env_dir}` | {n} |")
    lines += [
        "",
        "## TFE run-trigger order",
        "",
        "```",
        "account-infra -> identity -> {uc-foundation, workspace-*} -> uc-governance-*",
        "```",
        "",
        "Every resource carries its native `import {}` block: a first plan should be",
        "pure adopt/no-op. Keep the zero-destroy gate on and `restrict_destroy` armed",
        "on account-infra + uc-foundation. Refine flat resources into `for_each` module",
        "maps (see `modules/`) as the next pass.",
    ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
