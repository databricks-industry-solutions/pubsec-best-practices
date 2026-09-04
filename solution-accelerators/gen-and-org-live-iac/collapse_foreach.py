#!/usr/bin/env python3
"""
collapse_foreach.py — the second refactor pass: fold the exporter's flat, one-
block-per-resource output into idiomatic `for_each` maps.

Run it on a plane root produced by plane_transform.py. For each targeted resource
type it turns N separate `resource "T" "name" { ... }` blocks into:

    locals { <plural> = { "name" = { ...attrs... }, ... } }
    resource "T" "this" {
      for_each = local.<plural>
      attr = each.value.attr
      dynamic "block" { for_each = ...; content { ... } }   # nested blocks
    }

Crucially it preserves the ADOPT property:
  * every resource's native import block is rewritten to the new for_each
    address  ->  import { to = T.this["name"], id = "..." }
  * every CROSS-REFERENCE to a collapsed resource (e.g. a schema's
    catalog_name = databricks_catalog.foo.id, or a grant's catalog = ...) is
    rewritten to databricks_catalog.this["foo"].id, so the dependency graph and
    a clean plan survive the collapse.

Nested blocks are handled generically via `dynamic` blocks; `depends_on` is
dropped (the reference rewrite preserves the real dependency). Run
`terraform fmt` afterwards — emitted indentation is intentionally loose.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plane_transform import split_top_level_blocks, Block  # noqa: E402

DEFAULT_TYPES = "databricks_cluster,databricks_sql_endpoint,databricks_catalog,databricks_schema,databricks_grants"
SCAFFOLD_FILES = {"providers.tf", "versions.tf", "backend.tf", "data.tf",
                  "_exporter_shared.tf"}
_ID_RE = re.compile(r'\bid\s*=\s*"([^"]*)"')
_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")
_HEREDOC_RE = re.compile(r"<<-?\s*([A-Za-z_][A-Za-z0-9_]*)")


def _skip_heredoc(s: str, i: int) -> int | None:
    """If s[i:] opens a heredoc, return the index just past its terminator; else None."""
    m = _HEREDOC_RE.match(s, i)
    if not m:
        return None
    nl = s.find("\n", i)
    j = len(s) if nl < 0 else nl + 1
    term = re.compile(r"^[ \t]*" + re.escape(m.group(1)) + r"[ \t]*$", re.M)
    mm = term.search(s, j)
    return mm.end() if mm else len(s)


# ─── HCL body parser (attributes + nested blocks) ────────────────────────────
def _read_value(s: str, i: int) -> tuple[str, int]:
    """Read an attribute RHS starting at i; ends at newline while depth==0."""
    n = len(s)
    while i < n and s[i] in " \t":
        i += 1
    start = i
    depth = 0
    while i < n:
        c = s[i]
        if c == "#" or (c == "/" and i + 1 < n and s[i + 1] == "/"):
            nl = s.find("\n", i)
            if depth == 0:
                return s[start:i].strip(), (n if nl < 0 else nl + 1)
            i = n if nl < 0 else nl + 1
            continue
        if c == "/" and i + 1 < n and s[i + 1] == "*":
            e = s.find("*/", i + 2)
            i = n if e < 0 else e + 2
            continue
        if c == "<" and i + 1 < n and s[i + 1] == "<":
            h = _skip_heredoc(s, i)
            if h is not None:
                i = h
                continue
        if c == '"':
            i += 1
            while i < n:
                if s[i] == "\\":
                    i += 2
                    continue
                if s[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if c in "([{":
            depth += 1
            i += 1
            continue
        if c in ")]}":
            depth -= 1
            i += 1
            continue
        if c == "\n" and depth == 0:
            return s[start:i].strip(), i + 1
        i += 1
    return s[start:].strip(), n


def _read_braced(s: str, i: int) -> tuple[str, int]:
    """s[i] == '{'; return (inner_text, index_after_matching_brace)."""
    n = len(s)
    depth = 0
    j = i
    while j < n:
        c = s[j]
        if c == "#" or (c == "/" and j + 1 < n and s[j + 1] == "/"):
            nl = s.find("\n", j)
            j = n if nl < 0 else nl + 1
            continue
        if c == "<" and j + 1 < n and s[j + 1] == "<":
            h = _skip_heredoc(s, j)
            if h is not None:
                j = h
                continue
        if c == '"':
            j += 1
            while j < n:
                if s[j] == "\\":
                    j += 2
                    continue
                if s[j] == '"':
                    j += 1
                    break
                j += 1
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return s[i + 1:j], j + 1
        j += 1
    return s[i + 1:], n


def parse_body(body: str) -> list[tuple[str, str, str]]:
    """Return ordered items: ('attr', name, value) | ('block', name, inner)."""
    items: list[tuple[str, str, str]] = []
    n = len(body)
    i = 0
    while i < n:
        c = body[i]
        if c.isspace():
            i += 1
            continue
        if c == "#" or (c == "/" and i + 1 < n and body[i + 1] == "/"):
            nl = body.find("\n", i)
            i = n if nl < 0 else nl + 1
            continue
        m = _IDENT_RE.match(body, i)
        if not m:
            i += 1
            continue
        name = m.group(0)
        i = m.end()
        while i < n and body[i] in " \t":
            i += 1
        if i < n and body[i] == "=":
            val, i = _read_value(body, i + 1)
            items.append(("attr", name, val))
        elif i < n and body[i] == "{":
            inner, i = _read_braced(body, i)
            items.append(("block", name, inner))
        else:
            nl = body.find("\n", i)
            i = n if nl < 0 else nl + 1
    return items


# ─── schema inference across all instances of a type ─────────────────────────
def collect_schema(bodies: list[list]) -> dict:
    attrs: list[str] = []
    seen: set[str] = set()
    block_max: dict[str, int] = defaultdict(int)
    block_bodies: dict[str, list] = defaultdict(list)
    for items in bodies:
        counts: dict[str, int] = defaultdict(int)
        for kind, name, val in items:
            if name == "depends_on":
                continue
            if kind == "attr":
                if name not in seen:
                    seen.add(name)
                    attrs.append(name)
            else:
                counts[name] += 1
                block_bodies[name].append(parse_body(val))
        for name, c in counts.items():
            block_max[name] = max(block_max[name], c)
    blocks = {name: {"max": block_max[name], "schema": collect_schema(block_bodies[name])}
              for name in block_max}
    return {"attrs": attrs, "blocks": blocks}


def build_object(items: list, schema: dict, rewrite) -> str:
    amap = {name: val for kind, name, val in items if kind == "attr" and name != "depends_on"}
    lines = ["{"]
    for a in schema["attrs"]:
        v = rewrite(amap[a]) if a in amap else "null"
        lines.append(f"{a} = {v}")
    for bname, bspec in schema["blocks"].items():
        blks = [parse_body(val) for kind, name, val in items
                if kind == "block" and name == bname]
        if bspec["max"] <= 1:
            lines.append(f"{bname} = " + (build_object(blks[0], bspec["schema"], rewrite)
                                          if blks else "null"))
        else:
            objs = [build_object(b, bspec["schema"], rewrite) for b in blks]
            lines.append(f"{bname} = [{', '.join(objs)}]")
    lines.append("}")
    return "\n".join(lines)


def emit_content(iter_name: str, schema: dict, indent: str) -> list[str]:
    out = []
    for a in schema["attrs"]:
        out.append(f"{indent}{a} = {iter_name}.value.{a}")
    for sub, spec in schema["blocks"].items():
        fe = (f"{iter_name}.value.{sub} == null ? [] : [{iter_name}.value.{sub}]"
              if spec["max"] <= 1 else f"{iter_name}.value.{sub}")
        out.append(f'{indent}dynamic "{sub}" {{')
        out.append(f"{indent}  for_each = {fe}")
        out.append(f"{indent}  content {{")
        out += emit_content(sub, spec["schema"], indent + "    ")
        out.append(f"{indent}  }}")
        out.append(f"{indent}}}")
    return out


def emit_resource(rtype: str, plural: str, schema: dict) -> str:
    L = [f'resource "{rtype}" "this" {{', f"  for_each = local.{plural}", ""]
    for a in schema["attrs"]:
        L.append(f"  {a} = each.value.{a}")
    for bname, spec in schema["blocks"].items():
        fe = (f"each.value.{bname} == null ? [] : [each.value.{bname}]"
              if spec["max"] <= 1 else f"each.value.{bname}")
        L += ["", f'  dynamic "{bname}" {{', f"    for_each = {fe}", "    content {"]
        L += emit_content(bname, spec["schema"], "      ")
        L += ["    }", "  }"]
    L.append("}")
    return "\n".join(L)


def plural_of(rtype: str) -> str:
    base = rtype[len("databricks_"):] if rtype.startswith("databricks_") else rtype
    return base if base.endswith("s") else base + "s"


# ─── driver ──────────────────────────────────────────────────────────────────
def collapse_root(root: Path, types: list[str]) -> None:
    tf_files = [f for f in sorted(root.glob("*.tf")) if f.name not in SCAFFOLD_FILES]

    # parse every file into blocks; index resources/imports by type
    resources: dict[str, list[Block]] = defaultdict(list)   # rtype -> [resource Block]
    ids: dict[str, str] = {}                                # "T.name" -> import id
    other_files: list[Path] = []                            # non-collapsed resource files
    for f in tf_files:
        blocks = [Block(b) for b in split_top_level_blocks(f.read_text())]
        rtypes_here = {b.rtype for b in blocks if b.kind == "resource"}
        for b in blocks:
            if b.kind == "resource":
                resources[b.rtype].append(b)
            elif b.kind == "import" and b.import_to:
                base = re.sub(r"\[.*\]$", "", b.import_to)
                m = _ID_RE.search(b.text)
                if m:
                    ids[base] = m.group(1)
        if not (rtypes_here & set(types)):
            other_files.append(f)

    present = [t for t in types if resources.get(t)]
    if not present:
        print(f"  {root.name}: no target types present, skipping")
        return

    # global rename map: databricks_T.name -> databricks_T.this["name"]
    names_by_type: dict[str, set[str]] = {t: {b.name for b in resources[t]} for t in present}

    def rewrite(text: str) -> str:
        def sub(m: re.Match) -> str:
            t, name = m.group(1), m.group(2)
            full = f"databricks_{t}"
            if full in names_by_type and name in names_by_type[full]:
                return f'databricks_{t}.this["{name}"]'
            return m.group(0)
        return re.sub(r"databricks_([a-z_]+)\.([A-Za-z0-9_]+)", sub, text)

    # regenerate each collapsed type's file
    for rtype in present:
        blocks = resources[rtype]
        plural = plural_of(rtype)
        bodies = [parse_body(_read_braced(b.text, b.text.index("{"))[0]) for b in blocks]
        schema = collect_schema(bodies)

        loc = ["locals {", f"  {plural} = {{"]
        import_ids: list[tuple[str, str]] = []
        for b, items in zip(blocks, bodies):
            obj = build_object(items, schema, rewrite)
            loc.append(f'"{b.name}" = {obj}')
            addr = f"{rtype}.{b.name}"
            if addr in ids:
                import_ids.append((b.name, ids[addr]))
        loc.append("  }")
        # import-id map, consumed by ONE for_each import block below.
        loc.append(f"  {plural}_import_ids = {{")
        for k, iid in import_ids:
            loc.append(f'"{k}" = "{iid}"')
        loc += ["  }", "}"]

        # A single for_each import block — NOT one import block per instance.
        # Terraform honors only the FIRST of several import blocks aimed at the
        # same for_each resource; the for_each form adopts every instance.
        import_block = (f"import {{\n  for_each = local.{plural}_import_ids\n"
                        f"  to       = {rtype}.this[each.key]\n"
                        f"  id       = each.value\n}}")

        content = (import_block + "\n\n" +
                   "\n".join(loc) + "\n\n" +
                   emit_resource(rtype, plural, schema) + "\n")
        (root / f"{rtype}.tf").write_text(content)
        print(f"  {root.name}: {rtype}  {len(blocks)} blocks -> for_each[{len(blocks)}]")

    # rewrite cross-references in the non-collapsed resource files
    for f in other_files:
        txt = f.read_text()
        new = rewrite(txt)
        if new != txt:
            f.write_text(new)
            print(f"  {root.name}: rewrote refs in {f.name}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tree", required=True, help="generated/databricks-terraform")
    ap.add_argument("--types", default=DEFAULT_TYPES, help="comma-separated resource types to collapse")
    ap.add_argument("--root", help="only this environments/<root> (default: all)")
    args = ap.parse_args()

    types = [t.strip() for t in args.types.split(",") if t.strip()]
    envs = Path(args.tree) / "environments"
    roots = [envs / args.root] if args.root else sorted(p for p in envs.iterdir() if p.is_dir())
    print(f"Collapsing types: {', '.join(types)}")
    for r in roots:
        if r.name == "_unclassified":
            continue
        collapse_root(r, types)
    print("Done. Run `terraform fmt -recursive` on the tree.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
