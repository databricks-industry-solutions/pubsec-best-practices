#!/usr/bin/env python3
"""
bundle_deps.py
==============
Downloads everything needed to install R packages on an air-gapped Databricks
cluster (Ubuntu Noble amd64).  Run this on any internet-connected machine —
Linux, macOS, RHEL, Windows — using only Python stdlib.

Output layout (ready to upload to a Databricks UC volume):
  <out_dir>/
    *.tar.gz          <- pre-built R package binaries (Ubuntu Noble x86_64)
    syslibs/
      debs/           <- .deb files for system libraries
      *.so*           <- extracted shared objects (flat, for dyn.load)

Usage:
  python3 bundle_deps.py                        # defaults below
  python3 bundle_deps.py --packages sf,terra,units,zipcodeR --out ./bundle
"""

import argparse
import gzip
import io
import json
import struct
import sys
import tarfile
import urllib.request
from pathlib import Path
from urllib.parse import urljoin

# ── Configuration ─────────────────────────────────────────────────────────────

POSIT_BINARY_BASE = (
    "https://databricks.packagemanager.posit.co"
    "/cran/latest/bin/linux/noble-x86_64"
)
R_VERSION = "4.4"

# Ubuntu Noble (24.04) amd64 package index URLs
# main + universe cover everything we need
UBUNTU_MIRROR   = "http://archive.ubuntu.com/ubuntu"
UBUNTU_RELEASE  = "noble"
UBUNTU_COMPONENTS = ["main", "universe"]
UBUNTU_ARCH     = "binary-amd64"

# Root system packages whose transitive deps we need
SYSLIB_ROOTS = [
    "libgdal-dev",
    "libudunits2-dev",
    "libgeos-dev",
    "libproj-dev",
]

# R base/recommended packages — never need to be bundled
R_BASE_PKGS = {
    "R", "base", "boot", "class", "cluster", "codetools", "compiler",
    "datasets", "foreign", "grDevices", "graphics", "grid", "KernSmooth",
    "lattice", "MASS", "Matrix", "methods", "mgcv", "nlme", "nnet",
    "parallel", "rpart", "spatial", "splines", "stats", "stats4",
    "survival", "tcltk", "tools", "translations", "utils",
}

# Packages that Databricks DBR pre-installs at potentially wrong versions.
# The Posit PACKAGES index omits these as explicit deps (assumes them present),
# but on the target air-gapped cluster we need our own versions in r_lib.
# Derived empirically from diff between Posit-resolved set and full working set.
DATABRICKS_SUPPLEMENT = [
    "bit", "bit64", "blob", "cachem", "clipr", "crayon", "fastmap",
    "hms", "memoise", "pillar", "pkgconfig", "prettyunits", "progress",
    "purrr", "readr", "RSQLite", "rvest", "s2", "selectr", "stringi",
    "tibble", "tidyselect", "tzdb", "utf8", "vctrs", "vroom",
    "withr", "wk", "xml2",
]

# ── Ubuntu Packages.gz parser ─────────────────────────────────────────────────

def fetch_ubuntu_package_index(mirror, release, components, arch):
    """
    Download and parse Ubuntu Packages.gz for each component.
    Returns dict: package_name -> {version, depends, filename, ...}
    """
    db = {}
    for component in components:
        url = f"{mirror}/dists/{release}/{component}/{arch}/Packages.gz"
        print(f"  Fetching {url}")
        with urllib.request.urlopen(url) as resp:
            raw = gzip.decompress(resp.read()).decode("utf-8", errors="replace")

        # Each stanza is separated by a blank line
        for stanza in raw.strip().split("\n\n"):
            pkg = {}
            current_key = None
            for line in stanza.splitlines():
                if line.startswith(" ") or line.startswith("\t"):
                    # continuation
                    if current_key:
                        pkg[current_key] = pkg.get(current_key, "") + " " + line.strip()
                elif ": " in line:
                    k, _, v = line.partition(": ")
                    current_key = k.strip()
                    pkg[current_key] = v.strip()

            name = pkg.get("Package")
            if name:
                db[name] = pkg  # later component overwrites earlier (universe > main)

    print(f"  Loaded {len(db)} packages from Ubuntu index")
    return db


def parse_depends(dep_str):
    """
    Parse a Depends/Pre-Depends field into a list of package names.
    Handles alternates (a | b) by taking the first, strips version constraints.
    Skips virtual packages we can't resolve (handled by picking first alternative).
    """
    if not dep_str:
        return []
    names = []
    for clause in dep_str.split(","):
        clause = clause.strip()
        # Take first alternative
        alt = clause.split("|")[0].strip()
        # Strip version constraint
        name = alt.split("(")[0].strip()
        if name:
            names.append(name)
    return names


def resolve_syslib_deps(roots, db):
    """
    BFS over Depends + Pre-Depends from root packages.
    Returns set of package names (excluding ones not in db — virtual pkgs).
    """
    queue   = list(roots)
    visited = set()

    while queue:
        name = queue.pop(0)
        if name in visited:
            continue
        visited.add(name)

        if name not in db:
            print(f"    [warn] '{name}' not in Ubuntu index — skipping")
            continue

        pkg = db[name]
        for field in ("Depends", "Pre-Depends"):
            for dep in parse_depends(pkg.get(field, "")):
                if dep not in visited:
                    queue.append(dep)

    # Only keep packages actually in the index (drop virtual/missing)
    return {n for n in visited if n in db}


# ── .deb extractor (pure Python, no shell tools) ──────────────────────────────

AR_MAGIC = b"!<arch>\n"
AR_ENTRY_SIZE = 60


def parse_ar(data: bytes):
    """
    Parse a .deb (ar archive) and yield (filename, content_bytes) for each member.
    ar format: 8-byte magic, then 60-byte fixed headers + content (padded to even).
    Header layout:
      0-15  filename (space-padded)
      16-27 mtime
      28-33 owner uid
      34-39 owner gid
      40-47 mode (octal ascii)
      48-57 size (decimal ascii)
      58-59 magic 0x60 0x0a
    """
    if not data.startswith(AR_MAGIC):
        raise ValueError("Not an ar archive")
    pos = len(AR_MAGIC)
    while pos < len(data):
        if pos + AR_ENTRY_SIZE > len(data):
            break
        header = data[pos:pos + AR_ENTRY_SIZE]
        pos += AR_ENTRY_SIZE

        fname = header[0:16].rstrip(b" \x00/").decode("ascii", errors="replace")
        size  = int(header[48:58].rstrip(b" \x00"))

        content = data[pos:pos + size]
        pos += size
        if size % 2 == 1:
            pos += 1  # ar pads to even byte boundary

        yield fname, content


def extract_so_from_deb(deb_bytes: bytes, dest_dir: Path):
    """
    Extract all .so* files from a .deb into dest_dir (flat — no subdirs).
    .deb = ar archive containing control.tar.* and data.tar.*
    """
    extracted = 0
    for fname, content in parse_ar(deb_bytes):
        if not fname.startswith("data.tar"):
            continue
        # tarfile can handle .tar.xz, .tar.gz, .tar.zst (3.12+) natively
        try:
            with tarfile.open(fileobj=io.BytesIO(content)) as tf:
                for member in tf.getmembers():
                    bname = Path(member.name).name
                    if ".so" in bname and member.isfile():
                        fobj = tf.extractfile(member)
                        if fobj:
                            dest = dest_dir / bname
                            dest.write_bytes(fobj.read())
                            extracted += 1
        except Exception as e:
            print(f"    [warn] could not extract {fname}: {e}")
    return extracted


# ── Posit Package Manager — R package dep resolution + download ───────────────

def fetch_posit_packages_index(binary_base, r_version):
    """
    Fetch the PACKAGES index from Posit's binary repo.
    Returns dict: pkg_name -> {Version, Depends, Imports, LinkingTo, ...}
    """
    url = f"{binary_base}/{r_version}/src/contrib/PACKAGES"
    print(f"  Fetching {url}")
    with urllib.request.urlopen(url) as resp:
        raw = resp.read().decode("utf-8", errors="replace")

    db = {}
    for stanza in raw.strip().split("\n\n"):
        pkg = {}
        for line in stanza.splitlines():
            if ": " in line:
                k, _, v = line.partition(": ")
                pkg[k.strip()] = v.strip()
        name = pkg.get("Package")
        if name:
            db[name] = pkg

    print(f"  Loaded {len(db)} packages from Posit index")
    return db


def parse_r_deps(field):
    """Parse R Depends/Imports/LinkingTo field into package names."""
    if not field:
        return []
    names = []
    for clause in field.split(","):
        name = clause.strip().split("(")[0].strip().split(" ")[0].strip()
        if name and name != "R":
            names.append(name)
    return names


def resolve_r_deps(roots, db, skip):
    """BFS dep resolution over Depends + Imports + LinkingTo."""
    queue   = [p for p in roots if p not in skip]
    visited = set()

    while queue:
        name = queue.pop(0)
        if name in visited or name in skip:
            continue
        visited.add(name)

        if name not in db:
            print(f"    [warn] '{name}' not in Posit index — skipping")
            continue

        pkg = db[name]
        for field in ("Depends", "Imports", "LinkingTo"):
            for dep in parse_r_deps(pkg.get(field, "")):
                if dep not in visited and dep not in skip:
                    queue.append(dep)

    return sorted(visited)


# ── Download helpers ──────────────────────────────────────────────────────────

def download(url, dest: Path, label=None):
    label = label or dest.name
    if dest.exists():
        print(f"  [skip] {label}")
        return True
    try:
        with urllib.request.urlopen(url) as resp:
            dest.write_bytes(resp.read())
        print(f"  [ok]   {label}")
        return True
    except Exception as e:
        print(f"  [FAIL] {label} — {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--packages", default="zipcodeR,sf,terra,units,tidycensus,tigris",
                        help="Comma-separated R packages to bundle (default includes full geospatial stack)")
    parser.add_argument("--out", default="./bundle",
                        help="Output directory (default: ./bundle)")
    parser.add_argument("--r-version", default=R_VERSION,
                        help=f"R version on target cluster (default: {R_VERSION})")
    parser.add_argument("--no-syslibs", action="store_true",
                        help="Skip system library download")
    args = parser.parse_args()

    root_r_pkgs = [p.strip() for p in args.packages.split(",")]
    out_dir     = Path(args.out)
    debs_dir    = out_dir / "syslibs" / "debs"
    so_dir      = out_dir / "syslibs"

    out_dir.mkdir(parents=True, exist_ok=True)
    debs_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. R packages ──────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("Step 1: R packages")
    print(f"{'='*60}")

    posit_db  = fetch_posit_packages_index(POSIT_BINARY_BASE, args.r_version)
    all_r     = resolve_r_deps(root_r_pkgs + DATABRICKS_SUPPLEMENT, posit_db, R_BASE_PKGS)

    print(f"\nResolved {len(all_r)} R packages:")
    for p in all_r:
        print(f"  {p}")

    print(f"\nDownloading R packages to {out_dir}/")
    r_ok, r_fail = 0, 0
    contrib_url = f"{POSIT_BINARY_BASE}/{args.r_version}/src/contrib"
    for pkg in all_r:
        if pkg not in posit_db:
            print(f"  [FAIL] {pkg} — not in index")
            r_fail += 1
            continue
        ver      = posit_db[pkg]["Version"]
        filename = f"{pkg}_{ver}.tar.gz"
        url      = f"{contrib_url}/{filename}"
        if download(url, out_dir / filename):
            r_ok += 1
        else:
            r_fail += 1

    print(f"\nR packages: {r_ok} ok, {r_fail} failed")

    if args.no_syslibs:
        print("\n--no-syslibs set, skipping system library download.")
        return

    # ── 2. System libraries ────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("Step 2: System libraries (Ubuntu Noble amd64)")
    print(f"{'='*60}")

    print(f"\nFetching Ubuntu Noble package index...")
    ubuntu_db = fetch_ubuntu_package_index(
        UBUNTU_MIRROR, UBUNTU_RELEASE, UBUNTU_COMPONENTS, UBUNTU_ARCH
    )

    print(f"\nResolving transitive deps for: {', '.join(SYSLIB_ROOTS)}")
    all_debs = resolve_syslib_deps(SYSLIB_ROOTS, ubuntu_db)
    print(f"Resolved {len(all_debs)} system packages")

    print(f"\nDownloading .deb files to {debs_dir}/")
    deb_ok, deb_fail = 0, 0
    downloaded_debs = []
    for pkg_name in sorted(all_debs):
        pkg  = ubuntu_db[pkg_name]
        fname = Path(pkg["Filename"]).name
        url   = f"{UBUNTU_MIRROR}/{pkg['Filename']}"
        dest  = debs_dir / fname
        if download(url, dest, label=fname):
            deb_ok += 1
            downloaded_debs.append(dest)
        else:
            deb_fail += 1

    print(f"\nSystem packages: {deb_ok} ok, {deb_fail} failed")

    # ── 3. Extract .so files ───────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("Step 3: Extracting .so files from .deb archives")
    print(f"{'='*60}")

    total_so = 0
    for deb_path in sorted(debs_dir.glob("*.deb")):
        n = extract_so_from_deb(deb_path.read_bytes(), so_dir)
        if n:
            print(f"  [ok]   {deb_path.name} -> {n} .so file(s)")
            total_so += n

    print(f"\nExtracted {total_so} .so files to {so_dir}/")

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("Bundle complete")
    print(f"{'='*60}")
    r_count  = len(list(out_dir.glob("*.tar.gz")))
    so_count = len(list(so_dir.glob("*.so*")))
    print(f"  R packages : {r_count} .tar.gz in {out_dir}/")
    print(f"  System libs: {so_count} .so* files in {so_dir}/")
    print(f"  Debs kept  : {deb_ok} .deb files in {debs_dir}/")
    print(f"\nUpload {out_dir}/ to your Databricks UC volume, then run install_packages.ipynb")


if __name__ == "__main__":
    main()
