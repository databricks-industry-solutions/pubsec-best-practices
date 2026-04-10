#!/bin/bash
# init_r_packages.sh
# Cluster init script — runs as root on every cluster start.
#
# What it does:
#   1. Extracts .so files from .deb archives on the volume (skips if already done)
#   2. Installs R packages from volume into a persistent lib on the volume
#   3. Writes an .Rprofile that adds the lib to .libPaths() for every R session
#
# Prerequisites (on volume before attaching this script):
#   /Volumes/<cat>/<schema>/<vol>/syslibs/debs/*.deb  <- from bundle_deps.py
#   /Volumes/<cat>/<schema>/<vol>/*.tar.gz             <- from bundle_deps.py
#
# Configure these two variables:
# ── UPDATE THESE FOR YOUR ENVIRONMENT ─────────────────────────────────────────
VOLUME_PATH="/Volumes/<catalog>/<schema>/<volume>"
R_LIB="${VOLUME_PATH}/r_lib"
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

log() { echo "[init_r_packages] $*"; }

DEB_DIR="${VOLUME_PATH}/syslibs/debs"
SO_DIR="${VOLUME_PATH}/syslibs"
LOCAL_SO="/usr/local/lib/r_syslibs"

# ── 1. Extract .so files from debs (skip if already extracted) ────────────────
# We stamp a file after first extraction so subsequent cluster starts are fast.
STAMP="${SO_DIR}/.extracted"

# Always re-copy .so files to /tmp (ephemeral — wiped between jobs/restarts)
# Only skip the slower dpkg-deb extraction if .so files are already on the volume
if [[ ! -f "$STAMP" ]]; then
  log "Extracting .so files from debs in ${DEB_DIR}..."
  mkdir -p "$LOCAL_SO"

  for deb in "${DEB_DIR}"/*.deb; do
    [[ -f "$deb" ]] || continue
    tmp_dir=$(mktemp -d)
    dpkg-deb --extract "$deb" "$tmp_dir" 2>/dev/null
    find "$tmp_dir" -name "*.so*" -not -type d \
      -exec cp -n {} "${SO_DIR}/" \; 2>/dev/null || true
    rm -rf "$tmp_dir"
  done

  touch "$STAMP"
  log "Extraction complete."
else
  log "Skipping extraction (already done)."
fi

# ── 2. Copy .so files to /tmp (always — /tmp is ephemeral) ───────────────────
mkdir -p "$LOCAL_SO"
find "$SO_DIR" -maxdepth 1 -name "*.so*" -not -type d \
  -exec cp {} "$LOCAL_SO/" \; 2>/dev/null || true
log "Copied $(ls "$LOCAL_SO"/*.so* 2>/dev/null | wc -l) .so files to ${LOCAL_SO}"

# Persist LD_LIBRARY_PATH for the R session via /etc/environment
if ! grep -q "r_syslibs" /etc/environment 2>/dev/null; then
  EXISTING=$(grep "^LD_LIBRARY_PATH=" /etc/environment 2>/dev/null | cut -d= -f2- || true)
  if [[ -n "$EXISTING" ]]; then
    sed -i "s|^LD_LIBRARY_PATH=.*|LD_LIBRARY_PATH=${LOCAL_SO}:${EXISTING}|" /etc/environment
  else
    echo "LD_LIBRARY_PATH=${LOCAL_SO}" >> /etc/environment
  fi
fi

# Also export for any child processes spawned during init
export LD_LIBRARY_PATH="${LOCAL_SO}:${LD_LIBRARY_PATH:-}"
log "LD_LIBRARY_PATH set to include ${LOCAL_SO}"

# Register with ldconfig so ALL processes (including the R notebook kernel) can
# find the libs without relying on LD_LIBRARY_PATH inheritance
echo "${LOCAL_SO}" > /etc/ld.so.conf.d/r_syslibs.conf
ldconfig
log "ldconfig updated for ${LOCAL_SO}"

# ── 3. Create persistent R lib on volume ──────────────────────────────────────
mkdir -p "$R_LIB"

# ── 4. Install R packages (skip already-installed) ────────────────────────────
TARBALLS=( "${VOLUME_PATH}"/*.tar.gz )
if [[ ${#TARBALLS[@]} -eq 0 || ! -f "${TARBALLS[0]}" ]]; then
  log "No .tar.gz files found in ${VOLUME_PATH} — skipping R package install."
  exit 0
fi

log "Installing R packages from ${VOLUME_PATH} into ${R_LIB}..."

# Pass paths as env vars so the heredoc can be fully single-quoted (no bash expansion inside R)
export R_INIT_LIB="${R_LIB}"
export R_INIT_VOL="${VOLUME_PATH}"
export R_INIT_SO="${LOCAL_SO}"

Rscript - <<'RSCRIPT'
  lib      <- Sys.getenv("R_INIT_LIB")
  vol      <- Sys.getenv("R_INIT_VOL")
  local_so <- Sys.getenv("R_INIT_SO")

  # Ensure syslibs are loaded for this Rscript process too
  old_ld <- Sys.getenv("LD_LIBRARY_PATH")
  Sys.setenv(LD_LIBRARY_PATH = paste(local_so, old_ld, sep = ":"))

  load_order <- c("libgeos", "libgeos_c", "libproj", "libtiff",
                  "libjpeg", "libgdal", "libudunits2")
  for (pat in load_order) {
    for (f in list.files(local_so, pattern = pat, full.names = TRUE)) {
      tryCatch(dyn.load(f), error = function(e) invisible(NULL))
    }
  }

  pkgs      <- list.files(vol, pattern = "\\.tar\\.gz$", full.names = TRUE)
  # Check only r_lib — not system-wide — so we don't skip packages that exist
  # on the cluster at the wrong version
  installed <- rownames(installed.packages(lib.loc = lib, noCache = TRUE))
  pending   <- pkgs[!sub("_.*", "", basename(pkgs)) %in% installed]

  if (length(pending) == 0) {
    message("All packages already installed.")
    quit(status = 0)
  }

  message("Installing ", length(pending), " packages...")
  remaining <- pending
  for (i in seq_len(5)) {
    if (length(remaining) == 0) break
    results <- sapply(remaining, function(p) {
      tryCatch({
        install.packages(p, repos = NULL, type = "source",
                         lib = lib, quiet = TRUE)
        TRUE
      }, error = function(e) {
        message("  [FAIL] ", sub("_.*", "", basename(p)), ": ", conditionMessage(e))
        FALSE
      })
    })
    remaining <- remaining[!results]
  }

  if (length(remaining) > 0) {
    message("WARN: ", length(remaining), " packages failed to install:")
    message(paste(sub("_.*", "", basename(remaining)), collapse = ", "))
    quit(status = 1)
  }
  message("All packages installed successfully.")
RSCRIPT

log "R package installation complete."

# ── 5. Write Rprofile.site so every R session picks up the lib and syslibs ────
# Rprofile.site is loaded by ALL R sessions regardless of user — more reliable
# than per-user .Rprofile on Databricks where the notebook user varies.
RPROFILE_SITE="$(Rscript -e 'cat(R.home("etc"))')/Rprofile.site"

if ! grep -q "r_syslibs" "$RPROFILE_SITE" 2>/dev/null; then
  cat >> "$RPROFILE_SITE" <<RPROFILE_EOF

# Added by init_r_packages.sh — load syslibs and add volume lib to search path
local({
  so_dir <- "${LOCAL_SO}"
  r_lib  <- "${R_LIB}"

  if (dir.exists(so_dir)) {
    old_ld <- Sys.getenv("LD_LIBRARY_PATH")
    Sys.setenv(LD_LIBRARY_PATH = paste(so_dir, old_ld, sep = ":"))
    for (pat in c("libgeos", "libgeos_c", "libproj", "libtiff",
                  "libjpeg", "libgdal", "libudunits2")) {
      for (f in list.files(so_dir, pattern = pat, full.names = TRUE)) {
        tryCatch(dyn.load(f), error = function(e) invisible(NULL))
      }
    }
  }

  if (dir.exists(r_lib)) {
    .libPaths(c(r_lib, .libPaths()))
  }
})
RPROFILE_EOF
  log "Rprofile.site updated: ${RPROFILE_SITE}"
fi

log "Init script complete. R lib: ${R_LIB}"
