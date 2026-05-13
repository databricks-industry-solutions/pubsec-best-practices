# Databricks notebook source
# MAGIC %md
# MAGIC # Quickstart 01 — Build Drools Shaded JAR
# MAGIC
# MAGIC Builds `drools-dmn-shaded-2.0.0.jar` — a pure Drools/KIE dependency bundle
# MAGIC with no custom code. This JAR puts the Drools 9.x runtime on the cluster
# MAGIC classpath so the Scala UDF in `02_score_one_table` can use it.
# MAGIC
# MAGIC **Source of truth:** `drools-shaded/pom.xml` in the repo. This notebook
# MAGIC copies it to the build dir — change the version there to upgrade Drools.
# MAGIC
# MAGIC **When to rebuild:**
# MAGIC - Upgrading Drools version (`drools.version` in `drools-shaded/pom.xml`)
# MAGIC - Never needed for rule changes — those go through the DMN file
# MAGIC - Never needed for new input columns — those go through scoring notebooks
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Locate `drools-shaded/pom.xml` in the repo and copy it to the build dir
# MAGIC 2. Download Maven 3.9.9 if not cached
# MAGIC 3. Run `mvn package`
# MAGIC 4. Copy shaded JAR to UC Volume
# MAGIC 5. Restart Drools Runner cluster to pick it up

# COMMAND ----------

import os, shutil, subprocess, sys, glob

dbutils.widgets.text('catalog', 'main', 'UC catalog')
dbutils.widgets.text('schema',  'irs_rrp',                 'UC schema')
dbutils.widgets.text('volume',  'dmn_rules',               'UC volume')
dbutils.widgets.text('pom_path', '', 'Path to pom.xml (blank = auto-locate from notebook path)')

CATALOG     = dbutils.widgets.get('catalog').strip() or 'main'
SCHEMA      = dbutils.widgets.get('schema').strip()  or 'irs_rrp'
VOLUME      = dbutils.widgets.get('volume').strip()  or 'dmn_rules'
POM_PATH    = dbutils.widgets.get('pom_path').strip()
JAR_NAME    = "drools-dmn-shaded-2.0.0.jar"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{JAR_NAME}"
BUILD_DIR   = "/tmp/drools-build"
MVN_HOME    = "/tmp/maven"
MVN_BIN     = f"{MVN_HOME}/bin/mvn"
MVN_VER     = "3.9.9"

# COMMAND ----------

# MAGIC %md ## Step 1 — Locate and copy `drools-shaded/pom.xml` from the repo
# MAGIC
# MAGIC The repo root is two levels above this notebook (`notebooks/quickstart/`).
# MAGIC We start from this notebook's workspace path (or the `pom_path` widget
# MAGIC if set) and walk up to find a parent that contains `drools-shaded/pom.xml`,
# MAGIC then copy it into the Maven build dir.

# COMMAND ----------

def _find_pom(start: str) -> str:
  cur = os.path.abspath(start)
  for _ in range(8):  # walk up at most 8 levels
    candidate = os.path.join(cur, 'drools-shaded', 'pom.xml')
    if os.path.isfile(candidate):
      return candidate
    parent = os.path.dirname(cur)
    if parent == cur:
      break
    cur = parent
  raise FileNotFoundError(f"Could not locate drools-shaded/pom.xml walking up from {start}.")

if POM_PATH:
  if not os.path.isfile(POM_PATH):
    raise FileNotFoundError(f"pom_path widget = {POM_PATH!r} but file does not exist")
  SRC_POM = POM_PATH
else:
  notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  )
  notebook_dir_ws = os.path.dirname(notebook_path)
  notebook_dir_fs = '/Workspace' + notebook_dir_ws if not notebook_dir_ws.startswith('/Workspace') else notebook_dir_ws
  print(f"Notebook workspace path: {notebook_path}")
  print(f"Searching from:          {notebook_dir_fs}")
  try:
    SRC_POM = _find_pom(notebook_dir_fs)
  except FileNotFoundError:
    SRC_POM = _find_pom(os.getcwd())  # bundle-deploy / Repo fallback

os.makedirs(BUILD_DIR, exist_ok=True)
shutil.copy2(SRC_POM, f"{BUILD_DIR}/pom.xml")
print(f"✅ pom.xml copied: {SRC_POM} → {BUILD_DIR}/pom.xml")

# COMMAND ----------

# MAGIC %md ## Step 2 — Install Maven

# COMMAND ----------

def run(cmd, cwd=None, check=True):
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True, cwd=cwd)
    if result.stdout: print(result.stdout)
    if result.stderr: print(result.stderr, file=sys.stderr)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed (exit {result.returncode}): {cmd}")
    return result

if not os.path.exists(MVN_BIN):
    print(f"Downloading Maven {MVN_VER}...")
    MVN_URL = f"https://archive.apache.org/dist/maven/maven-3/{MVN_VER}/binaries/apache-maven-{MVN_VER}-bin.tar.gz"
    run(f"curl -fsSL {MVN_URL} -o /tmp/maven.tar.gz")
    run(f"tar -xzf /tmp/maven.tar.gz -C /tmp")
    run(f"mv /tmp/apache-maven-{MVN_VER} {MVN_HOME}")
else:
    print("Maven already cached.")

os.environ["PATH"] = f"{MVN_HOME}/bin:" + os.environ.get("PATH", "")
run(f"{MVN_BIN} --version")

# COMMAND ----------

# MAGIC %md ## Step 3 — Build

# COMMAND ----------

log_file = "/tmp/mvn-build.log"
result = subprocess.run(
    f"{MVN_BIN} package -DskipTests 2>&1 | tee {log_file}",
    shell=True, cwd=BUILD_DIR, text=True,
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT
)
with open(log_file) as f:
    log = f.read()
tail = log[-6000:] if len(log) > 6000 else log
print(tail)

if result.returncode != 0:
    dbutils.notebook.exit(f"FAILED (exit {result.returncode})\n\n{tail}")

run(f"ls -lh {BUILD_DIR}/target/")

# COMMAND ----------

# MAGIC %md ## Step 4 — Copy JAR to UC Volume

# COMMAND ----------

candidates  = glob.glob(f"{BUILD_DIR}/target/*.jar")
shaded_jars = [j for j in candidates if "original" not in j]

if not shaded_jars:
    dbutils.notebook.exit(f"FAILED: No shaded JAR in target/. Found: {candidates}")

local_jar = shaded_jars[0]
shutil.copy2(local_jar, VOLUME_PATH)
size_mb = os.path.getsize(VOLUME_PATH) / 1024 / 1024
print(f"✅ {JAR_NAME} → {VOLUME_PATH}  ({size_mb:.1f} MB)")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: {JAR_NAME} built and uploaded to {VOLUME_PATH}")
