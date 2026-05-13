import subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
ENGINE_DIR = ROOT / "drools_dmn_engine"
JAR = ENGINE_DIR / "target" / "drools-databricks-dmn-enterprise-solution-2.3.0.jar"
DEST = "dbfs:/Volumes/dbx_practice_ws/default/dbx-practice-volume/drools-databricks-dmn-enterprise-solution-2.3.0.jar"
def run(cmd, cwd=None):
    print("Running:", " ".join(map(str, cmd)))
    subprocess.run(list(map(str, cmd)), cwd=cwd, check=True)
run(["mvn.cmd", "clean", "package", "-U"], cwd=ENGINE_DIR)
run(["databricks", "fs", "cp", JAR, DEST, "--overwrite"])
print("Copied JAR to:", DEST)
