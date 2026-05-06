import subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
JAVA_DIR = ROOT / "java-dmn-engine"
JAR = JAVA_DIR / "target" / "drools-databricks-dmn-map-udf-demo-2.2.0.jar"
#DEST = "dbfs:/Volumes/dbx_practice_ws/default/dbx-practice-volume/drools-databricks-dmn-map-udf-demo-2.2.0.jar"
DEST = "dbfs:/Volumes/services_bureau_catalog/irs_rrp/dmn_rules/drools-databricks-dmn-map-udf-demo-2.2.0.jar"
def run(cmd, cwd=None):
    print("Running:", " ".join(map(str, cmd)))
    subprocess.run(list(map(str, cmd)), cwd=cwd, check=True)
run(["mvn", "clean", "package", "-U"], cwd=JAVA_DIR)
run(["databricks", "fs", "cp", JAR, DEST, "--overwrite"])
print("Copied JAR to:", DEST)
