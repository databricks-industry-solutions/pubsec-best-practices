import subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
LOCAL_DMN = ROOT / "complex_dmn" / "customer-decision-v2.dmn"
#DEST = "dbfs:/Volumes/dbx_practice_ws/default/dbx-practice-volume/customer-decision-v2.dmn"
DEST = "dbfs:/Volumes/services_bureau_catalog/irs_rrp/dmn_rules/customer-decision-v2.dmn"
subprocess.run(["databricks", "fs", "cp", str(LOCAL_DMN), DEST, "--overwrite"], check=True)
print("Copied DMN to:", DEST)
