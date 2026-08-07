import subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "dmn_models"
DEST_DIR = "dbfs:/Volumes/dbx_practice_ws/default/dbx-practice-volume"
for dmn_file in sorted(MODEL_DIR.glob("*.dmn")):
    dest = f"{DEST_DIR}/{dmn_file.name}"
    subprocess.run(["databricks", "fs", "cp", str(dmn_file), dest, "--overwrite"], check=True)
    print("Copied:", dmn_file, "->", dest)
