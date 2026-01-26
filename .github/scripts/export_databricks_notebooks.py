#!/usr/bin/env python3
"""Export notebooks from a Databricks workspace.

Requires DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.
"""

import os
import subprocess
import sys
from pathlib import Path


def export_notebooks(workspace_path: str, local_path: str) -> None:
    """Export notebooks from Databricks workspace to local directory.

    Args:
        workspace_path: Path in Databricks workspace (e.g., /Users/user@example.com/project)
        local_path: Local directory to export to
    """
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        print("Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
        sys.exit(1)

    local_dir = Path(local_path)
    local_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "databricks",
        "workspace",
        "export-dir",
        workspace_path,
        str(local_dir),
        "--overwrite",
    ]

    print(f"Exporting {workspace_path} to {local_dir}...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        sys.exit(1)

    print(f"Export complete: {local_dir}")


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: export_databricks_notebooks.py <workspace_path> <local_path>")
        print("Example: export_databricks_notebooks.py /Users/me/project ./notebooks")
        sys.exit(1)

    export_notebooks(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
