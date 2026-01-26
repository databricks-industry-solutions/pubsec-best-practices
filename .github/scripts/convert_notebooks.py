#!/usr/bin/env python3
"""Convert Jupyter notebooks to Databricks notebook format.

This script converts .ipynb files to Databricks-compatible .py files
with the # Databricks notebook source header.
"""

import json
import sys
from pathlib import Path


def convert_notebook(ipynb_path: Path, output_path: Path | None = None) -> None:
    """Convert a Jupyter notebook to Databricks format.

    Args:
        ipynb_path: Path to the .ipynb file
        output_path: Optional output path. Defaults to same name with .py extension.
    """
    if output_path is None:
        output_path = ipynb_path.with_suffix(".py")

    with open(ipynb_path) as f:
        notebook = json.load(f)

    lines = ["# Databricks notebook source"]

    for cell in notebook.get("cells", []):
        cell_type = cell.get("cell_type")
        source = "".join(cell.get("source", []))

        if cell_type == "markdown":
            lines.append("# MAGIC %md")
            for line in source.split("\n"):
                lines.append(f"# MAGIC {line}")
        elif cell_type == "code":
            lines.append(source)

        lines.append("")
        lines.append("# COMMAND ----------")
        lines.append("")

    # Remove trailing command separator
    while lines and lines[-1] in ("", "# COMMAND ----------"):
        lines.pop()

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    print(f"Converted: {ipynb_path} -> {output_path}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: convert_notebooks.py <notebook.ipynb> [output.py]")
        print("       convert_notebooks.py <directory>")
        sys.exit(1)

    path = Path(sys.argv[1])

    if path.is_file():
        output = Path(sys.argv[2]) if len(sys.argv) > 2 else None
        convert_notebook(path, output)
    elif path.is_dir():
        for ipynb in path.rglob("*.ipynb"):
            if ".ipynb_checkpoints" not in str(ipynb):
                convert_notebook(ipynb)
    else:
        print(f"Error: {path} not found")
        sys.exit(1)


if __name__ == "__main__":
    main()
