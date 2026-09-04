# Contributing to Generate & Organize Live IaC

Thanks for your interest in improving this accelerator!

## Prerequisites

- `terraform` 1.5+ (native `import {}` blocks) — the exporter ships inside the Databricks provider binary
- Python 3.9+ with `PyYAML` (`pip install pyyaml`)
- `bash` (macOS/Linux/WSL/git-bash) **or** PowerShell 7+ (pwsh) — every orchestration script has a `.sh` and a `.ps1` twin
- A Databricks **account-admin service principal** for account-scope exports, and read access to each workspace you export

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install pyyaml
./run.sh --offline --collapse    # rebuild the plane tree from the bundled example fixture (no creds)
```

## Making changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-change`)
3. Make your changes; keep the `.sh` and `.ps1` twins in sync
4. Verify `./run.sh --offline` still classifies the `captured/example/` fixture into the five planes
5. Commit, push, and open a Pull Request

## Guardrails specific to this accelerator

- **Never commit real account data.** The exporter output (`captured/<name>/`) carries real
  user names, service-principal ids, and catalog names. Only the fabricated `captured/example/`
  fixture is committed; everything else under `captured/` is gitignored. Sanitize before sharing.
- **The tool authors code and runs read-only exports + speculative plans only.** It must never
  run `apply` / `state mv` / real `import` — those belong to your VCS-driven Terraform workflow.
- **Keep plans clean.** A correct adoption plan is `N to import, 0 to destroy`. Preserve the
  zero-destroy gate in `04_plan.*`.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
