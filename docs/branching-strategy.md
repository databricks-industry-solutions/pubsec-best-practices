# Branching Strategy

## Convention

This mono-repo uses a per-project branching strategy:

```
<project-name>/<branch-type>
```

### Examples

| Branch | Purpose |
|--------|---------|
| `xsd-to-synthetic/feature-validation` | New feature for xsd-to-synthetic |
| `xsd-to-synthetic/fix-parser-bug` | Bug fix for xsd-to-synthetic |
| `demo-foo/add-notebook` | New notebook for demo-foo |
| `infra/update-ci-templates` | Changes to shared infrastructure |
| `shared/add-logging-utils` | Changes to shared code |

## Branch Types

While not strictly enforced, these prefixes help communicate intent:

- `feature-*` — New functionality
- `fix-*` — Bug fixes
- `refactor-*` — Code restructuring
- `docs-*` — Documentation updates
- `test-*` — Test additions/fixes

## Special Branches

| Branch | Purpose |
|--------|---------|
| `main` | Production-ready code |
| `infra/*` | Changes to CI/CD templates, root config |
| `shared/*` | Changes to `shared/` directory |

## Workflow

1. **Create a branch** from `main`:
   ```bash
   git checkout main
   git pull
   git checkout -b xsd-to-synthetic/feature-new-parser
   ```

2. **Make changes** within the project directory

3. **Push and create PR**:
   ```bash
   git push -u origin xsd-to-synthetic/feature-new-parser
   gh pr create
   ```

4. **CI runs automatically** — only workflows matching the changed paths will execute

5. **Merge to main** — triggers deployment (if configured)

## Path-Filtered CI

Each project has its own workflow that only runs when relevant files change:

```yaml
on:
  push:
    paths:
      - 'solution-accelerators/xsd-to-synthetic/**'
      - '.github/workflows/solution-accelerators/xsd-to-synthetic.yml'
```

This means:
- Changes to `xsd-to-synthetic/` only run `xsd-to-synthetic` tests
- Changes to `demo-foo/` only run `demo-foo` tests
- Changes to `shared/` may trigger multiple workflows (configure as needed)

## Tips

- Keep branches short-lived — merge frequently
- Delete branches after merging
- Use descriptive branch names that indicate the change
- For cross-project changes, use `infra/` or `shared/` prefix
