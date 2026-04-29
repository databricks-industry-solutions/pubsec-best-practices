# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Cleanup Archived Materialized Views
# MAGIC
# MAGIC Versioned scoring MVs (`scoring_input_v3_1`, `scoring_input_v4`, ...)
# MAGIC accumulate as DMN versions evolve. This notebook lists every
# MAGIC `scoring_input_*` MV in the schema and groups them by reference status:
# MAGIC
# MAGIC | Group | Meaning |
# MAGIC | --- | --- |
# MAGIC | **active** | Referenced by an `ACTIVE` rule_versions row. **Never drop.** |
# MAGIC | **draft** | Referenced by a `DRAFT` rule_versions row. **Never drop.** |
# MAGIC | **archived-recent** | Referenced only by `ARCHIVED` rows promoted < retention_days ago. Keep for rollback. |
# MAGIC | **cleanup-candidate** | Referenced only by `ARCHIVED` rows older than retention_days. Safe to drop. |
# MAGIC | **orphan** | Not referenced by any `rule_versions` row. Safe to drop. |
# MAGIC
# MAGIC Set `cleanup_confirm=true` to actually `DROP MATERIALIZED VIEW` the
# MAGIC cleanup-candidate + orphan groups. Default is dry-run (list only).

# COMMAND ----------

dbutils.widgets.text('retention_days',  '30',                      'Retention window (days)')
dbutils.widgets.text('cleanup_confirm', 'false',                   'Actually drop? (true|false)')
dbutils.widgets.text('catalog',         'main', 'UC catalog')
dbutils.widgets.text('schema',          'irs_rrp',                 'UC schema')

# COMMAND ----------

CATALOG = dbutils.widgets.get('catalog').strip() or 'main'
SCHEMA  = dbutils.widgets.get('schema').strip()  or 'irs_rrp'
MV_PREFIX = "scoring_input_"

retention_days = int(dbutils.widgets.get('retention_days'))
cleanup        = dbutils.widgets.get('cleanup_confirm').strip().lower() in ("true", "1", "yes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List MVs in the schema

# COMMAND ----------

# Materialized views show up in information_schema.tables with table_type
# = 'MATERIALIZED VIEW' on UC. Falling back to SHOW TABLES for compatibility.
mvs = [
    r["table_name"] for r in spark.sql(f"""
        SELECT table_name FROM {CATALOG}.information_schema.tables
        WHERE table_schema = '{SCHEMA}'
          AND table_type   = 'MATERIALIZED VIEW'
          AND table_name LIKE '{MV_PREFIX}%'
        ORDER BY table_name
    """).collect()
]
print(f"Found {len(mvs)} materialized view(s) under '{CATALOG}.{SCHEMA}.{MV_PREFIX}*'")
for m in mvs:
    print(f"  • {m}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build {mv_name → reference status} from rule_versions

# COMMAND ----------

from collections import defaultdict
from datetime import datetime, timedelta, timezone

rows = spark.sql(f"""
    SELECT version_id, status, input_view, promoted_at
    FROM {CATALOG}.{SCHEMA}.rule_versions
""").collect()

now = datetime.now(timezone.utc)
cutoff = now - timedelta(days=retention_days)

mv_refs = defaultdict(list)  # short_name → [(version_id, status, promoted_at), ...]
for r in rows:
    iv = r["input_view"]
    if not iv:
        continue
    short = iv.split(".")[-1]
    mv_refs[short].append((r["version_id"], r["status"], r["promoted_at"]))

groups = {"active": [], "draft": [], "archived-recent": [], "cleanup-candidate": [], "orphan": []}
for mv in mvs:
    refs = mv_refs.get(mv, [])
    if not refs:
        groups["orphan"].append((mv, []))
        continue
    statuses = {s for _, s, _ in refs}
    if "ACTIVE" in statuses:
        groups["active"].append((mv, refs))
    elif "DRAFT" in statuses:
        groups["draft"].append((mv, refs))
    else:
        # All ARCHIVED — bucket on most-recent promoted_at
        latest = max(
            ((p.replace(tzinfo=timezone.utc) if p and p.tzinfo is None else p) or datetime.min.replace(tzinfo=timezone.utc))
            for _, _, p in refs
        )
        if latest >= cutoff:
            groups["archived-recent"].append((mv, refs))
        else:
            groups["cleanup-candidate"].append((mv, refs))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Report

# COMMAND ----------

def _print_group(label):
    items = groups[label]
    print(f"── {label.upper()} ({len(items)}) " + "─" * (50 - len(label)))
    if not items:
        print("  (none)")
        return
    for mv, refs in items:
        ref_str = ", ".join(f"{v}({s})" for v, s, _ in refs) if refs else "—"
        print(f"  • {mv}  ← {ref_str}")

print(f"Retention window: {retention_days} days (cutoff: {cutoff.date()})")
print()
for g in ("active", "draft", "archived-recent", "cleanup-candidate", "orphan"):
    _print_group(g)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Drop (only if cleanup_confirm=true)

# COMMAND ----------

to_drop = [mv for mv, _ in groups["cleanup-candidate"]] + [mv for mv, _ in groups["orphan"]]
if not to_drop:
    print("Nothing to drop.")
elif not cleanup:
    print(f"⏭️  cleanup_confirm=false — would drop {len(to_drop)} MV(s):")
    for mv in to_drop:
        print(f"  - {mv}")
    print("Re-run with cleanup_confirm=true to actually drop.")
else:
    print(f"⚠️  cleanup_confirm=true — dropping {len(to_drop)} MV(s)...")
    for mv in to_drop:
        fq = f"{CATALOG}.{SCHEMA}.{mv}"
        print(f"  DROP MATERIALIZED VIEW {fq}")
        spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {fq}")
    print(f"✅ dropped {len(to_drop)} MV(s)")

# COMMAND ----------

dbutils.notebook.exit(
    f"SUCCESS: active={len(groups['active'])} "
    f"draft={len(groups['draft'])} "
    f"archived-recent={len(groups['archived-recent'])} "
    f"cleanup-candidate={len(groups['cleanup-candidate'])} "
    f"orphan={len(groups['orphan'])} "
    f"dropped={len(to_drop) if cleanup else 0}"
)
