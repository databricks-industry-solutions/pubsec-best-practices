# Adding Rules, Inputs, and Outputs

How a tax authority extends the Drools/DMN rules engine without touching code.

The pipeline has two contracts:

1. **The DMN** — declares input names, decisions, and rules. Authored in
   the rule-editor app or any DMN tool.
2. **The input view** — a Unity Catalog **table, view, or materialized
   view** whose columns match the DMN inputs by name. The tax authority owns the
   SQL. A materialized view is the recommended choice for batch scoring
   (pre-computed, refreshed on a schedule), but a plain table or view
   works too — the notebook reads whatever `input_view` points at.

The binding JSON in `rule_versions.binding_json` ties them together: it
names the `input_view` and lists the `outputs` (which DMN decisions land
in which Delta table column). **No joins, no per-column SQL.** All
flattening happens in the input view.

> **Adding a rule = DMN edit + (sometimes) input-view edit + promote.**
> The notebook never changes.

---

## How the pipeline works

```
┌──────────────────┐        ┌──────────────────┐
│ rule_versions    │───────▶│ UC Volume:       │
│  - dmn_path      │        │ <version>.dmn    │
│  - binding_json  │        └──────────────────┘
│  - input_view    │                  │
│  - status=ACTIVE │                  │
└──────────────────┘                  │
         │                            ▼
         │                    drools_score UDF
         │                    (Map[String,String]
         │                       → Map[String,String])
         ▼                            │
  scoring_input_vN ─────▶  one row → one map ──▶ all DMN decisions
  (TABLE / VIEW / MV — refreshed
   on warehouse, not this cluster)
                                                       │
                                                       ▼
                                          Write each output table
                                          (carry_columns, decisions,
                                           post_columns)
```

Two ingredients drive every run:

- **`scoring_input_vN`** — a UC table / view / materialized view. Every
  column is a DMN input, name-matched (lowercase snake_case). All
  `coalesce`/derivation lives here, in plain SQL.
- **`binding_json`** — a small JSON object naming the `input_view` and
  listing output decisions. ~20 lines, stable across rule edits.

`notebooks/04_batch_scoring.py` is a single generic notebook. It compiles
the DMN, validates the binding, reads `input_view`, runs the UDF once
per row, and writes each declared output. **Never edit it.** The notebook
does **not** refresh the input view — `REFRESH MATERIALIZED VIEW`
requires DBSQL Pro or Serverless and runs out-of-band on a SQL warehouse
(scheduled refresh, Lakeflow task, or `databricks pipelines start-update`).
Plain tables and plain views need no refresh at all.

---

## The binding JSON (v2)

Stored in `rule_versions.binding_json`:

```json
{
  "version": 2,
  "input_view": "<your-catalog>.<your-schema>.scoring_input_v3_1",
  "outputs": {
    "scored_results": {
      "mode": "overwrite",
      "carry_columns": ["return_id", "filing_status", "agi"],
      "decisions": {
        "audit_score":         "Tax Review Scoring",
        "recommended_action":  "Recommended Action"
      },
      "post_columns": {
        "dmn_version": "__VERSION__",
        "scored_at":   "current_timestamp()"
      }
    }
  }
}
```

| Field | What it does |
|---|---|
| `version` | Must be `2`. v1 (with joins inside the binding) is no longer supported. |
| `input_view` | Fully-qualified UC table, view, or materialized view. Every DMN input must appear as a same-named column. MV is the recommended choice for batch (pre-computed + explicit refresh); plain tables and views work too. |
| `columns` *(optional)* | Map of `dmn_input → SQL_expression` to override name-matching for individual inputs. Use only for the rare case (unit conversion). The whitelist of allowed SQL functions is enforced by the pre-flight. |
| `outputs.<table>.decisions` | Map of `output_column → "DMN Decision Name"`. Multiple output columns can come from a single DMN evaluation. |
| `outputs.<table>.carry_columns` | Columns to carry from the MV into the output. Must exist on the MV. |
| `outputs.<table>.post_columns` | SQL expressions evaluated against the output dataframe. Can reference decision columns. The bare token `__VERSION__` is substituted with `'<version_id>'` (already single-quoted as a SQL string literal — don't add your own quotes). Whitelisted functions only. |
| `outputs.<table>.mode` | `overwrite` (default) or `append`. |

---

## Step-by-step: adding a new rule (no new input)

The DMN already knows about the columns the rule needs.

1. **Edit the DMN.** Add the new `<dmn:rule>` row inside the existing
   decision table. Save to a new file in
   `/Volumes/<your-catalog>/<your-schema>/<your-volume>/<filename>.dmn`.
2. **Insert a `rule_versions` row** (`status='DRAFT'`) — copy the prior
   row's `binding_json` and `input_view` verbatim, point `dmn_path` at
   the new file, give it a new `version_id`.
3. **Validate.** Run notebook `00_validate_binding.py` with your new
   `version_id` — it compiles the DMN, parses the binding, runs every
   pre-flight check, and exits `SUCCESS` or `FAILED`.
4. **Promote.** Set the new row to `ACTIVE` and the prior `ACTIVE` row to
   `ARCHIVED`. (One `ACTIVE` per rule set at a time.)
5. **Run** the batch job (the one wired to `04_batch_scoring.py`). Use a
   small `proof_limit` first if you want a quick smoke test.

No notebook change. No JAR rebuild. No MV change.

---

## Step-by-step: adding a new input column

The new field drives a rule that doesn't yet exist in the DMN.

### 1. Add the column to the source table (or compute it)

If it's a brand-new field on `tax_returns`:

```sql
ALTER TABLE <your-catalog>.<your-schema>.tax_returns
ADD COLUMN crypto_income DOUBLE;
```

If it's a derived value (ratio, gap, flag), it can live entirely inside
the new MV — no source-table change needed.

### 2. Create a new versioned MV

Don't mutate the live MV. Create the next-numbered one so the prior
version stays runnable for rollback:

```sql
CREATE MATERIALIZED VIEW <your-catalog>.<your-schema>.scoring_input_v3_2
COMMENT 'Scoring input for DMN v3.2-irm — adds crypto_income'
AS
SELECT
  return_id, filing_status,
  coalesce(agi, 0.0)            AS agi,
  -- ... all existing columns ...
  coalesce(crypto_income, 0.0)  AS crypto_income
FROM <your-catalog>.<your-schema>.tax_returns;
```

The DMN input name (`crypto_income`) **must** match the MV column name
exactly — lowercase snake_case is the convention.

### 3. Add the input + rule to the DMN

In KIE Sandbox or the rule-editor app:

1. Add an **Input Data** node — name `crypto_income`, type `number`.
2. Wire it into the decision table that uses it. Add the input column with
   `<dmn:inputExpression>crypto_income</dmn:inputExpression>`.
3. Add rule rows. Save and upload to the UC Volume.

### 4. Insert + promote the new `rule_versions` row

Same as the no-new-input case, but the new row points at the new MV:

```sql
INSERT INTO <your-catalog>.<your-schema>.rule_versions VALUES (
  'v3.2-irm', 'Tax Return Review',
  '/Volumes/.../irs_tax_review_v3_2_irm.dmn',
  '<binding_json with input_view = scoring_input_v3_2>',
  '<your-catalog>.<your-schema>.scoring_input_v3_2',
  'DRAFT', 'analyst@example.gov', current_timestamp(), NULL, NULL,
  'Adds crypto_income input + 2 new rules'
);
```

Validate with `00_validate_binding.py`, then promote.

---

## Step-by-step: adding a new output table

You want a chained decision (e.g. `Recommended Action`) to write to a
separate Delta table.

### 1. Add the chained decision to the DMN

A `<dmn:decision>` with an `<dmn:informationRequirement>` pointing at the
upstream decision. See `Recommended Action` in `irs_tax_review_v3_1_irm.dmn`
for an example: a UNIQUE-policy decision table that reads `Tax Review
Scoring` and bands it into PASS/REVIEW/FLAG.

### 2. Add an output block to the binding

```json
"outputs": {
  "scored_results": { ... },
  "audit_actions": {
    "mode": "overwrite",
    "carry_columns": ["return_id"],
    "decisions": {
      "action": "Recommended Action"
    },
    "post_columns": {
      "decided_at": "current_timestamp()"
    }
  }
}
```

The UDF runs once per row regardless of how many output tables there are
— Spark caches the score map between writes.

---

## Pre-flight checks

`00_validate_binding.py` (and `04_batch_scoring.py` before scoring) runs
these. Errors abort the run; warnings print and continue.

**Errors:**

*Structural (JSON schema check, no-DB):*
- `binding_json` is not valid JSON
- `binding.version` is not `2`
- `binding.input_view` is missing or not a non-empty string
- `binding.outputs` is missing, not an object, or empty
- `binding.outputs.<table>` is not an object
- `binding.outputs.<table>.decisions` is missing or not a non-empty `{out_col: 'DMN Decision'}` map
- `binding.outputs.<table>.carry_columns` is not a list of strings
- `binding.outputs.<table>.post_columns` is not an object
- `binding.outputs.<table>.mode` is not `'overwrite'` or `'append'`
- `binding.columns` (when present) is not an object

*Semantic (after compiling DMN + reading input_view):*
- `input_view` doesn't exist or isn't queryable
- A DMN input has no matching column on `input_view` **and** no `columns` override
- An override expression in `binding.columns` contains disallowed characters or a non-whitelisted SQL function
- A `carry_columns` entry doesn't exist on `input_view`
- An output references a DMN decision name that doesn't exist
- A `post_columns` expression contains disallowed characters or a non-whitelisted SQL function

The expression whitelist allows only read-only scalar SQL: `coalesce`,
`cast`, `current_timestamp`, `current_date`, `abs`, `round`, `floor`,
`ceil`, `least`, `greatest`, `if`/`case`/`when`/`then`/`else`/`end`,
`concat`, `lower`, `upper`, `trim`, `length`, `to_date`, `to_timestamp`,
`date_format`. Forbidden tokens: `;`, `--`, `/* */`, ` select `, ` from `,
` union `, ` drop `, ` delete `, ` update `, ` insert `.

**Warnings:**
- `binding.columns` maps a name the DMN doesn't declare (stale override — ignored)
- A DMN input name isn't lowercase snake_case (works, but breaks convention)

---

## Common mistakes

### "Audit score is 100% NULL but the run reported SUCCESS"

A `COLLECT/SUM` decision table returns **null** when no rule matches, not 0.
In FEEL, `null + n = null`, so a composite literal that adds sub-decision
results silently returns null whenever any sub-decision had no rule fire.

**Fix:** wrap each summand in the composite literal:

```feel
(if Income Risk Score = null then 0 else Income Risk Score)
+ (if Deduction Risk Score = null then 0 else Deduction Risk Score)
+ ...
```

The exit gate now fails the run if any decision-derived column is 100%
null and prints the exact remediation hint.

### "I edited the MV in place and now I can't roll back"

Don't. Create a new versioned MV (`scoring_input_v3_2`) for the new DMN.
The old MV stays runnable as long as the old DMN's `rule_versions` row
points at it. Use `extras/08_cleanup_archived_mvs.py` (default 30-day retention)
to drop MVs whose only references are old `ARCHIVED` rows.

### Type mismatch between MV column and DMN input

The DMN declares `crypto_income` as `number` and the UDF coerces strings
to `BigDecimal`. If the MV column is `STRING` containing non-numerics
(`'unknown'`), you'll get `NumberFormatException` per row. Cast or
coalesce in the MV's SQL.

### Forgot to promote the DMN

`status='ACTIVE'` is what the notebook resolves when no `version_id`
parameter is passed. If you uploaded a new DMN but only marked it
`DRAFT`, the run still uses the old `ACTIVE` version. Pass
`version_id=...` explicitly during testing.

### Refreshing the MV

`04_batch_scoring.py` does **not** refresh the MV — `REFRESH MATERIALIZED
VIEW` is rejected on general-compute clusters (requires DBSQL Pro or
Serverless). The notebook just inspects the type and reads the data.
Refresh it separately via a SQL warehouse, a scheduled Lakeflow task, or
`databricks pipelines start-update` before triggering the scoring job.

Plain Delta tables and views work as-is — the notebook reads whatever
`input_view` points at, regardless of `table_type`. Use a regular table
when you don't need automatic incremental refresh; use a view when you
want re-evaluation on every read; use an MV when you want pre-computed
results with explicit refresh control.

---

## Reference: file paths

| What | Where |
|---|---|
| DMN files | `/Volumes/<your-catalog>/<your-schema>/<your-volume>/` |
| Shaded JAR | `/Volumes/<your-catalog>/<your-schema>/<your-volume>/drools-dmn-shaded-2.0.0.jar` |
| Rule versions | `<your-catalog>.<your-schema>.rule_versions` |
| Active scoring MV | `<your-catalog>.<your-schema>.scoring_input_v3_1` |
| Batch scoring notebook | `notebooks/04_batch_scoring.py` |
| Binding validator | `notebooks/00_validate_binding.py` |
| MV cleanup | `notebooks/extras/08_cleanup_archived_mvs.py` |
| Quickstart (build JAR) | `notebooks/quickstart/01_build_jar.py` |
| Quickstart (score one table) | `notebooks/quickstart/02_score_one_table.py` |
| Quickstart sample DMN | `notebooks/quickstart/sample_rules.dmn` |
| Batch scoring job | (set `SCORING_JOB_ID` in `apps/rules-editor/app.yaml` after creating the job) |

## What does NOT need to change

- `notebooks/04_batch_scoring.py` — never (the whole point)
- `notebooks/_binding_preflight.py` — only if the binding schema itself evolves
- The Scala UDF inside the notebook — never
- `drools-dmn-shaded-2.0.0.jar` — only when upgrading Drools
- Output table schemas — derived from the binding's `decisions` and
  `post_columns`; auto-evolves with `overwriteSchema=true`
