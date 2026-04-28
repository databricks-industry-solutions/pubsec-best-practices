# Multi-Rule-Set Support — Design Space

**Date:** 2026-04-27
**Status:** Decision pending — captured for discussion, not yet decided.

## Context

Today the system supports a single `ACTIVE` rule version per workspace.
`04_batch_scoring.py` resolves one row from `rule_versions` (highest
`promoted_at` where `status='ACTIVE'`), reads its `input_view`, runs its
DMN, and writes its `outputs.<table>`. One DMN, one input view, one set
of output tables.

We anticipate needing multiple rule sets running concurrently — for
example: one for individual returns, one for business returns, one for
digital-asset compliance. Each is a self-contained DMN + input view +
binding. The schema field `rule_set_name` already exists on
`rule_versions` but is not load-bearing; the notebook ignores it.

## The hard question

The hard question is not "can we run N rule sets" — that is mechanical.
The hard question is **what shape the results table(s) should take**,
and that depends on consumers (analysts, dashboards, downstream jobs)
more than on the engine.

## Result-shape options

### A. Per-rule-set output table

Each rule set's binding names its own output table:

```
scored_results_individual
scored_results_business
scored_results_crypto
```

- **Already supported.** No engine change needed — each binding's
  `outputs.<table>` is independent.
- **Best when** rule-set output schemas diverge (different decision
  columns per set).
- **Worst when** an analyst wants "all flagged returns regardless of
  rule set" — they UNION every table, and adding a rule set means
  updating every consumer query.

### B. Unified output table with `rule_set_name` column

Single `scored_results` table; rows tagged by `rule_set_name`:

```
return_id | rule_set_name | audit_score | recommended_action | dmn_version | scored_at
```

All rule sets must produce a compatible schema (shared columns).

- **Already supported** at the binding level — every binding can target
  `scored_results` with `mode: append` and a `post_columns` literal for
  `rule_set_name`. We would need to add basic schema-compat enforcement
  to pre-flight (today's pre-flight does not catch schema drift across
  rule sets).
- **Best when** dashboards do cross-rule-set analysis or when output
  schemas naturally agree.
- **Worst when** rule sets diverge — lots of NULL columns or
  stringly-typed JSON shoved into a generic column.

### C. Hybrid — per-rule-set tables + unified view

Each rule set writes its own table (Option A); a standing UC view
UNIONs them on shared columns:

```sql
CREATE VIEW scored_results_all AS
SELECT 'individual' AS rule_set_name, return_id, audit_score, ... FROM scored_results_individual
UNION ALL
SELECT 'business'   AS rule_set_name, return_id, audit_score, ... FROM scored_results_business
UNION ALL
...;
```

- **Best when** we want both per-rule-set isolation for evolution **and**
  unified consumption for dashboards.
- **Cost:** the view is an extra artifact to maintain. Schema drift in
  any underlying table can break it. View needs to be regenerated when
  rule sets are added or removed.

## Orchestration options (separate from result shape)

How do multiple rule sets actually run on the cluster?

### 1. Multiple parallel jobs

N copies of job `834669542144086`, one `rule_set_name` widget each. The
shared cluster's Spark scheduler runs them concurrently. Simple. Cluster
cost scales linearly per rule set running at the same time.

### 2. Single job, loops internally

One notebook run scores all `ACTIVE` rule sets sequentially. Cheaper
(one cluster start, one DMN-cache prime per set), but a slow rule set
blocks the next, and a single failure currently kills the whole run.
Would need per-rule-set try/except + per-set exit gates.

### 3. Single job, fused multi-DMN scoring

If multiple rule sets share an `input_view`, compile one DMN per rule
set on the driver, broadcast all N XML strings, score each row against
all N models in one pass. Theoretically the cheapest. Most invasive —
the UDF signature changes, the cache shape changes, and the binding
schema needs to express "this row contributes to multiple output
tables." Probably overkill.

## What changes in either case

Regardless of shape and orchestration:

- **`04_batch_scoring.py`** accepts an optional `rule_set_name` widget.
  Default behavior (blank): score all `ACTIVE` rule sets. Specific name:
  score only that one. The current "highest `promoted_at` ACTIVE row"
  selector becomes a `(rule_set_name) → row` mapping.
- **Promotion gate** (`00_validate_binding.py` and the rules-editor app)
  enforces "one `ACTIVE` per `rule_set_name`" rather than "one `ACTIVE`
  total." Promoting a new version to `ACTIVE` archives the prior
  `ACTIVE` row **with the same `rule_set_name`** rather than archiving
  every other `ACTIVE` row.
- **Rules-editor app** gains a rule-set picker. Today the app implicitly
  assumes a single rule set.
- **MV cleanup notebook (`08`)** scopes its "is this MV referenced by
  the active set?" check per `rule_set_name` (already mostly there).

## Information needed before deciding

We do not have enough signal yet to commit to a shape. The question to
answer is **how analysts will consume scored results across rule sets**:

- Do dashboards filter by `rule_set_name`, or do they aggregate?
- Do downstream jobs (case management, audit-prep workflows) treat
  rule sets as fungible (one row = one return, regardless of set) or
  as siloed (separate workflows per set)?
- Do rule sets share enough schema (most do `audit_score` +
  `recommended_action`) to make Option B viable, or do business and
  crypto rules want fundamentally different output columns?

Once we know that, the shape choice is mechanical:

- Most consumers cross-cut → Option B or C.
- Most consumers are siloed → Option A.
- Mixed → Option C.

## Recommendation (for when we revisit)

**Option C with Orchestration #2** is the safest default if the choice
stays ambiguous: per-rule-set tables preserve isolation and evolution,
the unified view satisfies cross-cut consumers, and a single job
sequentially scoring all `ACTIVE` sets keeps cluster cost flat.

**Option A with Orchestration #1** is the cheapest path to "ship
something" if rule sets are meaningfully siloed — no notebook surgery,
no view to maintain, just N jobs.

**Option 3 (fused multi-DMN scoring) is not recommended** unless we hit
a real cost ceiling. The complexity-per-row-saved trade-off is bad.

## Footer

Decision pending. Revisit when analyst/dashboard consumption patterns
are clearer.
