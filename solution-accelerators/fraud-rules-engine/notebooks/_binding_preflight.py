# Databricks notebook source
# MAGIC %md
# MAGIC # _binding_preflight — shared helpers
# MAGIC
# MAGIC Validates a v2 binding against a compiled DMN model and an input
# MAGIC materialized view. Imported by `04_batch_scoring.py` (full run) and
# MAGIC `00_validate_binding.py` (standalone validator).
# MAGIC
# MAGIC ## v2 binding shape
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "version": 2,
# MAGIC   "input_view": "<catalog>.<schema>.<materialized_view_name>",
# MAGIC   "outputs": {
# MAGIC     "<table>": {
# MAGIC       "mode": "overwrite",
# MAGIC       "carry_columns": ["..."],
# MAGIC       "decisions": { "<out_col>": "<DMN Decision Name>" },
# MAGIC       "post_columns": { "<col>": "<sql expression>" }
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC The binding declares **no joins, no per-column SQL**. The MV is the
# MAGIC contract: every DMN input must appear as a same-named column on the MV
# MAGIC (snake_case lowercase). For the rare exception (unit conversion, derived
# MAGIC value), an optional top-level `columns` map maps `dmn_input → sql_expr`,
# MAGIC overriding name-matching for that input only.

# COMMAND ----------

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

# COMMAND ----------

_SAFE_EXPR_RE = re.compile(r"^[A-Za-z0-9_,.\s+\-*/()'\"<>=!:?]+$")
_SNAKE_RE     = re.compile(r"^[a-z][a-z0-9_]*$")

# Whitelist of SQL functions allowed in optional `columns` overrides and
# `post_columns`. Keeps a stray binding from running arbitrary UDFs.
_ALLOWED_FUNCS = {
    "coalesce", "cast", "current_timestamp", "current_date",
    "abs", "round", "floor", "ceil", "least", "greatest",
    "if", "case", "when", "then", "else", "end",
    "concat", "lower", "upper", "trim", "length",
    "to_date", "to_timestamp", "date_format",
}

def _check_expr(expr: str, where: str, errors: List[str]) -> None:
    if not _SAFE_EXPR_RE.match(expr or ""):
        errors.append(f"{where}: expression contains disallowed characters: {expr!r}")
        return
    # Reject obvious nasties: subqueries, semicolons, comments
    lowered = expr.lower()
    for forbidden in (";", "--", "/*", "*/", " select ", " from ", " union ", " drop ", " delete ", " update ", " insert "):
        if forbidden in f" {lowered} ":
            errors.append(f"{where}: expression contains forbidden token {forbidden!r}: {expr!r}")
            return
    # Function names must be in the whitelist (any token followed by `(`)
    for fname in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(", expr):
        if fname.lower() not in _ALLOWED_FUNCS:
            errors.append(f"{where}: function {fname}() not in allowed list {sorted(_ALLOWED_FUNCS)}: {expr!r}")

# COMMAND ----------

@dataclass
class PreflightResult:
    binding: Dict[str, Any]
    input_view: str
    input_view_columns: List[str]
    column_exprs: Dict[str, str]   # dmn_input_name → sql_expression to use against the MV
    warnings: List[str]

def _validate_schema(binding: Dict[str, Any], errors: List[str]) -> None:
    """Lightweight JSON-schema check (no jsonschema dep). Catches structural problems."""
    if binding.get("version") != 2:
        errors.append(f"binding.version must be 2 (got {binding.get('version')!r})")
    if not isinstance(binding.get("input_view"), str) or not binding["input_view"]:
        errors.append("binding.input_view must be a non-empty fully-qualified table name")
    if "outputs" not in binding or not isinstance(binding["outputs"], dict) or not binding["outputs"]:
        errors.append("binding.outputs must be a non-empty object")
        return
    for tname, tspec in binding["outputs"].items():
        path = f"binding.outputs.{tname}"
        if not isinstance(tspec, dict):
            errors.append(f"{path}: must be an object")
            continue
        if not isinstance(tspec.get("decisions"), dict) or not tspec["decisions"]:
            errors.append(f"{path}.decisions: must be a non-empty {{out_col: 'DMN Decision'}} map")
        carry = tspec.get("carry_columns", [])
        if not isinstance(carry, list) or any(not isinstance(c, str) for c in carry):
            errors.append(f"{path}.carry_columns: must be a list of strings")
        post = tspec.get("post_columns", {})
        if not isinstance(post, dict):
            errors.append(f"{path}.post_columns: must be an object {{col: 'sql_expr'}}")
        mode = tspec.get("mode", "overwrite")
        if mode not in {"overwrite", "append"}:
            errors.append(f"{path}.mode: must be 'overwrite' or 'append' (got {mode!r})")
    cols = binding.get("columns", {})
    if not isinstance(cols, dict):
        errors.append("binding.columns (optional): must be an object {dmn_input: 'sql_expr'}")

def preflight(
    spark,
    binding_raw: str,
    dmn_input_names: List[str],
    dmn_decision_names: List[str],
    *,
    catalog: str,
    schema: str,
) -> PreflightResult:
    """
    Validate a v2 binding against the DMN and the input MV.

    Returns a PreflightResult with the resolved column expressions for every
    DMN input. Raises RuntimeError if any check fails — message lists every
    error so the operator can fix them all in one pass.
    """
    errors:   List[str] = []
    warnings: List[str] = []

    # ── Parse + structural validation ────────────────────────────────────
    try:
        binding = json.loads(binding_raw)
    except Exception as e:
        raise RuntimeError(f"binding_json is not valid JSON: {e}") from None

    _validate_schema(binding, errors)
    if errors:
        raise RuntimeError("Binding schema errors:\n  - " + "\n  - ".join(errors))

    input_view = binding["input_view"]
    if "." not in input_view:
        input_view = f"{catalog}.{schema}.{input_view}"

    # ── Materialized view exists, is queryable ───────────────────────────
    try:
        mv_cols = [f.name for f in spark.table(input_view).schema.fields]
    except Exception as e:
        raise RuntimeError(
            f"input_view '{input_view}' does not exist or is not queryable: "
            f"{type(e).__name__}: {e}"
        ) from None
    mv_col_set = set(mv_cols)

    # ── Build column expression map: name-match by default, override via
    #    optional top-level `columns` block.
    overrides = binding.get("columns", {}) or {}
    column_exprs: Dict[str, str] = {}
    for dmn_name in dmn_input_names:
        if not _SNAKE_RE.match(dmn_name):
            warnings.append(
                f"DMN input '{dmn_name}' is not lowercase snake_case — "
                f"convention is to match column names exactly"
            )
        if dmn_name in overrides:
            expr = overrides[dmn_name]
            _check_expr(expr, f"binding.columns.{dmn_name}", errors)
            column_exprs[dmn_name] = expr
        elif dmn_name in mv_col_set:
            column_exprs[dmn_name] = dmn_name
        else:
            errors.append(
                f"DMN input '{dmn_name}' has no matching column on input_view "
                f"'{input_view}' and no override in binding.columns. "
                f"Either add the column to the MV or add a 'columns' override."
            )

    # ── Stale override warnings ──────────────────────────────────────────
    for k in overrides:
        if k not in dmn_input_names:
            warnings.append(
                f"binding.columns maps '{k}' but the DMN does not declare it — ignored"
            )

    # ── Output decision name match + post_columns expression check ──────
    for out_table, out_spec in binding["outputs"].items():
        for out_col, dec_name in out_spec["decisions"].items():
            if dec_name not in dmn_decision_names:
                errors.append(
                    f"output '{out_table}.{out_col}' references DMN decision "
                    f"'{dec_name}' which does not exist. Available: {dmn_decision_names}"
                )
        for c in out_spec.get("carry_columns") or []:
            if c not in mv_col_set:
                errors.append(
                    f"output '{out_table}.carry_columns' references column "
                    f"'{c}' which is not on input_view '{input_view}'"
                )
        for col_name, expr in (out_spec.get("post_columns") or {}).items():
            _check_expr(expr, f"binding.outputs.{out_table}.post_columns.{col_name}", errors)

    if errors:
        raise RuntimeError("Pre-flight failed:\n  - " + "\n  - ".join(errors))

    return PreflightResult(
        binding=binding,
        input_view=input_view,
        input_view_columns=mv_cols,
        column_exprs=column_exprs,
        warnings=warnings,
    )
