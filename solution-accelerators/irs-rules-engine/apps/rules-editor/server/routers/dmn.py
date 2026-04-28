"""DMN rule version management routes."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.services.databricks import (
  get_workspace_client, CATALOG, SCHEMA, VOLUME_PATH,
)
from server.services.sql import run_sql

router = APIRouter()


# ── Models ───────────────────────────────────────────────────────────

class SaveDmnRequest(BaseModel):
  dmn_xml: str
  notes: str = ''


class PromoteRequest(BaseModel):
  version_id: str


class EvaluateRequest(BaseModel):
  filing_status: str
  agi: float
  deduction_ratio: float = 0.0
  foreign_income: float = 0.0
  se_income: float = 0.0
  charitable_ratio: float = 0.0
  prior_audit: bool = False
  # IRM 25.1.2 expanded inputs
  reported_income_gap: float = 0.0
  num_w2_forms: int = 1
  w2_agi_ratio: float = 0.85
  years_not_filed: int = 0
  amended_returns_count: int = 0
  num_dependents_claimed: int = 0
  dependents: int = 0
  employment_type: str = 'W2'
  digital_asset_transactions: int = 0
  digital_asset_proceeds: float = 0.0
  capital_gains: float = 0.0
  dmn_version_id: Optional[str] = None  # if None, uses hardcoded rules

# ── Versions ─────────────────────────────────────────────────────────

@router.get('/versions', operation_id='listVersions')
async def list_versions():
  """List all rule set versions, newest first. Deduplicates by version_id."""
  rows = run_sql(f"""
    SELECT version_id, rule_set_name, status, created_by, created_at, notes
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY version_id ORDER BY created_at DESC) AS rn
      FROM {CATALOG}.{SCHEMA}.rule_versions
    )
    WHERE rn = 1
    ORDER BY created_at DESC
  """)
  return {'versions': rows}


@router.get('/active', operation_id='getActiveDmn')
async def get_active_dmn():
  """Load the currently ACTIVE DMN version."""
  rows = run_sql(f"""
    SELECT version_id FROM {CATALOG}.{SCHEMA}.rule_versions
    WHERE status = 'ACTIVE'
    ORDER BY promoted_at DESC LIMIT 1
  """)
  if not rows:
    raise HTTPException(404, 'No active version found')
  return await get_dmn(rows[0]['version_id'])


@router.get('/sample-returns', operation_id='getSampleReturns')
async def get_sample_returns(count: int = 10):
  """Get a random sample of tax returns from Delta for testing."""
  # Databricks SQL doesn't support parameterized LIMIT — inline the value.
  # count is validated as int by FastAPI and clamped, so this is safe.
  limit = min(max(count, 1), 50)
  rows = run_sql(f"""
    SELECT return_id, filing_status, agi, se_income, foreign_income,
           prior_audit, coalesce(deduction_ratio, 0) as deduction_ratio,
           coalesce(charitable_ratio, 0) as charitable_ratio,
           coalesce(reported_income_gap, 0) as reported_income_gap,
           coalesce(num_w2_forms, 1) as num_w2_forms,
           coalesce(w2_agi_ratio, 0.85) as w2_agi_ratio,
           coalesce(years_not_filed, 0) as years_not_filed,
           coalesce(amended_returns_count, 0) as amended_returns_count,
           coalesce(num_dependents_claimed, 0) as num_dependents_claimed,
           coalesce(dependents, 0) as dependents,
           coalesce(employment_type, 'W2') as employment_type,
           coalesce(digital_asset_transactions, 0) as digital_asset_transactions,
           coalesce(digital_asset_proceeds, 0) as digital_asset_proceeds,
           coalesce(capital_gains, 0) as capital_gains
    FROM {CATALOG}.{SCHEMA}.tax_returns
    ORDER BY rand()
    LIMIT {limit}
  """)
  return {'returns': rows}


@router.get('/{version_id}', operation_id='getDmn')
async def get_dmn(version_id: str):
  """Load DMN XML for a specific version."""
  rows = run_sql(
    f"""
      SELECT dmn_path, status, rule_set_name, notes
      FROM {CATALOG}.{SCHEMA}.rule_versions
      WHERE version_id = :version_id
    """,
    parameters=[{'name': 'version_id', 'value': version_id, 'type': 'STRING'}],
  )
  if not rows:
    raise HTTPException(404, f'Version {version_id} not found')
  row = rows[0]

  # Fix #2 — path traversal guard: ensure the stored path is within the expected volume.
  dmn_path: str = row['dmn_path']
  if not dmn_path.startswith(VOLUME_PATH):
    raise HTTPException(400, 'Invalid DMN path: outside permitted volume')

  w = get_workspace_client()
  try:
    resp = w.files.download(dmn_path)
    dmn_xml = resp.contents.read().decode('utf-8')
  except Exception as e:
    raise HTTPException(500, f'Failed to read DMN file: {e}')

  return {
    'version_id': version_id,
    'dmn_xml': dmn_xml,
    'status': row['status'],
    'rule_set_name': row['rule_set_name'],
    'notes': row['notes'],
  }


# ── Deduplicate (admin) ──────────────────────────────────────────────

@router.post('/dedup', operation_id='dedupVersions')
async def dedup_versions():
  """Remove duplicate version_id rows from the Delta table, keeping the latest."""
  run_sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.rule_versions AS target
    USING (
      SELECT version_id, MAX(created_at) AS keep_created_at
      FROM {CATALOG}.{SCHEMA}.rule_versions
      GROUP BY version_id
      HAVING COUNT(*) > 1
    ) AS dups
    ON target.version_id = dups.version_id
       AND target.created_at < dups.keep_created_at
    WHEN MATCHED THEN DELETE
  """)
  return {'status': 'ok'}


# ── Save ─────────────────────────────────────────────────────────────

@router.post('/save', operation_id='saveDmn')
async def save_dmn(req: SaveDmnRequest):
  """Save a new DRAFT version of the DMN rules."""
  # Fix #3 — size limit: 2 MB max
  if len(req.dmn_xml) > 2_000_000:
    raise HTTPException(400, 'DMN file too large (max 2MB)')

  # Fix #7 — microsecond precision prevents same-second collisions
  version_id = f"v{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:19]}"
  filename = f'irs_tax_review_{version_id}.dmn'
  volume_file_path = f'{VOLUME_PATH}/{filename}'

  w = get_workspace_client()
  try:
    import io
    w.files.upload(volume_file_path, io.BytesIO(req.dmn_xml.encode('utf-8')), overwrite=True)
  except Exception as e:
    raise HTTPException(500, f'Failed to write DMN file: {e}')

  # Fix #1 — parameterized INSERT
  run_sql(
    f"""
      INSERT INTO {CATALOG}.{SCHEMA}.rule_versions
      (version_id, rule_set_name, dmn_path, status, created_by, created_at, notes)
      VALUES (
        :version_id,
        'IRS Tax Return Review',
        :dmn_path,
        'DRAFT',
        current_user(),
        current_timestamp(),
        :notes
      )
    """,
    parameters=[
      {'name': 'version_id',  'value': version_id,        'type': 'STRING'},
      {'name': 'dmn_path',    'value': volume_file_path,  'type': 'STRING'},
      {'name': 'notes',       'value': req.notes,         'type': 'STRING'},
    ],
  )

  return {'version_id': version_id, 'status': 'DRAFT', 'dmn_path': volume_file_path}


# ── Promote ──────────────────────────────────────────────────────────

@router.post('/promote', operation_id='promoteVersion')
async def promote_version(req: PromoteRequest):
  """Promote a version to ACTIVE. Archives all other active/draft versions atomically."""
  rows = run_sql(
    f"""
      SELECT status FROM {CATALOG}.{SCHEMA}.rule_versions
      WHERE version_id = :version_id
    """,
    parameters=[{'name': 'version_id', 'value': req.version_id, 'type': 'STRING'}],
  )
  if not rows:
    raise HTTPException(404, f'Version {req.version_id} not found')

  # Fix #6 — atomic CASE-based UPDATE: one statement, no race window between archive and promote.
  run_sql(
    f"""
      UPDATE {CATALOG}.{SCHEMA}.rule_versions
      SET
        status = CASE
          WHEN version_id = :target THEN 'ACTIVE'
          ELSE 'ARCHIVED'
        END,
        promoted_by = CASE
          WHEN version_id = :target THEN current_user()
          ELSE NULL
        END,
        promoted_at = CASE
          WHEN version_id = :target THEN current_timestamp()
          ELSE NULL
        END
      WHERE status IN ('ACTIVE', 'DRAFT') OR version_id = :target
    """,
    parameters=[{'name': 'target', 'value': req.version_id, 'type': 'STRING'}],
  )

  return {'version_id': req.version_id, 'status': 'ACTIVE'}


# ── Evaluate ─────────────────────────────────────────────────────────

def _score_python(req: EvaluateRequest) -> tuple[float, list[str]]:
  """
  Pure-Python implementation of the IRS scoring rules.
  21 rules across 6 IRM 25.1.2 categories, COLLECT+SUM model.
  """
  score = 0.0
  fired = []

  # ── Category A: Income Indicators (IRM 25.1.2.2) ───────────────────

  # R1: High income
  if req.agi > 500000:
    score += 25
    fired.append('R1: High-income return — AGI > $500K [IRM 25.1.2.2(3)]')

  # R2: FBAR/FATCA compliance risk
  if req.foreign_income > 10000:
    score += 20
    fired.append('R2: Foreign income > $10K — FBAR/FATCA risk [IRM 25.1.2.2(5)]')

  # R3: Schedule C audit risk
  if req.se_income > 200000:
    score += 15
    fired.append('R3: Schedule C risk — SE income > $200K [IRM 25.1.2.2(4)]')

  # R8: Unreported income — bank deposits method
  if req.reported_income_gap > 0.25:
    score += 20
    fired.append('R8: Unreported income — bank deposits exceed reported by >25% [IRM 25.1.2.2(1)]')

  # R9: Severe income gap (stacks with R8)
  if req.reported_income_gap > 0.50:
    score += 10
    fired.append('R9: Severe income gap — deposits exceed reported by >50% [IRM 25.1.2.2(1)]')

  # R10: W-2 AGI discrepancy
  if req.num_w2_forms >= 2 and req.w2_agi_ratio < 0.5:
    score += 10
    fired.append('R10: W-2/AGI discrepancy — multiple W-2s but low wage ratio [IRM 25.1.2.2(2)]')

  # ── Category B: Deduction/Expense Indicators (IRM 25.1.2.3) ────────

  # R4: Disproportionate deductions
  if req.deduction_ratio > 0.5:
    score += 20
    fired.append('R4: Disproportionate deductions — ratio > 50% [IRM 25.1.2.3(1)]')

  # R5: Charitable deduction overstatement
  if req.charitable_ratio > 0.3:
    score += 15
    fired.append('R5: Charitable overstatement — ratio > 30% [IRM 25.1.2.3(2)]')

  # R11: False dependency claims
  if req.num_dependents_claimed > req.dependents + 1:
    score += 10
    fired.append('R11: False dependency claims — claimed exceeds verified by >1 [IRM 25.1.2.3(3)]')

  # R12: Business expense as personal
  if req.employment_type == 'W2' and req.deduction_ratio > 0.4:
    score += 10
    fired.append('R12: W-2 employee with high deductions — possible personal expenses [IRM 25.1.2.3(4)]')

  # ── Category C: Filing/Conduct Indicators (IRM 25.1.2.4) ───────────

  # R6: MFS filing status
  if req.filing_status == 'MFS':
    score += 10
    fired.append('R6: Married Filing Separately — higher audit rate [IRM 25.1.2.4(1)]')

  # R7: Prior audit
  if req.prior_audit:
    score += 15
    fired.append('R7: Repeat audit candidate [IRM 25.1.2.4(2)]')

  # R13: Multi-year non-filing
  if req.years_not_filed >= 2:
    score += 15
    fired.append('R13: Multi-year non-filing — 2+ years missed [IRM 25.1.2.4(3)]')

  # R14: Single year non-filing
  if req.years_not_filed == 1:
    score += 5
    fired.append('R14: Single year non-filing [IRM 25.1.2.4(3)]')

  # ── Category D: Books/Records Indicators (IRM 25.1.2.5) ────────────

  # R15: Frequent amendments
  if req.amended_returns_count >= 2:
    score += 10
    fired.append('R15: Frequent amendments — 2+ in 3 years [IRM 25.1.2.5(1)]')

  # R16: Amendment with high deductions
  if req.amended_returns_count >= 1 and req.deduction_ratio > 0.4:
    score += 5
    fired.append('R16: Amendment + high deductions — possible inflated claims [IRM 25.1.2.5(2)]')

  # ── Category E: Employment Tax Indicators (IRM 25.1.2.6) ───────────

  # R17: Cash intensive business
  if req.employment_type == 'CASH_INTENSIVE' and req.se_income > 100000:
    score += 15
    fired.append('R17: Cash-intensive business — SE > $100K [IRM 25.1.2.6(1)]')

  # R18: Worker misclassification
  if req.employment_type == '1099' and req.se_income > 150000:
    score += 10
    fired.append('R18: Worker misclassification signal — 1099 with SE > $150K [IRM 25.1.2.6(2)]')

  # ── Category F: Digital Asset Indicators (IRM 25.1.2.7) ────────────

  # R19: Unreported crypto
  if req.digital_asset_transactions > 0 and req.capital_gains <= 0:
    score += 15
    fired.append('R19: Unreported digital asset gains — transactions but no capital gains [IRM 25.1.2.7(1)]')

  # R20: Large digital asset activity
  if req.digital_asset_proceeds > 100000:
    score += 10
    fired.append('R20: Large digital asset activity — proceeds > $100K [IRM 25.1.2.7(2)]')

  # R21: Crypto proceeds vs reported gains gap
  if req.digital_asset_proceeds > 50000 and req.capital_gains < req.digital_asset_proceeds * 0.5:
    score += 5
    fired.append('R21: Digital asset proceeds gap — reported gains < 50% of proceeds [IRM 25.1.2.7(3)]')

  return score, fired


@router.post('/evaluate', operation_id='evaluateReturn')
async def evaluate_return(req: EvaluateRequest):
  """Evaluate a single tax return using Python scoring logic.
  Mirrors the Drools DMN rules for instant test feedback.
  Batch scoring uses the full Drools engine on-cluster.
  """
  score, fired_rules = _score_python(req)

  if score < 30:
    action = 'PASS'
  elif score < 60:
    action = 'REVIEW'
  else:
    action = 'FLAG'

  return {
    'audit_score': score,
    'recommended_action': action,
    'fired_rules': fired_rules,
    'input': req.model_dump(),
  }
