"""Batch scoring routes."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from databricks.sdk.service.jobs import SubmitTask, NotebookTask

from server.services.databricks import (
  get_workspace_client, CATALOG, SCHEMA, VOLUME, JOB_ID,
)
from server.services.sql import run_sql

router = APIRouter()


class RunScoringRequest(BaseModel):
  limit: Optional[int] = None
  version_id: Optional[str] = None  # if None, uses ACTIVE version


@router.get('/history', operation_id='getScoringHistory')
async def get_scoring_history():
  """Get history of past scoring runs."""
  w = get_workspace_client()
  try:
    runs_response = w.jobs.list_runs(job_id=JOB_ID, limit=10)
    history = []
    for run in (runs_response or []):
      state = run.state
      lcs = state.life_cycle_state.value if state and state.life_cycle_state else None
      rs = state.result_state.value if state and state.result_state else None
      duration_s = None
      if run.start_time and run.end_time:
        duration_s = round((run.end_time - run.start_time) / 1000)
      notebook_output = None
      if rs in ('SUCCESS', 'FAILED'):
        try:
          # For run_now jobs, notebook output is on the task run, not the parent run.
          # Get task run_id from the run's tasks list.
          run_detail = w.jobs.get_run(run.run_id)
          for task in (run_detail.tasks or []):
            if task.run_id:
              try:
                task_out = w.jobs.get_run_output(task.run_id)
                if task_out.notebook_output:
                  notebook_output = task_out.notebook_output.result
                  break
              except Exception:
                pass
        except Exception:
          pass
      history.append({
        'run_id': run.run_id,
        'run_name': run.run_name or '',
        'run_page_url': run.run_page_url,
        'life_cycle_state': lcs,
        'result_state': rs,
        'state_message': state.state_message or '' if state else '',
        'start_time': run.start_time,
        'duration_s': duration_s,
        'notebook_output': notebook_output,
      })
    return {'history': history}
  except Exception as e:
    raise HTTPException(500, f'Failed to get run history: {e}')


@router.get('/results', operation_id='getScoringResults')
async def get_scoring_results():
  """Get summary of scored results."""
  summary = run_sql(f"""
    SELECT
      count(*) as total,
      sum(case when recommended_action = 'PASS' then 1 else 0 end) as pass_count,
      sum(case when recommended_action = 'REVIEW' then 1 else 0 end) as review_count,
      sum(case when recommended_action = 'FLAG' then 1 else 0 end) as flag_count,
      round(avg(audit_score), 1) as avg_score,
      max(scored_at) as last_scored_at,
      max(dmn_version) as dmn_version
    FROM {CATALOG}.{SCHEMA}.scored_results
  """)
  top_flags = run_sql(f"""
    SELECT return_id, filing_status, agi, audit_score, recommended_action
    FROM {CATALOG}.{SCHEMA}.scored_results
    ORDER BY audit_score DESC
    LIMIT 10
  """)
  return {'summary': summary[0] if summary else {}, 'top_flags': top_flags}


@router.post('/run', operation_id='runScoring')
async def run_scoring(req: RunScoringRequest = RunScoringRequest()):
  """Trigger the saved scoring job via run_now (runs as job owner, not SP)."""
  w = get_workspace_client()
  try:
    # Build notebook parameters. Always pass catalog/schema/volume from the
    # app's environment so the job uses the same UC target the app is wired to,
    # regardless of what's saved as the job's default widget values.
    notebook_params = {
      'catalog': CATALOG,
      'schema': SCHEMA,
      'volume': VOLUME,
    }
    if req.limit is not None:
      notebook_params['proof_limit'] = str(req.limit)
    if req.version_id:
      notebook_params['version_id'] = req.version_id

    # run_now executes as the job owner, not the app service principal —
    # so the job's personal/interactive cluster is usable.
    run = w.jobs.run_now(
      job_id=JOB_ID,
      notebook_params=notebook_params if notebook_params else None,
    )

    run_details = w.jobs.get_run(run.run_id)
    run_url = run_details.run_page_url or ''

    return {'run_id': run.run_id, 'run_url': run_url, 'status': 'SUBMITTED', 'limit': req.limit}
  except Exception as e:
    raise HTTPException(500, f'Failed to run scoring job: {e}')


@router.get('/run/{run_id}', operation_id='getScoringRunStatus')
async def get_scoring_status(run_id: int):
  """Check status of a scoring run."""
  w = get_workspace_client()
  try:
    run = w.jobs.get_run(run_id)
    state = run.state
    result = {
      'run_id': run_id,
      'life_cycle_state': state.life_cycle_state.value if state.life_cycle_state else None,
      'result_state': state.result_state.value if state.result_state else None,
      'state_message': state.state_message or '',
    }
    if state.result_state:
      rs_val = state.result_state.value
      notebook_output = None
      try:
        # For run_now jobs, notebook output lives on the task run, not the parent run.
        # This mirrors the same pattern used in get_scoring_history.
        run_detail = w.jobs.get_run(run_id)
        for task in (run_detail.tasks or []):
          if task.run_id:
            try:
              task_out = w.jobs.get_run_output(task.run_id)
              if task_out.notebook_output:
                notebook_output = task_out.notebook_output.result
                break
            except Exception:
              pass
      except Exception:
        pass
      result['notebook_output'] = notebook_output
    return result
  except Exception as e:
    raise HTTPException(500, f'Failed to get run status: {e}')
