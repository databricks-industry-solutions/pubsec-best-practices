import { ACCENT } from '../lib/theme';
import type { RunMeta, TaskSummary } from '../lib/types';
import { StatusPill, displayStatus } from './StatusPill';

// Shown when a parent run has multiple tasks (PARENT_RUN_HAS_MULTIPLE_TASKS):
// the user picks which task's cluster to read logs from (spec §3.3, §5).
export function TaskSelector({
  meta,
  tasks,
  onPick,
  onBrowse,
}: {
  meta: RunMeta | null;
  tasks: TaskSummary[];
  onPick: (taskRunId: number) => void;
  onBrowse: () => void;
}) {
  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
      <div
        style={{
          flex: 'none',
          padding: '13px 20px 12px',
          background: 'var(--app-panel)',
          borderBottom: '1px solid var(--app-border)',
          display: 'flex',
          alignItems: 'center',
          gap: '11px',
          flexWrap: 'wrap',
        }}
      >
        <span style={{ fontFamily: 'var(--font-serif)', fontWeight: 700, fontSize: '17px', color: 'var(--app-text)' }}>
          {meta?.run_name || `job ${meta?.job_id ?? '—'}`}
        </span>
        {meta && <StatusPill status={displayStatus(meta.state, meta.result_state)} />}
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--app-muted)' }}>
          run {meta?.run_id ?? '—'}
        </span>
      </div>

      <div style={{ flex: 1, overflowY: 'auto', minHeight: 0, padding: '28px 40px' }} className="elv-scroll">
        <div style={{ maxWidth: '760px', margin: '0 auto' }}>
          <h2 style={{ margin: '0 0 6px', fontFamily: 'var(--font-serif)', fontSize: '20px', color: 'var(--app-text)' }}>
            This run has multiple tasks
          </h2>
          <p style={{ margin: '0 0 20px', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
            Each task ran on its own cluster. Choose the task whose executor logs you want to read.
          </p>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            {tasks.map((t) => (
              <button
                key={t.run_id}
                onClick={() => onPick(t.run_id)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  gap: '12px',
                  textAlign: 'left',
                  background: 'var(--app-panel)',
                  border: '1px solid var(--app-border)',
                  borderRadius: '8px',
                  padding: '14px 16px',
                  cursor: 'pointer',
                  font: 'inherit',
                }}
              >
                <div style={{ minWidth: 0 }}>
                  <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, fontSize: '14px', color: 'var(--app-text)' }}>
                    {t.task_key || `task ${t.run_id}`}
                  </div>
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--app-muted)', marginTop: '4px' }}>
                    task run {t.run_id}
                    {t.cluster_id ? ` · cluster ${t.cluster_id}` : ' · no cluster instance'}
                  </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px', flex: 'none' }}>
                  {t.state && <StatusPill status={t.state} />}
                  <span style={{ color: ACCENT, fontWeight: 700, fontSize: '13px', fontFamily: 'var(--font-sans)' }}>Open →</span>
                </div>
              </button>
            ))}
          </div>

          <div style={{ marginTop: '22px' }}>
            <button
              onClick={onBrowse}
              style={{
                height: '38px',
                padding: '0 16px',
                border: '1px solid var(--app-border-strong)',
                borderRadius: '5px',
                background: 'var(--app-panel)',
                color: 'var(--app-text)',
                fontFamily: 'var(--font-sans)',
                fontWeight: 700,
                fontSize: '13px',
                cursor: 'pointer',
              }}
            >
              Back to recent runs
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
