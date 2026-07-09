import { useEffect, useMemo, useState, type CSSProperties } from 'react';
import type { LogFileNode, LogTree, RunMeta } from '../lib/types';
import { FileTooLargeError, LogFileFetchError } from '../lib/types';
import { fetchLogFile } from '../lib/api';
import { formatBytes, formatTimestamp } from '../lib/format';
import { ACCENT } from '../lib/theme';
import { CloseIcon, DownloadIcon, FolderIcon, InfoIcon, SearchIcon } from './icons';
import { StatusPill, displayStatus } from './StatusPill';
import { ExecutorSelector } from './ExecutorSelector';
import { LogBody } from './LogBody';

type ViewTab = 'driver' | 'executors';

interface LoadState {
  loading: boolean;
  text: string;
  startLine: number;
  totalSize: number | null;
  earlierHidden: number | null;
  mode: 'tail' | 'full' | null;
  tooLarge: boolean;
  error: string | null;
}

const EMPTY_LOAD: LoadState = {
  loading: false,
  text: '',
  startLine: 1,
  totalSize: null,
  earlierHidden: null,
  mode: null,
  tooLarge: false,
  error: null,
};

// Pick the default file for a leaf: newest stderr-like file, else newest file.
function pickDefaultFile(files: LogFileNode[]): LogFileNode | null {
  if (files.length === 0) return null;
  const byModifiedDesc = [...files].sort((a, b) => (b.modified ?? 0) - (a.modified ?? 0));
  const stderr = byModifiedDesc.find((f) => f.file_kind === 'stderr');
  return stderr ?? byModifiedDesc[0];
}

const segBtn = (active: boolean): CSSProperties => ({
  border: 'none',
  cursor: 'pointer',
  borderRadius: '4px',
  padding: '5px 12px',
  fontFamily: 'var(--font-mono)',
  fontWeight: active ? 600 : 400,
  fontSize: '12px',
  background: active ? ACCENT : 'transparent',
  color: active ? '#fff' : 'var(--app-muted)',
});

export function MainViewer({
  meta,
  tree,
  initialView,
  initialGroup,
  initialExec,
  initialFile,
  onSelectionChange,
}: {
  meta: RunMeta | null;
  tree: LogTree;
  initialView: ViewTab | null;
  initialGroup: string | null;
  initialExec: string | null;
  initialFile: string | null;
  onSelectionChange: (sel: { view: ViewTab; group: string | null; exec: string | null; file: string | null }) => void;
}) {
  const hasDriver = tree.driver_files.length > 0;
  const hasExecutors = tree.app_groups.some((g) => g.executors.length > 0);

  // ---- resolve initial selection ---------------------------------------- //
  const defaultView: ViewTab = initialView ?? (hasExecutors ? 'executors' : 'driver');
  const [view, setView] = useState<ViewTab>(defaultView === 'executors' && !hasExecutors ? 'driver' : defaultView);

  const firstGroup = tree.app_groups[0] ?? null;
  const firstExec = firstGroup?.executors[0] ?? null;

  const [appId, setAppId] = useState<string | null>(
    initialGroup ?? (view === 'executors' ? firstGroup?.app_id ?? null : null),
  );
  const [execId, setExecId] = useState<string | null>(
    initialExec ?? (view === 'executors' ? firstExec?.executor_id ?? null : null),
  );

  // Resolve the currently selected leaf's file list.
  const currentFiles: LogFileNode[] = useMemo(() => {
    if (view === 'driver') return tree.driver_files;
    const g = tree.app_groups.find((x) => x.app_id === appId);
    const e = g?.executors.find((x) => x.executor_id === execId);
    return e?.files ?? [];
  }, [view, appId, execId, tree]);

  const defaultFile = useMemo(() => pickDefaultFile(currentFiles), [currentFiles]);
  const [fileName, setFileName] = useState<string | null>(initialFile ?? defaultFile?.name ?? null);

  // Keep fileName valid whenever the leaf's files change.
  useEffect(() => {
    const stillThere = currentFiles.some((f) => f.name === fileName);
    if (!stillThere) setFileName(pickDefaultFile(currentFiles)?.name ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentFiles]);

  const currentFile = currentFiles.find((f) => f.name === fileName) ?? defaultFile ?? null;

  const [filter, setFilter] = useState('');
  const [wrap, setWrap] = useState(false);
  const [load, setLoad] = useState<LoadState>(EMPTY_LOAD);
  const [copied, setCopied] = useState(false);

  // Report selection upward for URL persistence.
  useEffect(() => {
    onSelectionChange({ view, group: view === 'executors' ? appId : null, exec: view === 'executors' ? execId : null, file: fileName });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view, appId, execId, fileName]);

  // ---- fetch the selected file (tail by default) ------------------------ //
  const loadFile = async (mode: 'tail' | 'full') => {
    if (!currentFile) {
      setLoad(EMPTY_LOAD);
      return;
    }
    setLoad((s) => ({ ...s, loading: true, error: null, tooLarge: false }));
    try {
      const { text, meta: m } = await fetchLogFile(currentFile.file_ref, mode);
      // First line number = number of earlier lines skipped + 1. We can't know
      // the exact earlier line count from a byte offset, so number the loaded
      // window from 1 when full, and show the tail banner (byte-based) when
      // earlier bytes were hidden.
      const startLine = 1;
      setLoad({
        loading: false,
        text,
        startLine,
        totalSize: m.totalSize,
        earlierHidden: m.earlierBytesHidden,
        mode: m.mode ?? mode,
        tooLarge: false,
        error: null,
      });
    } catch (err) {
      if (err instanceof FileTooLargeError) {
        setLoad((s) => ({ ...s, loading: false, tooLarge: true, totalSize: err.totalSize }));
        return;
      }
      const msg = err instanceof LogFileFetchError ? err.message : 'failed to load log file';
      setLoad({ ...EMPTY_LOAD, error: msg });
    }
  };

  // Reset + tail-load whenever the selected file changes.
  useEffect(() => {
    setFilter('');
    loadFile('tail');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentFile?.file_ref]);

  // "Run context" = we resolved from a run/job id and have real run metadata.
  // Reached by cluster id / browse => meta has no run_name/run_id/state, so we
  // render a cluster-first header instead of empty "job — / run — / UNKNOWN".
  const hasRunContext = !!(meta && (meta.run_name || meta.run_id != null || meta.job_id != null));
  const status = hasRunContext && (meta?.state || meta?.result_state)
    ? displayStatus(meta.state, meta.result_state)
    : '';
  const tailTruncated = load.mode === 'tail' && !!load.earlierHidden && load.earlierHidden > 0;
  const showLoadFull = tailTruncated && !load.tooLarge;
  const filtered = filter.trim().length > 0;

  const copyPath = () => {
    // We never have the raw Volume path client-side (security boundary). Copy a
    // shareable deep link to this exact selection instead.
    const link = window.location.href;
    navigator.clipboard?.writeText(link).catch(() => undefined);
    setCopied(true);
    setTimeout(() => setCopied(false), 1600);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: 0, flex: 1 }}>
      {/* header — adapts to how the logs were reached. When there's run/job
          context (resolved from a run/job id) we lead with the job name +
          status + run id. When reached by cluster id / browse there is no run
          metadata, so we lead with the cluster and omit the empty job/run/
          status scaffolding (no more "job — UNKNOWN run —"). */}
      <div style={{ flex: 'none', padding: '13px 20px 12px', background: 'var(--app-panel)', borderBottom: '1px solid var(--app-border)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '11px', flexWrap: 'wrap' }}>
          {hasRunContext ? (
            <>
              <span style={{ fontFamily: 'var(--font-serif)', fontWeight: 700, fontSize: '17px', color: 'var(--app-text)' }}>
                {meta?.run_name || `job ${meta?.job_id}`}
              </span>
              {status && <StatusPill status={status} />}
              {meta?.run_id != null && (
                <span style={{ fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--app-muted)' }}>run {meta.run_id}</span>
              )}
              <span style={{ color: 'var(--app-border-strong)' }}>·</span>
              <span style={{ fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--app-muted)' }}>cluster {tree.cluster_id}</span>
            </>
          ) : (
            <>
              <span style={{ fontFamily: 'var(--font-serif)', fontWeight: 700, fontSize: '17px', color: 'var(--app-text)' }}>Cluster logs</span>
              <span style={{ fontFamily: 'var(--font-mono)', fontSize: '13px', color: 'var(--app-text)' }}>{tree.cluster_id}</span>
            </>
          )}
          {meta?.run_page_url && (
            <>
              <span style={{ color: 'var(--app-border-strong)' }}>·</span>
              <a href={meta.run_page_url} target="_blank" rel="noreferrer" style={{ fontSize: '12px' }}>
                run page
              </a>
            </>
          )}
        </div>
      </div>

      {/* Driver | Executors tabs */}
      <div
        style={{
          flex: 'none',
          display: 'flex',
          alignItems: 'stretch',
          gap: '2px',
          padding: '0 20px',
          background: 'var(--app-panel)',
          borderBottom: '1px solid var(--app-border)',
        }}
      >
        <TabButton label="Driver" active={view === 'driver'} disabled={!hasDriver} count={tree.driver_files.length} onClick={() => setView('driver')} />
        <TabButton
          label="Executors"
          active={view === 'executors'}
          disabled={!hasExecutors}
          count={tree.app_groups.reduce((n, g) => n + g.executors.length, 0)}
          onClick={() => {
            setView('executors');
            if (!appId && firstGroup) {
              setAppId(firstGroup.app_id);
              setExecId(firstGroup.executors[0]?.executor_id ?? null);
            }
          }}
        />
      </div>

      {/* content: executor selector (left column) + file view (right) */}
      <div style={{ flex: 1, display: 'flex', minHeight: 0 }}>
        {view === 'executors' && (
          <div style={{ width: '250px', flex: 'none', borderRight: '1px solid var(--app-border)', background: 'var(--app-rail)', minHeight: 0 }}>
            <ExecutorSelector
              groups={tree.app_groups}
              selectedAppId={appId}
              selectedExecId={execId}
              onSelect={(a, e) => {
                setAppId(a);
                setExecId(e);
              }}
            />
          </div>
        )}

        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0 }}>
          {/* file selector + metadata toolbar */}
          <div
            style={{
              flex: 'none',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: '12px',
              padding: '8px 20px',
              background: 'var(--app-panel)',
              borderBottom: '1px solid var(--app-border)',
              flexWrap: 'wrap',
            }}
          >
            <div className="elv-scroll" style={{ display: 'flex', gap: '2px', overflowX: 'auto', background: 'var(--app-tab)', border: '1px solid var(--app-border)', borderRadius: '6px', padding: '2px' }}>
              {currentFiles.length === 0 ? (
                <span style={{ padding: '5px 12px', fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--app-faint)' }}>no files</span>
              ) : (
                currentFiles.map((f) => (
                  <button key={f.name} style={segBtn(f.name === (currentFile?.name ?? ''))} onClick={() => setFileName(f.name)} title={f.name}>
                    {f.name}
                  </button>
                ))
              )}
            </div>
          </div>

          {/* metadata bar */}
          <div
            style={{
              flex: 'none',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: '16px',
              padding: '9px 20px',
              background: 'var(--app-elev)',
              borderBottom: '1px solid var(--app-border)',
              flexWrap: 'wrap',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '14px', minWidth: 0, flexWrap: 'wrap' }}>
              <Meta label="Size" value={load.tooLarge || load.mode === 'full' ? formatBytes(load.totalSize) : `${formatBytes(load.totalSize)} · tail`} />
              <Meta label="Modified" value={formatTimestamp(currentFile?.modified ?? null)} />
              <Meta label="Kind" value={currentFile?.file_kind ?? '—'} />
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  minWidth: 0,
                  background: 'var(--app-input)',
                  border: '1px solid var(--app-border)',
                  borderRadius: '5px',
                  padding: '3px 4px 3px 9px',
                  maxWidth: '360px',
                }}
              >
                <FolderIcon />
                <span
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: '11.5px',
                    color: 'var(--app-muted)',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                  }}
                  title="Shareable link to this exact view (the raw Volume path is never exposed to the browser)"
                >
                  {view === 'driver' ? `driver / ${currentFile?.name ?? ''}` : `executor ${execId} / ${currentFile?.name ?? ''}`}
                </span>
                <button
                  onClick={copyPath}
                  style={{
                    flex: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    borderRadius: '4px',
                    padding: '4px 9px',
                    background: copied ? '#216e1f' : ACCENT,
                    color: '#fff',
                    fontFamily: 'var(--font-sans)',
                    fontWeight: 700,
                    fontSize: '11px',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {copied ? 'Copied link ✓' : 'Copy link'}
                </button>
              </div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '9px', flex: 'none' }}>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '7px',
                  background: 'var(--app-input)',
                  border: '1px solid var(--app-border-strong)',
                  borderRadius: '5px',
                  padding: '0 8px',
                  height: '32px',
                  width: '230px',
                }}
              >
                <SearchIcon size={14} />
                <input
                  id="elv-log-filter"
                  name="log-filter"
                  value={filter}
                  onChange={(e) => setFilter(e.target.value)}
                  placeholder="Filter loaded log…"
                  aria-label="Filter loaded log content"
                  style={{ flex: 1, minWidth: 0, border: 'none', outline: 'none', background: 'transparent', fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--app-text)' }}
                />
                {filtered && (
                  <button onClick={() => setFilter('')} style={{ flex: 'none', border: 'none', background: 'transparent', cursor: 'pointer', color: 'var(--app-faint)', display: 'inline-flex', padding: '2px' }} aria-label="Clear filter">
                    <CloseIcon />
                  </button>
                )}
              </div>
              <label style={{ display: 'inline-flex', alignItems: 'center', gap: '5px', fontSize: '12px', color: 'var(--app-muted)', cursor: 'pointer' }}>
                <input type="checkbox" checked={wrap} onChange={(e) => setWrap(e.target.checked)} />
                wrap
              </label>
              {showLoadFull && (
                <button
                  onClick={() => loadFile('full')}
                  style={{
                    flex: 'none',
                    height: '32px',
                    padding: '0 13px',
                    border: `1px solid ${ACCENT}`,
                    borderRadius: '5px',
                    background: 'transparent',
                    color: ACCENT,
                    fontFamily: 'var(--font-sans)',
                    fontWeight: 700,
                    fontSize: '12.5px',
                    cursor: 'pointer',
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '6px',
                  }}
                >
                  <DownloadIcon /> Load full log
                </button>
              )}
            </div>
          </div>

          {/* banners */}
          {tailTruncated && !filtered && (
            <Banner>
              <InfoIcon />
              Showing the last {formatBytes(load.text.length)} (tail).{' '}
              <b style={{ fontWeight: 500, fontFamily: 'var(--font-mono)', color: 'var(--app-text)' }}>{formatBytes(load.earlierHidden)}</b>
              &nbsp;of earlier content not loaded.
            </Banner>
          )}
          {load.mode === 'full' && (
            <Banner>
              <InfoIcon />
              Full log loaded ({formatBytes(load.totalSize)}).
            </Banner>
          )}
          {filtered && (
            <Banner>
              Filtering the <b style={{ fontFamily: 'var(--font-mono)', fontWeight: 500, color: 'var(--app-text)' }}>loaded</b> content only —
              <span style={{ fontFamily: 'var(--font-mono)', background: '#face00', color: '#1b1b1b', padding: '0 4px', borderRadius: '2px', margin: '0 4px' }}>{filter}</span>
              {tailTruncated ? '· earlier content is not searched until you Load full log.' : ''}
            </Banner>
          )}
          {load.tooLarge && (
            <Banner tone="warn">
              <InfoIcon fill="var(--theme-warning-darker)" />
              This file is too large to load in full ({formatBytes(load.totalSize)}). Showing the tail only.
              <button
                onClick={() => loadFile('tail')}
                style={{ marginLeft: '10px', border: 'none', background: 'transparent', color: ACCENT, fontWeight: 700, cursor: 'pointer', fontFamily: 'var(--font-sans)', fontSize: '12px' }}
              >
                Reload tail
              </button>
            </Banner>
          )}
          {load.error && (
            <Banner tone="warn">
              <InfoIcon fill="var(--theme-warning-darker)" />
              {load.error}
            </Banner>
          )}

          {/* log body */}
          {load.loading ? (
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#0e1116', color: '#7a8697', fontFamily: 'var(--font-mono)', fontSize: '13px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <div style={{ width: '20px', height: '20px', borderRadius: '50%', border: '3px solid #2a2d34', borderTopColor: ACCENT, animation: 'elv-spin .8s linear infinite' }} />
                Loading log…
              </div>
            </div>
          ) : currentFile ? (
            <LogBody text={load.text} startLineNumber={load.startLine} filter={filter} wrap={wrap} />
          ) : (
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#0e1116', color: '#7a8697', fontFamily: 'var(--font-mono)', fontSize: '13px' }}>
              {view === 'executors' && !execId ? 'Select an executor to read its logs.' : 'No log files in this location.'}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function TabButton({ label, active, disabled, count, onClick }: { label: string; active: boolean; disabled: boolean; count: number; onClick: () => void }) {
  return (
    <button
      onClick={disabled ? undefined : onClick}
      disabled={disabled}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px',
        flex: 'none',
        cursor: disabled ? 'default' : 'pointer',
        border: 'none',
        background: 'transparent',
        padding: '11px 14px 9px',
        borderBottom: `3px solid ${active ? ACCENT : 'transparent'}`,
        color: disabled ? 'var(--app-faint)' : active ? 'var(--app-text)' : 'var(--app-muted)',
        fontFamily: 'var(--font-sans)',
        fontWeight: active ? 700 : 500,
        fontSize: '13px',
        opacity: disabled ? 0.55 : 1,
        whiteSpace: 'nowrap',
      }}
    >
      {label}
      {count > 0 && (
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: '11px', color: 'var(--app-faint)', fontWeight: 400 }}>{count}</span>
      )}
    </button>
  );
}

function Meta({ label, value }: { label: string; value: string }) {
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '5px', fontSize: '12px', color: 'var(--app-muted)' }}>
      <span style={{ color: 'var(--app-faint)' }}>{label}</span>
      <b style={{ fontFamily: 'var(--font-mono)', fontWeight: 500, color: 'var(--app-text)' }}>{value}</b>
    </span>
  );
}

function Banner({ children, tone = 'notice' }: { children: React.ReactNode; tone?: 'notice' | 'warn' }) {
  return (
    <div
      style={{
        flex: 'none',
        display: 'flex',
        alignItems: 'center',
        gap: '9px',
        padding: '7px 20px',
        background: tone === 'warn' ? 'var(--app-warn-bg)' : 'var(--app-notice)',
        borderBottom: '1px solid var(--app-border)',
        fontSize: '12px',
        color: 'var(--app-muted)',
      }}
    >
      {children}
    </div>
  );
}
