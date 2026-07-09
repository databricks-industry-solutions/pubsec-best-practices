import { useCallback, useEffect, useRef, useState } from 'react';
import { resolveLogs } from './lib/api';
import type { ResolveResult } from './lib/types';
import { LogFileFetchError } from './lib/types';
import { ACCENT, applyTheme, type ThemeName } from './lib/theme';
import { parseHash, toHash, type Selection } from './lib/urlstate';
import { stateForReason } from './lib/uistate';
import { LogIcon, SidebarIcon } from './components/icons';
import { LeftRail } from './components/LeftRail';
import { MainViewer } from './components/MainViewer';
import { TaskSelector } from './components/TaskSelector';
import {
  AmbiguousScreen,
  DeliveryPendingScreen,
  EmptyScreen,
  LoadingScreen,
  NoCldScreen,
  NoClusterScreen,
  NoExecutorScreen,
  NotFoundScreen,
  PermissionScreen,
  RunningScreen,
} from './components/StateScreens';

export function App() {
  // ---- theme ------------------------------------------------------------- //
  const initialHash = parseHash(window.location.hash);
  const prefersDark = window.matchMedia?.('(prefers-color-scheme: dark)').matches ?? false;
  const [theme, setTheme] = useState<ThemeName>(initialHash.theme ?? (prefersDark ? 'dark' : 'light'));
  useEffect(() => applyTheme(theme), [theme]);

  // ---- selection (URL-backed) ------------------------------------------- //
  const [sel, setSel] = useState<Selection>(initialHash);

  // ---- left rail collapse (UI pref, localStorage — not in shareable URL) - //
  const [railCollapsed, setRailCollapsed] = useState<boolean>(
    () => localStorage.getItem('elv.railCollapsed') === '1',
  );
  const toggleRail = useCallback(() => {
    setRailCollapsed((c) => {
      const next = !c;
      localStorage.setItem('elv.railCollapsed', next ? '1' : '0');
      return next;
    });
  }, []);

  // Recent-clusters + volume-browse lists live inside LeftRail (each fetches
  // its own data). The /api/runs endpoint is retained on the backend but no
  // longer drives the rail's default recent list.

  // ---- resolution of the selected id ------------------------------------ //
  const [resolving, setResolving] = useState(false);
  const [result, setResult] = useState<ResolveResult | null>(null);
  const [lookupQuery, setLookupQuery] = useState<string>('');
  const resolveSeq = useRef(0);

  const doResolve = useCallback(async (id: string, taskRunId: number | null) => {
    const seq = ++resolveSeq.current;
    setResolving(true);
    setLookupQuery(id);
    try {
      const r = await resolveLogs(id, taskRunId);
      if (seq === resolveSeq.current) setResult(r);
    } catch (err) {
      if (seq !== resolveSeq.current) return;
      const msg = err instanceof LogFileFetchError ? err.message : 'failed to resolve run';
      setResult({ outcome: 'ERROR', reason_code: 'RUN_NOT_FOUND', run_meta: null, tasks: null, tree: null, detail: msg });
    } finally {
      if (seq === resolveSeq.current) setResolving(false);
    }
  }, []);

  // When id or task changes, (re)resolve.
  useEffect(() => {
    if (!sel.id) {
      setResult(null);
      return;
    }
    doResolve(sel.id, sel.task ? Number(sel.task) : null);
  }, [sel.id, sel.task, doResolve]);

  // ---- URL hash sync ----------------------------------------------------- //
  // Write selection+theme to the hash whenever they change.
  const suppressHashEffect = useRef(false);
  useEffect(() => {
    const next = toHash({ ...sel, theme });
    const current = window.location.hash;
    if (next !== current) {
      suppressHashEffect.current = true;
      window.history.replaceState(null, '', next || `${window.location.pathname}${window.location.search}`);
    }
  }, [sel, theme]);

  // Respond to back/forward navigation.
  useEffect(() => {
    const onHash = () => {
      if (suppressHashEffect.current) {
        suppressHashEffect.current = false;
        return;
      }
      const parsed = parseHash(window.location.hash);
      setSel(parsed);
      if (parsed.theme) setTheme(parsed.theme);
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  // ---- handlers ---------------------------------------------------------- //
  const selectRun = (runId: string) => {
    setSel({ id: runId, task: null, group: null, exec: null, file: null, view: null, theme });
  };
  const lookup = (value: string) => {
    setSel({ id: value, task: null, group: null, exec: null, file: null, view: null, theme });
  };
  const browse = () => {
    setSel({ id: null, task: null, group: null, exec: null, file: null, view: null, theme });
  };
  const pickTask = (taskRunId: number) => {
    setSel((s) => ({ ...s, task: String(taskRunId), group: null, exec: null, file: null, view: null }));
  };
  const refresh = () => {
    if (sel.id) doResolve(sel.id, sel.task ? Number(sel.task) : null);
  };

  // ---- render the right pane -------------------------------------------- //
  const renderMain = () => {
    if (resolving) return <LoadingScreen />;
    if (!sel.id || !result) return <EmptyScreen />;

    const uiState = stateForReason(result.reason_code);
    switch (uiState) {
      case 'main':
        if (!result.tree) return <EmptyScreen />;
        return (
          <MainViewer
            key={`${sel.id}:${sel.task ?? ''}`}
            meta={result.run_meta}
            tree={result.tree}
            initialView={sel.view}
            initialGroup={sel.group}
            initialExec={sel.exec}
            initialFile={sel.file}
            onSelectionChange={(next) =>
              setSel((s) => ({ ...s, view: next.view, group: next.group, exec: next.exec, file: next.file }))
            }
          />
        );
      case 'task-select':
        return (
          <TaskSelector meta={result.run_meta} tasks={result.tasks ?? []} onPick={pickTask} onBrowse={browse} />
        );
      case 'no-cld':
        return <NoCldScreen meta={result.run_meta} detail={result.detail} onBrowse={browse} />;
      case 'no-cluster':
        return <NoClusterScreen meta={result.run_meta} detail={result.detail} onBrowse={browse} />;
      case 'no-executor-dir': {
        const hasDriver = (result.tree?.driver_files.length ?? 0) > 0;
        return (
          <NoExecutorScreen
            meta={result.run_meta}
            detail={result.detail}
            hasDriver={hasDriver}
            onBrowse={browse}
            onViewDriver={() => {
              // If the backend delivered driver files in the tree, switch to the
              // main viewer's Driver tab by promoting the tree to 'main' locally.
              if (result.tree) {
                setResult({ ...result, reason_code: 'OK', outcome: 'OK' });
                setSel((s) => ({ ...s, view: 'driver' }));
              }
            }}
          />
        );
      }
      case 'delivery-pending':
        return <DeliveryPendingScreen meta={result.run_meta} onRefresh={refresh} />;
      case 'running':
        return <RunningScreen meta={result.run_meta} />;
      case 'permission':
        return (
          <PermissionScreen
            detail={result.detail}
            isScope={result.reason_code === 'FILES_SCOPE_MISSING'}
            onBrowse={browse}
          />
        );
      case 'ambiguous':
        return <AmbiguousScreen query={lookupQuery} onBrowse={browse} />;
      case 'not-found':
      default:
        return <NotFoundScreen query={lookupQuery} detail={result.detail} onBrowse={browse} />;
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', width: '100%', background: 'var(--app-bg)', color: 'var(--app-text)', fontFamily: 'var(--font-sans)', overflow: 'hidden' }}>
      {/* header */}
      <header
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: '16px',
          padding: '0 18px',
          height: '56px',
          flex: 'none',
          background: 'var(--app-rail)',
          borderBottom: '1px solid var(--app-border)',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '11px', minWidth: 0 }}>
          <button
            onClick={toggleRail}
            title={railCollapsed ? 'Show runs sidebar' : 'Hide runs sidebar'}
            aria-label={railCollapsed ? 'Show runs sidebar' : 'Hide runs sidebar'}
            aria-pressed={!railCollapsed}
            style={{
              flex: 'none', width: '30px', height: '30px', borderRadius: '6px',
              border: '1px solid var(--app-border)', background: 'var(--app-tab)',
              color: 'var(--app-muted)', cursor: 'pointer', display: 'flex',
              alignItems: 'center', justifyContent: 'center', padding: 0,
            }}
          >
            <SidebarIcon collapsed={railCollapsed} />
          </button>
          <div style={{ width: '30px', height: '30px', borderRadius: '6px', background: ACCENT, display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 'none' }}>
            <LogIcon />
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', lineHeight: 1.15, minWidth: 0 }}>
            <div style={{ fontFamily: 'var(--font-serif)', fontWeight: 700, fontSize: '16px', color: 'var(--app-text)', whiteSpace: 'nowrap' }}>Executor Log Viewer</div>
            <div style={{ fontSize: '11px', color: 'var(--app-faint)', whiteSpace: 'nowrap' }}>Databricks App · Spark cluster logs</div>
          </div>
        </div>
        <div style={{ display: 'flex', background: 'var(--app-tab)', border: '1px solid var(--app-border)', borderRadius: '6px', padding: '2px', gap: '2px' }}>
          <ThemeBtn label="Light" active={theme === 'light'} onClick={() => setTheme('light')} />
          <ThemeBtn label="Dark" active={theme === 'dark'} onClick={() => setTheme('dark')} />
        </div>
      </header>

      <div style={{ flex: 1, display: 'flex', minHeight: 0 }}>
        {!railCollapsed && (
          <LeftRail selectedId={sel.id} onSelectRun={selectRun} onLookup={lookup} />
        )}
        <main style={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, background: 'var(--app-bg)' }}>
          {renderMain()}
        </main>
      </div>
    </div>
  );
}

function ThemeBtn({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      style={{
        border: 'none',
        cursor: 'pointer',
        borderRadius: '4px',
        padding: '5px 12px',
        fontFamily: 'var(--font-sans)',
        fontWeight: 700,
        fontSize: '12px',
        background: active ? ACCENT : 'transparent',
        color: active ? '#fff' : 'var(--app-muted)',
      }}
    >
      {label}
    </button>
  );
}
