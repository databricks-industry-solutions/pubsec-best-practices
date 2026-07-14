import { useCallback, useEffect, useState, type CSSProperties } from 'react';
import type { BrowseCluster, ClusterEntry } from '../lib/types';
import { LogFileFetchError } from '../lib/types';
import { browseLogRoot, listClustersWithLogs, listLogRoots } from '../lib/api';
import { relativeAge } from '../lib/format';
import { ACCENT } from '../lib/theme';
import { SearchIcon } from './icons';

interface LeftRailProps {
  selectedId: string | null;
  onSelectRun: (runId: string) => void;
  onLookup: (value: string) => void;
}

// Section label — matches the "Jump to a run" header.
const sectionLabelStyle: CSSProperties = {
  fontFamily: 'var(--font-sans)',
  fontWeight: 700,
  fontSize: '11px',
  letterSpacing: '.03em',
  textTransform: 'uppercase',
  color: 'var(--app-faint)',
};

export function LeftRail({ selectedId, onSelectRun, onLookup }: LeftRailProps) {
  const [lookup, setLookup] = useState('');

  const doLookup = () => {
    const q = lookup.trim();
    if (q) onLookup(q);
  };

  return (
    <aside
      style={{
        width: '346px',
        flex: 'none',
        display: 'flex',
        flexDirection: 'column',
        background: 'var(--app-rail)',
        borderRight: '1px solid var(--app-border)',
        minHeight: 0,
      }}
    >
      {/* lookup box */}
      <div style={{ padding: '14px 16px 12px', borderBottom: '1px solid var(--app-border)' }}>
        <label style={{ ...sectionLabelStyle, display: 'block', marginBottom: '7px' }}>
          Jump to a run
        </label>
        <div style={{ display: 'flex', gap: '7px' }}>
          <div
            style={{
              flex: 1,
              display: 'flex',
              alignItems: 'center',
              gap: '7px',
              background: 'var(--app-input)',
              border: '1px solid var(--app-border-strong)',
              borderRadius: '5px',
              padding: '0 9px',
              height: '36px',
            }}
          >
            <SearchIcon />
            <input
              id="elv-lookup"
              name="lookup"
              value={lookup}
              onChange={(e) => setLookup(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') doLookup();
              }}
              placeholder="run ID, cluster ID, or job ID"
              aria-label="Run, cluster, or job identifier"
              style={{
                flex: 1,
                minWidth: 0,
                border: 'none',
                outline: 'none',
                background: 'transparent',
                fontFamily: 'var(--font-mono)',
                fontSize: '12.5px',
                color: 'var(--app-text)',
              }}
            />
          </div>
          <button
            onClick={doLookup}
            style={{
              flex: 'none',
              height: '36px',
              padding: '0 15px',
              border: 'none',
              borderRadius: '5px',
              background: ACCENT,
              color: '#fff',
              fontFamily: 'var(--font-sans)',
              fontWeight: 700,
              fontSize: '13px',
              cursor: 'pointer',
            }}
          >
            Go
          </button>
        </div>
        <div style={{ marginTop: '6px', fontSize: '11px', color: 'var(--app-faint)' }}>
          Paste an identifier to open its logs directly.
        </div>
      </div>

      {/* PRIMARY recent list: clusters with delivered logs (SP clusters.list) */}
      <RecentClustersSection selectedId={selectedId} onSelectCluster={onSelectRun} />

      {/* SECONDARY manual path: browse a log volume under the user's OBO token */}
      <BrowseSection selectedId={selectedId} onSelectCluster={onSelectRun} />

      <div style={{ flex: 1, minHeight: 0 }} />
    </aside>
  );
}

// --------------------------------------------------------------------------- //
// Recent clusters with logs — the PRIMARY browse source.                      //
//                                                                             //
// Sourced from the app SP's clusters.list (GET /api/clusters): every cluster  //
// whose CLD delivers to an allowlisted Volume — no per-job / per-cluster       //
// grants, low maintenance, includes TERMINATED clusters. Metadata only; the   //
// log CONTENT is still gated per-user by the OBO read on click. Clicking a     //
// cluster reuses the existing resolve path (onSelectCluster -> onSelectRun ->  //
// /api/runs/{cluster_id}/logs, which classifies the dashed id as a cluster).   //
// --------------------------------------------------------------------------- //
type RecentState =
  | { kind: 'loading' }
  | { kind: 'ready'; clusters: ClusterEntry[] }
  | { kind: 'error'; message: string };

function RecentClustersSection({
  selectedId,
  onSelectCluster,
}: {
  selectedId: string | null;
  onSelectCluster: (clusterId: string) => void;
}) {
  const [state, setState] = useState<RecentState>({ kind: 'loading' });

  const load = useCallback(async () => {
    setState({ kind: 'loading' });
    try {
      const res = await listClustersWithLogs(50);
      setState({ kind: 'ready', clusters: res.clusters });
    } catch (err) {
      const message =
        err instanceof LogFileFetchError && err.message
          ? err.message
          : 'Failed to load recent clusters.';
      setState({ kind: 'error', message });
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const count = state.kind === 'ready' ? state.clusters.length : 0;

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        borderBottom: '1px solid var(--app-border)',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '11px 16px 8px',
        }}
      >
        <span style={sectionLabelStyle}>Recent clusters with logs</span>
        <span style={{ fontSize: '11px', color: 'var(--app-faint)' }}>
          {state.kind === 'loading'
            ? 'loading…'
            : `${count} cluster${count === 1 ? '' : 's'}`}
        </span>
      </div>

      <div
        className="elv-scroll"
        style={{
          maxHeight: '46vh',
          overflowY: 'auto',
          minHeight: 0,
          borderTop: '1px solid var(--app-border)',
        }}
      >
        {state.kind === 'error' && (
          <div style={{ padding: '16px', fontSize: '12.5px', color: 'var(--theme-error-dark)', lineHeight: 1.5 }}>
            {state.message}
          </div>
        )}
        {state.kind === 'ready' && state.clusters.length === 0 && (
          <div style={{ padding: '16px', fontSize: '12.5px', color: 'var(--app-faint)', lineHeight: 1.5 }}>
            No recent clusters with delivered logs. Browse a log volume or paste an ID.
          </div>
        )}
        {state.kind === 'ready' &&
          state.clusters.map((c) => (
            <ClusterEntryRow
              key={c.cluster_id}
              cluster={c}
              selected={c.cluster_id === selectedId}
              onSelect={() => onSelectCluster(c.cluster_id)}
            />
          ))}
        <div style={{ height: '8px' }} />
      </div>
    </div>
  );
}

// A small state badge for a cluster's lifecycle state (TERMINATED / RUNNING).
function StateBadge({ state }: { state: string | null }) {
  // No badge when state is unknown (e.g. Volume-sourced clusters carry no
  // cluster state) — rendering "UNKNOWN" is noise, not information.
  if (!state) return null;
  const up = state.toUpperCase();
  const running = up === 'RUNNING' || up === 'PENDING' || up === 'RESIZING' || up === 'RESTARTING';
  const bg = running ? ACCENT : '#565c65';
  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '5px',
        background: bg,
        color: '#fff',
        fontFamily: 'var(--font-sans)',
        fontWeight: 700,
        fontSize: '10px',
        letterSpacing: '.04em',
        textTransform: 'uppercase',
        padding: '2px 7px',
        borderRadius: '3px',
        whiteSpace: 'nowrap',
        flex: 'none',
      }}
    >
      {running && (
        <span
          style={{
            width: '6px',
            height: '6px',
            borderRadius: '50%',
            background: '#fff',
            animation: 'elv-pulse 1.1s ease-in-out infinite',
            flex: 'none',
          }}
        />
      )}
      {up}
    </span>
  );
}

function ClusterEntryRow({
  cluster,
  selected,
  onSelect,
}: {
  cluster: ClusterEntry;
  selected: boolean;
  onSelect: () => void;
}) {
  const [hover, setHover] = useState(false);
  const age = relativeAge(cluster.terminated_at ?? cluster.started_at);
  // Prefer the friendly job name; then any raw cluster name; then, if we parsed
  // a job id but the job was deleted (no friendly name), show "Job <id>"; else
  // fall back to the cluster id.
  const title =
    cluster.job_name ||
    cluster.cluster_name ||
    (cluster.job_id ? `Job ${cluster.job_id}` : cluster.cluster_id);
  const source = cluster.cluster_source;

  return (
    <button
      onClick={onSelect}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      aria-pressed={selected}
      style={{
        display: 'block',
        width: '100%',
        textAlign: 'left',
        cursor: 'pointer',
        border: 'none',
        borderLeft: `3px solid ${selected ? ACCENT : 'transparent'}`,
        borderBottom: '1px solid var(--app-border)',
        background: selected
          ? `color-mix(in srgb, ${ACCENT} 14%, transparent)`
          : hover
            ? 'var(--app-hover)'
            : 'transparent',
        padding: '10px 15px 11px',
        font: 'inherit',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px' }}>
        <span
          style={{
            fontFamily: 'var(--font-sans)',
            fontWeight: 500,
            fontSize: '13px',
            color: 'var(--app-text)',
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
          }}
        >
          {title}
        </span>
        <StateBadge state={cluster.state} />
      </div>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: '8px',
          marginTop: '5px',
        }}
      >
        <span style={{ display: 'flex', alignItems: 'center', gap: '7px', minWidth: 0 }}>
          <span
            style={{
              fontFamily: 'var(--font-mono)',
              fontSize: '11.5px',
              color: 'var(--app-muted)',
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
            }}
          >
            {cluster.cluster_id}
            {cluster.run_id && (
              <span style={{ color: 'var(--app-faint)' }}> · run {cluster.run_id}</span>
            )}
          </span>
          {source && (
            <span
              style={{
                flex: 'none',
                fontSize: '10px',
                fontWeight: 700,
                letterSpacing: '.03em',
                color: 'var(--app-faint)',
                border: '1px solid var(--app-border-strong)',
                borderRadius: '3px',
                padding: '0 4px',
              }}
            >
              {source.toUpperCase()}
            </span>
          )}
        </span>
        {age && <span style={{ flex: 'none', fontSize: '11.5px', color: 'var(--app-faint)' }}>{age}</span>}
      </div>
    </button>
  );
}

// --------------------------------------------------------------------------- //
// Browse a log volume — user-scoped discovery (SECONDARY / manual).           //
//                                                                             //
// Lists the CLD Volume DIRECTLY with the user's OBO token: UC enforces        //
// per-user access on the listing, so what comes back is exactly the log dirs  //
// THIS user can see (survives cluster aging-out). Clicking a cluster reuses    //
// the existing resolve path (onSelectCluster -> onSelectRun ->                 //
// /api/runs/{cluster_id}/logs), which classifies the dashed id as a cluster.   //
// --------------------------------------------------------------------------- //
type BrowseState =
  | { kind: 'idle' }
  | { kind: 'loading' }
  | { kind: 'ready'; path: string; clusters: BrowseCluster[] }
  | { kind: 'error'; message: string };

function BrowseSection({
  selectedId,
  onSelectCluster,
}: {
  selectedId: string | null;
  onSelectCluster: (clusterId: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [path, setPath] = useState('');
  const [roots, setRoots] = useState<string[] | null>(null);
  const [state, setState] = useState<BrowseState>({ kind: 'idle' });

  // Lazy-load the configured roots the first time the section is opened.
  const ensureRoots = useCallback(async () => {
    if (roots !== null) return;
    try {
      const r = await listLogRoots();
      setRoots(r.roots);
    } catch {
      setRoots([]); // roots are a convenience; failure is non-fatal
    }
  }, [roots]);

  const toggleOpen = () => {
    setOpen((o) => {
      const next = !o;
      if (next) void ensureRoots();
      return next;
    });
  };

  const runBrowse = useCallback(async (p: string) => {
    const trimmed = p.trim();
    if (!trimmed) return;
    setState({ kind: 'loading' });
    try {
      const res = await browseLogRoot(trimmed);
      setState({ kind: 'ready', path: res.path, clusters: res.clusters });
    } catch (err) {
      let message = 'Could not browse this path.';
      if (err instanceof LogFileFetchError) {
        if (err.reasonCode === 'FILES_FORBIDDEN' || err.status === 403) {
          message = "You don't have READ on this Volume path.";
        } else if (err.reasonCode === 'CLD_ROOT_NOT_FOUND' || err.status === 404) {
          message = 'Path not found.';
        } else if (err.reasonCode === 'NOT_VOLUME' || err.status === 400) {
          message = 'Enter a /Volumes/… path.';
        } else if (err.status === 401) {
          message = 'Authentication required — reload the app.';
        } else {
          message = err.message || message;
        }
      }
      setState({ kind: 'error', message });
    }
  }, []);

  const useRoot = (r: string) => {
    setPath(r);
    void runBrowse(r);
  };

  return (
    <div style={{ borderBottom: '1px solid var(--app-border)' }}>
      <button
        onClick={toggleOpen}
        aria-expanded={open}
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          width: '100%',
          padding: '11px 16px',
          border: 'none',
          background: 'transparent',
          cursor: 'pointer',
        }}
      >
        <span style={sectionLabelStyle}>Browse a log volume</span>
        <span style={{ fontSize: '11px', color: 'var(--app-faint)' }} aria-hidden>
          {open ? '▾' : '▸'}
        </span>
      </button>

      {open && (
        <div style={{ padding: '0 16px 13px' }}>
          {/* path input + Browse button */}
          <div style={{ display: 'flex', gap: '7px' }}>
            <input
              value={path}
              onChange={(e) => setPath(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') void runBrowse(path);
              }}
              placeholder="/Volumes/catalog/schema/volume/…"
              aria-label="Volume path to browse"
              style={{
                flex: 1,
                minWidth: 0,
                height: '34px',
                padding: '0 9px',
                background: 'var(--app-input)',
                border: '1px solid var(--app-border-strong)',
                borderRadius: '5px',
                outline: 'none',
                fontFamily: 'var(--font-mono)',
                fontSize: '11.5px',
                color: 'var(--app-text)',
              }}
            />
            <button
              onClick={() => void runBrowse(path)}
              style={{
                flex: 'none',
                height: '34px',
                padding: '0 13px',
                border: 'none',
                borderRadius: '5px',
                background: ACCENT,
                color: '#fff',
                fontFamily: 'var(--font-sans)',
                fontWeight: 700,
                fontSize: '12.5px',
                cursor: 'pointer',
              }}
            >
              Browse
            </button>
          </div>

          {/* root shortcut chips */}
          {roots && roots.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px', marginTop: '8px' }}>
              {roots.map((r) => (
                <button
                  key={r}
                  onClick={() => useRoot(r)}
                  title={r}
                  style={{
                    maxWidth: '100%',
                    padding: '3px 9px',
                    border: '1px solid var(--app-border-strong)',
                    borderRadius: '999px',
                    background: 'var(--app-tab)',
                    color: 'var(--app-muted)',
                    fontFamily: 'var(--font-mono)',
                    fontSize: '11px',
                    cursor: 'pointer',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                  }}
                >
                  {rootLabel(r)}
                </button>
              ))}
            </div>
          )}

          {/* results / states */}
          <div style={{ marginTop: '10px' }}>
            {state.kind === 'loading' && (
              <div style={{ fontSize: '11.5px', color: 'var(--app-faint)' }}>Browsing…</div>
            )}
            {state.kind === 'error' && (
              <div style={{ fontSize: '11.5px', color: 'var(--theme-error-dark)', lineHeight: 1.5 }}>
                {state.message}
              </div>
            )}
            {state.kind === 'ready' && state.clusters.length === 0 && (
              <div style={{ fontSize: '11.5px', color: 'var(--app-faint)', lineHeight: 1.5 }}>
                No log directories found under this path.
              </div>
            )}
            {state.kind === 'ready' && state.clusters.length > 0 && (
              <div
                style={{
                  border: '1px solid var(--app-border)',
                  borderRadius: '6px',
                  overflow: 'hidden',
                }}
              >
                {state.clusters.map((c) => (
                  <ClusterRow
                    key={c.path}
                    cluster={c}
                    selected={c.cluster_id === selectedId}
                    onSelect={() => onSelectCluster(c.cluster_id)}
                  />
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// Show the last two path segments of a root so a chip stays legible.
function rootLabel(root: string): string {
  const parts = root.split('/').filter(Boolean);
  if (parts.length <= 2) return root;
  return '…/' + parts.slice(-2).join('/');
}

function ClusterRow({
  cluster,
  selected,
  onSelect,
}: {
  cluster: BrowseCluster;
  selected: boolean;
  onSelect: () => void;
}) {
  const [hover, setHover] = useState(false);
  const age = relativeAge(cluster.modified);
  // executor/driver presence hints, only when known (peeked).
  const hint: string | null =
    cluster.has_executor === true
      ? 'executor ✓'
      : cluster.has_driver === true
        ? 'driver'
        : null;

  return (
    <button
      onClick={onSelect}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      aria-pressed={selected}
      style={{
        display: 'block',
        width: '100%',
        textAlign: 'left',
        cursor: 'pointer',
        border: 'none',
        borderLeft: `3px solid ${selected ? ACCENT : 'transparent'}`,
        borderBottom: '1px solid var(--app-border)',
        background: selected
          ? `color-mix(in srgb, ${ACCENT} 14%, transparent)`
          : hover
            ? 'var(--app-hover)'
            : 'transparent',
        padding: '8px 11px',
        font: 'inherit',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px' }}>
        <span
          style={{
            fontFamily: 'var(--font-mono)',
            fontSize: '12px',
            color: 'var(--app-text)',
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
          }}
        >
          {cluster.cluster_id}
        </span>
        {age && (
          <span style={{ flex: 'none', fontSize: '11px', color: 'var(--app-faint)' }}>{age}</span>
        )}
      </div>
      {hint && (
        <div style={{ marginTop: '3px', fontSize: '10.5px', color: 'var(--app-faint)' }}>{hint}</div>
      )}
    </button>
  );
}
