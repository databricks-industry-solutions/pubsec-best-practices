import { useLayoutEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import type { AppGroupNode, ExecutorNode, LogFileNode } from '../lib/types';
import { formatBytes } from '../lib/format';
import { ACCENT } from '../lib/theme';
import { SearchIcon } from './icons';

const ROW_H = 46;
const HEADER_H = 30;
const OVERSCAN = 8;

// A flattened, virtualizable row: either an app-id group heading (when there is
// more than one app-id) or an executor row.
type Row =
  | { kind: 'header'; appId: string; count: number }
  | { kind: 'exec'; appId: string; executor: ExecutorNode };

function buildRows(groups: AppGroupNode[], filter: string): Row[] {
  const f = filter.trim().toLowerCase();
  const multiApp = groups.length > 1;
  const rows: Row[] = [];
  for (const g of groups) {
    const execs = f
      ? g.executors.filter(
          (e) => e.executor_id.toLowerCase().includes(f) || g.app_id.toLowerCase().includes(f),
        )
      : g.executors;
    if (execs.length === 0) continue;
    if (multiApp) rows.push({ kind: 'header', appId: g.app_id, count: execs.length });
    for (const e of execs) rows.push({ kind: 'exec', appId: g.app_id, executor: e });
  }
  return rows;
}

function totalBytes(files: LogFileNode[]): number | null {
  let sum = 0;
  let any = false;
  for (const fl of files) {
    if (fl.size != null) {
      sum += fl.size;
      any = true;
    }
  }
  return any ? sum : null;
}

export function ExecutorSelector({
  groups,
  selectedAppId,
  selectedExecId,
  onSelect,
}: {
  groups: AppGroupNode[];
  selectedAppId: string | null;
  selectedExecId: string | null;
  onSelect: (appId: string, execId: string) => void;
}) {
  const [filter, setFilter] = useState('');
  const scrollRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportH, setViewportH] = useState(400);

  const rows = useMemo(() => buildRows(groups, filter), [groups, filter]);

  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const measure = () => setViewportH(el.clientHeight);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const rowHeight = (r: Row) => (r.kind === 'header' ? HEADER_H : ROW_H);
  // Precompute cumulative offsets (rows have two heights).
  const offsets = useMemo(() => {
    const arr: number[] = [];
    let acc = 0;
    for (const r of rows) {
      arr.push(acc);
      acc += rowHeight(r);
    }
    return { arr, total: acc };
  }, [rows]);

  const first = (() => {
    let lo = 0;
    let hi = offsets.arr.length;
    const target = scrollTop - OVERSCAN * ROW_H;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (offsets.arr[mid] < target) lo = mid + 1;
      else hi = mid;
    }
    return Math.max(0, lo - 1);
  })();
  const last = (() => {
    const bottom = scrollTop + viewportH + OVERSCAN * ROW_H;
    let i = first;
    while (i < rows.length && offsets.arr[i] < bottom) i += 1;
    return Math.min(rows.length, i + 1);
  })();

  const slice = rows.slice(first, last);

  const execCount = groups.reduce((n, g) => n + g.executors.length, 0);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: 0, height: '100%' }}>
      <div style={{ flex: 'none', padding: '10px 12px', borderBottom: '1px solid var(--app-border)' }}>
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
          }}
        >
          <SearchIcon size={14} />
          <input
            id="elv-exec-filter"
            name="exec-filter"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder={`Filter ${execCount} executors…`}
            aria-label="Filter executors"
            style={{
              flex: 1,
              minWidth: 0,
              border: 'none',
              outline: 'none',
              background: 'transparent',
              fontFamily: 'var(--font-mono)',
              fontSize: '12px',
              color: 'var(--app-text)',
            }}
          />
        </div>
      </div>

      <div
        ref={scrollRef}
        className="elv-scroll"
        onScroll={(e) => setScrollTop((e.target as HTMLDivElement).scrollTop)}
        style={{ flex: 1, overflowY: 'auto', minHeight: 0 }}
      >
        {rows.length === 0 ? (
          <div style={{ padding: '16px', fontSize: '12px', color: 'var(--app-faint)', fontFamily: 'var(--font-mono)' }}>
            No executors match.
          </div>
        ) : (
          <div style={{ height: offsets.total, position: 'relative' }}>
            {slice.map((r, idx) => {
              const rowIndex = first + idx;
              const top = offsets.arr[rowIndex];
              if (r.kind === 'header') {
                return (
                  <div
                    key={`h-${r.appId}`}
                    style={{
                      position: 'absolute',
                      top,
                      left: 0,
                      right: 0,
                      height: HEADER_H,
                      display: 'flex',
                      alignItems: 'center',
                      padding: '0 12px',
                      background: 'var(--app-elev)',
                      borderBottom: '1px solid var(--app-border)',
                      fontFamily: 'var(--font-mono)',
                      fontSize: '10.5px',
                      letterSpacing: '.02em',
                      color: 'var(--app-faint)',
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                    }}
                    title={r.appId}
                  >
                    {r.appId} · {r.count}
                  </div>
                );
              }
              const e = r.executor;
              const selected = r.appId === selectedAppId && e.executor_id === selectedExecId;
              const size = totalBytes(e.files);
              const rowStyle: CSSProperties = {
                position: 'absolute',
                top,
                left: 0,
                right: 0,
                height: ROW_H,
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'center',
                gap: '3px',
                padding: '0 12px',
                cursor: 'pointer',
                border: 'none',
                borderLeft: `3px solid ${selected ? ACCENT : 'transparent'}`,
                borderBottom: '1px solid var(--app-border)',
                background: selected ? `color-mix(in srgb, ${ACCENT} 14%, transparent)` : 'transparent',
                font: 'inherit',
                textAlign: 'left',
                width: '100%',
              };
              return (
                <button key={`${r.appId}/${e.executor_id}`} style={rowStyle} onClick={() => onSelect(r.appId, e.executor_id)} aria-pressed={selected}>
                  <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, fontSize: '12.5px', color: 'var(--app-text)' }}>
                    executor {e.executor_id}
                  </span>
                  <span style={{ fontFamily: 'var(--font-mono)', fontSize: '11px', color: 'var(--app-faint)' }}>
                    {e.files.length} file{e.files.length === 1 ? '' : 's'}
                    {size != null ? ` · ${formatBytes(size)}` : ''}
                  </span>
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
