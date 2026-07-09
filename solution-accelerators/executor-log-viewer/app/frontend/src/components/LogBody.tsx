import { useLayoutEffect, useMemo, useRef, useState } from 'react';
import {
  LEVEL_STYLES,
  LINE_NUM_COLOR,
  LOG_BODY_BG,
  highlightSegments,
  parseLines,
  type ParsedLine,
} from '../lib/loglines';

const LINE_HEIGHT = 20; // px per line (12.5px mono * 1.55 rounded, fixed for virtualization)
const OVERSCAN = 12;

interface LogBodyProps {
  text: string;
  // Starting line number for the first line of `text` (tail reads start > 1).
  startLineNumber: number;
  filter: string;
  wrap: boolean;
  // A short streaming/notice row rendered under the last line, if any.
  footer?: React.ReactNode;
}

// Virtualized, line-numbered log renderer. Only the visible window of rows is
// mounted, so files with tens of thousands of lines stay smooth.
export function LogBody({ text, startLineNumber, filter, wrap, footer }: LogBodyProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportH, setViewportH] = useState(600);

  const allLines = useMemo(() => parseLines(text, startLineNumber), [text, startLineNumber]);

  const f = filter.trim();
  const lines: ParsedLine[] = useMemo(() => {
    if (!f) return allLines;
    const lf = f.toLowerCase();
    return allLines.filter((l) => l.text.toLowerCase().includes(lf));
  }, [allLines, f]);

  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const measure = () => setViewportH(el.clientHeight);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Reset scroll to top when the underlying content or filter changes.
  useLayoutEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = 0;
      setScrollTop(0);
    }
  }, [text, startLineNumber, f]);

  const total = lines.length;
  const totalHeight = total * LINE_HEIGHT;
  const first = Math.max(0, Math.floor(scrollTop / LINE_HEIGHT) - OVERSCAN);
  const visibleCount = Math.ceil(viewportH / LINE_HEIGHT) + OVERSCAN * 2;
  const last = Math.min(total, first + visibleCount);
  const slice = lines.slice(first, last);

  const noMatches = !!f && total === 0;

  return (
    <div
      ref={scrollRef}
      className="elv-scroll"
      onScroll={(e) => setScrollTop((e.target as HTMLDivElement).scrollTop)}
      style={{ flex: 1, overflow: 'auto', minHeight: 0, background: LOG_BODY_BG, padding: '8px 0 40px' }}
      role="log"
      aria-label="Log contents"
    >
      {noMatches ? (
        <div style={{ padding: '26px 22px', fontFamily: 'var(--font-mono)', fontSize: '12.5px', color: '#7a8697' }}>
          No lines in the loaded content match this filter. Try “Load full log” to search the entire file.
        </div>
      ) : (
        <div style={{ height: totalHeight, position: 'relative' }}>
          <div style={{ position: 'absolute', top: first * LINE_HEIGHT, left: 0, right: 0 }}>
            {slice.map((line) => {
              const st = LEVEL_STYLES[line.level];
              const segs = highlightSegments(line.text, f);
              return (
                <div
                  key={line.n}
                  style={{
                    display: 'flex',
                    alignItems: 'flex-start',
                    background: st.bg,
                    borderLeft: `3px solid ${st.bar}`,
                    height: wrap ? undefined : LINE_HEIGHT,
                    minHeight: LINE_HEIGHT,
                  }}
                >
                  <span
                    style={{
                      flex: 'none',
                      width: '60px',
                      padding: '1px 12px 1px 0',
                      textAlign: 'right',
                      fontFamily: 'var(--font-mono)',
                      fontSize: '12px',
                      lineHeight: `${LINE_HEIGHT - 2}px`,
                      color: LINE_NUM_COLOR,
                      userSelect: 'none',
                    }}
                  >
                    {line.n}
                  </span>
                  <span
                    style={{
                      flex: 1,
                      padding: '1px 16px 1px 10px',
                      fontFamily: 'var(--font-mono)',
                      fontSize: '12.5px',
                      lineHeight: `${LINE_HEIGHT - 2}px`,
                      color: st.color,
                      whiteSpace: wrap ? 'pre-wrap' : 'pre',
                      wordBreak: wrap ? 'break-word' : 'normal',
                    }}
                  >
                    {segs.map((s, i) =>
                      s.match ? (
                        <mark key={i} style={{ background: '#face00', color: '#1b1b1b', borderRadius: '2px', padding: '0 1px' }}>
                          {s.text}
                        </mark>
                      ) : (
                        <span key={i}>{s.text}</span>
                      ),
                    )}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}
      {footer}
    </div>
  );
}
