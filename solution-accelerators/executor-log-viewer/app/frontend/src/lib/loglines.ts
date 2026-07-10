// Log-line parsing + level classification, matching the wireframe's LEVELS.
// ERROR -> red, WARN -> amber; continuation/stack-trace lines (a leading level
// of '') inherit the error tint.

export type LogLevel = 'ERROR' | 'WARN' | 'INFO' | 'CONT';

export interface LineStyle {
  color: string;
  bg: string;
  bar: string;
}

// Colors from the wireframe LEVELS map (dark log body #0e1116).
export const LEVEL_STYLES: Record<LogLevel, LineStyle> = {
  ERROR: { color: '#ff8d7b', bg: 'rgba(229,34,7,0.15)', bar: '#e52207' },
  WARN: { color: '#ffbe2e', bg: 'rgba(255,190,46,0.08)', bar: '#c2850c' },
  INFO: { color: '#c3ccd6', bg: 'transparent', bar: 'transparent' },
  // continuation (stack-trace) lines: subtle red tint like the wireframe ''
  CONT: { color: '#e0a99f', bg: 'rgba(229,34,7,0.08)', bar: '#e52207' },
};

export const LOG_BODY_BG = '#0e1116';
export const LINE_NUM_COLOR = '#556072';

export interface ParsedLine {
  n: number; // 1-based line number
  text: string;
  level: LogLevel;
}

// Spark log lines look like: "26/07/08 19:42:09 INFO Executor: ...".
// A line with no leading level (indented stack-trace / "at ..." / exception
// class) is treated as a continuation and keeps the error tint.
const LEVEL_RE = /\b(ERROR|WARN|WARNING|INFO|DEBUG|TRACE)\b/;
const CONT_RE = /^\s*(at\s|Caused by:|\.{3}\s|[a-z0-9_.]+\.[A-Za-z]\w*(Exception|Error)\b)/;

export function classifyLevel(text: string): LogLevel {
  const m = LEVEL_RE.exec(text.slice(0, 64));
  if (m) {
    const lv = m[1];
    if (lv === 'ERROR') return 'ERROR';
    if (lv === 'WARN' || lv === 'WARNING') return 'WARN';
    return 'INFO';
  }
  if (CONT_RE.test(text)) return 'CONT';
  return 'INFO';
}

// Split raw text into numbered, classified lines. A trailing newline does not
// produce a spurious empty final line.
export function parseLines(text: string, startLineNumber = 1): ParsedLine[] {
  if (text === '') return [];
  const rawLines = text.split('\n');
  if (rawLines.length && rawLines[rawLines.length - 1] === '') rawLines.pop();
  return rawLines.map((t, i) => ({
    n: startLineNumber + i,
    text: t,
    level: classifyLevel(t),
  }));
}

export interface HighlightSegment {
  text: string;
  match: boolean;
}

// Case-insensitive substring highlight for the client-side filter box.
export function highlightSegments(text: string, filter: string): HighlightSegment[] {
  if (!filter) return [{ text, match: false }];
  const low = text.toLowerCase();
  const lf = filter.toLowerCase();
  const out: HighlightSegment[] = [];
  let pos = 0;
  let i = low.indexOf(lf, pos);
  if (i === -1) return [{ text, match: false }];
  while (i !== -1) {
    if (i > pos) out.push({ text: text.slice(pos, i), match: false });
    out.push({ text: text.slice(i, i + filter.length), match: true });
    pos = i + filter.length;
    i = low.indexOf(lf, pos);
  }
  if (pos < text.length) out.push({ text: text.slice(pos), match: false });
  return out;
}
