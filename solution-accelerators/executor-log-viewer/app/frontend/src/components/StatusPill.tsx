import type { CSSProperties } from 'react';
import { ACCENT } from '../lib/theme';

// Derives a display status + color from the backend life-cycle/result states.
// life-cycle TERMINATED + result SUCCESS/FAILED -> SUCCESS/FAILED; otherwise the
// life-cycle state (RUNNING, PENDING, etc.).
export function displayStatus(
  state: string | null | undefined,
  resultState: string | null | undefined,
): string {
  if (state === 'TERMINATED' && resultState) return resultState;
  return state || 'UNKNOWN';
}

const PILL_BG: Record<string, string> = {
  SUCCESS: '#216e1f',
  FAILED: '#b21d38',
  TIMEDOUT: '#b21d38',
  CANCELED: '#565c65',
  CANCELLED: '#565c65',
  RUNNING: ACCENT,
  PENDING: ACCENT,
  QUEUED: ACCENT,
  TERMINATING: ACCENT,
};

export function StatusPill({ status }: { status: string }) {
  const up = status.toUpperCase();
  const bg = PILL_BG[up] || '#565c65';
  const animated = up === 'RUNNING' || up === 'PENDING' || up === 'QUEUED';
  const pillStyle: CSSProperties = {
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
  };
  return (
    <span style={pillStyle}>
      {animated && (
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
      {status}
    </span>
  );
}
