import type { ReactNode } from 'react';
import { ACCENT } from '../lib/theme';
import type { RunMeta } from '../lib/types';
import {
  ErrorIcon,
  ExternalIcon,
  InfoIcon,
  ListIcon,
  RefreshIcon,
  SearchIcon,
  WarnIcon,
} from './icons';
import { StatusPill, displayStatus } from './StatusPill';

// A minimal USWDS-style alert box (the wireframe used the USWDS Alert
// component; we reproduce its look inline to stay self-contained).
function Alert({
  type,
  heading,
  children,
}: {
  type: 'warning' | 'info' | 'error';
  heading: string;
  children: ReactNode;
}) {
  const bg =
    type === 'warning' ? 'var(--app-warn-bg)' : type === 'info' ? 'var(--app-info-bg)' : 'var(--app-err-bg)';
  const bar =
    type === 'warning'
      ? 'var(--theme-warning-darker)'
      : type === 'info'
        ? 'var(--theme-info-darker)'
        : 'var(--theme-error-dark)';
  return (
    <div style={{ background: bg, borderLeft: `6px solid ${bar}`, padding: '14px 18px', borderRadius: '2px', textAlign: 'left' }}>
      <div style={{ fontFamily: 'var(--font-serif)', fontWeight: 700, fontSize: '15px', color: 'var(--app-text)', marginBottom: '6px' }}>
        {heading}
      </div>
      <div style={{ fontSize: '13.5px', lineHeight: 1.6, color: 'var(--app-muted)' }}>{children}</div>
    </div>
  );
}

function Header({ meta }: { meta: RunMeta | null }) {
  if (!meta) return null;
  // Same adaptive logic as MainViewer: only show job/run/status when we have
  // real run context; otherwise lead with the cluster (reached by cluster id /
  // browse) — never render empty "job — / run — / UNKNOWN".
  const hasRunContext = !!(meta.run_name || meta.run_id != null || meta.job_id != null);
  const status =
    hasRunContext && (meta.state || meta.result_state)
      ? displayStatus(meta.state, meta.result_state)
      : '';
  return (
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
      {hasRunContext ? (
        <>
          <span style={{ fontFamily: 'var(--font-serif)', fontWeight: 700, fontSize: '17px', color: 'var(--app-text)' }}>
            {meta.run_name || `job ${meta.job_id}`}
          </span>
          {status && <StatusPill status={status} />}
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--app-muted)' }}>
            {meta.run_id != null ? `run ${meta.run_id}` : ''}
            {meta.cluster_id ? `${meta.run_id != null ? ' · ' : ''}cluster ${meta.cluster_id}` : ''}
          </span>
        </>
      ) : (
        <>
          <span style={{ fontFamily: 'var(--font-serif)', fontWeight: 700, fontSize: '17px', color: 'var(--app-text)' }}>Cluster logs</span>
          {meta.cluster_id && (
            <span style={{ fontFamily: 'var(--font-mono)', fontSize: '13px', color: 'var(--app-text)' }}>{meta.cluster_id}</span>
          )}
        </>
      )}
    </div>
  );
}

const centerWrap: React.CSSProperties = {
  flex: 1,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  padding: '40px',
};

function IconBadge({ bg, children }: { bg: string; children: ReactNode }) {
  return (
    <div
      style={{
        width: '64px',
        height: '64px',
        margin: '0 auto 18px',
        borderRadius: '14px',
        background: bg,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      {children}
    </div>
  );
}

const primaryBtn: React.CSSProperties = {
  height: '40px',
  padding: '0 18px',
  border: 'none',
  borderRadius: '5px',
  background: ACCENT,
  color: '#fff',
  fontFamily: 'var(--font-sans)',
  fontWeight: 700,
  fontSize: '14px',
  cursor: 'pointer',
  display: 'inline-flex',
  alignItems: 'center',
  gap: '7px',
  textDecoration: 'none',
};

const secondaryBtn: React.CSSProperties = {
  height: '40px',
  padding: '0 18px',
  border: '1px solid var(--app-border-strong)',
  borderRadius: '5px',
  background: 'var(--app-panel)',
  color: 'var(--app-text)',
  fontFamily: 'var(--font-sans)',
  fontWeight: 700,
  fontSize: '14px',
  cursor: 'pointer',
};

// ---- No run selected ---------------------------------------------------- //
export function EmptyScreen() {
  return (
    <div style={{ ...centerWrap }}>
      <div style={{ maxWidth: '440px', textAlign: 'center' }}>
        <div
          style={{
            width: '72px',
            height: '72px',
            margin: '0 auto 20px',
            borderRadius: '14px',
            background: 'var(--app-elev)',
            border: '1px solid var(--app-border)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <ListIcon />
        </div>
        <h2 style={{ margin: '0 0 8px', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
          Select a run to read its logs
        </h2>
        <p style={{ margin: '0 0 18px', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
          Pick a job run from the list on the left, or paste a run ID, cluster ID, or job ID into the lookup box to jump
          straight to its executor logs.
        </p>
        <div
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '7px',
            fontSize: '12.5px',
            color: 'var(--app-faint)',
            background: 'var(--app-elev)',
            border: '1px solid var(--app-border)',
            borderRadius: '6px',
            padding: '8px 12px',
            fontFamily: 'var(--font-mono)',
          }}
        >
          <SearchIcon size={14} fill="currentColor" />
          e.g. 5581123 · 0708-194133-abcd1234
        </div>
      </div>
    </div>
  );
}

// ---- Loading ------------------------------------------------------------ //
export function LoadingScreen() {
  return (
    <div
      style={{
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '16px',
        color: 'var(--app-muted)',
      }}
    >
      <div
        style={{
          width: '34px',
          height: '34px',
          borderRadius: '50%',
          border: '3px solid var(--app-border)',
          borderTopColor: ACCENT,
          animation: 'elv-spin .8s linear infinite',
        }}
      />
      <div style={{ fontSize: '14px' }}>Fetching log directory from the Volume…</div>
      <div style={{ fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--app-faint)' }}>
        Listing executor folders for the selected run
      </div>
    </div>
  );
}

// ---- No cluster log delivery -------------------------------------------- //
export function NoCldScreen({ meta, detail, onBrowse }: { meta: RunMeta | null; detail?: string | null; onBrowse: () => void }) {
  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
      <Header meta={meta} />
      <div style={centerWrap}>
        <div style={{ maxWidth: '560px', width: '100%' }}>
          <IconBadge bg="var(--app-warn-bg)">
            <WarnIcon />
          </IconBadge>
          <h2 style={{ margin: '0 0 8px', textAlign: 'center', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
            No cluster log delivery configured
          </h2>
          <p style={{ margin: '0 auto 18px', maxWidth: '460px', textAlign: 'center', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
            {detail ||
              "This run's cluster wasn't set up to deliver logs to a Volume, so there's nothing to read here. Executor logs are only retained when log delivery is enabled before the cluster starts."}
          </p>
          <Alert type="warning" heading="How to fix this for future runs">
            Set a cluster log destination under <b>Compute → Logging</b> (or the{' '}
            <code style={{ fontFamily: 'var(--font-mono)' }}>cluster_log_conf</code> field in the job's cluster spec) to a
            Unity Catalog Volume path. New runs will then deliver driver and executor logs there.
          </Alert>
          <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginTop: '18px' }}>
            <button onClick={onBrowse} style={secondaryBtn}>
              Back to recent runs
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---- Delivered, no executor folder (all-purpose) ------------------------ //
export function NoExecutorScreen({
  meta,
  detail,
  hasDriver,
  onBrowse,
  onViewDriver,
}: {
  meta: RunMeta | null;
  detail?: string | null;
  hasDriver: boolean;
  onBrowse: () => void;
  onViewDriver: () => void;
}) {
  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
      <Header meta={meta} />
      <div style={centerWrap}>
        <div style={{ maxWidth: '580px', width: '100%' }}>
          <IconBadge bg="var(--app-info-bg)">
            <InfoIcon size={32} fill="var(--theme-info-darker)" />
          </IconBadge>
          <h2 style={{ margin: '0 0 8px', textAlign: 'center', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
            Logs delivered, but no executor folder
          </h2>
          <p style={{ margin: '0 auto 18px', maxWidth: '500px', textAlign: 'center', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
            {detail || (
              <>
                We found this cluster's log directory in the Volume, but there's no{' '}
                <code style={{ fontFamily: 'var(--font-mono)' }}>executor/</code> subfolder to read.
              </>
            )}
          </p>
          <Alert type="info" heading="Why this happens">
            This run likely used an <b>all-purpose (interactive) cluster</b>. Executor logs are only delivered for{' '}
            <b>job clusters</b> — clusters created for a single job run. Interactive clusters deliver driver logs and
            event logs, but not per-executor stdout/stderr.
          </Alert>
          <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginTop: '18px' }}>
            <button onClick={onBrowse} style={secondaryBtn}>
              Back to recent runs
            </button>
            {hasDriver && (
              <button onClick={onViewDriver} style={primaryBtn}>
                View driver log instead
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ---- No cluster instance (serverless / pipeline / non-compute task) ----- //
export function NoClusterScreen({
  meta,
  detail,
  onBrowse,
}: {
  meta: RunMeta | null;
  detail?: string | null;
  onBrowse: () => void;
}) {
  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
      <Header meta={meta} />
      <div style={centerWrap}>
        <div style={{ maxWidth: '560px', width: '100%' }}>
          <IconBadge bg="var(--app-info-bg)">
            <InfoIcon size={32} fill="var(--theme-info-darker)" />
          </IconBadge>
          <h2 style={{ margin: '0 0 8px', textAlign: 'center', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
            No Spark cluster for this run
          </h2>
          <p style={{ margin: '0 auto 18px', maxWidth: '500px', textAlign: 'center', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
            {detail ||
              "This run didn't use a classic Spark cluster, so it has no executor logs to show."}
          </p>
          <Alert type="info" heading="Why this happens">
            Executor <code style={{ fontFamily: 'var(--font-mono)' }}>stdout</code>/
            <code style={{ fontFamily: 'var(--font-mono)' }}>stderr</code> logs exist only for runs on a{' '}
            <b>classic job cluster</b>. Runs on <b>serverless compute</b> or{' '}
            <b>pipeline / DLT</b> tasks don't produce per-executor Cluster Log Delivery output, so there's
            nothing for this viewer to open.
          </Alert>
          <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginTop: '18px' }}>
            <button onClick={onBrowse} style={secondaryBtn}>
              Back to recent runs
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---- Delivery pending --------------------------------------------------- //
export function DeliveryPendingScreen({ meta, onRefresh }: { meta: RunMeta | null; onRefresh: () => void }) {
  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
      <Header meta={meta} />
      <div style={centerWrap}>
        <div style={{ maxWidth: '520px', width: '100%' }}>
          <IconBadge bg="var(--app-info-bg)">
            <InfoIcon size={32} fill="var(--theme-info-darker)" />
          </IconBadge>
          <h2 style={{ margin: '0 0 8px', textAlign: 'center', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
            Logs are still being delivered
          </h2>
          <p style={{ margin: '0 auto 18px', maxWidth: '460px', textAlign: 'center', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
            This cluster terminated recently and its logs haven't landed in the Volume yet. Cluster log delivery can lag a
            few minutes behind termination. Try again shortly.
          </p>
          <div style={{ display: 'flex', justifyContent: 'center', marginTop: '4px' }}>
            <button onClick={onRefresh} style={primaryBtn}>
              <RefreshIcon /> Refresh
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---- Running -> live Spark UI ------------------------------------------- //
export function RunningScreen({ meta }: { meta: RunMeta | null }) {
  const url = meta?.run_page_url || null;
  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
      <Header meta={meta} />
      <div style={centerWrap}>
        <div style={{ maxWidth: '520px', width: '100%' }}>
          <IconBadge bg="var(--app-info-bg)">
            <InfoIcon size={32} fill="var(--theme-info-darker)" />
          </IconBadge>
          <h2 style={{ margin: '0 0 8px', textAlign: 'center', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
            This run is still in progress
          </h2>
          <p style={{ margin: '0 auto 18px', maxWidth: '460px', textAlign: 'center', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
            Cluster log delivery lags several minutes behind a live cluster, so a true live tail isn't available here.
            View the live Spark UI on the run page for real-time executor output; come back once the run has terminated to
            read the delivered logs.
          </p>
          <div style={{ display: 'flex', justifyContent: 'center', marginTop: '4px' }}>
            {url ? (
              <a href={url} target="_blank" rel="noreferrer" style={primaryBtn}>
                Open the run page <ExternalIcon />
              </a>
            ) : (
              <span style={{ fontSize: '13px', color: 'var(--app-faint)', fontFamily: 'var(--font-mono)' }}>
                No run-page URL available for this run.
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ---- Not found / no access --------------------------------------------- //
export function NotFoundScreen({ query, detail, onBrowse }: { query: string; detail?: string | null; onBrowse: () => void }) {
  return (
    <div style={centerWrap}>
      <div style={{ maxWidth: '520px', width: '100%' }}>
        <IconBadge bg="var(--app-err-bg)">
          <ErrorIcon />
        </IconBadge>
        <h2 style={{ margin: '0 0 8px', textAlign: 'center', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
          No run found for that identifier
        </h2>
        <p style={{ margin: '0 auto 16px', textAlign: 'center', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
          {detail || 'Nothing matched'}{' '}
          <span
            style={{
              fontFamily: 'var(--font-mono)',
              background: 'var(--app-err-bg)',
              color: 'var(--theme-error-dark)',
              padding: '1px 6px',
              borderRadius: '3px',
            }}
          >
            {query}
          </span>{' '}
          among the runs you can access.
        </p>
        <div
          style={{
            background: 'var(--app-elev)',
            border: '1px solid var(--app-border)',
            borderRadius: '8px',
            padding: '14px 16px',
            fontSize: '13px',
            lineHeight: 1.7,
            color: 'var(--app-muted)',
          }}
        >
          <div style={{ fontWeight: 700, color: 'var(--app-text)', marginBottom: '6px', fontSize: '12.5px' }}>
            A few things to check
          </div>
          <div>
            • Make sure you pasted a <b style={{ color: 'var(--app-text)' }}>run ID</b>,{' '}
            <b style={{ color: 'var(--app-text)' }}>cluster ID</b>, or <b style={{ color: 'var(--app-text)' }}>job ID</b> — not a
            task or notebook URL.
          </div>
          <div>
            • The run may be older than the log <b style={{ color: 'var(--app-text)' }}>retention window</b>, or in a workspace
            you don't have access to.
          </div>
          <div>
            • Cluster IDs look like <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--app-text)' }}>0708-194133-abcd1234</span>.
          </div>
        </div>
        <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginTop: '18px' }}>
          <button onClick={onBrowse} style={primaryBtn}>
            Browse recent runs
          </button>
        </div>
      </div>
    </div>
  );
}

// ---- Permission / scope error ------------------------------------------ //
export function PermissionScreen({ detail, isScope, onBrowse }: { detail?: string | null; isScope: boolean; onBrowse: () => void }) {
  return (
    <div style={centerWrap}>
      <div style={{ maxWidth: '520px', width: '100%' }}>
        <IconBadge bg="var(--app-err-bg)">
          <ErrorIcon />
        </IconBadge>
        <h2 style={{ margin: '0 0 8px', textAlign: 'center', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
          {isScope ? 'Reload the app to continue' : "You don't have access to these logs"}
        </h2>
        <p style={{ margin: '0 auto 16px', textAlign: 'center', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
          {isScope
            ? detail ||
              'The app could not read your access token. Reload the app so it can re-obtain your on-behalf-of authorization.'
            : detail ||
              "Unity Catalog denied read access to this cluster's log Volume for your identity. This is a UC permission on the Volume, not an app error — ask a workspace admin to grant you READ on the team's log Volume."}
        </p>
        <div style={{ display: 'flex', gap: '10px', justifyContent: 'center', marginTop: '4px' }}>
          {isScope ? (
            <button onClick={() => window.location.reload()} style={primaryBtn}>
              <RefreshIcon /> Reload
            </button>
          ) : (
            <button onClick={onBrowse} style={secondaryBtn}>
              Back to recent runs
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

// ---- Ambiguous input ---------------------------------------------------- //
export function AmbiguousScreen({ query, onBrowse }: { query: string; onBrowse: () => void }) {
  return (
    <div style={centerWrap}>
      <div style={{ maxWidth: '520px', width: '100%' }}>
        <IconBadge bg="var(--app-warn-bg)">
          <WarnIcon />
        </IconBadge>
        <h2 style={{ margin: '0 0 8px', textAlign: 'center', fontFamily: 'var(--font-serif)', fontSize: '22px', color: 'var(--app-text)' }}>
          Couldn't read that identifier
        </h2>
        <p style={{ margin: '0 auto 16px', textAlign: 'center', fontSize: '14px', lineHeight: 1.6, color: 'var(--app-muted)' }}>
          <span style={{ fontFamily: 'var(--font-mono)', background: 'var(--app-warn-bg)', padding: '1px 6px', borderRadius: '3px' }}>{query}</span>{' '}
          isn't a run ID, job ID (all digits) or a cluster ID (like{' '}
          <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--app-text)' }}>0708-194133-abcd1234</span>).
        </p>
        <div style={{ display: 'flex', justifyContent: 'center', marginTop: '4px' }}>
          <button onClick={onBrowse} style={primaryBtn}>
            Browse recent runs
          </button>
        </div>
      </div>
    </div>
  );
}
