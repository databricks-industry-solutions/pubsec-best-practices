// API client. All requests are relative so the same code works whether the SPA
// is served by the FastAPI backend in prod or via the Vite dev proxy.
import {
  FileTooLargeError,
  LogFileFetchError,
  type BrowseResult,
  type ClustersResult,
  type LogFileMeta,
  type LogFileResult,
  type LogRootsResult,
  type ResolveResult,
  type RunsPage,
} from './types';

async function jsonOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) {
    let detail = res.statusText;
    let reason: string | undefined;
    try {
      const body = await res.json();
      detail = body?.detail || body?.reason_code || detail;
      reason = body?.reason_code;
    } catch {
      /* non-JSON body */
    }
    throw new LogFileFetchError(res.status, detail, reason as never);
  }
  return res.json() as Promise<T>;
}

export interface ListRunsParams {
  limit?: number;
  pageToken?: string | null;
  terminatedOnly?: boolean;
  jobId?: number | null;
  includeClusterless?: boolean;
}

export async function listRuns(params: ListRunsParams = {}): Promise<RunsPage> {
  const q = new URLSearchParams();
  if (params.limit != null) q.set('limit', String(params.limit));
  if (params.pageToken) q.set('page_token', params.pageToken);
  if (params.terminatedOnly) q.set('terminated_only', 'true');
  if (params.jobId != null) q.set('job_id', String(params.jobId));
  if (params.includeClusterless) q.set('include_clusterless', 'true');
  const res = await fetch(`/api/runs?${q.toString()}`);
  return jsonOrThrow<RunsPage>(res);
}

// Resolve a run/cluster/job id into the logs tree (or an error reason_code).
// The backend returns HTTP 200 with an ERROR outcome for expected states, so we
// parse the JSON body rather than treating non-2xx as fatal.
export async function resolveLogs(
  value: string,
  taskRunId?: number | null,
): Promise<ResolveResult> {
  const q = new URLSearchParams();
  if (taskRunId != null) q.set('task_run_id', String(taskRunId));
  const suffix = q.toString() ? `?${q.toString()}` : '';
  const res = await fetch(`/api/runs/${encodeURIComponent(value)}/logs${suffix}`);
  // 401 (auth) still carries a JSON body with reason_code; surface it as a
  // ResolveResult-shaped error so the UI maps it to the scope/permission state.
  if (res.status === 401 || res.status === 403) {
    const body = await res.json().catch(() => ({}));
    return {
      outcome: 'ERROR',
      reason_code: body?.reason_code ?? 'FILES_SCOPE_MISSING',
      run_meta: null,
      tasks: null,
      tree: null,
      detail: body?.detail ?? 'authentication required; reload the app',
    };
  }
  return jsonOrThrow<ResolveResult>(res);
}

// Recent clusters that have executor logs — the PRIMARY recent list. Sourced
// from the app SP's clusters.list (metadata only). Clicking a cluster reuses
// the normal resolve path via listClustersWithLogs -> onSelectRun(cluster_id).
export async function listClustersWithLogs(limit?: number): Promise<ClustersResult> {
  const q = new URLSearchParams();
  if (limit != null) q.set('limit', String(limit));
  const suffix = q.toString() ? `?${q.toString()}` : '';
  const res = await fetch(`/api/clusters${suffix}`);
  return jsonOrThrow<ClustersResult>(res);
}

// The configured CLD roots, offered as browse shortcuts. Not auth-sensitive.
export async function listLogRoots(): Promise<LogRootsResult> {
  const res = await fetch('/api/log-roots');
  return jsonOrThrow<LogRootsResult>(res);
}

// Browse the cluster-level log directories under a CLD root, AS THE USER.
// UC-gated: 403 (no READ), 404 (path not found), 400 (not a /Volumes path).
// jsonOrThrow surfaces the reason_code/detail as a LogFileFetchError so callers
// can branch on err.reasonCode / err.status.
export async function browseLogRoot(path: string): Promise<BrowseResult> {
  const res = await fetch(`/api/browse?path=${encodeURIComponent(path)}`);
  return jsonOrThrow<BrowseResult>(res);
}

function parseIntHeader(v: string | null): number | null {
  if (v == null) return null;
  const n = parseInt(v, 10);
  return Number.isNaN(n) ? null : n;
}

function readMeta(headers: Headers): LogFileMeta {
  return {
    totalSize: parseIntHeader(headers.get('X-Log-Total-Size')),
    rangeStart: parseIntHeader(headers.get('X-Log-Range-Start')),
    rangeEnd: parseIntHeader(headers.get('X-Log-Range-End')),
    earlierBytesHidden: parseIntHeader(headers.get('X-Log-Earlier-Bytes-Hidden')),
    mode: (headers.get('X-Log-Mode') as 'tail' | 'full' | null) ?? null,
    outcome: headers.get('X-Log-Outcome'),
  };
}

// Fetch a log file's text body plus its X-Log-* header metadata.
// - 413 -> FileTooLargeError (the file-too-large UI state).
// - 404 -> LogFileFetchError with FILE_NOT_FOUND.
export async function fetchLogFile(
  fileRef: string,
  mode: 'tail' | 'full' = 'tail',
): Promise<LogFileResult> {
  const res = await fetch(
    `/api/log-files/${encodeURIComponent(fileRef)}?mode=${mode}`,
  );

  if (res.status === 413) {
    const meta = readMeta(res.headers);
    // Drain the (JSON) body so the connection is freed.
    await res.text().catch(() => undefined);
    throw new FileTooLargeError(meta.totalSize);
  }

  if (!res.ok) {
    let detail = res.statusText;
    let reason: string | undefined;
    try {
      const body = await res.json();
      detail = body?.detail || detail;
      reason = body?.reason_code;
    } catch {
      /* ignore */
    }
    throw new LogFileFetchError(res.status, detail, reason as never);
  }

  const meta = readMeta(res.headers);
  // Stream/consume the text body. The backend sends text/plain (possibly
  // chunked); res.text() concatenates the full stream.
  const text = await res.text();
  return { text, meta };
}
