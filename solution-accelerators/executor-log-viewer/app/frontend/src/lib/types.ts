// TypeScript types mirroring the FastAPI backend response shapes EXACTLY.
// Source of truth: app/backend/app.py, runs.py, resolver.py, logs.py.

// ---- reason codes (spec §3.4) ------------------------------------------- //
export type ReasonCode =
  | 'OK'
  | 'INPUT_AMBIGUOUS'
  | 'PARENT_RUN_HAS_MULTIPLE_TASKS'
  | 'RUN_NOT_FOUND'
  | 'RUN_NO_ACCESS'
  | 'TASK_NOT_FOUND'
  | 'RUNNING'
  | 'NOT_JOB_CLUSTER'
  | 'NO_CLUSTER_INSTANCE'
  | 'CLUSTER_NOT_FOUND'
  | 'CLUSTER_NO_ACCESS'
  | 'CLUSTER_METADATA_UNAVAILABLE'
  | 'NO_CLD'
  | 'NOT_VOLUME'
  | 'CLD_DESTINATION_UNKNOWN'
  | 'FILES_SCOPE_MISSING'
  | 'FILES_FORBIDDEN'
  | 'CLD_ROOT_NOT_FOUND'
  | 'DELIVERY_PENDING'
  | 'NO_EXECUTOR_DIR'
  | 'NO_LOG_FILES'
  | 'FILE_TOO_LARGE'
  | 'FILE_NOT_FOUND';

// ---- GET /api/runs ------------------------------------------------------ //
export interface RunSummary {
  run_id: number;
  job_id: number | null;
  job_name: string | null;
  state: string | null; // life-cycle state e.g. TERMINATED / RUNNING
  result_state: string | null; // SUCCESS / FAILED / null
  started_at: number | null; // unix epoch millis
  run_page_url: string | null;
  cluster_id: string | null; // resolved from tasks[].cluster_instance
  has_cluster: boolean; // false => serverless/pipeline; no executor logs
  has_logs: boolean; // cluster has CLD->Volume (executor logs deliverable)
}

export interface RunsPage {
  runs: RunSummary[];
  next_page_token: string | null;
}

// ---- GET /api/runs/{run_id}/logs ---------------------------------------- //
export interface LogFileNode {
  name: string;
  file_kind: string; // stdout | stderr | log
  size: number | null;
  modified: number | null;
  file_ref: string; // opaque signed ref
}

export interface ExecutorNode {
  executor_id: string;
  files: LogFileNode[];
}

export interface AppGroupNode {
  app_id: string;
  executors: ExecutorNode[];
}

export interface LogTree {
  cluster_id: string;
  cld_root: string;
  app_groups: AppGroupNode[];
  driver_files: LogFileNode[];
}

export interface RunMeta {
  run_id?: number | null;
  job_id?: number | null;
  run_name?: string | null;
  state?: string | null;
  result_state?: string | null;
  run_page_url?: string | null;
  cluster_id?: string | null;
}

export interface TaskSummary {
  run_id: number;
  task_key: string | null;
  state: string | null;
  cluster_id: string | null;
}

export interface ResolveResult {
  outcome: 'OK' | 'ERROR';
  reason_code: ReasonCode;
  run_meta: RunMeta | null;
  tasks: TaskSummary[] | null;
  tree: LogTree | null;
  detail: string | null;
}

// ---- GET /api/clusters -------------------------------------------------- //
// Recent clusters that have executor logs — the PRIMARY browse source. Sourced
// from the app SP's clusters.list (metadata only). Returns cluster METADATA
// only; NEVER a file_ref, never content, and (security fix MEDIUM #8) never the
// raw CLD Volume path — that stays server-side for resolution. Clicking a
// cluster reuses the normal resolve path (/api/runs/{cluster_id}/logs).
export interface ClusterEntry {
  cluster_id: string;
  cluster_name: string | null;
  state: string | null; // e.g. TERMINATED / RUNNING
  cluster_source: string | null; // e.g. JOB / UI / API
  started_at: number | null; // unix epoch millis
  terminated_at: number | null; // unix epoch millis
  job_id?: string | null; // parsed from job-<id>-run-<id> cluster name
  run_id?: string | null; // parsed run id
  job_name?: string | null; // friendly job name (jobs.get); null if deleted
}

export interface ClustersResult {
  clusters: ClusterEntry[];
}

// ---- GET /api/log-roots ------------------------------------------------- //
export interface LogRootsResult {
  roots: string[];
}

// ---- GET /api/browse?path=... ------------------------------------------- //
// User-scoped Volume browse: DISCOVERY only. Lists directory NAMES; it never
// returns a file_ref. Clicking a cluster goes through the normal resolve path.
export interface BrowseCluster {
  cluster_id: string; // the directory basename
  path: string; // normalized /Volumes/... dir path (name only, not a ref)
  modified: number | null; // unix epoch millis when available
  has_executor: boolean | null; // null => unknown (not peeked; resolve decides)
  has_driver: boolean | null; // null => unknown
}

export interface BrowseResult {
  path: string; // the normalized root that was browsed
  clusters: BrowseCluster[];
}

// ---- GET /api/log-files/{file_ref} -------------------------------------- //
// Body is streamed text/plain; metadata comes from X-Log-* response headers.
export interface LogFileMeta {
  totalSize: number | null;
  rangeStart: number | null;
  rangeEnd: number | null;
  earlierBytesHidden: number | null;
  mode: 'tail' | 'full' | null;
  outcome: string | null;
}

export interface LogFileResult {
  text: string;
  meta: LogFileMeta;
}

// Thrown for the FILE_TOO_LARGE (413) case so the UI can show that state.
export class FileTooLargeError extends Error {
  reasonCode: ReasonCode = 'FILE_TOO_LARGE';
  totalSize: number | null;
  constructor(totalSize: number | null) {
    super('file exceeds full-load cap');
    this.totalSize = totalSize;
  }
}

export class LogFileFetchError extends Error {
  status: number;
  reasonCode?: ReasonCode;
  constructor(status: number, message: string, reasonCode?: ReasonCode) {
    super(message);
    this.status = status;
    this.reasonCode = reasonCode;
  }
}
