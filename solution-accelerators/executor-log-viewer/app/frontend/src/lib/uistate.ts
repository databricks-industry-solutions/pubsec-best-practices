// Map backend reason_code (spec §3.4) to the coarse UI state the right pane
// renders. This is the single authoritative table (mirrors the design §3.4).
import type { ReasonCode } from './types';

export type UiState =
  | 'main' // OK — the log viewer
  | 'task-select' // PARENT_RUN_HAS_MULTIPLE_TASKS
  | 'no-cld' // no cluster log delivery configured
  | 'no-cluster' // run never used a classic Spark cluster (serverless/pipeline)
  | 'no-executor-dir' // delivered but no executor/ folder (show driver if present)
  | 'delivery-pending' // recently terminated; retry + refresh
  | 'running' // run in progress -> live Spark UI / run page
  | 'not-found' // not found / no access
  | 'permission' // permission / scope error
  | 'file-too-large' // full load over cap
  | 'ambiguous'; // input could not be classified

export function stateForReason(code: ReasonCode): UiState {
  switch (code) {
    case 'OK':
      return 'main';
    case 'PARENT_RUN_HAS_MULTIPLE_TASKS':
      return 'task-select';
    case 'NO_CLD':
    case 'NOT_VOLUME':
    case 'CLD_DESTINATION_UNKNOWN':
    case 'CLD_ROOT_NOT_FOUND':
      return 'no-cld';
    case 'NO_EXECUTOR_DIR':
    case 'NOT_JOB_CLUSTER':
    case 'NO_LOG_FILES':
      return 'no-executor-dir';
    case 'DELIVERY_PENDING':
      return 'delivery-pending';
    case 'RUNNING':
      return 'running';
    case 'NO_CLUSTER_INSTANCE':
      return 'no-cluster';
    case 'RUN_NOT_FOUND':
    case 'RUN_NO_ACCESS':
    case 'TASK_NOT_FOUND':
    case 'CLUSTER_NOT_FOUND':
    case 'CLUSTER_NO_ACCESS':
    case 'CLUSTER_METADATA_UNAVAILABLE':
    case 'FILE_NOT_FOUND':
      return 'not-found';
    case 'FILES_FORBIDDEN':
    case 'FILES_SCOPE_MISSING':
      return 'permission';
    case 'FILE_TOO_LARGE':
      return 'file-too-large';
    case 'INPUT_AMBIGUOUS':
      return 'ambiguous';
    default:
      return 'not-found';
  }
}
