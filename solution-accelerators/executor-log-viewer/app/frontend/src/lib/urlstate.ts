// Selection state persisted in the URL hash so a view is shareable/reloadable
// (spec §5 / task 3.7). We use the hash (not query params) so no server-side
// routing is needed and the FastAPI catch-all always serves index.html.
//
// Shape: #id=<lookup>&task=<taskRunId>&group=<appId>&exec=<execId>&file=<name>&stream=<kind>&theme=<light|dark>

export interface Selection {
  id: string | null; // run/cluster/job id being viewed
  task: string | null; // chosen task_run_id (multi-task parent runs)
  group: string | null; // app-id group
  exec: string | null; // executor id ("driver" sentinel handled separately)
  file: string | null; // selected file name within the leaf
  view: 'driver' | 'executors' | null; // high-level tab
  theme: 'light' | 'dark' | null;
}

const EMPTY: Selection = {
  id: null,
  task: null,
  group: null,
  exec: null,
  file: null,
  view: null,
  theme: null,
};

export function parseHash(hash: string): Selection {
  const h = hash.replace(/^#/, '');
  if (!h) return { ...EMPTY };
  const params = new URLSearchParams(h);
  const get = (k: string) => {
    const v = params.get(k);
    return v == null || v === '' ? null : v;
  };
  const view = get('view');
  const theme = get('theme');
  return {
    id: get('id'),
    task: get('task'),
    group: get('group'),
    exec: get('exec'),
    file: get('file'),
    view: view === 'driver' || view === 'executors' ? view : null,
    theme: theme === 'light' || theme === 'dark' ? theme : null,
  };
}

export function toHash(sel: Selection): string {
  const params = new URLSearchParams();
  if (sel.id) params.set('id', sel.id);
  if (sel.task) params.set('task', sel.task);
  if (sel.group) params.set('group', sel.group);
  if (sel.exec) params.set('exec', sel.exec);
  if (sel.file) params.set('file', sel.file);
  if (sel.view) params.set('view', sel.view);
  if (sel.theme) params.set('theme', sel.theme);
  const s = params.toString();
  return s ? `#${s}` : '';
}
