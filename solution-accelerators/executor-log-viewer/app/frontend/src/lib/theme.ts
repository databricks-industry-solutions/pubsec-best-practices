// USWDS-derived theme tokens, lifted verbatim from the approved wireframe
// (wireframe/extracted/Executor Log Viewer.dc.html renderVals()). Applied as
// CSS custom properties on the root element so the whole app themes at once.
export type ThemeName = 'light' | 'dark';

export const ACCENT = '#005ea2'; // USWDS primary; wireframe default accentColor

export const LIGHT_VARS: Record<string, string> = {
  '--app-bg': '#f5f6f7',
  '--app-panel': '#ffffff',
  '--app-rail': '#ffffff',
  '--app-elev': '#fbfcfd',
  '--app-input': '#ffffff',
  '--app-notice': '#f1f3f6',
  '--app-tab': '#edeff0',
  '--app-text': '#1b1b1b',
  '--app-muted': '#565c65',
  '--app-faint': '#71767a',
  '--app-border': '#dfe1e2',
  '--app-border-strong': '#c6cace',
  '--app-hover': '#f1f3f6',
  '--app-warn-bg': '#faf3d1',
  '--app-info-bg': '#e7f6f8',
  '--app-err-bg': '#f9eeee',
  '--theme-warning-darker': '#936f38',
  '--theme-info-darker': '#2e6276',
  '--theme-error-dark': '#b50909',
  '--link-default': '#005ea2',
  '--link-hover': '#1a4480',
};

export const DARK_VARS: Record<string, string> = {
  '--app-bg': '#131519',
  '--app-panel': '#1b1d22',
  '--app-rail': '#17191d',
  '--app-elev': '#20232a',
  '--app-input': '#22252b',
  '--app-notice': '#20232a',
  '--app-tab': '#22252b',
  '--app-text': '#eceef1',
  '--app-muted': '#a9aeb1',
  '--app-faint': '#8d9297',
  '--app-border': '#2a2d34',
  '--app-border-strong': '#3a3e46',
  '--app-hover': '#23262d',
  '--app-warn-bg': 'rgba(255,190,46,0.12)',
  '--app-info-bg': 'rgba(0,189,227,0.13)',
  '--app-err-bg': 'rgba(216,57,51,0.15)',
  '--theme-warning-darker': '#e6b45c',
  '--theme-info-darker': '#6fc6dc',
  '--theme-error-dark': '#ff8d7b',
  '--link-default': '#58b4ff',
  '--link-hover': '#8fd0ff',
};

// System font stacks approximating the USWDS families used by the wireframe.
export const FONT_SANS =
  "'Source Sans 3', 'Source Sans Pro', system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";
export const FONT_SERIF =
  "'Merriweather', Georgia, Cambria, 'Times New Roman', Times, serif";
export const FONT_MONO =
  "'Roboto Mono', ui-monospace, 'SF Mono', SFMono-Regular, Menlo, Consolas, 'Liberation Mono', monospace";

export function applyTheme(theme: ThemeName): void {
  const vars = theme === 'dark' ? DARK_VARS : LIGHT_VARS;
  const root = document.documentElement;
  for (const [k, v] of Object.entries(vars)) root.style.setProperty(k, v);
  root.style.setProperty('--font-sans', FONT_SANS);
  root.style.setProperty('--font-serif', FONT_SERIF);
  root.style.setProperty('--font-mono', FONT_MONO);
  root.setAttribute('data-theme', theme);
}
