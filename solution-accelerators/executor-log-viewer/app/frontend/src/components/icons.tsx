// Inline SVG icons (from the wireframe). Self-contained — no external assets,
// so nothing violates the Databricks Apps / artifact CSP.
import type { CSSProperties } from 'react';

interface IconProps {
  size?: number;
  fill?: string;
  style?: CSSProperties;
}

export function SidebarIcon({
  size = 16,
  fill = 'currentColor',
  style,
  collapsed = false,
}: IconProps & { collapsed?: boolean }) {
  // Panel-left glyph; when collapsed the inner divider shifts to hint "expand".
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill="none" stroke={fill} strokeWidth={2} style={style}>
      <rect x="3" y="4" width="18" height="16" rx="2" />
      <line x1={collapsed ? '8' : '9'} y1="4" x2={collapsed ? '8' : '9'} y2="20" />
    </svg>
  );
}

export function LogIcon({ size = 18, fill = '#fff', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M4 5h16v2H4zM4 9h10v2H4zM4 13h16v2H4zM4 17h7v2H4zM16 10l4 3-4 3z" />
    </svg>
  );
}

export function ListIcon({ size = 34, fill = 'var(--app-faint)', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M4 5h16v2H4zM4 9h10v2H4zM4 13h16v2H4zM4 17h7v2H4z" />
    </svg>
  );
}

export function SearchIcon({ size = 15, fill = 'var(--app-faint)', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M15.5 14h-.79l-.28-.27C15.41 12.59 16 11.11 16 9.5 16 5.91 13.09 3 9.5 3S3 5.91 3 9.5 5.91 16 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z" />
    </svg>
  );
}

export function CloseIcon({ size = 14, fill = 'currentColor', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M19 6.41 17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z" />
    </svg>
  );
}

export function FolderIcon({ size = 13, fill = 'var(--app-faint)', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M10 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z" />
    </svg>
  );
}

export function DownloadIcon({ size = 14, fill = 'currentColor', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z" />
    </svg>
  );
}

export function InfoIcon({ size = 15, fill = 'var(--app-faint)', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-6h2v6zm0-8h-2V7h2v2z" />
    </svg>
  );
}

export function WarnIcon({ size = 32, fill = 'var(--theme-warning-darker)', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z" />
    </svg>
  );
}

export function ErrorIcon({ size = 32, fill = 'var(--theme-error-dark)', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z" />
    </svg>
  );
}

export function ExternalIcon({ size = 15, fill = 'currentColor', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M19 19H5V5h7V3H5c-1.11 0-2 .9-2 2v14c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2v-7h-2v7zM14 3v2h3.59l-9.83 9.83 1.41 1.41L19 6.41V10h2V3z" />
    </svg>
  );
}

export function RefreshIcon({ size = 15, fill = 'currentColor', style }: IconProps) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={fill} style={style}>
      <path d="M17.65 6.35A7.958 7.958 0 0012 4c-4.42 0-7.99 3.58-7.99 8s3.57 8 7.99 8c3.73 0 6.84-2.55 7.73-6h-2.08A5.99 5.99 0 0112 18c-3.31 0-6-2.69-6-6s2.69-6 6-6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z" />
    </svg>
  );
}
