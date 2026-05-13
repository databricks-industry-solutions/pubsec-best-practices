import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      path: 'path-browserify',
      stream: 'stream-browserify',
    },
  },
  optimizeDeps: {
    include: [
      'react',
      'react-dom',
      'use-sync-external-store/shim/with-selector',
      'use-sync-external-store/shim',
      'zustand',
      'immer',
      'path-browserify',
    ],
  },
  server: {
    port: 3333,
    proxy: {
      '/api': 'http://localhost:8000',
    },
  },
  build: {
    outDir: 'build',
  },
})
