import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The FastAPI backend serves the built SPA out of frontend/dist/ and exposes
// the API under /api, plus /whoami /probe /healthz. In local dev we proxy those
// to a locally running uvicorn so the same relative fetch() calls work.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': { target: 'http://localhost:8000', changeOrigin: true },
      '/whoami': { target: 'http://localhost:8000', changeOrigin: true },
      '/probe': { target: 'http://localhost:8000', changeOrigin: true },
      '/healthz': { target: 'http://localhost:8000', changeOrigin: true },
    },
  },
});
