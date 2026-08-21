import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

// The app makes same-origin requests to `/api` and fetches `/config.json` at
// runtime. In production both are served from the same origin as the built
// assets. For local development you may point `/api` at a backend by setting
// `VITE_DEV_API_TARGET` (e.g. https://localhost:8443) in a `.env.local` file.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '.', ['VITE_']);
  const apiTarget = env['VITE_DEV_API_TARGET'];

  return {
    plugins: [react()],
    server: {
      port: 5173,
      strictPort: true,
      ...(apiTarget
        ? {
            proxy: {
              '/api': {
                target: apiTarget,
                changeOrigin: true,
                secure: false,
              },
            },
          }
        : {}),
    },
    build: {
      target: 'es2022',
      sourcemap: false,
      outDir: 'dist',
    },
  };
});
