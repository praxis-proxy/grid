import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        // Split the big, rarely-changing dependencies into their own vendor
        // chunks so app code changes don't invalidate their browser cache
        // entry. Matched by path (not a plain package-name list) because
        // React ships as react/jsx-runtime, react-dom/client, etc.
        manualChunks(id) {
          if (!id.includes('node_modules')) return undefined
          if (id.includes('/react-dom/') || id.includes('/react/') || id.includes('/scheduler/')) return 'react'
          if (id.includes('/leaflet/') || id.includes('/leaflet.markercluster/')) return 'leaflet'
          if (id.includes('/recharts/')) return 'recharts'
          return undefined
        },
      },
    },
  },
  server: {
    proxy: {
      // The Go backend (or dev/mock-server.mjs) listens on 8080.
      '/api': { target: 'http://localhost:8080', changeOrigin: true },
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./vitest.setup.ts'],
    css: false,
  },
})
