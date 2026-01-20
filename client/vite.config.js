import { fileURLToPath, URL } from 'node:url'

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import vueJsx from '@vitejs/plugin-vue-jsx'
import vueDevTools from 'vite-plugin-vue-devtools'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    vueJsx(),
    vueDevTools(),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    },
  },
  // Dev server proxy: forward websocket requests to backend during development
  server: {
    host: true,
    proxy: {
      // Proxy WebSocket connections from /ws to backend WebSocket server
      '/ws': {
        target: 'ws://127.0.0.1:4456',
        ws: true,
        changeOrigin: true,
        secure: false,
      },
    },
  },
})
