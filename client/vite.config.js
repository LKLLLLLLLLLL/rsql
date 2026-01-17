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
  // =============== 开发环境配置 ===============
  // 当运行 npm run dev 时，Vite会启动开发服务器
  // 但API和WebSocket会代理到后端Rust服务器
  server: {
    proxy: {
      // 代理 /ws (WebSocket) 到后端服务器
      '/ws': {
        target: 'ws://127.0.0.1:4455',
        ws: true,
        changeOrigin: true,
      },
      // 代理 /api (HTTP API) 到后端服务器
      '/api': {
        target: 'http://127.0.0.1:4455',
        changeOrigin: true,
      },
    },
  },
})

