import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// wfrest 没有通配路由，构建产物必须用确定性文件名 (index.html + app.js + style.css)，
// 由后端显式路由逐一挂载
export default defineConfig({
  base: '/spa/',
  plugins: [vue()],
  build: {
    outDir: '../static/spa',
    emptyOutDir: true,
    cssCodeSplit: false,
    assetsInlineLimit: 10 * 1024 * 1024,
    rollupOptions: {
      output: {
        entryFileNames: 'app.js',
        chunkFileNames: 'chunk-[name].js',
        assetFileNames: '[name].[ext]'
      }
    }
  },
  server: {
    // dev 模式 (npm run dev, 默认 5173 端口) 把 API 请求代理到 C++ 后端，免跨域
    proxy: {
      '/user': 'http://localhost:8888',
      '/file': 'http://localhost:8888'
    }
  }
})
