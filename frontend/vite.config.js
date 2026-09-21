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
    // 监听所有网卡: 默认只绑 127.0.0.1, 在远程/虚拟机开发时浏览器连不上
    host: true,
    // dev 模式把 API 请求代理到 C++ 后端, 免跨域。
    // ⚠️ 新增后端路由时这里要同步加 —— 漏了的话该路由在 dev 模式下会 404,
    //    但在生产 (走 8888) 却正常, 很容易查错方向。
    proxy: {
      '/user': 'http://localhost:8888',
      '/file': 'http://localhost:8888',
      '/dir':  'http://localhost:8888'
    }
  }
})
