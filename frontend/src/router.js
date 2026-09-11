import { createRouter, createWebHashHistory } from 'vue-router'

// hash 模式: URL 形如 /spa/index.html#/login
// 免去后端做 history fallback (wfrest 只有显式路由)
const routes = [
  { path: '/', redirect: '/home' },
  { path: '/login', component: () => import('./views/Login.vue') },
  { path: '/register', component: () => import('./views/Register.vue') },
  { path: '/home', component: () => import('./views/Home.vue') }
]

const router = createRouter({
  history: createWebHashHistory(),
  routes
})

// 未登录一律赶回登录页
router.beforeEach((to) => {
  const authed = !!localStorage.getItem('token')
  if (!authed && to.path !== '/login' && to.path !== '/register') {
    return '/login'
  }
})

export default router
