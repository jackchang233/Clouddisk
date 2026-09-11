import axios from 'axios'
import { ElMessage } from 'element-plus'

const api = axios.create({ timeout: 30000 })

// 与后端约定: username/token 走 URL query (原 auth.js 的 queryParams 方案)
// 拦截器统一注入，业务代码不用每次手动拼
api.interceptors.request.use((config) => {
  const url = config.url || ''
  if (url.startsWith('/user') || url.startsWith('/file')) {
    config.params = {
      username: localStorage.getItem('username') || '',
      token: localStorage.getItem('token') || '',
      ...config.params
    }
  }
  return config
})

// token 失效 => 清缓存并回登录页
api.interceptors.response.use(
  (resp) => resp,
  (error) => {
    if (error.response && error.response.status === 401) {
      localStorage.removeItem('token')
      localStorage.removeItem('username')
      ElMessage.error('登录已过期，请重新登录')
      if (location.hash !== '#/login') location.hash = '#/login'
    }
    return Promise.reject(error)
  }
)

// 后端 resp->String() 输出的 JSON 的 Content-Type 不一定是 application/json，
// axios 对非 JSON 响应给的是字符串，这里统一解包
export function unwrap(resp) {
  return typeof resp.data === 'string' ? JSON.parse(resp.data) : resp.data
}

// 后端表单接口只认 application/x-www-form-urlencoded
export function formBody(obj) {
  return new URLSearchParams(obj)
}

// 后端错误体已统一为 {"code":<状态码>,"msg":"<文案>"}, 优先展示后端给的 msg
export function errMsg(error, fallback) {
  const data = error.response && error.response.data
  if (data) {
    try {
      const body = typeof data === 'string' ? JSON.parse(data) : data
      if (body && body.msg) return body.msg
    } catch (_) { /* 非 JSON 响应, 走下面的状态码兜底 */ }
  }
  if (error.response && error.response.status >= 500) return '服务器开小差了，请稍后重试'
  return fallback || '网络异常，请稍后重试'
}

export default api
