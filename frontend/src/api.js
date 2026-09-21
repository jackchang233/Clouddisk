import axios from 'axios'
import { ElMessage } from 'element-plus'

const api = axios.create({ timeout: 30000 })

// 与后端约定: username/token 走 URL query
//
// 这里**不判断路径前缀**, 一律注入。原先写的是
//   if (url.startsWith('/user') || url.startsWith('/file'))
// 结果加 /dir 路由时忘了同步, /dir/list 拿不到 token -> 401 ->
// 响应拦截器清 token 并提示"登录已过期" —— 刚登录就被登出, 且现象
// 指向"token 失效"而不是"某个路由漏配", 排查方向完全被带偏。
//
// 对自有后端发 token 没有副作用, 所以去掉这个必须手工维护的列表。
api.interceptors.request.use((config) => {
  config.params = {
    username: localStorage.getItem('username') || '',
    token: localStorage.getItem('token') || '',
    ...config.params
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
