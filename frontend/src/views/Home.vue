<template>
  <el-container class="home-page">
    <!-- 侧栏: 用户信息 -->
    <el-aside class="home-aside" width="230px">
      <el-avatar :size="84" src="/static/img/avatar.jpeg" />
      <div class="aside-username">{{ userInfo.Username || '...' }}</div>
      <div class="aside-meta">注册于 {{ userInfo.SignupAt || '--' }}</div>
      <el-button
        style="margin-top: 18px"
        :icon="SwitchButton"
        @click="onLogout"
      >退出登录</el-button>
      <el-button
        style="margin-top: 10px"
        :icon="Delete"
        @click="openRecycle"
      >回收站</el-button>
    </el-aside>

    <!-- 主区: 上传 + 文件列表 -->
    <el-main class="home-main">
      <div class="panel-card">
        <h2 class="panel-title">
          <span>文件列表</span>
          <el-button :icon="Refresh" circle @click="loadFiles" />
        </h2>

        <el-upload
          class="drop-zone"
          drag
          multiple
          :show-file-list="false"
          :http-request="doUpload"
        >
          <el-icon :size="42" color="#4f6ef7"><UploadFilled /></el-icon>
          <div class="el-upload__text">拖拽文件到这里，或 <em>点击选择文件</em></div>
        </el-upload>

        <el-progress
          v-if="uploading"
          class="upload-progress"
          :percentage="uploadPercent"
        />

        <el-table :data="files" v-loading="loadingList" style="width: 100%">
          <el-table-column label="文件名" min-width="220">
            <template #default="{ row }">
              <el-icon style="vertical-align: -2px; margin-right: 6px"><Document /></el-icon>
              {{ row.FileName }}
            </template>
          </el-table-column>
          <el-table-column label="大小" width="110">
            <template #default="{ row }">{{ fmtSize(row.FileSize) }}</template>
          </el-table-column>
          <el-table-column prop="UploadAt" label="上传时间" width="180" />
          <el-table-column prop="LastUpdated" label="更新时间" width="180" />
          <el-table-column label="操作" width="140">
            <template #default="{ row }">
              <a class="download-link" :href="downloadUrl(row)">
                <el-button type="primary" link :icon="Download">下载</el-button>
              </a>
              <el-button type="danger" link :icon="Delete" @click="onDelete(row)">删除</el-button>
            </template>
          </el-table-column>
          <template #empty>
            <el-empty description="还没有文件，上传一个试试" />
          </template>
        </el-table>
      </div>
    </el-main>

    <!-- 回收站弹窗 -->
    <el-dialog v-model="recycleVisible" title="回收站" width="640px">
      <el-table :data="recycleFiles" v-loading="loadingRecycle" style="width: 100%">
        <el-table-column label="文件名" min-width="220">
          <template #default="{ row }">
            <el-icon style="vertical-align: -2px; margin-right: 6px"><Document /></el-icon>
            {{ row.FileName }}
          </template>
        </el-table-column>
        <el-table-column label="大小" width="110">
          <template #default="{ row }">{{ fmtSize(row.FileSize) }}</template>
        </el-table-column>
        <el-table-column prop="DeletedAt" label="删除时间" width="180" />
        <el-table-column label="操作" width="170">
          <template #default="{ row }">
            <el-button type="primary" link :icon="RefreshRight" @click="onRestore(row)">恢复</el-button>
            <el-button type="danger" link :icon="Delete" @click="onPurge(row)">彻底删除</el-button>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty description="回收站是空的" />
        </template>
      </el-table>
    </el-dialog>
  </el-container>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { UploadFilled, Document, Download, Refresh, SwitchButton, Delete, RefreshRight } from '@element-plus/icons-vue'
import api, { unwrap, formBody, errMsg } from '../api'

const router = useRouter()
const userInfo = reactive({ Username: '', SignupAt: '' })
const files = ref([])
const loadingList = ref(false)
const uploading = ref(false)
const uploadPercent = ref(0)
const recycleVisible = ref(false)
const recycleFiles = ref([])
const loadingRecycle = ref(false)

onMounted(() => {
  loadUserInfo()
  loadFiles()
})

// 无效 token 现在统一回 401，由 api.js 的响应拦截器全局处理 (清 token + 回登录页)
async function loadUserInfo() {
  try {
    const body = unwrap(await api.get('/user/info'))
    userInfo.Username = body.data.Username
    userInfo.SignupAt = body.data.SignupAt
  } catch (err) {
    /* 401 已由拦截器统一处理，这里只需吞掉异常避免控制台报未捕获 */
  }
}

async function loadFiles() {
  loadingList.value = true
  try {
    const list = unwrap(await api.post('/file/query', formBody({ limit: 50 })))
    files.value = Array.isArray(list) ? list : []
  } catch (err) {
    ElMessage.error(errMsg(err, '文件列表加载失败'))
  } finally {
    loadingList.value = false
  }
}

// 分片大小。超过这个尺寸走分片上传, 否则走原有的整文件上传。
// 与后端约定一致即可, 后端从 init 参数里读。
const CHUNK_SIZE = 5 * 1024 * 1024

// 采样指纹: 文件名 + 大小 + 首尾各 1MB 一起做 SHA-256。
//
// 为什么不读整个文件算哈希: 2GB 文件要读若干秒, 用户会以为页面卡死。
// 为什么不用随机 UUID: 那样换浏览器/清缓存就丢了, 本质上不是"续传"。
// 采样指纹只有 2MB 的读取量, 且**由内容决定** —— 同一文件在任何设备
// 算出的值相同, 所以断点续传真正可用。
async function fileFingerprint(file) {
  if (!window.crypto || !window.crypto.subtle) {
    // crypto.subtle 只在安全上下文 (https 或 localhost) 可用。
    // 非安全上下文下降级为随机标识: 仍能上传, 但刷新后无法续传。
    console.warn('crypto.subtle 不可用, 降级为随机上传标识 (无法跨会话续传)')
    return Array.from({ length: 32 }, () => Math.floor(Math.random() * 16).toString(16)).join('')
  }

  const SAMPLE = 1024 * 1024
  const head = await file.slice(0, SAMPLE).arrayBuffer()
  const tail = await file.slice(Math.max(0, file.size - SAMPLE)).arrayBuffer()
  const meta = new TextEncoder().encode(`${file.name}:${file.size}`)

  const buf = new Uint8Array(head.byteLength + tail.byteLength + meta.byteLength)
  buf.set(new Uint8Array(head), 0)
  buf.set(new Uint8Array(tail), head.byteLength)
  buf.set(meta, head.byteLength + tail.byteLength)

  const digest = await crypto.subtle.digest('SHA-256', buf)
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('')
}

// 分片上传 + 断点续传
async function doChunkedUpload(file) {
  const uploadId = await fileFingerprint(file)

  // init: 建会话并询问后端"传到哪了"
  const init = unwrap(
    await api.post(
      '/file/upload/init',
      formBody({
        upload_id: uploadId,
        filename: file.name,
        size: file.size,
        chunk_size: CHUNK_SIZE
      })
    )
  )
  const done = new Set(init.done || [])
  const total = Math.ceil(file.size / CHUNK_SIZE)

  if (done.size > 0 && done.size < total) {
    ElMessage.info(`检测到未完成的上传，从 ${done.size}/${total} 片继续`)
  }

  let uploaded = done.size
  uploadPercent.value = Math.round((uploaded / total) * 100)

  // 只传缺失的分片 —— 断点续传就是这一行 if
  for (let i = 0; i < total; i++) {
    if (done.has(i)) continue
    const fd = new FormData()
    fd.append('upload_id', uploadId)
    fd.append('index', i)
    fd.append('chunk', file.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE))
    await api.post('/file/upload/chunk', fd)
    uploaded++
    uploadPercent.value = Math.round((uploaded / total) * 100)
  }

  await api.post('/file/upload/complete', formBody({ upload_id: uploadId }))
}

async function doUpload({ file }) {
  uploading.value = true
  uploadPercent.value = 0
  try {
    if (file.size > CHUNK_SIZE) {
      await doChunkedUpload(file)
    } else {
      const fd = new FormData()
      fd.append('file', file)
      // 不手动设 Content-Type，让 axios/浏览器自动带上 multipart boundary
      await api.post('/file/upload', fd, {
        onUploadProgress: (e) => {
          if (e.total) uploadPercent.value = Math.round((e.loaded / e.total) * 100)
        }
      })
    }
    ElMessage.success(`${file.name} 上传成功`)
    loadFiles()
  } catch (err) {
    ElMessage.error(errMsg(err, `${file.name} 上传失败`))
  } finally {
    uploading.value = false
    uploadPercent.value = 0
  }
}

// 下载不走 AJAX: 带 query 的普通链接，浏览器按 Content-Disposition 落盘
function downloadUrl(row) {
  const p = new URLSearchParams({
    filename: row.FileName,
    filehash: row.FileHash,
    username: localStorage.getItem('username') || '',
    token: localStorage.getItem('token') || ''
  })
  return '/file/download?' + p.toString()
}

async function onDelete(row) {
  try {
    await ElMessageBox.confirm(`确定删除「${row.FileName}」吗？`, '删除确认', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消'
    })
  } catch (_) {
    return  // 用户取消
  }
  try {
    await api.post('/file/delete', formBody({ id: row.Id }))
    ElMessage.success('删除成功')
    loadFiles()
  } catch (err) {
    ElMessage.error(errMsg(err, '删除失败'))
  }
}

async function openRecycle() {
  recycleVisible.value = true
  await loadRecycle()
}

async function loadRecycle() {
  loadingRecycle.value = true
  try {
    const list = unwrap(await api.post('/file/recycle', formBody({})))
    recycleFiles.value = Array.isArray(list) ? list : []
  } catch (err) {
    ElMessage.error(errMsg(err, '回收站加载失败'))
  } finally {
    loadingRecycle.value = false
  }
}

async function onRestore(row) {
  try {
    await api.post('/file/restore', formBody({ id: row.Id }))
    ElMessage.success(`已恢复「${row.FileName}」`)
    loadRecycle()
    loadFiles()
  } catch (err) {
    ElMessage.error(errMsg(err, '恢复失败'))
  }
}

async function onPurge(row) {
  try {
    await ElMessageBox.confirm(`彻底删除「${row.FileName}」后将无法恢复，确定吗？`, '彻底删除确认', {
      type: 'warning',
      confirmButtonText: '彻底删除',
      cancelButtonText: '取消'
    })
  } catch (_) {
    return
  }
  try {
    await api.post('/file/purge', formBody({ id: row.Id }))
    ElMessage.success('已彻底删除')
    loadRecycle()
  } catch (err) {
    ElMessage.error(errMsg(err, '彻底删除失败'))
  }
}

function fmtSize(n) {
  if (n == null) return '--'
  if (n < 1024) return n + ' B'
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB'
  if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' MB'
  return (n / 1024 / 1024 / 1024).toFixed(1) + ' GB'
}

function onLogout() {
  localStorage.removeItem('token')
  localStorage.removeItem('username')
  router.push('/login')
}
</script>
