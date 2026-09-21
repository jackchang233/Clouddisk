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

    <!-- 主区: 目录导航 + 文件列表 -->
    <el-main class="home-main">
      <div class="panel-card">
        <!-- 工具栏 -->
        <div class="toolbar">
          <el-button type="primary" :icon="Upload" @click="pickFiles">上传</el-button>
          <el-button :icon="FolderAdd" @click="onCreateDir">新建文件夹</el-button>
          <el-button :icon="Refresh" circle @click="loadDir()" />
        </div>

        <!-- 面包屑: 由后端返回的 path 渲染。
             前端不自己维护路径栈 —— 刷新页面/浏览器前进后退/直接跳转都会对不上 -->
        <el-breadcrumb separator="/" class="breadcrumb">
          <el-breadcrumb-item v-for="(n, i) in path" :key="n.id">
            <a
              class="crumb"
              :class="{ 'crumb-current': i === path.length - 1 }"
              @click="enterDir(n.id, i)"
            >{{ n.name }}</a>
          </el-breadcrumb-item>
        </el-breadcrumb>

        <!-- 上传控件隐藏在按钮后面 -->
        <el-upload
          ref="uploadRef"
          style="display: none"
          multiple
          :show-file-list="false"
          :http-request="doUpload"
        />

        <el-progress
          v-if="uploading"
          class="upload-progress"
          :percentage="uploadPercent"
        />

        <el-table
          :data="entries"
          v-loading="loading"
          style="width: 100%"
          @row-dblclick="onRowDblClick"
        >
          <el-table-column label="文件名" min-width="300">
            <template #default="{ row }">
              <el-icon class="entry-icon" :class="row.IsDir ? 'icon-dir' : 'icon-file'">
                <Folder v-if="row.IsDir" /><Document v-else />
              </el-icon>
              {{ row.Name }}
            </template>
          </el-table-column>
          <el-table-column label="大小" width="110">
            <template #default="{ row }">{{ row.IsDir ? '-' : fmtSize(row.Size) }}</template>
          </el-table-column>
          <el-table-column label="类型" width="110">
            <template #default="{ row }">{{ row.IsDir ? '文件夹' : fileType(row.Name) }}</template>
          </el-table-column>
          <el-table-column prop="UpdatedAt" label="修改时间" width="180" />
          <el-table-column label="操作" width="180">
            <template #default="{ row }">
              <a v-if="!row.IsDir" class="download-link" :href="downloadUrl(row)">
                <el-button type="primary" link :icon="Download">下载</el-button>
              </a>
              <el-button link :icon="EditPen" @click="onRename(row)">重命名</el-button>
              <el-button type="danger" link :icon="Delete" @click="onDelete(row)">删除</el-button>
            </template>
          </el-table-column>
          <template #empty>
            <el-empty description="这个文件夹是空的，拖拽文件到「上传」按钮试试" />
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
import {
  Upload, FolderAdd, Folder, Document, Download, Refresh, SwitchButton,
  Delete, EditPen, RefreshRight
} from '@element-plus/icons-vue'
import api, { unwrap, formBody, errMsg } from '../api'

const router = useRouter()
const userInfo = reactive({ Username: '', SignupAt: '' })

// 当前目录状态。path 由后端返回, 前端只渲染
const currentDirId = ref(0)
const path = ref([{ id: 0, name: '全部文件' }])
const entries = ref([])
const loading = ref(false)

const uploading = ref(false)
const uploadPercent = ref(0)
const uploadRef = ref(null)

const recycleVisible = ref(false)
const recycleFiles = ref([])
const loadingRecycle = ref(false)

const CHUNK_SIZE = 5 * 1024 * 1024

onMounted(() => {
  loadUserInfo()
  loadDir(0)
})

async function loadUserInfo() {
  try {
    const body = unwrap(await api.get('/user/info'))
    userInfo.Username = body.data.Username
    userInfo.SignupAt = body.data.SignupAt
  } catch (err) {
    /* 401 已由拦截器统一处理 */
  }
}

// 加载目录。dirId 省略则刷新当前目录
async function loadDir(dirId) {
  loading.value = true
  try {
    const body = unwrap(await api.post('/dir/list', formBody({
      parent_id: dirId == null ? currentDirId.value : dirId
    })))
    currentDirId.value = dirId == null ? currentDirId.value : dirId
    path.value = body.path || []
    entries.value = body.entries || []
  } catch (err) {
    ElMessage.error(errMsg(err, '目录加载失败'))
  } finally {
    loading.value = false
  }
}

// 点面包屑: 跳到那一层, 丢弃它之后的部分
function enterDir(id, index) {
  if (index === path.value.length - 1) return   // 当前层, 无需重复加载
  path.value = path.value.slice(0, index + 1)
  loadDir(id)
}

// 双击: 目录进入, 文件下载。这是从资源管理器带过来的肌肉记忆
function onRowDblClick(row) {
  if (row.IsDir) loadDir(row.Id)
  else download(row)
}

async function pickFiles() {
  // el-upload 的隐藏 input 通过点击触发
  uploadRef.value?.$el?.querySelector('input')?.click()
}

async function onCreateDir() {
  let name
  try {
    ({ value: name } = await ElMessageBox.prompt('请输入文件夹名称', '新建文件夹', {
      inputPattern: /^[^/\\:*?"<>|]{1,255}$/,
      inputErrorMessage: '名称不能为空, 且不能包含 / \\ : * ? " < > |',
      confirmButtonText: '创建',
      cancelButtonText: '取消'
    }))
  } catch {
    return   // 用户取消
  }
  try {
    await api.post('/dir/create', formBody({ parent_id: currentDirId.value, name }))
    ElMessage.success('创建成功')
    loadDir()
  } catch (err) {
    ElMessage.error(errMsg(err, '创建失败'))
  }
}

async function onRename(row) {
  let name
  try {
    ({ value: name } = await ElMessageBox.prompt('请输入新名称', '重命名', {
      inputValue: row.Name,
      inputPattern: /^[^/\\:*?"<>|]{1,255}$/,
      inputErrorMessage: '名称不能为空, 且不能包含 / \\ : * ? " < > |',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    }))
  } catch {
    return
  }
  if (name === row.Name) return
  try {
    await api.post('/dir/rename', formBody({ id: row.Id, name }))
    ElMessage.success('已重命名')
    loadDir()
  } catch (err) {
    ElMessage.error(errMsg(err, '重命名失败'))
  }
}

async function onDelete(row) {
  const what = row.IsDir ? '文件夹及其全部内容' : '文件'
  try {
    await ElMessageBox.confirm(`确定删除${what}「${row.Name}」吗？`, '删除确认', {
      type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消'
    })
  } catch {
    return
  }
  try {
    await api.post('/file/delete', formBody({ id: row.Id }))
    ElMessage.success('已移入回收站')
    loadDir()
  } catch (err) {
    ElMessage.error(errMsg(err, '删除失败'))
  }
}

// ---------- 上传 ----------
// 采样指纹: 文件名 + 大小 + 首尾各 1MB 的 SHA-256。
// 只读 2MB, 大文件不会卡首屏; 标识由内容决定, 跨设备/刷新后仍能续传同一文件。
async function fileFingerprint(file) {
  if (!window.crypto || !window.crypto.subtle) {
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

// 秒传阈值。超过它就不再尝试秒传, 直接走正常上传。
//
// ⚠️ 原因: Web Crypto 的 `crypto.subtle.digest` **不支持增量哈希** ——
// 要算完整 SHA-256 就必须把整个文件读进内存。50MB 以上时, 读取 + 哈希的
// 耗时和内存占用会超过"直接上传"的代价, 反而更慢。
// 真正的网盘客户端用增量哈希库 (或 WASM) 解决这个问题, 本项目不引入额外依赖。
const INSTANT_MAX = 50 * 1024 * 1024

// 计算完整 SHA-256, 用于秒传判定。只对小文件调用 (见 INSTANT_MAX)
async function fullHash(file) {
  const buf = await file.arrayBuffer()
  const digest = await crypto.subtle.digest('SHA-256', buf)
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('')
}

// 秒传: 先问服务端"这个内容你有没有"。
// 命中则一个字节都不用传, 直接建好节点。
// 服务端只查**当前用户自己**的文件 —— 所以拿到别人的 hash 也没用。
async function tryInstantUpload(file) {
  if (file.size > INSTANT_MAX) return false
  if (!window.crypto || !window.crypto.subtle) return false

  let hashcode
  try {
    hashcode = await fullHash(file)
  } catch {
    return false   // 哈希失败就退回正常上传
  }

  const r = unwrap(await api.post('/file/upload/check', formBody({
    hashcode,
    filename: file.name,
    parent_id: currentDirId.value
  })))
  return r.instant === true
}

async function doChunkedUpload(file) {
  const uploadId = await fileFingerprint(file)

  const init = unwrap(await api.post('/file/upload/init', formBody({
    upload_id: uploadId,
    filename: file.name,
    size: file.size,
    chunk_size: CHUNK_SIZE,
    parent_id: currentDirId.value
  })))
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
    // 先试秒传 —— 命中则一个字节都不用传
    if (await tryInstantUpload(file)) {
      ElMessage.success(`${file.name} 秒传成功`)
      loadDir()
      return
    }
    if (file.size > CHUNK_SIZE) {
      await doChunkedUpload(file)
    } else {
      const fd = new FormData()
      fd.append('parent_id', currentDirId.value)
      fd.append('file', file)
      await api.post('/file/upload', fd, {
        onUploadProgress: (e) => {
          if (e.total) uploadPercent.value = Math.round((e.loaded / e.total) * 100)
        }
      })
    }
    ElMessage.success(`${file.name} 上传成功`)
    loadDir()
  } catch (err) {
    ElMessage.error(errMsg(err, `${file.name} 上传失败`))
  } finally {
    uploading.value = false
    uploadPercent.value = 0
  }
}

// 下载不走 AJAX: 带 query 的普通链接, 浏览器按 Content-Disposition 落盘。
// 用**节点 id** 定位而不是文件名 —— 有了目录之后不同目录下可以有同名文件,
// 只传文件名会给后端造成歧义。
function downloadUrl(row) {
  const p = new URLSearchParams({
    id: row.Id,
    token: localStorage.getItem('token') || ''
  })
  return '/file/download?' + p.toString()
}

function download(row) {
  window.location.href = downloadUrl(row)
}

// ---------- 回收站 ----------
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
    loadDir()
  } catch (err) {
    ElMessage.error(errMsg(err, '恢复失败'))
  }
}

async function onPurge(row) {
  try {
    await ElMessageBox.confirm(
      `彻底删除「${row.FileName}」后将无法恢复，确定吗？`, '彻底删除确认',
      { type: 'warning', confirmButtonText: '彻底删除', cancelButtonText: '取消' }
    )
  } catch {
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

// ---------- 工具 ----------
function fmtSize(n) {
  if (n == null) return '--'
  if (n < 1024) return n + ' B'
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB'
  if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' MB'
  return (n / 1024 / 1024 / 1024).toFixed(1) + ' GB'
}

function fileType(name) {
  const ext = (name.split('.').pop() || '').toLowerCase()
  if (!ext || ext === name.toLowerCase()) return '文件'
  return ext.toUpperCase()
}

function onLogout() {
  localStorage.removeItem('token')
  localStorage.removeItem('username')
  router.push('/login')
}
</script>

<style scoped>
.toolbar {
  display: flex;
  gap: 10px;
  align-items: center;
  margin-bottom: 14px;
}
.breadcrumb {
  margin-bottom: 16px;
  font-size: 14px;
}
.crumb {
  cursor: pointer;
  color: #4f6ef7;
}
.crumb:hover { text-decoration: underline; }
/* 当前层不可点, 视觉上也弱化掉可点的暗示 */
.crumb-current {
  color: #303133;
  cursor: default;
  font-weight: 600;
}
.crumb-current:hover { text-decoration: none; }

.entry-icon {
  vertical-align: -2px;
  margin-right: 6px;
}
.icon-dir { color: #ffb800; }
.icon-file { color: #909399; }

.upload-progress { margin-bottom: 14px; }
</style>
