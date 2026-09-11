<template>
  <div class="auth-page">
    <div class="auth-card">
      <div class="auth-logo">
        <el-icon :size="46" color="#4f6ef7"><Cloudy /></el-icon>
      </div>
      <h1 class="auth-title">创建账号</h1>
      <p class="auth-sub">注册一个新账号，开始上传你的文件</p>

      <el-form ref="formRef" :model="form" :rules="rules" size="large" @keyup.enter="onSubmit">
        <el-form-item prop="username">
          <el-input v-model="form.username" placeholder="用户名（3-32位，字母/数字/下划线）" :prefix-icon="User" clearable />
        </el-form-item>
        <el-form-item prop="password">
          <el-input v-model="form.password" type="password" placeholder="密码（至少6位，含字母和数字）" :prefix-icon="Lock" show-password />
        </el-form-item>
        <el-form-item prop="confirm">
          <el-input v-model="form.confirm" type="password" placeholder="确认密码" :prefix-icon="Lock" show-password />
        </el-form-item>
        <el-button class="auth-btn" type="primary" :loading="loading" @click="onSubmit">注 册</el-button>
      </el-form>

      <div class="auth-switch">
        已有账号？
        <router-link to="/login">直接登录</router-link>
      </div>
    </div>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { User, Lock, Cloudy } from '@element-plus/icons-vue'
import api, { errMsg } from '../api'

const router = useRouter()
const formRef = ref()
const loading = ref(false)
const form = reactive({ username: '', password: '', confirm: '' })

const rules = {
  username: [
    { required: true, message: '请输入用户名', trigger: 'blur' },
    { pattern: /^[A-Za-z0-9_]{3,32}$/, message: '3-32 位，仅字母/数字/下划线', trigger: 'blur' }
  ],
  password: [
    { required: true, message: '请输入密码', trigger: 'blur' },
    { min: 6, message: '密码至少 6 位', trigger: 'blur' },
    { pattern: /^(?=.*[A-Za-z])(?=.*\d).+$/, message: '须同时包含字母和数字', trigger: 'blur' }
  ],
  confirm: [
    { required: true, message: '请再次输入密码', trigger: 'blur' },
    {
      validator: (_rule, value, cb) =>
        value === form.password ? cb() : cb(new Error('两次输入的密码不一致')),
      trigger: 'blur'
    }
  ]
}

async function onSubmit() {
  await formRef.value.validate()
  loading.value = true
  try {
    await api.post('/user/signup', new URLSearchParams({
      username: form.username,
      password: form.password
    }))
    ElMessage.success('注册成功，请登录')
    router.push('/login')
  } catch (err) {
    // 后端插入冲突 (用户名已存在) 走 500
    ElMessage.error(err.response && err.response.status >= 500
      ? '注册失败，用户名可能已被占用'
      : errMsg(err))
  } finally {
    loading.value = false
  }
}
</script>
