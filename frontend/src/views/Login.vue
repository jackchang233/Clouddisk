<template>
  <div class="auth-page">
    <div class="auth-card">
      <div class="auth-logo">
        <el-icon :size="46" color="#4f6ef7"><Cloudy /></el-icon>
      </div>
      <h1 class="auth-title">CloudDisk 云盘</h1>
      <p class="auth-sub">登录你的账号，继续管理云端文件</p>

      <el-form ref="formRef" :model="form" :rules="rules" size="large" @keyup.enter="onSubmit">
        <el-form-item prop="username">
          <el-input v-model="form.username" placeholder="用户名" :prefix-icon="User" clearable />
        </el-form-item>
        <el-form-item prop="password">
          <el-input v-model="form.password" type="password" placeholder="密码" :prefix-icon="Lock" show-password />
        </el-form-item>
        <el-button class="auth-btn" type="primary" :loading="loading" @click="onSubmit">登 录</el-button>
      </el-form>

      <div class="auth-switch">
        还没有账号？
        <router-link to="/register">立即注册</router-link>
      </div>
    </div>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { User, Lock, Cloudy } from '@element-plus/icons-vue'
import api, { unwrap, formBody, errMsg } from '../api'

const router = useRouter()
const formRef = ref()
const loading = ref(false)
const form = reactive({ username: '', password: '' })

const rules = {
  username: [{ required: true, message: '请输入用户名', trigger: 'blur' }],
  password: [{ required: true, message: '请输入密码', trigger: 'blur' }]
}

async function onSubmit() {
  await formRef.value.validate()
  loading.value = true
  try {
    const body = unwrap(await api.post('/user/signin', formBody(form)))
    localStorage.setItem('token', body.data.Token)
    localStorage.setItem('username', body.data.Username)
    ElMessage.success(`欢迎回来，${body.data.Username}`)
    router.push('/home')
  } catch (err) {
    ElMessage.error(errMsg(err))
  } finally {
    loading.value = false
  }
}
</script>
