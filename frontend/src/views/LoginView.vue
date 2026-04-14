<template>
  <div class="login-page">
    <section class="login-card">
      <div class="login-avatar-shell">
        <img class="login-avatar" src="/aniu.ico" alt="Aniu avatar" />
      </div>

      <div class="login-copy">
        <h1>Aniu</h1>
        <p>登录 AI 模拟交易系统</p>
      </div>

      <form class="login-form" @submit.prevent="handleSubmit">
        <label class="field">
          <span>用户名</span>
          <input v-model="username" type="text" placeholder="请输入用户名" autocomplete="username" />
        </label>

        <label class="field">
          <span>密码</span>
          <input v-model="password" type="password" placeholder="请输入密码" autocomplete="current-password" />
        </label>

        <label class="login-remember-row">
          <input v-model="rememberCredentials" type="checkbox" />
          <span>默认记住账号密码</span>
        </label>

        <p v-if="errorMessage" class="login-error">{{ errorMessage }}</p>

        <button class="button primary login-submit" :disabled="submitting" type="submit">登录</button>
      </form>
    </section>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'

import {
  api,
  clearStoredLoginFlag,
  clearStoredToken,
  getStoredLoginFlag,
  getStoredToken,
  setStoredLoginFlag,
  setStoredToken,
} from '@/services/api'
import { useAppStore } from '@/stores/legacy'
import {
  REMEMBERED_PASSWORD_STORAGE_KEY,
  REMEMBERED_USERNAME_STORAGE_KEY,
} from '@/constants'

const store = useAppStore()
const router = useRouter()
const username = ref('')
const password = ref('')
const rememberCredentials = ref(true)
const errorMessage = ref('')
const submitting = ref(false)

onMounted(() => {
  username.value = window.localStorage.getItem(REMEMBERED_USERNAME_STORAGE_KEY) ?? ''
  password.value = window.localStorage.getItem(REMEMBERED_PASSWORD_STORAGE_KEY) ?? ''

  if (getStoredLoginFlag() && getStoredToken()) {
    router.replace('/overview')
  }
})

async function handleSubmit() {
  if (!username.value.trim() || !password.value.trim()) {
    errorMessage.value = '请输入用户名和密码。'
    return
  }

  submitting.value = true
  try {
    const response = await api.login({
      username: username.value.trim(),
      password: password.value,
    })
    if (!response.authenticated || !response.token) {
      throw new Error('登录失败，请检查用户名和密码。')
    }
    setStoredToken(response.token)
    setStoredLoginFlag(response.authenticated)
    if (rememberCredentials.value) {
      window.localStorage.setItem(REMEMBERED_USERNAME_STORAGE_KEY, username.value.trim())
      window.localStorage.setItem(REMEMBERED_PASSWORD_STORAGE_KEY, password.value)
    }
    await store.initialize()
    errorMessage.value = ''
    router.replace('/overview')
  } catch (error) {
    clearStoredToken()
    clearStoredLoginFlag()
    errorMessage.value = (error as Error).message
  } finally {
    submitting.value = false
  }
}
</script>
