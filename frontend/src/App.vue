<template>
  <div class="page-shell">
    <header v-if="!isLoginPage" class="app-header">
      <div class="app-brand">
        <img class="app-brand-logo" src="/aniu.ico" alt="Aniu logo" />
        <div class="app-brand-copy">
          <strong>Aniu</strong>
          <span>AI模拟交易</span>
        </div>
      </div>

      <div class="app-header-actions">
        <button class="header-action-button header-action-link" type="button">
          <span class="header-action-icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 .5C5.65.5.5 5.65.5 12a11.5 11.5 0 0 0 7.86 10.92c.58.1.79-.25.79-.56v-2.16c-3.2.7-3.88-1.36-3.88-1.36-.52-1.33-1.28-1.68-1.28-1.68-1.04-.71.08-.7.08-.7 1.15.08 1.76 1.18 1.76 1.18 1.02 1.75 2.67 1.24 3.32.95.1-.74.4-1.24.72-1.53-2.56-.29-5.24-1.28-5.24-5.7 0-1.26.45-2.28 1.18-3.08-.12-.29-.51-1.47.11-3.05 0 0 .97-.31 3.17 1.18a11.02 11.02 0 0 1 5.78 0c2.19-1.49 3.16-1.18 3.16-1.18.63 1.58.24 2.76.12 3.05.74.8 1.18 1.82 1.18 3.08 0 4.43-2.69 5.41-5.25 5.69.41.35.78 1.03.78 2.08v3.08c0 .31.21.67.8.56A11.5 11.5 0 0 0 23.5 12C23.5 5.65 18.35.5 12 .5Z" />
            </svg>
          </span>
          <span>Github</span>
        </button>

        <button class="header-action-button" type="button" @click="handleLogout">退出</button>
      </div>
    </header>

    <div v-if="errorMessage && !isLoginPage" class="hero-error app-shell-error">
      {{ errorMessage }}
    </div>

    <div v-if="!isLoginPage" class="tabs-container">
      <nav class="app-header-nav">
        <router-link
          v-for="tab in tabs"
          :key="tab.id"
          :to="'/' + tab.id"
          class="tab-button"
          active-class="active"
        >
          {{ tab.name }}
        </router-link>
      </nav>
    </div>

    <main class="stack-layout">
      <router-view></router-view>
    </main>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useAppStore } from '@/stores/legacy'
import { clearStoredLoginFlag, clearStoredToken } from '@/services/api'

const store = useAppStore()
const router = useRouter()
const route = useRoute()
const { errorMessage, tabs } = storeToRefs(store)

const isLoginPage = computed(() => route.path === '/login')

function handleLogout() {
  clearStoredToken()
  clearStoredLoginFlag()
  store.resetState()
  router.replace('/login')
}
</script>
