<template>
<div class="tab-content">
        <section class="content-grid content-grid-primary">
          <section class="panel settings-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>功能设置</h2>
                <p class="section-kicker">Configuration</p>
              </div>
            </div>

            <div class="settings-two-col">
              <div class="settings-left">
                <label class="field">
                  <span>Base URL</span>
                  <input v-model="settings.llm_base_url" placeholder="https://api.openai.com/v1" />
                  <p class="field-help">大模型 API 的基础地址，默认为 OpenAI API 地址</p>
                </label>
                <label class="field">
                  <span>API Key</span>
                  <input v-model="settings.llm_api_key" type="password" placeholder="sk-..." />
                  <p class="field-help">用于访问大模型 API 的密钥</p>
                </label>
                <label class="field">
                  <span>模型名</span>
                  <input v-model="settings.llm_model" />
                  <p class="field-help">要使用的大模型名称，如 gpt-4o-mini</p>
                </label>
                <label class="field">
                  <span>妙想密钥</span>
                  <input v-model="settings.mx_api_key" type="password" placeholder="妙想接口 apikey" />
                  <p class="field-help">用于访问东方财富妙想接口的密钥</p>
                </label>
              </div>
              <div class="settings-right">
                <label class="field">
                  <span>系统提示词</span>
                  <textarea v-model="settings.system_prompt" rows="8" />
                  <p class="field-help">指导大模型行为的系统提示词，影响 AI 的决策方式</p>
                </label>
              </div>
            </div>

            <div v-if="errorMessage" class="error-banner">{{ errorMessage }}</div>

            <div class="panel-actions">
              <button class="button primary" :class="{ 'is-loading': busy }" @click="saveSettings" :disabled="busy">保存设置</button>
            </div>
          </section>
        </section>


      </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import { storeToRefs } from 'pinia'
import { useAppStore } from '@/stores/legacy'

const store = useAppStore()
const { settings, busy, errorMessage } = storeToRefs(store)
const { saveSettings } = store

onMounted(async () => {
  try {
    await store.loadSettings()
  } catch (error) {
    errorMessage.value = (error as Error).message
  }
})

</script>
