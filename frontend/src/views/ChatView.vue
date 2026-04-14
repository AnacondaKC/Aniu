<template>
  <div class="tab-content">
    <section class="content-grid content-grid-primary">
      <section class="panel chat-panel">
        <div class="panel-head">
          <div class="head-main">
            <h2>AI聊天</h2>
            <p class="section-kicker">AI Chat</p>
          </div>
        </div>

        <div class="chat-layout">
          <aside class="chat-sidebar">
            <h3>上下文开关</h3>
            <label class="chat-toggle">
              <input v-model="toggles.includeSystemPrompt" type="checkbox" />
              <span>附带系统提示词</span>
            </label>
            <label class="chat-toggle">
              <input v-model="toggles.includeAccountSummary" type="checkbox" />
              <span>附带账户摘要</span>
            </label>
            <label class="chat-toggle">
              <input v-model="toggles.includePositionsOrders" type="checkbox" />
              <span>附带持仓和委托摘要</span>
            </label>
            <label class="chat-toggle">
              <input v-model="toggles.includeLatestRunSummary" type="checkbox" />
              <span>附带最近运行摘要</span>
            </label>

            <button class="button ghost chat-clear-button" @click="clearMessages">清空聊天</button>
          </aside>

          <div class="chat-main">
            <div v-if="errorMessage" class="error-banner">{{ errorMessage }}</div>

            <div v-if="messages.length === 0" class="empty-state chat-empty-state">
              <p>开始和 AI 交流。你可以直接提问，也可以打开上下文开关，让它结合账户和运行信息回答。</p>
            </div>

            <div v-else class="chat-message-list">
              <article
                v-for="(message, index) in messages"
                :key="`${message.role}-${index}`"
                class="chat-message"
                :class="`role-${message.role}`"
              >
                <div class="chat-message-role">{{ message.role === 'user' ? '我' : 'AI' }}</div>
                <div class="chat-message-body">{{ message.content }}</div>
              </article>
            </div>

            <div class="chat-composer">
              <textarea
                v-model="input"
                rows="4"
                placeholder="输入你想和 AI 讨论的内容..."
                @keydown.enter.exact.prevent="sendMessage"
              />
              <div class="chat-composer-actions">
                <span class="chat-tip">按 Enter 发送，Shift + Enter 换行</span>
                <button class="button primary" :class="{ 'is-loading': sending }" :disabled="!canSend" @click="sendMessage">
                  发送
                </button>
              </div>
            </div>
          </div>
        </div>
      </section>
    </section>
  </div>
</template>

<script setup lang="ts">
import { useChatSession } from '@/composables/useChatSession'

const {
  messages,
  input,
  sending,
  errorMessage,
  toggles,
  canSend,
  sendMessage,
  clearMessages,
} = useChatSession()
</script>
