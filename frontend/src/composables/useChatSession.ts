import { computed, ref, watch } from 'vue'

import { api } from '@/services/api'
import type { ChatMessage } from '@/types'

const CHAT_STORAGE_KEY = 'aniu-chat-history'
const CHAT_TOGGLE_KEY = 'aniu-chat-toggles'
const MAX_PERSISTED_MESSAGES = 40

type ChatToggles = {
  includeSystemPrompt: boolean
  includeAccountSummary: boolean
  includePositionsOrders: boolean
  includeLatestRunSummary: boolean
}

function loadMessages(): ChatMessage[] {
  try {
    const raw = localStorage.getItem(CHAT_STORAGE_KEY)
    return raw ? JSON.parse(raw) : []
  } catch {
    return []
  }
}

function loadToggles(): ChatToggles {
  try {
    const raw = localStorage.getItem(CHAT_TOGGLE_KEY)
    if (raw) {
      return JSON.parse(raw)
    }
  } catch {
    // ignore parse failures
  }

  return {
    includeSystemPrompt: true,
    includeAccountSummary: false,
    includePositionsOrders: false,
    includeLatestRunSummary: false,
  }
}

export function useChatSession() {
  const messages = ref<ChatMessage[]>(loadMessages())
  const input = ref('')
  const sending = ref(false)
  const errorMessage = ref('')
  const toggles = ref<ChatToggles>(loadToggles())

  watch(messages, (value) => {
    try {
      const trimmed = value.slice(-MAX_PERSISTED_MESSAGES)
      if (trimmed.length !== value.length) {
        messages.value = trimmed
        return
      }
      localStorage.setItem(CHAT_STORAGE_KEY, JSON.stringify(trimmed))
    } catch {
      // Ignore storage quota or privacy mode failures.
    }
  }, { deep: true })

  watch(toggles, (value) => {
    try {
      localStorage.setItem(CHAT_TOGGLE_KEY, JSON.stringify(value))
    } catch {
      // Ignore storage quota or privacy mode failures.
    }
  }, { deep: true })

  const canSend = computed(() => input.value.trim().length > 0 && !sending.value)

  async function sendMessage() {
    const content = input.value.trim()
    if (!content || sending.value) return

    errorMessage.value = ''
    sending.value = true
    const nextMessages = [...messages.value, { role: 'user', content } as ChatMessage]
    messages.value = nextMessages
    input.value = ''

    try {
      const response = await api.chat({
        messages: nextMessages,
        include_system_prompt: toggles.value.includeSystemPrompt,
        include_account_summary: toggles.value.includeAccountSummary,
        include_positions_orders: toggles.value.includePositionsOrders,
        include_latest_run_summary: toggles.value.includeLatestRunSummary,
      })
      messages.value = [...messages.value, response.message]
    } catch (error) {
      errorMessage.value = (error as Error).message
      messages.value = messages.value.slice(0, -1)
      input.value = content
    } finally {
      sending.value = false
    }
  }

  function clearMessages() {
    messages.value = []
    errorMessage.value = ''
  }

  return {
    messages,
    input,
    sending,
    errorMessage,
    toggles,
    canSend,
    sendMessage,
    clearMessages,
  }
}
