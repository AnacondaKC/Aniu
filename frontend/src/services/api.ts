import type { AccountOverview, AppSettings, ChatRequest, ChatResponse, LoginRequest, LoginResponse, RunDetail, RunSummary, RunSummaryPage, RuntimeOverview, ScheduleConfig } from '../types'
import { LOGIN_STORAGE_KEY, TOKEN_STORAGE_KEY } from '@/constants'

const API_PREFIX = '/api/aniu'
const DEFAULT_TIMEOUT_MS = 20000

function readStorageItem(key: string): string | null {
  return window.localStorage.getItem(key)
}

function writeStorageItem(key: string, value: string): void {
  window.localStorage.setItem(key, value)
}

function removeStorageItem(key: string): void {
  window.localStorage.removeItem(key)
}

export function getStoredToken(): string | null {
  return readStorageItem(TOKEN_STORAGE_KEY)
}

export function setStoredToken(token: string): void {
  writeStorageItem(TOKEN_STORAGE_KEY, token)
}

export function clearStoredToken(): void {
  removeStorageItem(TOKEN_STORAGE_KEY)
}

export function getStoredLoginFlag(): boolean {
  return readStorageItem(LOGIN_STORAGE_KEY) === 'true'
}

export function setStoredLoginFlag(authenticated: boolean): void {
  writeStorageItem(LOGIN_STORAGE_KEY, String(authenticated))
}

export function clearStoredLoginFlag(): void {
  removeStorageItem(LOGIN_STORAGE_KEY)
}

interface RequestOptions extends RequestInit {
  timeoutMs?: number
}

interface ListRunsOptions {
  limit?: number
  date?: string
  status?: string
  beforeId?: number
}

async function request<T>(url: string, options?: RequestOptions): Promise<T> {
  const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const controller = new AbortController()
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs)

  const token = getStoredToken()
  const authHeaders: Record<string, string> = token
    ? { Authorization: `Bearer ${token}` }
    : {}

  const { timeoutMs: _ignored, ...fetchOptions } = options ?? {}

  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...authHeaders,
      ...(fetchOptions?.headers ?? {}),
    },
    ...fetchOptions,
    signal: fetchOptions?.signal ?? controller.signal,
  }).catch((error: unknown) => {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new Error('请求超时，请稍后重试。')
    }
    throw error
  }).finally(() => {
    window.clearTimeout(timeoutId)
  })

  if (response.status === 401) {
    clearStoredToken()
    clearStoredLoginFlag()
    if (window.location.pathname !== '/login') {
      window.location.href = '/login'
    }
    throw new Error('认证已过期，请重新登录。')
  }

  if (!response.ok) {
    let message = `请求失败: ${response.status}`
    try {
      const payload = await response.json()
      if (response.status === 422 && Array.isArray(payload.detail)) {
        const fields = payload.detail
          .map((err: { loc?: string[]; msg?: string }) => {
            const field = (err.loc ?? []).filter((s: string) => s !== 'body').join('.')
            return field ? `${field}: ${err.msg ?? '验证失败'}` : (err.msg ?? '验证失败')
          })
          .join('; ')
        message = fields || '请求参数验证失败'
      } else {
        message = payload.detail ?? payload.message ?? message
      }
    } catch {
      // ignore json parse failures
    }
    throw new Error(message)
  }

  if (response.status === 204) {
    return undefined as T
  }

  const contentType = response.headers.get('content-type') ?? ''
  if (!contentType.includes('application/json')) {
    return undefined as T
  }

  const text = await response.text()
  if (!text.trim()) {
    return undefined as T
  }

  return JSON.parse(text) as T
}

export const api = {
  login(payload: LoginRequest) {
    return request<LoginResponse>(`${API_PREFIX}/login`, {
      method: 'POST',
      body: JSON.stringify(payload),
    })
  },
  getSettings() {
    return request<AppSettings>(`${API_PREFIX}/settings`)
  },
  updateSettings(payload: Omit<AppSettings, 'id' | 'created_at' | 'updated_at'>) {
    return request<AppSettings>(`${API_PREFIX}/settings`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    })
  },
  getSchedule() {
    return request<ScheduleConfig[]>(`${API_PREFIX}/schedule`)
  },
  updateSchedule(payload: Array<Partial<ScheduleConfig>>) {
    return request<ScheduleConfig[]>(`${API_PREFIX}/schedule`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    })
  },
  runNow(scheduleId?: number) {
    const suffix = typeof scheduleId === 'number' ? `?schedule_id=${scheduleId}` : ''
    return request<RunDetail>(`${API_PREFIX}/run${suffix}`, {
      method: 'POST',
      timeoutMs: 10 * 60 * 1000,
    })
  },
  listRuns(options: number | ListRunsOptions = 20) {
    const config = typeof options === 'number' ? { limit: options } : options
    const params = new URLSearchParams()
    params.set('limit', String(config.limit ?? 20))
    if (config.date) {
      params.set('date', config.date)
    }
    if (config.status) {
      params.set('status', config.status)
    }
    if (typeof config.beforeId === 'number') {
      params.set('before_id', String(config.beforeId))
    }
    return request<RunSummary[]>(`${API_PREFIX}/runs?${params.toString()}`)
  },
  listRunsPage(options: ListRunsOptions = {}) {
    const params = new URLSearchParams()
    params.set('limit', String(options.limit ?? 20))
    if (options.date) {
      params.set('date', options.date)
    }
    if (options.status) {
      params.set('status', options.status)
    }
    if (typeof options.beforeId === 'number') {
      params.set('before_id', String(options.beforeId))
    }
    return request<RunSummaryPage>(`${API_PREFIX}/runs-feed?${params.toString()}`)
  },
  getRun(runId: number) {
    return request<RunDetail>(`${API_PREFIX}/runs/${runId}`)
  },
  getRuntimeOverview() {
    return request<RuntimeOverview>(`${API_PREFIX}/runtime-overview`)
  },
  getAccount(forceRefresh = false) {
    const params = new URLSearchParams()
    if (forceRefresh) {
      params.set('force_refresh', 'true')
    }
    const suffix = params.size > 0 ? `?${params.toString()}` : ''
    return request<AccountOverview>(`${API_PREFIX}/account${suffix}`, {
      timeoutMs: 60000,
    })
  },
  chat(payload: ChatRequest) {
    return request<ChatResponse>(`${API_PREFIX}/chat`, {
      method: 'POST',
      body: JSON.stringify(payload),
      timeoutMs: 3 * 60 * 1000,
    })
  },
}
