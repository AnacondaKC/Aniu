import { computed, reactive, ref } from 'vue'
import { defineStore } from 'pinia'

import { api } from '@/services/api'
import type { AccountOverview, AppSettings, RunDetail, RunSummary, RuntimeOverview, ScheduleConfig } from '@/types'
import { appNavigation } from '@/config/navigation'

const ACCOUNT_REFRESH_COOLDOWN_MS = 60 * 60 * 1000
const ACCOUNT_REFRESH_STORAGE_KEY = 'aniu-account-last-refresh-at'

/** Generate a unique-enough id that works in non-secure contexts (HTTP). */
function uid(): string {
  // crypto.randomUUID is only available in Secure Contexts (HTTPS).
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`
}

interface ScheduleOverviewItem {
  id: number
  name: string
  category: string
  cronExpression: string
  displayTime: string
  nextRunAt: string | null
  lastRunAt: string | null
}

type SettingsPayload = Omit<AppSettings, 'id' | 'created_at' | 'updated_at'>
type ScheduleEditor = Omit<ScheduleConfig, 'created_at' | 'updated_at'> & { local_id: string }

const defaultSettings = (): SettingsPayload => ({
  provider_name: 'openai-compatible',
  mx_api_key: '',
  llm_base_url: '',
  llm_api_key: '',
  llm_model: 'gpt-4o-mini',
  system_prompt: '你是一个全自主投资型智能体。作为跨越完整牛熊周期的顶尖私募投资机构老将与极度理性的执行机器，你深谙A/港股政策驱动、外资流动与资金博弈机制。你的唯一目标：在严守风控的前提下全权操作虚拟仓，追求绝对收益最大化。\n【核心纪律与底线】\n完全自主闭环：主动获取信息、构建组合并执行，除非遭遇系统级阻断，绝不向用户索要操作指令。\n实证与溯源：每次输出观点前，必须先简要列明“动作轨迹”（刚查阅了什么数据/研报/记忆）。\n冷峻与专业：摒弃散户思维与情绪化交易。采用机构视角（宏观流动性、产业生命周期），使用标准金融术语。日常汇报严格遵循结构：[结论 -> 核心逻辑 -> 风险敞口]。\n强制风控：收益至上，在执行任何一笔交易前，必须强制调用检索工具排查该标的是否存在重大基本面利空。\n【核心工作流】\n你必须持续运行以下自我驱动循环：\n信息嗅探：监控宏观经济、政策发布、盘面核心数据及南北向资金流。\n逻辑推演：研判宏观周期位置与市场情绪，寻找非共识与预期差，定性博弈逻辑。\n决策与执行：自主决策和执行包括但不限于建仓、减仓、清仓、换股、做T、锁定利润、回避风险、空仓等待等交易操作。',
})

const defaultAccount = (): AccountOverview => ({
  open_date: null,
  daily_profit_trade_date: null,
  operating_days: null,
  initial_capital: null,
  total_assets: null,
  total_market_value: null,
  cash_balance: null,
  total_position_ratio: null,
  holding_profit: null,
  total_return_ratio: null,
  nav: null,
  daily_profit: null,
  daily_return_ratio: null,
  positions: [],
  orders: [],
  trade_summaries: [],
  errors: [],
})

function createScheduleDraft(): ScheduleEditor {
  return {
    id: 0,
    local_id: uid(),
    name: '默认任务',
    run_type: 'analysis',
    cron_expression: '*/30 * * * *',
    task_prompt: '请根据当前市场和持仓情况生成交易决策。',
    timeout_seconds: 1800,
    enabled: false,
    last_run_at: null,
    next_run_at: null,
  }
}

function readLastAccountRefreshAt() {
  if (typeof window === 'undefined') {
    return 0
  }

  const raw = window.localStorage.getItem(ACCOUNT_REFRESH_STORAGE_KEY)
  const numeric = Number(raw)
  return Number.isFinite(numeric) && numeric > 0 ? numeric : 0
}

function persistLastAccountRefreshAt(value: number) {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.setItem(ACCOUNT_REFRESH_STORAGE_KEY, String(value))
}

function formatCooldownDuration(ms: number) {
  const totalMinutes = Math.ceil(Math.max(ms, 0) / (60 * 1000))
  const hours = Math.floor(totalMinutes / 60)
  const minutes = totalMinutes % 60
  if (hours > 0 && minutes > 0) {
    return `${hours}小时${minutes}分钟`
  }
  if (hours > 0) {
    return `${hours}小时`
  }
  return `${Math.max(totalMinutes, 1)}分钟`
}

function defaultRuntimeOverview(): RuntimeOverview {
  return {
    last_run: {
      start_time: '--',
      end_time: '--',
      status: 'idle',
      status_text: '暂无记录',
      duration: '--',
      input_tokens: '--',
      output_tokens: '--',
      total_tokens: '--',
    },
    today: {
      analysis_count: 0,
      api_calls: 0,
      trades: 0,
      success_rate: 0,
      input_tokens: '--',
      output_tokens: '--',
      total_tokens: '--',
    },
    recent_3_days: {
      analysis_count: 0,
      api_calls: 0,
      trades: 0,
      success_rate: 0,
      input_tokens: '--',
      output_tokens: '--',
      total_tokens: '--',
    },
    recent_7_days: {
      analysis_count: 0,
      api_calls: 0,
      trades: 0,
      success_rate: 0,
      input_tokens: '--',
      output_tokens: '--',
      total_tokens: '--',
    },
  }
}

export const useAppStore = defineStore('app', () => {
  const settings = reactive<SettingsPayload>(defaultSettings())
  const schedules = ref<ScheduleEditor[]>([])
  const account = ref<AccountOverview>(defaultAccount())
  const runSummaries = ref<RunSummary[]>([])
  const latestRun = ref<RunSummary | RunDetail | null>(null)
  const runtimeOverview = ref<RuntimeOverview>(defaultRuntimeOverview())
  const runDetailsMap = ref<Record<number, RunDetail>>({})
  const accountLastManualRefreshAt = ref(readLastAccountRefreshAt())
  const accountRefreshTick = ref(Date.now())

  const busy = ref(false)
  const accountRefreshing = ref(false)
  const notice = ref('')
  const errorMessage = ref('')
  const initialized = ref(false)
  const tabs = ref(appNavigation)

  const enabledTaskCount = computed(() => schedules.value.filter((task) => task.enabled).length)
  const accountPositionCount = computed(() => account.value.positions.length)
  const activeScheduleCards = computed<ScheduleOverviewItem[]>(() => {
    const items = schedules.value
      .filter((item) => item.enabled)
      .slice()
      .map((item) => {
        const parts = (item.cron_expression || '').trim().split(/\s+/)
        const minute = Number(parts[0]) || 0
        const hour = Number(parts[1]) || 0
        const displayTime = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`
        const sortKey = hour * 60 + minute
        const displayName = item.name.replace(/#(\d+)$/, '$1号')
        const category = item.run_type === 'trade' ? '交易任务' : '分析任务'
        return {
          id: item.id,
          name: displayName,
          category,
          cronExpression: item.cron_expression,
          displayTime,
          nextRunAt: item.next_run_at,
          lastRunAt: item.last_run_at,
          sortKey,
        }
      })
    items.sort((a, b) => a.sortKey - b.sortKey)
    return items.map(({ sortKey, ...rest }) => rest)
  })
  const nextScheduledTask = computed(() => {
    const cards = activeScheduleCards.value.filter((card) => !!card.nextRunAt)
    if (cards.length === 0) return null
    const sorted = [...cards].sort((a, b) => (a.nextRunAt ?? '').localeCompare(b.nextRunAt ?? ''))
    return sorted[0]
  })
  const accountRefreshRemainingMs = computed(() => {
    void accountRefreshTick.value
    if (!accountLastManualRefreshAt.value) {
      return 0
    }
    const elapsed = Date.now() - accountLastManualRefreshAt.value
    return elapsed >= ACCOUNT_REFRESH_COOLDOWN_MS ? 0 : ACCOUNT_REFRESH_COOLDOWN_MS - elapsed
  })
  const canManualRefreshAccount = computed(() => accountRefreshRemainingMs.value <= 0)
  const accountRefreshCooldownText = computed(() => formatCooldownDuration(accountRefreshRemainingMs.value))

  function applySettings(payload: AppSettings) {
    settings.provider_name = payload.provider_name
    settings.mx_api_key = payload.mx_api_key ?? ''
    settings.llm_base_url = payload.llm_base_url ?? ''
    settings.llm_api_key = payload.llm_api_key ?? ''
    settings.llm_model = payload.llm_model
    settings.system_prompt = payload.system_prompt
  }

  function applySchedules(payload: ScheduleConfig[]) {
    schedules.value = payload.length
      ? payload.map((item) => ({ ...item, local_id: uid() }))
      : [createScheduleDraft()]
  }

  async function loadSettings() {
    applySettings(await api.getSettings())
  }

  async function loadSchedule() {
    applySchedules(await api.getSchedule())
  }

  async function refreshRunSummaries(options: number | { limit?: number, date?: string, status?: string, beforeId?: number } = 50) {
    runSummaries.value = await api.listRuns(options)
    return runSummaries.value
  }

  async function loadRunDetail(runId: number) {
    if (runDetailsMap.value[runId]) {
      return runDetailsMap.value[runId]
    }
    const detail = await api.getRun(runId)
    runDetailsMap.value = {
      ...runDetailsMap.value,
      [runId]: detail,
    }
    return detail
  }

  async function refreshLatestRun() {
    await refreshRunSummaries(50)
    latestRun.value = runSummaries.value[0] ?? null
  }

  async function refreshRuntimeOverview() {
    runtimeOverview.value = await api.getRuntimeOverview()
  }

  async function refreshAccountData() {
    account.value = await api.getAccount(false)
  }

  async function refreshAccountDataWithCooldown() {
    if (!canManualRefreshAccount.value) {
      throw new Error(`账户信息每 1 小时只能手动刷新一次，请在 ${accountRefreshCooldownText.value}后重试。`)
    }

    accountRefreshing.value = true
    errorMessage.value = ''

    try {
      account.value = await api.getAccount(true)
      const now = Date.now()
      accountLastManualRefreshAt.value = now
      persistLastAccountRefreshAt(now)
      notice.value = '账户信息已刷新。'
    } catch (error) {
      errorMessage.value = (error as Error).message
      throw error
    } finally {
      accountRefreshing.value = false
    }
  }

  async function refreshAll() {
    busy.value = true
    errorMessage.value = ''

    try {
      const results = await Promise.allSettled([
        loadSettings(),
        loadSchedule(),
        refreshAccountData(),
        refreshLatestRun(),
      ])
      const errors = results
        .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
        .map((result) => result.reason instanceof Error ? result.reason.message : '刷新失败')

      if (errors.length === 0) {
        notice.value = '已刷新账户、任务与系统设置。'
      } else {
        errorMessage.value = errors[0]
      }
    } finally {
      busy.value = false
    }
  }

  function resetState() {
    Object.assign(settings, defaultSettings())
    schedules.value = []
    account.value = defaultAccount()
    runSummaries.value = []
    latestRun.value = null
    runtimeOverview.value = defaultRuntimeOverview()
    runDetailsMap.value = {}
    accountLastManualRefreshAt.value = readLastAccountRefreshAt()
    accountRefreshTick.value = Date.now()
    busy.value = false
    accountRefreshing.value = false
    notice.value = ''
    errorMessage.value = ''
    initialized.value = false
  }

  async function saveSettings() {
    busy.value = true
    errorMessage.value = ''

    try {
      const payload = await api.updateSettings({
        ...settings,
        mx_api_key: settings.mx_api_key || null,
        llm_base_url: settings.llm_base_url || null,
        llm_api_key: settings.llm_api_key || null,
      })
      applySettings(payload)
      notice.value = '系统设置已保存。'
    } catch (error) {
      errorMessage.value = (error as Error).message
    } finally {
      busy.value = false
    }
  }

  async function saveSchedule(schedulePayload?: Array<Partial<ScheduleConfig>>) {
    busy.value = true
    errorMessage.value = ''

    try {
      const payload = await api.updateSchedule(
        schedulePayload ?? schedules.value.map(({ local_id, ...item }) => item),
      )
      applySchedules(payload)
      await loadSchedule()
      notice.value = '定时任务已保存。'
    } catch (error) {
      errorMessage.value = (error as Error).message
    } finally {
      busy.value = false
    }
  }

  async function runNow(scheduleId?: number) {
    busy.value = true
    errorMessage.value = ''

    try {
      const run = await api.runNow(scheduleId)
      latestRun.value = run
      notice.value = `任务运行完成：#${run.id} ${run.status}`
      await Promise.all([refreshAccountData(), refreshLatestRun(), refreshRuntimeOverview(), loadSchedule()])
      return run
    } catch (error) {
      errorMessage.value = (error as Error).message
      throw error
    } finally {
      busy.value = false
    }
  }

  async function initialize() {
    if (initialized.value) {
      return
    }

    initialized.value = true
  }

  function touchAccountRefreshTick() {
    accountRefreshTick.value = Date.now()
  }

  return {
    settings,
    schedules,
    account,
    runSummaries,
    latestRun,
    busy,
    notice,
    errorMessage,
    tabs,
    enabledTaskCount,
    accountPositionCount,
    activeScheduleCards,
    nextScheduledTask,
    runtimeOverview,
    accountRefreshing,
    canManualRefreshAccount,
    accountRefreshCooldownText,
    applySettings,
    applySchedules,
    loadSettings,
    loadSchedule,
    refreshRunSummaries,
    loadRunDetail,
    refreshLatestRun,
    refreshRuntimeOverview,
    refreshAccountData,
    refreshAccountDataWithCooldown,
    refreshAll,
    touchAccountRefreshTick,
    saveSettings,
    saveSchedule,
    runNow,
    initialize,
    resetState,
  }
})
