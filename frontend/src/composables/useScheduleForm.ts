import { reactive, ref } from 'vue'

import type { ScheduleConfig } from '@/types'

type ScheduleLike = Pick<ScheduleConfig, 'id' | 'name' | 'run_type' | 'cron_expression' | 'task_prompt' | 'timeout_seconds' | 'enabled'>

type ScheduleKey = 'preMarket' | 'midday' | 'postMarket'
type SessionKey = 'morning' | 'afternoon'

type FixedTaskTimeOption = {
  hour: number
  minute: number
  label: string
}

export const FIXED_TASK_TIME_OPTIONS = {
  preMarket: {
    options: [
      { hour: 8, minute: 0, label: '08:00' },
      { hour: 8, minute: 15, label: '08:15' },
      { hour: 8, minute: 30, label: '08:30' },
      { hour: 8, minute: 45, label: '08:45' },
    ] as FixedTaskTimeOption[],
  },
  midday: {
    options: [
      { hour: 12, minute: 0, label: '12:00' },
      { hour: 12, minute: 15, label: '12:15' },
      { hour: 12, minute: 30, label: '12:30' },
      { hour: 12, minute: 45, label: '12:45' },
    ] as FixedTaskTimeOption[],
  },
  postMarket: {
    options: [
      { hour: 15, minute: 15, label: '15:15' },
      { hour: 15, minute: 30, label: '15:30' },
      { hour: 15, minute: 45, label: '15:45' },
      { hour: 16, minute: 0, label: '16:00' },
    ] as FixedTaskTimeOption[],
  },
} as const

export const RUN_COUNT_OPTIONS = [1, 2, 3, 4] as const

export interface ScheduleFormState {
  preMarket: { enabled: boolean; hour: number; minute: number; prompt: string }
  postMarket: { enabled: boolean; hour: number; minute: number; prompt: string }
  midday: { enabled: boolean; hour: number; minute: number; prompt: string }
  morning: { enabled: boolean; runCount: number; prompt: string }
  afternoon: { enabled: boolean; runCount: number; prompt: string }
}

const FIXED_TASK_NAMES = {
  preMarket: '盘前分析',
  midday: '午间复盘',
  postMarket: '收盘分析',
} as const

const SESSION_TASK_NAMES = {
  morning: '上午运行',
  afternoon: '下午运行',
} as const

const DEFAULT_TIMEOUT = 1800

function normalizeSectionTime(section: ScheduleKey, hour: number, minute: number) {
  const options = FIXED_TASK_TIME_OPTIONS[section]

  if (section === 'preMarket' && hour === 7 && minute === 15) {
    return {
      hour: 8,
      minute: 0,
    }
  }

  const exactMatch = options.options.find((option) => option.hour === hour && option.minute === minute)
  if (exactMatch) {
    return {
      hour: exactMatch.hour,
      minute: exactMatch.minute,
    }
  }

  const currentMinutes = hour * 60 + minute
  const nearest = options.options.reduce((best, option) => {
    const optionMinutes = option.hour * 60 + option.minute
    const bestMinutes = best.hour * 60 + best.minute
    return Math.abs(optionMinutes - currentMinutes) < Math.abs(bestMinutes - currentMinutes) ? option : best
  }, options.options[0])

  return {
    hour: nearest.hour,
    minute: nearest.minute,
  }
}

const defaultState = (): ScheduleFormState => ({
  preMarket: { enabled: false, hour: 8, minute: 0, prompt: '请分析今日市场情况和持仓情况，包括宏观经济数据、行业动态、重要新闻等，并提供今日可能的市场走势预测，为接下来的决策和交易做好准备。' },
  postMarket: { enabled: false, hour: 15, minute: 30, prompt: '请对今日市场和交易操作进行全面复盘，分析主要指数表现、板块涨跌、热门个股、成交量变化等，并总结今日市场特点和明日可能的走势。' },
  midday: { enabled: false, hour: 12, minute: 0, prompt: '请对上午市场和交易操作进行复盘，分析上午市场的主要变化、热点板块、资金流向等，并对下午市场可能的走势进行预测，为接下来的决策和交易做好准备。' },
  morning: { enabled: true, runCount: 2, prompt: '你正在执行盘中交易操作，你必须像主动型交易员一样思考，在可承受风险下最大化收益，你可以进行一切交易操作，包括但不限于建仓、减仓、清仓、换股、做T、锁定利润、回避风险、空仓等待等。' },
  afternoon: { enabled: true, runCount: 2, prompt: '你正在执行盘中交易操作，你必须像主动型交易员一样思考，在可承受风险下最大化收益，你可以进行一切交易操作，包括但不限于建仓、减仓、清仓、换股、做T、锁定利润、回避风险、空仓等待等。' },
})

function parseCron(cronExpression: string) {
  const [minuteText = '0', hourText = '0'] = cronExpression.split(' ')
  return {
    minute: Number(minuteText),
    hour: Number(hourText),
  }
}

function buildCron(hour: number, minute: number) {
  return `${minute} ${hour} * * 1-5`
}

function getSessionTimes(session: SessionKey, runCount: number) {
  const count = Number(runCount)

  if (session === 'morning') {
    switch (count) {
      case 1:
        return [{ hour: 10, minute: 30 }]
      case 2:
        return [{ hour: 10, minute: 0 }, { hour: 11, minute: 0 }]
      case 3:
        return [{ hour: 9, minute: 30 }, { hour: 10, minute: 15 }, { hour: 11, minute: 0 }]
      case 4:
        return [{ hour: 9, minute: 30 }, { hour: 10, minute: 0 }, { hour: 10, minute: 30 }, { hour: 11, minute: 0 }]
      default:
        return [{ hour: 10, minute: 0 }, { hour: 11, minute: 0 }]
    }
  }

  switch (count) {
    case 1:
      return [{ hour: 14, minute: 0 }]
    case 2:
      return [{ hour: 13, minute: 30 }, { hour: 14, minute: 30 }]
    case 3:
      return [{ hour: 13, minute: 0 }, { hour: 13, minute: 45 }, { hour: 14, minute: 30 }]
    case 4:
      return [{ hour: 13, minute: 0 }, { hour: 13, minute: 30 }, { hour: 14, minute: 0 }, { hour: 14, minute: 30 }]
    default:
      return [{ hour: 13, minute: 30 }, { hour: 14, minute: 30 }]
  }
}

function getSessionTimeLabels(session: SessionKey, runCount: number) {
  return getSessionTimes(session, runCount)
    .map(({ hour, minute }) => `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`)
    .join(', ')
}

export function useScheduleForm() {
  const timeErrors = ref<Record<string, boolean>>({})
  const scheduleSettings = reactive<ScheduleFormState>(defaultState())

  function syncFromSchedules(schedules: ScheduleLike[]) {
    Object.assign(scheduleSettings, defaultState())

    ;(Object.keys(FIXED_TASK_NAMES) as ScheduleKey[]).forEach((key) => {
      const matched = schedules.find((item) => item.name === FIXED_TASK_NAMES[key])
      if (!matched) {
        return
      }

      const { hour, minute } = parseCron(matched.cron_expression)
      const normalizedTime = normalizeSectionTime(key, hour, minute)
      scheduleSettings[key].enabled = matched.enabled
      scheduleSettings[key].hour = normalizedTime.hour
      scheduleSettings[key].minute = normalizedTime.minute
      scheduleSettings[key].prompt = matched.task_prompt || scheduleSettings[key].prompt
    })

    ;(Object.keys(SESSION_TASK_NAMES) as SessionKey[]).forEach((key) => {
      const matched = schedules
        .filter((item) => item.name.startsWith(SESSION_TASK_NAMES[key]))
        .sort((a, b) => a.cron_expression.localeCompare(b.cron_expression))

      if (matched.length === 0) {
        return
      }

      scheduleSettings[key].enabled = matched.some((item) => item.enabled)
      scheduleSettings[key].runCount = Number(matched.length)
      scheduleSettings[key].prompt = matched[0].task_prompt || scheduleSettings[key].prompt
    })
  }

  function buildPayload(existingSchedules: ScheduleLike[]) {
    const fixedPayload = (Object.keys(FIXED_TASK_NAMES) as ScheduleKey[]).map((key) => {
      const existing = existingSchedules.find((item) => item.name === FIXED_TASK_NAMES[key])
      const current = scheduleSettings[key]
      return {
        id: existing?.id,
        name: FIXED_TASK_NAMES[key],
        run_type: 'analysis' as const,
        cron_expression: buildCron(current.hour, current.minute),
        task_prompt: current.prompt,
        timeout_seconds: existing?.timeout_seconds ?? DEFAULT_TIMEOUT,
        enabled: current.enabled,
      }
    })

    const sessionPayload = (Object.keys(SESSION_TASK_NAMES) as SessionKey[]).flatMap((key) => {
      const current = scheduleSettings[key]
      const existing = existingSchedules.filter((item) => item.name.startsWith(SESSION_TASK_NAMES[key]))
      return getSessionTimes(key, current.runCount).map((time, index) => ({
        id: existing[index]?.id,
        name: `${SESSION_TASK_NAMES[key]}${index + 1}号`,
        run_type: 'trade' as const,
        cron_expression: buildCron(time.hour, time.minute),
        task_prompt: current.prompt,
        timeout_seconds: existing[index]?.timeout_seconds ?? DEFAULT_TIMEOUT,
        enabled: current.enabled,
      }))
    })

    return [...fixedPayload, ...sessionPayload]
  }

  function validateTime(section: ScheduleKey, field: 'hour' | 'minute') {
    void field
    timeErrors.value[section] = false
  }

  function setFixedTaskTime(section: ScheduleKey, option: FixedTaskTimeOption) {
    scheduleSettings[section].hour = option.hour
    scheduleSettings[section].minute = option.minute
    timeErrors.value[section] = false
  }

  function autoResizeTextarea(event: Event) {
    const textarea = event.target as HTMLTextAreaElement
    textarea.style.height = 'auto'
    textarea.style.height = `${Math.min(textarea.scrollHeight, 200)}px`
  }

  function getMorningRunTimes() {
    return getSessionTimeLabels('morning', scheduleSettings.morning.runCount)
  }

  function getAfternoonRunTimes() {
    return getSessionTimeLabels('afternoon', scheduleSettings.afternoon.runCount)
  }

  return {
    timeErrors,
    scheduleSettings,
    fixedTaskTimeOptions: FIXED_TASK_TIME_OPTIONS,
    runCountOptions: RUN_COUNT_OPTIONS,
    syncFromSchedules,
    buildPayload,
    validateTime,
    setFixedTaskTime,
    autoResizeTextarea,
    getMorningRunTimes,
    getAfternoonRunTimes,
  }
}
