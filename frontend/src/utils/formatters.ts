const BEIJING_TIMEZONE = 'Asia/Shanghai'

function parseDateParts(isoStr: string | null | undefined) {
  if (!isoStr) return null

  const d = new Date(isoStr)
  if (Number.isNaN(d.getTime())) return null

  const formatter = new Intl.DateTimeFormat('zh-CN', {
    timeZone: BEIJING_TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })

  const parts = formatter.formatToParts(d)
  const partMap = Object.fromEntries(parts.map((part) => [part.type, part.value]))

  return {
    year: partMap.year,
    month: partMap.month,
    day: partMap.day,
    hour: partMap.hour,
    minute: partMap.minute,
    second: partMap.second,
  }
}

export function formatMoney(val: number | null | undefined): string {
  if (val === undefined || val === null) return '¥--'
  return `¥${val.toFixed(2)}`
}

export function formatPercent(val: number | null | undefined): string {
  if (val === undefined || val === null) return '--%'
  return `${(val * 100).toFixed(2)}%`
}

export function formatTime(isoStr: string | null | undefined): string {
  if (!isoStr) return '从未运行'
  const parts = parseDateParts(isoStr)
  if (!parts) return '--'
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`
}

export function formatShortTime(isoStr: string | null | undefined): string {
  if (!isoStr) return '--'
  const parts = parseDateParts(isoStr)
  if (!parts) return '--'
  return `${parts.hour}:${parts.minute}`
}

export function formatMinuteTime(isoStr: string | null | undefined): string {
  if (!isoStr) return '--'
  const parts = parseDateParts(isoStr)
  if (!parts) return typeof isoStr === 'string' ? isoStr.slice(0, 16) : '--'
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}`
}

export function formatWeekdayMinuteTime(isoStr: string | null | undefined): string {
  if (!isoStr) return '--'

  const d = new Date(isoStr)
  if (Number.isNaN(d.getTime())) {
    return typeof isoStr === 'string' ? isoStr : '--'
  }

  const dateText = new Intl.DateTimeFormat('zh-CN', {
    timeZone: BEIJING_TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(d)

  const normalized = dateText.replace(/\//g, '-').replace(',', '')
  return normalized.replace(/周([一二三四五六日天])/, '（周$1）')
}

export function statusTone(status: string) {
  switch (status) {
    case 'running': return 'tone-info'
    case 'completed': return 'tone-success'
    case 'failed': return 'tone-error'
    case 'error': return 'tone-error'
    default: return 'tone-idle'
  }
}

export function statusText(status: string) {
  switch (status) {
    case 'running': return '执行中'
    case 'completed': return '成功'
    case 'failed': return '失败'
    case 'pending': return '等待中'
    default: return status || '空闲'
  }
}

export function pnlClass(val: number | null | undefined): string {
  if (val == null || Number.isNaN(val)) return 'pnl-zero'
  if (val === 0) return 'pnl-zero'
  return val > 0 ? 'pnl-up' : 'pnl-down'
}

export function formatPnl(val: number | null | undefined): string {
  if (val === undefined || val === null) return '--'
  const prefix = val > 0 ? '+' : ''
  return `${prefix}${val.toFixed(2)}`
}
