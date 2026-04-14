<template>
<div class="tab-content">
        <section class="content-grid content-grid-primary">
          <section class="panel overview-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>账户总览</h2>
                <p class="section-kicker">Overview</p>
              </div>
              <div class="panel-head-actions">
                <button
                  class="button ghost small overview-refresh-button"
                  :class="{ 'is-loading': accountRefreshing }"
                  :disabled="accountRefreshing || !canManualRefreshAccount"
                  @click="handleManualRefresh"
                >
                  {{ canManualRefreshAccount ? '刷新' : `${accountRefreshCooldownText}后可刷新` }}
                </button>
              </div>
            </div>
            <div class="overview-stats">
              <div class="stat-card">
                <span class="meta-label">账户状态</span>
                <strong>{{ formatDays(account?.operating_days) }}</strong>
                <p>运作天数</p>
                <div class="stat-card-inline">
                  <span>开户日期</span>
                  <b>{{ account?.open_date || '--' }}</b>
                </div>
                <div class="stat-card-inline">
                  <span>交易成功率</span>
                  <b :class="tradeSuccessRateClass(tradeSuccessRate)">{{ formatPercent(tradeSuccessRate) }}</b>
                </div>
              </div>
              <div class="stat-card">
                <span class="meta-label">资金规模</span>
                <strong :class="profitClass(getAssetDelta(account?.total_assets, account?.initial_capital))">{{ formatMoney(account?.total_assets) }}</strong>
                <p>账户总资产</p>
                <div class="stat-card-inline">
                  <span>初始资金</span>
                  <b>{{ formatMoney(account?.initial_capital) }}</b>
                </div>
                <div class="stat-card-inline">
                  <span>初始净值</span>
                  <b>1.000</b>
                </div>
              </div>
              <div class="stat-card">
                <span class="meta-label">仓位情况</span>
                <strong>{{ formatMoney(account?.total_market_value) }}</strong>
                <p>总持仓市值</p>
                <div class="stat-card-inline">
                  <span>现金余额</span>
                  <b>{{ formatMoney(account?.cash_balance) }}</b>
                </div>
                <div class="stat-card-inline">
                  <span>总仓位比例</span>
                  <b>{{ formatPercent(account?.total_position_ratio) }}</b>
                </div>
              </div>
              <div class="stat-card">
                <span class="meta-label">累计表现</span>
                <strong :class="profitClass(account?.holding_profit)">{{ formatSignedMoney(account?.holding_profit) }}</strong>
                <p>总持仓盈亏</p>
                <div class="stat-card-inline">
                  <span>总收益率</span>
                  <b :class="profitClass(account?.total_return_ratio)">{{ formatPercent(account?.total_return_ratio) }}</b>
                </div>
                <div class="stat-card-inline">
                  <span>当前净值</span>
                  <b :class="profitClass(getNavDelta(account?.nav))">{{ formatNav(account?.nav) }}</b>
                </div>
              </div>
              <div class="stat-card">
                <span class="meta-label">今日表现（{{ account?.daily_profit_trade_date || '--' }}）</span>
                <strong :class="profitClass(account?.daily_profit)">{{ formatSignedMoney(account?.daily_profit) }}</strong>
                <p>当日盈亏金额</p>
                <div class="stat-card-inline">
                  <span>当日收益率</span>
                  <b :class="profitClass(account?.daily_return_ratio)">{{ formatPercent(account?.daily_return_ratio) }}</b>
                </div>
                <div class="stat-card-inline">
                  <span>今日交易次数</span>
                  <b>{{ formatTradeCount(todayTradeCount) }}</b>
                </div>
              </div>
            </div>
          </section>

          <!-- 持仓情况面板 -->
          <section class="panel positions-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>持仓情况</h2>
                <p class="section-kicker">Positions</p>
              </div>
              <div class="panel-head-actions">
                <button
                  class="button ghost small overview-refresh-button"
                  :class="{ 'is-loading': accountRefreshing }"
                  :disabled="accountRefreshing || !canManualRefreshAccount"
                  @click="handleManualRefresh"
                >
                  {{ canManualRefreshAccount ? '刷新' : `${accountRefreshCooldownText}后可刷新` }}
                </button>
              </div>
            </div>
            <div
              class="positions-table-wrap"
              :class="{ 'table-wrap-scrollable': hasPositionOverflow }"
              v-if="displayPositions.length"
            >
              <table class="positions-table">
                <thead>
                  <tr>
                    <th>名称 / 代码</th>
                    <th>持仓市值</th>
                    <th>持仓股数 / 可卖股数</th>
                    <th>当日盈亏 / 当日收益率</th>
                    <th>总盈亏 / 总收益率</th>
                    <th>当前价 / 成本价</th>
                    <th>仓位占比</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="pos in displayPositions" :key="pos.symbol">
                    <td>
                      <div class="position-cell-main">
                        <strong class="position-name">{{ pos.name }}</strong>
                        <span class="position-symbol">{{ pos.symbol }}</span>
                      </div>
                    </td>
                    <td>{{ formatMoney(pos.amount) }}</td>
                    <td>
                      <div class="position-cell-stack">
                        <span>{{ formatVolume(pos.volume) }}</span>
                        <span class="metric-sub">可卖 {{ formatVolume(pos.available_volume) }}</span>
                      </div>
                    </td>
                    <td>
                      <div class="position-cell-stack">
                        <span :class="profitClass(pos.day_profit)">{{ formatSignedMoney(pos.day_profit) }}</span>
                        <span class="metric-sub" :class="profitClass(pos.day_profit_ratio)">{{ formatPercent(pos.day_profit_ratio) }}</span>
                      </div>
                    </td>
                    <td>
                      <div class="position-cell-stack">
                        <span :class="profitClass(pos.profit)">{{ formatSignedMoney(pos.profit) }}</span>
                        <span class="metric-sub" :class="profitClass(pos.profit_ratio)">{{ formatPercent(pos.profit_ratio) }}</span>
                      </div>
                    </td>
                    <td>
                      <div class="position-cell-stack">
                        <span>{{ formatMoney(pos.current_price) }}</span>
                        <span class="metric-sub">{{ formatMoney(pos.cost_price) }}</span>
                      </div>
                    </td>
                    <td>{{ getPositionRatioText(pos.position_ratio) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div v-else class="empty-state">
              <p>{{ errorMessage || '账户接口暂未返回真实持仓数据。请检查后端账户接口或先执行一次任务刷新账户快照。' }}</p>
            </div>
          </section>

          <section class="panel trade-summary-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>交易信息</h2>
                <p class="section-kicker">Trades</p>
              </div>
              <div class="panel-head-actions">
                <button
                  class="button ghost small overview-refresh-button"
                  :class="{ 'is-loading': accountRefreshing }"
                  :disabled="accountRefreshing || !canManualRefreshAccount"
                  @click="handleManualRefresh"
                >
                  {{ canManualRefreshAccount ? '刷新' : `${accountRefreshCooldownText}后可刷新` }}
                </button>
              </div>
            </div>
            <div
              class="positions-table-wrap"
              :class="{ 'trade-summary-table-wrap-scrollable': hasTradeSummaryOverflow }"
              v-if="displayTradeSummaries.length"
            >
              <table class="positions-table trade-summary-table">
                <thead>
                  <tr>
                    <th>名称 / 代码</th>
                    <th>买入时间</th>
                    <th>卖出时间</th>
                    <th>成交股数</th>
                    <th>卖出均价 / 买入均价</th>
                    <th>卖出金额 / 买入金额</th>
                    <th>收益 / 收益率</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="trade in displayTradeSummaries" :key="`${trade.symbol}-${trade.closed_at || '--'}`">
                    <td>
                      <div class="position-cell-main">
                        <strong class="position-name">{{ trade.name }}</strong>
                        <span class="position-symbol">{{ trade.symbol }}</span>
                      </div>
                    </td>
                    <td>{{ formatMinuteTime(trade.opened_at) }}</td>
                    <td>{{ formatMinuteTime(trade.closed_at) }}</td>
                    <td>{{ formatVolume(trade.volume) }}</td>
                    <td>
                      <div class="position-cell-stack">
                        <span>{{ formatMoney(trade.sell_price) }}</span>
                        <span class="metric-sub">{{ formatMoney(trade.buy_price) }}</span>
                      </div>
                    </td>
                    <td>
                      <div class="position-cell-stack">
                        <span>{{ formatMoney(trade.sell_amount) }}</span>
                        <span class="metric-sub">{{ formatMoney(trade.buy_amount) }}</span>
                      </div>
                    </td>
                    <td>
                      <div class="position-cell-stack">
                        <span :class="profitClass(trade.profit)">{{ formatSignedMoney(trade.profit) }}</span>
                        <span class="metric-sub" :class="profitClass(trade.profit_ratio)">{{ formatPercent(trade.profit_ratio) }}</span>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div v-else class="empty-state">
              <p>当前还没有可展示的完整交易闭环。买入后全部卖出的股票会展示在这里。</p>
            </div>
          </section>

          <section class="panel orders-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>委托信息</h2>
                <p class="section-kicker">Orders</p>
              </div>
              <div class="panel-head-actions">
                <button
                  class="button ghost small overview-refresh-button"
                  :class="{ 'is-loading': accountRefreshing }"
                  :disabled="accountRefreshing || !canManualRefreshAccount"
                  @click="handleManualRefresh"
                >
                  {{ canManualRefreshAccount ? '刷新' : `${accountRefreshCooldownText}后可刷新` }}
                </button>
              </div>
            </div>
            <div
              class="positions-table-wrap"
              :class="{ 'orders-table-wrap-scrollable': hasOrderOverflow }"
              v-if="displayOrders.length"
            >
              <table class="positions-table orders-table">
                <thead>
                  <tr>
                    <th>名称 / 代码</th>
                    <th>委托时间</th>
                    <th>买卖方向</th>
                    <th>委托价格 / 数量</th>
                    <th>成交价格 / 数量</th>
                    <th>委托状态</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="order in displayOrders" :key="order.order_id">
                    <td>
                      <div class="position-cell-main">
                        <strong class="position-name">{{ order.name }}</strong>
                        <span class="position-symbol">{{ order.symbol }}</span>
                      </div>
                    </td>
                    <td>{{ order.order_time || '--' }}</td>
                    <td>
                      <span class="order-side" :class="order.side === 'buy' ? 'profit-up' : 'profit-down'">
                        {{ order.side_text }}
                      </span>
                    </td>
                    <td>
                      <div class="position-cell-stack">
                        <span>{{ formatMoney(order.order_price) }}</span>
                        <span class="metric-sub">{{ formatVolume(order.order_quantity) }}</span>
                      </div>
                    </td>
                    <td>
                      <div class="position-cell-stack">
                        <span>{{ formatMoney(order.filled_price) }}</span>
                        <span class="metric-sub">{{ formatVolume(order.filled_quantity) }}</span>
                      </div>
                    </td>
                    <td>
                      <span class="order-status" :class="getOrderStatusClass(order.status_text)">
                        {{ order.status_text || '--' }}
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div v-else class="empty-state">
              <p>当前没有可展示的委托记录。完成一次买入、卖出或撤单后，委托信息会显示在这里。</p>
            </div>
          </section>

          <!-- 运行总览面板 -->
          <section class="panel runtime-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>运行总览</h2>
                <p class="section-kicker">Runtime Overview</p>
              </div>
            </div>
            <div class="runtime-content">
              <!-- 最近一次分析 -->
              <div class="runtime-section">
                <h4 class="runtime-title">最近一次分析</h4>
                <div class="runtime-stats">
                  <div class="runtime-stat">
                    <span class="stat-label">启动时间</span>
                    <span class="stat-value stat-value-compact">{{ formatRuntimeTime(runtimeOverview.last_run.start_time) }}</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">停止时间</span>
                    <span class="stat-value stat-value-compact">{{ formatRuntimeTime(runtimeOverview.last_run.end_time) }}</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">状态</span>
                    <span class="stat-value" :class="runtimeOverview.last_run.status === 'completed' ? 'status-success' : runtimeOverview.last_run.status === 'failed' ? 'status-failed' : ''">{{ runtimeOverview.last_run.status_text }}</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">持续时长</span>
                    <span class="stat-value">{{ runtimeOverview.last_run.duration }}</span>
                  </div>
                </div>
                <div class="runtime-token-bar">
                  <div class="runtime-token-item">
                    <span class="token-label">输入</span>
                    <span class="token-value">{{ runtimeOverview.last_run.input_tokens }}</span>
                  </div>
                  <div class="runtime-token-item">
                    <span class="token-label">输出</span>
                    <span class="token-value">{{ runtimeOverview.last_run.output_tokens }}</span>
                  </div>
                  <div class="runtime-token-item runtime-token-item-total">
                    <span class="token-label">总量</span>
                    <span class="token-value">{{ runtimeOverview.last_run.total_tokens }}</span>
                  </div>
                </div>
              </div>

              <!-- 今日统计 -->
              <div class="runtime-section">
                <h4 class="runtime-title">今日统计</h4>
                <div class="runtime-stats">
                  <div class="runtime-stat">
                    <span class="stat-label">AI分析</span>
                    <span class="stat-value">{{ runtimeOverview.today.analysis_count }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">接口调用</span>
                    <span class="stat-value">{{ runtimeOverview.today.api_calls }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">交易执行</span>
                    <span class="stat-value">{{ runtimeOverview.today.trades }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">成功率</span>
                    <span class="stat-value">{{ runtimeOverview.today.success_rate }}%</span>
                  </div>
                </div>
                <div class="runtime-token-bar">
                  <div class="runtime-token-item runtime-token-item-muted">
                    <span class="token-label">输入</span>
                    <span class="token-value">{{ runtimeOverview.today.input_tokens }}</span>
                  </div>
                  <div class="runtime-token-item runtime-token-item-muted">
                    <span class="token-label">输出</span>
                    <span class="token-value">{{ runtimeOverview.today.output_tokens }}</span>
                  </div>
                  <div class="runtime-token-item runtime-token-item-total">
                    <span class="token-label">总量</span>
                    <span class="token-value">{{ runtimeOverview.today.total_tokens }}</span>
                  </div>
                </div>
              </div>

              <!-- 近3个交易日 -->
              <div class="runtime-section">
                <h4 class="runtime-title">近3个交易日</h4>
                <div class="runtime-stats">
                  <div class="runtime-stat">
                    <span class="stat-label">AI分析</span>
                    <span class="stat-value">{{ runtimeOverview.recent_3_days.analysis_count }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">接口调用</span>
                    <span class="stat-value">{{ runtimeOverview.recent_3_days.api_calls }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">交易执行</span>
                    <span class="stat-value">{{ runtimeOverview.recent_3_days.trades }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">成功率</span>
                    <span class="stat-value">{{ runtimeOverview.recent_3_days.success_rate }}%</span>
                  </div>
                </div>
                <div class="runtime-token-bar">
                  <div class="runtime-token-item runtime-token-item-muted">
                    <span class="token-label">输入</span>
                    <span class="token-value">{{ runtimeOverview.recent_3_days.input_tokens }}</span>
                  </div>
                  <div class="runtime-token-item runtime-token-item-muted">
                    <span class="token-label">输出</span>
                    <span class="token-value">{{ runtimeOverview.recent_3_days.output_tokens }}</span>
                  </div>
                  <div class="runtime-token-item runtime-token-item-total">
                    <span class="token-label">总量</span>
                    <span class="token-value">{{ runtimeOverview.recent_3_days.total_tokens }}</span>
                  </div>
                </div>
              </div>

              <!-- 近7个交易日 -->
              <div class="runtime-section">
                <h4 class="runtime-title">近7个交易日</h4>
                <div class="runtime-stats">
                  <div class="runtime-stat">
                    <span class="stat-label">AI分析</span>
                    <span class="stat-value">{{ runtimeOverview.recent_7_days.analysis_count }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">接口调用</span>
                    <span class="stat-value">{{ runtimeOverview.recent_7_days.api_calls }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">交易执行</span>
                    <span class="stat-value">{{ runtimeOverview.recent_7_days.trades }}次</span>
                  </div>
                  <div class="runtime-stat">
                    <span class="stat-label">成功率</span>
                    <span class="stat-value">{{ runtimeOverview.recent_7_days.success_rate }}%</span>
                  </div>
                </div>
                <div class="runtime-token-bar">
                  <div class="runtime-token-item runtime-token-item-muted">
                    <span class="token-label">输入</span>
                    <span class="token-value">{{ runtimeOverview.recent_7_days.input_tokens }}</span>
                  </div>
                  <div class="runtime-token-item runtime-token-item-muted">
                    <span class="token-label">输出</span>
                    <span class="token-value">{{ runtimeOverview.recent_7_days.output_tokens }}</span>
                  </div>
                  <div class="runtime-token-item runtime-token-item-total">
                    <span class="token-label">总量</span>
                    <span class="token-value">{{ runtimeOverview.recent_7_days.total_tokens }}</span>
                  </div>
                </div>
              </div>
            </div>
          </section>
        </section>
      </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted } from 'vue'
import { storeToRefs } from 'pinia'
import { useAppStore } from '@/stores/legacy'
import { formatMinuteTime, formatMoney, formatPercent, formatTime } from '@/utils/formatters'

const store = useAppStore()
const { account, runtimeOverview, errorMessage, accountRefreshing, canManualRefreshAccount, accountRefreshCooldownText } = storeToRefs(store)

const displayPositions = computed(() => account.value.positions.filter((position) => (position.volume ?? 0) > 0))
const displayOrders = computed(() => account.value.orders)
const displayTradeSummaries = computed(() => account.value.trade_summaries)
const hasPositionOverflow = computed(() => displayPositions.value.length > 5)
const hasOrderOverflow = computed(() => displayOrders.value.length > 5)
const hasTradeSummaryOverflow = computed(() => displayTradeSummaries.value.length > 5)
const tradeSuccessRate = computed(() => {
  const total = displayTradeSummaries.value.length
  if (total === 0) {
    return null
  }
  const profitableCount = displayTradeSummaries.value.filter((trade) => trade.profit > 0).length
  return profitableCount / total
})
const todayTradeCount = computed(() => {
  const now = new Date()
  const todayPrefix = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`
  return displayOrders.value.filter((order) => (
    Boolean(order.order_time?.startsWith(todayPrefix))
    && (order.filled_quantity ?? 0) > 0
  )).length
})
let cooldownTimer: number | null = null

function formatSignedMoney(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return '--'
  const formatted = formatMoney(Math.abs(value))
  if (value > 0) return `+${formatted}`
  if (value < 0) return `-${formatted}`
  return formatted
}

function profitClass(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return ''
  if (value > 0) return 'profit-up'
  if (value < 0) return 'profit-down'
  return ''
}

function tradeSuccessRateClass(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) return ''
  return value >= 0.5 ? 'profit-up' : 'profit-down'
}

function getPositionRatioText(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '--'
  }
  return `${(value * 100).toFixed(1)}%`
}

function formatVolume(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '--'
  }
  return `${value.toLocaleString()} 股`
}

function formatNav(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '--'
  }
  return value.toFixed(3)
}

function formatDays(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '--'
  }
  return `${value} 天`
}

function formatTradeCount(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '--'
  }
  return `${value} 次`
}

function formatRuntimeTime(value: string | null | undefined) {
  const text = formatTime(value)
  if (text === '从未运行' || text === '--') {
    return text
  }

  const match = text.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}:\d{2}:\d{2})$/)
  if (!match) {
    return text
  }

  const [, , month, day, time] = match
  return `${month}-${day} ${time}`
}

function getAssetDelta(totalAssets: number | null | undefined, initialCapital: number | null | undefined) {
  if (
    totalAssets === null || totalAssets === undefined || Number.isNaN(totalAssets)
    || initialCapital === null || initialCapital === undefined || Number.isNaN(initialCapital)
  ) {
    return null
  }
  return totalAssets - initialCapital
}

function getNavDelta(nav: number | null | undefined) {
  if (nav === null || nav === undefined || Number.isNaN(nav)) {
    return null
  }
  return nav - 1
}

function getOrderStatusClass(statusText: string | null | undefined) {
  return statusText === '已成交' ? 'order-status-success' : 'order-status-danger'
}

onMounted(async () => {
  const tasks: Promise<unknown>[] = []

  if (account.value.positions.length === 0) {
    tasks.push(store.refreshAccountData())
  }

  tasks.push(store.refreshRuntimeOverview())

  if (tasks.length > 0) {
    const results = await Promise.allSettled(tasks)
    const failed = results.find((result): result is PromiseRejectedResult => result.status === 'rejected')
    if (failed) {
      errorMessage.value = failed.reason instanceof Error ? failed.reason.message : '总览数据加载失败'
    }
  }

  cooldownTimer = window.setInterval(() => {
    store.touchAccountRefreshTick()
  }, 60 * 1000)
})

onUnmounted(() => {
  if (cooldownTimer !== null) {
    window.clearInterval(cooldownTimer)
  }
})

async function handleManualRefresh() {
  if (!canManualRefreshAccount.value || accountRefreshing.value) {
    return
  }

  try {
    await store.refreshAccountDataWithCooldown()
  } catch {
    // Error message is already synchronized in the store.
  }
}

</script>
