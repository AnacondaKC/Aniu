<template>
<div class="tab-content">
        <section class="content-grid content-grid-primary">
          <!-- 记录选择区域 - 方块卡片式 -->
          <section class="panel run-grid-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>运行记录</h2>
                <p class="section-kicker">Run History</p>
              </div>
            </div>

            <div v-if="analysisError" class="error-banner">{{ analysisError }}</div>

            <div class="runs-container">
              <!-- 今日运行 - 方块网格 -->
              <div class="run-group" v-if="todayRuns.length || todaySuccessCount || todayFailedCount">
                <div class="group-label">
                  <span class="label-text">今日</span>
                  <div class="group-label-meta">
                    <span class="run-summary-text run-summary-success">成功{{ todaySuccessCount }}次</span>
                    <button
                      type="button"
                      class="run-summary-text run-summary-failed"
                      :class="{ 'is-active': showFailedRuns }"
                      @click="toggleFailedRuns"
                    >
                      失败{{ todayFailedCount }}次
                    </button>
                  </div>
                </div>
                <div class="run-grid" v-if="todayRuns.length">
                  <div 
                    v-for="run in todayRuns" 
                     :key="run.id"
                     class="run-card"
                     :class="{ active: selectedRun?.id === run.id }"
                     @click="handleSelectRun(run.id, todayRuns)"
                   >
                    <div class="run-card-type">{{ run.analysisType }}</div>
                    <div class="run-card-time">{{ formatShortTime(run.startTime) }}</div>
                    <div class="run-card-duration">{{ run.duration }}</div>
                     <div class="run-card-status" :class="statusTone(run.status)"></div>
                   </div>
                </div>
                <div v-else class="run-grid-empty">
                  今日暂无可展示的运行记录。
                </div>
                <div v-if="todayRuns.length && todayHasMore" class="panel-actions">
                  <button class="button ghost" :class="{ 'is-loading': todayLoadingMore }" @click="loadMoreTodayRuns" :disabled="todayLoadingMore">
                    加载更多
                  </button>
                </div>
              </div>
              <div v-else class="run-grid-empty">
                今日暂无运行记录。
              </div>

              <!-- 历史记录 - 日期选择 + 方块网格 -->
              <div class="run-group">
                <div class="group-label">
                  <span class="label-text">历史</span>
                  <div class="group-label-meta">
                    <button type="button" class="date-input-trigger" @click="openHistoryDatePicker">
                      <span class="date-input-value">{{ historyDateDisplay }}</span>
                    </button>
                    <input
                      ref="historyDateInput"
                      type="date"
                      v-model="selectedDate"
                      @change="loadHistoryRuns"
                      class="date-input-native"
                      tabindex="-1"
                      aria-hidden="true"
                    />
                  </div>
                </div>
                <div class="run-grid" v-if="historyRuns.length">
                  <div 
                    v-for="run in historyRuns" 
                     :key="run.id"
                     class="run-card"
                     :class="{ active: selectedRun?.id === run.id }"
                     @click="handleSelectRun(run.id, historyRuns)"
                   >
                    <div class="run-card-type">{{ run.analysisType }}</div>
                    <div class="run-card-time">{{ formatShortTime(run.startTime) }}</div>
                    <div class="run-card-duration">{{ run.duration }}</div>
                    <div class="run-card-status" :class="statusTone(run.status)"></div>
                  </div>
                </div>
                <div v-if="historyRuns.length && historyHasMore" class="panel-actions">
                  <button class="button ghost" :class="{ 'is-loading': historyLoadingMore }" @click="loadMoreHistoryRuns" :disabled="historyLoadingMore">
                    加载更多
                  </button>
                </div>
                <div v-else-if="selectedDate" class="run-grid-empty">
                  该日期没有找到运行记录，请切换日期后重试。
                </div>
              </div>
            </div>
          </section>

          <!-- 分析详情内容区域 - 三列布局 -->
          <section class="panel analysis-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>分析详情</h2>
                <p class="section-kicker">Analysis Detail</p>
              </div>
            </div>

            <!-- 三列详情网格 -->
            <div class="detail-grid" v-if="selectedRun">
              <!-- 第一列：运行状态 -->
              <div class="detail-column status-column">
                <h4 class="column-title">运行状态</h4>
                <div class="detail-column-body">
                  <div class="stat-compact">
                    <div class="stat-main">
                      <span class="time-value">{{ formatTime(selectedRun.startTime) }}</span>
                      <span class="duration-value">{{ selectedRun.duration }}</span>
                      <span class="status-dot" :class="'dot-' + statusTone(selectedRun.status)"></span>
                    </div>
                    <div class="token-row">
                      <span class="token-item">
                        <i>输入</i>
                        <b>{{ selectedRun.inputTokens || '0' }}</b>
                      </span>
                      <span class="token-item">
                        <i>输出</i>
                        <b>{{ selectedRun.outputTokens || '1.2k' }}</b>
                      </span>
                      <span class="token-item total">
                        <i>总量</i>
                        <b>{{ selectedRun.totalTokens || '1.25k' }}</b>
                      </span>
                    </div>
                  </div>
                </div>
              </div>

              <!-- 第二列：接口调用 -->
              <div class="detail-column api-column">
                <h4 class="column-title">接口调用 ({{ selectedRun.apiCalls }})</h4>
                <div class="detail-column-body">
                  <div class="compact-list analysis-compact-list" v-if="selectedRun.apiDetails.length">
                    <button
                      v-for="(api, idx) in selectedRun.apiDetails"
                      :key="idx"
                      type="button"
                      class="compact-item api-item compact-item-button"
                      :class="{ active: activePreviewIndex === api.preview_index }"
                      @click="focusPreview(api.preview_index)"
                    >
                      <div class="compact-main api-main">
                        <span class="item-name" :title="api.name">{{ api.name }}</span>
                        <span class="item-summary" :title="api.summary">{{ api.summary }}</span>
                      </div>
                    </button>
                  </div>
                  <div v-else-if="selectedRun.detailLoaded" class="detail-empty-state">
                    本次分析没有生成可展示的接口调用记录。
                  </div>
                </div>
              </div>

              <!-- 第三列：交易执行 -->
              <div class="detail-column trade-column">
                <h4 class="column-title">交易执行 ({{ displayTradeDetails.length }})</h4>
                <div class="detail-column-body">
                  <div class="compact-list analysis-compact-list" v-if="displayTradeDetails.length">
                    <button
                      v-for="(trade, idx) in displayTradeDetails"
                      :key="idx"
                      type="button"
                      class="compact-item trade-item compact-item-button"
                      :class="{ active: activePreviewIndex === trade.preview_index }"
                      @click="focusPreview(trade.preview_index)"
                    >
                      <div class="compact-main trade-main">
                        <span class="trade-text-action" :class="trade.action">{{ trade.action_text }}</span>
                        <span class="trade-text-summary" :title="trade.summary">{{ trade.summary }}</span>
                      </div>
                    </button>
                  </div>
                  <div v-else-if="selectedRun.detailLoaded" class="detail-empty-state">
                    本次分析没有生成可展示的模拟操作。
                  </div>
                </div>
              </div>
            </div>

            <div v-if="selectedRunLoading" class="detail-empty-state">
              正在加载本次运行详情...
            </div>

             <!-- 分析输出内容 / 原始返回联动预览 -->
             <div class="output-section" v-if="selectedRun?.output || activePreview">
               <div class="output-surface" @click="handleOutputSurfaceClick">
                  <div v-if="activePreview" class="raw-output-content">
                    {{ activePreview.preview }}
                  </div>
                 <div v-else-if="renderedOutputLoading" class="detail-empty-state">
                   正在渲染分析输出...
                 </div>
                 <div v-else class="markdown-content" v-html="renderedOutputHtml"></div>
               </div>
              </div>

            <div v-if="selectedRun?.status === 'failed'" class="error-banner">
              当前记录执行失败，请优先检查后端运行日志、模型配置或妙想接口状态。
            </div>

            <!-- 无数据提示 -->
            <div v-if="!selectedRun" class="empty-state">
              <p>当前没有可展示的运行详情。完成一次任务执行后，这里会显示完整分析结果。</p>
            </div>
          </section>
        </section>
      </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onBeforeUnmount, ref, watch } from 'vue'

import { useAnalysisRuns } from '@/composables/useAnalysisRuns'
import { api } from '@/services/api'
import { useAppStore } from '@/stores/legacy'
import { formatShortTime, formatTime, statusTone } from '@/utils/formatters'
import type { RawToolPreview, TradeDetail } from '@/types'

const store = useAppStore()

const {
  selectedRun,
  selectedRunLoading,
  showFailedRuns,
  todayRuns,
  historyRuns,
  todaySuccessCount,
  todayFailedCount,
  todayLoadingMore,
  historyLoadingMore,
  todayHasMore,
  historyHasMore,
  selectedDate,
  errorMessage: analysisError,
  renderedOutputHtml,
  renderedOutputLoading,
  loadInitialRuns,
  selectRun,
  loadHistoryRuns,
  loadMoreTodayRuns,
  loadMoreHistoryRuns,
  toggleFailedRuns,
} = useAnalysisRuns({
  listRunsPage: api.listRunsPage,
  loadRunDetail: store.loadRunDetail,
})

onMounted(() => {
  loadInitialRuns()
})

const historyDateInput = ref<HTMLInputElement | null>(null)
const activePreviewIndex = ref<number | null>(null)
const demoTradeDetails: TradeDetail[] = [
  {
    action: 'buy',
    action_text: '模拟买入',
    symbol: '000001',
    name: '平安银行',
    volume: 100,
    price: 12.34,
    amount: 1234,
    summary: '示例买入000001共100股（仅用于样式验证）。',
    tool_name: 'demo_trade_preview_1',
    preview_index: -1,
  },
  {
    action: 'sell',
    action_text: '模拟卖出',
    symbol: '600519',
    name: '贵州茅台',
    volume: 10,
    price: 1688,
    amount: 16880,
    summary: '示例卖出600519共10股（仅用于样式验证）。',
    tool_name: 'demo_trade_preview_2',
    preview_index: -2,
  },
  {
    action: 'buy',
    action_text: '模拟买入',
    symbol: '300750',
    name: '宁德时代',
    volume: 20,
    price: 202.5,
    amount: 4050,
    summary: '示例买入300750共20股（仅用于样式验证）。',
    tool_name: 'demo_trade_preview_3',
    preview_index: -3,
  },
  {
    action: 'sell',
    action_text: '模拟卖出',
    symbol: '601318',
    name: '中国平安',
    volume: 200,
    price: 41.26,
    amount: 8252,
    summary: '示例卖出601318共200股（仅用于样式验证）。',
    tool_name: 'demo_trade_preview_4',
    preview_index: -4,
  },
  {
    action: 'buy',
    action_text: '模拟买入',
    symbol: '002594',
    name: '比亚迪',
    volume: 15,
    price: 221.18,
    amount: 3317.7,
    summary: '示例买入002594共15股（仅用于样式验证）。',
    tool_name: 'demo_trade_preview_5',
    preview_index: -5,
  },
  {
    action: 'sell',
    action_text: '模拟卖出',
    symbol: '688981',
    name: '中芯国际',
    volume: 60,
    price: 103.91,
    amount: 6234.6,
    summary: '示例卖出688981共60股（仅用于样式验证）。',
    tool_name: 'demo_trade_preview_6',
    preview_index: -6,
  },
]

const demoTradePreviews: RawToolPreview[] = demoTradeDetails.map((detail, index) => ({
  preview_index: detail.preview_index ?? -(index + 1),
  tool_name: detail.tool_name ?? `demo_trade_preview_${index + 1}`,
  display_name: `示例交易执行返回 ${index + 1}`,
  summary: '仅用于前端样式验证的本地示例，不会写入后端。',
  preview: JSON.stringify(
    {
      code: '200',
      message: '示例下单成功',
      data: {
        orderId: `DEMO-ORDER-00${index + 1}`,
        action: detail.action.toUpperCase(),
        symbol: detail.symbol,
        name: detail.name,
        quantity: detail.volume,
        price: detail.price,
        amount: detail.amount,
        status: 'submitted',
      },
    },
    null,
    2,
  ),
}))

const displayTradeDetails = computed<TradeDetail[]>(() => {
  if (!selectedRun.value) {
    return []
  }
  if (selectedRun.value.tradeDetails.length > 0) {
    return selectedRun.value.tradeDetails
  }
  if (!import.meta.env.DEV) {
    return []
  }
  return demoTradeDetails
})

const activePreview = computed(() => {
  if (typeof activePreviewIndex.value !== 'number') {
    return null
  }
  if (activePreviewIndex.value < 0) {
    return demoTradePreviews.find((item) => item.preview_index === activePreviewIndex.value) ?? null
  }
  if (!selectedRun.value) {
    return null
  }
  return selectedRun.value.rawToolPreviews.find((item) => item.preview_index === activePreviewIndex.value) ?? null
})

const historyDateDisplay = computed(() => {
  if (!selectedDate.value) {
    return '年/月/日'
  }

  const [year, month, day] = selectedDate.value.split('-')
  if (!year || !month || !day) {
    return '年/月/日'
  }

  return `${year}年/${month}月/${day}日`
})

function openHistoryDatePicker() {
  const input = historyDateInput.value
  if (!input) {
    return
  }

  if ('showPicker' in input && typeof input.showPicker === 'function') {
    input.showPicker()
    return
  }

  input.focus()
  input.click()
}

function handleSelectRun(runId: number, runs: typeof todayRuns.value) {
  const target = runs.find((item) => item.id === runId)
  if (!target) {
    return
  }
  void selectRun(target)
}

function focusPreview(index: number | null) {
  if (typeof index !== 'number') {
    return
  }
  activePreviewIndex.value = activePreviewIndex.value === index ? null : index
}

function clearPreviewFocus() {
  activePreviewIndex.value = null
}

function handleOutputSurfaceClick(event: MouseEvent) {
  if (!activePreview.value) {
    return
  }
  const target = event.target
  if (!(target instanceof HTMLElement)) {
    return
  }
  if (target.closest('.compact-item-button')) {
    return
  }
  if (target.closest('.raw-output-content')) {
    return
  }
  clearPreviewFocus()
}

function handleKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape' && activePreviewIndex.value !== null) {
    clearPreviewFocus()
  }
}

watch(
  () => selectedRun.value?.id,
  () => {
    activePreviewIndex.value = null
  },
  { immediate: true },
)

onMounted(() => {
  window.addEventListener('keydown', handleKeydown)
})

onBeforeUnmount(() => {
  window.removeEventListener('keydown', handleKeydown)
})

</script>

<style scoped>
.compact-item-button {
  width: 100%;
  display: flex;
  align-items: flex-start;
  gap: 6px;
  height: 25px;
  min-height: 25px;
  padding: 5px 8px;
  box-sizing: border-box;
  border: none;
  background: rgba(0, 0, 0, 0.2);
  border-radius: 4px;
  text-align: left;
  cursor: pointer;
  font: inherit;
  font-size: 10px;
  line-height: 1.25;
  color: inherit;
  appearance: none;
  -webkit-appearance: none;
}

.compact-item-button:hover {
  background: rgba(255, 255, 255, 0.04);
}

.compact-item-button.active {
  box-shadow: none;
}

.compact-item-button.active .item-name,
.compact-item-button.active .trade-text-action {
  color: #f6fbff;
}

.compact-item-button.active .item-summary,
.compact-item-button.active .trade-text-summary {
  color: #a9c7e8;
}

.compact-item-button .item-name,
.compact-item-button .trade-text-action {
  font-size: 10.5px;
  line-height: 1.2;
}

.compact-item-button .item-summary,
.compact-item-button .trade-text-summary {
  font-size: 10px;
  line-height: 1.2;
}

.analysis-compact-list {
  height: calc(25px * 3 + 4px * 2);
  min-height: calc(25px * 3 + 4px * 2);
}

.trade-text-summary {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  -webkit-line-clamp: unset;
  -webkit-box-orient: unset;
}

.output-surface {
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  padding: 12px;
  overflow: hidden;
  height: 700px;
  border: 1px solid rgba(145, 170, 214, 0.12);
  border-radius: 8px;
  background: rgba(15, 23, 42, 0.4);
}

.markdown-content {
  flex: 1 1 auto;
  min-height: 0;
  overflow-y: auto;
  scrollbar-width: thin;
  scrollbar-color: transparent transparent;
}

.markdown-content:hover {
  scrollbar-color: rgba(145, 170, 214, 0.3) rgba(15, 23, 42, 0.05);
}

.markdown-content::-webkit-scrollbar {
  width: 5px;
}

.markdown-content::-webkit-scrollbar-track {
  background: transparent;
}

.markdown-content::-webkit-scrollbar-thumb {
  background: rgba(145, 170, 214, 0.25);
  border-radius: 10px;
}

.markdown-content:hover::-webkit-scrollbar-thumb {
  background: rgba(145, 170, 214, 0.45);
}

.markdown-content :deep(p:first-child) {
  margin-top: 0;
}

.markdown-content :deep(p:last-child) {
  margin-bottom: 0;
}

.raw-output-content {
  flex: 1 1 auto;
  min-height: 0;
  margin: 0;
  padding: 0;
  overflow: auto;
  scrollbar-width: thin;
  scrollbar-color: transparent transparent;
  color: #e2e8f0;
  font-size: 12px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-word;
}

.raw-output-content:hover {
  scrollbar-color: rgba(145, 170, 214, 0.3) rgba(15, 23, 42, 0.05);
}

.raw-output-content::-webkit-scrollbar {
  width: 5px;
}

.raw-output-content::-webkit-scrollbar-track {
  background: transparent;
}

.raw-output-content::-webkit-scrollbar-thumb {
  background: rgba(145, 170, 214, 0.25);
  border-radius: 10px;
}

.raw-output-content:hover::-webkit-scrollbar-thumb {
  background: rgba(145, 170, 214, 0.45);
}

.output-surface > .detail-empty-state {
  flex: 1 1 auto;
  min-height: 0;
}
</style>
