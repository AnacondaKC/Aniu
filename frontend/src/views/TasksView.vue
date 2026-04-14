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
                <div v-if="todayRuns.length && todayHasMore" class="panel-actions">
                  <button class="button ghost" :class="{ 'is-loading': todayLoadingMore }" @click="loadMoreTodayRuns" :disabled="todayLoadingMore">
                    加载更多
                  </button>
                </div>
                <div v-else-if="todayFailedCount > 0 && !showFailedRuns" class="run-grid-empty">
                  今日失败记录已隐藏，可点击右侧“失败{{ todayFailedCount }}次”查看。
                </div>
                <div v-else class="run-grid-empty">
                  今日暂无运行记录。
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
                    <div v-for="(api, idx) in selectedRun.apiDetails" :key="idx" class="compact-item api-item">
                      <div class="compact-main api-main">
                        <span class="item-name" :title="api.name">{{ api.name }}</span>
                        <span class="item-summary" :title="api.summary">{{ api.summary }}</span>
                      </div>
                    </div>
                  </div>
                  <div v-else-if="selectedRun.detailLoaded" class="detail-empty-state">
                    本次分析没有生成可展示的接口调用记录。
                  </div>
                </div>
              </div>

              <!-- 第三列：交易执行 -->
              <div class="detail-column trade-column">
                <h4 class="column-title">交易执行 ({{ selectedRun.tradeCount }})</h4>
                <div class="detail-column-body">
                  <div class="compact-list analysis-compact-list" v-if="selectedRun.tradeDetails.length">
                    <div v-for="(trade, idx) in selectedRun.tradeDetails" :key="idx" class="compact-item trade-item">
                      <div class="compact-main trade-main">
                        <span class="trade-text-action" :class="trade.action">{{ trade.action_text }}</span>
                        <span class="trade-text-summary" :title="trade.summary">{{ trade.summary }}</span>
                      </div>
                    </div>
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

            <!-- 分析输出内容 -->
            <div class="output-section" v-if="selectedRun?.output">
              <h4 class="detail-title">分析输出</h4>
              <div v-if="renderedOutputLoading" class="detail-empty-state">
                正在渲染分析输出...
              </div>
              <div v-else class="markdown-panel" v-html="renderedOutputHtml"></div>
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
import { computed, onMounted, ref } from 'vue'

import { useAnalysisRuns } from '@/composables/useAnalysisRuns'
import { api } from '@/services/api'
import { useAppStore } from '@/stores/legacy'
import { formatShortTime, formatTime, statusTone } from '@/utils/formatters'

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

</script>
