<template>
<div class="tab-content">
        <section class="content-grid content-grid-primary">
          <section class="panel schedule-overview-wrapper">
            <div class="panel-head">
              <div class="head-main">
                <h2>当前定时任务</h2>
                <p class="section-kicker">Live Schedules</p>
              </div>

            </div>

            <div v-if="activeScheduleCards.length" class="schedule-overview-list">
              <article v-for="task in activeScheduleCards" :key="task.id" class="schedule-overview-card">
                <span class="overview-tag" :class="task.category === '交易任务' ? 'tag-trade' : 'tag-analysis'">{{ task.category }}</span>
                <strong>{{ task.name }}</strong>
                <p>交易日 {{ task.displayTime }}</p>
              </article>
            </div>
            <div v-else class="empty-state">
              <p>当前没有已启用的定时任务。</p>
            </div>
            <div v-if="nextScheduledTask" class="next-run-info">
              <span class="next-run-prefix">下次运行：</span>
              <span
                class="next-run-category"
                :class="nextScheduledTask.category === '交易任务' ? 'is-trade' : 'is-analysis'"
              >
                {{ nextScheduledTask.category }}
              </span>
              <strong class="next-run-name">{{ nextScheduledTask.name }}</strong>
              <span class="next-run-time">{{ formatWeekdayMinuteTime(nextScheduledTask.nextRunAt) }}</span>
            </div>
          </section>

          <section class="panel tasks-panel">
            <div class="panel-head">
              <div class="head-main">
                <h2>定时任务设置</h2>
                <p class="section-kicker">Schedules</p>
              </div>
            </div>

            <div v-if="errorMessage" class="error-banner">{{ errorMessage }}</div>

            <div class="schedule-settings">
              <section class="schedule-section">
                <header class="section-header">
                  <h3>分析任务</h3>
                  <p class="section-subtitle">配置自动执行的 AI 分析任务</p>
                </header>

                <div class="task-grid">
                  <!-- 盘前分析 -->
                  <article class="task-card" :class="{ 'is-active': scheduleSettings.preMarket.enabled }">
                    <div class="task-card-header">
                      <div class="task-card-meta">
                        <h4 class="task-card-name">盘前分析</h4>
                        <p class="task-card-desc">开盘前的市场预测与策略建议</p>
                      </div>
                      <label class="switch">
                        <input type="checkbox" v-model="scheduleSettings.preMarket.enabled" />
                        <span class="switch-track"></span>
                      </label>
                    </div>
                    <div class="task-card-body" :class="{ 'is-disabled': !scheduleSettings.preMarket.enabled }">
                      <div class="task-field">
                        <span class="field-label">执行时间</span>
                        <div class="choice-chip-group" :class="{ 'is-disabled': !scheduleSettings.preMarket.enabled }">
                          <button
                            v-for="option in fixedTaskTimeOptions.preMarket.options"
                            :key="`pre-${option.label}`"
                            type="button"
                            class="choice-chip"
                            :class="{ 'is-active': scheduleSettings.preMarket.hour === option.hour && scheduleSettings.preMarket.minute === option.minute }"
                            :disabled="!scheduleSettings.preMarket.enabled"
                            @click="setFixedTaskTime('preMarket', option)"
                          >
                            {{ option.label }}
                          </button>
                        </div>
                      </div>
                      <div class="task-field">
                        <span class="field-label">分析提示词 <small>{{ scheduleSettings.preMarket.prompt.length }}字</small></span>
                        <textarea 
                          v-model="scheduleSettings.preMarket.prompt" 
                          rows="3"
                          @input="autoResizeTextarea($event)"
                          :disabled="!scheduleSettings.preMarket.enabled"
                        ></textarea>
                      </div>
                    </div>
                  </article>

                  <!-- 午间复盘 -->
                  <article class="task-card" :class="{ 'is-active': scheduleSettings.midday.enabled }">
                    <div class="task-card-header">
                      <div class="task-card-meta">
                        <h4 class="task-card-name">午间复盘</h4>
                        <p class="task-card-desc">中午时段的市场动态追踪</p>
                      </div>
                      <label class="switch">
                        <input type="checkbox" v-model="scheduleSettings.midday.enabled" />
                        <span class="switch-track"></span>
                      </label>
                    </div>
                    <div class="task-card-body" :class="{ 'is-disabled': !scheduleSettings.midday.enabled }">
                      <div class="task-field">
                        <span class="field-label">执行时间</span>
                        <div class="choice-chip-group" :class="{ 'is-disabled': !scheduleSettings.midday.enabled }">
                          <button
                            v-for="option in fixedTaskTimeOptions.midday.options"
                            :key="`mid-${option.label}`"
                            type="button"
                            class="choice-chip"
                            :class="{ 'is-active': scheduleSettings.midday.hour === option.hour && scheduleSettings.midday.minute === option.minute }"
                            :disabled="!scheduleSettings.midday.enabled"
                            @click="setFixedTaskTime('midday', option)"
                          >
                            {{ option.label }}
                          </button>
                        </div>
                      </div>
                      <div class="task-field">
                        <span class="field-label">分析提示词 <small>{{ scheduleSettings.midday.prompt.length }}字</small></span>
                        <textarea 
                          v-model="scheduleSettings.midday.prompt" 
                          rows="3"
                          @input="autoResizeTextarea($event)"
                          :disabled="!scheduleSettings.midday.enabled"
                        ></textarea>
                      </div>
                    </div>
                  </article>

                  <!-- 收盘分析 -->
                  <article class="task-card" :class="{ 'is-active': scheduleSettings.postMarket.enabled }">
                    <div class="task-card-header">
                      <div class="task-card-meta">
                        <h4 class="task-card-name">收盘分析</h4>
                        <p class="task-card-desc">收盘后的全面总结与回顾</p>
                      </div>
                      <label class="switch">
                        <input type="checkbox" v-model="scheduleSettings.postMarket.enabled" />
                        <span class="switch-track"></span>
                      </label>
                    </div>
                    <div class="task-card-body" :class="{ 'is-disabled': !scheduleSettings.postMarket.enabled }">
                      <div class="task-field">
                        <span class="field-label">执行时间</span>
                        <div class="choice-chip-group" :class="{ 'is-disabled': !scheduleSettings.postMarket.enabled }">
                          <button
                            v-for="option in fixedTaskTimeOptions.postMarket.options"
                            :key="`post-${option.label}`"
                            type="button"
                            class="choice-chip"
                            :class="{ 'is-active': scheduleSettings.postMarket.hour === option.hour && scheduleSettings.postMarket.minute === option.minute }"
                            :disabled="!scheduleSettings.postMarket.enabled"
                            @click="setFixedTaskTime('postMarket', option)"
                          >
                            {{ option.label }}
                          </button>
                        </div>
                      </div>
                      <div class="task-field">
                        <span class="field-label">分析提示词 <small>{{ scheduleSettings.postMarket.prompt.length }}字</small></span>
                        <textarea 
                          v-model="scheduleSettings.postMarket.prompt" 
                          rows="3"
                          @input="autoResizeTextarea($event)"
                          :disabled="!scheduleSettings.postMarket.enabled"
                        ></textarea>
                      </div>
                    </div>
                  </article>
                </div>
              </section>

              <section class="schedule-section">
                <header class="section-header">
                  <h3>交易任务</h3>
                  <p class="section-subtitle">配置交易时段内的定时任务频率</p>
                </header>

                <div class="run-list">
                  <article class="run-item">
                    <div class="run-main">
                      <div class="run-meta">
                        <h4 class="run-name">上午运行</h4>
                        <p class="run-time">09:30 - 11:30</p>
                      </div>
                      <div class="run-control">
                        <div class="choice-chip-group run-count-group">
                          <button
                            v-for="count in runCountOptions"
                            :key="`morning-${count}`"
                            type="button"
                            class="choice-chip"
                            :class="{ 'is-active': scheduleSettings.morning.runCount === count }"
                            @click="scheduleSettings.morning.runCount = count"
                          >
                            {{ count }}次
                          </button>
                        </div>
                      </div>
                    </div>
                    <div class="run-schedule">
                      <span class="schedule-label">计划运行时间</span>
                      <div class="schedule-badges">
                        <span 
                          v-for="(time, index) in getMorningRunTimes().split(', ')" 
                          :key="'m'+index"
                          class="badge"
                        >{{ time }}</span>
                      </div>
                    </div>
                    <div class="run-prompt">
                      <span class="prompt-label">分析提示词 <small>{{ scheduleSettings.morning.prompt.length }}字</small></span>
                      <textarea 
                        v-model="scheduleSettings.morning.prompt" 
                        rows="2"
                        @input="autoResizeTextarea($event)"
                      ></textarea>
                    </div>
                  </article>

                  <article class="run-item">
                    <div class="run-main">
                      <div class="run-meta">
                        <h4 class="run-name">下午运行</h4>
                        <p class="run-time">13:00 - 15:00</p>
                      </div>
                      <div class="run-control">
                        <div class="choice-chip-group run-count-group">
                          <button
                            v-for="count in runCountOptions"
                            :key="`afternoon-${count}`"
                            type="button"
                            class="choice-chip"
                            :class="{ 'is-active': scheduleSettings.afternoon.runCount === count }"
                            @click="scheduleSettings.afternoon.runCount = count"
                          >
                            {{ count }}次
                          </button>
                        </div>
                      </div>
                    </div>
                    <div class="run-schedule">
                      <span class="schedule-label">计划运行时间</span>
                      <div class="schedule-badges">
                        <span 
                          v-for="(time, index) in getAfternoonRunTimes().split(', ')" 
                          :key="'a'+index"
                          class="badge"
                        >{{ time }}</span>
                      </div>
                    </div>
                    <div class="run-prompt">
                      <span class="prompt-label">分析提示词 <small>{{ scheduleSettings.afternoon.prompt.length }}字</small></span>
                      <textarea 
                        v-model="scheduleSettings.afternoon.prompt" 
                        rows="2"
                        @input="autoResizeTextarea($event)"
                      ></textarea>
                    </div>
                  </article>
                </div>
              </section>
            </div>

            <div class="panel-actions">
              <button class="button primary" :class="{ 'is-loading': busy }" @click="saveScheduleSettings" :disabled="busy">保存设置</button>
            </div>
          </section>
        </section>
      </div>
</template>

<script setup lang="ts">
import { onMounted, watch } from 'vue'
import { storeToRefs } from 'pinia'

import { useAppStore } from '@/stores/legacy'
import { useScheduleForm } from '@/composables/useScheduleForm'
import { formatWeekdayMinuteTime } from '@/utils/formatters'

const store = useAppStore()
const { busy, schedules, errorMessage, activeScheduleCards, nextScheduledTask } = storeToRefs(store)
const {
  scheduleSettings,
  fixedTaskTimeOptions,
  runCountOptions,
  syncFromSchedules,
  buildPayload,
  setFixedTaskTime,
  autoResizeTextarea,
  getMorningRunTimes,
  getAfternoonRunTimes,
} = useScheduleForm()

async function saveScheduleSettings() {
  await store.saveSchedule(buildPayload(schedules.value))
}

watch(
  schedules,
  (value) => {
    syncFromSchedules(value)
  },
  { immediate: true },
)

onMounted(async () => {
  if (schedules.value.length === 0) {
    await store.loadSchedule()
  }
})

</script>
