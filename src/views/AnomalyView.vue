<template>
  <div class="page">
    <div class="header">
      <div>
        <div class="title">异常行程诊断</div>
        <div class="subtitle">输入 trip_id，对单条轨迹进行异常检测、地图定位、回放跳转和解释型诊断。</div>
      </div>

      <div class="controls">
        <SearchSelect
          v-model="tripId"
          :fetch-options="fetchTripSuggestions"
          :min-chars="1"
          :max-items="200"
          placeholder="选择或输入 trip_id，例如 2399062"
        />
        <button class="btn" :disabled="!tripId || loading" @click="submitDiagnosis">诊断</button>
      </div>
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div class="summary-grid">
      <div class="risk-card" :class="`risk-card--${summary?.risk_level || 'low'}`">
        <div class="risk-label">风险等级</div>
        <div class="risk-value">{{ riskLabel(summary?.risk_level) }}</div>
        <div class="risk-score">异常评分 {{ summary?.anomaly_score ?? '-' }}</div>
        <div class="risk-total">异常事件 {{ summary?.total_events ?? 0 }} 个</div>
      </div>

      <div class="metrics-card">
        <div class="card-title">关键指标</div>
        <div class="metrics">
          <div class="metric">
            <span>起终点直线距离</span>
            <b>{{ fmt(metrics?.direct_distance_km) }} km</b>
          </div>
          <div class="metric">
            <span>实际距离</span>
            <b>{{ fmt(metrics?.actual_distance_km) }} km</b>
          </div>
          <div class="metric">
            <span>绕行系数</span>
            <b>{{ fmt(metrics?.directness_ratio) }}</b>
          </div>
          <div class="metric">
            <span>最高速度</span>
            <b>{{ fmt(metrics?.max_speed_kph) }} km/h</b>
          </div>
          <div class="metric">
            <span>异常停留总时长</span>
            <b>{{ fmtDuration(metrics?.stop_seconds_total) }}</b>
          </div>
          <div class="metric">
            <span>重复道路占比</span>
            <b>{{ fmtRatio(metrics?.repeated_road_ratio) }}</b>
          </div>
        </div>
      </div>
    </div>

    <div class="content-grid">
      <div class="card map-card">
        <AmapAnomalyMap
          :trip="trip"
          :segments="segments"
          :events="events"
          :selected-event-id="selectedEventId"
          @select-event="selectEvent"
        />
      </div>

      <div class="side-panel">
        <div class="card trip-card">
          <div class="card-title">行程信息</div>
          <div v-if="trip" class="kv-list">
            <div class="kv"><span>行程 ID</span><b>{{ trip.trip_id }}</b></div>
            <div class="kv"><span>车辆 ID</span><b>{{ trip.devid ?? '-' }}</b></div>
            <div class="kv"><span>行程日期</span><b>{{ trip.log_date }}</b></div>
            <div class="kv"><span>距离</span><b>{{ fmt(trip.distance_km) }} km</b></div>
            <div class="kv"><span>时长</span><b>{{ fmtDuration(trip.duration_seconds) }}</b></div>
            <div class="kv"><span>轨迹点数</span><b>{{ trip.points?.length ?? 0 }}</b></div>
          </div>
          <div v-else class="muted">暂无行程数据。</div>
        </div>

        <div class="card list-card">
          <div class="card-title">异常事件</div>
          <div v-if="events.length" class="event-list">
            <button
              v-for="event in events"
              :key="event.id"
              type="button"
              class="event-item"
              :class="{ active: event.id === selectedEventId }"
              @click="selectEvent(event.id)"
            >
              <div class="event-item__head">
                <div class="event-item__title">
                  <span class="event-dot" :style="{ backgroundColor: event.color }"></span>
                  <span>{{ event.title }}</span>
                </div>
                <span class="severity" :class="`severity--${event.severity}`">{{ severityLabel(event.severity) }}</span>
              </div>
              <div class="event-item__desc">{{ event.description }}</div>
              <div class="event-item__meta">
                <span>{{ typeLabel(event.type) }}</span>
                <span>#{{ event.start_index }} - #{{ event.end_index }}</span>
              </div>
            </button>
          </div>
          <div v-else class="muted">当前行程未检测到异常事件。</div>
        </div>

        <div class="card detail-card">
          <div class="card-title">异常详情</div>
          <div v-if="selectedEvent" class="detail">
            <div class="detail-title">
              <span class="event-dot" :style="{ backgroundColor: selectedEvent.color }"></span>
              <b>{{ selectedEvent.title }}</b>
            </div>
            <p class="detail-desc">{{ selectedEvent.description }}</p>
            <div class="kv-list">
              <div class="kv"><span>严重级别</span><b>{{ severityLabel(selectedEvent.severity) }}</b></div>
              <div class="kv"><span>开始时间</span><b>{{ formatDateTime(selectedEvent.start_time) }}</b></div>
              <div class="kv"><span>结束时间</span><b>{{ formatDateTime(selectedEvent.end_time) }}</b></div>
            </div>
            <div class="evidence-title">触发证据</div>
            <div class="evidence-list">
              <div v-for="item in formatEvidence(selectedEvent.evidence)" :key="item.key" class="evidence-item">
                <span>{{ item.label }}</span>
                <b>{{ item.value }}</b>
              </div>
            </div>
          </div>
          <div v-else class="muted">点击左侧异常列表，可查看解释和地图定位。</div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">统计图</div>
      <div class="charts">
        <div ref="chartCountEl" class="chart"></div>
        <div ref="chartSpeedEl" class="chart"></div>
      </div>
    </div>

    <div class="card">
      <div class="leaderboard-head">
        <div>
          <div class="card-title">异常路段分布</div>
          <div class="leaderboard-subtitle">
            基于最近 {{ roadDistribution?.sample_trip_count ?? 0 }} 条行程聚合命中的异常道路。
          </div>
        </div>
        <button class="btn btn-secondary" :disabled="roadLoading" @click="loadRoadDistribution">刷新路段</button>
      </div>

      <div v-if="roadError" class="muted">{{ roadError }}</div>
      <div v-else-if="roadDistribution?.items?.length" class="road-grid">
        <AmapRoadDistributionMap
          :items="roadDistribution.items"
          :selected-road-id="selectedRoadId"
          @select-road="selectRoad"
        />

        <div class="road-list">
          <div
            v-for="item in roadDistribution.items"
            :key="`${item.road_id}-${item.sample_trip_id}`"
            class="road-item"
            :class="{ active: item.road_id === selectedRoadId }"
            @click="selectRoad(item.road_id)"
          >
            <div class="road-item__head">
              <div class="road-item__title">
                <span class="event-dot" :style="{ backgroundColor: TYPE_COLOR[item.dominant_type] || '#4f7cff' }"></span>
                <span>道路 {{ item.road_id }}</span>
              </div>
              <span class="leaderboard-badge">{{ typeLabel(item.dominant_type) }}</span>
            </div>

            <div class="road-item__meta">
              <span>异常命中 {{ item.occurrence_count }}</span>
              <span>涉及行程 {{ item.trip_count }}</span>
              <span>最高级别 {{ severityLabel(item.max_severity) }}</span>
              <span>平均速度 {{ fmt(item.avg_speed_kph) }} km/h</span>
            </div>

            <div class="road-item__actions">
              <button
                v-if="item.sample_trip_id"
                type="button"
                class="btn btn-compact"
                @click.stop="openRoadSampleTrip(item.sample_trip_id)"
              >
                查看样例行程
              </button>
            </div>
          </div>
        </div>
      </div>
      <div v-else class="muted">当前样本范围内还没有聚合出异常路段。</div>
    </div>

    <div class="card">
      <div class="leaderboard-head">
        <div>
          <div class="card-title">异常车辆排行</div>
          <div class="leaderboard-subtitle">
            基于最近 {{ vehicleRanking?.sample_trip_count ?? 0 }} 条行程，每车最多分析 {{ rankingPerVehicle }} 条。
          </div>
        </div>
        <button class="btn btn-secondary" :disabled="rankingLoading" @click="loadVehicleRanking">刷新排行</button>
      </div>

      <div v-if="rankingError" class="muted">{{ rankingError }}</div>
      <div v-else-if="vehicleRanking?.items?.length" class="leaderboard-list">
        <div
          v-for="(item, index) in vehicleRanking.items"
          :key="`${item.device_id}-${item.worst_trip_id}`"
          class="leaderboard-item"
        >
          <div class="leaderboard-rank">#{{ index + 1 }}</div>

          <div class="leaderboard-main">
            <div class="leaderboard-title-row">
              <button type="button" class="link-button" @click="openCar(item.device_id)">
                车辆 {{ item.device_id }}
              </button>
              <span class="leaderboard-badge">{{ typeLabel(item.dominant_type) }}</span>
            </div>
            <div class="leaderboard-meta">
              <span>异常事件 {{ item.total_events }}</span>
              <span>高风险行程 {{ item.high_risk_trips }}</span>
              <span>平均评分 {{ fmt(item.avg_anomaly_score) }}</span>
              <span>采样行程 {{ item.trip_count }}</span>
            </div>
          </div>

          <div class="leaderboard-actions">
            <button
              v-if="item.worst_trip_id"
              type="button"
              class="btn btn-compact"
              @click="openWorstTrip(item.worst_trip_id)"
            >
              查看最差行程
            </button>
          </div>
        </div>
      </div>
      <div v-else class="muted">当前样本范围内还没有统计到异常车辆。</div>
    </div>
  </div>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import * as echarts from 'echarts'

import { api } from '@/lib/api'
import SearchSelect from '@/components/SearchSelect.vue'
import AmapAnomalyMap from '@/components/AmapAnomalyMap.vue'
import AmapRoadDistributionMap from '@/components/AmapRoadDistributionMap.vue'

const route = useRoute()
const router = useRouter()

const TYPE_LABEL = {
  detour: '绕路',
  stop: '异常停留',
  speed_jump: '速度突变',
  drift: '定位漂移',
  jump_point: '异常跳点',
}

const TYPE_COLOR = {
  detour: '#f59e0b',
  stop: '#ef4444',
  speed_jump: '#8b5cf6',
  drift: '#06b6d4',
  jump_point: '#f97316',
}

const EVIDENCE_LABEL = {
  duration_seconds: '停留时长',
  avg_speed_kph: '平均速度',
  radius_meters: '范围半径',
  speed_before_kph: '变化前速度',
  speed_after_kph: '变化后速度',
  delta_kph: '速度变化量',
  detour_ratio: '局部离群比',
  max_segment_distance_km: '局部最长位移',
  max_segment_speed_kph: '局部最高速度',
  path_km: '局部累计路径',
  net_km: '净位移',
  actual_distance_km: '实际距离',
  direct_distance_km: '直线距离',
  directness_ratio: '绕行系数',
  repeated_road_ratio: '重复道路占比',
  backtrack_count: '回摆次数',
}

const tripId = ref('2399062')
const loading = ref(false)
const error = ref('')
const diagnosis = ref(null)
const selectedEventId = ref('')
const selectedRoadId = ref(null)

const vehicleRanking = ref(null)
const rankingLoading = ref(false)
const rankingError = ref('')
const rankingTripSample = 300
const rankingPerVehicle = 5

const roadDistribution = ref(null)
const roadLoading = ref(false)
const roadError = ref('')
const roadTripSample = 300
const roadLimit = 12

const chartCountEl = ref(null)
const chartSpeedEl = ref(null)
let chartCount = null
let chartSpeed = null

const trip = computed(() => diagnosis.value?.trip ?? null)
const summary = computed(() => diagnosis.value?.summary ?? null)
const metrics = computed(() => diagnosis.value?.metrics ?? null)
const events = computed(() => diagnosis.value?.events ?? [])
const segments = computed(() => diagnosis.value?.segments ?? [])
const selectedEvent = computed(() => events.value.find((item) => item.id === selectedEventId.value) || null)

function fmt(n) {
  if (n === null || n === undefined || Number.isNaN(Number(n))) return '-'
  return Number(n).toFixed(2)
}

function fmtRatio(n) {
  if (n === null || n === undefined || Number.isNaN(Number(n))) return '-'
  return `${(Number(n) * 100).toFixed(1)}%`
}

function fmtDuration(sec) {
  if (!sec && sec !== 0) return '-'
  const s = Math.max(0, Math.floor(Number(sec)))
  const hh = String(Math.floor(s / 3600)).padStart(2, '0')
  const mm = String(Math.floor((s % 3600) / 60)).padStart(2, '0')
  const ss = String(s % 60).padStart(2, '0')
  return `${hh}:${mm}:${ss}`
}

function riskLabel(level) {
  return { low: '低风险', medium: '中风险', high: '高风险' }[level] || '低风险'
}

function severityLabel(level) {
  return { low: '低', medium: '中', high: '高' }[level] || '低'
}

function typeLabel(type) {
  if (!type) return '未命中'
  return TYPE_LABEL[type] || '正常'
}

function formatDateTime(value) {
  if (!value) return '-'
  return String(value).replace('T', ' ')
}

function formatEvidence(evidence = {}) {
  return Object.entries(evidence).map(([key, value]) => {
    let formatted = value
    if (typeof value === 'number') {
      if (key === 'repeated_road_ratio') {
        formatted = fmtRatio(value)
      } else if (key.includes('ratio')) {
        formatted = Number(value).toFixed(2)
      } else if (key.includes('seconds')) {
        formatted = fmtDuration(value)
      } else {
        formatted = Number(value).toFixed(2)
      }
    }
    return {
      key,
      label: EVIDENCE_LABEL[key] || key,
      value: formatted,
    }
  })
}

function handleResize() {
  chartCount?.resize()
  chartSpeed?.resize()
}

function renderCharts() {
  const counts = summary.value?.event_counts || []
  const speedSeries = segments.value.map((item) => item.speed_kph ?? 0)
  const labels = counts.map((item) => typeLabel(item.type))
  const values = counts.map((item) => item.count)

  chartCount?.dispose()
  chartSpeed?.dispose()
  chartCount = chartCountEl.value ? echarts.init(chartCountEl.value) : null
  chartSpeed = chartSpeedEl.value ? echarts.init(chartSpeedEl.value) : null

  chartCount?.setOption({
    backgroundColor: 'transparent',
    title: { text: '异常类型分布', textStyle: { color: '#102033', fontSize: 12 } },
    tooltip: { trigger: 'axis' },
    grid: { left: 42, right: 18, top: 28, bottom: 32 },
    xAxis: {
      type: 'category',
      data: labels,
      axisLabel: { color: 'rgba(16,32,51,0.68)' },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: 'rgba(16,32,51,0.68)' },
      splitLine: { lineStyle: { color: 'rgba(148,163,184,0.14)' } },
    },
    series: [{
      type: 'bar',
      data: values,
      barMaxWidth: 28,
      itemStyle: {
        color: (params) => {
          const target = counts.find((item) => typeLabel(item.type) === params.name)
          return TYPE_COLOR[target?.type] || 'rgba(79,124,255,0.68)'
        },
      },
    }],
  })

  const eventPoints = events.value.map((item) => {
    const segment = segments.value[Math.max(0, Math.min(item.focus_index, segments.value.length - 1))]
    return {
      name: item.title,
      value: segment?.speed_kph ?? 0,
      xAxis: Math.max(0, Math.min(item.focus_index, speedSeries.length - 1)),
      yAxis: segment?.speed_kph ?? 0,
      itemStyle: { color: item.color },
    }
  })

  chartSpeed?.setOption({
    backgroundColor: 'transparent',
    title: { text: '速度剖面与异常位置', textStyle: { color: '#102033', fontSize: 12 } },
    tooltip: { trigger: 'axis' },
    grid: { left: 42, right: 18, top: 28, bottom: 32 },
    xAxis: {
      type: 'category',
      data: speedSeries.map((_, index) => index + 1),
      axisLabel: { color: 'rgba(16,32,51,0.68)' },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: 'rgba(16,32,51,0.68)' },
      splitLine: { lineStyle: { color: 'rgba(148,163,184,0.14)' } },
    },
    series: [{
      type: 'line',
      data: speedSeries,
      smooth: true,
      symbolSize: 4,
      lineStyle: { width: 3, color: 'rgba(79,124,255,0.78)' },
      itemStyle: { color: 'rgba(79,124,255,0.78)' },
      markPoint: {
        symbol: 'circle',
        symbolSize: 26,
        label: {
          color: '#fff',
          fontSize: 10,
          formatter: (params) => params.data?.name?.slice(0, 2) || '',
        },
        data: eventPoints,
      },
    }],
  })

  window.removeEventListener('resize', handleResize)
  window.addEventListener('resize', handleResize)
}

function selectEvent(eventId) {
  selectedEventId.value = eventId
}

function selectRoad(roadId) {
  selectedRoadId.value = roadId
}

async function loadDiagnosis(id = tripId.value) {
  error.value = ''
  loading.value = true
  try {
    const numericId = Number(id)
    if (!Number.isFinite(numericId)) {
      throw new Error('trip_id 必须是数字')
    }
    const response = await api.get(`/api/trips/${numericId}/diagnosis`)
    diagnosis.value = response.data
    selectedEventId.value = response.data?.events?.[0]?.id || ''
    await nextTick()
    renderCharts()
  } catch (e) {
    diagnosis.value = null
    selectedEventId.value = ''
    error.value = e?.response?.data?.detail || e?.message || String(e)
  } finally {
    loading.value = false
  }
}

async function loadVehicleRanking() {
  rankingError.value = ''
  rankingLoading.value = true
  try {
    const response = await api.get('/api/anomaly/vehicles', {
      params: {
        limit: 10,
        trip_sample: rankingTripSample,
        per_vehicle: rankingPerVehicle,
      },
    })
    vehicleRanking.value = response.data
  } catch (e) {
    vehicleRanking.value = null
    rankingError.value = e?.response?.data?.detail || e?.message || String(e)
  } finally {
    rankingLoading.value = false
  }
}

async function loadRoadDistribution() {
  roadError.value = ''
  roadLoading.value = true
  try {
    const response = await api.get('/api/anomaly/roads', {
      params: {
        limit: roadLimit,
        trip_sample: roadTripSample,
      },
    })
    roadDistribution.value = response.data
    const nextSelectedRoad = response.data?.items?.some((item) => item.road_id === selectedRoadId.value)
      ? selectedRoadId.value
      : response.data?.items?.[0]?.road_id ?? null
    selectedRoadId.value = nextSelectedRoad
  } catch (e) {
    roadDistribution.value = null
    selectedRoadId.value = null
    roadError.value = e?.response?.data?.detail || e?.message || String(e)
  } finally {
    roadLoading.value = false
  }
}

function submitDiagnosis() {
  const id = Number(tripId.value)
  if (!Number.isFinite(id)) {
    error.value = 'trip_id 必须是数字'
    return
  }
  const nextId = String(id)
  if (String(route.query?.id || '') === nextId) {
    loadDiagnosis(nextId)
    return
  }
  router.replace({ path: '/anomaly', query: { id: nextId } })
}

function openWorstTrip(worstTripId) {
  router.push({ path: '/anomaly', query: { id: String(worstTripId) } })
}

function openRoadSampleTrip(sampleTripId) {
  router.push({ path: '/anomaly', query: { id: String(sampleTripId) } })
}

function openCar(deviceId) {
  router.push({ path: '/car', query: { id: String(deviceId) } })
}

async function fetchTripSuggestions(keyword = '') {
  const trimmed = String(keyword || '').trim()
  if (!trimmed) return []
  const response = await api.get('/api/meta/trip-ids', {
    params: { q: trimmed, limit: 200 },
  })
  return response.data || []
}

onMounted(() => {
  const queryId = route.query?.id
  if (queryId) {
    tripId.value = String(queryId)
  }
  if (tripId.value) {
    loadDiagnosis(tripId.value)
  }
  loadRoadDistribution()
  loadVehicleRanking()
})

watch(
  () => route.query?.id,
  (queryId) => {
    if (!queryId) return
    tripId.value = String(queryId)
    loadDiagnosis(queryId)
  }
)

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  chartCount?.dispose()
  chartSpeed?.dispose()
})
</script>

<style scoped>
.page {
  display: grid;
  gap: 16px;
}

.header {
  display: flex;
  gap: 16px;
  justify-content: space-between;
  align-items: flex-start;
  flex-wrap: wrap;
}

.title {
  font-size: 18px;
  font-weight: 700;
}

.subtitle {
  margin-top: 4px;
  color: var(--text-muted);
  font-size: 12px;
}

.controls {
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: nowrap;
  width: 100%;
  min-width: 0;
}

.btn {
  height: 36px;
  padding: 0 14px;
  border-radius: 10px;
  border: 1px solid rgba(79, 124, 255, 0.22);
  background: rgba(79, 124, 255, 0.1);
  color: var(--text);
  cursor: pointer;
  flex: 0 0 auto;
  white-space: nowrap;
  box-shadow: var(--shadow-sm);
}

.btn:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}

.btn:hover:not(:disabled) {
  border-color: rgba(79, 124, 255, 0.3);
  background: rgba(79, 124, 255, 0.14);
}

.btn-secondary {
  background: rgba(148, 163, 184, 0.08);
  border-color: rgba(148, 163, 184, 0.22);
}

.btn-compact {
  height: 32px;
  padding: 0 12px;
}

.summary-grid {
  display: grid;
  grid-template-columns: 340px 1fr;
  gap: 12px;
}

.risk-card,
.metrics-card,
.card {
  border-radius: var(--radius-lg);
  padding: 16px;
  border: 1px solid var(--border);
  background: linear-gradient(180deg, var(--surface-2), var(--surface-0));
  box-shadow: var(--shadow-sm);
}

.risk-card {
  color: #fff;
  position: relative;
  overflow: hidden;
}

.risk-card::after {
  content: '';
  position: absolute;
  inset: auto -20% -45% auto;
  width: 180px;
  height: 180px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.12);
}

.risk-card--low {
  background: linear-gradient(135deg, #2f9f67, #55b17f);
}

.risk-card--medium {
  background: linear-gradient(135deg, #f59e0b, #f97316);
}

.risk-card--high {
  background: linear-gradient(135deg, #d95b73, #ef4444);
}

.risk-label {
  font-size: 12px;
  opacity: 0.84;
}

.risk-value {
  margin-top: 8px;
  font-size: 32px;
  font-weight: 700;
}

.risk-score,
.risk-total {
  margin-top: 6px;
  position: relative;
  z-index: 1;
}

.card-title {
  font-weight: 700;
  margin-bottom: 10px;
  color: var(--text);
}

.metrics {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}

.metric {
  padding: 10px 12px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.84);
  border: 1px solid rgba(148, 163, 184, 0.12);
}

.metric span {
  display: block;
  color: var(--text-muted);
  font-size: 12px;
}

.metric b {
  display: block;
  margin-top: 6px;
  font-size: 16px;
  font-weight: 700;
}

.content-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.65fr) minmax(360px, 1fr);
  gap: 12px;
}

.map-card {
  padding: 12px;
}

.side-panel {
  display: grid;
  gap: 12px;
}

.trip-card,
.list-card,
.detail-card {
  min-height: 0;
}

.event-list {
  display: grid;
  gap: 10px;
}

.event-item {
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: 14px;
  padding: 12px;
  text-align: left;
  background: rgba(255, 255, 255, 0.88);
  cursor: pointer;
  transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
}

.event-item:hover,
.event-item.active {
  border-color: rgba(79, 124, 255, 0.28);
  transform: translateY(-1px);
  box-shadow: var(--shadow-sm);
}

.event-item__head {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  align-items: center;
}

.event-item__title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 700;
}

.event-dot {
  width: 10px;
  height: 10px;
  border-radius: 999px;
  display: inline-block;
}

.event-item__desc {
  margin-top: 8px;
  color: var(--text-muted);
  font-size: 13px;
  line-height: 1.6;
}

.event-item__meta {
  margin-top: 10px;
  display: flex;
  justify-content: space-between;
  gap: 8px;
  color: var(--text-soft);
  font-size: 12px;
}

.severity {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 38px;
  height: 24px;
  padding: 0 8px;
  border-radius: 999px;
  font-size: 12px;
  color: #fff;
}

.severity--low {
  background: #2f9f67;
}

.severity--medium {
  background: #f59e0b;
}

.severity--high {
  background: #ef4444;
}

.detail-title {
  display: flex;
  align-items: center;
  gap: 8px;
}

.detail-desc {
  margin-top: 10px;
  color: var(--text-muted);
  line-height: 1.7;
}

.kv-list {
  display: grid;
  gap: 8px;
}

.kv {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  padding: 8px 0;
  border-bottom: 1px solid rgba(148, 163, 184, 0.12);
}

.kv span {
  color: var(--text-muted);
}

.evidence-title {
  margin-top: 14px;
  font-size: 13px;
  font-weight: 700;
}

.evidence-list {
  margin-top: 8px;
  display: grid;
  gap: 8px;
}

.evidence-item {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 12px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.84);
  border: 1px solid rgba(148, 163, 184, 0.12);
}

.evidence-item span {
  color: var(--text-muted);
}

.charts {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
}

.road-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(320px, 1fr);
  gap: 12px;
}

.road-list {
  display: grid;
  gap: 10px;
  align-content: start;
}

.road-item {
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: 14px;
  padding: 12px;
  background: rgba(255, 255, 255, 0.88);
  cursor: pointer;
  transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
}

.road-item:hover,
.road-item.active {
  border-color: rgba(79, 124, 255, 0.28);
  transform: translateY(-1px);
  box-shadow: var(--shadow-sm);
}

.road-item__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.road-item__title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 700;
}

.road-item__meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px 14px;
  margin-top: 10px;
  color: var(--text-muted);
  font-size: 12px;
}

.road-item__actions {
  display: flex;
  justify-content: flex-end;
  margin-top: 10px;
}

.chart {
  height: 320px;
  border-radius: 12px;
  border: 1px solid rgba(148, 163, 184, 0.12);
  background: rgba(255, 255, 255, 0.92);
}

.leaderboard-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}

.leaderboard-subtitle {
  color: var(--text-muted);
  font-size: 12px;
}

.leaderboard-list {
  display: grid;
  gap: 10px;
}

.leaderboard-item {
  display: grid;
  grid-template-columns: 52px minmax(0, 1fr) auto;
  gap: 12px;
  align-items: center;
  padding: 14px;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.88);
  border: 1px solid rgba(148, 163, 184, 0.14);
}

.leaderboard-rank {
  display: grid;
  place-items: center;
  width: 52px;
  height: 52px;
  border-radius: 16px;
  background: rgba(79, 124, 255, 0.1);
  color: var(--primary);
  font-weight: 700;
}

.leaderboard-main {
  min-width: 0;
}

.leaderboard-title-row {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.leaderboard-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px 14px;
  margin-top: 8px;
  color: var(--text-muted);
  font-size: 12px;
}

.leaderboard-badge {
  display: inline-flex;
  align-items: center;
  height: 24px;
  padding: 0 10px;
  border-radius: 999px;
  background: rgba(79, 124, 255, 0.1);
  color: var(--primary-strong);
  font-size: 12px;
  font-weight: 700;
}

.leaderboard-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.link-button {
  border: 0;
  padding: 0;
  background: transparent;
  color: var(--text);
  font: inherit;
  font-weight: 700;
  cursor: pointer;
}

.link-button:hover {
  color: var(--primary-strong);
}

.muted {
  color: var(--text-muted);
}

.error {
  padding: 10px 12px;
  border-radius: 12px;
  border: 1px solid rgba(217, 91, 115, 0.2);
  background: rgba(217, 91, 115, 0.08);
  color: #9f1f3d;
}

@media (max-width: 1280px) {
  .summary-grid,
  .content-grid,
  .charts,
  .road-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 980px) {
  .controls,
  .leaderboard-head {
    flex-wrap: wrap;
    align-items: center;
  }

  .metrics {
    grid-template-columns: 1fr 1fr;
  }

  .leaderboard-item {
    grid-template-columns: 1fr;
  }

  .leaderboard-rank {
    width: 44px;
    height: 44px;
  }
}

@media (max-width: 680px) {
  .metrics {
    grid-template-columns: 1fr;
  }
}
</style>
