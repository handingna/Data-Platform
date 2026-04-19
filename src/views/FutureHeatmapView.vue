<template>
  <div class="page">
    <div class="header">
      <div>
        <div class="title">轨迹拥堵预测平台</div>
        <div class="subtitle">选择一条轨迹，再查看该轨迹及周边道路的当前拥堵程度与未来时段趋势。</div>
      </div>

      <div class="controls">
        <SearchSelect
          v-model="tripId"
          :fetch-options="fetchTripSuggestions"
          :min-chars="1"
          :max-items="200"
          placeholder="选择或输入 trip_id，例如 286254"
        />

        <label class="label">
          预测时距
          <select v-model.number="forecastAfterMinutes" class="input">
            <option v-for="item in timeOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
          </select>
        </label>

        <label class="label">
          热力半径(米)
          <input v-model.number="radiusMeters" type="number" class="input small" min="10" max="300" step="10" />
        </label>

        <button class="btn" :disabled="!tripId || loading" @click="submitTrip">查询拥堵</button>
      </div>
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div class="summary-grid">
      <div class="stat-card">
        <div class="stat-label">轨迹编号</div>
        <div class="stat-value">{{ trip?.trip_id ?? '-' }}</div>
        <div class="stat-note">车辆 {{ trip?.devid ?? '-' }}</div>
      </div>

      <div class="stat-card">
        <div class="stat-label">行程日期</div>
        <div class="stat-value">{{ tripDateLabel }}</div>
        <div class="stat-note">{{ tripTimeRangeLabel }}</div>
      </div>

      <div class="stat-card">
        <div class="stat-label">预测时段</div>
        <div class="stat-value">{{ selectedTimeLabel }}</div>
        <div class="stat-note">行程后 {{ forecastAfterMinutes }} 分钟</div>
      </div>

      <div class="stat-card">
        <div class="stat-label">平均拥堵强度</div>
        <div class="stat-value">{{ fmtPercent(currentCongestionAvg) }}</div>
        <div class="stat-note">模型预测点 {{ displayPoints.length }}</div>
      </div>

      <div class="stat-card">
        <div class="stat-label">最高拥堵强度</div>
        <div class="stat-value">{{ fmtPercent(futureCongestionAvg) }}</div>
        <div class="stat-note">强度降序展示</div>
      </div>

      <div class="stat-card">
        <div class="stat-label">拥堵预警结论</div>
        <div class="stat-value">{{ overallRiskLabel }}</div>
        <div class="stat-note">{{ congestionStartOffsetLabel }} </div>
      </div>
    </div>

    <div class="card">
      <AmapFutureHeatmap
        :points="displayPoints"
        :path-points="tripPath"
        :radius-meters="radiusMeters"
        :min-fit-zoom="10"
      />
    </div>

    <div class="card">
      <div class="card-title">短时速度预测与拥堵预警</div>
      <div class="curve-summary-grid">
        <div class="mini-card">
          <div class="mini-label">预计拥堵开始</div>
          <div class="mini-value">{{ congestionStartLabel }}</div>
          <div class="mini-note">{{ congestionStartOffsetLabel }}</div>
        </div>
        <div class="mini-card">
          <div class="mini-label">预计拥堵总时长</div>
          <div class="mini-value">{{ congestionDurationLabel }}</div>
          <div class="mini-note">窗口 {{ speedCurveWindows.length }} 段</div>
        </div>
        <div class="mini-card">
          <div class="mini-label">平均预测速度</div>
          <div class="mini-value">{{ avgPredictedSpeedLabel }}</div>
          <div class="mini-note">最低 {{ minPredictedSpeedLabel }}</div>
        </div>
        <div class="mini-card">
          <div class="mini-label">整体风险等级</div>
          <div class="mini-value">{{ overallRiskLabel }}</div>
          <div class="mini-note">峰值强度 {{ fmtPercent(speedCurveSummary?.max_predicted_intensity ?? 0) }}</div>
        </div>
      </div>
      <div ref="speedCurveEl" class="curve-chart"></div>
    </div>

    <div class="card">
      <div class="card-title">拥堵热区明细</div>
      <div v-if="displayPoints.length" class="table">
        <div class="tr head">
          <div>排名</div>
          <div>经度</div>
          <div>纬度</div>
          <div>拥堵强度</div>
          <div>预测热度</div>
          <div>样本量</div>
        </div>
        <div v-for="(item, idx) in displayPoints.slice(0, 20)" :key="`${item.lon}-${item.lat}-${idx}`" class="tr">
          <div>#{{ idx + 1 }}</div>
          <div>{{ fmt(item.lon, 5) }}</div>
          <div>{{ fmt(item.lat, 5) }}</div>
          <div>{{ fmtPercent(item.intensity) }}</div>
          <div>{{ fmt(item.predicted_trips, 3) }}</div>
          <div>{{ item.sample_count ?? 0 }}</div>
        </div>
      </div>
      <div v-else class="muted">请先选择轨迹并点击“查询拥堵”。</div>
    </div>
  </div>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import * as echarts from 'echarts'

import { api } from '@/lib/api'
import AmapFutureHeatmap from '@/components/AmapFutureHeatmap.vue'
import SearchSelect from '@/components/SearchSelect.vue'

const route = useRoute()
const router = useRouter()

const loading = ref(false)
const error = ref('')

const tripId = ref('')
const radiusMeters = ref(120)
const forecastAfterMinutes = ref(60)

const trip = ref(null)
const forecastSummary = ref(null)
const forecastPoints = ref([])
const speedCurveSummary = ref(null)
const speedCurvePoints = ref([])
const speedCurveWindows = ref([])

const speedCurveEl = ref(null)
let speedCurveChart = null

const timeOptions = [
  { value: 30, label: '行程后 30 分钟' },
  { value: 60, label: '行程后 1 小时' },
  { value: 90, label: '行程后 1.5 小时' },
  { value: 120, label: '行程后 2 小时' },
  { value: 150, label: '行程后 2.5 小时' },
  { value: 180, label: '行程后 3 小时' },
]

const selectedTimeLabel = computed(() => {
  return forecastSummary.value?.time_label || (timeOptions.find((item) => item.value === forecastAfterMinutes.value)?.label || '-')
})

const congestionStartLabel = computed(() => {
  const value = speedCurveSummary.value?.congestion_start_time
  if (!value) return '未出现拥堵'
  return formatDateTime(value)
})

const congestionStartOffsetLabel = computed(() => {
  const value = speedCurveSummary.value?.congestion_start_offset_minutes
  if (value === null || value === undefined) return '未来3小时低风险'
  return `行程后 ${value} 分钟`
})

const congestionDurationLabel = computed(() => {
  const value = Number(speedCurveSummary.value?.congestion_duration_minutes || 0)
  return `${value} 分钟`
})

const avgPredictedSpeedLabel = computed(() => {
  const value = speedCurveSummary.value?.avg_predicted_speed_kph
  if (value === null || value === undefined) return '-'
  return `${fmt(value, 1)} km/h`
})

const minPredictedSpeedLabel = computed(() => {
  const value = speedCurveSummary.value?.min_predicted_speed_kph
  if (value === null || value === undefined) return '-'
  return `${fmt(value, 1)} km/h`
})

const overallRiskLabel = computed(() => {
  const risk = speedCurveSummary.value?.overall_risk_level
  return ({ low: '低风险', medium: '中风险', high: '高风险' })[risk] || '-'
})

const tripDateLabel = computed(() => {
  const raw = trip.value?.start_time || trip.value?.end_time || trip.value?.log_date
  if (!raw) return '-'
  const dt = new Date(raw)
  if (Number.isNaN(dt.getTime())) return String(raw)
  return dt.toLocaleDateString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  })
})

const tripTimeRangeLabel = computed(() => {
  const start = trip.value?.start_time
  const end = trip.value?.end_time
  if (!start || !end) return '未提供起止时间'

  const startDt = new Date(start)
  const endDt = new Date(end)
  if (Number.isNaN(startDt.getTime()) || Number.isNaN(endDt.getTime())) return '时间格式异常'

  const hhmm = (dt) => dt.toLocaleTimeString('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
  return `${hhmm(startDt)}-${hhmm(endDt)}`
})

const tripPath = computed(() => {
  const points = trip.value?.points || []
  return points.map((item) => [Number(item.lon), Number(item.lat)])
})

const displayPoints = computed(() => {
  const list = (forecastPoints.value || []).map((item) => ({
    lon: Number(item.lon),
    lat: Number(item.lat),
    intensity: Number(item.intensity || 0),
    predicted_trips: Number(item.predicted_trips || 0),
    sample_count: Number(item.sample_count || 0),
  }))
  list.sort((a, b) => b.intensity - a.intensity)
  return list
})

const currentCongestionAvg = computed(() => {
  if (!displayPoints.value.length) return 0
  const total = displayPoints.value.reduce((acc, item) => acc + Number(item.intensity || 0), 0)
  return total / displayPoints.value.length
})

const futureCongestionAvg = computed(() => {
  if (!displayPoints.value.length) return 0
  return Number(displayPoints.value[0]?.intensity || 0)
})

function fmt(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '-'
  return Number(value).toFixed(digits)
}

function fmtPercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '-'
  return `${(Number(value) * 100).toFixed(1)}%`
}

function formatDateTime(value) {
  if (!value) return '-'
  const dt = new Date(value)
  if (Number.isNaN(dt.getTime())) return String(value)
  return dt.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
}

function renderSpeedCurveChart() {
  speedCurveChart?.dispose()
  speedCurveChart = speedCurveEl.value ? echarts.init(speedCurveEl.value) : null
  if (!speedCurveChart) return

  const rows = speedCurvePoints.value || []
  const threshold = Number(speedCurveSummary.value?.congestion_speed_kph || 20)
  const labels = rows.map((item) => `+${item.offset_minutes}m`)
  const speedData = rows.map((item) => Number(item.predicted_speed_kph || 0))
  const intensityData = rows.map((item) => Number(item.predicted_intensity || 0))

  speedCurveChart.setOption({
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      formatter: (params) => {
        const idx = params?.[0]?.dataIndex ?? 0
        const row = rows[idx]
        return [
          `时距: +${row?.offset_minutes ?? 0} 分钟`,
          `目标时刻: ${formatDateTime(row?.target_time)}`,
          `预测速度: ${fmt(row?.predicted_speed_kph, 2)} km/h`,
          `拥堵强度: ${fmtPercent(row?.predicted_intensity ?? 0)}`,
          `风险等级: ${({ low: '低', medium: '中', high: '高' })[row?.risk_level] || '-'}`,
        ].join('<br/>')
      },
    },
    legend: {
      top: 6,
      textStyle: { color: 'rgba(16,32,51,0.72)' },
    },
    grid: { left: 48, right: 48, top: 44, bottom: 34 },
    xAxis: {
      type: 'category',
      data: labels,
      axisLabel: { color: 'rgba(16,32,51,0.72)' },
    },
    yAxis: [
      {
        type: 'value',
        name: '速度 km/h',
        axisLabel: { color: 'rgba(16,32,51,0.72)' },
        splitLine: { lineStyle: { color: 'rgba(148,163,184,0.14)' } },
      },
      {
        type: 'value',
        name: '强度',
        min: 0,
        max: 1,
        axisLabel: {
          color: 'rgba(16,32,51,0.56)',
          formatter: (v) => `${Math.round(Number(v) * 100)}%`,
        },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: '预测速度',
        type: 'line',
        smooth: true,
        symbolSize: 8,
        data: speedData,
        lineStyle: { width: 3, color: 'rgba(37,99,235,0.88)' },
        itemStyle: { color: 'rgba(37,99,235,0.88)' },
        areaStyle: { color: 'rgba(37,99,235,0.12)' },
      },
      {
        name: '拥堵阈值',
        type: 'line',
        symbol: 'none',
        data: labels.map(() => threshold),
        lineStyle: { type: 'dashed', width: 2, color: 'rgba(239,68,68,0.78)' },
      },
      {
        name: '拥堵强度',
        type: 'line',
        yAxisIndex: 1,
        smooth: true,
        symbolSize: 6,
        data: intensityData,
        lineStyle: { width: 2, color: 'rgba(245,158,11,0.82)' },
        itemStyle: { color: 'rgba(245,158,11,0.82)' },
      },
    ],
  })
}

function handleResize() {
  speedCurveChart?.resize()
}

async function loadTripCongestion() {
  const id = Number(tripId.value)
  if (!Number.isFinite(id)) {
    error.value = 'trip_id 必须是数字'
    return
  }

  loading.value = true
  error.value = ''
  try {
    const [tripResp, forecastResp, speedCurveResp] = await Promise.all([
      api.get(`/api/trips/${id}`),
      api.get('/api/forecast/heatmap/by-trip', {
        params: {
          trip_id: id,
          forecast_after_minutes: forecastAfterMinutes.value,
          top_k: 20000,
        },
      }),
      api.get('/api/forecast/speed/by-trip', {
        params: {
          trip_id: id,
          horizon_minutes: 180,
          step_minutes: 30,
          top_k: 20000,
        },
      }),
    ])
    trip.value = tripResp.data || null
    forecastSummary.value = forecastResp.data?.summary || null
    forecastPoints.value = forecastResp.data?.points || []
    speedCurveSummary.value = speedCurveResp.data?.summary || null
    speedCurvePoints.value = speedCurveResp.data?.points || []
    speedCurveWindows.value = speedCurveResp.data?.windows || []
  } catch (e) {
    trip.value = null
    forecastSummary.value = null
    forecastPoints.value = []
    speedCurveSummary.value = null
    speedCurvePoints.value = []
    speedCurveWindows.value = []
    error.value = e?.response?.data?.detail || e?.message || String(e)
  } finally {
    loading.value = false
  }
}

function submitTrip() {
  const id = Number(tripId.value)
  if (!Number.isFinite(id)) {
    error.value = 'trip_id 必须是数字'
    return
  }
  const nextId = String(id)
  if (String(route.query?.id || '') === nextId) {
    loadTripCongestion()
    return
  }
  router.replace({ path: '/forecast-heatmap', query: { id: nextId } })
}

async function fetchTripSuggestions(keyword = '') {
  const trimmed = String(keyword || '').trim()
  if (!trimmed) return []
  const resp = await api.get('/api/meta/trip-ids', {
    params: { q: trimmed, limit: 200 },
  })
  return resp.data || []
}

onMounted(() => {
  window.addEventListener('resize', handleResize)
  const qid = route.query?.id
  if (qid) {
    tripId.value = String(qid)
    loadTripCongestion()
  }
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  speedCurveChart?.dispose()
  speedCurveChart = null
})

watch(
  () => route.query?.id,
  (qid) => {
    if (!qid) return
    tripId.value = String(qid)
    loadTripCongestion()
  }
)

watch(
  () => forecastAfterMinutes.value,
  () => {
    if (!tripId.value) return
    loadTripCongestion()
  }
)

watch(
  () => [speedCurvePoints.value, speedCurveSummary.value],
  async () => {
    await nextTick()
    renderSpeedCurveChart()
  },
  { deep: true }
)
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
  flex-wrap: wrap;
  width: 100%;
}

.label {
  display: flex;
  gap: 8px;
  align-items: center;
  color: var(--text-muted);
  font-size: 12px;
  white-space: nowrap;
}

.input {
  height: 36px;
  padding: 0 10px;
  border-radius: 10px;
  border: 1px solid var(--border);
  background: var(--surface-0);
  color: var(--text);
  outline: none;
}

.input.small {
  width: 90px;
}

.btn {
  height: 36px;
  padding: 0 14px;
  border-radius: 10px;
  border: 1px solid rgba(79, 124, 255, 0.22);
  background: rgba(79, 124, 255, 0.1);
  color: var(--text);
  cursor: pointer;
  box-shadow: var(--shadow-sm);
}

.btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.summary-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}

.stat-card {
  border-radius: var(--radius-lg);
  padding: 14px;
  border: 1px solid var(--border);
  background: linear-gradient(180deg, var(--surface-2), var(--surface-0));
}

.stat-label {
  color: var(--text-muted);
  font-size: 12px;
}

.stat-value {
  margin-top: 8px;
  font-size: 22px;
  font-weight: 700;
}

.stat-note {
  margin-top: 4px;
  color: var(--text-muted);
  font-size: 12px;
}

.card {
  border-radius: var(--radius-lg);
  padding: 16px;
  border: 1px solid var(--border);
  background: linear-gradient(180deg, var(--surface-2), var(--surface-0));
  box-shadow: var(--shadow-sm);
}

.card-title {
  font-weight: 700;
  margin-bottom: 10px;
}

.curve-summary-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.mini-card {
  border-radius: 10px;
  border: 1px solid rgba(148, 163, 184, 0.24);
  padding: 10px 12px;
  background: rgba(255, 255, 255, 0.68);
}

.mini-label {
  color: var(--text-muted);
  font-size: 12px;
}

.mini-value {
  margin-top: 6px;
  font-size: 18px;
  font-weight: 700;
}

.mini-note {
  margin-top: 4px;
  color: var(--text-muted);
  font-size: 12px;
}

.curve-chart {
  margin-top: 12px;
  width: 100%;
  height: 320px;
}

.table {
  display: grid;
  gap: 4px;
}

.tr {
  display: grid;
  grid-template-columns: 0.8fr 1fr 1fr 1fr 1fr 1fr;
  gap: 10px;
  padding: 10px 12px;
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.65);
  border: 1px solid rgba(148, 163, 184, 0.2);
  font-size: 13px;
}

.tr.head {
  background: rgba(79, 124, 255, 0.08);
  font-weight: 600;
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

@media (max-width: 1100px) {
  .summary-grid {
    grid-template-columns: 1fr;
  }

  .curve-summary-grid {
    grid-template-columns: 1fr;
  }

  .tr,
  .tr.head {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}
</style>
