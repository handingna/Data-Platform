<template>
  <div class="page">
    <div class="header">
      <div>
        <div class="title">车辆画像</div>
        <div class="subtitle">输入车辆 ID，查看车辆概要、两小时分布和关联行程列表。</div>
      </div>

      <div class="controls">
        <SearchSelect
          v-model="deviceId"
          :fetch-options="fetchDeviceSuggestions"
          :min-chars="1"
          :max-items="200"
          placeholder="选择或输入 device_id，例如 100032066"
        />
        <button class="btn" :disabled="!deviceId || loading" @click="submitCar">查询</button>
      </div>
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div class="grid">
      <div class="card">
        <div class="card-title">统计图（每 2 小时）</div>
        <div class="charts">
          <div ref="chartTripsEl" class="chart"></div>
          <div ref="chartDistEl" class="chart"></div>
        </div>
      </div>

      <div class="card info">
        <div class="card-title">车辆信息</div>
        <div v-if="car">
          <div class="kv"><span>车辆 ID</span><b>{{ car.device_id }}</b></div>
          <div class="kv"><span>总行程数</span><b>{{ car.trips_total }}</b></div>
          <div class="kv"><span>累计里程 (km)</span><b>{{ fmt(car.total_distance) }}</b></div>
        </div>
        <div v-else class="muted">暂无数据。请先查询一个 device_id。</div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">该车辆行程列表（最近 {{ trips.length }} 条）</div>
      <div class="table">
        <div class="tr head">
          <div>行程 ID</div>
          <div>行程日期</div>
          <div>行程公里数</div>
          <div>行程时长</div>
          <div>开始时间</div>
          <div>结束时间</div>
        </div>
        <div v-for="t in trips" :key="`${t.trip_id}-${t.log_date}`" class="tr">
          <div><code class="click" @click="goTrip(t.trip_id)">{{ t.trip_id }}</code></div>
          <div>{{ t.log_date }}</div>
          <div>{{ fmt(t.distance_km) }}</div>
          <div>{{ fmtDuration(t.duration_seconds) }}</div>
          <div>{{ t.start_time ?? '-' }}</div>
          <div>{{ t.end_time ?? '-' }}</div>
        </div>
        <div v-if="!trips.length" class="muted" style="padding: 10px 0">
          暂无列表数据。该 device_id 可能没有关联到 `car` 表或 `trip_data` 记录。
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import * as echarts from 'echarts'

import { api } from '@/lib/api'
import SearchSelect from '@/components/SearchSelect.vue'

const router = useRouter()
const route = useRoute()

const deviceId = ref('100032066')
const loading = ref(false)
const error = ref('')

const car = ref(null)
const trips = ref([])

const chartTripsEl = ref(null)
const chartDistEl = ref(null)
let chartTrips = null
let chartDist = null

function fmt(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-'
  return Number(n).toFixed(2)
}

function fmtDuration(sec) {
  if (!sec && sec !== 0) return '-'
  const s = Math.max(0, Math.floor(sec))
  const hh = String(Math.floor(s / 3600)).padStart(2, '0')
  const mm = String(Math.floor((s % 3600) / 60)).padStart(2, '0')
  const ss = String(s % 60).padStart(2, '0')
  return `${hh}:${mm}:${ss}`
}

function goTrip(tripId) {
  router.push({ path: '/trip', query: { id: String(tripId) } })
}

function handleResize() {
  chartTrips?.resize()
  chartDist?.resize()
}

function renderCharts() {
  const profile = car.value
  if (!profile) return

  const labels = Object.keys(profile.trips_total_by_2h || {})
  const tripCounts = labels.map((key) => profile.trips_total_by_2h[key] ?? 0)
  const distances = labels.map((key) => profile.total_distance_by_2h[key] ?? 0)

  chartTrips?.dispose()
  chartDist?.dispose()
  chartTrips = echarts.init(chartTripsEl.value)
  chartDist = echarts.init(chartDistEl.value)

  const baseGrid = {
    left: 45,
    right: 18,
    top: 24,
    bottom: 34,
    containLabel: false,
  }

  chartTrips.setOption({
    backgroundColor: 'transparent',
    title: { text: '行程数量', textStyle: { color: '#102033', fontSize: 12 } },
    grid: baseGrid,
    xAxis: { type: 'category', data: labels, axisLabel: { color: 'rgba(16,32,51,0.7)' } },
    yAxis: {
      type: 'value',
      axisLabel: { color: 'rgba(16,32,51,0.7)' },
      splitLine: { lineStyle: { color: 'rgba(148,163,184,0.14)' } },
    },
    series: [{ type: 'bar', data: tripCounts, itemStyle: { color: 'rgba(79,124,255,0.7)' }, barMaxWidth: 26 }],
    tooltip: { trigger: 'axis' },
  })

  chartDist.setOption({
    backgroundColor: 'transparent',
    title: { text: '里程 (km)', textStyle: { color: '#102033', fontSize: 12 } },
    grid: baseGrid,
    xAxis: { type: 'category', data: labels, axisLabel: { color: 'rgba(16,32,51,0.7)' } },
    yAxis: {
      type: 'value',
      axisLabel: { color: 'rgba(16,32,51,0.7)' },
      splitLine: { lineStyle: { color: 'rgba(148,163,184,0.14)' } },
    },
    series: [{
      type: 'line',
      data: distances,
      smooth: true,
      symbolSize: 6,
      lineStyle: { width: 3, color: 'rgba(47,159,103,0.72)' },
      itemStyle: { color: 'rgba(47,159,103,0.72)' },
    }],
    tooltip: { trigger: 'axis' },
  })

  window.removeEventListener('resize', handleResize)
  window.addEventListener('resize', handleResize)
}

async function loadCar() {
  error.value = ''
  loading.value = true
  try {
    const [carResp, tripsResp] = await Promise.all([
      api.get(`/api/cars/${encodeURIComponent(deviceId.value)}`),
      api.get(`/api/cars/${encodeURIComponent(deviceId.value)}/trips`, { params: { limit: 200 } }),
    ])
    car.value = carResp.data
    trips.value = tripsResp.data || []
    await nextTick()
    renderCharts()
  } catch (e) {
    car.value = null
    trips.value = []
    error.value = e?.response?.data?.detail || e?.message || String(e)
  } finally {
    loading.value = false
  }
}

function submitCar() {
  const nextId = String(deviceId.value || '').trim()
  if (!nextId) {
    error.value = 'device_id 不能为空'
    return
  }
  if (String(route.query?.id || '') === nextId) {
    loadCar()
    return
  }
  router.replace({ path: '/car', query: { id: nextId } })
}

async function fetchDeviceSuggestions(keyword = '') {
  const trimmed = String(keyword || '').trim()
  if (!trimmed) return []
  const resp = await api.get('/api/meta/device-ids', {
    params: { q: trimmed, limit: 200 },
  })
  return resp.data || []
}

onMounted(() => {
  const qid = route.query?.id
  if (qid) {
    deviceId.value = String(qid)
  }
  if (deviceId.value) {
    loadCar()
  }
})

watch(
  () => route.query?.id,
  (qid) => {
    if (!qid) return
    deviceId.value = String(qid)
    loadCar()
  }
)

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  chartTrips?.dispose()
  chartDist?.dispose()
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

.grid {
  display: grid;
  grid-template-columns: 1.6fr 1fr;
  gap: 12px;
}

.card {
  border-radius: var(--radius-lg);
  padding: 16px;
  border: 1px solid var(--border);
  background: var(--surface-0);
  box-shadow: var(--shadow-sm);
}

.card-title {
  font-weight: 700;
  margin-bottom: 10px;
  color: var(--text);
}

.charts {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
}

.chart {
  height: 300px;
  border-radius: 12px;
  border: 1px solid rgba(148, 163, 184, 0.12);
  background: rgba(255, 255, 255, 0.9);
}

.info .kv {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  padding: 8px 0;
  border-bottom: 1px solid rgba(148, 163, 184, 0.12);
}

.info .kv span {
  color: var(--text-muted);
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

.table {
  width: 100%;
  overflow: auto;
}

.tr {
  display: grid;
  grid-template-columns: 110px 110px 110px 110px 1fr 1fr;
  gap: 12px;
  padding: 10px 0;
  border-bottom: 1px solid rgba(148, 163, 184, 0.12);
  min-width: 860px;
}

.tr.head {
  color: var(--text-muted);
  font-size: 12px;
}

.click {
  cursor: pointer;
  color: var(--primary);
}

@media (max-width: 1100px) {
  .grid {
    grid-template-columns: 1fr;
  }

  .charts {
    grid-template-columns: 1fr;
  }

  .controls {
    flex-wrap: wrap;
    align-items: center;
  }
}
</style>
