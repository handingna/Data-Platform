<template>
  <div class="page">
    <div class="header">
      <div>
        <div class="title">轨迹分析</div>
        <div class="subtitle">输入轨迹 ID，还原真实行驶路径并标注拥堵（红）/畅通（绿）。</div>
      </div>
      <div class="controls">
        <SearchSelect
          v-model="tripId"
          :fetch-options="fetchTripSuggestions"
          placeholder="选择或输入 trip_id，例如 286254"
        />
        <button class="btn" :disabled="!tripId || loading" @click="loadTrip">查询</button>
        <label class="label">
          拥堵阈值(km/h)
          <input v-model.number="congestionKph" type="number" class="input small" min="0" max="200" />
        </label>
      </div>
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div class="grid">
      <div class="card">
        <AmapTripMap :segments="segments" :trip="trip" />
      </div>
      <div class="card info">
        <div class="card-title">行程信息</div>
        <div v-if="trip">
          <div class="kv"><span>行程ID</span><b>{{ trip.trip_id }}</b></div>
          <div class="kv"><span>行程日期</span><b>{{ trip.log_date }}</b></div>
          <div class="kv"><span>车辆ID</span><b>{{ trip.devid ?? '-' }}</b></div>
          <div class="kv"><span>距离(km)</span><b>{{ fmt(trip.distance_km) }}</b></div>
          <div class="kv"><span>时长</span><b>{{ fmtDuration(trip.duration_seconds) }}</b></div>
          <div class="kv"><span>均速(km/h)</span><b>{{ fmt(trip.avg_speed_kph) }}</b></div>
          <div class="kv"><span>起点时间</span><b>{{ trip.start_time ?? '-' }}</b></div>
          <div class="kv"><span>终点时间</span><b>{{ trip.end_time ?? '-' }}</b></div>
          <div class="kv"><span>点数量</span><b>{{ trip.points?.length ?? 0 }}</b></div>
        </div>
        <div v-else class="muted">暂无数据。请先查询一个 trip_id。</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '@/lib/api'
import SearchSelect from '@/components/SearchSelect.vue'
import AmapTripMap from '@/components/AmapTripMap.vue'

const route = useRoute()

const tripId = ref('286254')
const congestionKph = ref(20)
const loading = ref(false)
const error = ref('')

const trip = ref(null)
const segments = ref([])

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

async function loadTrip() {
  error.value = ''
  loading.value = true
  try {
    const id = Number(tripId.value)
    if (!Number.isFinite(id)) throw new Error('trip_id 必须是数字')

    const [tripResp, segResp] = await Promise.all([
      api.get(`/api/trips/${id}`),
      api.get(`/api/trips/${id}/segments`, { params: { congestion_kph: congestionKph.value } }),
    ])
    trip.value = tripResp.data
    segments.value = segResp.data.segments || []
  } catch (e) {
    trip.value = null
    segments.value = []
    error.value = e?.response?.data?.detail || e?.message || String(e)
  } finally {
    loading.value = false
  }
}

async function fetchTripSuggestions(keyword = '') {
  const resp = await api.get('/api/meta/trip-ids', {
    params: { q: keyword || '' },
  })
  return resp.data || []
}

onMounted(() => {
  const qid = route.query?.id
  if (qid) tripId.value = String(qid)
  if (tripId.value) loadTrip()
})

watch(
  () => route.query?.id,
  (qid) => {
    if (!qid) return
    tripId.value = String(qid)
    loadTrip()
  }
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
  flex-wrap: nowrap;
  width: 100%;
  min-width: 0;
}
.input {
  height: 36px;
  padding: 0 10px;
  border-radius: 10px;
  border: 1px solid var(--border);
  background: var(--surface-3);
  color: var(--text);
  outline: none;
}
.input.small {
  width: 92px;
}
.select {
  height: 10px;
  width: 140px;
  padding: 0 10px;
  border-radius: 10px;
  border: 1px solid var(--border);
  background: var(--surface-3);
  color: var(--text);
  outline: none;
}
.label {
  display: flex;
  gap: 8px;
  align-items: center;
  color: var(--text-muted);
  font-size: 12px;
  white-space: nowrap;
  flex: 0 0 auto;
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
  grid-template-columns: 1.7fr 1fr;
  gap: 12px;
}
.card {
  border-radius: var(--radius-lg);
  padding: 16px;
  border: 1px solid var(--border);
  background: linear-gradient(180deg, var(--surface-2), var(--surface-3));
  box-shadow: var(--shadow-sm);
}
.card-title {
  font-weight: 700;
  margin-bottom: 10px;
  color: var(--text);
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
  border: 1px solid rgba(228, 106, 128, 0.22);
  background: rgba(228, 106, 128, 0.08);
  color: #ffd6df;
}
@media (max-width: 1100px) {
  .grid {
    grid-template-columns: 1fr;
  }

  .controls {
    flex-wrap: wrap;
    align-items: center;
  }

  .label {
    white-space: normal;
  }
}
</style>

