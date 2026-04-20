<template>
  <div class="page">
    <div class="header">
      <div>
        <div class="title">城市出租车需求洞察</div>
        <div class="subtitle">通过网格聚合显示上车 / 下车热点区域，避免单点标注，呈现更具可视范围的需求热区。</div>
      </div>

      <div class="controls">
        <label class="select-label">
          数据类型
          <select v-model="demandType" class="input select">
            <option value="both">上车+下车</option>
            <option value="pickup">上车热点</option>
            <option value="dropoff">下车热点</option>
          </select>
        </label>

        <label class="select-label">
          时间范围
          <select v-model.number="hourFrom" class="input select">
            <option v-for="option in hourOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
          </select>
        </label>

        <span class="divider">至</span>

        <label class="select-label">
          <select v-model.number="hourTo" class="input select">
            <option v-for="option in hourOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
          </select>
        </label>

        <label class="select-label">
          样本条数
          <select v-model.number="sampleCount" class="input select">
            <option :value="1000">1000</option>
            <option :value="3000">3000</option>
            <option :value="5000">5000</option>
            <option :value="10000">10000</option>
          </select>
        </label>

        <label class="select-label">
          热点数量
          <select v-model.number="limit" class="input select">
            <option :value="10">10</option>
            <option :value="15">15</option>
            <option :value="20">20</option>
            <option :value="30">30</option>
          </select>
        </label>
      </div>

      <button class="btn btn-refresh" :disabled="loading" @click="loadHotspots">刷新热点</button>
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div class="summary-grid">
      <div class="stat-card">
        <div class="stat-label">样本行程</div>
        <div class="stat-value">{{ hotspotSampleCount }}</div>
        <div class="stat-note">基于最近数据样本聚合，上车 / 下车热点均以网格范围显示。</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">热点范围</div>
        <div class="stat-value">{{ hotspotCount }}</div>
        <div class="stat-note">每个热点格网保留区域范围，避免精确点定位。</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">最大热度</div>
        <div class="stat-value">{{ maxTripCount }}</div>
        <div class="stat-note">网格内总上车/下车次数。</div>
      </div>
    </div>

    <div class="content-grid">
      <div class="card map-card">
        <AmapDemandMap
          :items="hotspots"
          :selected-zone-id="selectedZoneId"
          @select-zone="selectZone"
        />
      </div>

      <div class="card list-card">
        <div class="card-title">热点列表</div>
        <div v-if="hotspots.length" class="hotspot-list">
          <button
            v-for="(item, index) in visibleHotspots"
            :key="item.zone_id"
            type="button"
            class="hotspot-item"
            :class="{ active: item.zone_id === selectedZoneId }"
            @click="selectZone(item.zone_id)"
          >
            <div class="hotspot-row">
              <span class="hotspot-rank">#{{ index + 1 }}</span>
              <span class="hotspot-type">{{ demandLabel(item.demand_type) }}</span>
            </div>
            <div class="hotspot-metric">热度 {{ item.trip_count }} 次</div>
            <div class="hotspot-meta">
              <span>上车 {{ item.pickup_count }}</span>
              <span>下车 {{ item.dropoff_count }}</span>
              <span>平均时段 {{ formatHour(item.avg_hour) }}</span>
            </div>
          </button>
          <button
            v-if="hotspots.length > 5"
            type="button"
            class="btn-expand"
            @click="showAllHotspots = !showAllHotspots"
          >
            {{ showAllHotspots ? '收起' : `展开更多 (${hotspots.length - 5})` }}
          </button>
        </div>
        <div v-else class="empty-state">暂无热点数据，请尝试扩大样本数或选择其它时间范围。</div>
      </div>
    </div>

    <div class="secondary-grid">
      <div class="card chart-card">
        <div class="card-title">分时段热力分析</div>
        <div class="card-subtitle">显示当前时间范围内各 2 小时段的上车、下车和总量趋势。</div>
        <div ref="chartTimeEl" class="chart chart--medium"></div>
      </div>

      <div class="card migration-card">
        <div class="card-title">热点迁移分析</div>
        <div class="card-subtitle">比较所选时段前后两半段的网格热度变化，以后半段热度倒序排名。</div>
        <div v-if="visibleMigrations.length" class="migration-list">
          <div
            v-for="item in visibleMigrations"
            :key="item.zone_id"
            class="migration-item"
          >
            <div class="migration-row">
              <div>
                <div class="migration-zone">区域 {{ item.zone_id }}</div>
                <div class="migration-label">{{ item.demand_type === 'mixed' ? '混合热点' : item.demand_type === 'pickup' ? '上车热点' : '下车热点' }}</div>
              </div>
              <div class="migration-badges">
                <div class="migration-badge" :class="`migration-badge--${item.trend}`">
                  {{ item.trend === 'up' ? '上升' : item.trend === 'down' ? '下降' : item.trend === 'new' ? '新增' : item.trend === 'fade' ? '衰退' : '稳定' }}
                </div>
                <div v-if="item.rank_change !== null" class="migration-rank-change" :class="item.rank_change > 0 ? 'rank-up' : item.rank_change < 0 ? 'rank-down' : 'rank-stable'">
                  {{ item.rank_change > 0 ? `↑${item.rank_change}` : item.rank_change < 0 ? `↓${Math.abs(item.rank_change)}` : '-' }}
                </div>
              </div>
            </div>
            <div class="migration-meta">
              <span>前半段排名 {{ item.early_rank ?? '-' }}</span>
              <span>后半段排名 {{ item.late_rank ?? '-' }}</span>
              <span>前半段 {{ item.early_count }} 次</span>
              <span>后半段 {{ item.late_count }} 次</span>
            </div>
          </div>
          <button
            v-if="migrationItems.length > 5"
            type="button"
            class="btn-expand"
            @click="showAllMigrations = !showAllMigrations"
          >
            {{ showAllMigrations ? '收起' : `展开更多 (${migrationItems.length - 5})` }}
          </button>
        </div>
        <div v-else class="empty-state">暂无迁移变化结果，尝试扩大时间范围或增加样本量。</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts'
import { api } from '@/lib/api'
import AmapDemandMap from '@/components/AmapDemandMap.vue'

const demandType = ref('both')
const hourFrom = ref(0)
const hourTo = ref(23)
const sampleCount = ref(5000)
const limit = ref(20)
const loading = ref(false)
const error = ref('')
const hotspots = ref([])
const selectedZoneId = ref(null)
const hotspotSampleCount = ref(0)
const timeBuckets = ref([])
const migrationAnalysis = ref(null)
const chartTimeEl = ref(null)
const showAllHotspots = ref(false)
const showAllMigrations = ref(false)
let chartTime = null

const hourOptions = Array.from({ length: 24 }, (_, index) => ({
  value: index,
  label: `${String(index).padStart(2, '0')}:00`,
}))

const hotspotCount = computed(() => hotspots.value.length)
const maxTripCount = computed(() => {
  if (!hotspots.value.length) return 0
  return Math.max(...hotspots.value.map((item) => item.trip_count))
})
const migrationItems = computed(() => migrationAnalysis.value?.items || [])
const visibleHotspots = computed(() => showAllHotspots.value ? hotspots.value : hotspots.value.slice(0, 5))
const visibleMigrations = computed(() => showAllMigrations.value ? migrationItems.value : migrationItems.value.slice(0, 5))

function formatHour(hour) {
  if (hour === null || hour === undefined) return '-'
  const h = Math.floor(hour) % 24
  return `${String(h).padStart(2, '0')}:00`
}

function demandLabel(type) {
  if (type === 'pickup') return '上车热点'
  if (type === 'dropoff') return '下车热点'
  return '混合热点'
}

function selectZone(zoneId) {
  selectedZoneId.value = zoneId
}

function _buildTimeHeatmapOption() {
  const labels = timeBuckets.value.map((bucket) => bucket.label)
  const pickupData = timeBuckets.value.map((bucket) => bucket.pickup_count)
  const dropoffData = timeBuckets.value.map((bucket) => bucket.dropoff_count)
  const totalData = timeBuckets.value.map((bucket) => bucket.total_count)

  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter(params) {
        if (!Array.isArray(params)) return params
        return params.map((item) => `${item.seriesName}: ${item.value}`).join('<br/>')
      },
    },
    legend: {
      data: ['上车', '下车', '总量'],
      bottom: 0,
    },
    grid: { left: 20, right: 16, top: 24, bottom: 40 },
    xAxis: {
      type: 'category',
      data: labels,
      axisLabel: { rotate: 0, color: '#475569' },
      axisLine: { lineStyle: { color: '#CBD5E1' } },
    },
    yAxis: {
      type: 'value',
      axisLine: { lineStyle: { color: '#CBD5E1' } },
      splitLine: { lineStyle: { type: 'dashed', color: '#E2E8F0' } },
    },
    series: [
      {
        name: '上车',
        type: 'bar',
        stack: 'demand',
        itemStyle: { color: '#2563eb' },
        data: pickupData,
      },
      {
        name: '下车',
        type: 'bar',
        stack: 'demand',
        itemStyle: { color: '#dc2626' },
        data: dropoffData,
      },
      {
        name: '总量',
        type: 'line',
        itemStyle: { color: '#22c55e' },
        smooth: true,
        data: totalData,
      },
    ],
  }
}

function drawTimeHeatmap() {
  if (!chartTime || !timeBuckets.value.length) {
    chartTime?.clear?.()
    return
  }
  const option = _buildTimeHeatmapOption()
  chartTime.setOption(option)
}

async function loadHotspots() {
  loading.value = true
  error.value = ''
  selectedZoneId.value = null
  try {
    const resp = await api.get('/api/demand/hotspots', {
      params: {
        demand_type: demandType.value,
        hour_from: hourFrom.value,
        hour_to: hourTo.value,
        sample_trip_count: sampleCount.value,
        limit: limit.value,
      },
    })
    hotspots.value = resp.data.items || []
    hotspotSampleCount.value = resp.data.sample_trip_count || 0
    timeBuckets.value = resp.data.time_buckets || []
    migrationAnalysis.value = resp.data.migration_analysis || null
    drawTimeHeatmap()
  } catch (err) {
    hotspots.value = []
    hotspotSampleCount.value = 0
    timeBuckets.value = []
    migrationAnalysis.value = null
    error.value = err?.response?.data?.detail || err?.message || String(err)
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  if (chartTimeEl.value) {
    chartTime = echarts.init(chartTimeEl.value)
  }
  loadHotspots()
})

watch(
  () => timeBuckets.value,
  () => drawTimeHeatmap(),
  { deep: true }
)

onBeforeUnmount(() => {
  if (chartTime) {
    chartTime.dispose()
    chartTime = null
  }
})
</script>

<style scoped>
.page {
  display: grid;
  gap: 18px;
}

.header {
  display: flex;
  flex-wrap: wrap;
  gap: 18px;
  justify-content: space-between;
  align-items: flex-start;
}

.title {
  font-size: 18px;
  font-weight: 700;
}

.subtitle {
  margin-top: 4px;
  color: var(--text-muted);
  font-size: 13px;
}

.controls {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  align-items: flex-end;
}

.select-label {
  display: grid;
  gap: 8px;
  font-size: 12px;
  color: var(--text-muted);
}

.select {
  width: 100%;
  min-width: 160px;
}

.input,
.select {
  height: 38px;
  border-radius: 10px;
  border: 1px solid var(--border);
  background: var(--surface-0);
  color: var(--text);
  padding: 0 10px;
  outline: none;
}

.divider {
  display: inline-block;
  margin: 0 6px;
  color: var(--text-muted);
}

.btn {
  height: 38px;
  padding: 0 16px;
  border-radius: 10px;
  border: 1px solid rgba(79, 124, 255, 0.22);
  background: rgba(79, 124, 255, 0.12);
  color: var(--text);
  cursor: pointer;
  white-space: nowrap;
}

.btn-refresh {
  margin-top: 12px;
  height: 44px;
  padding: 0 20px;
  font-size: 16px;
  font-weight: 600;
}

.summary-grid {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.stat-card {
  padding: 18px;
  border-radius: 18px;
  background: var(--surface-0);
  border: 1px solid var(--border);
}

.stat-label {
  font-size: 12px;
  color: var(--text-muted);
}

.stat-value {
  margin: 10px 0 6px;
  font-size: 28px;
  font-weight: 700;
}

.stat-note {
  color: var(--text-muted);
  font-size: 12px;
}

.content-grid {
  display: grid;
  grid-template-columns: 1.7fr 0.9fr;
  gap: 18px;
}

.secondary-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 18px;
}

.card {
  padding: 18px;
  border-radius: 18px;
  background: var(--surface-0);
  border: 1px solid var(--border);
}

.map-card {
  min-height: 600px;
}

.chart-card {
  min-height: 360px;
}

.chart {
  width: 100%;
  min-height: 320px;
}

.migration-card {
  display: grid;
  gap: 12px;
}

.migration-list {
  display: grid;
  gap: 12px;
}

.migration-item {
  padding: 14px 16px;
  border-radius: 16px;
  border: 1px solid var(--border);
  background: var(--surface-1);
}

.migration-row {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  margin-bottom: 10px;
}

.migration-badges {
  display: flex;
  gap: 8px;
  align-items: center;
}

.migration-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 60px;
  height: 28px;
  border-radius: 999px;
  color: #fff;
  font-size: 12px;
  font-weight: 700;
}

.migration-badge--up {
  background: #16a34a;
}

.migration-badge--down {
  background: #dc2626;
}

.migration-badge--new {
  background: #2563eb;
}

.migration-badge--fade {
  background: #6b7280;
}

.migration-badge--stable {
  background: #475569;
}

.migration-rank-change {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 50px;
  height: 28px;
  border-radius: 999px;
  color: #fff;
  font-size: 12px;
  font-weight: 700;
}

.rank-up {
  background: #16a34a;
}

.rank-down {
  background: #dc2626;
}

.rank-stable {
  background: #6b7280;
}

.migration-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  color: var(--text-muted);
  font-size: 12px;
}

.btn-expand {
  width: 100%;
  margin-top: 12px;
  padding: 10px 16px;
  border-radius: 10px;
  border: 1px solid var(--border);
  background: var(--surface-1);
  color: var(--text);
  cursor: pointer;
  font-size: 14px;
  transition: background 0.2s;
}

.btn-expand:hover {
  background: var(--surface-0);
}

.list-card {
  display: grid;
  gap: 14px;
}

.card-title {
  font-size: 15px;
  font-weight: 700;
  margin-bottom: 12px;
}

.hotspot-list {
  display: grid;
  gap: 10px;
}

.hotspot-item {
  text-align: left;
  width: 100%;
  padding: 14px 16px;
  border-radius: 16px;
  border: 1px solid var(--border);
  background: var(--surface-1);
  color: var(--text);
  cursor: pointer;
  transition: transform 0.15s ease, box-shadow 0.15s ease;
}

.hotspot-item:hover,
.hotspot-item.active {
  transform: translateY(-1px);
  box-shadow: 0 18px 36px rgba(15, 23, 42, 0.08);
}

.hotspot-row {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 10px;
  font-size: 14px;
  font-weight: 700;
}

.hotspot-rank {
  color: var(--text-muted);
}

.hotspot-type {
  color: var(--text);
}

.hotspot-metric {
  font-size: 16px;
  margin-bottom: 8px;
}

.hotspot-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  color: var(--text-muted);
  font-size: 12px;
}

.empty-state {
  color: var(--text-muted);
  padding: 26px;
  text-align: center;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.7);
}

@media (max-width: 1080px) {
  .content-grid {
    grid-template-columns: 1fr;
  }
}
</style>
