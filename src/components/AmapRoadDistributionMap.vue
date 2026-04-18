<template>
  <div class="map-wrap">
    <div ref="el" class="map"></div>

    <div class="legend">
      <div v-for="item in legendItems" :key="item.type" class="legend-item">
        <span class="legend-color" :style="{ backgroundColor: item.color }"></span>
        <span>{{ item.label }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import coordtransform from 'coordtransform'

const props = defineProps({
  items: { type: Array, default: () => [] },
  selectedRoadId: { type: [Number, String], default: null },
  center: { type: Array, default: () => [116.397428, 39.90923] },
  zoom: { type: Number, default: 11 },
})

const emit = defineEmits(['select-road'])

const TYPE_COLOR = {
  detour: '#f59e0b',
  stop: '#ef4444',
  speed_jump: '#8b5cf6',
  drift: '#06b6d4',
  jump_point: '#f97316',
}

const TYPE_LABEL = {
  detour: '绕路',
  stop: '停留',
  speed_jump: '速度突变',
  drift: '漂移',
  jump_point: '跳点',
}

const el = ref(null)
let map = null
let overlays = []

const legendItems = computed(() => [
  { type: 'detour', label: TYPE_LABEL.detour, color: TYPE_COLOR.detour },
  { type: 'stop', label: TYPE_LABEL.stop, color: TYPE_COLOR.stop },
  { type: 'speed_jump', label: TYPE_LABEL.speed_jump, color: TYPE_COLOR.speed_jump },
  { type: 'drift', label: TYPE_LABEL.drift, color: TYPE_COLOR.drift },
  { type: 'jump_point', label: TYPE_LABEL.jump_point, color: TYPE_COLOR.jump_point },
])

function toMapPoint(point) {
  if (!point || point.length < 2) return point
  const lon = Number(point[0])
  const lat = Number(point[1])
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return point
  return coordtransform.wgs84togcj02(lon, lat)
}

function clearOverlays() {
  if (!map) return
  overlays.forEach((item) => {
    try { map.remove(item) } catch { /* noop */ }
  })
  overlays = []
}

function focusSelectedRoad() {
  if (!map || !props.selectedRoadId) return
  const selected = props.items.find((item) => item.road_id === props.selectedRoadId)
  if (!selected) return
  map.setZoomAndCenter(14, toMapPoint(selected.center_point))
}

function draw() {
  if (!map) return
  clearOverlays()

  const targets = []
  for (const item of props.items || []) {
    const color = TYPE_COLOR[item.dominant_type] || '#4f7cff'
    const active = item.road_id === props.selectedRoadId
    const path = [toMapPoint(item.start_point), toMapPoint(item.end_point)]

    const line = new window.AMap.Polyline({
      path,
      strokeColor: color,
      strokeWeight: active ? 9 : 6,
      strokeOpacity: active ? 0.98 : 0.82,
      zIndex: active ? 14 : 10,
    })
    line.on('click', () => emit('select-road', item.road_id))
    overlays.push(line)
    targets.push(line)
    map.add(line)

    const marker = new window.AMap.Marker({
      position: toMapPoint(item.center_point),
      offset: new window.AMap.Pixel(-16, -16),
      content: `<div class="road-marker ${active ? 'active' : ''}" style="--marker-color:${color}">${item.occurrence_count}</div>`,
    })
    marker.on('click', () => emit('select-road', item.road_id))
    overlays.push(marker)
    targets.push(marker)
    map.add(marker)
  }

  if (!props.items.length) return
  if (props.selectedRoadId) {
    focusSelectedRoad()
  } else if (targets.length) {
    map.setFitView(targets)
  }
}

onMounted(() => {
  if (!window.AMap) {
    throw new Error('AMap is not loaded. Check index.html script tag.')
  }
  map = new window.AMap.Map(el.value, { zoom: props.zoom, center: props.center })
  draw()
})

watch(
  () => props.items,
  () => draw(),
  { deep: true }
)

watch(
  () => props.selectedRoadId,
  () => draw()
)

onBeforeUnmount(() => {
  clearOverlays()
  try { map?.destroy?.() } catch { /* noop */ }
  map = null
})
</script>

<style scoped>
.map-wrap {
  width: 100%;
  height: 420px;
  border-radius: 14px;
  overflow: hidden;
  border: 1px solid var(--border);
  background: var(--surface-0);
  position: relative;
}

.map {
  width: 100%;
  height: 100%;
}

.legend {
  position: absolute;
  top: 12px;
  left: 12px;
  display: grid;
  gap: 8px;
  padding: 10px 12px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.94);
  border: 1px solid var(--border);
  box-shadow: var(--shadow-sm);
  z-index: 20;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: var(--text);
}

.legend-color {
  width: 12px;
  height: 12px;
  border-radius: 999px;
}

:deep(.road-marker) {
  display: grid;
  place-items: center;
  width: 32px;
  height: 32px;
  border-radius: 999px;
  background: var(--marker-color);
  color: #fff;
  font-size: 12px;
  font-weight: 700;
  border: 3px solid rgba(255, 255, 255, 0.96);
  box-shadow: 0 8px 18px rgba(15, 23, 42, 0.18);
}

:deep(.road-marker.active) {
  transform: scale(1.08);
  box-shadow: 0 0 0 8px rgba(255, 255, 255, 0.32), 0 12px 24px rgba(15, 23, 42, 0.24);
}
</style>
