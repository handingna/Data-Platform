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
  selectedZoneId: { type: [String, Number], default: null },
  center: { type: Array, default: () => [126.65893, 45.752956] },
  zoom: { type: Number, default: 11 },
})

const emit = defineEmits(['select-zone'])

const TYPE_COLOR = {
  pickup: '#2563eb',
  dropoff: '#dc2626',
  mixed: '#6957ff',
}

const el = ref(null)
let map = null
let overlays = []

const legendItems = computed(() => [
  { type: 'pickup', label: '上车热区', color: TYPE_COLOR.pickup },
  { type: 'dropoff', label: '下车热区', color: TYPE_COLOR.dropoff },
  { type: 'mixed', label: '混合热区', color: TYPE_COLOR.mixed },
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

function focusZone(zone) {
  if (!map || !zone) return
  map.setZoomAndCenter(props.zoom + 2, toMapPoint(zone.center))
}

function draw() {
  if (!map) return
  clearOverlays()

  const targets = []
  for (const item of props.items || []) {
    const polygonPath = (item.bounds || []).map(toMapPoint)
    if (!polygonPath.length) continue

    const color = TYPE_COLOR[item.demand_type] || TYPE_COLOR.mixed
    const isSelected = String(item.zone_id) === String(props.selectedZoneId)
    const polygon = new window.AMap.Polygon({
      path: polygonPath,
      fillColor: color,
      fillOpacity: isSelected ? 0.42 : 0.28,
      strokeColor: color,
      strokeOpacity: 0.86,
      strokeWeight: isSelected ? 3 : 2,
      zIndex: isSelected ? 110 : 100,
    })
    polygon.on('click', () => emit('select-zone', item.zone_id))
    overlays.push(polygon)
    map.add(polygon)
    targets.push(polygon)

    const marker = new window.AMap.Marker({
      position: toMapPoint(item.center),
      offset: new window.AMap.Pixel(-24, -24),
      content: `<div class="hotspot-pill ${isSelected ? 'active' : ''}" style="background:${color}">${item.trip_count}</div>`,
    })
    marker.on('click', () => emit('select-zone', item.zone_id))
    overlays.push(marker)
    map.add(marker)
    targets.push(marker)
  }

  if (!props.items.length) {
    if (map) {
      map.setCenter(props.center)
      map.setZoom(props.zoom)
    }
    return
  }

  const selected = props.items.find((item) => String(item.zone_id) === String(props.selectedZoneId))
  if (selected) {
    focusZone(selected)
  } else if (targets.length) {
    map.setFitView(targets)
  }
}

onMounted(() => {
  if (!window.AMap) {
    throw new Error('AMap is not loaded. Check index.html script tag.')
  }
  map = new window.AMap.Map(el.value, {
    zoom: props.zoom,
    center: props.center,
  })
  draw()
})

watch(
  () => props.items,
  () => draw(),
  { deep: true }
)

watch(
  () => props.selectedZoneId,
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
  height: 560px;
  border-radius: 16px;
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
  top: 16px;
  left: 16px;
  display: grid;
  gap: 8px;
  padding: 12px 14px;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.95);
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

.hotspot-pill {
  display: inline-grid;
  place-items: center;
  min-width: 48px;
  min-height: 28px;
  padding: 0 10px;
  border-radius: 999px;
  color: #fff;
  font-weight: 700;
  font-size: 12px;
  text-shadow: 0 0 4px rgba(0, 0, 0, 0.25);
  border: 2px solid rgba(255, 255, 255, 0.92);
}

.hotspot-pill.active {
  box-shadow: 0 0 0 10px rgba(255, 255, 255, 0.16);
}
</style>
