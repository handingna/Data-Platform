<template>
  <div class="map-wrap">
    <div ref="el" class="map"></div>

    <div class="legend">
      <div v-for="item in legendItems" :key="item.type" class="legend-item">
        <span class="legend-color" :style="{ backgroundColor: item.color }"></span>
        <span>{{ item.label }}</span>
      </div>
    </div>

    <div class="playback-panel" ref="panelEl" :style="panelStyle">
      <div class="playback-handle" @pointerdown.prevent="startPanelDrag"></div>

      <div class="playback-buttons">
        <button class="ctrl" :disabled="!hasPath" @click="togglePlayback">
          {{ isPlaying ? '暂停' : '播放' }}
        </button>
        <button class="ctrl" :disabled="!hasPath" @click="resetPlayback">重置</button>
      </div>

      <button class="ctrl jump" :disabled="!selectedEvent" @click="jumpToSelectedEvent">跳到选中异常</button>

      <div class="speed-control">
        <button class="ctrl mini" :disabled="speedMultiplier <= minSpeed" type="button" @click="adjustSpeed(-1)">-</button>
        <div class="speed-label">{{ speedMultiplier }}x</div>
        <button class="ctrl mini" :disabled="speedMultiplier >= maxSpeed" type="button" @click="adjustSpeed(1)">+</button>
      </div>

      <div class="progress">
        <div class="progress-bar" :style="{ width: `${progress}%` }"></div>
      </div>

      <div class="status">{{ playbackStatus }} · {{ speedMultiplier }}x</div>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import coordtransform from 'coordtransform'

const props = defineProps({
  trip: { type: Object, default: null },
  segments: { type: Array, default: () => [] },
  events: { type: Array, default: () => [] },
  selectedEventId: { type: String, default: '' },
  center: { type: Array, default: () => [116.397428, 39.90923] },
  zoom: { type: Number, default: 13 },
})

const emit = defineEmits(['select-event'])

const TYPE_COLOR = {
  normal: 'rgba(148, 163, 184, 0.45)',
  detour: '#f59e0b',
  stop: '#ef4444',
  speed_jump: '#8b5cf6',
  drift: '#06b6d4',
  jump_point: '#f97316',
}

const el = ref(null)
const panelEl = ref(null)
let map = null
let overlays = []
let carMarker = null
let rafId = 0
let selectedBadge = null

const minSpeed = 1
const maxSpeed = 20
const speedMultiplier = ref(10)
const isPlaying = ref(false)
const progress = ref(0)
const playbackStatus = ref('等待数据')

const playbackState = reactive({
  path: [],
  durations: [],
  segIdx: 0,
  segProgress: 0,
  segmentStart: 0,
})

const panelPos = reactive({ x: null, y: null })
const panelStyle = computed(() => {
  if (panelPos.x === null || panelPos.y === null) {
    return { right: '12px', bottom: '12px' }
  }
  return {
    left: `${panelPos.x}px`,
    top: `${panelPos.y}px`,
    right: 'auto',
    bottom: 'auto',
  }
})

const selectedEvent = computed(() => props.events.find((item) => item.id === props.selectedEventId) || null)
const hasPath = computed(() => playbackState.path.length > 1)
const legendItems = computed(() => [
  { type: 'detour', label: '绕路', color: TYPE_COLOR.detour },
  { type: 'stop', label: '停留', color: TYPE_COLOR.stop },
  { type: 'speed_jump', label: '速度突变', color: TYPE_COLOR.speed_jump },
  { type: 'drift', label: '漂移', color: TYPE_COLOR.drift },
  { type: 'jump_point', label: '跳点', color: TYPE_COLOR.jump_point },
])

let draggingPanel = false
const dragOrigin = { x: 0, y: 0 }
const panelOrigin = { x: 0, y: 0 }

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max)
}

function getScaledDuration(idx) {
  const rawSeconds = playbackState.durations[idx] || 1
  return (rawSeconds * 1000) / Math.max(speedMultiplier.value, minSpeed)
}

function adjustSpeed(delta) {
  const next = Math.min(maxSpeed, Math.max(minSpeed, speedMultiplier.value + delta))
  if (next === speedMultiplier.value) return
  speedMultiplier.value = next
  if (isPlaying.value && hasPath.value) {
    const duration = getScaledDuration(playbackState.segIdx)
    playbackState.segmentStart = performance.now() - playbackState.segProgress * duration
  }
}

function startPanelDrag(event) {
  if (!panelEl.value || !el.value) return
  draggingPanel = true
  dragOrigin.x = event.clientX
  dragOrigin.y = event.clientY
  const panelRect = panelEl.value.getBoundingClientRect()
  const mapRect = el.value.getBoundingClientRect()
  panelOrigin.x = panelPos.x !== null ? panelPos.x : panelRect.left - mapRect.left
  panelOrigin.y = panelPos.y !== null ? panelPos.y : panelRect.top - mapRect.top
  panelPos.x = panelOrigin.x
  panelPos.y = panelOrigin.y
  window.addEventListener('pointermove', onPanelDrag)
  window.addEventListener('pointerup', stopPanelDrag)
}

function onPanelDrag(event) {
  if (!draggingPanel || !panelEl.value || !el.value) return
  const mapRect = el.value.getBoundingClientRect()
  const panelRect = panelEl.value.getBoundingClientRect()
  const deltaX = event.clientX - dragOrigin.x
  const deltaY = event.clientY - dragOrigin.y
  panelPos.x = clamp(panelOrigin.x + deltaX, 0, Math.max(0, mapRect.width - panelRect.width))
  panelPos.y = clamp(panelOrigin.y + deltaY, 0, Math.max(0, mapRect.height - panelRect.height))
}

function stopPanelDrag() {
  if (!draggingPanel) return
  draggingPanel = false
  window.removeEventListener('pointermove', onPanelDrag)
  window.removeEventListener('pointerup', stopPanelDrag)
}

function toMapPoint(point) {
  if (!point || point.length < 2) return point
  const lon = Number(point[0])
  const lat = Number(point[1])
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return point
  return coordtransform.wgs84togcj02(lon, lat)
}

function haversineKm(lon1, lat1, lon2, lat2) {
  const r = 6371.0
  const toRad = (deg) => (deg * Math.PI) / 180
  const dPhi = toRad(lat2 - lat1)
  const dLam = toRad(lon2 - lon1)
  const phi1 = toRad(lat1)
  const phi2 = toRad(lat2)
  const a = Math.sin(dPhi / 2) ** 2 + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLam / 2) ** 2
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  return r * c
}

function clearOverlays() {
  if (!map) return
  overlays.forEach((item) => {
    try { map.remove(item) } catch { /* noop */ }
  })
  overlays = []
  if (selectedBadge) {
    try { map.remove(selectedBadge) } catch { /* noop */ }
    selectedBadge = null
  }
}

function clearCarMarker() {
  if (carMarker && map) {
    try { map.remove(carMarker) } catch { /* noop */ }
  }
  carMarker = null
}

function placeCar(position) {
  if (!map || !position) return
  if (!carMarker) {
    carMarker = new window.AMap.Marker({
      position,
      offset: new window.AMap.Pixel(-10, -10),
      content: '<div class="car-marker"></div>',
    })
    map.add(carMarker)
  } else {
    carMarker.setPosition(position)
  }
}

function cancelAnimation() {
  if (rafId) {
    cancelAnimationFrame(rafId)
    rafId = 0
  }
}

function buildPlaybackPath() {
  const points = props.trip?.points || []
  cancelAnimation()
  isPlaying.value = false
  playbackState.path = []
  playbackState.durations = []
  playbackState.segIdx = 0
  playbackState.segProgress = 0
  playbackState.segmentStart = 0
  progress.value = 0
  clearCarMarker()

  if (points.length < 2) {
    playbackStatus.value = '暂无轨迹'
    return
  }

  playbackState.path = points.map((point) => toMapPoint([point.lon, point.lat]))
  for (let idx = 0; idx < points.length - 1; idx += 1) {
    const current = points[idx]
    const next = points[idx + 1]
    let dt = null
    if (Number.isFinite(current.t) && Number.isFinite(next.t) && next.t > current.t) {
      dt = next.t - current.t
    } else {
      const speed = Number.isFinite(current.speed_kph) ? current.speed_kph : next.speed_kph
      const distanceKm = haversineKm(current.lon, current.lat, next.lon, next.lat)
      if (speed && speed > 0) {
        dt = (distanceKm / speed) * 3600
      }
    }
    playbackState.durations.push(dt && dt > 0 ? dt : 1)
  }

  playbackStatus.value = '准备回放'
  placeCar(playbackState.path[0])
}

function renderSelectedBadge(event) {
  if (!map || !event) return
  const position = toMapPoint(event.focus_point)
  selectedBadge = new window.AMap.Marker({
    position,
    offset: new window.AMap.Pixel(-44, -46),
    content: `<div class="event-badge" style="--badge-color:${event.color}">${event.title}</div>`,
  })
  overlays.push(selectedBadge)
  map.add(selectedBadge)
}

function draw() {
  if (!map) return
  clearOverlays()

  const points = props.trip?.points || []
  const anomalySegments = props.segments || []
  if (!points.length) return

  const path = points.map((point) => toMapPoint([point.lon, point.lat]))
  const baseLine = new window.AMap.Polyline({
    path,
    strokeColor: TYPE_COLOR.normal,
    strokeWeight: 5,
    strokeOpacity: 0.95,
  })
  overlays.push(baseLine)
  map.add(baseLine)

  for (const segment of anomalySegments) {
    if (!segment || segment.type === 'normal') continue
    const line = new window.AMap.Polyline({
      path: [toMapPoint(segment.start), toMapPoint(segment.end)],
      strokeColor: segment.color || TYPE_COLOR[segment.type] || TYPE_COLOR.normal,
      strokeWeight: segment.type === 'detour' ? 7 : 8,
      strokeOpacity: 0.96,
      zIndex: 12,
    })
    overlays.push(line)
    map.add(line)
  }

  const startMarker = new window.AMap.Marker({
    position: path[0],
    offset: new window.AMap.Pixel(-14, -30),
    content: '<div class="label-marker start">起点</div>',
  })
  const endMarker = new window.AMap.Marker({
    position: path[path.length - 1],
    offset: new window.AMap.Pixel(-14, -30),
    content: '<div class="label-marker end">终点</div>',
  })
  overlays.push(startMarker, endMarker)
  map.add([startMarker, endMarker])

  for (const event of props.events || []) {
    const marker = new window.AMap.Marker({
      position: toMapPoint(event.focus_point),
      offset: new window.AMap.Pixel(-10, -10),
      content: `<div class="anomaly-marker ${event.id === props.selectedEventId ? 'active' : ''}" style="--marker-color:${event.color}"></div>`,
    })
    marker.on('click', () => emit('select-event', event.id))
    overlays.push(marker)
    map.add(marker)
  }

  if (selectedEvent.value) {
    renderSelectedBadge(selectedEvent.value)
  } else {
    map.setFitView(overlays)
  }
}

function focusEvent(event) {
  if (!map || !event || !hasPath.value) return
  const focusPos = toMapPoint(event.focus_point)
  const index = clamp(event.start_index, 0, Math.max(0, playbackState.path.length - 2))
  playbackState.segIdx = index
  playbackState.segProgress = 0
  playbackState.segmentStart = 0
  progress.value = playbackState.path.length > 1 ? (index / (playbackState.path.length - 1)) * 100 : 0
  placeCar(playbackState.path[index] || focusPos)
  map.setZoomAndCenter(16, focusPos)
  playbackStatus.value = `已定位到${event.title}`
}

function jumpToSelectedEvent() {
  if (!selectedEvent.value) return
  focusEvent(selectedEvent.value)
}

function resetPlayback() {
  cancelAnimation()
  isPlaying.value = false
  playbackState.segIdx = 0
  playbackState.segProgress = 0
  progress.value = 0
  if (hasPath.value) {
    placeCar(playbackState.path[0])
    playbackStatus.value = '准备回放'
  } else {
    playbackStatus.value = '暂无轨迹'
    clearCarMarker()
  }
}

function playbackStep(ts) {
  if (!isPlaying.value || !hasPath.value) return
  const duration = getScaledDuration(playbackState.segIdx)
  const elapsed = ts - playbackState.segmentStart
  const ratio = duration > 0 ? Math.min(1, elapsed / duration) : 1
  playbackState.segProgress = ratio

  const start = playbackState.path[playbackState.segIdx]
  const end = playbackState.path[playbackState.segIdx + 1]
  const position = [start[0] + (end[0] - start[0]) * ratio, start[1] + (end[1] - start[1]) * ratio]
  placeCar(position)

  progress.value = ((playbackState.segIdx + ratio) / (playbackState.path.length - 1)) * 100
  playbackStatus.value = '回放中'

  if (ratio >= 1) {
    if (playbackState.segIdx < playbackState.path.length - 2) {
      playbackState.segIdx += 1
      playbackState.segProgress = 0
      playbackState.segmentStart = ts
      rafId = requestAnimationFrame(playbackStep)
    } else {
      isPlaying.value = false
      progress.value = 100
      playbackStatus.value = '已完成'
    }
  } else {
    rafId = requestAnimationFrame(playbackStep)
  }
}

function togglePlayback() {
  if (!hasPath.value) return
  if (isPlaying.value) {
    isPlaying.value = false
    cancelAnimation()
    playbackStatus.value = '已暂停'
    return
  }
  const duration = getScaledDuration(playbackState.segIdx)
  playbackState.segmentStart = performance.now() - playbackState.segProgress * duration
  isPlaying.value = true
  rafId = requestAnimationFrame(playbackStep)
}

onMounted(() => {
  if (!window.AMap) {
    throw new Error('AMap is not loaded. Check index.html script tag.')
  }
  map = new window.AMap.Map(el.value, { zoom: props.zoom, center: props.center })
  buildPlaybackPath()
  draw()
})

watch(
  () => props.trip,
  () => {
    buildPlaybackPath()
    draw()
    if (selectedEvent.value) {
      focusEvent(selectedEvent.value)
    }
  },
  { deep: true }
)

watch(
  () => props.events,
  () => {
    draw()
    if (selectedEvent.value) {
      focusEvent(selectedEvent.value)
    }
  },
  { deep: true }
)

watch(
  () => props.segments,
  () => draw(),
  { deep: true }
)

watch(
  () => props.selectedEventId,
  () => {
    draw()
    if (selectedEvent.value) {
      focusEvent(selectedEvent.value)
    }
  }
)

onBeforeUnmount(() => {
  clearOverlays()
  clearCarMarker()
  cancelAnimation()
  stopPanelDrag()
  try { map?.destroy?.() } catch { /* noop */ }
  map = null
})
</script>

<style scoped>
.map-wrap {
  width: 100%;
  height: 620px;
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

.playback-panel {
  position: absolute;
  right: 12px;
  bottom: 12px;
  background: rgba(255, 255, 255, 0.96);
  padding: 10px;
  border-radius: 12px;
  border: 1px solid var(--border);
  backdrop-filter: blur(10px);
  width: 250px;
  box-shadow: var(--shadow-md);
  z-index: 20;
}

.playback-handle {
  width: 48px;
  height: 6px;
  background: rgba(148, 163, 184, 0.52);
  border-radius: 4px;
  margin: 0 auto 8px;
  cursor: grab;
}

.playback-handle:active {
  cursor: grabbing;
}

.playback-buttons {
  display: flex;
  gap: 8px;
}

.jump {
  width: 100%;
  margin-top: 8px;
}

.speed-control {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-top: 8px;
  gap: 6px;
}

.ctrl {
  flex: 1;
  height: 32px;
  border-radius: 10px;
  border: 1px solid rgba(79, 124, 255, 0.22);
  background: rgba(79, 124, 255, 0.1);
  color: var(--text);
  cursor: pointer;
}

.ctrl:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.ctrl.mini {
  flex: 0;
  width: 32px;
  padding: 0;
}

.speed-label {
  flex: 1;
  text-align: center;
  font-weight: 700;
}

.progress {
  margin-top: 8px;
  height: 6px;
  background: rgba(148, 163, 184, 0.18);
  border-radius: 6px;
  overflow: hidden;
}

.progress-bar {
  height: 100%;
  width: 0;
  background: linear-gradient(90deg, rgba(79, 124, 255, 0.72), rgba(245, 158, 11, 0.82));
  transition: width 0.12s linear;
}

.status {
  margin-top: 6px;
  font-size: 12px;
  color: var(--text-muted);
}

:deep(.label-marker) {
  padding: 4px 8px;
  color: #fff;
  border-radius: 8px;
  font-size: 12px;
  font-weight: 700;
  box-shadow: 0 6px 16px rgba(15, 23, 42, 0.14);
}

:deep(.label-marker.start) {
  background: linear-gradient(135deg, #2f9f67, #55b17f);
}

:deep(.label-marker.end) {
  background: linear-gradient(135deg, #d95b73, #f08a5d);
}

:deep(.anomaly-marker) {
  width: 18px;
  height: 18px;
  border-radius: 999px;
  background: var(--marker-color);
  border: 3px solid rgba(255, 255, 255, 0.95);
  box-shadow: 0 6px 16px rgba(15, 23, 42, 0.16);
}

:deep(.anomaly-marker.active) {
  width: 22px;
  height: 22px;
  transform: translate(-2px, -2px);
  box-shadow: 0 0 0 6px rgba(255, 255, 255, 0.38), 0 10px 22px rgba(15, 23, 42, 0.22);
}

:deep(.event-badge) {
  padding: 6px 10px;
  border-radius: 10px;
  color: #fff;
  background: var(--badge-color);
  font-size: 12px;
  font-weight: 700;
  box-shadow: 0 10px 18px rgba(15, 23, 42, 0.16);
}

:deep(.car-marker) {
  width: 20px;
  height: 20px;
  border-radius: 6px;
  background: linear-gradient(135deg, #7fa6ff, #4f7cff);
  border: 2px solid rgba(255, 255, 255, 0.92);
  box-shadow: 0 6px 16px rgba(15, 23, 42, 0.16);
}
</style>
