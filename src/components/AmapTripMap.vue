<template>
  <div class="map-wrap">
    <div ref="el" class="map"></div>
    <div class="playback-panel" ref="panelEl" :style="panelStyle">
      <div class="playback-handle" @pointerdown.prevent="startPanelDrag"></div>
        <div class="playback-buttons">
            <button class="ctrl" :disabled="!hasPath" @click="togglePlayback">
              {{ isPlaying ? '暂停' : '播放' }}
            </button>
            <button class="ctrl" :disabled="!hasPath" @click="resetPlayback">重置</button>
          </div>
          <div class="speed-control">
            <button class="ctrl mini" :disabled="speedMultiplier <= minSpeed" type="button" @click="adjustSpeed(-1)">-</button>
            <div class="speed-label">{{ speedMultiplier }}x</div>
            <button class="ctrl mini" :disabled="speedMultiplier >= maxSpeed" type="button" @click="adjustSpeed(1)">+</button>
          </div>
          <div class="progress">
            <div class="progress-bar" :style="{ width: progress + '%' }"></div>
          </div>
          <div class="status">{{ playbackStatus }} · {{ speedMultiplier }}x</div>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import coordtransform from 'coordtransform'

const props = defineProps({
  center: { type: Array, default: () => [116.397428, 39.90923] },
  zoom: { type: Number, default: 13 },
  segments: { type: Array, default: () => [] }, // [{start:[lon,lat], end:[lon,lat], status}]
  trip: { type: Object, default: null },
})

const el = ref(null)
const panelEl = ref(null)
let map = null
let overlays = []
let carMarker = null
let rafId = 0

const isPlaying = ref(false)
const progress = ref(0)
const playbackStatus = ref('等待数据')
const minSpeed = 1
const maxSpeed = 20
const speedMultiplier = ref(10)

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
let draggingPanel = false
const dragOrigin = { x: 0, y: 0 }
const panelOrigin = { x: 0, y: 0 }

const hasPath = computed(() => (playbackState.path?.length || 0) > 1)

function getScaledDuration(idx) {
  const rawSeconds = playbackState.durations[idx] || 1
  const multiplier = Math.max(speedMultiplier.value, minSpeed)
  return (rawSeconds * 1000) / multiplier
}

function adjustSpeed(delta) {
  const next = Math.min(maxSpeed, Math.max(minSpeed, speedMultiplier.value + delta))
  if (next === speedMultiplier.value) return
  speedMultiplier.value = next
  if (isPlaying.value && hasPath.value) {
    const newDuration = getScaledDuration(playbackState.segIdx)
    playbackState.segmentStart = performance.now() - playbackState.segProgress * newDuration
  }
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max)
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
  let deltaX = event.clientX - dragOrigin.x
  let deltaY = event.clientY - dragOrigin.y
  const newLeft = clamp(panelOrigin.x + deltaX, 0, Math.max(0, mapRect.width - panelRect.width))
  const newTop = clamp(panelOrigin.y + deltaY, 0, Math.max(0, mapRect.height - panelRect.height))
  panelPos.x = newLeft
  panelPos.y = newTop
}

function stopPanelDrag() {
  if (!draggingPanel) return
  draggingPanel = false
  window.removeEventListener('pointermove', onPanelDrag)
  window.removeEventListener('pointerup', stopPanelDrag)
}

const toMapPoint = (p) => {
  if (!p || p.length < 2) return p
  const lon = Number(p[0])
  const lat = Number(p[1])
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return p
  const out = coordtransform.wgs84togcj02(lon, lat)
  return [out[0], out[1]]
}

function clearOverlays() {
  if (!map) return
  overlays.forEach(o => {
    try { map.remove(o) } catch { /* noop */ }
  })
  overlays = []
}

function clearCarMarker() {
  if (carMarker && map) {
    try { map.remove(carMarker) } catch { /* noop */ }
  }
  carMarker = null
}

function haversineKm(lon1, lat1, lon2, lat2) {
  const r = 6371.0
  const toRad = (d) => (d * Math.PI) / 180
  const dPhi = toRad(lat2 - lat1)
  const dLam = toRad(lon2 - lon1)
  const phi1 = toRad(lat1)
  const phi2 = toRad(lat2)
  const a = Math.sin(dPhi / 2) ** 2 + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLam / 2) ** 2
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  return r * c
}

function buildPlaybackPath() {
  const pts = props.trip?.points || []
  cancelAnimation()
  isPlaying.value = false
  playbackState.path = []
  playbackState.durations = []
  playbackState.segIdx = 0
  playbackState.segProgress = 0
  playbackState.segmentStart = 0
  progress.value = 0
  clearCarMarker()
  if (!pts.length || pts.length < 2) {
    playbackStatus.value = '暂无轨迹'
    return
  }

  playbackState.path = pts.map((p) => toMapPoint([p.lon, p.lat]))
  for (let i = 0; i < pts.length - 1; i++) {
    const p1 = pts[i]
    const p2 = pts[i + 1]
    let dt = null
    if (Number.isFinite(p1.t) && Number.isFinite(p2.t) && p2.t > p1.t) {
      dt = p2.t - p1.t
    } else {
      const sp = Number.isFinite(p1.speed_kph) ? p1.speed_kph : p2.speed_kph
      const distKm = haversineKm(p1.lon, p1.lat, p2.lon, p2.lat)
      if (sp && sp > 0) dt = (distKm / sp) * 3600
    }
    if (!dt || dt <= 0) dt = 1
    playbackState.durations.push(dt)
  }
  playbackStatus.value = '准备播放'
  placeCar(playbackState.path[0])
}

function placeCar(pos) {
  if (!map || !pos) return
  if (!carMarker) {
    carMarker = new window.AMap.Marker({
      position: pos,
      offset: new window.AMap.Pixel(-10, -10),
      content: '<div class="car-marker"></div>',
    })
    map.add(carMarker)
  } else {
    carMarker.setPosition(pos)
  }
}

function draw() {
  if (!map) return
  clearOverlays()
  const segs = props.segments || []
  if (!segs.length && !hasPath.value) return

  const path = []
  for (const s of segs) path.push(toMapPoint(s.start))
  if (segs.length) path.push(toMapPoint(segs[segs.length - 1].end))

  for (const s of segs) {
    const color = s.status === 'congested' ? '#ef4444' : '#22c55e'
    const start = toMapPoint(s.start)
    const end = toMapPoint(s.end)
    const line = new window.AMap.Polyline({
      path: [start, end],
      strokeColor: color,
      strokeWeight: 6,
      strokeOpacity: 0.9,
    })
    overlays.push(line)
    map.add(line)

  }

  const startPos = path[0] || playbackState.path[0]
  const endPos = path[path.length - 1] || playbackState.path[playbackState.path.length - 1]
  if (startPos && endPos) {
    const startMarker = new window.AMap.Marker({
      position: startPos,
      offset: new window.AMap.Pixel(-14, -30),
      content: '<div class="label-marker start">起点</div>',
    })
    const endMarker = new window.AMap.Marker({
      position: endPos,
      offset: new window.AMap.Pixel(-14, -30),
      content: '<div class="label-marker end">终点</div>',
    })
    overlays.push(startMarker, endMarker)
    map.add([startMarker, endMarker])
  }

  map.setFitView(overlays)
}

onMounted(() => {
  if (!window.AMap) {
    // AMap 脚本在 index.html 引入
    throw new Error('AMap is not loaded. Check index.html script tag.')
  }
  map = new window.AMap.Map(el.value, { zoom: props.zoom, center: props.center })
   buildPlaybackPath()
  draw()
})

watch(() => props.segments, () => draw(), { deep: true })
watch(() => props.trip, () => { buildPlaybackPath(); draw() }, { deep: true })

function cancelAnimation() {
  if (rafId) {
    cancelAnimationFrame(rafId)
    rafId = 0
  }
}

function resetPlayback() {
  cancelAnimation()
  isPlaying.value = false
  playbackState.segIdx = 0
  playbackState.segProgress = 0
  progress.value = 0
  if (hasPath.value) {
    playbackStatus.value = '准备播放'
    placeCar(playbackState.path[0])
  } else {
    playbackStatus.value = '暂无轨迹'
    clearCarMarker()
  }
}

function playbackStep(ts) {
  if (!isPlaying.value || !hasPath.value) return
  const segDurMs = getScaledDuration(playbackState.segIdx)
  const elapsed = ts - playbackState.segmentStart
  const ratio = segDurMs > 0 ? Math.min(1, elapsed / segDurMs) : 1
  playbackState.segProgress = ratio

  const a = playbackState.path[playbackState.segIdx]
  const b = playbackState.path[playbackState.segIdx + 1]
  const pos = [a[0] + (b[0] - a[0]) * ratio, a[1] + (b[1] - a[1]) * ratio]
  placeCar(pos)

  progress.value = ((playbackState.segIdx + ratio) / (playbackState.path.length - 1)) * 100
  playbackStatus.value = '播放中'

  if (ratio >= 1) {
    if (playbackState.segIdx < playbackState.path.length - 2) {
      playbackState.segIdx += 1
      playbackState.segProgress = 0
      playbackState.segmentStart = ts
      rafId = requestAnimationFrame(playbackStep)
    } else {
      isPlaying.value = false
      playbackStatus.value = '已完成'
      progress.value = 100
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
  const segDurMs = getScaledDuration(playbackState.segIdx)
  playbackState.segmentStart = performance.now() - playbackState.segProgress * segDurMs
  isPlaying.value = true
  rafId = requestAnimationFrame(playbackStep)
}

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
  height: 560px;
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
.playback-panel {
  position: absolute;
  right: 12px;
  bottom: 12px;
  background: rgba(255, 255, 255, 0.96);
  padding: 10px;
  border-radius: 12px;
  border: 1px solid var(--border);
  backdrop-filter: blur(10px);
  width: 240px;
  box-shadow: var(--shadow-md);
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
.speed-control {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-top: 8px;
  gap: 6px;
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
  background: linear-gradient(90deg, rgba(47, 159, 103, 0.78), rgba(79, 124, 255, 0.72));
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
:deep(.arrow-marker) {
  width: 0;
  height: 0;
  border-top: 7px solid transparent;
  border-bottom: 7px solid transparent;
  border-left: 12px solid #2f9f67;
  transform-origin: center;
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

