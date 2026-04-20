<template>
  <div class="map-wrap">
    <div ref="el" class="map"></div>

    <div class="legend">
      <div class="legend-title">未来热力强度</div>
      <div class="legend-bar"></div>
      <div class="legend-scale">
        <span>低</span>
        <span>高</span>
      </div>
    </div>
    <div class="playback-panel">
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
        <div class="progress-bar" :style="{ width: `${progress}%` }"></div>
      </div>

      <div class="status">{{ playbackStatus }} · {{ speedMultiplier }}x · {{ currentSimSpeedLabel }}</div>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import coordtransform from 'coordtransform'

const props = defineProps({
  points: { type: Array, default: () => [] },
  pathPoints: { type: Array, default: () => [] },
  center: { type: Array, default: () => [126.63, 45.75] },
  zoom: { type: Number, default: 11 },
  radiusMeters: { type: Number, default: 10 },
  minFitZoom: { type: Number, default: 16 },
})

const el = ref(null)
let map = null
let heatmap = null
let overlays = []
let lastFitPathKey = ''
let carMarker = null
let rafId = 0

const minSpeed = 1
const maxSpeed = 20
const speedMultiplier = ref(10)
const isPlaying = ref(false)
const progress = ref(0)
const playbackStatus = ref('等待轨迹')

const playbackState = reactive({
  path: [],
  durations: [],
  segmentSpeeds: [],
  segIdx: 0,
  segProgress: 0,
  segmentStart: 0,
})

const currentSimSpeedLabel = computed(() => {
  const speed = Number(playbackState.segmentSpeeds[playbackState.segIdx] || 0)
  if (!Number.isFinite(speed) || speed <= 0) return '速度 -'
  return `速度 ${speed.toFixed(1)} km/h`
})

const hasPath = computed(() => playbackState.path.length > 1)

function calcHeatRadius(radiusMeters) {
  return Math.max(15, Math.min(22, Math.round(Number(radiusMeters || 10) / 3)))
}

function toMapPoint(point) {
  if (!point || point.length < 2) return point
  const lon = Number(point[0])
  const lat = Number(point[1])
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return point
  return coordtransform.wgs84togcj02(lon, lat)
}

function clamp(value, min = 0, max = 1) {
  return Math.min(max, Math.max(min, Number(value || 0)))
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

function normalizePathPoint(point) {
  if (!point || point.length < 2) return null
  const lon = Number(point[0])
  const lat = Number(point[1])
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null
  return [lon, lat]
}

function getNearestIntensityAt(lon, lat, points) {
  if (!Array.isArray(points) || points.length === 0) return 0
  let nearestIntensity = 0
  let nearestDistanceKm = Number.POSITIVE_INFINITY
  for (const item of points) {
    const pLon = Number(item?.lon)
    const pLat = Number(item?.lat)
    if (!Number.isFinite(pLon) || !Number.isFinite(pLat)) continue
    const d = haversineKm(lon, lat, pLon, pLat)
    if (d < nearestDistanceKm) {
      nearestDistanceKm = d
      nearestIntensity = clamp(Number(item?.intensity ?? 0), 0, 1)
    }
  }
  return nearestDistanceKm <= 0.8 ? nearestIntensity : 0
}

function segmentPlaybackSpeedKph(intensity) {
  const normalized = clamp(intensity, 0, 1)
  const base = 40.0
  const speed = base * Math.pow(1 - normalized, 4)
  return Math.max(3.0, Math.min(40.0, speed))
}

function cancelAnimation() {
  if (rafId) {
    cancelAnimationFrame(rafId)
    rafId = 0
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
      zIndex: 40,
    })
    map.add(carMarker)
  } else {
    carMarker.setPosition(position)
  }
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

function buildPlaybackPath() {
  cancelAnimation()
  isPlaying.value = false
  playbackState.path = []
  playbackState.durations = []
  playbackState.segmentSpeeds = []
  playbackState.segIdx = 0
  playbackState.segProgress = 0
  playbackState.segmentStart = 0
  progress.value = 0
  clearCarMarker()

  const source = props.pathPoints || []
  if (source.length < 2) {
    playbackStatus.value = '暂无轨迹'
    return
  }

  const rawPath = source.map((item) => normalizePathPoint(item)).filter((p) => p)
  playbackState.path = rawPath
    .map((item) => toMapPoint(item))
    .filter((p) => p && Number.isFinite(p[0]) && Number.isFinite(p[1]))

  if (playbackState.path.length < 2 || rawPath.length < 2) {
    playbackStatus.value = '暂无轨迹'
    return
  }

  const predictedPoints = (props.points || []).filter((item) => Number.isFinite(Number(item?.lon)) && Number.isFinite(Number(item?.lat)))

  for (let idx = 0; idx < rawPath.length - 1; idx += 1) {
    const a = rawPath[idx]
    const b = rawPath[idx + 1]
    const distanceKm = haversineKm(a[0], a[1], b[0], b[1])
    const midLon = (a[0] + b[0]) / 2
    const midLat = (a[1] + b[1]) / 2
    const segIntensity = getNearestIntensityAt(midLon, midLat, predictedPoints)
    const speedKph = segmentPlaybackSpeedKph(segIntensity)
    const durationSec = Math.max(1, (distanceKm / speedKph) * 3600)
    playbackState.segmentSpeeds.push(speedKph)
    playbackState.durations.push(durationSec)
  }

  playbackStatus.value = '准备模拟'
  placeCar(playbackState.path[0])
}

function resetPlayback() {
  cancelAnimation()
  isPlaying.value = false
  playbackState.segIdx = 0
  playbackState.segProgress = 0
  progress.value = 0
  if (hasPath.value) {
    placeCar(playbackState.path[0])
    playbackStatus.value = '准备模拟'
  } else {
    playbackStatus.value = '暂无轨迹'
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

function clearOverlays() {
  if (!map) return
  overlays.forEach((item) => {
    try { map.remove(item) } catch { /* noop */ }
  })
  overlays = []
}

function draw() {
  if (!map || !heatmap) return
  clearOverlays()

  const raw = props.points || []
  const hasIntensity = raw.some((item) => Number.isFinite(Number(item?.intensity)))
  const pathPoints = (props.pathPoints || []).map((item) => toMapPoint(item))
  const pathFitKey = pathPoints.length >= 2
    ? `${pathPoints.length}:${pathPoints[0]?.[0]}-${pathPoints[0]?.[1]}:${pathPoints[pathPoints.length - 1]?.[0]}-${pathPoints[pathPoints.length - 1]?.[1]}`
    : ''
  const data = raw
    .map((item) => {
      const mapPoint = toMapPoint([item.lon, item.lat])
      const intensity = Number(item.intensity)
      const predictedTrips = Number(item.predicted_trips)
      const baseIntensity = clamp(intensity, 0, 1)
      return {
        lng: mapPoint[0],
        lat: mapPoint[1],
        count: hasIntensity
          // Slight gamma boost makes medium/high peaks easier to see
          // without reverting to fully relative scaling.
          ? Math.pow(baseIntensity, 0.75)
          : (Number.isFinite(predictedTrips) ? predictedTrips : 0),
      }
    })
    .filter((item) => Number.isFinite(item.lng) && Number.isFinite(item.lat) && item.count > 0)

  const max = data.reduce((acc, item) => Math.max(acc, item.count), 0)
  heatmap.setDataSet({
    data,
    // Keep near-absolute intensity while ensuring low-variance windows
    // still have enough visual contrast for inspection.
    max: hasIntensity ? Math.max(0.25, max || 0) : (max || 1),
  })

  const fitTargets = []

  if (pathPoints.length >= 2) {
    const line = new window.AMap.Polyline({
      path: pathPoints,
      strokeColor: '#334155',
      strokeWeight: 5,
      strokeOpacity: 0.72,
      zIndex: 18,
    })
    const startMarker = new window.AMap.Marker({
      position: pathPoints[0],
      offset: new window.AMap.Pixel(-14, -30),
      content: '<div class="label-marker start">起点</div>',
      zIndex: 30,
    })
    const endMarker = new window.AMap.Marker({
      position: pathPoints[pathPoints.length - 1],
      offset: new window.AMap.Pixel(-14, -30),
      content: '<div class="label-marker end">终点</div>',
      zIndex: 30,
    })
    overlays.push(line)
    overlays.push(startMarker, endMarker)
    fitTargets.push(line)
  }

  if (overlays.length) {
    map.add(overlays)
  }

  const shouldFit = pathFitKey && pathFitKey !== lastFitPathKey
  if (shouldFit) {
    if (fitTargets.length) {
      map.setFitView(fitTargets)
    } else if (pathPoints.length >= 2) {
      map.setFitView()
    }

    const minFitZoom = Number(props.minFitZoom || 0)
    if (minFitZoom > 0) {
      const currentZoom = Number(map.getZoom?.() || 0)
      if (currentZoom > 0 && currentZoom < minFitZoom) {
        map.setZoom(minFitZoom)
      }
    }
    lastFitPathKey = pathFitKey
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

  map.plugin(['AMap.HeatMap'], () => {
    heatmap = new window.AMap.HeatMap(map, {
      radius: calcHeatRadius(props.radiusMeters),
      opacity: [0.12, 0.92],
      gradient: {
        0.0: '#16a34a',
        0.35: '#84cc16',
        0.7: '#f59e0b',
        1.0: '#ef4444',
      },
    })
    buildPlaybackPath()
    draw()
  })
})

watch(
  () => props.points,
  () => {
    buildPlaybackPath()
    draw()
  },
  { deep: true }
)

watch(
  () => props.pathPoints,
  () => {
    buildPlaybackPath()
    draw()
  },
  { deep: true }
)

watch(
  () => props.radiusMeters,
  () => {
    if (!heatmap) return
    heatmap.setOptions({
      radius: calcHeatRadius(props.radiusMeters),
    })
    draw()
  }
)

onBeforeUnmount(() => {
  clearOverlays()
  clearCarMarker()
  cancelAnimation()
  try { map?.destroy?.() } catch { /* noop */ }
  map = null
  heatmap = null
  lastFitPathKey = ''
})
</script>

<style scoped>
.map-wrap {
  width: 100%;
  height: 520px;
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
  width: 140px;
  padding: 10px 12px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.94);
  border: 1px solid var(--border);
  box-shadow: var(--shadow-sm);
  z-index: 20;
}

.legend-title {
  font-size: 12px;
  color: var(--text);
  font-weight: 600;
}

.legend-bar {
  margin-top: 8px;
  height: 10px;
  border-radius: 999px;
  background: linear-gradient(90deg, #16a34a 0%, #84cc16 35%, #f59e0b 70%, #ef4444 100%);
}

.legend-scale {
  margin-top: 6px;
  display: flex;
  justify-content: space-between;
  font-size: 11px;
  color: var(--text-muted);
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

:deep(.car-marker) {
  width: 20px;
  height: 20px;
  border-radius: 6px;
  background: linear-gradient(135deg, #7fa6ff, #4f7cff);
  border: 2px solid rgba(255, 255, 255, 0.92);
  box-shadow: 0 6px 16px rgba(15, 23, 42, 0.16);
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

</style>
