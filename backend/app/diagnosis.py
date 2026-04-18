from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
import math
from typing import Any

from .schemas import (
    AnomalyEvent,
    AnomalyEventCount,
    DiagnosisSegment,
    TripDetail,
    TripDiagnosisMetrics,
    TripDiagnosisResponse,
    TripDiagnosisSummary,
)

ANOMALY_COLORS: dict[str, str] = {
    "detour": "#f59e0b",
    "stop": "#ef4444",
    "speed_jump": "#8b5cf6",
    "drift": "#06b6d4",
    "jump_point": "#f97316",
}

SEVERITY_ORDER = {"none": 0, "low": 1, "medium": 2, "high": 3}
SCORE_DEDUCTION = {"low": 10, "medium": 20, "high": 35}

STOP_RADIUS_KM = 0.05
STOP_DURATION_SECONDS = 180.0
STOP_MAX_SPEED_KPH = 5.0

SPEED_JUMP_DELTA_KPH = 35.0
SPEED_JUMP_HIGH_DELTA_KPH = 55.0
SPEED_JUMP_ACTIVE_KPH = 60.0
SPEED_JUMP_LOW_SPEED_KPH = 10.0

JUMP_POINT_RATIO = 3.0
JUMP_POINT_MIN_DISTANCE_KM = 0.5
JUMP_POINT_HIGH_SPEED_KPH = 140.0
JUMP_POINT_SPEED_KPH = 100.0

DRIFT_RADIUS_KM = 0.08
DRIFT_MIN_PATH_KM = 0.25
DRIFT_MAX_NET_KM = 0.05
DRIFT_MIN_POINTS = 4

DETOUR_MIN_DISTANCE_KM = 2.0
DETOUR_MIN_DIRECT_KM = 0.8
DETOUR_RATIO = 2.2
DETOUR_HIGH_RATIO = 3.0
DETOUR_REPEAT_RATIO = 0.25
DETOUR_HIGH_REPEAT_RATIO = 0.45


@dataclass
class SegmentStat:
    start_index: int
    end_index: int
    distance_km: float
    duration_seconds: float
    speed_kph: float | None
    road_id: int | None
    heading_deg: float | None


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _bearing_deg(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_lam = math.radians(lon2 - lon1)
    y = math.sin(d_lam) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(d_lam)
    deg = math.degrees(math.atan2(y, x))
    return (deg + 360.0) % 360.0


def _angle_delta_deg(a: float | None, b: float | None) -> float:
    if a is None or b is None:
        return 0.0
    diff = abs(a - b) % 360.0
    return diff if diff <= 180.0 else 360.0 - diff


def _to_dt(ts: float | None) -> datetime | None:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts))
    except Exception:
        return None


def _build_segment_stats(trip: TripDetail) -> list[SegmentStat]:
    stats: list[SegmentStat] = []
    for idx in range(len(trip.points) - 1):
        p1 = trip.points[idx]
        p2 = trip.points[idx + 1]
        distance_km = _haversine_km(p1.lon, p1.lat, p2.lon, p2.lat)
        duration_seconds = 0.0
        if p1.t is not None and p2.t is not None and p2.t > p1.t:
            duration_seconds = float(p2.t - p1.t)

        speed_kph = p1.speed_kph
        if speed_kph is None and p2.speed_kph is not None:
            speed_kph = p2.speed_kph
        if speed_kph is None and duration_seconds > 0:
            speed_kph = distance_km / (duration_seconds / 3600.0)

        stats.append(
            SegmentStat(
                start_index=idx,
                end_index=idx + 1,
                distance_km=distance_km,
                duration_seconds=duration_seconds,
                speed_kph=float(speed_kph) if speed_kph is not None else None,
                road_id=trip.points[idx].road_id,
                heading_deg=_bearing_deg(p1.lon, p1.lat, p2.lon, p2.lat),
            )
        )
    return stats


def _sum_segment_distance(segments: list[SegmentStat], start_index: int, end_index: int) -> float:
    return sum(seg.distance_km for seg in segments[start_index:end_index])


def _road_repeat_ratio(segments: list[SegmentStat]) -> float:
    roads = [seg.road_id for seg in segments if seg.road_id is not None]
    if not roads:
        return 0.0
    counts = Counter(roads)
    repeated = sum(count for count in counts.values() if count > 1)
    return repeated / len(roads)


def _backtrack_count(segments: list[SegmentStat]) -> int:
    count = 0
    for idx in range(len(segments) - 1):
        if _angle_delta_deg(segments[idx].heading_deg, segments[idx + 1].heading_deg) >= 150.0:
            count += 1
    return count


def _make_event(
    *,
    event_id: str,
    event_type: str,
    severity: str,
    title: str,
    description: str,
    trip: TripDetail,
    start_index: int,
    end_index: int,
    focus_index: int,
    evidence: dict[str, Any],
) -> AnomalyEvent:
    points = trip.points
    start_point = points[max(0, min(start_index, len(points) - 1))]
    end_point = points[max(0, min(end_index, len(points) - 1))]
    focus_point = points[max(0, min(focus_index, len(points) - 1))]
    return AnomalyEvent(
        id=event_id,
        type=event_type,
        severity=severity,
        color=ANOMALY_COLORS[event_type],
        title=title,
        description=description,
        start_index=start_index,
        end_index=end_index,
        focus_index=focus_index,
        start_time=_to_dt(start_point.t),
        end_time=_to_dt(end_point.t),
        start_point=(float(start_point.lon), float(start_point.lat)),
        end_point=(float(end_point.lon), float(end_point.lat)),
        focus_point=(float(focus_point.lon), float(focus_point.lat)),
        evidence=evidence,
    )


def _detect_stop_events(trip: TripDetail, segments: list[SegmentStat]) -> list[AnomalyEvent]:
    events: list[AnomalyEvent] = []
    idx = 1
    while idx < len(trip.points) - 2:
        anchor = trip.points[idx]
        end = idx
        max_radius = 0.0
        while end + 1 < len(trip.points) - 1:
            candidate = trip.points[end + 1]
            radius = _haversine_km(anchor.lon, anchor.lat, candidate.lon, candidate.lat)
            if radius > STOP_RADIUS_KM:
                break
            max_radius = max(max_radius, radius)
            end += 1

        if end > idx and anchor.t is not None and trip.points[end].t is not None:
            duration = float(trip.points[end].t - anchor.t)
            local_distance = _sum_segment_distance(segments, idx, end)
            avg_speed = local_distance / (duration / 3600.0) if duration > 0 else 0.0
            if duration >= STOP_DURATION_SECONDS and avg_speed <= STOP_MAX_SPEED_KPH:
                severity = "high" if duration >= 600 else "medium"
                events.append(
                    _make_event(
                        event_id=f"stop-{idx}-{end}",
                        event_type="stop",
                        severity=severity,
                        title="异常停留",
                        description="车辆在较小范围内停留时间过长，可能存在异常等待或非正常驻留。",
                        trip=trip,
                        start_index=idx,
                        end_index=end,
                        focus_index=(idx + end) // 2,
                        evidence={
                            "duration_seconds": round(duration, 2),
                            "avg_speed_kph": round(avg_speed, 2),
                            "radius_meters": round(max_radius * 1000.0, 2),
                        },
                    )
                )
                idx = end + 1
                continue
        idx += 1
    return events


def _detect_speed_jump_events(trip: TripDetail, segments: list[SegmentStat]) -> list[AnomalyEvent]:
    events: list[AnomalyEvent] = []
    for idx in range(len(segments) - 1):
        first = segments[idx]
        second = segments[idx + 1]
        if first.speed_kph is None or second.speed_kph is None:
            continue
        delta = abs(second.speed_kph - first.speed_kph)
        if delta < SPEED_JUMP_DELTA_KPH:
            continue
        if max(first.speed_kph, second.speed_kph) < SPEED_JUMP_ACTIVE_KPH and min(first.speed_kph, second.speed_kph) > SPEED_JUMP_LOW_SPEED_KPH:
            continue
        severity = "high" if delta >= SPEED_JUMP_HIGH_DELTA_KPH else "medium"
        events.append(
            _make_event(
                event_id=f"speed-jump-{idx}",
                event_type="speed_jump",
                severity=severity,
                title="速度突变",
                description="相邻轨迹段速度变化异常剧烈，可能存在数据突刺或异常驾驶行为。",
                trip=trip,
                start_index=idx,
                end_index=idx + 2,
                focus_index=idx + 1,
                evidence={
                    "speed_before_kph": round(first.speed_kph, 2),
                    "speed_after_kph": round(second.speed_kph, 2),
                    "delta_kph": round(delta, 2),
                },
            )
        )
    return events


def _detect_jump_point_events(trip: TripDetail, segments: list[SegmentStat]) -> list[AnomalyEvent]:
    events: list[AnomalyEvent] = []
    for idx in range(1, len(trip.points) - 1):
        prev_seg = segments[idx - 1]
        next_seg = segments[idx] if idx < len(segments) else None
        if next_seg is None:
            continue

        prev_point = trip.points[idx - 1]
        next_point = trip.points[idx + 1]
        direct = _haversine_km(prev_point.lon, prev_point.lat, next_point.lon, next_point.lat)
        around = prev_seg.distance_km + next_seg.distance_km
        ratio = around / max(direct, 0.001)
        local_max_distance = max(prev_seg.distance_km, next_seg.distance_km)
        local_max_speed = max(prev_seg.speed_kph or 0.0, next_seg.speed_kph or 0.0)
        if ratio < JUMP_POINT_RATIO:
            continue
        if local_max_distance < JUMP_POINT_MIN_DISTANCE_KM and local_max_speed < JUMP_POINT_SPEED_KPH:
            continue

        severity = "high" if local_max_speed >= JUMP_POINT_HIGH_SPEED_KPH or local_max_distance >= 1.0 else "medium"
        events.append(
            _make_event(
                event_id=f"jump-point-{idx}",
                event_type="jump_point",
                severity=severity,
                title="异常跳点",
                description="局部点位与前后轨迹连续性不一致，疑似定位跳变。",
                trip=trip,
                start_index=idx - 1,
                end_index=idx + 1,
                focus_index=idx,
                evidence={
                    "detour_ratio": round(ratio, 2),
                    "max_segment_distance_km": round(local_max_distance, 3),
                    "max_segment_speed_kph": round(local_max_speed, 2),
                },
            )
        )
    return events


def _event_overlaps(events: list[AnomalyEvent], start_index: int, end_index: int, event_type: str | None = None) -> bool:
    for event in events:
        if event_type is not None and event.type != event_type:
            continue
        if max(start_index, event.start_index) <= min(end_index, event.end_index):
            return True
    return False


def _detect_drift_events(trip: TripDetail, segments: list[SegmentStat], existing_events: list[AnomalyEvent]) -> list[AnomalyEvent]:
    events: list[AnomalyEvent] = []
    idx = 0
    while idx <= len(trip.points) - DRIFT_MIN_POINTS:
        anchor = trip.points[idx]
        end = idx
        max_radius = 0.0
        while end + 1 < len(trip.points):
            candidate = trip.points[end + 1]
            radius = _haversine_km(anchor.lon, anchor.lat, candidate.lon, candidate.lat)
            if radius > DRIFT_RADIUS_KM:
                break
            max_radius = max(max_radius, radius)
            end += 1

        if end - idx + 1 >= DRIFT_MIN_POINTS:
            path_km = _sum_segment_distance(segments, idx, end)
            net_km = _haversine_km(anchor.lon, anchor.lat, trip.points[end].lon, trip.points[end].lat)
            if path_km >= DRIFT_MIN_PATH_KM and net_km <= DRIFT_MAX_NET_KM:
                if not _event_overlaps(existing_events, idx, end, "stop"):
                    severity = "high" if path_km >= 0.45 else "medium"
                    events.append(
                        _make_event(
                            event_id=f"drift-{idx}-{end}",
                            event_type="drift",
                            severity=severity,
                            title="定位漂移",
                            description="轨迹点在小范围内来回抖动，但局部累计路径异常偏长，疑似 GPS 漂移。",
                            trip=trip,
                            start_index=idx,
                            end_index=end,
                            focus_index=(idx + end) // 2,
                            evidence={
                                "radius_meters": round(max_radius * 1000.0, 2),
                                "path_km": round(path_km, 3),
                                "net_km": round(net_km, 3),
                            },
                        )
                    )
                    idx = end + 1
                    continue
        idx += 1
    return events


def _detect_detour_event(trip: TripDetail, segments: list[SegmentStat], repeated_road_ratio: float, backtrack_count: int) -> list[AnomalyEvent]:
    actual_distance_km = float(trip.distance_km) if trip.distance_km is not None else sum(seg.distance_km for seg in segments)
    if actual_distance_km < DETOUR_MIN_DISTANCE_KM or len(trip.points) < 2:
        return []
    start = trip.points[0]
    end = trip.points[-1]
    direct_distance_km = _haversine_km(start.lon, start.lat, end.lon, end.lat)
    if direct_distance_km < DETOUR_MIN_DIRECT_KM:
        return []
    ratio = actual_distance_km / max(direct_distance_km, 0.001)
    if ratio < DETOUR_RATIO:
        return []
    if repeated_road_ratio < DETOUR_REPEAT_RATIO and backtrack_count < 1:
        return []

    severity = "high" if ratio >= DETOUR_HIGH_RATIO or repeated_road_ratio >= DETOUR_HIGH_REPEAT_RATIO else "medium"
    return [
        _make_event(
            event_id="detour-trip",
            event_type="detour",
            severity=severity,
            title="疑似绕路",
            description="行程实际距离明显高于起终点直线距离，并伴随道路重复或回摆特征。",
            trip=trip,
            start_index=0,
            end_index=len(trip.points) - 1,
            focus_index=max(0, len(trip.points) // 2),
            evidence={
                "actual_distance_km": round(actual_distance_km, 3),
                "direct_distance_km": round(direct_distance_km, 3),
                "directness_ratio": round(ratio, 2),
                "repeated_road_ratio": round(repeated_road_ratio, 3),
                "backtrack_count": backtrack_count,
            },
        )
    ]


def _build_segments(trip: TripDetail, stats: list[SegmentStat], events: list[AnomalyEvent]) -> list[DiagnosisSegment]:
    segments: list[DiagnosisSegment] = []
    segment_types = ["normal"] * len(stats)
    segment_severity = ["none"] * len(stats)

    for event in sorted(
        events,
        key=lambda item: (SEVERITY_ORDER[item.severity], -(item.end_index - item.start_index), item.type),
    ):
        start_segment = max(0, min(event.start_index, len(stats) - 1))
        end_segment = max(start_segment, min(event.end_index - 1, len(stats) - 1))
        for idx in range(start_segment, end_segment + 1):
            current = segment_severity[idx]
            if SEVERITY_ORDER[event.severity] >= SEVERITY_ORDER[current]:
                segment_types[idx] = event.type
                segment_severity[idx] = event.severity

    for idx, stat in enumerate(stats):
        p1 = trip.points[stat.start_index]
        p2 = trip.points[stat.end_index]
        segments.append(
            DiagnosisSegment(
                start=(float(p1.lon), float(p1.lat)),
                end=(float(p2.lon), float(p2.lat)),
                start_index=stat.start_index,
                end_index=stat.end_index,
                type=segment_types[idx],
                severity=segment_severity[idx],
                speed_kph=float(stat.speed_kph) if stat.speed_kph is not None else None,
                road_id=stat.road_id,
                color=ANOMALY_COLORS.get(segment_types[idx]),
            )
        )
    return segments


def _event_counts(events: list[AnomalyEvent]) -> list[AnomalyEventCount]:
    counts = Counter(event.type for event in events)
    ordered = ["detour", "stop", "speed_jump", "drift", "jump_point"]
    return [AnomalyEventCount(type=event_type, count=int(counts.get(event_type, 0))) for event_type in ordered]


def analyze_trip_diagnosis(trip: TripDetail) -> TripDiagnosisResponse:
    stats = _build_segment_stats(trip)
    events: list[AnomalyEvent] = []

    stop_events = _detect_stop_events(trip, stats)
    speed_jump_events = _detect_speed_jump_events(trip, stats)
    jump_point_events = _detect_jump_point_events(trip, stats)
    drift_events = _detect_drift_events(trip, stats, stop_events)
    repeated_road_ratio = _road_repeat_ratio(stats)
    backtrack_count = _backtrack_count(stats)
    detour_events = _detect_detour_event(trip, stats, repeated_road_ratio, backtrack_count)

    events.extend(stop_events)
    events.extend(speed_jump_events)
    events.extend(jump_point_events)
    events.extend(drift_events)
    events.extend(detour_events)
    events.sort(key=lambda item: (item.start_index, item.end_index, item.type))

    actual_distance_km = float(trip.distance_km) if trip.distance_km is not None else sum(seg.distance_km for seg in stats)
    direct_distance_km = 0.0
    if len(trip.points) >= 2:
        direct_distance_km = _haversine_km(trip.points[0].lon, trip.points[0].lat, trip.points[-1].lon, trip.points[-1].lat)
    max_speed_kph = max((seg.speed_kph or 0.0) for seg in stats) if stats else 0.0
    stop_seconds_total = sum(float(event.evidence.get("duration_seconds", 0.0)) for event in events if event.type == "stop")

    score = 100
    for event in events:
        score -= SCORE_DEDUCTION[event.severity]
    anomaly_score = max(0, score)

    medium_count = sum(1 for event in events if event.severity == "medium")
    high_count = sum(1 for event in events if event.severity == "high")
    if high_count >= 1 or medium_count >= 2:
        risk_level = "high"
    elif medium_count == 1 or len(events) >= 2:
        risk_level = "medium"
    else:
        risk_level = "low"

    return TripDiagnosisResponse(
        trip=trip,
        summary=TripDiagnosisSummary(
            risk_level=risk_level,
            anomaly_score=anomaly_score,
            total_events=len(events),
            event_counts=_event_counts(events),
        ),
        metrics=TripDiagnosisMetrics(
            direct_distance_km=round(direct_distance_km, 3),
            actual_distance_km=round(actual_distance_km, 3),
            directness_ratio=round(actual_distance_km / max(direct_distance_km, 0.001), 2) if direct_distance_km > 0 else None,
            max_speed_kph=round(max_speed_kph, 2),
            stop_seconds_total=round(stop_seconds_total, 2),
            repeated_road_ratio=round(repeated_road_ratio, 3),
            backtrack_count=backtrack_count,
        ),
        events=events,
        segments=_build_segments(trip, stats, events),
    )
