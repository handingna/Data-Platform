from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
import math
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .schemas import (
    ActiveTimeBin,
    CarPortraitResponse,
    CarPortraitSummary,
    DailyRhythmItem,
    PeerGroupItem,
    RegionRadarItem,
    RouteClusterItem,
)

GRID_SIZE_DEGREES = 0.02
REGION_ORDER = ["north", "northeast", "east", "southeast", "south", "southwest", "west", "northwest"]
SHIFT_BUCKETS = {
    "night": {"00-02", "02-04", "04-06", "22-24"},
    "morning_peak": {"06-08", "08-10"},
    "daytime": {"10-12", "12-14", "14-16"},
    "evening_peak": {"16-18", "18-20", "20-22"},
}


@dataclass(slots=True)
class VehicleTripSample:
    trip_id: int
    device_id: str
    log_date: date
    distance_km: float
    duration_seconds: float | None
    start_time: datetime | None
    end_time: datetime | None
    start_point: tuple[float, float] | None
    end_point: tuple[float, float] | None


def _duration_seconds_from_interval(val: Any) -> float | None:
    if val is None:
        return None
    seconds = getattr(val, "total_seconds", None)
    if callable(seconds):
        return float(seconds())
    return None


def _two_hour_bins() -> list[tuple[int, int, str]]:
    bins: list[tuple[int, int, str]] = []
    for start in range(0, 24, 2):
        end = start + 2
        bins.append((start, end, f"{start:02d}-{end:02d}"))
    return bins


def _sample_from_row(row: Any) -> VehicleTripSample:
    start_point = None
    if row.get("start_lon") is not None and row.get("start_lat") is not None:
        start_point = (float(row["start_lon"]), float(row["start_lat"]))

    end_point = None
    if row.get("end_lon") is not None and row.get("end_lat") is not None:
        end_point = (float(row["end_lon"]), float(row["end_lat"]))

    return VehicleTripSample(
        trip_id=int(row["trip_id"]),
        device_id=str(row.get("devid") if row.get("devid") is not None else row.get("device_id")),
        log_date=row["log_date"],
        distance_km=float(row.get("distance_km") or 0.0),
        duration_seconds=_duration_seconds_from_interval(row.get("duration")),
        start_time=row.get("start_time"),
        end_time=row.get("end_time"),
        start_point=start_point,
        end_point=end_point,
    )


def _sort_trip_samples(trips: list[VehicleTripSample]) -> list[VehicleTripSample]:
    return sorted(
        trips,
        key=lambda item: (
            item.start_time or datetime.combine(item.log_date, dt_time.min),
            item.trip_id,
        ),
    )


def _hour_of_day(value: datetime | None, *, base_date: date | None = None) -> float | None:
    if value is None:
        return None
    if base_date is None:
        return value.hour + (value.minute / 60.0) + (value.second / 3600.0)
    base_dt = datetime.combine(base_date, dt_time.min)
    return (value - base_dt).total_seconds() / 3600.0


def build_active_time_bins(trips: list[VehicleTripSample]) -> list[ActiveTimeBin]:
    trip_total = len(trips)
    counts = {label: 0 for _, _, label in _two_hour_bins()}
    distances = {label: 0.0 for _, _, label in _two_hour_bins()}

    for trip in trips:
        if trip.start_time is None:
            continue
        hour = trip.start_time.hour
        label = f"{(hour // 2) * 2:02d}-{((hour // 2) * 2) + 2:02d}"
        counts[label] += 1
        distances[label] += trip.distance_km

    return [
        ActiveTimeBin(
            label=label,
            trip_count=counts[label],
            distance_km=round(distances[label], 2),
            share_ratio=round((counts[label] / trip_total), 4) if trip_total else 0.0,
        )
        for _, _, label in _two_hour_bins()
    ]


def _direction_from_point(
    point: tuple[float, float],
    center: tuple[float, float],
) -> str:
    dx = point[0] - center[0]
    dy = point[1] - center[1]
    if abs(dx) < 1e-9 and abs(dy) < 1e-9:
        return "north"

    angle = (math.degrees(math.atan2(dy, dx)) + 360.0) % 360.0
    if 67.5 <= angle < 112.5:
        return "north"
    if 22.5 <= angle < 67.5:
        return "northeast"
    if angle >= 337.5 or angle < 22.5:
        return "east"
    if 292.5 <= angle < 337.5:
        return "southeast"
    if 247.5 <= angle < 292.5:
        return "south"
    if 202.5 <= angle < 247.5:
        return "southwest"
    if 157.5 <= angle < 202.5:
        return "west"
    return "northwest"


def build_region_radar(trips: list[VehicleTripSample]) -> tuple[list[RegionRadarItem], float]:
    points: list[tuple[float, float]] = []
    for trip in trips:
        if trip.start_point is not None:
            points.append(trip.start_point)
        if trip.end_point is not None:
            points.append(trip.end_point)

    if not points:
        return [RegionRadarItem(region=region, score=0.0, trip_count=0) for region in REGION_ORDER], 0.0

    center = (
        sum(point[0] for point in points) / len(points),
        sum(point[1] for point in points) / len(points),
    )
    counts = {region: 0 for region in REGION_ORDER}
    for point in points:
        counts[_direction_from_point(point, center)] += 1

    max_count = max(counts.values()) if counts else 0
    hotspot_concentration = (max_count / len(points)) if points else 0.0
    items = [
        RegionRadarItem(
            region=region,
            score=round((counts[region] / max_count) * 100.0, 2) if max_count else 0.0,
            trip_count=counts[region],
        )
        for region in REGION_ORDER
    ]
    return items, round(hotspot_concentration, 4)


def build_daily_rhythm(trips: list[VehicleTripSample]) -> list[DailyRhythmItem]:
    groups: dict[date, dict[str, Any]] = {}
    for trip in trips:
        bucket = groups.setdefault(
            trip.log_date,
            {
                "first_start_hour": None,
                "last_end_hour": None,
                "trip_count": 0,
                "distance_km": 0.0,
            },
        )
        bucket["trip_count"] += 1
        bucket["distance_km"] += trip.distance_km

        start_hour = _hour_of_day(trip.start_time, base_date=trip.log_date)
        end_hour = _hour_of_day(trip.end_time, base_date=trip.log_date)

        if start_hour is not None:
            prev = bucket["first_start_hour"]
            bucket["first_start_hour"] = start_hour if prev is None else min(prev, start_hour)
        if end_hour is not None:
            prev = bucket["last_end_hour"]
            bucket["last_end_hour"] = end_hour if prev is None else max(prev, end_hour)

    items: list[DailyRhythmItem] = []
    for log_date, bucket in sorted(groups.items(), key=lambda item: item[0]):
        first_start = bucket["first_start_hour"]
        last_end = bucket["last_end_hour"]
        span = None
        if first_start is not None and last_end is not None:
            span = max(0.0, last_end - first_start)

        items.append(
            DailyRhythmItem(
                date=log_date,
                first_start_hour=round(first_start, 2) if first_start is not None else None,
                last_end_hour=round(last_end, 2) if last_end is not None else None,
                work_span_hours=round(span, 2) if span is not None else None,
                trip_count=int(bucket["trip_count"]),
                distance_km=round(bucket["distance_km"], 2),
            )
        )
    return items


def _grid_cell(point: tuple[float, float], size: float = GRID_SIZE_DEGREES) -> tuple[int, int]:
    return (math.floor(point[0] / size), math.floor(point[1] / size))


def _start_hour_for_trip(trip: VehicleTripSample) -> float | None:
    return _hour_of_day(trip.start_time)


def build_route_clusters(trips: list[VehicleTripSample], limit: int = 5) -> list[RouteClusterItem]:
    valid_trips = [trip for trip in trips if trip.start_point is not None and trip.end_point is not None]
    if len(valid_trips) < 2:
        return []

    buckets: dict[str, dict[str, Any]] = {}
    for trip in valid_trips:
        start_cell = _grid_cell(trip.start_point)
        end_cell = _grid_cell(trip.end_point)
        key = f"{start_cell[0]}:{start_cell[1]}->{end_cell[0]}:{end_cell[1]}"
        bucket = buckets.setdefault(
            key,
            {
                "trip_count": 0,
                "distance_sum": 0.0,
                "start_hour_sum": 0.0,
                "start_hour_count": 0,
                "start_lon_sum": 0.0,
                "start_lat_sum": 0.0,
                "end_lon_sum": 0.0,
                "end_lat_sum": 0.0,
            },
        )
        bucket["trip_count"] += 1
        bucket["distance_sum"] += trip.distance_km
        start_hour = _start_hour_for_trip(trip)
        if start_hour is not None:
            bucket["start_hour_sum"] += start_hour
            bucket["start_hour_count"] += 1
        bucket["start_lon_sum"] += trip.start_point[0]
        bucket["start_lat_sum"] += trip.start_point[1]
        bucket["end_lon_sum"] += trip.end_point[0]
        bucket["end_lat_sum"] += trip.end_point[1]

    items: list[RouteClusterItem] = []
    for cluster_key, bucket in buckets.items():
        trip_count = bucket["trip_count"]
        items.append(
            RouteClusterItem(
                cluster_key=cluster_key,
                trip_count=trip_count,
                avg_distance_km=round(bucket["distance_sum"] / max(trip_count, 1), 2),
                avg_start_hour=round(bucket["start_hour_sum"] / bucket["start_hour_count"], 2)
                if bucket["start_hour_count"] > 0
                else None,
                start_center=(
                    round(bucket["start_lon_sum"] / max(trip_count, 1), 6),
                    round(bucket["start_lat_sum"] / max(trip_count, 1), 6),
                ),
                end_center=(
                    round(bucket["end_lon_sum"] / max(trip_count, 1), 6),
                    round(bucket["end_lat_sum"] / max(trip_count, 1), 6),
                ),
            )
        )

    items.sort(key=lambda item: (-item.trip_count, -item.avg_distance_km, item.cluster_key))
    return items[:limit]


def _sum_bin_counts(active_time_bins: list[ActiveTimeBin], labels: set[str]) -> int:
    return sum(item.trip_count for item in active_time_bins if item.label in labels)


def determine_dominant_shift(active_time_bins: list[ActiveTimeBin], total_trips: int) -> str:
    if total_trips <= 0:
        return "mixed"

    shift_scores = {
        shift: _sum_bin_counts(active_time_bins, labels)
        for shift, labels in SHIFT_BUCKETS.items()
    }
    top_shift, top_count = max(shift_scores.items(), key=lambda item: item[1])
    if top_count / total_trips < 0.35:
        return "mixed"
    return top_shift


def classify_operation_mode(metrics: dict[str, Any]) -> str:
    total_trips = int(metrics.get("total_trips") or 0)
    if total_trips <= 0:
        return "steady_all_day"

    night_trip_ratio = float(metrics.get("night_trip_ratio") or 0.0)
    peak_trip_ratio = float(metrics.get("peak_trip_ratio") or 0.0)
    avg_trip_distance_km = float(metrics.get("avg_trip_distance_km") or 0.0)
    avg_daily_work_hours = float(metrics.get("avg_daily_work_hours") or 0.0)
    hotspot_concentration = float(metrics.get("hotspot_concentration") or 0.0)
    dominant_shift = str(metrics.get("dominant_shift") or "mixed")

    if night_trip_ratio >= 0.45 or (dominant_shift == "night" and night_trip_ratio >= 0.35):
        return "night_shift"
    if peak_trip_ratio >= 0.55 and avg_daily_work_hours <= 10.5:
        return "commuter_peak"
    if avg_trip_distance_km >= 13.0 and avg_daily_work_hours >= 6.0:
        return "long_haul"
    if avg_trip_distance_km <= 5.0 and hotspot_concentration >= 0.30:
        return "local_shuttle"
    return "steady_all_day"


def analyze_vehicle_operations(
    device_id: str,
    trips: list[VehicleTripSample],
) -> dict[str, Any]:
    if not trips:
        raise ValueError("trips must not be empty")

    trips = _sort_trip_samples(trips)
    total_trips = len(trips)
    total_distance_km = round(sum(trip.distance_km for trip in trips), 2)
    avg_trip_distance_km = round(total_distance_km / max(total_trips, 1), 2)

    duration_values = [trip.duration_seconds for trip in trips if trip.duration_seconds is not None]
    avg_trip_duration_minutes = round((sum(duration_values) / len(duration_values)) / 60.0, 2) if duration_values else 0.0

    active_days = len({trip.log_date for trip in trips})
    active_time_bins = build_active_time_bins(trips)
    region_radar, hotspot_concentration = build_region_radar(trips)
    daily_rhythm = build_daily_rhythm(trips)
    route_clusters = build_route_clusters(trips)

    work_spans = [item.work_span_hours for item in daily_rhythm if item.work_span_hours is not None]
    avg_daily_work_hours = round(sum(work_spans) / len(work_spans), 2) if work_spans else 0.0

    night_trip_ratio = round(_sum_bin_counts(active_time_bins, SHIFT_BUCKETS["night"]) / max(total_trips, 1), 4)
    peak_trip_ratio = round(
        (
            _sum_bin_counts(active_time_bins, SHIFT_BUCKETS["morning_peak"])
            + _sum_bin_counts(active_time_bins, SHIFT_BUCKETS["evening_peak"])
        )
        / max(total_trips, 1),
        4,
    )
    dominant_shift = determine_dominant_shift(active_time_bins, total_trips)
    operation_mode = classify_operation_mode(
        {
            "total_trips": total_trips,
            "night_trip_ratio": night_trip_ratio,
            "peak_trip_ratio": peak_trip_ratio,
            "avg_trip_distance_km": avg_trip_distance_km,
            "avg_daily_work_hours": avg_daily_work_hours,
            "hotspot_concentration": hotspot_concentration,
            "dominant_shift": dominant_shift,
        }
    )

    summary = CarPortraitSummary(
        device_id=device_id,
        total_trips=total_trips,
        total_distance_km=total_distance_km,
        avg_trip_distance_km=avg_trip_distance_km,
        avg_trip_duration_minutes=avg_trip_duration_minutes,
        active_days=active_days,
        avg_daily_work_hours=avg_daily_work_hours,
        dominant_shift=dominant_shift,
        operation_mode=operation_mode,
        night_trip_ratio=night_trip_ratio,
        hotspot_concentration=round(hotspot_concentration, 4),
    )
    return {
        "summary": summary,
        "active_time_bins": active_time_bins,
        "region_radar": region_radar,
        "daily_rhythm": daily_rhythm,
        "route_clusters": route_clusters,
    }


def build_car_portrait_payload(
    device_id: str,
    trips: list[VehicleTripSample],
    peer_groups: list[PeerGroupItem] | None = None,
) -> CarPortraitResponse | None:
    if not trips:
        return None

    analysis = analyze_vehicle_operations(device_id, trips)
    return CarPortraitResponse(
        summary=analysis["summary"],
        active_time_bins=analysis["active_time_bins"],
        region_radar=analysis["region_radar"],
        daily_rhythm=analysis["daily_rhythm"],
        route_clusters=analysis["route_clusters"],
        peer_groups=list(peer_groups or []),
    )


# async def fetch_car_trip_samples(db: AsyncSession, device_id: str) -> list[VehicleTripSample]:
#     q_ids = text(
#         """
#         SELECT trip_ids
#         FROM public.car
#         WHERE device_id = :device_id
#         LIMIT 1
#         """
#     )
#     ids_row = (await db.execute(q_ids, {"device_id": device_id})).mappings().first()
#     trip_ids = list((ids_row or {}).get("trip_ids") or [])
#
#     rows: list[Any] = []
#     if trip_ids:
#         q = text(
#             """
#             SELECT DISTINCT ON (trip_id)
#                    trip_id,
#                    log_date,
#                    devid,
#                    distance_km,
#                    duration,
#                    start_time,
#                    end_time,
#                    CASE WHEN array_length(lon, 1) >= 1 THEN lon[1] END AS start_lon,
#                    CASE WHEN array_length(lat, 1) >= 1 THEN lat[1] END AS start_lat,
#                    CASE WHEN array_length(lon, 1) >= 1 THEN lon[array_length(lon, 1)] END AS end_lon,
#                    CASE WHEN array_length(lat, 1) >= 1 THEN lat[array_length(lat, 1)] END AS end_lat
#             FROM public.trip_data
#             WHERE trip_id = ANY(:trip_ids)
#             ORDER BY trip_id, log_date DESC
#             """
#         )
#         rows = (await db.execute(q, {"trip_ids": trip_ids})).mappings().all()
#     else:
#         try:
#             devid_num = int(device_id)
#         except Exception:
#             return []
#
#         q = text(
#             """
#             SELECT DISTINCT ON (trip_id)
#                    trip_id,
#                    log_date,
#                    devid,
#                    distance_km,
#                    duration,
#                    start_time,
#                    end_time,
#                    CASE WHEN array_length(lon, 1) >= 1 THEN lon[1] END AS start_lon,
#                    CASE WHEN array_length(lat, 1) >= 1 THEN lat[1] END AS start_lat,
#                    CASE WHEN array_length(lon, 1) >= 1 THEN lon[array_length(lon, 1)] END AS end_lon,
#                    CASE WHEN array_length(lat, 1) >= 1 THEN lat[array_length(lat, 1)] END AS end_lat
#             FROM public.trip_data
#             WHERE devid = :devid
#             ORDER BY trip_id, log_date DESC
#             """
#         )
#         rows = (await db.execute(q, {"devid": devid_num})).mappings().all()
#
#     return _sort_trip_samples([_sample_from_row(row) for row in rows])
async def fetch_car_trip_samples(db: AsyncSession, device_id: str) -> list[VehicleTripSample]:
    """
    【修改说明】
    从 DW 层 dw_fact_trip 获取车辆行程数据，
    start_lon/start_lat/end_lon/end_lat 已预计算，
    无需再展开数组。
    """
    q = text("""
        SELECT trip_id, log_date, devid, distance_km, duration_seconds,
               start_time, end_time, start_lon, start_lat, end_lon, end_lat
        FROM dw_fact_trip
        WHERE devid = :devid
        ORDER BY start_time DESC
    """)
    try:
        devid_num = int(device_id)
    except Exception:
        return []

    rows = (await db.execute(q, {"devid": devid_num})).mappings().all()

    return _sort_trip_samples([_sample_from_dw_row(row) for row in rows])


def _sample_from_dw_row(row: Any) -> VehicleTripSample:
    """从 dw_fact_trip 行构建 VehicleTripSample（替代 _sample_from_row）"""
    start_point = None
    if row.get("start_lon") is not None and row.get("start_lat") is not None:
        start_point = (float(row["start_lon"]), float(row["start_lat"]))

    end_point = None
    if row.get("end_lon") is not None and row.get("end_lat") is not None:
        end_point = (float(row["end_lon"]), float(row["end_lat"]))

    return VehicleTripSample(
        trip_id=int(row["trip_id"]),
        device_id=str(row["devid"]),
        log_date=row["log_date"],
        distance_km=float(row.get("distance_km") or 0.0),
        duration_seconds=float(row.get("duration_seconds")) if row.get("duration_seconds") is not None else None,
        start_time=row.get("start_time"),
        end_time=row.get("end_time"),
        start_point=start_point,
        end_point=end_point,
    )


# async def _fetch_peer_trip_samples(
#     db: AsyncSession,
#     device_ids: list[str],
# ) -> dict[str, list[VehicleTripSample]]:
#     numeric_ids: list[int] = []
#     for device_id in device_ids:
#         try:
#             numeric_ids.append(int(device_id))
#         except Exception:
#             continue
#
#     if not numeric_ids:
#         return {}
#
#     q = text(
#         """
#         SELECT DISTINCT ON (trip_id)
#                trip_id,
#                log_date,
#                devid,
#                distance_km,
#                duration,
#                start_time,
#                end_time,
#                CASE WHEN array_length(lon, 1) >= 1 THEN lon[1] END AS start_lon,
#                CASE WHEN array_length(lat, 1) >= 1 THEN lat[1] END AS start_lat,
#                CASE WHEN array_length(lon, 1) >= 1 THEN lon[array_length(lon, 1)] END AS end_lon,
#                CASE WHEN array_length(lat, 1) >= 1 THEN lat[array_length(lat, 1)] END AS end_lat
#         FROM public.trip_data
#         WHERE devid = ANY(:devids)
#         ORDER BY trip_id, log_date DESC
#         """
#     )
#     rows = (await db.execute(q, {"devids": numeric_ids})).mappings().all()
#     grouped: dict[str, list[VehicleTripSample]] = defaultdict(list)
#     for row in rows:
#         sample = _sample_from_row(row)
#         grouped[sample.device_id].append(sample)
#
#     for device_id, items in grouped.items():
#         grouped[device_id] = _sort_trip_samples(items)
#     return grouped
async def _fetch_peer_trip_samples(
        db: AsyncSession,
        device_ids: list[str],
) -> dict[str, list[VehicleTripSample]]:
    """
    【修改说明】
    改为从 DW 层批量查询同行车辆行程数据。
    """
    numeric_ids: list[int] = []
    for device_id in device_ids:
        try:
            numeric_ids.append(int(device_id))
        except Exception:
            continue

    if not numeric_ids:
        return {}

    q = text("""
        SELECT trip_id, log_date, devid, distance_km, duration_seconds,
               start_time, end_time, start_lon, start_lat, end_lon, end_lat
        FROM dw_fact_trip
        WHERE devid = ANY(:devids)
        ORDER BY devid, start_time DESC
    """)
    rows = (await db.execute(q, {"devids": numeric_ids})).mappings().all()

    grouped: dict[str, list[VehicleTripSample]] = defaultdict(list)
    for row in rows:
        sample = _sample_from_dw_row(row)
        grouped[sample.device_id].append(sample)

    for device_id, items in grouped.items():
        grouped[device_id] = _sort_trip_samples(items)
    return grouped

# async def _fetch_top_peer_device_ids(
#     db: AsyncSession,
#     current_device_id: str,
#     limit: int = 120,
# ) -> list[str]:
#     q = text(
#         """
#         SELECT device_id
#         FROM public.car
#         WHERE trips_total > 0 AND device_id <> :device_id
#         ORDER BY trips_total DESC, total_distance DESC, device_id
#         LIMIT :limit
#         """
#     )
#     rows = (await db.execute(q, {"device_id": current_device_id, "limit": limit})).all()
#     return [str(row[0]) for row in rows]

async def _fetch_top_peer_device_ids(
    db: AsyncSession,
    current_device_id: str,
    limit: int = 120,
) -> list[str]:
    """
    【修改说明】
    改为从 DW 层 dw_dim_car 查询同行车辆，
    dw_dim_car 有预计算的 total_trips 和 total_distance_km。
    """
    q = text("""
        SELECT device_id
        FROM dw_dim_car
        WHERE total_trips > 0 AND device_id <> :device_id
        ORDER BY total_trips DESC, total_distance_km DESC, device_id
        LIMIT :limit
    """)
    rows = (await db.execute(q, {"device_id": current_device_id, "limit": limit})).all()
    return [str(row[0]) for row in rows]
async def fetch_car_portrait(
    db: AsyncSession,
    device_id: str,
    peer_limit: int = 120,
) -> CarPortraitResponse | None:
    trips = await fetch_car_trip_samples(db, device_id)
    if not trips:
        return None

    analysis = analyze_vehicle_operations(device_id, trips)

    peer_groups: list[PeerGroupItem] = [
        PeerGroupItem(
            device_id=device_id,
            operation_mode=analysis["summary"].operation_mode,
            avg_trip_distance_km=analysis["summary"].avg_trip_distance_km,
            avg_daily_work_hours=analysis["summary"].avg_daily_work_hours,
            total_trips=analysis["summary"].total_trips,
            is_current=True,
        )
    ]

    peer_ids = await _fetch_top_peer_device_ids(db, device_id, limit=peer_limit)
    peer_samples = await _fetch_peer_trip_samples(db, peer_ids)
    for peer_device_id in peer_ids:
        items = peer_samples.get(peer_device_id) or []
        if not items:
            continue
        peer_analysis = analyze_vehicle_operations(peer_device_id, items)
        peer_groups.append(
            PeerGroupItem(
                device_id=peer_device_id,
                operation_mode=peer_analysis["summary"].operation_mode,
                avg_trip_distance_km=peer_analysis["summary"].avg_trip_distance_km,
                avg_daily_work_hours=peer_analysis["summary"].avg_daily_work_hours,
                total_trips=peer_analysis["summary"].total_trips,
                is_current=False,
            )
        )

    return CarPortraitResponse(
        summary=analysis["summary"],
        active_time_bins=analysis["active_time_bins"],
        region_radar=analysis["region_radar"],
        daily_rhythm=analysis["daily_rhythm"],
        route_clusters=analysis["route_clusters"],
        peer_groups=peer_groups,
    )
