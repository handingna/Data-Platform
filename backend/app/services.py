from __future__ import annotations

from collections import Counter
import math
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .diagnosis import analyze_trip_diagnosis
from .schemas import (
    AnomalyEventCount,
    AnomalyRoadDistributionItem,
    AnomalyRoadDistributionResponse,
    AnomalyVehicleRankingItem,
    AnomalyVehicleRankingResponse,
    CarProfile,
    CarTripsItem,
    Segment,
    TripDetail,
    TripDiagnosisResponse,
    TripPoint,
    TripSegmentsResponse,
    TripSummary,
)

ANOMALY_TYPE_ORDER = ["detour", "stop", "speed_jump", "drift", "jump_point"]
SEVERITY_WEIGHT = {"low": 1, "medium": 2, "high": 3}


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lam = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lam / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _duration_seconds_from_interval(val: Any) -> float | None:
    # asyncpg may return datetime.timedelta for INTERVAL
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
        label = f"{start:02d}-{end:02d}"
        bins.append((start, end, label))
    return bins


def _trip_detail_from_row(row: Any) -> TripDetail:
    lon_arr = list(row.get("lon") or [])
    lat_arr = list(row.get("lat") or [])
    tms_arr = list(row.get("tms") or [])
    speed_arr = list(row.get("speed_array") or [])
    road_arr = list(row.get("roads") or [])

    n = min(len(lon_arr), len(lat_arr))
    points: list[TripPoint] = []
    for i in range(n):
        t = float(tms_arr[i]) if i < len(tms_arr) and tms_arr[i] is not None else None
        sp = float(speed_arr[i]) if i < len(speed_arr) and speed_arr[i] is not None else None
        road_id = int(road_arr[i]) if i < len(road_arr) and road_arr[i] is not None else None
        points.append(TripPoint(lon=float(lon_arr[i]), lat=float(lat_arr[i]), t=t, speed_kph=sp, road_id=road_id))

    duration_s = _duration_seconds_from_interval(row.get("duration"))
    distance_km = float(row["distance_km"]) if row.get("distance_km") is not None else None

    avg_speed = None
    if distance_km is not None and duration_s and duration_s > 0:
        avg_speed = float(distance_km / (duration_s / 3600))

    return TripDetail(
        trip_id=int(row["trip_id"]),
        log_date=row["log_date"],
        devid=int(row["devid"]) if row.get("devid") is not None else None,
        distance_km=distance_km,
        duration_seconds=duration_s,
        start_time=row.get("start_time"),
        end_time=row.get("end_time"),
        avg_speed_kph=avg_speed,
        points=points,
    )


def _ordered_anomaly_counts(counter: Counter[str]) -> list[AnomalyEventCount]:
    return [
        AnomalyEventCount(type=event_type, count=int(counter.get(event_type, 0)))
        for event_type in ANOMALY_TYPE_ORDER
    ]


def _midpoint(start: tuple[float, float], end: tuple[float, float]) -> tuple[float, float]:
    return ((start[0] + end[0]) / 2.0, (start[1] + end[1]) / 2.0)


def _merge_diagnosis_road_occurrences(diagnosis: TripDiagnosisResponse) -> list[dict[str, Any]]:
    occurrences: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def flush() -> None:
        nonlocal current
        if current is None:
            return
        current["avg_speed_kph"] = (
            round(current["speed_sum"] / current["speed_count"], 2)
            if current["speed_count"] > 0
            else None
        )
        current["center_point"] = _midpoint(current["start_point"], current["end_point"])
        occurrences.append(current)
        current = None

    for segment in diagnosis.segments:
        if segment.type == "normal" or segment.road_id is None:
            flush()
            continue

        if (
            current is not None
            and current["road_id"] == segment.road_id
            and current["type"] == segment.type
            and current["end_index"] == segment.start_index
        ):
            current["end_point"] = tuple(segment.end)
            current["end_index"] = segment.end_index
            current["segment_count"] += 1
            if segment.speed_kph is not None:
                current["speed_sum"] += float(segment.speed_kph)
                current["speed_count"] += 1
            if SEVERITY_WEIGHT[segment.severity] > SEVERITY_WEIGHT[current["max_severity"]]:
                current["max_severity"] = segment.severity
            continue

        flush()
        current = {
            "road_id": int(segment.road_id),
            "trip_id": diagnosis.trip.trip_id,
            "type": segment.type,
            "max_severity": segment.severity,
            "start_point": tuple(segment.start),
            "end_point": tuple(segment.end),
            "start_index": segment.start_index,
            "end_index": segment.end_index,
            "segment_count": 1,
            "speed_sum": float(segment.speed_kph) if segment.speed_kph is not None else 0.0,
            "speed_count": 1 if segment.speed_kph is not None else 0,
        }

    flush()
    return occurrences


def summarize_anomaly_road_distribution(
    diagnoses: list[TripDiagnosisResponse],
    sample_trip_count: int,
    limit: int = 12,
) -> AnomalyRoadDistributionResponse:
    aggregates: dict[int, dict[str, Any]] = {}

    for diagnosis in diagnoses:
        for occurrence in _merge_diagnosis_road_occurrences(diagnosis):
            road_id = int(occurrence["road_id"])
            bucket = aggregates.setdefault(
                road_id,
                {
                    "occurrence_count": 0,
                    "trip_ids": set(),
                    "type_counter": Counter(),
                    "max_severity": "low",
                    "speed_sum": 0.0,
                    "speed_count": 0,
                    "representative": occurrence,
                },
            )
            bucket["occurrence_count"] += 1
            bucket["trip_ids"].add(int(occurrence["trip_id"]))
            bucket["type_counter"].update([occurrence["type"]])
            if occurrence["avg_speed_kph"] is not None:
                bucket["speed_sum"] += float(occurrence["avg_speed_kph"])
                bucket["speed_count"] += 1

            severity = occurrence["max_severity"]
            if SEVERITY_WEIGHT[severity] > SEVERITY_WEIGHT[bucket["max_severity"]]:
                bucket["max_severity"] = severity

            representative = bucket["representative"]
            replace_representative = (
                SEVERITY_WEIGHT[severity] > SEVERITY_WEIGHT[representative["max_severity"]]
                or (
                    SEVERITY_WEIGHT[severity] == SEVERITY_WEIGHT[representative["max_severity"]]
                    and occurrence["segment_count"] > representative["segment_count"]
                )
            )
            if replace_representative:
                bucket["representative"] = occurrence

    items: list[AnomalyRoadDistributionItem] = []
    for road_id, bucket in aggregates.items():
        representative = bucket["representative"]
        type_counter: Counter[str] = bucket["type_counter"]
        dominant_type = type_counter.most_common(1)[0][0] if type_counter else None
        avg_speed = (
            round(bucket["speed_sum"] / bucket["speed_count"], 2)
            if bucket["speed_count"] > 0
            else representative["avg_speed_kph"]
        )
        items.append(
            AnomalyRoadDistributionItem(
                road_id=road_id,
                occurrence_count=int(bucket["occurrence_count"]),
                trip_count=len(bucket["trip_ids"]),
                dominant_type=dominant_type,
                max_severity=bucket["max_severity"],
                avg_speed_kph=avg_speed,
                sample_trip_id=int(representative["trip_id"]),
                start_point=tuple(representative["start_point"]),
                end_point=tuple(representative["end_point"]),
                center_point=tuple(representative["center_point"]),
                event_counts=_ordered_anomaly_counts(type_counter),
            )
        )

    items.sort(
        key=lambda item: (
            -item.occurrence_count,
            -SEVERITY_WEIGHT.get(item.max_severity or "low", 0),
            -item.trip_count,
            item.road_id,
        )
    )

    return AnomalyRoadDistributionResponse(
        sample_trip_count=sample_trip_count,
        road_count=len(items),
        items=items[:limit],
    )


async def fetch_trip_by_id(db: AsyncSession, trip_id: int) -> TripDetail | None:
    # partitioned table: PK(trip_id, log_date). trip_id should be unique by sequence,
    # but we still defensively pick the newest log_date.
    q = text(
        """
        SELECT trip_id, log_date, devid, lon, lat, tms, roads, distance_km, duration, start_time, end_time, speed_array
        FROM public.trip_data
        WHERE trip_id = :trip_id
        ORDER BY log_date DESC
        LIMIT 1
        """
    )
    row = (await db.execute(q, {"trip_id": trip_id})).mappings().first()
    if not row:
        return None
    return _trip_detail_from_row(row)


async def fetch_trip_diagnosis(db: AsyncSession, trip_id: int) -> TripDiagnosisResponse | None:
    trip = await fetch_trip_by_id(db, trip_id)
    if trip is None or len(trip.points) < 2:
        return None
    return analyze_trip_diagnosis(trip)


async def fetch_anomaly_road_distribution(
    db: AsyncSession,
    limit: int = 12,
    trip_sample: int = 300,
) -> AnomalyRoadDistributionResponse:
    q = text(
        """
        SELECT trip_id, log_date, devid, lon, lat, tms, roads, distance_km, duration, start_time, end_time, speed_array
        FROM public.trip_data
        WHERE array_length(lon, 1) >= 2
        ORDER BY trip_id DESC
        LIMIT :trip_sample
        """
    )
    rows = (await db.execute(q, {"trip_sample": trip_sample})).mappings().all()

    diagnoses: list[TripDiagnosisResponse] = []
    for row in rows:
        trip = _trip_detail_from_row(row)
        if len(trip.points) < 2:
            continue
        diagnoses.append(analyze_trip_diagnosis(trip))

    return summarize_anomaly_road_distribution(
        diagnoses=diagnoses,
        sample_trip_count=len(rows),
        limit=limit,
    )


async def fetch_anomaly_vehicle_ranking(
    db: AsyncSession,
    limit: int = 10,
    trip_sample: int = 300,
    per_vehicle: int = 5,
) -> AnomalyVehicleRankingResponse:
    q = text(
        """
        SELECT trip_id, log_date, devid, lon, lat, tms, roads, distance_km, duration, start_time, end_time, speed_array
        FROM public.trip_data
        WHERE array_length(lon, 1) >= 2
        ORDER BY trip_id DESC
        LIMIT :trip_sample
        """
    )
    rows = (await db.execute(q, {"trip_sample": trip_sample})).mappings().all()

    picked_per_vehicle: dict[str, int] = {}
    aggregates: dict[str, dict[str, Any]] = {}

    for row in rows:
        device_id = str(row.get("devid")) if row.get("devid") is not None else ""
        if not device_id:
            continue
        if picked_per_vehicle.get(device_id, 0) >= per_vehicle:
            continue

        trip = _trip_detail_from_row(row)
        if len(trip.points) < 2:
            continue

        diagnosis = analyze_trip_diagnosis(trip)
        picked_per_vehicle[device_id] = picked_per_vehicle.get(device_id, 0) + 1

        bucket = aggregates.setdefault(
            device_id,
            {
                "trip_count": 0,
                "total_events": 0,
                "high_risk_trips": 0,
                "score_sum": 0.0,
                "worst_trip_id": None,
                "worst_score": 101,
                "worst_risk_level": None,
                "type_counter": Counter(),
            },
        )
        bucket["trip_count"] += 1
        bucket["total_events"] += diagnosis.summary.total_events
        bucket["score_sum"] += diagnosis.summary.anomaly_score
        if diagnosis.summary.risk_level == "high":
            bucket["high_risk_trips"] += 1
        if diagnosis.summary.anomaly_score < bucket["worst_score"]:
            bucket["worst_score"] = diagnosis.summary.anomaly_score
            bucket["worst_trip_id"] = diagnosis.trip.trip_id
            bucket["worst_risk_level"] = diagnosis.summary.risk_level
        bucket["type_counter"].update(event.type for event in diagnosis.events)

    items: list[AnomalyVehicleRankingItem] = []
    for device_id, bucket in aggregates.items():
        if bucket["total_events"] <= 0:
            continue
        type_counter: Counter[str] = bucket["type_counter"]
        dominant_type = type_counter.most_common(1)[0][0] if type_counter else None
        items.append(
            AnomalyVehicleRankingItem(
                device_id=device_id,
                trip_count=int(bucket["trip_count"]),
                total_events=int(bucket["total_events"]),
                high_risk_trips=int(bucket["high_risk_trips"]),
                avg_anomaly_score=round(bucket["score_sum"] / max(bucket["trip_count"], 1), 2),
                worst_trip_id=bucket["worst_trip_id"],
                worst_risk_level=bucket["worst_risk_level"],
                dominant_type=dominant_type,
                event_counts=_ordered_anomaly_counts(type_counter),
            )
        )

    items.sort(key=lambda item: (-item.total_events, -item.high_risk_trips, item.avg_anomaly_score, -item.trip_count))

    return AnomalyVehicleRankingResponse(
        sample_trip_count=len(rows),
        vehicle_count=len(items),
        items=items[:limit],
    )


async def fetch_trip_segments(
    db: AsyncSession,
    trip_id: int,
    congestion_threshold_kph: float = 20.0,
) -> TripSegmentsResponse | None:
    trip = await fetch_trip_by_id(db, trip_id)
    if trip is None or len(trip.points) < 2:
        return None

    segments: list[Segment] = []
    for i in range(len(trip.points) - 1):
        p1 = trip.points[i]
        p2 = trip.points[i + 1]

        speed = p1.speed_kph
        if speed is None and p1.t is not None and p2.t is not None and p2.t > p1.t:
            d_km = _haversine_km(p1.lon, p1.lat, p2.lon, p2.lat)
            dt_h = (p2.t - p1.t) / 3600.0
            speed = d_km / dt_h if dt_h > 0 else None

        status = "congested" if (speed is not None and speed < congestion_threshold_kph) else "smooth"
        segments.append(
            Segment(
                start=(p1.lon, p1.lat),
                end=(p2.lon, p2.lat),
                speed_kph=float(speed) if speed is not None else None,
                status=status,
            )
        )

    summary = TripSummary(
        trip_id=trip.trip_id,
        log_date=trip.log_date,
        devid=trip.devid,
        distance_km=trip.distance_km,
        duration_seconds=trip.duration_seconds,
        start_time=trip.start_time,
        end_time=trip.end_time,
        avg_speed_kph=trip.avg_speed_kph,
    )
    return TripSegmentsResponse(trip=summary, congestion_threshold_kph=congestion_threshold_kph, segments=segments)


async def fetch_car_profile(db: AsyncSession, device_id: str) -> CarProfile | None:
    q = text(
        """
        SELECT device_id, trip_ids, trips_distance, total_distance, trips_total,
               trips_total_0_2, trips_total_2_4, trips_total_4_6, trips_total_6_8, trips_total_8_10, trips_total_10_12,
               trips_total_12_14, trips_total_14_16, trips_total_16_18, trips_total_18_20, trips_total_20_22, trips_total_22_24,
               total_distance_0_2, total_distance_2_4, total_distance_4_6, total_distance_6_8, total_distance_8_10, total_distance_10_12,
               total_distance_12_14, total_distance_14_16, total_distance_16_18, total_distance_18_20, total_distance_20_22, total_distance_22_24
        FROM public.car
        WHERE device_id = :device_id
        LIMIT 1
        """
    )
    row = (await db.execute(q, {"device_id": device_id})).mappings().first()
    if not row:
        return None

    trips_total_by_2h: dict[str, int] = {}
    total_distance_by_2h: dict[str, float] = {}
    for start, end, label in _two_hour_bins():
        trips_total_by_2h[label] = int(row.get(f"trips_total_{start}_{end}") or 0)
        total_distance_by_2h[label] = float(row.get(f"total_distance_{start}_{end}") or 0.0)

    return CarProfile(
        device_id=str(row["device_id"]),
        total_distance=float(row.get("total_distance") or 0.0),
        trips_total=int(row.get("trips_total") or 0),
        trip_ids=list(row.get("trip_ids") or []),
        trips_distance=[float(x) for x in (row.get("trips_distance") or [])],
        trips_total_by_2h=trips_total_by_2h,
        total_distance_by_2h=total_distance_by_2h,
    )


async def fetch_car_trips(db: AsyncSession, device_id: str, limit: int = 200) -> list[CarTripsItem]:
    # Preferred: use public.car.trip_ids (authoritative list for this vehicle)
    q_ids = text(
        """
        SELECT trip_ids
        FROM public.car
        WHERE device_id = :device_id
        LIMIT 1
        """
    )
    ids_row = (await db.execute(q_ids, {"device_id": device_id})).mappings().first()
    trip_ids = list((ids_row or {}).get("trip_ids") or [])

    rows = []
    if trip_ids:
        q = text(
            """
            SELECT DISTINCT ON (trip_id)
                   trip_id, log_date, distance_km, duration, start_time, end_time
            FROM public.trip_data
            WHERE trip_id = ANY(:trip_ids)
            ORDER BY trip_id DESC, log_date DESC
            LIMIT :limit
            """
        )
        rows = (await db.execute(q, {"trip_ids": trip_ids, "limit": limit})).mappings().all()
    else:
        # Fallback: try matching by devid (when trip_ids not materialized)
        try:
            devid_num = int(device_id)
        except Exception:
            return []

        q = text(
            """
            SELECT trip_id, log_date, distance_km, duration, start_time, end_time
            FROM public.trip_data
            WHERE devid = :devid
            ORDER BY log_date DESC, trip_id DESC
            LIMIT :limit
            """
        )
        rows = (await db.execute(q, {"devid": devid_num, "limit": limit})).mappings().all()

    out: list[CarTripsItem] = []
    for r in rows:
        out.append(
            CarTripsItem(
                trip_id=int(r["trip_id"]),
                log_date=r["log_date"],
                distance_km=float(r["distance_km"]) if r.get("distance_km") is not None else None,
                duration_seconds=_duration_seconds_from_interval(r.get("duration")),
                start_time=r.get("start_time"),
                end_time=r.get("end_time"),
            )
        )
    return out


async def search_trip_ids(db: AsyncSession, q: str = "", limit: int | None = None) -> list[int]:
    if limit is None:
        limit = 200
    if q.strip():
        sql = """
            SELECT DISTINCT trip_id
            FROM public.trip_data
            WHERE CAST(trip_id AS TEXT) ILIKE :kw
            ORDER BY trip_id DESC
        """
        params: dict[str, Any] = {"kw": f"%{q.strip()}%"}
        if limit is not None:
            sql += "\n            LIMIT :limit"
            params["limit"] = limit
        rows = (await db.execute(text(sql), params)).all()
    else:
        sql = """
            SELECT DISTINCT trip_id
            FROM public.trip_data
            ORDER BY trip_id DESC
        """
        params: dict[str, Any] = {}
        if limit is not None:
            sql += "\n            LIMIT :limit"
            params["limit"] = limit
        rows = (await db.execute(text(sql), params)).all()
    return [int(r[0]) for r in rows]


async def search_device_ids(db: AsyncSession, q: str = "", limit: int | None = None) -> list[str]:
    if limit is None:
        limit = 200
    if q.strip():
        sql = """
            SELECT device_id
            FROM public.car
            WHERE device_id ILIKE :kw
            ORDER BY device_id
        """
        params: dict[str, Any] = {"kw": f"%{q.strip()}%"}
        if limit is not None:
            sql += "\n            LIMIT :limit"
            params["limit"] = limit
        rows = (await db.execute(text(sql), params)).all()
    else:
        sql = """
            SELECT device_id
            FROM public.car
            ORDER BY device_id
        """
        params: dict[str, Any] = {}
        if limit is not None:
            sql += "\n            LIMIT :limit"
            params["limit"] = limit
        rows = (await db.execute(text(sql), params)).all()
    return [str(r[0]) for r in rows]
