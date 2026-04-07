from __future__ import annotations

import math
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .schemas import CarProfile, CarTripsItem, Segment, TripDetail, TripSegmentsResponse, TripSummary, TripPoint


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


async def fetch_trip_by_id(db: AsyncSession, trip_id: int) -> TripDetail | None:
    # partitioned table: PK(trip_id, log_date). trip_id should be unique by sequence,
    # but we still defensively pick the newest log_date.
    q = text(
        """
        SELECT trip_id, log_date, devid, lon, lat, tms, distance_km, duration, start_time, end_time, speed_array
        FROM public.trip_data
        WHERE trip_id = :trip_id
        ORDER BY log_date DESC
        LIMIT 1
        """
    )
    row = (await db.execute(q, {"trip_id": trip_id})).mappings().first()
    if not row:
        return None

    lon_arr = list(row.get("lon") or [])
    lat_arr = list(row.get("lat") or [])
    tms_arr = list(row.get("tms") or [])
    speed_arr = list(row.get("speed_array") or [])

    n = min(len(lon_arr), len(lat_arr))
    points: list[TripPoint] = []
    for i in range(n):
        t = float(tms_arr[i]) if i < len(tms_arr) and tms_arr[i] is not None else None
        sp = float(speed_arr[i]) if i < len(speed_arr) and speed_arr[i] is not None else None
        points.append(TripPoint(lon=float(lon_arr[i]), lat=float(lat_arr[i]), t=t, speed_kph=sp))

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

