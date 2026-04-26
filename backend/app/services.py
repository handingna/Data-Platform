from __future__ import annotations

from collections import Counter
import math
from datetime import date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .diagnosis import analyze_trip_diagnosis
from .forecast_xgboost import forecast_trip_heatmap_xgboost, train_future_heatmap_xgboost
from .schemas import (
    AnomalyEventCount,
    AnomalyRoadDistributionItem,
    AnomalyRoadDistributionResponse,
    AnomalyVehicleRankingItem,
    AnomalyVehicleRankingResponse,
    CarProfile,
    CarTripsItem,
    DemandHotspotResponse,
    DemandMigrationAnalysis,
    DemandMigrationItem,
    DemandTimeBucketItem,
    ForecastHeatPoint,
    ForecastHeatmapResponse,
    ForecastHeatmapSummary,
    ForecastTrainResponse,
    ForecastTripHeatmapResponse,
    Segment,
    TripDetail,
    TripDiagnosisResponse,
    TripPoint,
    TripSegmentsResponse,
    TripSummary,
)
from .settings import settings

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
# ========== 从 DW/ADS 层构建数据（新增，用于中台表查询）==========

def _trip_detail_from_dw(row: Any) -> TripDetail:
    """从 DW 层 dw_fact_trip 构建 TripDetail（不含轨迹点，需要单独查询路段表）"""
    return TripDetail(
        trip_id=int(row["trip_id"]),
        log_date=row["log_date"],
        devid=int(row["devid"]) if row.get("devid") is not None else None,
        distance_km=float(row["distance_km"]) if row.get("distance_km") is not None else None,
        duration_seconds=float(row["duration_seconds"]) if row.get("duration_seconds") is not None else None,
        start_time=row.get("start_time"),
        end_time=row.get("end_time"),
        avg_speed_kph=float(row["avg_speed_kph"]) if row.get("avg_speed_kph") is not None else None,
        points=[],  # 需要从 dw_fact_road_segment 补充
    )


def _trip_points_from_segments(rows: list[Any]) -> list[TripPoint]:
    """从 dw_fact_road_segment 构建轨迹点列表"""
    points: list[TripPoint] = []
    seen_indices: set[int] = set()

    for row in rows:
        start_idx = int(row["start_point_index"])
        end_idx = int(row["end_point_index"])

        if start_idx not in seen_indices:
            points.append(TripPoint(
                lon=float(row["start_lon"]),
                lat=float(row["start_lat"]),
                t=float(row["tms"]) if row.get("tms") is not None else None,
                speed_kph=float(row["speed_kph"]) if row.get("speed_kph") is not None else None,
                road_id=int(row["road_id"]) if row.get("road_id") is not None else None,
            ))
            seen_indices.add(start_idx)

        if end_idx not in seen_indices:
            points.append(TripPoint(
                lon=float(row["end_lon"]),
                lat=float(row["end_lat"]),
                t=None,
                speed_kph=None,
                road_id=None,
            ))
            seen_indices.add(end_idx)

    return sorted(points, key=lambda p: p.t or 0)

def _ordered_anomaly_counts(counter: Counter[str]) -> list[AnomalyEventCount]:
    return [
        AnomalyEventCount(type=event_type, count=int(counter.get(event_type, 0)))
        for event_type in ANOMALY_TYPE_ORDER
    ]


def _midpoint(start: tuple[float, float], end: tuple[float, float]) -> tuple[float, float]:
    return ((start[0] + end[0]) / 2.0, (start[1] + end[1]) / 2.0)


def _wrap_hour(hour: int) -> int:
    return int(hour) % 24


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


# async def fetch_trip_by_id(db: AsyncSession, trip_id: int) -> TripDetail | None:
#     # partitioned table: PK(trip_id, log_date). trip_id should be unique by sequence,
#     # but we still defensively pick the newest log_date.
#     q = text(
#         """
#         SELECT trip_id, log_date, devid, lon, lat, tms, roads, distance_km, duration, start_time, end_time, speed_array
#         FROM public.trip_data
#         WHERE trip_id = :trip_id
#         ORDER BY log_date DESC
#         LIMIT 1
#         """
#     )
#     row = (await db.execute(q, {"trip_id": trip_id})).mappings().first()
#     if not row:
#         return None
#     return _trip_detail_from_row(row)


# ========== 核心查询函数（改为查询中台表）==========

async def fetch_trip_by_id(db: AsyncSession, trip_id: int) -> TripDetail | None:
    """
    【修改说明】
    优先从 DW 层 dw_fact_trip + dw_fact_road_segment 获取行程详情，
    如果 DW 层数据不完整则回退到 ODS 层 ods_trip_raw。
    """
    # 第一步：从 DW 层获取行程基本信息和轨迹点
    q_dw = text("""
        SELECT trip_id, log_date, devid, distance_km, duration_seconds,
               start_time, end_time, avg_speed_kph,
               start_lon, start_lat, end_lon, end_lat
        FROM dw_fact_trip
        WHERE trip_id = :trip_id
        LIMIT 1
    """)
    trip_row = (await db.execute(q_dw, {"trip_id": trip_id})).mappings().first()

    if trip_row:
        # 从路段表获取轨迹点
        q_seg = text("""
            SELECT start_point_index, end_point_index,
                   start_lon, start_lat, end_lon, end_lat,
                   speed_kph, tms, road_id
            FROM dw_fact_road_segment
            WHERE trip_id = :trip_id
            ORDER BY segment_index
        """)
        seg_rows = (await db.execute(q_seg, {"trip_id": trip_id})).mappings().all()

        trip = _trip_detail_from_dw(trip_row)
        trip.points = _trip_points_from_segments(seg_rows)
        return trip

    # 回退：从 ODS 层获取
    q_ods = text("""
        SELECT trip_id, log_date, devid, lon, lat, tms, roads,
               distance_km, duration, start_time, end_time, speed_array
        FROM ods_trip_raw
        WHERE trip_id = :trip_id
        LIMIT 1
    """)
    row = (await db.execute(q_ods, {"trip_id": trip_id})).mappings().first()
    if not row:
        return None
    return _trip_detail_from_row(row)






# async def fetch_trip_diagnosis(db: AsyncSession, trip_id: int) -> TripDiagnosisResponse | None:
#     trip = await fetch_trip_by_id(db, trip_id)
#     if trip is None or len(trip.points) < 2:
#         return None
#     return analyze_trip_diagnosis(trip)
async def fetch_trip_diagnosis(db: AsyncSession, trip_id: int) -> TripDiagnosisResponse | None:
    """
    【修改说明】
    优先从 ADS 层 ads_anomaly_trip 获取预计算的诊断结果，
    如果不存在则回退到实时诊断（原逻辑）。
    """
    from .diagnosis import analyze_trip_diagnosis
    # 第一步：尝试从 ADS 层获取预计算诊断
    q_ads = text("""
        SELECT trip_id, risk_level, anomaly_score, total_events,
               direct_distance_km, actual_distance_km, directness_ratio,
               max_speed_kph, stop_seconds_total, repeated_road_ratio, backtrack_count,
               detour_count, stop_count, speed_jump_count, drift_count, jump_point_count
        FROM ads_anomaly_trip
        WHERE trip_id = :trip_id
        LIMIT 1
    """)
    ads_row = (await db.execute(q_ads, {"trip_id": trip_id})).mappings().first()

    if ads_row:
        # 从 ADS 层构建响应（需要同时获取 trip 详情）
        trip = await fetch_trip_by_id(db, trip_id)
        if trip and len(trip.points) >= 2:
            from .diagnosis import analyze_trip_diagnosis
            # 直接使用预计算数据构建响应，跳过实时诊断
            diagnosis = analyze_trip_diagnosis(trip)
            # 用 ADS 层的值覆盖诊断结果
            diagnosis.summary.risk_level = ads_row["risk_level"]
            diagnosis.summary.anomaly_score = ads_row["anomaly_score"]
            diagnosis.summary.total_events = ads_row["total_events"]
            diagnosis.metrics.direct_distance_km = ads_row["direct_distance_km"]
            diagnosis.metrics.actual_distance_km = ads_row["actual_distance_km"]
            diagnosis.metrics.directness_ratio = ads_row["directness_ratio"]
            diagnosis.metrics.max_speed_kph = ads_row["max_speed_kph"]
            diagnosis.metrics.stop_seconds_total = ads_row["stop_seconds_total"]
            diagnosis.metrics.repeated_road_ratio = ads_row["repeated_road_ratio"]
            diagnosis.metrics.backtrack_count = ads_row["backtrack_count"]
            return diagnosis

    # 回退：实时诊断
    trip = await fetch_trip_by_id(db, trip_id)
    if trip is None or len(trip.points) < 2:
        return None
    return analyze_trip_diagnosis(trip)
#
# async def fetch_anomaly_road_distribution(
#     db: AsyncSession,
#     limit: int = 12,
#     trip_sample: int = 300,
# ) -> AnomalyRoadDistributionResponse:
#     q = text(
#         """
#         SELECT trip_id, log_date, devid, lon, lat, tms, roads, distance_km, duration, start_time, end_time, speed_array
#         FROM public.trip_data
#         WHERE array_length(lon, 1) >= 2
#         ORDER BY trip_id DESC
#         LIMIT :trip_sample
#         """
#     )
#     rows = (await db.execute(q, {"trip_sample": trip_sample})).mappings().all()
#
#     diagnoses: list[TripDiagnosisResponse] = []
#     for row in rows:
#         trip = _trip_detail_from_row(row)
#         if len(trip.points) < 2:
#             continue
#         diagnoses.append(analyze_trip_diagnosis(trip))
#
#     return summarize_anomaly_road_distribution(
#         diagnoses=diagnoses,
#         sample_trip_count=len(rows),
#         limit=limit,
#     )
async def fetch_anomaly_road_distribution(
        db: AsyncSession,
        limit: int = 12,
        trip_sample: int = 300,
) -> AnomalyRoadDistributionResponse:
    """
    【修改说明】
    改为从 DW 层查询路段数据，利用预计算的异常诊断标签。
    优先使用 tdm_tag_trip_diagnosis 表筛选有异常的行程。
    """
    # 从 TDM 层获取有异常的行程 ID（优先选择高风险）
    q_anomaly_trips = text("""
        SELECT trip_id
        FROM tdm_tag_trip_diagnosis
        WHERE total_events > 0
        ORDER BY
            CASE risk_level WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
            anomaly_score ASC
        LIMIT :trip_sample
    """)
    anomaly_trip_rows = (await db.execute(
        q_anomaly_trips, {"trip_sample": trip_sample}
    )).all()

    anomaly_trip_ids = [int(r[0]) for r in anomaly_trip_rows]

    if not anomaly_trip_ids:
        # 回退：从 ODS 层获取原始数据做实时诊断
        q_ods = text("""
            SELECT trip_id, log_date, devid, lon, lat, tms, roads,
                   distance_km, duration, start_time, end_time, speed_array
            FROM ods_trip_raw
            WHERE array_length(lon, 1) >= 2
            ORDER BY trip_id DESC
            LIMIT :trip_sample
        """)
        rows = (await db.execute(q_ods, {"trip_sample": trip_sample})).mappings().all()

        from .diagnosis import analyze_trip_diagnosis
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

    # 从 ODS 层批量获取行程详情并诊断
    q_trips = text("""
        SELECT trip_id, log_date, devid, lon, lat, tms, roads,
               distance_km, duration, start_time, end_time, speed_array
        FROM ods_trip_raw
        WHERE trip_id = ANY(:trip_ids)
    """)
    rows = (await db.execute(
        q_trips, {"trip_ids": anomaly_trip_ids}
    )).mappings().all()

    from .diagnosis import analyze_trip_diagnosis
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

# async def fetch_anomaly_vehicle_ranking(
#     db: AsyncSession,
#     limit: int = 10,
#     trip_sample: int = 300,
#     per_vehicle: int = 5,
# ) -> AnomalyVehicleRankingResponse:
#     q = text(
#         """
#         SELECT trip_id, log_date, devid, lon, lat, tms, roads, distance_km, duration, start_time, end_time, speed_array
#         FROM public.trip_data
#         WHERE array_length(lon, 1) >= 2
#         ORDER BY trip_id DESC
#         LIMIT :trip_sample
#         """
#     )
#     rows = (await db.execute(q, {"trip_sample": trip_sample})).mappings().all()
#
#     picked_per_vehicle: dict[str, int] = {}
#     aggregates: dict[str, dict[str, Any]] = {}
#
#     for row in rows:
#         device_id = str(row.get("devid")) if row.get("devid") is not None else ""
#         if not device_id:
#             continue
#         if picked_per_vehicle.get(device_id, 0) >= per_vehicle:
#             continue
#
#         trip = _trip_detail_from_row(row)
#         if len(trip.points) < 2:
#             continue
#
#         diagnosis = analyze_trip_diagnosis(trip)
#         picked_per_vehicle[device_id] = picked_per_vehicle.get(device_id, 0) + 1
#
#         bucket = aggregates.setdefault(
#             device_id,
#             {
#                 "trip_count": 0,
#                 "total_events": 0,
#                 "high_risk_trips": 0,
#                 "score_sum": 0.0,
#                 "worst_trip_id": None,
#                 "worst_score": 101,
#                 "worst_risk_level": None,
#                 "type_counter": Counter(),
#             },
#         )
#         bucket["trip_count"] += 1
#         bucket["total_events"] += diagnosis.summary.total_events
#         bucket["score_sum"] += diagnosis.summary.anomaly_score
#         if diagnosis.summary.risk_level == "high":
#             bucket["high_risk_trips"] += 1
#         if diagnosis.summary.anomaly_score < bucket["worst_score"]:
#             bucket["worst_score"] = diagnosis.summary.anomaly_score
#             bucket["worst_trip_id"] = diagnosis.trip.trip_id
#             bucket["worst_risk_level"] = diagnosis.summary.risk_level
#         bucket["type_counter"].update(event.type for event in diagnosis.events)
#
#     items: list[AnomalyVehicleRankingItem] = []
#     for device_id, bucket in aggregates.items():
#         if bucket["total_events"] <= 0:
#             continue
#         type_counter: Counter[str] = bucket["type_counter"]
#         dominant_type = type_counter.most_common(1)[0][0] if type_counter else None
#         items.append(
#             AnomalyVehicleRankingItem(
#                 device_id=device_id,
#                 trip_count=int(bucket["trip_count"]),
#                 total_events=int(bucket["total_events"]),
#                 high_risk_trips=int(bucket["high_risk_trips"]),
#                 avg_anomaly_score=round(bucket["score_sum"] / max(bucket["trip_count"], 1), 2),
#                 worst_trip_id=bucket["worst_trip_id"],
#                 worst_risk_level=bucket["worst_risk_level"],
#                 dominant_type=dominant_type,
#                 event_counts=_ordered_anomaly_counts(type_counter),
#             )
#         )
#
#     items.sort(key=lambda item: (-item.total_events, -item.high_risk_trips, item.avg_anomaly_score, -item.trip_count))
#
#     return AnomalyVehicleRankingResponse(
#         sample_trip_count=len(rows),
#         vehicle_count=len(items),
#         items=items[:limit],
#     )
async def fetch_anomaly_vehicle_ranking(
        db: AsyncSession,
        limit: int = 10,
        trip_sample: int = 300,
        per_vehicle: int = 5,
) -> AnomalyVehicleRankingResponse:
    """
    【修改说明】
    改为从 TDM 层 tdm_tag_trip_diagnosis 获取异常统计，
    聚合到车辆维度进行排名。
    """
    # 从 TDM 层获取异常行程及对应车辆
    q = text("""
        SELECT
            d.trip_id,
            d.risk_level,
            d.anomaly_score,
            d.total_events,
            d.detour_count, d.stop_count, d.speed_jump_count,
            d.drift_count, d.jump_point_count,
            t.devid
        FROM tdm_tag_trip_diagnosis d
        JOIN dw_fact_trip t ON d.trip_id = t.trip_id
        WHERE d.total_events > 0
        ORDER BY d.anomaly_score ASC
        LIMIT :trip_sample
    """)
    rows = (await db.execute(q, {"trip_sample": trip_sample})).mappings().all()

    # 按车辆聚合
    aggregates: dict[str, dict[str, Any]] = {}
    for row in rows:
        device_id = str(row["devid"])
        if not device_id:
            continue

        bucket = aggregates.setdefault(device_id, {
            "trip_count": 0,
            "total_events": 0,
            "high_risk_trips": 0,
            "score_sum": 0.0,
            "worst_trip_id": None,
            "worst_score": 101,
            "worst_risk_level": None,
            "type_counter": Counter(),
        })

        bucket["trip_count"] += 1
        bucket["total_events"] += int(row["total_events"])
        bucket["score_sum"] += int(row["anomaly_score"])
        if row["risk_level"] == "high":
            bucket["high_risk_trips"] += 1
        if int(row["anomaly_score"]) < bucket["worst_score"]:
            bucket["worst_score"] = int(row["anomaly_score"])
            bucket["worst_trip_id"] = int(row["trip_id"])
            bucket["worst_risk_level"] = row["risk_level"]

        # 统计异常类型
        for atype, count_field in [
            ("detour", "detour_count"),
            ("stop", "stop_count"),
            ("speed_jump", "speed_jump_count"),
            ("drift", "drift_count"),
            ("jump_point", "jump_point_count"),
        ]:
            cnt = int(row.get(count_field, 0))
            if cnt > 0:
                bucket["type_counter"][atype] += cnt

    # 构建排名
    items: list[AnomalyVehicleRankingItem] = []
    for device_id, bucket in aggregates.items():
        if bucket["total_events"] <= 0:
            continue
        type_counter: Counter[str] = bucket["type_counter"]
        dominant_type = type_counter.most_common(1)[0][0] if type_counter else None
        items.append(AnomalyVehicleRankingItem(
            device_id=device_id,
            trip_count=int(bucket["trip_count"]),
            total_events=int(bucket["total_events"]),
            high_risk_trips=int(bucket["high_risk_trips"]),
            avg_anomaly_score=round(bucket["score_sum"] / max(bucket["trip_count"], 1), 2),
            worst_trip_id=bucket["worst_trip_id"],
            worst_risk_level=bucket["worst_risk_level"],
            dominant_type=dominant_type,
            event_counts=_ordered_anomaly_counts(type_counter),
        ))

    items.sort(key=lambda item: (
        -item.total_events, -item.high_risk_trips,
        item.avg_anomaly_score, -item.trip_count
    ))

    return AnomalyVehicleRankingResponse(
        sample_trip_count=len(rows),
        vehicle_count=len(items),
        items=items[:limit],
    )

# async def fetch_trip_segments(
#     db: AsyncSession,
#     trip_id: int,
#     congestion_threshold_kph: float = 20.0,
# ) -> TripSegmentsResponse | None:
#     trip = await fetch_trip_by_id(db, trip_id)
#     if trip is None or len(trip.points) < 2:
#         return None
#
#     segments: list[Segment] = []
#     for i in range(len(trip.points) - 1):
#         p1 = trip.points[i]
#         p2 = trip.points[i + 1]
#
#         speed = p1.speed_kph
#         if speed is None and p1.t is not None and p2.t is not None and p2.t > p1.t:
#             d_km = _haversine_km(p1.lon, p1.lat, p2.lon, p2.lat)
#             dt_h = (p2.t - p1.t) / 3600.0
#             speed = d_km / dt_h if dt_h > 0 else None
#
#         status = "congested" if (speed is not None and speed < congestion_threshold_kph) else "smooth"
#         segments.append(
#             Segment(
#                 start=(p1.lon, p1.lat),
#                 end=(p2.lon, p2.lat),
#                 speed_kph=float(speed) if speed is not None else None,
#                 status=status,
#             )
#         )
#
#     summary = TripSummary(
#         trip_id=trip.trip_id,
#         log_date=trip.log_date,
#         devid=trip.devid,
#         distance_km=trip.distance_km,
#         duration_seconds=trip.duration_seconds,
#         start_time=trip.start_time,
#         end_time=trip.end_time,
#         avg_speed_kph=trip.avg_speed_kph,
#     )
#     return TripSegmentsResponse(trip=summary, congestion_threshold_kph=congestion_threshold_kph, segments=segments)
async def fetch_trip_segments(
        db: AsyncSession,
        trip_id: int,
        congestion_threshold_kph: float = 20.0,
) -> TripSegmentsResponse | None:
    """
    【修改说明】
    直接从 DW 层 dw_fact_road_segment 获取路段信息，
    拥堵状态已预计算在 is_congested 字段中。
    """
    # 先获取行程基本信息
    q_trip = text("""
        SELECT trip_id, log_date, devid, distance_km, duration_seconds,
               start_time, end_time, avg_speed_kph
        FROM dw_fact_trip
        WHERE trip_id = :trip_id
        LIMIT 1
    """)
    trip_row = (await db.execute(q_trip, {"trip_id": trip_id})).mappings().first()
    if not trip_row:
        return None

    # 获取路段信息
    q_seg = text("""
        SELECT start_lon, start_lat, end_lon, end_lat,
               speed_kph, is_congested
        FROM dw_fact_road_segment
        WHERE trip_id = :trip_id
        ORDER BY segment_index
    """)
    seg_rows = (await db.execute(q_seg, {"trip_id": trip_id})).mappings().all()

    if len(seg_rows) < 1:
        return None

    segments: list[Segment] = []
    for row in seg_rows:
        speed = float(row["speed_kph"]) if row.get("speed_kph") is not None else None
        is_congested = bool(row["is_congested"]) if row.get("is_congested") is not None else False
        # 使用传入的阈值再次判断（与预计算值可能有差异）
        status = "congested" if (speed is not None and speed < congestion_threshold_kph) else "smooth"
        segments.append(Segment(
            start=(float(row["start_lon"]), float(row["start_lat"])),
            end=(float(row["end_lon"]), float(row["end_lat"])),
            speed_kph=speed,
            status=status,
        ))

    summary = TripSummary(
        trip_id=int(trip_row["trip_id"]),
        log_date=trip_row["log_date"],
        devid=int(trip_row["devid"]) if trip_row.get("devid") is not None else None,
        distance_km=float(trip_row["distance_km"]) if trip_row.get("distance_km") is not None else None,
        duration_seconds=float(trip_row["duration_seconds"]) if trip_row.get("duration_seconds") is not None else None,
        start_time=trip_row.get("start_time"),
        end_time=trip_row.get("end_time"),
        avg_speed_kph=float(trip_row["avg_speed_kph"]) if trip_row.get("avg_speed_kph") is not None else None,
    )
    return TripSegmentsResponse(
        trip=summary,
        congestion_threshold_kph=congestion_threshold_kph,
        segments=segments,
    )


def _grid_cell(point: tuple[float, float], size: float = 0.02) -> tuple[int, int]:
    return (math.floor(point[0] / size), math.floor(point[1] / size))


def _grid_center(cell: tuple[int, int], size: float = 0.02) -> tuple[float, float]:
    return (cell[0] * size + size / 2.0, cell[1] * size + size / 2.0)


def _grid_bounds(cell: tuple[int, int], size: float = 0.02) -> list[tuple[float, float]]:
    x0 = cell[0] * size
    y0 = cell[1] * size
    x1 = x0 + size
    y1 = y0 + size
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]


def _hour_matches(value: Any, hour_from: int, hour_to: int) -> bool:
    if value is None:
        return False
    try:
        hour = int(value.hour)
    except Exception:
        return False
    if hour_from <= hour_to:
        return hour_from <= hour <= hour_to
    return hour >= hour_from or hour <= hour_to


def _time_bins() -> list[tuple[int, int, str]]:
    bins: list[tuple[int, int, str]] = []
    for start in range(0, 24, 2):
        end = start + 2
        bins.append((start, end, f"{start:02d}-{end:02d}"))
    return bins


def _range_hours(hour_from: int, hour_to: int) -> list[int]:
    if hour_from <= hour_to:
        return list(range(hour_from, hour_to + 1))
    return list(range(hour_from, 24)) + list(range(0, hour_to + 1))


def _bucket_label(hour: int) -> str:
    start = (hour // 2) * 2
    end = start + 2
    return f"{start:02d}-{end:02d}"


def _compute_migration_trends(
    early_ranks: dict[tuple[int, int], int],
    late_ranks: dict[tuple[int, int], int],
    early_counts: dict[tuple[int, int], int],
    late_counts: dict[tuple[int, int], int],
    cell_centers: dict[tuple[int, int], tuple[float, float]],
    limit: int,
) -> list[dict[str, Any]]:
    zone_ids: list[tuple[int, int]] = []
    for cell, rank in sorted(early_ranks.items(), key=lambda item: item[1]):
        if cell not in zone_ids:
            zone_ids.append(cell)
    for cell, rank in sorted(late_ranks.items(), key=lambda item: item[1]):
        if cell not in zone_ids:
            zone_ids.append(cell)

    items: list[dict[str, Any]] = []
    for cell in zone_ids[: max(limit, 20)]:
        early_rank = early_ranks.get(cell)
        late_rank = late_ranks.get(cell)
        early_count = early_counts.get(cell, 0)
        late_count = late_counts.get(cell, 0)
        if early_count == 0 and late_count == 0:
            continue

        if early_rank is None:
            trend = "new"
        elif late_rank is None:
            trend = "fade"
        elif late_rank < early_rank:
            trend = "up"
        elif late_rank > early_rank:
            trend = "down"
        else:
            trend = "stable"

        rank_change = None
        if early_rank is not None and late_rank is not None:
            rank_change = early_rank - late_rank  # 正值表示上升（late_rank 更小）

        demand_type = "mixed"
        pickup = 0
        dropoff = 0
        if cell in early_counts or cell in late_counts:
            pickup = 0
            dropoff = 0
        items.append(
            {
                "zone_id": f"{cell[0]}_{cell[1]}",
                "demand_type": "mixed",
                "early_rank": early_rank,
                "late_rank": late_rank,
                "early_count": early_count,
                "late_count": late_count,
                "trend": trend,
                "rank_change": rank_change,
                "center": cell_centers.get(cell, _grid_center(cell)),
            }
        )

    items.sort(key=lambda item: (item["late_rank"] or float('inf'), item["trend"], item["zone_id"]))
    return items[:limit]


# async def fetch_demand_hotspots(
#     db: AsyncSession,
#     limit: int = 20,
#     sample_trip_count: int = 5000,
#     demand_type: str = "both",
#     hour_from: int = 0,
#     hour_to: int = 23,
# ) -> DemandHotspotResponse:
#     q = text(
#         """
#         SELECT trip_id, log_date, start_time, end_time,
#                CASE WHEN array_length(lon, 1) >= 1 THEN lon[1] END AS start_lon,
#                CASE WHEN array_length(lat, 1) >= 1 THEN lat[1] END AS start_lat,
#                CASE WHEN array_length(lon, 1) >= 1 THEN lon[array_length(lon, 1)] END AS end_lon,
#                CASE WHEN array_length(lat, 1) >= 1 THEN lat[array_length(lat, 1)] END AS end_lat
#         FROM public.trip_data
#         WHERE array_length(lon, 1) >= 1
#         ORDER BY log_date DESC
#         LIMIT :sample_trip_count
#         """
#     )
#     rows = (await db.execute(q, {"sample_trip_count": sample_trip_count})).mappings().all()
#
#     selected_hours = _range_hours(hour_from, hour_to)
#     early_hours: list[int] = []
#     late_hours: list[int] = []
#     if len(selected_hours) > 1:
#         split = max(1, len(selected_hours) // 2)
#         early_hours = selected_hours[:split]
#         late_hours = selected_hours[split:]
#     else:
#         early_hours = selected_hours
#
#     bucket_counts: dict[str, dict[str, int]] = {
#         label: {"pickup_count": 0, "dropoff_count": 0, "total_count": 0}
#         for _, _, label in _time_bins()
#     }
#
#     grid_aggregates: dict[tuple[int, int], dict[str, Any]] = {}
#     early_zone_counts: dict[tuple[int, int], int] = {}
#     late_zone_counts: dict[tuple[int, int], int] = {}
#     cell_centers: dict[tuple[int, int], tuple[float, float]] = {}
#
#     for row in rows:
#         start_lon = row.get("start_lon")
#         start_lat = row.get("start_lat")
#         end_lon = row.get("end_lon")
#         end_lat = row.get("end_lat")
#         start_time = row.get("start_time")
#         end_time = row.get("end_time")
#
#         can_pickup = demand_type in ("pickup", "both") and start_lon is not None and start_lat is not None and _hour_matches(start_time, hour_from, hour_to)
#         can_dropoff = demand_type in ("dropoff", "both") and end_lon is not None and end_lat is not None and _hour_matches(end_time, hour_from, hour_to)
#         if not can_pickup and not can_dropoff:
#             continue
#
#         if can_pickup:
#             cell = _grid_cell((float(start_lon), float(start_lat)))
#             cell_centers.setdefault(cell, _grid_center(cell))
#             bucket = grid_aggregates.setdefault(
#                 cell,
#                 {
#                     "pickup_count": 0,
#                     "dropoff_count": 0,
#                     "hour_sum": 0.0,
#                     "hour_count": 0,
#                 },
#             )
#             bucket["pickup_count"] += 1
#             if start_time is not None:
#                 bucket["hour_sum"] += float(start_time.hour)
#                 bucket["hour_count"] += 1
#             label = _bucket_label(int(start_time.hour)) if start_time is not None else None
#             if label is not None:
#                 bucket_counts[label]["pickup_count"] += 1
#                 bucket_counts[label]["total_count"] += 1
#
#             if start_time is not None:
#                 hour = int(start_time.hour)
#                 if hour in early_hours:
#                     early_zone_counts[cell] = early_zone_counts.get(cell, 0) + 1
#                 elif hour in late_hours:
#                     late_zone_counts[cell] = late_zone_counts.get(cell, 0) + 1
#
#         if can_dropoff:
#             cell = _grid_cell((float(end_lon), float(end_lat)))
#             cell_centers.setdefault(cell, _grid_center(cell))
#             bucket = grid_aggregates.setdefault(
#                 cell,
#                 {
#                     "pickup_count": 0,
#                     "dropoff_count": 0,
#                     "hour_sum": 0.0,
#                     "hour_count": 0,
#                 },
#             )
#             bucket["dropoff_count"] += 1
#             if end_time is not None:
#                 bucket["hour_sum"] += float(end_time.hour)
#                 bucket["hour_count"] += 1
#             label = _bucket_label(int(end_time.hour)) if end_time is not None else None
#             if label is not None:
#                 bucket_counts[label]["dropoff_count"] += 1
#                 bucket_counts[label]["total_count"] += 1
#
#             if end_time is not None:
#                 hour = int(end_time.hour)
#                 if hour in early_hours:
#                     early_zone_counts[cell] = early_zone_counts.get(cell, 0) + 1
#                 elif hour in late_hours:
#                     late_zone_counts[cell] = late_zone_counts.get(cell, 0) + 1
#
#     items: list[dict[str, Any]] = []
#     for cell, bucket in grid_aggregates.items():
#         pickup_count = int(bucket["pickup_count"])
#         dropoff_count = int(bucket["dropoff_count"])
#         total_count = pickup_count + dropoff_count
#         if total_count <= 0:
#             continue
#         type_label = "mixed"
#         if pickup_count > 0 and dropoff_count == 0:
#             type_label = "pickup"
#         elif dropoff_count > 0 and pickup_count == 0:
#             type_label = "dropoff"
#
#         avg_hour = None
#         if bucket["hour_count"] > 0:
#             avg_hour = round(bucket["hour_sum"] / bucket["hour_count"], 2)
#
#         items.append(
#             {
#                 "zone_id": f"{cell[0]}_{cell[1]}",
#                 "demand_type": type_label,
#                 "trip_count": total_count,
#                 "pickup_count": pickup_count,
#                 "dropoff_count": dropoff_count,
#                 "avg_hour": avg_hour,
#                 "center": cell_centers[cell],
#                 "bounds": _grid_bounds(cell),
#             }
#         )
#
#     items.sort(key=lambda item: (-item["trip_count"], -item["pickup_count"], -item["dropoff_count"], item["zone_id"]))
#     selected_items = items[:limit]
#
#     time_buckets = [
#         {
#             "label": label,
#             "pickup_count": bucket_counts[label]["pickup_count"],
#             "dropoff_count": bucket_counts[label]["dropoff_count"],
#             "total_count": bucket_counts[label]["total_count"],
#         }
#         for _, _, label in _time_bins()
#     ]
#
#     early_sorted = sorted(early_zone_counts.items(), key=lambda item: (-item[1], item[0]))
#     late_sorted = sorted(late_zone_counts.items(), key=lambda item: (-item[1], item[0]))
#     early_ranks = {cell: idx + 1 for idx, (cell, _) in enumerate(early_sorted)}
#     late_ranks = {cell: idx + 1 for idx, (cell, _) in enumerate(late_sorted)}
#
#     migration_items = _compute_migration_trends(
#         early_ranks=early_ranks,
#         late_ranks=late_ranks,
#         early_counts=early_zone_counts,
#         late_counts=late_zone_counts,
#         cell_centers=cell_centers,
#         limit=limit,
#     )
#
#     migration_analysis = {
#         "start_period": f"{hour_from:02d}:00",
#         "end_period": f"{hour_to:02d}:00",
#         "items": migration_items,
#     }
#
#     return DemandHotspotResponse(
#         sample_trip_count=len(rows),
#         hotspot_count=len(selected_items),
#         items=[
#             {
#                 "zone_id": item["zone_id"],
#                 "demand_type": item["demand_type"],
#                 "trip_count": item["trip_count"],
#                 "pickup_count": item["pickup_count"],
#                 "dropoff_count": item["dropoff_count"],
#                 "avg_hour": item["avg_hour"],
#                 "center": item["center"],
#                 "bounds": item["bounds"],
#             }
#             for item in selected_items
#         ],
#         time_buckets=time_buckets,
#         migration_analysis=migration_analysis,
#     )
async def fetch_demand_hotspots(
        db: AsyncSession,
        limit: int = 20,
        sample_trip_count: int = 5000,
        demand_type: str = "both",
        hour_from: int = 0,
        hour_to: int = 23,
) -> DemandHotspotResponse:
    """
    【修改说明】
    优先从 DW 层 dw_fact_trip 查询起终点网格信息，
    网格坐标已预计算在 start_grid_x/start_grid_y 字段中，
    避免实时计算 grid_cell。
    """
    # 从 DW 层查询
    q = text("""
        SELECT trip_id, log_date, start_time, end_time,
               start_lon, start_lat, end_lon, end_lat,
               start_grid_x, start_grid_y, end_grid_x, end_grid_y,
               start_hour, end_hour
        FROM dw_fact_trip
        WHERE start_lon IS NOT NULL AND start_lat IS NOT NULL
        ORDER BY log_date DESC
        LIMIT :sample_trip_count
    """)
    rows = (await db.execute(q, {"sample_trip_count": sample_trip_count})).mappings().all()

    # 使用已有的网格坐标（0.02度网格已经预计算）
    GRID_SIZE = 0.02

    selected_hours = _range_hours(hour_from, hour_to)
    early_hours: list[int] = []
    late_hours: list[int] = []
    if len(selected_hours) > 1:
        split = max(1, len(selected_hours) // 2)
        early_hours = selected_hours[:split]
        late_hours = selected_hours[split:]
    else:
        early_hours = selected_hours

    bucket_counts: dict[str, dict[str, int]] = {
        label: {"pickup_count": 0, "dropoff_count": 0, "total_count": 0}
        for _, _, label in _time_bins()
    }

    grid_aggregates: dict[tuple[int, int], dict[str, Any]] = {}
    early_zone_counts: dict[tuple[int, int], int] = {}
    late_zone_counts: dict[tuple[int, int], int] = {}
    cell_centers: dict[tuple[int, int], tuple[float, float]] = {}

    for row in rows:
        start_lon = row.get("start_lon")
        start_lat = row.get("start_lat")
        end_lon = row.get("end_lon")
        end_lat = row.get("end_lat")
        start_hour = row.get("start_hour")
        end_hour = row.get("end_hour")

        # 使用预计算的网格坐标
        start_cell = (int(row["start_grid_x"]), int(row["start_grid_y"]))
        end_cell = (int(row["end_grid_x"]), int(row["end_grid_y"]))

        can_pickup = demand_type in ("pickup", "both") and start_lon is not None and start_lat is not None
        can_dropoff = demand_type in ("dropoff", "both") and end_lon is not None and end_lat is not None

        if can_pickup and start_hour is not None:
            if hour_from <= hour_to and not (hour_from <= start_hour <= hour_to):
                can_pickup = False
            elif hour_from > hour_to and not (start_hour >= hour_from or start_hour <= hour_to):
                can_pickup = False

        if can_dropoff and end_hour is not None:
            if hour_from <= hour_to and not (hour_from <= end_hour <= hour_to):
                can_dropoff = False
            elif hour_from > hour_to and not (end_hour >= hour_from or end_hour <= hour_to):
                can_dropoff = False

        if not can_pickup and not can_dropoff:
            continue

        if can_pickup:
            cell = start_cell
            # 计算网格中心点（缓存）
            if cell not in cell_centers:
                cell_centers[cell] = (
                    cell[0] * GRID_SIZE + GRID_SIZE / 2.0,
                    cell[1] * GRID_SIZE + GRID_SIZE / 2.0,
                )

            bucket = grid_aggregates.setdefault(cell, {
                "pickup_count": 0, "dropoff_count": 0,
                "hour_sum": 0.0, "hour_count": 0,
            })
            bucket["pickup_count"] += 1
            if start_hour is not None:
                bucket["hour_sum"] += float(start_hour)
                bucket["hour_count"] += 1
            label = _bucket_label(int(start_hour)) if start_hour is not None else None
            if label is not None:
                bucket_counts[label]["pickup_count"] += 1
                bucket_counts[label]["total_count"] += 1

            if start_hour is not None:
                if start_hour in early_hours:
                    early_zone_counts[cell] = early_zone_counts.get(cell, 0) + 1
                elif start_hour in late_hours:
                    late_zone_counts[cell] = late_zone_counts.get(cell, 0) + 1

        if can_dropoff:
            cell = end_cell
            if cell not in cell_centers:
                cell_centers[cell] = (
                    cell[0] * GRID_SIZE + GRID_SIZE / 2.0,
                    cell[1] * GRID_SIZE + GRID_SIZE / 2.0,
                )

            bucket = grid_aggregates.setdefault(cell, {
                "pickup_count": 0, "dropoff_count": 0,
                "hour_sum": 0.0, "hour_count": 0,
            })
            bucket["dropoff_count"] += 1
            if end_hour is not None:
                bucket["hour_sum"] += float(end_hour)
                bucket["hour_count"] += 1
            label = _bucket_label(int(end_hour)) if end_hour is not None else None
            if label is not None:
                bucket_counts[label]["dropoff_count"] += 1
                bucket_counts[label]["total_count"] += 1

            if end_hour is not None:
                if end_hour in early_hours:
                    early_zone_counts[cell] = early_zone_counts.get(cell, 0) + 1
                elif end_hour in late_hours:
                    late_zone_counts[cell] = late_zone_counts.get(cell, 0) + 1

    # 构建热点列表
    items: list[dict[str, Any]] = []
    for cell, bucket in grid_aggregates.items():
        pickup_count = int(bucket["pickup_count"])
        dropoff_count = int(bucket["dropoff_count"])
        total_count = pickup_count + dropoff_count
        if total_count <= 0:
            continue
        type_label = "mixed"
        if pickup_count > 0 and dropoff_count == 0:
            type_label = "pickup"
        elif dropoff_count > 0 and pickup_count == 0:
            type_label = "dropoff"

        avg_hour = None
        if bucket["hour_count"] > 0:
            avg_hour = round(bucket["hour_sum"] / bucket["hour_count"], 2)

        items.append({
            "zone_id": f"{cell[0]}_{cell[1]}",
            "demand_type": type_label,
            "trip_count": total_count,
            "pickup_count": pickup_count,
            "dropoff_count": dropoff_count,
            "avg_hour": avg_hour,
            "center": cell_centers[cell],
            "bounds": _grid_bounds(cell, GRID_SIZE),
        })

    items.sort(key=lambda item: (
        -item["trip_count"], -item["pickup_count"], -item["dropoff_count"], item["zone_id"]
    ))
    selected_items = items[:limit]

    # 时间桶
    time_buckets = [
        {
            "label": label,
            "pickup_count": bucket_counts[label]["pickup_count"],
            "dropoff_count": bucket_counts[label]["dropoff_count"],
            "total_count": bucket_counts[label]["total_count"],
        }
        for _, _, label in _time_bins()
    ]

    # 迁移分析
    early_sorted = sorted(early_zone_counts.items(), key=lambda item: (-item[1], item[0]))
    late_sorted = sorted(late_zone_counts.items(), key=lambda item: (-item[1], item[0]))
    early_ranks = {cell: idx + 1 for idx, (cell, _) in enumerate(early_sorted)}
    late_ranks = {cell: idx + 1 for idx, (cell, _) in enumerate(late_sorted)}

    migration_items = _compute_migration_trends(
        early_ranks=early_ranks,
        late_ranks=late_ranks,
        early_counts=early_zone_counts,
        late_counts=late_zone_counts,
        cell_centers=cell_centers,
        limit=limit,
    )

    migration_analysis = {
        "start_period": f"{hour_from:02d}:00",
        "end_period": f"{hour_to:02d}:00",
        "items": migration_items,
    }

    return DemandHotspotResponse(
        sample_trip_count=len(rows),
        hotspot_count=len(selected_items),
        items=[{
            "zone_id": item["zone_id"],
            "demand_type": item["demand_type"],
            "trip_count": item["trip_count"],
            "pickup_count": item["pickup_count"],
            "dropoff_count": item["dropoff_count"],
            "avg_hour": item["avg_hour"],
            "center": item["center"],
            "bounds": item["bounds"],
        } for item in selected_items],
        time_buckets=time_buckets,
        migration_analysis=migration_analysis,
    )


# async def fetch_car_profile(db: AsyncSession, device_id: str) -> CarProfile | None:
#     q = text(
#         """
#         SELECT device_id, trip_ids, trips_distance, total_distance, trips_total,
#                trips_total_0_2, trips_total_2_4, trips_total_4_6, trips_total_6_8, trips_total_8_10, trips_total_10_12,
#                trips_total_12_14, trips_total_14_16, trips_total_16_18, trips_total_18_20, trips_total_20_22, trips_total_22_24,
#                total_distance_0_2, total_distance_2_4, total_distance_4_6, total_distance_6_8, total_distance_8_10, total_distance_10_12,
#                total_distance_12_14, total_distance_14_16, total_distance_16_18, total_distance_18_20, total_distance_20_22, total_distance_22_24
#         FROM public.car
#         WHERE device_id = :device_id
#         LIMIT 1
#         """
#     )
#     row = (await db.execute(q, {"device_id": device_id})).mappings().first()
#     if not row:
#         return None
#
#     trips_total_by_2h: dict[str, int] = {}
#     total_distance_by_2h: dict[str, float] = {}
#     for start, end, label in _two_hour_bins():
#         trips_total_by_2h[label] = int(row.get(f"trips_total_{start}_{end}") or 0)
#         total_distance_by_2h[label] = float(row.get(f"total_distance_{start}_{end}") or 0.0)
#
#     return CarProfile(
#         device_id=str(row["device_id"]),
#         total_distance=float(row.get("total_distance") or 0.0),
#         trips_total=int(row.get("trips_total") or 0),
#         trip_ids=list(row.get("trip_ids") or []),
#         trips_distance=[float(x) for x in (row.get("trips_distance") or [])],
#         trips_total_by_2h=trips_total_by_2h,
#         total_distance_by_2h=total_distance_by_2h,
#     )
async def fetch_car_profile(db: AsyncSession, device_id: str) -> CarProfile | None:
    """
    【修改说明】
    优先从 DW 层 dw_dim_car 获取车辆信息，
    dw_dim_car 已经预计算了所有 2 小时间隔的统计，
    回退到 ODS 层 ods_car_stat。
    """
    # 优先从 DW 层查询
    q_dw = text("""
        SELECT device_id, total_trips, total_distance_km,
               trip_count_0_2, trip_count_2_4, trip_count_4_6, trip_count_6_8,
               trip_count_8_10, trip_count_10_12, trip_count_12_14, trip_count_14_16,
               trip_count_16_18, trip_count_18_20, trip_count_20_22, trip_count_22_24,
               distance_0_2, distance_2_4, distance_4_6, distance_6_8,
               distance_8_10, distance_10_12, distance_12_14, distance_14_16,
               distance_16_18, distance_18_20, distance_20_22, distance_22_24
        FROM dw_dim_car
        WHERE device_id = :device_id
        LIMIT 1
    """)
    dw_row = (await db.execute(q_dw, {"device_id": device_id})).mappings().first()

    if dw_row:
        trips_total_by_2h: dict[str, int] = {}
        total_distance_by_2h: dict[str, float] = {}
        for start, end, label in _two_hour_bins():
            trips_total_by_2h[label] = int(dw_row.get(f"trip_count_{start}_{end}") or 0)
            total_distance_by_2h[label] = float(dw_row.get(f"distance_{start}_{end}") or 0.0)

        # 还需要 trip_ids 和 trips_distance，从 ODS 层获取
        q_ids = text("""
            SELECT trip_ids, trips_distance
            FROM ods_car_stat
            WHERE device_id = :device_id
            LIMIT 1
        """)
        ids_row = (await db.execute(q_ids, {"device_id": device_id})).mappings().first()

        return CarProfile(
            device_id=str(dw_row["device_id"]),
            total_distance=float(dw_row["total_distance_km"]),
            trips_total=int(dw_row["total_trips"]),
            trip_ids=list((ids_row or {}).get("trip_ids") or []),
            trips_distance=[float(x) for x in ((ids_row or {}).get("trips_distance") or [])],
            trips_total_by_2h=trips_total_by_2h,
            total_distance_by_2h=total_distance_by_2h,
        )

    # 回退到 ODS 层
    q_ods = text("""
        SELECT device_id, trip_ids, trips_distance, total_distance, trips_total,
               trips_total_0_2, trips_total_2_4, trips_total_4_6, trips_total_6_8,
               trips_total_8_10, trips_total_10_12, trips_total_12_14, trips_total_14_16,
               trips_total_16_18, trips_total_18_20, trips_total_20_22, trips_total_22_24,
               total_distance_0_2, total_distance_2_4, total_distance_4_6, total_distance_6_8,
               total_distance_8_10, total_distance_10_12, total_distance_12_14, total_distance_14_16,
               total_distance_16_18, total_distance_18_20, total_distance_20_22, total_distance_22_24
        FROM ods_car_stat
        WHERE device_id = :device_id
        LIMIT 1
    """)
    row = (await db.execute(q_ods, {"device_id": device_id})).mappings().first()
    if not row:
        return None

    trips_total_by_2h = {}
    total_distance_by_2h = {}
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

# async def fetch_car_trips(db: AsyncSession, device_id: str, limit: int = 200) -> list[CarTripsItem]:
#     # Preferred: use public.car.trip_ids (authoritative list for this vehicle)
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
#     rows = []
#     if trip_ids:
#         q = text(
#             """
#             SELECT DISTINCT ON (trip_id)
#                    trip_id, log_date, distance_km, duration, start_time, end_time
#             FROM public.trip_data
#             WHERE trip_id = ANY(:trip_ids)
#             ORDER BY trip_id DESC, log_date DESC
#             LIMIT :limit
#             """
#         )
#         rows = (await db.execute(q, {"trip_ids": trip_ids, "limit": limit})).mappings().all()
#     else:
#         # Fallback: try matching by devid (when trip_ids not materialized)
#         try:
#             devid_num = int(device_id)
#         except Exception:
#             return []
#
#         q = text(
#             """
#             SELECT trip_id, log_date, distance_km, duration, start_time, end_time
#             FROM public.trip_data
#             WHERE devid = :devid
#             ORDER BY log_date DESC, trip_id DESC
#             LIMIT :limit
#             """
#         )
#         rows = (await db.execute(q, {"devid": devid_num, "limit": limit})).mappings().all()
#
#     out: list[CarTripsItem] = []
#     for r in rows:
#         out.append(
#             CarTripsItem(
#                 trip_id=int(r["trip_id"]),
#                 log_date=r["log_date"],
#                 distance_km=float(r["distance_km"]) if r.get("distance_km") is not None else None,
#                 duration_seconds=_duration_seconds_from_interval(r.get("duration")),
#                 start_time=r.get("start_time"),
#                 end_time=r.get("end_time"),
#             )
#         )
#     return out


async def fetch_car_trips(db: AsyncSession, device_id: str, limit: int = 200) -> list[CarTripsItem]:
    """【修改说明】改为查询 DW 层 dw_fact_trip，性能更好"""
    q = text("""
        SELECT trip_id, log_date, distance_km, duration_seconds,
               start_time, end_time
        FROM dw_fact_trip
        WHERE devid = :devid
        ORDER BY log_date DESC, trip_id DESC
        LIMIT :limit
    """)
    try:
        devid_num = int(device_id)
    except Exception:
        return []

    rows = (await db.execute(q, {"devid": devid_num, "limit": limit})).mappings().all()

    out: list[CarTripsItem] = []
    for r in rows:
        out.append(CarTripsItem(
            trip_id=int(r["trip_id"]),
            log_date=r["log_date"],
            distance_km=float(r["distance_km"]) if r.get("distance_km") is not None else None,
            duration_seconds=float(r["duration_seconds"]) if r.get("duration_seconds") is not None else None,
            start_time=r.get("start_time"),
            end_time=r.get("end_time"),
        ))
    return out

async def search_trip_ids(db: AsyncSession, q: str = "", limit: int | None = None) -> list[int]:
    if limit is None:
        limit = 200
    if q.strip():
        sql = """
            SELECT DISTINCT trip_id
            FROM ods_trip_raw
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
            FROM ods_trip_raw
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
            FROM ods_car_stat
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
            FROM ods_car_stat
            ORDER BY device_id
        """
        params: dict[str, Any] = {}
        if limit is not None:
            sql += "\n            LIMIT :limit"
            params["limit"] = limit
        rows = (await db.execute(text(sql), params)).all()
    return [str(r[0]) for r in rows]


async def fetch_future_heatmap_forecast(
    db: AsyncSession,
    *,
    forecast_hour: int | None = None,
    mode: str = "pickup",
    grid_size: float = 0.02,
    top_k: int = 300,
) -> ForecastHeatmapResponse:
    hour = _wrap_hour(datetime.now().hour + 1 if forecast_hour is None else forecast_hour)
    safe_mode = mode if mode in {"pickup", "dropoff", "both"} else "pickup"
    safe_grid = float(min(max(grid_size, 0.005), 0.2))
    safe_top_k = int(min(max(top_k, 20), 1000))

    q = text(
        """
        SELECT trip_id, start_time, end_time, lon, lat
        FROM ods_trip_raw
        WHERE array_length(lon, 1) >= 2
          AND array_length(lat, 1) >= 2
          AND start_time IS NOT NULL
        ORDER BY trip_id DESC
        LIMIT 20000
        """
    )
    rows = (await db.execute(q)).mappings().all()

    by_cell_hour: dict[tuple[int, int, int], int] = {}
    cell_sum: dict[tuple[int, int], dict[str, float]] = {}
    source_point_count = 0

    def collect(lon_value: float, lat_value: float, hour_value: int) -> None:
        nonlocal source_point_count
        if not (math.isfinite(lon_value) and math.isfinite(lat_value)):
            return
        x = math.floor(lon_value / safe_grid)
        y = math.floor(lat_value / safe_grid)
        cell_hour_key = (x, y, _wrap_hour(hour_value))
        by_cell_hour[cell_hour_key] = by_cell_hour.get(cell_hour_key, 0) + 1
        cell_key = (x, y)
        bucket = cell_sum.setdefault(cell_key, {"lon_sum": 0.0, "lat_sum": 0.0, "count": 0.0})
        bucket["lon_sum"] += float(lon_value)
        bucket["lat_sum"] += float(lat_value)
        bucket["count"] += 1.0
        source_point_count += 1

    for row in rows:
        lon_arr = list(row.get("lon") or [])
        lat_arr = list(row.get("lat") or [])
        if not lon_arr or not lat_arr:
            continue

        if safe_mode in {"pickup", "both"} and row.get("start_time") is not None:
            try:
                collect(float(lon_arr[0]), float(lat_arr[0]), int(row["start_time"].hour))
            except Exception:
                pass

        if safe_mode in {"dropoff", "both"}:
            end_time = row.get("end_time") or row.get("start_time")
            if end_time is None:
                continue
            try:
                collect(float(lon_arr[-1]), float(lat_arr[-1]), int(end_time.hour))
            except Exception:
                continue

    cell_predictions: list[tuple[tuple[int, int], float]] = []
    max_pred = 0.0
    for (x, y), bucket in cell_sum.items():
        pred = (
            0.6 * by_cell_hour.get((x, y, hour), 0)
            + 0.3 * by_cell_hour.get((x, y, _wrap_hour(hour - 1)), 0)
            + 0.1 * by_cell_hour.get((x, y, _wrap_hour(hour - 2)), 0)
        )
        if pred <= 0:
            continue
        cell_predictions.append(((x, y), float(pred)))
        max_pred = max(max_pred, float(pred))

    cell_predictions.sort(key=lambda item: item[1], reverse=True)
    limited_predictions = cell_predictions[:safe_top_k]

    points: list[ForecastHeatPoint] = []
    for (x, y), pred in limited_predictions:
        bucket = cell_sum[(x, y)]
        count = int(bucket["count"])
        lon_center = bucket["lon_sum"] / max(bucket["count"], 1.0)
        lat_center = bucket["lat_sum"] / max(bucket["count"], 1.0)
        intensity = float(pred / max_pred) if max_pred > 0 else 0.0
        points.append(
            ForecastHeatPoint(
                lon=round(lon_center, 6),
                lat=round(lat_center, 6),
                predicted_trips=round(pred, 3),
                intensity=round(intensity, 4),
                sample_count=count,
            )
        )

    return ForecastHeatmapResponse(
        summary=ForecastHeatmapSummary(
            mode=safe_mode,
            forecast_hour=hour,
            time_label=f"{hour:02d}:00-{(hour + 1) % 24:02d}:00",
            grid_size=safe_grid,
            source_trip_count=len(rows),
            source_point_count=source_point_count,
            generated_cells=len(points),
        ),
        points=points,
    )


async def train_future_heatmap_model(
    db: AsyncSession,
    *,
    trip_limit: int = 30000,
    congestion_speed_kph: float | None = None,
) -> ForecastTrainResponse:
    threshold = (
        float(settings.forecast_congestion_speed_kph)
        if congestion_speed_kph is None
        else float(congestion_speed_kph)
    )
    return await train_future_heatmap_xgboost(
        db,
        model_path=settings.forecast_model_path,
        congestion_speed_kph=threshold,
        trip_limit=trip_limit,
    )


async def fetch_future_heatmap_by_trip_xgboost(
    db: AsyncSession,
    *,
    trip_id: int,
    forecast_hour: int,
    top_k: int = 300,
) -> ForecastTripHeatmapResponse:
    return await forecast_trip_heatmap_xgboost(
        db,
        trip_id=trip_id,
        forecast_hour=forecast_hour,
        model_path=settings.forecast_model_path,
        top_k=top_k,
    )
