from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
import math

import joblib
import numpy as np
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from xgboost import XGBRegressor

from .schemas import (
    ForecastHeatPoint,
    ForecastSpeedPoint,
    ForecastSpeedSummary,
    ForecastSpeedWindow,
    ForecastTrainResponse,
    ForecastTrainSummary,
    ForecastTripHeatmapResponse,
    ForecastTripHeatmapSummary,
    ForecastTripSpeedResponse,
)

FEATURE_ORDER = [
    "road_id",
    "hour",
    "hour_sin",
    "hour_cos",
    "lag1",
    "lag2",
    "lag3",
    "road_total_count",
    "hour_global_mean",
]

def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _hour_label(hour: int) -> str:
    safe = int(hour) % 24
    return f"{safe:02d}:00-{(safe + 1) % 24:02d}:00"


def _hour_cyclic(hour: int) -> tuple[float, float]:
    rad = 2.0 * math.pi * (int(hour) % 24) / 24.0
    return math.sin(rad), math.cos(rad)


def _hour_congestion_prior(hour: int) -> float:
    h = int(hour) % 24
    if 7 <= h <= 9:
        return 1.0  # morning peak
    if 17 <= h <= 19:
        return 1.35  # evening peak
    if 11 <= h <= 13:
        return 1.12  # noon lunch-traffic bump
    if 0 <= h <= 5:
        return 0.02  # deep night off-peak
    if h == 6 or h == 22 or h == 23:
        return 0.82
    return 0.96


def _temporal_adjust_intensity(
    raw_intensity: float,
    *,
    hour: int,
    hour_global_mean: dict[int, float],
    global_mean: float,
) -> float:
    # Blend domain prior (rush-hour/off-peak) with historical hour profile.
    prior_factor = _hour_congestion_prior(hour)
    base = max(float(global_mean), 1e-6)
    data_factor = float(hour_global_mean.get(int(hour) % 24, global_mean)) / base
    mixed_factor = 0.8 + 0.6 * (prior_factor - 1.0) + 0.4 * (data_factor - 1.0)
    return _clamp01(float(raw_intensity) * mixed_factor)


def _safe_hour_from_ts(ts: Any, fallback_hour: int) -> int:
    try:
        if ts is None:
            return fallback_hour
        value = int(float(ts))
        return int(datetime.utcfromtimestamp(value).hour)
    except Exception:
        return fallback_hour


def _risk_level_from_intensity(intensity: float) -> str:
    val = _clamp01(intensity)
    if val >= 0.65:
        return "high"
    if val >= 0.35:
        return "medium"
    return "low"


def _speed_from_intensity(intensity: float, *, min_kph: float = 5.0, max_kph: float = 48.0) -> float:
    val = _clamp01(intensity)
    # Non-linear projection: heavy congestion drops speed quickly.
    projected = min_kph + (max_kph - min_kph) * math.pow(1.0 - val, 1.25)
    return float(max(min_kph, min(max_kph, projected)))


def _prepare_training_matrices(
    road_hour_mean: dict[int, dict[int, float]],
    road_total_count: dict[int, int],
    hour_global_mean: dict[int, float],
    global_mean: float,
) -> tuple[np.ndarray, np.ndarray]:
    rows_x: list[list[float]] = []
    rows_y: list[float] = []

    for road_id, hour_map in road_hour_mean.items():
        for hour, target in hour_map.items():
            lag1 = hour_map.get((hour - 1) % 24, hour_global_mean.get((hour - 1) % 24, global_mean))
            lag2 = hour_map.get((hour - 2) % 24, hour_global_mean.get((hour - 2) % 24, global_mean))
            lag3 = hour_map.get((hour - 3) % 24, hour_global_mean.get((hour - 3) % 24, global_mean))
            hour_sin, hour_cos = _hour_cyclic(hour)
            rows_x.append(
                [
                    float(road_id),
                    float(hour),
                    float(hour_sin),
                    float(hour_cos),
                    float(lag1),
                    float(lag2),
                    float(lag3),
                    float(road_total_count.get(road_id, 0)),
                    float(hour_global_mean.get(hour, global_mean)),
                ]
            )
            rows_y.append(float(target))

    if not rows_x:
        return np.zeros((0, len(FEATURE_ORDER))), np.zeros((0,))

    return np.asarray(rows_x, dtype=np.float32), np.asarray(rows_y, dtype=np.float32)


def _build_feature_vector(
    *,
    road_id: int,
    target_hour: int,
    road_hour_mean: dict[int, dict[int, float]],
    road_total_count: dict[int, int],
    hour_global_mean: dict[int, float],
    global_mean: float,
) -> list[float]:
    hour = int(target_hour) % 24
    hour_map = road_hour_mean.get(road_id, {})
    lag1 = hour_map.get((hour - 1) % 24, hour_global_mean.get((hour - 1) % 24, global_mean))
    lag2 = hour_map.get((hour - 2) % 24, hour_global_mean.get((hour - 2) % 24, global_mean))
    lag3 = hour_map.get((hour - 3) % 24, hour_global_mean.get((hour - 3) % 24, global_mean))
    hour_sin, hour_cos = _hour_cyclic(hour)
    return [
        float(road_id),
        float(hour),
        float(hour_sin),
        float(hour_cos),
        float(lag1),
        float(lag2),
        float(lag3),
        float(road_total_count.get(road_id, 0)),
        float(hour_global_mean.get(hour, global_mean)),
    ]


def _artifact_to_runtime(artifact: dict[str, Any]) -> tuple[XGBRegressor, dict[int, dict[int, float]], dict[int, int], dict[int, float], float]:
    model = artifact["model"]
    road_hour_mean = {
        int(road_id): {int(h): float(v) for h, v in hour_map.items()}
        for road_id, hour_map in (artifact.get("road_hour_mean") or {}).items()
    }
    road_total_count = {int(k): int(v) for k, v in (artifact.get("road_total_count") or {}).items()}
    hour_global_mean = {int(k): float(v) for k, v in (artifact.get("hour_global_mean") or {}).items()}
    global_mean = float(artifact.get("global_mean") or 0.5)
    return model, road_hour_mean, road_total_count, hour_global_mean, global_mean


async def train_future_heatmap_xgboost(
    db: AsyncSession,
    *,
    model_path: str,
    congestion_speed_kph: float = 20.0,
    trip_limit: int = 30000,
) -> ForecastTrainResponse:
    # q = text(
    #     """
    #     SELECT trip_id, start_time, roads, speed_array, tms
    #     FROM public.trip_data
    #     WHERE array_length(roads, 1) >= 2
    #       AND array_length(speed_array, 1) >= 1
    #     ORDER BY trip_id DESC
    #     LIMIT :trip_limit
    #     """
    # )
    # rows = (await db.execute(q, {"trip_limit": int(max(1000, trip_limit))})).mappings().all()
    #
    # road_hour_sum_count: dict[int, dict[int, list[float]]] = {}
    # hour_sum_count: dict[int, list[float]] = {}
    # trained_segment_count = 0
    #
    # for row in rows:
    #     roads = list(row.get("roads") or [])
    #     speeds = list(row.get("speed_array") or [])
    #     tms = list(row.get("tms") or [])
    #     if not roads or not speeds:
    #         continue
    #
    #     fallback_hour = int(getattr(row.get("start_time"), "hour", 0) or 0)
    #     usable = min(len(speeds), max(0, len(roads) - 1))
    #
    #     for idx in range(usable):
    #         road_raw = roads[idx]
    #         speed_raw = speeds[idx]
    #         if road_raw is None or speed_raw is None:
    #             continue
    #         try:
    #             road_id = int(road_raw)
    #             speed = float(speed_raw)
    #         except Exception:
    #             continue
    #         if not math.isfinite(speed):
    #             continue
    #
    #         ts = tms[idx] if idx < len(tms) else None
    #         hour = _safe_hour_from_ts(ts, fallback_hour)
    #         intensity = _clamp01((float(congestion_speed_kph) - speed) / max(float(congestion_speed_kph), 1.0))
    #
    #         road_bucket = road_hour_sum_count.setdefault(road_id, {})
    #         pair = road_bucket.setdefault(hour, [0.0, 0.0])
    #         pair[0] += intensity
    #         pair[1] += 1.0
    #
    #         hour_pair = hour_sum_count.setdefault(hour, [0.0, 0.0])
    #         hour_pair[0] += intensity
    #         hour_pair[1] += 1.0
    #         trained_segment_count += 1
    """
    【修改说明】
    从 DW 层 dw_fact_road_segment 获取路段速度和道路信息，
    congestion_intensity 已预计算，可直接用于训练。
    """
    q = text("""
        SELECT s.trip_id, s.road_id, s.speed_kph, s.start_hour,
               s.congestion_intensity
        FROM dw_fact_road_segment s
        WHERE s.speed_kph IS NOT NULL
          AND s.road_id IS NOT NULL
        ORDER BY s.trip_id DESC
        LIMIT :segment_limit
    """)
    # 估算路段数 = 行程数 * 平均路段数（约50）
    segment_limit = trip_limit * 50
    rows = (await db.execute(q, {"segment_limit": segment_limit})).mappings().all()

    road_hour_sum_count: dict[int, dict[int, list[float]]] = {}
    hour_sum_count: dict[int, list[float]] = {}
    trained_segment_count = 0

    for row in rows:
        road_id = int(row["road_id"])
        hour = int(row["start_hour"])
        intensity = float(row["congestion_intensity"]) if row.get("congestion_intensity") is not None else 0.0

        road_bucket = road_hour_sum_count.setdefault(road_id, {})
        pair = road_bucket.setdefault(hour, [0.0, 0.0])
        pair[0] += intensity
        pair[1] += 1.0

        hour_pair = hour_sum_count.setdefault(hour, [0.0, 0.0])
        hour_pair[0] += intensity
        hour_pair[1] += 1.0
        trained_segment_count += 1
    if trained_segment_count == 0:
        raise ValueError("No valid segment samples found for training.")

    road_hour_mean: dict[int, dict[int, float]] = {}
    road_total_count: dict[int, int] = {}
    total_sum = 0.0
    total_count = 0.0

    for road_id, hour_map in road_hour_sum_count.items():
        road_hour_mean[road_id] = {}
        road_count = 0
        for hour, pair in hour_map.items():
            s, c = pair
            if c <= 0:
                continue
            m = float(s / c)
            road_hour_mean[road_id][hour] = m
            road_count += int(c)
            total_sum += s
            total_count += c
        road_total_count[road_id] = road_count

    hour_global_mean = {
        hour: float(pair[0] / pair[1])
        for hour, pair in hour_sum_count.items()
        if pair[1] > 0
    }
    global_mean = float(total_sum / total_count) if total_count > 0 else 0.5

    x, y = _prepare_training_matrices(road_hour_mean, road_total_count, hour_global_mean, global_mean)
    if x.shape[0] < 24:
        raise ValueError("Not enough training rows after aggregation. Need at least 24 rows.")

    n = x.shape[0]
    rng = np.random.default_rng(42)
    indices = np.arange(n)
    rng.shuffle(indices)
    split = max(1, int(n * 0.8))
    train_idx = indices[:split]
    valid_idx = indices[split:] if split < n else indices[:]

    x_train, y_train = x[train_idx], y[train_idx]
    x_valid, y_valid = x[valid_idx], y[valid_idx]

    model = XGBRegressor(
        objective="reg:squarederror",
        n_estimators=260,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_alpha=0.0,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=2,
    )
    model.fit(x_train, y_train)

    pred = model.predict(x_valid)
    rmse = float(np.sqrt(np.mean((pred - y_valid) ** 2)))
    mae = float(np.mean(np.abs(pred - y_valid)))

    model_file = Path(model_path)
    model_file.parent.mkdir(parents=True, exist_ok=True)
    trained_at = datetime.utcnow()

    artifact = {
        "model_type": "xgboost",
        "trained_at": trained_at.isoformat(),
        "feature_order": FEATURE_ORDER,
        "global_mean": global_mean,
        "road_hour_mean": {str(k): {str(h): v for h, v in hm.items()} for k, hm in road_hour_mean.items()},
        "road_total_count": {str(k): v for k, v in road_total_count.items()},
        "hour_global_mean": {str(k): v for k, v in hour_global_mean.items()},
        "congestion_speed_kph": float(congestion_speed_kph),
        "model": model,
    }
    joblib.dump(artifact, model_file)

    return ForecastTrainResponse(
        ok=True,
        summary=ForecastTrainSummary(
            model_type="xgboost",
            model_path=str(model_file),
            trained_trip_count=len(rows),
            trained_segment_count=trained_segment_count,
            training_row_count=n,
            rmse=round(rmse, 6),
            mae=round(mae, 6),
            trained_at=trained_at,
        ),
    )


# async def forecast_trip_heatmap_xgboost(
#     db: AsyncSession,
#     *,
#     trip_id: int,
#     forecast_after_minutes: int,
#     model_path: str,
#     top_k: int = 300,
# ) -> ForecastTripHeatmapResponse:
#     model_file = Path(model_path)
#     if not model_file.exists():
#         raise FileNotFoundError(f"Model file not found: {model_file}")
#
#     artifact = joblib.load(model_file)
#     model, road_hour_mean, road_total_count, hour_global_mean, global_mean = _artifact_to_runtime(artifact)
#
#     q = text(
#         """
#         SELECT trip_id, lon, lat, roads, start_time, end_time
#         FROM public.trip_data
#         WHERE trip_id = :trip_id
#         ORDER BY log_date DESC
#         LIMIT 1
#         """
#     )
#     row = (await db.execute(q, {"trip_id": int(trip_id)})).mappings().first()
#     if not row:
#         raise ValueError("trip not found")
#
#     safe_after_minutes = int(forecast_after_minutes)
#     if safe_after_minutes < 0:
#         safe_after_minutes = 0
#
#     lon_arr = list(row.get("lon") or [])
#     lat_arr = list(row.get("lat") or [])
#     roads = list(row.get("roads") or [])
#     base_time = row.get("end_time") or row.get("start_time")
#     target_time = (base_time + timedelta(minutes=safe_after_minutes)) if base_time is not None else None
#     safe_hour = int(getattr(target_time, "hour", 0) or 0) % 24
#     target_label = target_time.strftime("%Y-%m-%d %H:%M") if target_time is not None else f"{safe_hour:02d}:00"
#     time_label = f"行程后{safe_after_minutes}分钟（目标 {target_label}）"
#
#     n_points = min(len(lon_arr), len(lat_arr))
#     if n_points < 1:
#         return ForecastTripHeatmapResponse(
#             summary=ForecastTripHeatmapSummary(
#                 model_type="xgboost",
#                 trip_id=int(trip_id),
#                 forecast_after_minutes=safe_after_minutes,
#                 forecast_hour=safe_hour,
#                 time_label=time_label,
#                 base_time=base_time,
#                 target_time=target_time,
#                 point_count=0,
#             ),
#             points=[],
#         )
#
#     points: list[ForecastHeatPoint] = []
#     last_valid_road_id: int | None = None
#
#     for idx in range(n_points):
#         try:
#             lon = float(lon_arr[idx])
#             lat = float(lat_arr[idx])
#         except Exception:
#             continue
#
#         road_raw = roads[idx] if idx < len(roads) else None
#         road_id: int | None = None
#         if road_raw is not None:
#             try:
#                 road_id = int(road_raw)
#                 last_valid_road_id = road_id
#             except Exception:
#                 road_id = None
#         if road_id is None:
#             road_id = last_valid_road_id
#         if road_id is None:
#             continue
#
#         feature = _build_feature_vector(
#             road_id=road_id,
#             target_hour=safe_hour,
#             road_hour_mean=road_hour_mean,
#             road_total_count=road_total_count,
#             hour_global_mean=hour_global_mean,
#             global_mean=global_mean,
#         )
#         pred = float(model.predict(np.asarray([feature], dtype=np.float32))[0])
#         intensity = _temporal_adjust_intensity(
#             pred,
#             hour=safe_hour,
#             hour_global_mean=hour_global_mean,
#             global_mean=global_mean,
#         )
#         sample_count = int(road_total_count.get(road_id, 0))
#         predicted_trips = max(0.01, intensity * max(sample_count / 24.0, 1.0))
#         if 0 <= safe_hour <= 5:
#             predicted_trips = min(predicted_trips, 1)
#
#         points.append(
#             ForecastHeatPoint(
#                 lon=round(lon, 6),
#                 lat=round(lat, 6),
#                 predicted_trips=round(predicted_trips, 4),
#                 intensity=round(intensity, 4),
#                 sample_count=sample_count,
#             )
#         )
#
#     safe_top_k = int(max(1, min(top_k, 20000)))
#     limited = points if len(points) <= safe_top_k else points[:safe_top_k]
#
#     return ForecastTripHeatmapResponse(
#         summary=ForecastTripHeatmapSummary(
#             model_type="xgboost",
#             trip_id=int(trip_id),
#             forecast_after_minutes=safe_after_minutes,
#             forecast_hour=safe_hour,
#             time_label=time_label,
#             base_time=base_time,
#             target_time=target_time,
#             point_count=len(limited),
#         ),
#         points=limited,
#     )
async def forecast_trip_heatmap_xgboost(
    db: AsyncSession,
    *,
    trip_id: int,
    forecast_after_minutes: int,
    model_path: str,
    top_k: int = 300,
) -> ForecastTripHeatmapResponse:
    model_file = Path(model_path)
    if not model_file.exists():
        raise FileNotFoundError(f"Model file not found: {model_file}")

    artifact = joblib.load(model_file)
    model, road_hour_mean, road_total_count, hour_global_mean, global_mean = _artifact_to_runtime(artifact)

    # 【修改】从 DW 层获取行程基本信息
    q_trip = text("""
        SELECT trip_id, start_time, end_time
        FROM dw_fact_trip
        WHERE trip_id = :trip_id
        LIMIT 1
    """)
    trip_row = (await db.execute(q_trip, {"trip_id": int(trip_id)})).mappings().first()
    if not trip_row:
        raise ValueError("trip not found")

    safe_after_minutes = int(forecast_after_minutes)
    if safe_after_minutes < 0:
        safe_after_minutes = 0

    # 【修改】从 DW 层路段表获取坐标和 road_id（已展开，无需处理数组）
    q_segments = text("""
        SELECT segment_index, start_lon, start_lat, road_id
        FROM dw_fact_road_segment
        WHERE trip_id = :trip_id
          AND start_lon IS NOT NULL
          AND start_lat IS NOT NULL
        ORDER BY segment_index
    """)
    seg_rows = (await db.execute(q_segments, {"trip_id": int(trip_id)})).mappings().all()

    base_time = trip_row.get("end_time") or trip_row.get("start_time")
    target_time = (base_time + timedelta(minutes=safe_after_minutes)) if base_time is not None else None
    safe_hour = int(getattr(target_time, "hour", 0) or 0) % 24
    target_label = target_time.strftime("%Y-%m-%d %H:%M") if target_time is not None else f"{safe_hour:02d}:00"
    time_label = f"行程后{safe_after_minutes}分钟（目标 {target_label}）"

    n_points = len(seg_rows)
    if n_points < 1:
        return ForecastTripHeatmapResponse(
            summary=ForecastTripHeatmapSummary(
                model_type="xgboost",
                trip_id=int(trip_id),
                forecast_after_minutes=safe_after_minutes,
                forecast_hour=safe_hour,
                time_label=time_label,
                base_time=base_time,
                target_time=target_time,
                point_count=0,
            ),
            points=[],
        )

    points: list[ForecastHeatPoint] = []
    last_valid_road_id: int | None = None

    for row in seg_rows:
        try:
            lon = float(row["start_lon"])
            lat = float(row["start_lat"])
        except Exception:
            continue

        road_raw = row.get("road_id")
        road_id: int | None = None
        if road_raw is not None:
            try:
                road_id = int(road_raw)
                last_valid_road_id = road_id
            except Exception:
                road_id = None
        if road_id is None:
            road_id = last_valid_road_id
        if road_id is None:
            continue

        feature = _build_feature_vector(
            road_id=road_id,
            target_hour=safe_hour,
            road_hour_mean=road_hour_mean,
            road_total_count=road_total_count,
            hour_global_mean=hour_global_mean,
            global_mean=global_mean,
        )
        pred = float(model.predict(np.asarray([feature], dtype=np.float32))[0])
        intensity = _temporal_adjust_intensity(
            pred,
            hour=safe_hour,
            hour_global_mean=hour_global_mean,
            global_mean=global_mean,
        )
        sample_count = int(road_total_count.get(road_id, 0))
        predicted_trips = max(0.01, intensity * max(sample_count / 24.0, 1.0))
        if 0 <= safe_hour <= 5:
            predicted_trips = min(predicted_trips, 1)

        points.append(
            ForecastHeatPoint(
                lon=round(lon, 6),
                lat=round(lat, 6),
                predicted_trips=round(predicted_trips, 4),
                intensity=round(intensity, 4),
                sample_count=sample_count,
            )
        )

    safe_top_k = int(max(1, min(top_k, 20000)))
    limited = points if len(points) <= safe_top_k else points[:safe_top_k]

    return ForecastTripHeatmapResponse(
        summary=ForecastTripHeatmapSummary(
            model_type="xgboost",
            trip_id=int(trip_id),
            forecast_after_minutes=safe_after_minutes,
            forecast_hour=safe_hour,
            time_label=time_label,
            base_time=base_time,
            target_time=target_time,
            point_count=len(limited),
        ),
        points=limited,
    )

async def forecast_trip_speed_curve_xgboost(
    db: AsyncSession,
    *,
    trip_id: int,
    horizon_minutes: int,
    step_minutes: int,
    model_path: str,
    top_k: int = 20000,
    congestion_speed_kph: float = 20.0,
) -> ForecastTripSpeedResponse:
    model_file = Path(model_path)
    if not model_file.exists():
        raise FileNotFoundError(f"Model file not found: {model_file}")

    artifact = joblib.load(model_file)
    model, road_hour_mean, road_total_count, hour_global_mean, global_mean = _artifact_to_runtime(artifact)

    threshold_kph = float(artifact.get("congestion_speed_kph") or congestion_speed_kph)

    # q = text(
    #     """
    #     SELECT trip_id, roads, start_time, end_time
    #     FROM public.trip_data
    #     WHERE trip_id = :trip_id
    #     ORDER BY log_date DESC
    #     LIMIT 1
    #     """
    # )
    # row = (await db.execute(q, {"trip_id": int(trip_id)})).mappings().first()
    # if not row:
    #     raise ValueError("trip not found")
    #
    # roads_raw = list(row.get("roads") or [])
    # road_ids: list[int] = []
    # for item in roads_raw:
    #     if item is None:
    #         continue
    #     try:
    #         road_ids.append(int(item))
    #     except Exception:
    #         continue
    #
    # if not road_ids:
    #     raise ValueError("trip has no usable road ids")
    #
    # safe_top_k = int(max(1, min(top_k, 20000)))
    # if len(road_ids) > safe_top_k:
    #     road_ids = road_ids[:safe_top_k]
    #
    # safe_horizon = int(max(30, min(horizon_minutes, 24 * 60)))
    # safe_step = int(max(5, min(step_minutes, safe_horizon)))
    # offset_list = list(range(safe_step, safe_horizon + 1, safe_step))
    # if not offset_list:
    #     offset_list = [safe_step]
    #
    # base_time = row.get("end_time") or row.get("start_time")
    #
    # points: list[ForecastSpeedPoint] = []
    # congested_offsets: list[int] = []
    # avg_speed_acc = 0.0
    # min_speed = float("inf")
    # max_intensity = 0.0
    #
    # for offset_minutes in offset_list:
    #     target_time = (base_time + timedelta(minutes=offset_minutes)) if base_time is not None else None
    #     target_hour = int(getattr(target_time, "hour", 0) or 0) % 24
    #
    #     intensity_sum = 0.0
    #     intensity_count = 0
    #     for road_id in road_ids:
    #         feature = _build_feature_vector(
    #             road_id=road_id,
    #             target_hour=target_hour,
    #             road_hour_mean=road_hour_mean,
    #             road_total_count=road_total_count,
    #             hour_global_mean=hour_global_mean,
    #             global_mean=global_mean,
    #         )
    #         pred = float(model.predict(np.asarray([feature], dtype=np.float32))[0])
    #         intensity = _temporal_adjust_intensity(
    #             pred,
    #             hour=target_hour,
    #             hour_global_mean=hour_global_mean,
    #             global_mean=global_mean,
    #         )
    #         intensity_sum += intensity
    #         intensity_count += 1
    #
    #     mean_intensity = (intensity_sum / intensity_count) if intensity_count > 0 else 0.0
    #     predicted_speed = _speed_from_intensity(mean_intensity)
    #     risk = _risk_level_from_intensity(mean_intensity)
    #
    #     if predicted_speed < threshold_kph:
    #         congested_offsets.append(offset_minutes)
    #
    #     avg_speed_acc += predicted_speed
    #     min_speed = min(min_speed, predicted_speed)
    #     max_intensity = max(max_intensity, mean_intensity)
    #
    #     points.append(
    #         ForecastSpeedPoint(
    #             offset_minutes=offset_minutes,
    #             target_time=target_time,
    #             predicted_speed_kph=round(predicted_speed, 3),
    #             predicted_intensity=round(mean_intensity, 4),
    #             risk_level=risk,  # type: ignore[arg-type]
    #         )
    #     )
    # 【修改】从 DW 层 dw_fact_trip + dw_fact_road_segment 获取数据
    # 不再从 trip_data 展开数组
    q_trip = text("""
        SELECT trip_id, start_time, end_time
        FROM dw_fact_trip
        WHERE trip_id = :trip_id
        LIMIT 1
    """)
    trip_row = (await db.execute(q_trip, {"trip_id": int(trip_id)})).mappings().first()
    if not trip_row:
        raise ValueError("trip not found")

    # 【修改】从 DW 层路段表获取 road_id 列表（已展开）
    q_segments = text("""
        SELECT road_id
        FROM dw_fact_road_segment
        WHERE trip_id = :trip_id
          AND road_id IS NOT NULL
        ORDER BY segment_index
    """)
    seg_rows = (await db.execute(q_segments, {"trip_id": int(trip_id)})).mappings().all()

    road_ids: list[int] = []
    for row in seg_rows:
        try:
            road_ids.append(int(row["road_id"]))
        except Exception:
            continue

    if not road_ids:
        raise ValueError("trip has no usable road ids")

    safe_top_k = int(max(1, min(top_k, 20000)))
    if len(road_ids) > safe_top_k:
        road_ids = road_ids[:safe_top_k]

    safe_horizon = int(max(30, min(horizon_minutes, 24 * 60)))
    safe_step = int(max(5, min(step_minutes, safe_horizon)))
    offset_list = list(range(safe_step, safe_horizon + 1, safe_step))
    if not offset_list:
        offset_list = [safe_step]

    base_time = trip_row.get("end_time") or trip_row.get("start_time")

    points: list[ForecastSpeedPoint] = []
    congested_offsets: list[int] = []
    avg_speed_acc = 0.0
    min_speed = float("inf")
    max_intensity = 0.0

    for offset_minutes in offset_list:
        target_time = (base_time + timedelta(minutes=offset_minutes)) if base_time is not None else None
        target_hour = int(getattr(target_time, "hour", 0) or 0) % 24

        intensity_sum = 0.0
        intensity_count = 0
        for road_id in road_ids:
            feature = _build_feature_vector(
                road_id=road_id,
                target_hour=target_hour,
                road_hour_mean=road_hour_mean,
                road_total_count=road_total_count,
                hour_global_mean=hour_global_mean,
                global_mean=global_mean,
            )
            pred = float(model.predict(np.asarray([feature], dtype=np.float32))[0])
            intensity = _temporal_adjust_intensity(
                pred,
                hour=target_hour,
                hour_global_mean=hour_global_mean,
                global_mean=global_mean,
            )
            intensity_sum += intensity
            intensity_count += 1

        mean_intensity = (intensity_sum / intensity_count) if intensity_count > 0 else 0.0
        predicted_speed = _speed_from_intensity(mean_intensity)
        risk = _risk_level_from_intensity(mean_intensity)

        if predicted_speed < threshold_kph:
            congested_offsets.append(offset_minutes)

        avg_speed_acc += predicted_speed
        min_speed = min(min_speed, predicted_speed)
        max_intensity = max(max_intensity, mean_intensity)

        points.append(
            ForecastSpeedPoint(
                offset_minutes=offset_minutes,
                target_time=target_time,
                predicted_speed_kph=round(predicted_speed, 3),
                predicted_intensity=round(mean_intensity, 4),
                risk_level=risk,
            )
        )
    total_points = max(1, len(points))
    avg_speed = avg_speed_acc / total_points
    if min_speed == float("inf"):
        min_speed = avg_speed

    congestion_start_offset = congested_offsets[0] if congested_offsets else None
    congestion_start_time = (
        base_time + timedelta(minutes=congestion_start_offset)
        if (base_time is not None and congestion_start_offset is not None)
        else None
    )
    congestion_duration = len(congested_offsets) * safe_step
    overall_risk = _risk_level_from_intensity(max_intensity)

    windows: list[ForecastSpeedWindow] = []
    if congested_offsets:
        start_idx = congested_offsets[0]
        prev_idx = congested_offsets[0]
        for off in congested_offsets[1:]:
            if off == prev_idx + safe_step:
                prev_idx = off
                continue
            win_start = start_idx
            win_end = prev_idx
            window_points = [p for p in points if win_start <= p.offset_minutes <= win_end]
            win_max_intensity = max((p.predicted_intensity for p in window_points), default=0.0)
            win_risk = _risk_level_from_intensity(win_max_intensity)
            windows.append(
                ForecastSpeedWindow(
                    start_offset_minutes=win_start,
                    end_offset_minutes=win_end,
                    duration_minutes=(win_end - win_start + safe_step),
                    start_time=(base_time + timedelta(minutes=win_start)) if base_time is not None else None,
                    end_time=(base_time + timedelta(minutes=win_end)) if base_time is not None else None,
                    max_predicted_intensity=round(win_max_intensity, 4),
                    risk_level=win_risk,  # type: ignore[arg-type]
                )
            )
            start_idx = off
            prev_idx = off

        win_start = start_idx
        win_end = prev_idx
        window_points = [p for p in points if win_start <= p.offset_minutes <= win_end]
        win_max_intensity = max((p.predicted_intensity for p in window_points), default=0.0)
        win_risk = _risk_level_from_intensity(win_max_intensity)
        windows.append(
            ForecastSpeedWindow(
                start_offset_minutes=win_start,
                end_offset_minutes=win_end,
                duration_minutes=(win_end - win_start + safe_step),
                start_time=(base_time + timedelta(minutes=win_start)) if base_time is not None else None,
                end_time=(base_time + timedelta(minutes=win_end)) if base_time is not None else None,
                max_predicted_intensity=round(win_max_intensity, 4),
                risk_level=win_risk,  # type: ignore[arg-type]
            )
        )

    return ForecastTripSpeedResponse(
        summary=ForecastSpeedSummary(
            model_type="xgboost",
            trip_id=int(trip_id),
            horizon_minutes=safe_horizon,
            step_minutes=safe_step,
            congestion_speed_kph=round(threshold_kph, 3),
            avg_predicted_speed_kph=round(avg_speed, 3),
            min_predicted_speed_kph=round(min_speed, 3),
            max_predicted_intensity=round(max_intensity, 4),
            congestion_start_offset_minutes=congestion_start_offset,
            congestion_start_time=congestion_start_time,
            congestion_duration_minutes=congestion_duration,
            overall_risk_level=overall_risk,  # type: ignore[arg-type]
        ),
        points=points,
        windows=windows,
    )
