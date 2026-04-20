from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


AnomalyType = Literal["detour", "stop", "speed_jump", "drift", "jump_point"]
SegmentType = Literal["normal", "detour", "stop", "speed_jump", "drift", "jump_point"]
SeverityLevel = Literal["none", "low", "medium", "high"]
RiskLevel = Literal["low", "medium", "high"]
OperationMode = Literal["night_shift", "commuter_peak", "long_haul", "local_shuttle", "steady_all_day"]
ShiftCode = Literal["night", "morning_peak", "daytime", "evening_peak", "mixed"]
RegionCode = Literal["north", "northeast", "east", "southeast", "south", "southwest", "west", "northwest"]


class TripPoint(BaseModel):
    lon: float
    lat: float
    t: float | None = None
    speed_kph: float | None = None
    road_id: int | None = None


class TripSummary(BaseModel):
    trip_id: int
    log_date: date
    devid: int | None = None
    distance_km: float | None = None
    duration_seconds: float | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    avg_speed_kph: float | None = None


class TripDetail(TripSummary):
    points: list[TripPoint] = Field(default_factory=list)


class Segment(BaseModel):
    start: tuple[float, float]  # (lon, lat)
    end: tuple[float, float]
    speed_kph: float | None = None
    status: Literal["congested", "smooth"]


class TripSegmentsResponse(BaseModel):
    trip: TripSummary
    congestion_threshold_kph: float
    segments: list[Segment]


class CarProfile(BaseModel):
    device_id: str
    total_distance: float
    trips_total: int
    trip_ids: list[int] = Field(default_factory=list)
    trips_distance: list[float] = Field(default_factory=list)

    trips_total_by_2h: dict[str, int] = Field(default_factory=dict)
    total_distance_by_2h: dict[str, float] = Field(default_factory=dict)


class CarTripsItem(BaseModel):
    trip_id: int
    log_date: date
    distance_km: float | None = None
    duration_seconds: float | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None


class CarPortraitSummary(BaseModel):
    device_id: str
    total_trips: int
    total_distance_km: float
    avg_trip_distance_km: float
    avg_trip_duration_minutes: float
    active_days: int
    avg_daily_work_hours: float
    dominant_shift: ShiftCode
    operation_mode: OperationMode
    night_trip_ratio: float
    hotspot_concentration: float


class ActiveTimeBin(BaseModel):
    label: str
    trip_count: int
    distance_km: float
    share_ratio: float


class RegionRadarItem(BaseModel):
    region: RegionCode
    score: float
    trip_count: int


class DailyRhythmItem(BaseModel):
    date: date
    first_start_hour: float | None = None
    last_end_hour: float | None = None
    work_span_hours: float | None = None
    trip_count: int
    distance_km: float


class RouteClusterItem(BaseModel):
    cluster_key: str
    trip_count: int
    avg_distance_km: float
    avg_start_hour: float | None = None
    start_center: tuple[float, float]
    end_center: tuple[float, float]


class PeerGroupItem(BaseModel):
    device_id: str
    operation_mode: OperationMode
    avg_trip_distance_km: float
    avg_daily_work_hours: float
    total_trips: int
    is_current: bool = False


class CarPortraitResponse(BaseModel):
    summary: CarPortraitSummary
    active_time_bins: list[ActiveTimeBin] = Field(default_factory=list)
    region_radar: list[RegionRadarItem] = Field(default_factory=list)
    daily_rhythm: list[DailyRhythmItem] = Field(default_factory=list)
    route_clusters: list[RouteClusterItem] = Field(default_factory=list)
    peer_groups: list[PeerGroupItem] = Field(default_factory=list)


class HealthResponse(BaseModel):
    ok: bool = True
    db: Literal["up", "down"] = "up"
    details: dict[str, Any] = Field(default_factory=dict)


class AnomalyEventCount(BaseModel):
    type: AnomalyType
    count: int = 0


class TripDiagnosisSummary(BaseModel):
    risk_level: RiskLevel
    anomaly_score: int
    total_events: int
    event_counts: list[AnomalyEventCount] = Field(default_factory=list)


class TripDiagnosisMetrics(BaseModel):
    direct_distance_km: float
    actual_distance_km: float
    directness_ratio: float | None = None
    max_speed_kph: float
    stop_seconds_total: float
    repeated_road_ratio: float
    backtrack_count: int


class AnomalyEvent(BaseModel):
    id: str
    type: AnomalyType
    severity: Literal["low", "medium", "high"]
    color: str
    title: str
    description: str
    start_index: int
    end_index: int
    focus_index: int
    start_time: datetime | None = None
    end_time: datetime | None = None
    start_point: tuple[float, float]
    end_point: tuple[float, float]
    focus_point: tuple[float, float]
    evidence: dict[str, Any] = Field(default_factory=dict)


class DiagnosisSegment(BaseModel):
    start: tuple[float, float]
    end: tuple[float, float]
    start_index: int
    end_index: int
    type: SegmentType = "normal"
    severity: SeverityLevel = "none"
    speed_kph: float | None = None
    road_id: int | None = None
    color: str | None = None


class TripDiagnosisResponse(BaseModel):
    trip: TripDetail
    summary: TripDiagnosisSummary
    metrics: TripDiagnosisMetrics
    events: list[AnomalyEvent] = Field(default_factory=list)
    segments: list[DiagnosisSegment] = Field(default_factory=list)


class AnomalyVehicleRankingItem(BaseModel):
    device_id: str
    trip_count: int
    total_events: int
    high_risk_trips: int
    avg_anomaly_score: float
    worst_trip_id: int | None = None
    worst_risk_level: RiskLevel | None = None
    dominant_type: AnomalyType | None = None
    event_counts: list[AnomalyEventCount] = Field(default_factory=list)


class AnomalyVehicleRankingResponse(BaseModel):
    sample_trip_count: int
    vehicle_count: int
    items: list[AnomalyVehicleRankingItem] = Field(default_factory=list)


class AnomalyRoadDistributionItem(BaseModel):
    road_id: int
    occurrence_count: int
    trip_count: int
    dominant_type: AnomalyType | None = None
    max_severity: Literal["low", "medium", "high"] | None = None
    avg_speed_kph: float | None = None
    sample_trip_id: int | None = None
    start_point: tuple[float, float]
    end_point: tuple[float, float]
    center_point: tuple[float, float]
    event_counts: list[AnomalyEventCount] = Field(default_factory=list)


class AnomalyRoadDistributionResponse(BaseModel):
    sample_trip_count: int
    road_count: int
    items: list[AnomalyRoadDistributionItem] = Field(default_factory=list)


class ForecastHeatPoint(BaseModel):
    lon: float
    lat: float
    predicted_trips: float
    intensity: float
    sample_count: int


class ForecastHeatmapSummary(BaseModel):
    mode: Literal["pickup", "dropoff", "both"]
    forecast_hour: int
    time_label: str
    grid_size: float
    source_trip_count: int
    source_point_count: int
    generated_cells: int


class ForecastHeatmapResponse(BaseModel):
    summary: ForecastHeatmapSummary
    points: list[ForecastHeatPoint] = Field(default_factory=list)


class ForecastTrainSummary(BaseModel):
    model_type: Literal["xgboost"]
    model_path: str
    trained_trip_count: int
    trained_segment_count: int
    training_row_count: int
    rmse: float
    mae: float
    trained_at: datetime


class ForecastTrainResponse(BaseModel):
    ok: bool
    summary: ForecastTrainSummary


class ForecastTripHeatmapSummary(BaseModel):
    model_type: Literal["xgboost"]
    trip_id: int
    forecast_after_minutes: int = 0
    forecast_hour: int
    time_label: str
    base_time: datetime | None = None
    target_time: datetime | None = None
    point_count: int


class ForecastTripHeatmapResponse(BaseModel):
    summary: ForecastTripHeatmapSummary
    points: list[ForecastHeatPoint] = Field(default_factory=list)


class ForecastSpeedPoint(BaseModel):
    offset_minutes: int
    target_time: datetime | None = None
    predicted_speed_kph: float
    predicted_intensity: float
    risk_level: RiskLevel


class ForecastSpeedWindow(BaseModel):
    start_offset_minutes: int
    end_offset_minutes: int
    duration_minutes: int
    start_time: datetime | None = None
    end_time: datetime | None = None
    max_predicted_intensity: float
    risk_level: RiskLevel


class ForecastSpeedSummary(BaseModel):
    model_type: Literal["xgboost"]
    trip_id: int
    horizon_minutes: int
    step_minutes: int
    congestion_speed_kph: float
    avg_predicted_speed_kph: float
    min_predicted_speed_kph: float
    max_predicted_intensity: float
    congestion_start_offset_minutes: int | None = None
    congestion_start_time: datetime | None = None
    congestion_duration_minutes: int
    overall_risk_level: RiskLevel


class ForecastTripSpeedResponse(BaseModel):
    summary: ForecastSpeedSummary
    points: list[ForecastSpeedPoint] = Field(default_factory=list)
    windows: list[ForecastSpeedWindow] = Field(default_factory=list)

