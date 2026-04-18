from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


AnomalyType = Literal["detour", "stop", "speed_jump", "drift", "jump_point"]
SegmentType = Literal["normal", "detour", "stop", "speed_jump", "drift", "jump_point"]
SeverityLevel = Literal["none", "low", "medium", "high"]
RiskLevel = Literal["low", "medium", "high"]


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

