from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_db
from .schemas import (
    AnomalyRoadDistributionResponse,
    AnomalyVehicleRankingResponse,
    CarProfile,
    CarTripsItem,
    HealthResponse,
    TripDetail,
    TripDiagnosisResponse,
    TripSegmentsResponse,
)
from .services import (
    fetch_anomaly_road_distribution,
    fetch_anomaly_vehicle_ranking,
    fetch_car_profile,
    fetch_car_trips,
    fetch_trip_by_id,
    fetch_trip_diagnosis,
    fetch_trip_segments,
    search_device_ids,
    search_trip_ids,
)
from .settings import settings

app = FastAPI(title="Traffic Data Platform API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health", response_model=HealthResponse)
async def health(db: AsyncSession = Depends(get_db)) -> HealthResponse:
    try:
        await db.execute(text("SELECT 1"))
        return HealthResponse(ok=True, db="up")
    except Exception as e:
        return HealthResponse(ok=False, db="down", details={"error": str(e)})


@app.get("/api/trips/{trip_id}", response_model=TripDetail)
async def get_trip(trip_id: int, db: AsyncSession = Depends(get_db)) -> TripDetail:
    trip = await fetch_trip_by_id(db, trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="trip not found")
    return trip


@app.get("/api/trips/{trip_id}/diagnosis", response_model=TripDiagnosisResponse)
async def get_trip_diagnosis(trip_id: int, db: AsyncSession = Depends(get_db)) -> TripDiagnosisResponse:
    diagnosis = await fetch_trip_diagnosis(db, trip_id)
    if not diagnosis:
        raise HTTPException(status_code=404, detail="trip not found or not enough points")
    return diagnosis


@app.get("/api/anomaly/vehicles", response_model=AnomalyVehicleRankingResponse)
async def get_anomaly_vehicle_ranking(
    limit: int = Query(default=10, ge=1, le=50),
    trip_sample: int = Query(default=300, ge=50, le=2000),
    per_vehicle: int = Query(default=5, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
) -> AnomalyVehicleRankingResponse:
    return await fetch_anomaly_vehicle_ranking(db, limit=limit, trip_sample=trip_sample, per_vehicle=per_vehicle)


@app.get("/api/anomaly/roads", response_model=AnomalyRoadDistributionResponse)
async def get_anomaly_road_distribution(
    limit: int = Query(default=12, ge=1, le=50),
    trip_sample: int = Query(default=300, ge=50, le=2000),
    db: AsyncSession = Depends(get_db),
) -> AnomalyRoadDistributionResponse:
    return await fetch_anomaly_road_distribution(db, limit=limit, trip_sample=trip_sample)


@app.get("/api/trips/{trip_id}/segments", response_model=TripSegmentsResponse)
async def get_trip_segments(
    trip_id: int,
    congestion_kph: float = Query(20.0, ge=0.0, le=200.0),
    db: AsyncSession = Depends(get_db),
) -> TripSegmentsResponse:
    resp = await fetch_trip_segments(db, trip_id, congestion_threshold_kph=congestion_kph)
    if not resp:
        raise HTTPException(status_code=404, detail="trip not found or not enough points")
    return resp


@app.get("/api/cars/{device_id}", response_model=CarProfile)
async def get_car(device_id: str, db: AsyncSession = Depends(get_db)) -> CarProfile:
    car = await fetch_car_profile(db, device_id)
    if not car:
        raise HTTPException(status_code=404, detail="car not found")
    return car


@app.get("/api/cars/{device_id}/trips", response_model=list[CarTripsItem])
async def get_car_trips(
    device_id: str,
    limit: int = Query(200, ge=1, le=2000),
    db: AsyncSession = Depends(get_db),
) -> list[CarTripsItem]:
    return await fetch_car_trips(db, device_id, limit=limit)


@app.get("/api/meta/trip-ids", response_model=list[int])
async def get_trip_ids(
    q: str = Query("", description="keyword for fuzzy search"),
    limit: int | None = Query(default=200, ge=1, le=1000000),
    db: AsyncSession = Depends(get_db),
) -> list[int]:
    return await search_trip_ids(db, q=q, limit=limit)


@app.get("/api/meta/device-ids", response_model=list[str])
async def get_device_ids(
    q: str = Query("", description="keyword for fuzzy search"),
    limit: int | None = Query(default=200, ge=1, le=1000000),
    db: AsyncSession = Depends(get_db),
) -> list[str]:
    return await search_device_ids(db, q=q, limit=limit)
