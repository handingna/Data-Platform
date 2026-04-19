from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BACKEND_DIR / ".env")


class Settings:
    def __init__(self) -> None:
        self.database_url: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/trajdb")
        self.app_cors_origins: str = os.getenv("APP_CORS_ORIGINS", "http://localhost:8081")
        self.forecast_model_path: str = os.getenv("FORECAST_MODEL_PATH", str(BACKEND_DIR / "models" / "future_heatmap_xgb.joblib"))
        self.forecast_congestion_speed_kph: float = float(os.getenv("FORECAST_CONGESTION_SPEED_KPH", "20"))

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.app_cors_origins.split(",") if o.strip()]


settings = Settings()

