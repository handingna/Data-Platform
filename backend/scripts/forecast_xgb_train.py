from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.app.db import SessionLocal
from backend.app.forecast_xgboost import train_future_heatmap_xgboost
from backend.app.settings import settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train future heatmap XGBoost model")
    parser.add_argument("--trip-limit", type=int, default=20000, help="Number of trips used for training")
    parser.add_argument(
        "--model-path",
        type=str,
        default=settings.forecast_model_path,
        help="Output path for model artifact file",
    )
    parser.add_argument(
        "--congestion-speed-kph",
        type=float,
        default=20.0,
        help="Congestion speed threshold used to derive intensity",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    async with SessionLocal() as db:
        resp = await train_future_heatmap_xgboost(
            db,
            model_path=args.model_path,
            trip_limit=args.trip_limit,
            congestion_speed_kph=args.congestion_speed_kph,
        )
    print(resp.model_dump())


if __name__ == "__main__":
    asyncio.run(main())
