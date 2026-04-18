from __future__ import annotations

import unittest

from backend.app.diagnosis import analyze_trip_diagnosis
from backend.app.schemas import TripDetail, TripPoint


def make_trip(points: list[TripPoint], trip_id: int = 1) -> TripDetail:
    return TripDetail(
        trip_id=trip_id,
        log_date="2015-01-01",
        devid=10001,
        points=points,
    )


class DiagnosisRuleTests(unittest.TestCase):
    def test_normal_trip_has_no_events(self) -> None:
        trip = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=35, road_id=1),
                TripPoint(lon=0.01, lat=0.0, t=120, speed_kph=35, road_id=2),
                TripPoint(lon=0.02, lat=0.0, t=240, speed_kph=35, road_id=3),
            ]
        )
        diagnosis = analyze_trip_diagnosis(trip)
        self.assertEqual([], diagnosis.events)
        self.assertEqual("low", diagnosis.summary.risk_level)

    def test_stop_event_detected(self) -> None:
        trip = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=25, road_id=1),
                TripPoint(lon=0.005, lat=0.0, t=60, speed_kph=25, road_id=2),
                TripPoint(lon=0.00502, lat=0.0, t=180, speed_kph=1, road_id=2),
                TripPoint(lon=0.00501, lat=0.0, t=300, speed_kph=1, road_id=2),
                TripPoint(lon=0.00503, lat=0.0, t=420, speed_kph=1, road_id=2),
                TripPoint(lon=0.006, lat=0.0, t=540, speed_kph=20, road_id=3),
            ],
            trip_id=2,
        )
        diagnosis = analyze_trip_diagnosis(trip)
        self.assertIn("stop", [event.type for event in diagnosis.events])

    def test_speed_jump_detected(self) -> None:
        trip = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=20, road_id=1),
                TripPoint(lon=0.002, lat=0.0, t=60, speed_kph=20, road_id=1),
                TripPoint(lon=0.004, lat=0.0, t=120, speed_kph=82, road_id=2),
                TripPoint(lon=0.0042, lat=0.0, t=180, speed_kph=5, road_id=2),
            ],
            trip_id=3,
        )
        diagnosis = analyze_trip_diagnosis(trip)
        self.assertIn("speed_jump", [event.type for event in diagnosis.events])

    def test_jump_point_detected(self) -> None:
        trip = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=30, road_id=1),
                TripPoint(lon=0.004, lat=0.0, t=60, speed_kph=30, road_id=2),
                TripPoint(lon=0.02, lat=0.02, t=120, speed_kph=130, road_id=9),
                TripPoint(lon=0.0045, lat=0.0, t=180, speed_kph=30, road_id=2),
                TripPoint(lon=0.008, lat=0.0, t=240, speed_kph=30, road_id=3),
            ],
            trip_id=4,
        )
        diagnosis = analyze_trip_diagnosis(trip)
        self.assertIn("jump_point", [event.type for event in diagnosis.events])

    def test_drift_detected(self) -> None:
        trip = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=20, road_id=1),
                TripPoint(lon=0.00045, lat=0.0, t=60, speed_kph=20, road_id=2),
                TripPoint(lon=0.0, lat=0.00035, t=120, speed_kph=18, road_id=3),
                TripPoint(lon=0.00045, lat=0.0001, t=180, speed_kph=18, road_id=4),
                TripPoint(lon=0.0, lat=0.0, t=240, speed_kph=18, road_id=5),
                TripPoint(lon=0.00045, lat=0.00035, t=300, speed_kph=18, road_id=6),
                TripPoint(lon=0.0, lat=0.0001, t=360, speed_kph=18, road_id=7),
            ],
            trip_id=5,
        )
        diagnosis = analyze_trip_diagnosis(trip)
        self.assertIn("drift", [event.type for event in diagnosis.events])

    def test_detour_detected(self) -> None:
        trip = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=30, road_id=1),
                TripPoint(lon=0.009, lat=0.0, t=120, speed_kph=30, road_id=2),
                TripPoint(lon=0.0, lat=0.0, t=240, speed_kph=30, road_id=1),
                TripPoint(lon=0.009, lat=0.0, t=360, speed_kph=30, road_id=2),
            ],
            trip_id=6,
        )
        diagnosis = analyze_trip_diagnosis(trip)
        self.assertIn("detour", [event.type for event in diagnosis.events])


if __name__ == "__main__":
    unittest.main()
