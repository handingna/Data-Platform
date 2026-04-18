from __future__ import annotations

import unittest

from backend.app.diagnosis import analyze_trip_diagnosis
from backend.app.schemas import TripDetail, TripPoint
from backend.app.services import summarize_anomaly_road_distribution


def make_trip(points: list[TripPoint], trip_id: int) -> TripDetail:
    return TripDetail(
        trip_id=trip_id,
        log_date="2015-01-01",
        devid=10001,
        points=points,
    )


class AnomalyRoadDistributionTests(unittest.TestCase):
    def test_same_road_speed_jump_segments_merge_into_one_occurrence(self) -> None:
        trip = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=18, road_id=1),
                TripPoint(lon=0.002, lat=0.0, t=60, speed_kph=18, road_id=9),
                TripPoint(lon=0.004, lat=0.0, t=120, speed_kph=82, road_id=9),
                TripPoint(lon=0.0042, lat=0.0, t=180, speed_kph=5, road_id=9),
            ],
            trip_id=11,
        )

        response = summarize_anomaly_road_distribution(
            diagnoses=[analyze_trip_diagnosis(trip)],
            sample_trip_count=1,
            limit=10,
        )

        self.assertEqual(1, response.road_count)
        self.assertEqual(1, len(response.items))
        road = response.items[0]
        self.assertEqual(9, road.road_id)
        self.assertEqual(1, road.occurrence_count)
        self.assertEqual(1, road.trip_count)
        self.assertEqual("speed_jump", road.dominant_type)
        self.assertEqual(1, road.event_counts[2].count)
        self.assertEqual(11, road.sample_trip_id)

    def test_distribution_aggregates_multiple_trips_by_road(self) -> None:
        trip_a = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=18, road_id=1),
                TripPoint(lon=0.002, lat=0.0, t=60, speed_kph=18, road_id=9),
                TripPoint(lon=0.004, lat=0.0, t=120, speed_kph=82, road_id=9),
                TripPoint(lon=0.0042, lat=0.0, t=180, speed_kph=5, road_id=9),
            ],
            trip_id=21,
        )
        trip_b = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=15, road_id=2),
                TripPoint(lon=0.0022, lat=0.0, t=60, speed_kph=15, road_id=9),
                TripPoint(lon=0.0042, lat=0.0, t=120, speed_kph=75, road_id=9),
                TripPoint(lon=0.0044, lat=0.0, t=180, speed_kph=8, road_id=9),
            ],
            trip_id=22,
        )
        trip_c = make_trip(
            [
                TripPoint(lon=0.0, lat=0.0, t=0, speed_kph=30, road_id=3),
                TripPoint(lon=0.004, lat=0.0, t=60, speed_kph=30, road_id=7),
                TripPoint(lon=0.02, lat=0.02, t=120, speed_kph=130, road_id=7),
                TripPoint(lon=0.0045, lat=0.0, t=180, speed_kph=30, road_id=7),
                TripPoint(lon=0.008, lat=0.0, t=240, speed_kph=30, road_id=8),
            ],
            trip_id=23,
        )

        response = summarize_anomaly_road_distribution(
            diagnoses=[analyze_trip_diagnosis(trip_a), analyze_trip_diagnosis(trip_b), analyze_trip_diagnosis(trip_c)],
            sample_trip_count=3,
            limit=10,
        )

        self.assertEqual(3, response.sample_trip_count)
        self.assertGreaterEqual(response.road_count, 2)
        top_road = response.items[0]
        self.assertEqual(9, top_road.road_id)
        self.assertEqual(2, top_road.occurrence_count)
        self.assertEqual(2, top_road.trip_count)
        self.assertEqual("speed_jump", top_road.dominant_type)

        road_ids = [item.road_id for item in response.items]
        self.assertIn(7, road_ids)


if __name__ == "__main__":
    unittest.main()
