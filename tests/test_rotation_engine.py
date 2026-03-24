from __future__ import annotations

import unittest
from datetime import date

from rotation_engine import RotationRule, compute_zone_rotation


class RotationEngineTests(unittest.TestCase):
    def test_ordered_rotation_supports_team_pattern(self) -> None:
        rule = RotationRule(
            rotation_mode="ordered",
            rotation_every_days=1,
            team_pattern=(2, 3, 2),
            anchor_date=date(2026, 1, 1),
            member_order=(101, 102, 103, 104, 105),
        )

        self.assertEqual(compute_zone_rotation(rule, date(2026, 1, 1)), (101, 102))
        self.assertEqual(compute_zone_rotation(rule, date(2026, 1, 2)), (103, 104, 105))
        self.assertEqual(compute_zone_rotation(rule, date(2026, 1, 3)), (101, 102))

    def test_ordered_rotation_supports_multi_day_interval(self) -> None:
        rule = RotationRule(
            rotation_mode="ordered",
            rotation_every_days=10,
            team_pattern=(1,),
            anchor_date=date(2026, 1, 1),
            member_order=(1, 2, 3),
        )

        self.assertEqual(compute_zone_rotation(rule, date(2026, 1, 1)), (1,))
        self.assertEqual(compute_zone_rotation(rule, date(2026, 1, 10)), (1,))
        self.assertEqual(compute_zone_rotation(rule, date(2026, 1, 11)), (2,))

    def test_grouped_rotation_supports_fixed_pairs(self) -> None:
        rule = RotationRule(
            rotation_mode="grouped",
            rotation_every_days=7,
            team_pattern=(2,),
            anchor_date=date(2025, 12, 29),
            member_groups=((11, 12), (21, 22), (31, 32)),
        )

        self.assertEqual(compute_zone_rotation(rule, date(2025, 12, 29)), (11, 12))
        self.assertEqual(compute_zone_rotation(rule, date(2026, 1, 5)), (21, 22))
        self.assertEqual(compute_zone_rotation(rule, date(2026, 1, 12)), (31, 32))
