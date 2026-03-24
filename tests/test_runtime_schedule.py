from __future__ import annotations

import unittest
from datetime import date

from instance_config import InstanceDefinition, InstanceSettings, ZoneDefinition, default_feature_flags
from rotation_engine import RotationRule
from runtime_schedule import apply_legacy_swaps_to_assignments, apply_zone_overrides_to_assignments, build_zone_assignments


class RuntimeScheduleTests(unittest.TestCase):
    def test_build_zone_assignments_uses_runtime_definition(self) -> None:
        definition = InstanceDefinition(
            settings=InstanceSettings(
                coliving_name="Test",
                timezone="Europe/Kyiv",
                owner_id=1,
                group_id=2,
                language="uk",
                setup_complete=False,
            ),
            feature_flags=default_feature_flags(),
            zones=(
                ZoneDefinition(
                    code="kitchen",
                    title="Кухня",
                    enabled=True,
                    sort_order=10,
                    team_size_mode="pattern",
                    report_required=True,
                    report_deadline_time="01:00",
                    private_reminder_time="23:00",
                    group_reminder_enabled=False,
                    private_reminder_enabled=True,
                    rotation_enabled=True,
                    rule=RotationRule(
                        rotation_mode="ordered",
                        rotation_every_days=1,
                        team_pattern=(2, 1),
                        anchor_date=date(2026, 1, 1),
                        member_order=(10, 20, 30),
                    ),
                ),
            ),
        )
        resident_map = {
            10: {"full_name": "A"},
            20: {"full_name": "B"},
            30: {"full_name": "C"},
        }

        first = build_zone_assignments(definition, resident_map, date(2026, 1, 1))
        second = build_zone_assignments(definition, resident_map, date(2026, 1, 2))

        self.assertEqual(first["kitchen"].member_ids, (10, 20))
        self.assertEqual(second["kitchen"].member_ids, (30,))

    def test_legacy_swaps_are_applied_to_runtime_assignments(self) -> None:
        definition = InstanceDefinition(
            settings=InstanceSettings(
                coliving_name="Test",
                timezone="Europe/Kyiv",
                owner_id=1,
                group_id=2,
                language="uk",
                setup_complete=False,
            ),
            feature_flags=default_feature_flags(),
            zones=(
                ZoneDefinition(
                    code="general",
                    title="Общак",
                    enabled=True,
                    sort_order=30,
                    team_size_mode="fixed",
                    report_required=True,
                    report_deadline_time="01:00",
                    private_reminder_time="23:00",
                    group_reminder_enabled=False,
                    private_reminder_enabled=True,
                    rotation_enabled=True,
                    rule=RotationRule(
                        rotation_mode="grouped",
                        rotation_every_days=7,
                        team_pattern=(2,),
                        anchor_date=date(2025, 12, 29),
                        member_groups=((11, 12),),
                    ),
                ),
            ),
        )
        resident_map = {
            11: {"full_name": "A"},
            12: {"full_name": "B"},
            99: {"full_name": "Swap"},
        }

        assignments = build_zone_assignments(definition, resident_map, date(2025, 12, 29))
        swapped = apply_legacy_swaps_to_assignments(
            assignments,
            resident_map,
            [{"zone": "General", "from_id": 12, "to_id": 99, "date": "2025-12-29"}],
        )

        self.assertEqual(swapped["general"].member_ids, (11, 99))

    def test_runtime_overrides_are_applied_to_any_zone_code(self) -> None:
        definition = InstanceDefinition(
            settings=InstanceSettings(
                coliving_name="Test",
                timezone="Europe/Kyiv",
                owner_id=1,
                group_id=2,
                language="uk",
                setup_complete=False,
            ),
            feature_flags=default_feature_flags(),
            zones=(
                ZoneDefinition(
                    code="laundry",
                    title="Пральня",
                    enabled=True,
                    sort_order=30,
                    team_size_mode="fixed",
                    report_required=True,
                    report_deadline_time="21:00",
                    private_reminder_time="19:00",
                    group_reminder_enabled=False,
                    private_reminder_enabled=True,
                    rotation_enabled=True,
                    rule=RotationRule(
                        rotation_mode="ordered",
                        rotation_every_days=7,
                        team_pattern=(2,),
                        anchor_date=date(2026, 1, 1),
                        member_order=(11, 12, 13),
                    ),
                ),
            ),
        )
        resident_map = {
            11: {"full_name": "A"},
            12: {"full_name": "B"},
            13: {"full_name": "C"},
            98: {"full_name": "Override A"},
            99: {"full_name": "Override B"},
        }

        assignments = build_zone_assignments(definition, resident_map, date(2026, 1, 1))
        overridden = apply_zone_overrides_to_assignments(
            assignments,
            resident_map,
            [
                {"zone_name": "laundry", "slot_index": 0, "telegram_id": 99},
                {"zone_name": "laundry", "slot_index": 1, "telegram_id": 98},
            ],
        )

        self.assertEqual(overridden["laundry"].member_ids, (99, 98))
