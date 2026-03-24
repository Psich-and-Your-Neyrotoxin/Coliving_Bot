from __future__ import annotations

import unittest
from datetime import date, timedelta

from scheduler import SchedulerService
from handlers.common import KYIV_TZ


class _FakeDb:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.dynamic_zone_enabled = False
        self.dynamic_zone_deadline = "01:00"
        self.dynamic_zone_report_offset_days = 0

    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        return self.values.get(key, default)

    async def has_dynamic_zones(self) -> bool:
        return self.dynamic_zone_enabled

    async def list_instance_settings(self) -> dict[str, str]:
        return {}

    async def list_feature_flags(self) -> dict[str, bool]:
        return {}

    async def list_zone_records(self) -> list[dict]:
        if not self.dynamic_zone_enabled:
            return []
        return [
            {
                "code": "bath",
                "title": "Ванна",
                "enabled": 1,
                "sort_order": 20,
                "team_size_mode": "fixed",
                "report_required": 1,
                "report_deadline_time": self.dynamic_zone_deadline,
                "private_reminder_time": "23:00",
                "group_reminder_enabled": 0,
                "private_reminder_enabled": 1,
                "rotation_enabled": 1,
            }
        ]

    async def list_all_zone_members(self) -> list[dict]:
        return [{"zone_code": "bath", "telegram_id": 1, "sort_order": 0, "group_index": None, "slot_index": None, "is_active": 1}]

    async def list_all_zone_rules(self) -> list[dict]:
        if not self.dynamic_zone_enabled:
            return []
        return [
            {
                "zone_code": "bath",
                "rotation_mode": "ordered",
                "rotation_every_days": 7,
                "team_pattern_json": "[1]",
                "anchor_date": "2025-12-29",
                "config_json": '{"member_order":[1],"member_groups":[],"extra_config":{"report_offset_days":%s}}'
                % int(self.dynamic_zone_report_offset_days),
            }
        ]


class SchedulerDeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def test_deadline_defaults_to_next_day_for_early_hours(self) -> None:
        service = SchedulerService(bot=object(), db=_FakeDb(), group_id=1)
        due_at = await service._deadline_due_at("Kitchen", date(2026, 3, 19))
        self.assertEqual(due_at.strftime("%Y-%m-%d %H:%M"), "2026-03-20 01:00")

    async def test_deadline_override_can_move_to_same_day(self) -> None:
        db = _FakeDb()
        db.values["deadline_override:2026-03-19"] = '{"Kitchen":"23:59"}'
        service = SchedulerService(bot=object(), db=db, group_id=1)
        due_at = await service._deadline_due_at("Kitchen", date(2026, 3, 19))
        self.assertEqual(due_at.strftime("%Y-%m-%d %H:%M"), "2026-03-19 23:59")

    async def test_old_deadline_is_outside_fresh_alert_window(self) -> None:
        service = SchedulerService(bot=object(), db=_FakeDb(), group_id=1)
        due_at = await service._deadline_due_at("Kitchen", date(2026, 3, 19))
        stale_now = due_at + timedelta(hours=12, minutes=1)
        self.assertGreater(stale_now - due_at, timedelta(hours=12))
        self.assertEqual(stale_now.tzinfo, KYIV_TZ)

    async def test_runtime_zone_deadline_default_is_used(self) -> None:
        db = _FakeDb()
        db.dynamic_zone_enabled = True
        db.dynamic_zone_deadline = "22:30"
        service = SchedulerService(bot=object(), db=db, group_id=1)
        due_at = await service._deadline_due_at("Bath", date(2026, 3, 19))
        self.assertEqual(due_at.strftime("%Y-%m-%d %H:%M"), "2026-03-19 22:30")

    async def test_runtime_zone_report_day_offset_is_used(self) -> None:
        db = _FakeDb()
        db.dynamic_zone_enabled = True
        db.dynamic_zone_report_offset_days = 6
        service = SchedulerService(bot=object(), db=db, group_id=1)
        self.assertTrue(await service._is_legacy_report_day("Bath", date(2026, 1, 4)))
        self.assertFalse(await service._is_legacy_report_day("Bath", date(2026, 1, 3)))
