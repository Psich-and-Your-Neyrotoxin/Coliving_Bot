from __future__ import annotations

import json
import unittest
from datetime import date, datetime
from unittest.mock import patch

from deadline_policy import deadline_user_override_key, get_deadline_due_at_for_user
from handlers.common import get_user_report_options, get_user_report_zones
from scheduler import should_skip_any_scheduled_reminder_date


class _FakeDb:
    """Мінімальна async-заглушка для unit-тестів без реальної SQLite."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        return self.values.get(key, default)


class DeadlinePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_per_user_override_is_used_for_due_at(self) -> None:
        """Персональний дедлайн має перекривати стандартний дедлайн зони."""
        db = _FakeDb()
        db.values[deadline_user_override_key("Kitchen", date(2026, 3, 20), 123)] = "2026-03-25T12:00:00+02:00"

        due_at = await get_deadline_due_at_for_user(db, "Kitchen", date(2026, 3, 20), 123)

        self.assertEqual(due_at.strftime("%Y-%m-%d %H:%M"), "2026-03-25 12:00")

    async def test_report_options_include_older_duty_with_active_personal_deadline(self) -> None:
        """Меню здачі звіту повинно бачити старішу дату, якщо дедлайн ще живий."""
        db = _FakeDb()
        now = datetime.fromisoformat("2026-03-24T10:00:00+02:00")

        async def fake_zones(_db, _user_id, target_date):
            return {"Kitchen"} if target_date == date(2026, 3, 20) else set()

        async def fake_due_at(_db, zone, duty_date, _user_id):
            self.assertEqual(zone, "Kitchen")
            self.assertEqual(duty_date, date(2026, 3, 20))
            return datetime.fromisoformat("2026-03-25T12:00:00+02:00")

        with patch("handlers.common.get_user_report_zones", side_effect=fake_zones), patch(
            "handlers.common.get_deadline_due_at_for_user",
            side_effect=fake_due_at,
        ):
            options = await get_user_report_options(db, 123, now=now)

        self.assertIn(("Kitchen", date(2026, 3, 20)), options)

    async def test_skip_dates_cover_duty_date_crossing_midnight(self) -> None:
        """Пропуск дати чергування має гасити дедлайн і після переходу через північ."""
        db = _FakeDb()
        db.values["reminder_skip_dates_json"] = json.dumps(["2026-03-23"])

        should_skip = await should_skip_any_scheduled_reminder_date(db, date(2026, 3, 23), date(2026, 3, 24))

        self.assertTrue(should_skip)

    async def test_calendar_exception_blocks_report_zones(self) -> None:
        """Свято або day_off не повинні відкривати зони для здачі звіту."""
        db = _FakeDb()
        db.values["calendar_exceptions_json"] = json.dumps(
            [{"date": "2026-03-24", "kind": "holiday", "note": "Holiday"}]
        )

        zones = await get_user_report_zones(db, 123, date(2026, 3, 24))

        self.assertEqual(zones, set())
