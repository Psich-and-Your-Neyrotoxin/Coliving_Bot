from __future__ import annotations

import json
import unittest
from datetime import date, timedelta
from pathlib import Path

from logic import load_schedule_config


class LogicScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        residents = json.loads(Path("residents.json").read_text(encoding="utf-8"))
        self.name_by_id = {int(row["telegram_id"]): row["full_name"] for row in residents}

    def test_kitchen_order_from_config(self) -> None:
        schedule = load_schedule_config()
        actual = [self.name_by_id[item] for item in schedule.kitchen_ids]
        expected = [
            "Resident One",
            "Resident Two",
            "Resident Three",
            "Resident Four",
        ]
        self.assertEqual(actual, expected)

    def test_kitchen_cycle_is_stable(self) -> None:
        schedule = load_schedule_config()
        start = date(2026, 1, 1)
        names = []
        for offset in range(18):
            current = start + timedelta(days=offset)
            resident_id = schedule.kitchen_ids[(current.timetuple().tm_yday - 1) % len(schedule.kitchen_ids)]
            names.append(self.name_by_id[resident_id])
        cycle = len(schedule.kitchen_ids)
        self.assertEqual(names[:cycle], names[cycle : cycle * 2])
