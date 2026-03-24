from __future__ import annotations

import unittest
from datetime import date

from excel_schedule import Record, ordered_people


class ExportOrderingTests(unittest.TestCase):
    def test_ordered_people_uses_configured_kitchen_order(self) -> None:
        records = [
            Record(date(2026, 3, 1), "Кухня", "Resident One", ""),
            Record(date(2026, 3, 2), "Кухня", "Resident Two", ""),
            Record(date(2026, 3, 3), "Кухня", "Resident Three", ""),
            Record(date(2026, 3, 4), "Кухня", "Resident Four", ""),
        ]
        self.assertEqual(
            ordered_people(records, "Кухня"),
            [
                "Resident One",
                "Resident Two",
                "Resident Three",
                "Resident Four",
            ],
        )
