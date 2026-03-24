from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path

from database import Database


class DatabaseIntegrationTests(unittest.TestCase):
    @unittest.skipUnless(
        os.getenv("RUN_SQLITE_INTEGRATION") == "1",
        "SQLite integration test запускається окремо, коли середовище стабільне для aiosqlite.",
    )
    def test_sync_residents_updates_and_deactivates_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            db = Database(base / "test.db")
            residents_file = base / "residents.json"
            residents_file.write_text(
                json.dumps(
                    [
                        {"telegram_id": 1, "full_name": "Тест Один", "role": "admin"},
                        {"telegram_id": 2, "full_name": "Тест Два", "role": "resident"},
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            asyncio.run(db.init_schema())
            asyncio.run(db.seed_residents_if_empty(residents_file))

            first = asyncio.run(db.list_all_residents_full())
            self.assertEqual({row["telegram_id"] for row in first}, {1, 2})

            residents_file.write_text(
                json.dumps(
                    [
                        {"telegram_id": 1, "full_name": "Тест Один Оновлений", "role": "admin"},
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            asyncio.run(db.sync_residents_from_json(residents_file))
            resident_1 = asyncio.run(db.get_resident(1))
            resident_2 = asyncio.run(db.get_resident(2))
            self.assertEqual(resident_1["full_name"], "Тест Один Оновлений")
            self.assertEqual(int(resident_2["is_active"]), 0)
