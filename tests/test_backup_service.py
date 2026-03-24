from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from backup_service import BackupService


class BackupServiceTests(unittest.TestCase):
    def test_restore_backup_reverts_files_and_keeps_cleanup_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            db_path = base / "coliving.db"
            residents_path = base / "residents.json"
            backup_dir = base / "backups"

            db_path.write_text("v1", encoding="utf-8")
            residents_path.write_text('{"residents":1}', encoding="utf-8")

            service = BackupService(
                db_path=db_path,
                residents_path=residents_path,
                enabled=False,
                local_dir=backup_dir,
                keep_count=2,
            )

            backup_dir.mkdir(parents=True, exist_ok=True)
            first_backup = backup_dir / "coliving_backup_1.zip"
            service._write_zip(first_backup)
            db_path.write_text("v2", encoding="utf-8")
            residents_path.write_text('{"residents":2}', encoding="utf-8")
            second_backup = backup_dir / "coliving_backup_2.zip"
            service._write_zip(second_backup)
            db_path.write_text("v3", encoding="utf-8")
            residents_path.write_text('{"residents":3}', encoding="utf-8")
            third_backup = backup_dir / "coliving_backup_3.zip"
            service._write_zip(third_backup)

            service._cleanup_old_backups()
            backups = service.list_backups(limit=10)
            self.assertEqual(len(backups), 2)
            self.assertNotIn(first_backup.name, {item.name for item in backups})

            service._restore_zip(second_backup)
            self.assertEqual(db_path.read_text(encoding="utf-8"), "v2")
            self.assertEqual(residents_path.read_text(encoding="utf-8"), '{"residents":2}')
