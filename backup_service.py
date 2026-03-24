from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from zipfile import BadZipFile
from zipfile import ZIP_DEFLATED, ZipFile

from aiogram.types import BufferedInputFile


KYIV_TZ = ZoneInfo("Europe/Kyiv")


def _now_kyiv() -> datetime:
    return datetime.now(KYIV_TZ)


@dataclass
class BackupService:
    db_path: Path
    residents_path: Path
    enabled: bool = False
    interval_hours: int = 48
    local_dir: Path = Path("backups")
    include_env: bool = False
    env_path: Path = Path(".env")
    keep_count: int = 10
    admin_id: int = 0
    bot: object | None = None
    _task: asyncio.Task | None = field(default=None, init=False, repr=False)

    def start(self) -> None:
        if not self.enabled:
            logging.info("Автобекап вимкнений.")
            return
        if self._task and not self._task.done():
            return
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self._task = asyncio.create_task(self._runner())

    async def _runner(self) -> None:
        logging.info("Автобекап увімкнено. Інтервал: %s год.", self.interval_hours)
        while True:
            try:
                await asyncio.sleep(max(1, int(self.interval_hours)) * 3600)
                backup_path = await self.create_backup()
                await self.send_backup_to_admin(backup_path, automatic=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.exception("Помилка автобекапу: %s", exc)
                await self._notify_admin(f"⚠️ Помилка автобекапу: <b>{type(exc).__name__}</b>")

    async def create_backup(self, *, cleanup: bool = True) -> Path:
        timestamp = _now_kyiv().strftime("%Y-%m-%d_%H-%M-%S_%f")
        backup_path = self.local_dir / f"coliving_backup_{timestamp}.zip"
        await asyncio.to_thread(self._write_zip, backup_path)
        if cleanup:
            await asyncio.to_thread(self._cleanup_old_backups)
        return backup_path

    def _write_zip(self, backup_path: Path) -> None:
        with ZipFile(backup_path, "w", compression=ZIP_DEFLATED) as archive:
            if self.db_path.exists():
                archive.write(self.db_path, arcname="coliving.db")
            if self.residents_path.exists():
                archive.write(self.residents_path, arcname="residents.json")
            if self.include_env and self.env_path.exists():
                archive.write(self.env_path, arcname=".env")

    def list_backups(self, limit: int = 20) -> list[Path]:
        self.local_dir.mkdir(parents=True, exist_ok=True)
        files = sorted(self.local_dir.glob("coliving_backup_*.zip"), reverse=True)
        return files[:limit]

    def _cleanup_old_backups(self) -> None:
        if self.keep_count <= 0:
            return
        files = self.list_backups(limit=10_000)
        for old in files[self.keep_count :]:
            try:
                old.unlink(missing_ok=True)
            except Exception:
                logging.exception("Не вдалося видалити старий бекап %s", old)

    async def restore_backup(self, backup_name: str) -> Path:
        target = self.local_dir / backup_name
        if not target.exists():
            raise FileNotFoundError(f"Бекап {backup_name} не знайдено.")
        safety_backup = await self.create_backup(cleanup=False)
        await asyncio.to_thread(self._restore_zip, target)
        await asyncio.to_thread(self._cleanup_old_backups)
        return safety_backup

    def _restore_zip(self, backup_path: Path) -> None:
        try:
            with ZipFile(backup_path, "r") as archive:
                archive.extract("coliving.db", path=str(self.db_path.parent))
                if "residents.json" in archive.namelist():
                    archive.extract("residents.json", path=str(self.residents_path.parent))
                if self.include_env and ".env" in archive.namelist():
                    archive.extract(".env", path=str(self.env_path.parent))
        except BadZipFile as exc:
            raise RuntimeError("Пошкоджений backup zip.") from exc

    async def send_backup_to_admin(self, backup_path: Path, *, automatic: bool = False) -> None:
        if not self.bot or not self.admin_id:
            return
        caption_prefix = "🗂 Автобекап" if automatic else "🗂 Ручний бекап"
        caption = (
            f"{caption_prefix}\n"
            f"Створено: <b>{_now_kyiv().strftime('%d.%m.%Y %H:%M:%S')}</b>\n"
            f"Файл: <b>{backup_path.name}</b>"
        )
        payload = await asyncio.to_thread(backup_path.read_bytes)
        await self.bot.send_document(
            chat_id=int(self.admin_id),
            document=BufferedInputFile(payload, filename=backup_path.name),
            caption=caption,
        )

    async def _notify_admin(self, text: str) -> None:
        if not self.bot or not self.admin_id:
            return
        try:
            await self.bot.send_message(chat_id=int(self.admin_id), text=text)
        except Exception:
            logging.info("Не вдалося надіслати статус автобекапу адміну.")
