from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject


class BackupMiddleware(BaseMiddleware):
    def __init__(self, backup_service: object) -> None:
        self._backup_service = backup_service

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        data["backup_service"] = self._backup_service
        return await handler(event, data)
