from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject


class SchedulerMiddleware(BaseMiddleware):
    def __init__(self, scheduler_service: object) -> None:
        self._scheduler_service = scheduler_service

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        data["scheduler_service"] = self._scheduler_service
        return await handler(event, data)

