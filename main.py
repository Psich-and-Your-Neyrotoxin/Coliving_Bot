from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web

from backup_service import BackupService
from config import (
    ADMIN_ID,
    APP_ENV,
    APP_VERSION,
    BACKUP_ENABLED,
    BOT_MODE,
    BACKUP_INCLUDE_ENV,
    BACKUP_INTERVAL_HOURS,
    BACKUP_KEEP_COUNT,
    BACKUP_LOCAL_DIR,
    DB_PATH,
    GROUP_ID,
    LOG_FILE,
    LOG_DIR,
    RESIDENTS_JSON_PATH,
    TOKEN,
    WEBHOOK_BASE_URL,
    WEBHOOK_HOST,
    WEBHOOK_PATH,
    WEBHOOK_PORT,
    is_webhook_mode,
    validate_config,
)
from database import Database
from instance_config import load_instance_definition, seed_runtime_config_if_empty
from handlers.core import router as core_router
from handlers.admin import router as admin_router
from handlers.duty import router as duty_router
from handlers.fines import router as fines_router
from handlers.swap import router as swap_router
from middlewares.backup import BackupMiddleware
from middlewares.db import DbMiddleware
from scheduler import SchedulerService
from middlewares.scheduler import SchedulerMiddleware


# Optional bootstrap payment folders for a fresh instance.
# Keep empty in the public template; owners can import a bundle or fill folders from the admin panel.
PAYMENT_FOLDER_LINKS: dict[int, str] = {}


def setup_logging() -> None:
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
        force=True,
    )


async def main() -> None:
    setup_logging()

    if os.getenv("BOT_ENABLED", "true").lower() != "true":
        logging.info("Бот вимкнено через BOT_ENABLED")
        return

    # Перевірка залежностей (щоб помилка була очевидна в консолі)
    try:
        import aiogram  # noqa: F401
        import aiosqlite  # noqa: F401
        import apscheduler  # noqa: F401
    except Exception as e:
        logging.exception("Не встановлені залежності з requirements.txt: %s", e)
        return

    try:
        validate_config()
    except Exception as e:
        logging.exception("Помилка конфігурації: %s", e)
        return

    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher(storage=MemoryStorage())
    await bot.set_my_commands([])

    db = Database()
    try:
        await db.init_schema()
        runtime_already_seeded = await db.has_dynamic_zones()
        await db.seed_residents_if_empty(RESIDENTS_JSON_PATH)
        if not runtime_already_seeded:
            await db.sync_residents_from_json(RESIDENTS_JSON_PATH)
        await seed_runtime_config_if_empty(
            db,
            residents_path=RESIDENTS_JSON_PATH,
            owner_id=int(ADMIN_ID),
            group_id=int(GROUP_ID),
        )
        runtime_definition = await load_instance_definition(
            db,
            residents_path=RESIDENTS_JSON_PATH,
            owner_id=int(ADMIN_ID),
            group_id=int(GROUP_ID),
        )
        await db.set_setting("app_env", APP_ENV)
        await db.set_setting("app_version", APP_VERSION)
        await db.set_setting("db_path_shadow", DB_PATH)
        for telegram_id, folder_url in PAYMENT_FOLDER_LINKS.items():
            await db.set_setting(f"payment_folder:{telegram_id}", folder_url)
    except Exception as e:
        logging.exception("Помилка ініціалізації бази даних: %s", e)
        return

    scheduler_service = SchedulerService(bot=bot, db=db, group_id=int(GROUP_ID))
    scheduler_service.start()

    webhook_mode = is_webhook_mode()

    logging.info(
        "Runtime config | setup_complete=%s | coliving=%s | zones_total=%s | zones_enabled=%s | bot_mode=%s",
        runtime_definition.settings.setup_complete,
        runtime_definition.settings.coliving_name,
        len(runtime_definition.zones),
        sum(1 for zone in runtime_definition.zones if zone.enabled),
        BOT_MODE or ("webhook" if webhook_mode else "polling"),
    )
    if not runtime_definition.settings.setup_complete:
        logging.warning(
            "Runtime config is not marked as setup_complete yet. Legacy mode remains compatible, but setup wizard still needs completion."
        )

    backup_service = BackupService(
        db_path=Path(DB_PATH),
        residents_path=Path(RESIDENTS_JSON_PATH),
        enabled=BACKUP_ENABLED,
        interval_hours=BACKUP_INTERVAL_HOURS,
        local_dir=Path(BACKUP_LOCAL_DIR),
        include_env=BACKUP_INCLUDE_ENV,
        env_path=Path(".env"),
        keep_count=BACKUP_KEEP_COUNT,
        admin_id=int(ADMIN_ID),
        bot=bot,
    )
    backup_service.start()

    dp.update.middleware(DbMiddleware(db))
    dp.update.middleware(SchedulerMiddleware(scheduler_service))
    dp.update.middleware(BackupMiddleware(backup_service))
    dp.include_router(core_router)
    dp.include_router(admin_router)
    dp.include_router(duty_router)
    dp.include_router(swap_router)
    dp.include_router(fines_router)

    if webhook_mode:
        from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

        app = web.Application()
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)
        webhook_url = f"{WEBHOOK_BASE_URL.rstrip('/')}{WEBHOOK_PATH}"
        await bot.set_webhook(webhook_url)
        logging.info("Запуск у webhook-режимі: %s", webhook_url)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=WEBHOOK_HOST, port=WEBHOOK_PORT)
        await site.start()
        await asyncio.Event().wait()
    else:
        await bot.delete_webhook(drop_pending_updates=False)
        logging.info("Запуск у polling-режимі.")
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
