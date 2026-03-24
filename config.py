from __future__ import annotations

import os
from pathlib import Path


def _load_dotenv() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv()

TOKEN = os.getenv("TOKEN", "")
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BOT_MODE = os.getenv("BOT_MODE", "").strip().lower()
APP_ENV = os.getenv("APP_ENV", "production")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).with_name("coliving.db")))
RESIDENTS_JSON_PATH = os.getenv("RESIDENTS_JSON_PATH", str(Path(__file__).with_name("residents.json")))
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_FILE = os.getenv("LOG_FILE", str(Path(LOG_DIR) / "bot.log"))

BACKUP_ENABLED = os.getenv("BACKUP_ENABLED", "0") == "1"
BACKUP_INTERVAL_HOURS = int(os.getenv("BACKUP_INTERVAL_HOURS", "48"))
BACKUP_LOCAL_DIR = os.getenv("BACKUP_LOCAL_DIR", "backups")
BACKUP_INCLUDE_ENV = os.getenv("BACKUP_INCLUDE_ENV", "0") == "1"
BACKUP_KEEP_COUNT = int(os.getenv("BACKUP_KEEP_COUNT", "10"))
BACKUP_DESTINATION = os.getenv("BACKUP_DESTINATION", "both").strip().lower()

WEBHOOK_ENABLED = os.getenv("WEBHOOK_ENABLED", "0") == "1"
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "0.0.0.0")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8080"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "")


def is_webhook_mode() -> bool:
    if BOT_MODE in {"polling", "webhook"}:
        return BOT_MODE == "webhook"
    return WEBHOOK_ENABLED


def validate_config() -> None:
    missing: list[str] = []
    if not TOKEN:
        missing.append("TOKEN")
    if not GROUP_ID:
        missing.append("GROUP_ID")
    if not ADMIN_ID:
        missing.append("ADMIN_ID")

    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(
            "Не заповнено конфігурацію. Створіть .env на основі .env.example "
            f"і вкажіть: {joined}."
        )

    if BOT_MODE and BOT_MODE not in {"polling", "webhook"}:
        raise RuntimeError("BOT_MODE має бути або 'polling', або 'webhook'.")

    if BACKUP_DESTINATION not in {"local", "admin", "both"}:
        raise RuntimeError("BACKUP_DESTINATION має бути 'local', 'admin' або 'both'.")

    if is_webhook_mode() and not WEBHOOK_BASE_URL:
        raise RuntimeError("Для webhook-режиму потрібно заповнити WEBHOOK_BASE_URL.")
