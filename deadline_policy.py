from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from database import Database
from instance_config import get_legacy_zone_from_definition, load_instance_definition


KYIV_TZ = ZoneInfo("Europe/Kyiv")
DEADLINE_DEFAULTS_KEY = "deadline_defaults_json"
DEADLINE_OVERRIDE_PREFIX = "deadline_override:"
DEADLINE_USER_OVERRIDE_PREFIX = "deadline_user_override:"
DEADLINE_WAIVE_PREFIX = "deadline_waive:"


def deadline_override_key(duty_date: date) -> str:
    return f"{DEADLINE_OVERRIDE_PREFIX}{duty_date.isoformat()}"


def deadline_user_override_key(zone: str, duty_date: date, user_id: int) -> str:
    return f"{DEADLINE_USER_OVERRIDE_PREFIX}{duty_date.isoformat()}:{zone}:{int(user_id)}"


def deadline_waive_key(zone: str, duty_date: date, user_id: int) -> str:
    return f"{DEADLINE_WAIVE_PREFIX}{duty_date.isoformat()}:{zone}:{int(user_id)}"


def parse_deadline_user_override_key(key: str) -> tuple[date, str, int] | None:
    raw = str(key or "")
    if not raw.startswith(DEADLINE_USER_OVERRIDE_PREFIX):
        return None
    payload = raw[len(DEADLINE_USER_OVERRIDE_PREFIX) :]
    try:
        duty_date_iso, zone, user_id_raw = payload.split(":", 2)
        return date.fromisoformat(duty_date_iso), zone, int(user_id_raw)
    except Exception:
        return None


def parse_deadline_waive_key(key: str) -> tuple[date, str, int] | None:
    raw = str(key or "")
    if not raw.startswith(DEADLINE_WAIVE_PREFIX):
        return None
    payload = raw[len(DEADLINE_WAIVE_PREFIX) :]
    try:
        duty_date_iso, zone, user_id_raw = payload.split(":", 2)
        return date.fromisoformat(duty_date_iso), zone, int(user_id_raw)
    except Exception:
        return None


def parse_deadline_hhmm(value: str, default: str = "01:00") -> tuple[int, int]:
    raw = (value or default).strip()
    try:
        hh, mm = raw.split(":", 1)
        hour = int(hh)
        minute = int(mm)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        return hour, minute
    except Exception:
        fallback_hh, fallback_mm = default.split(":", 1)
        return int(fallback_hh), int(fallback_mm)


async def get_deadline_defaults(db: Database) -> dict[str, str]:
    raw_defaults = await db.get_setting(DEADLINE_DEFAULTS_KEY, "")
    defaults = {"Kitchen": "01:00", "Bath": "01:00", "General": "01:00"}
    try:
        definition = await load_instance_definition(db)
        for legacy_zone in ("Kitchen", "Bath", "General"):
            zone = get_legacy_zone_from_definition(definition, legacy_zone)
            if zone and zone.report_deadline_time:
                defaults[legacy_zone] = str(zone.report_deadline_time)
    except Exception:
        pass
    try:
        parsed = json.loads(raw_defaults) if raw_defaults else {}
        if isinstance(parsed, dict):
            defaults.update({str(k): str(v) for k, v in parsed.items()})
    except Exception:
        pass
    return defaults


async def get_deadline_due_at(db: Database, zone: str, duty_date: date) -> datetime:
    defaults = await get_deadline_defaults(db)
    raw_override = await db.get_setting(deadline_override_key(duty_date), "")
    overrides: dict[str, str] = {}
    try:
        parsed = json.loads(raw_override) if raw_override else {}
        if isinstance(parsed, dict):
            overrides.update({str(k): str(v) for k, v in parsed.items()})
    except Exception:
        pass

    hhmm = overrides.get(zone) or defaults.get(zone) or "01:00"
    hour, minute = parse_deadline_hhmm(hhmm)
    due_day = duty_date if hour >= 12 else duty_date + timedelta(days=1)
    return datetime.combine(due_day, time(hour=hour, minute=minute), tzinfo=KYIV_TZ)


async def get_deadline_due_at_for_user(db: Database, zone: str, duty_date: date, user_id: int) -> datetime:
    raw_override = (await db.get_setting(deadline_user_override_key(zone, duty_date, user_id), "")) or ""
    if raw_override.strip():
        try:
            parsed = datetime.fromisoformat(raw_override.strip())
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=KYIV_TZ)
            return parsed.astimezone(KYIV_TZ)
        except Exception:
            pass
    return await get_deadline_due_at(db, zone, duty_date)
