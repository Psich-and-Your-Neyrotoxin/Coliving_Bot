from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from html import escape
from zoneinfo import ZoneInfo

from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message

from database import Database
from deadline_policy import get_deadline_due_at, get_deadline_due_at_for_user
from instance_config import (
    LEGACY_ZONE_CODE_BY_NAME,
    LEGACY_ZONE_NAME_BY_CODE,
    get_legacy_zone_from_definition,
    is_zone_report_day,
    load_instance_definition,
)
from logic import DutyAssignment, calculate_assignment
from runtime_schedule import (
    apply_legacy_swaps_to_assignments,
    apply_zone_overrides_to_assignments,
    build_zone_assignments,
    get_assigned_zone_codes_for_user,
)


KYIV_TZ = ZoneInfo("Europe/Kyiv")
SUBMIT_REPORT_BUTTON = "📸 Здати звіт"
CALENDAR_EXCEPTIONS_KEY = "calendar_exceptions_json"
DEADLINE_DEFAULTS_KEY = "deadline_defaults_json"
REPORT_LOOKBACK_DAYS = 14
LEGACY_ZONE_LABELS = {"Kitchen": "Кухня", "Bath": "Ванна", "General": "Общак"}

SPECIAL_NAME_MAP = {
    "yaro": "Ярослав Шарга",
    "~yaro~": "Ярослав Шарга",
    "yaroslav_sharga": "Ярослав Шарга",
    "шарга ярослав євгенович": "Ярослав Шарга",
    "ярослав шарга": "Ярослав Шарга",
    "kolya": "Микола Васюк",
    "коля": "Микола Васюк",
}


def kyiv_now() -> datetime:
    return datetime.now(KYIV_TZ)


def kyiv_today():
    return kyiv_now().date()


def is_within_late_report_window(now: datetime | None = None) -> bool:
    current = now or kyiv_now()
    return (current.hour, current.minute) <= (1, 0)


def _normalized_key(value: str | None) -> str:
    return " ".join((value or "").strip().lower().replace("~", "").split())


def canonical_full_name(full_name: str | None, username: str | None = None) -> str:
    username_key = _normalized_key(username)
    if username_key in SPECIAL_NAME_MAP:
        return SPECIAL_NAME_MAP[username_key]

    name_key = _normalized_key(full_name)
    if name_key in SPECIAL_NAME_MAP:
        return SPECIAL_NAME_MAP[name_key]

    parts = [part for part in (full_name or "").split() if part]
    if len(parts) >= 3:
        return f"{parts[1]} {parts[0]}"
    if len(parts) == 2:
        return f"{parts[1]} {parts[0]}"
    if len(parts) == 1:
        return parts[0]
    return "Користувач"


def zone_label(zone_name: str) -> str:
    return LEGACY_ZONE_LABELS.get(str(zone_name), str(zone_name))


def zone_code_from_identifier(zone_identifier: str) -> str:
    raw = str(zone_identifier)
    return LEGACY_ZONE_CODE_BY_NAME.get(raw, raw)


def zone_identifier_from_code(zone_code: str) -> str:
    raw = str(zone_code)
    return LEGACY_ZONE_NAME_BY_CODE.get(raw, raw)


def parse_user_date(value: str, *, base_date: date | None = None) -> date:
    raw = value.strip().replace("/", ".")
    today = base_date or kyiv_today()

    for fmt in ("%d.%m.%Y", "%d.%m"):
        try:
            parsed = datetime.strptime(raw, fmt)
            if fmt == "%d.%m":
                parsed = parsed.replace(year=today.year)
            return parsed.date()
        except ValueError:
            continue

    raise ValueError("Bad date")


def parse_date_period(value: str, *, base_date: date | None = None) -> tuple[date, date]:
    today = base_date or kyiv_today()
    normalized = value.replace("—", "-").replace("–", "-")
    parts = [part.strip() for part in normalized.split("-", 1)]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Bad period")

    start = parse_user_date(parts[0], base_date=today)
    end = parse_user_date(parts[1], base_date=today)
    if end < start:
        raise ValueError("Bad period")
    return start, end


def format_resident_mention(resident: dict | None, fallback_name: str | None = None) -> str:
    full_name = canonical_full_name(fallback_name)
    if resident and resident.get("full_name"):
        full_name = canonical_full_name(str(resident["full_name"]), resident.get("username"))
    escaped_name = escape(full_name or "Користувач")
    if resident and resident.get("telegram_id"):
        return f'<a href="tg://user?id={int(resident["telegram_id"])}">{escaped_name}</a>'
    return f"<b>{escaped_name}</b>"


def format_resident_name_with_tag(resident: dict | None, fallback_name: str | None = None) -> str:
    full_name = canonical_full_name(fallback_name)
    if resident and resident.get("full_name"):
        full_name = canonical_full_name(str(resident["full_name"]), resident.get("username"))

    escaped_name = f"<b>{escape(full_name or 'Користувач')}</b>"
    if resident and resident.get("username"):
        username = str(resident["username"]).lstrip("@")
        return f'{escaped_name} (<a href="https://t.me/{escape(username)}">@{escape(username)}</a>)'
    return escaped_name


def format_resident_name_with_username_text(resident: dict | None, fallback_name: str | None = None) -> str:
    full_name = canonical_full_name(fallback_name)
    if resident and resident.get("full_name"):
        full_name = canonical_full_name(str(resident["full_name"]), resident.get("username"))

    if resident and resident.get("username"):
        username = str(resident["username"]).lstrip("@")
        return f"{escape(full_name or 'Користувач')} @{escape(username)}"
    return escape(full_name or "Користувач")


def format_resident_name_plain(resident: dict | None, fallback_name: str | None = None) -> str:
    full_name = canonical_full_name(fallback_name)
    if resident and resident.get("full_name"):
        full_name = canonical_full_name(str(resident["full_name"]), resident.get("username"))
    return escape(full_name or "Користувач")


def is_admin_id(user_id: int) -> bool:
    from config import ADMIN_ID

    return str(user_id) == str(ADMIN_ID)


async def is_test_mode_enabled(db: Database) -> bool:
    return (await db.get_setting("test_mode", "0")) == "1"


async def is_test_user(db: Database, telegram_id: int) -> bool:
    return await is_test_mode_enabled(db) and await db.is_test_whitelisted(int(telegram_id))


async def list_calendar_exceptions(db: Database) -> list[dict]:
    raw = await db.get_setting(CALENDAR_EXCEPTIONS_KEY, "[]")
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return []
    items: list[dict] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            continue
        iso_date = str(entry.get("date") or "").strip()
        kind = str(entry.get("kind") or "").strip()
        note = str(entry.get("note") or "").strip()
        if not iso_date or kind not in {"holiday", "day_off", "special_rules"}:
            continue
        items.append({"date": iso_date, "kind": kind, "note": note})
    return sorted(items, key=lambda item: (item["date"], item["kind"]))


async def get_calendar_exception(db: Database, target_date: date) -> dict | None:
    iso_value = target_date.isoformat()
    for entry in await list_calendar_exceptions(db):
        if entry["date"] == iso_value:
            return entry
    return None


def calendar_exception_blocks_duties(exception: dict | None) -> bool:
    if not exception:
        return False
    return str(exception.get("kind")) in {"holiday", "day_off"}


async def get_assignment_for_date(db: Database, target_date: date) -> DutyAssignment:
    residents = await db.list_active_residents_full()
    swaps_all = await db.list_swaps_for_date(target_date)
    swaps_for_logic = [
        {"zone": s["zone"], "from_id": int(s["from_id"]), "to_id": int(s["to_id"]), "date": str(s["date"])}
        for s in swaps_all
    ]
    resident_map = {int(resident["telegram_id"]): resident for resident in residents}

    assignment = None
    kitchen_id = 0
    kitchen_name = ""
    bath_id = 0
    bath_name = ""
    general_ids = [0, 0]
    general_names = ["", ""]

    try:
        _, runtime_assignments = await get_runtime_zone_assignments_for_date(db, target_date)
        kitchen = runtime_assignments.get("kitchen")
        bath = runtime_assignments.get("bath")
        general = runtime_assignments.get("general")
        if kitchen and len(kitchen.member_ids) >= 1 and bath and len(bath.member_ids) >= 1 and general and len(general.member_ids) >= 2:
            kitchen_id = int(kitchen.member_ids[0])
            kitchen_name = canonical_full_name(kitchen.member_names[0], resident_map.get(kitchen_id, {}).get("username"))
            bath_id = int(bath.member_ids[0])
            bath_name = canonical_full_name(bath.member_names[0], resident_map.get(bath_id, {}).get("username"))
            general_ids = [int(general.member_ids[0]), int(general.member_ids[1])]
            general_names = [
                canonical_full_name(general.member_names[0], resident_map.get(general_ids[0], {}).get("username")),
                canonical_full_name(general.member_names[1], resident_map.get(general_ids[1], {}).get("username")),
            ]
        else:
            raise ValueError("dynamic legacy adapter requires kitchen/bath/general zones")
    except Exception:
        assignment = calculate_assignment(residents, swaps_for_logic, today=target_date)
        kitchen_id = assignment.kitchen_id
        kitchen_name = assignment.kitchen_name
        bath_id = assignment.bath_id
        bath_name = assignment.bath_name
        general_ids = [assignment.general_ids[0], assignment.general_ids[1]]
        general_names = [assignment.general_names[0], assignment.general_names[1]]

    async def _apply_overrides(overrides: list[dict]) -> None:
        nonlocal kitchen_id, kitchen_name, bath_id, bath_name, general_ids, general_names
        for override in overrides:
            override_id = int(override["telegram_id"])
            resident = resident_map.get(override_id)
            if not resident:
                continue
            resident_name = canonical_full_name(
                override.get("full_name") or (resident["full_name"] if resident else None),
                override.get("username") or (resident.get("username") if resident else None),
            )
            zone_name = str(override["zone_name"])
            slot_index = int(override["slot_index"])
            if zone_name == "Kitchen":
                kitchen_id = override_id
                kitchen_name = resident_name
            elif zone_name == "Bath":
                bath_id = override_id
                bath_name = resident_name
            elif zone_name == "General" and slot_index in (0, 1):
                general_ids[slot_index] = override_id
                general_names[slot_index] = resident_name

    if assignment is not None:
        if await is_test_mode_enabled(db):
            await _apply_overrides(await db.get_test_overrides_for_date(target_date))
        await _apply_overrides(await db.get_manual_overrides_for_date(target_date))

    return DutyAssignment(
        kitchen_id=kitchen_id,
        kitchen_name=kitchen_name,
        bath_id=bath_id,
        bath_name=bath_name,
        general_ids=(general_ids[0], general_ids[1]),
        general_names=(general_names[0], general_names[1]),
    )


async def get_runtime_zone_assignments_for_date(db: Database, target_date: date) -> tuple[object, dict[str, object]]:
    residents = await db.list_active_residents_full()
    resident_map = {int(resident["telegram_id"]): resident for resident in residents}
    swaps_all = await db.list_swaps_for_date(target_date)
    swaps_for_logic = [
        {"zone": s["zone"], "from_id": int(s["from_id"]), "to_id": int(s["to_id"]), "date": str(s["date"])}
        for s in swaps_all
    ]
    definition = await load_instance_definition(db)
    assignments = build_zone_assignments(definition, resident_map, target_date)
    assignments = apply_legacy_swaps_to_assignments(assignments, resident_map, swaps_for_logic)
    if await is_test_mode_enabled(db):
        assignments = apply_zone_overrides_to_assignments(
            assignments,
            resident_map,
            await db.get_test_overrides_for_date(target_date),
        )
    assignments = apply_zone_overrides_to_assignments(
        assignments,
        resident_map,
        await db.get_manual_overrides_for_date(target_date),
    )
    return definition, assignments


async def get_runtime_zone_titles(db: Database) -> dict[str, str]:
    definition = await load_instance_definition(db)
    titles = {zone.code: zone.title for zone in definition.zones}
    for zone_code, legacy_name in LEGACY_ZONE_NAME_BY_CODE.items():
        titles[legacy_name] = titles.get(zone_code, zone_label(legacy_name))
    return titles


async def get_zone_title(db: Database, zone_identifier: str) -> str:
    titles = await get_runtime_zone_titles(db)
    return titles.get(str(zone_identifier), zone_label(zone_identifier))


async def list_enabled_zone_choices(db: Database, *, report_required_only: bool = False) -> list[tuple[str, str]]:
    definition = await load_instance_definition(db)
    rows: list[tuple[str, str]] = []
    for zone in sorted(definition.zones, key=lambda item: item.sort_order):
        if not zone.enabled or not zone.rotation_enabled:
            continue
        if report_required_only and not zone.report_required:
            continue
        rows.append((zone_identifier_from_code(zone.code), zone.title))
    return rows


async def get_zone_assignment_for_date(db: Database, zone_identifier: str, target_date: date) -> tuple[str, tuple[int, ...], tuple[str, ...]] | None:
    definition, runtime_assignments = await get_runtime_zone_assignments_for_date(db, target_date)
    zone_code = zone_code_from_identifier(zone_identifier)
    assignment = runtime_assignments.get(zone_code)
    if not assignment:
        return None
    zone_title = next((zone.title for zone in definition.zones if zone.code == zone_code), zone_label(zone_identifier))
    return zone_title, assignment.member_ids, assignment.member_names


async def get_user_report_zones(db: Database, user_id: int, target_date: date) -> set[str]:
    if calendar_exception_blocks_duties(await get_calendar_exception(db, target_date)):
        return set()
    definition, runtime_assignments = await get_runtime_zone_assignments_for_date(db, target_date)
    if is_admin_id(user_id) and await is_test_mode_enabled(db):
        zones = {
            zone_identifier_from_code(zone.code)
            for zone in definition.zones
            if zone.enabled and zone.report_required
        }
        return zones or {"Kitchen", "Bath", "General"}

    matched_codes = get_assigned_zone_codes_for_user(
        definition,
        runtime_assignments,
        int(user_id),
        report_day_predicate=lambda zone: is_zone_report_day(zone, target_date),
    )
    zones = {zone_identifier_from_code(code) for code in matched_codes}

    if await is_test_user(db, user_id):
        overrides = await db.get_test_overrides_for_date(target_date)
        explicit_zones = {
            str(override["zone_name"])
            for override in overrides
            if int(override["telegram_id"]) == int(user_id)
        }
        if explicit_zones:
            return explicit_zones

    return zones

async def get_user_report_targets(db: Database, user_id: int, now: datetime | None = None) -> dict[str, date]:
    current = now or kyiv_now()
    today = current.date()
    targets: dict[str, date] = {}

    for days_back in range(REPORT_LOOKBACK_DAYS + 1):
        candidate_date = today - timedelta(days=days_back)
        candidate_zones = await get_user_report_zones(db, user_id, candidate_date)
        for zone in candidate_zones:
            if zone in targets:
                continue
            due_at = await get_deadline_due_at_for_user(db, zone, candidate_date, user_id)
            if current <= due_at:
                targets[zone] = candidate_date

    return targets


async def get_user_report_options(db: Database, user_id: int, now: datetime | None = None) -> list[tuple[str, date]]:
    current = now or kyiv_now()
    today = current.date()
    options: list[tuple[str, date]] = []

    for days_back in range(REPORT_LOOKBACK_DAYS + 1):
        candidate_date = today - timedelta(days=days_back)
        candidate_zones = await get_user_report_zones(db, user_id, candidate_date)
        for zone in sorted(candidate_zones):
            due_at = await get_deadline_due_at_for_user(db, zone, candidate_date, user_id)
            if current <= due_at:
                options.append((zone, candidate_date))

    return options


async def require_resident(message: Message, db: Database) -> dict | None:
    user = message.from_user
    if not user:
        await message.answer("Доступ обмежено")
        return None

    await db.touch_user_contact(int(user.id))

    if is_admin_id(user.id):
        resident = await db.get_resident(user.id)
        if resident:
            await db.update_resident_profile(
                user.id,
                str(resident["full_name"]),
                user.username,
            )
            resident = await db.get_resident(user.id)
        return resident or {
            "telegram_id": user.id,
            "full_name": canonical_full_name(user.full_name or "Адмін", user.username),
            "role": "admin",
            "is_active": 1,
            "username": user.username,
        }

    resident = await db.get_resident(user.id)
    if not resident:
        await message.answer("Доступ обмежено")
        return None
    if not resident.get("is_active") and not await is_test_user(db, user.id):
        await message.answer("Доступ обмежено")
        return None
    await db.update_resident_profile(
        user.id,
        str(resident["full_name"]),
        user.username,
    )
    resident = await db.get_resident(user.id)
    return resident


async def refresh_section_message(
    callback: CallbackQuery,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup | None,
) -> bool:
    """Send a fresh section message and remove the previous one when possible."""
    if not callback.message:
        return False
    try:
        await callback.message.answer(text, reply_markup=reply_markup)
        try:
            await callback.message.delete()
        except Exception:
            pass
        return True
    except Exception:
        return False


def is_admin(telegram_id: int) -> bool:
    from config import ADMIN_ID

    return int(telegram_id) == int(ADMIN_ID)
