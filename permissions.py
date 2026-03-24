from __future__ import annotations

import json

from config import ADMIN_ID
from database import Database


PERM_REPORTS_REVIEW = "reports_review"
PERM_HISTORY_VIEW = "history_view"
PERM_REMINDERS_MANAGE = "reminders_manage"
PERM_SCHEDULE_MANAGE = "schedule_manage"
PERM_EXCEPTIONS_MANAGE = "exceptions_manage"
PERM_FINES_MANAGE = "fines_manage"
PERM_PAYMENTS_MANAGE = "payments_manage"
PERM_BACKUPS_MANAGE = "backups_manage"
PERM_SYSTEM_VIEW = "system_view"
PERM_TEST_MODE_MANAGE = "test_mode_manage"
PERM_DELEGATES_MANAGE = "delegates_manage"

DELEGATE_PERMISSIONS_PREFIX = "admin_delegate_permissions:"

ALL_PERMISSIONS = [
    PERM_REPORTS_REVIEW,
    PERM_HISTORY_VIEW,
    PERM_REMINDERS_MANAGE,
    PERM_SCHEDULE_MANAGE,
    PERM_EXCEPTIONS_MANAGE,
    PERM_FINES_MANAGE,
    PERM_PAYMENTS_MANAGE,
    PERM_BACKUPS_MANAGE,
    PERM_SYSTEM_VIEW,
    PERM_TEST_MODE_MANAGE,
    PERM_DELEGATES_MANAGE,
]

PERMISSION_LABELS = {
    PERM_REPORTS_REVIEW: "Перевірка звітів",
    PERM_HISTORY_VIEW: "Історія і статистика",
    PERM_REMINDERS_MANAGE: "Нагадування",
    PERM_SCHEDULE_MANAGE: "Графік і час",
    PERM_EXCEPTIONS_MANAGE: "Винятки і дедлайни",
    PERM_FINES_MANAGE: "Штрафи",
    PERM_PAYMENTS_MANAGE: "Оплати і банка",
    PERM_BACKUPS_MANAGE: "Бекапи і відновлення",
    PERM_SYSTEM_VIEW: "Системний огляд і логи",
    PERM_TEST_MODE_MANAGE: "Тестовий режим",
    PERM_DELEGATES_MANAGE: "Керування заступниками",
}


def is_owner_id(user_id: int) -> bool:
    return int(user_id) == int(ADMIN_ID)


def delegate_permissions_key(user_id: int) -> str:
    return f"{DELEGATE_PERMISSIONS_PREFIX}{int(user_id)}"


def parse_delegate_permissions_key(key: str) -> int | None:
    raw = str(key or "").strip()
    if not raw.startswith(DELEGATE_PERMISSIONS_PREFIX):
        return None
    try:
        return int(raw[len(DELEGATE_PERMISSIONS_PREFIX) :])
    except Exception:
        return None


async def get_user_permissions(db: Database, user_id: int) -> set[str]:
    if is_owner_id(user_id):
        return set(ALL_PERMISSIONS)

    raw = await db.get_setting(delegate_permissions_key(user_id), "[]")
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return set()
    return {str(item) for item in parsed if str(item) in ALL_PERMISSIONS}


async def has_permission(db: Database, user_id: int, permission: str) -> bool:
    if is_owner_id(user_id):
        return True
    return permission in await get_user_permissions(db, user_id)


async def can_access_admin_panel(db: Database, user_id: int) -> bool:
    return is_owner_id(user_id) or bool(await get_user_permissions(db, user_id))


async def list_permission_user_ids(db: Database, permission: str, *, include_owner: bool = True) -> list[int]:
    user_ids: set[int] = set()
    if include_owner:
        user_ids.add(int(ADMIN_ID))
    for row in await db.list_settings_by_prefix(DELEGATE_PERMISSIONS_PREFIX):
        user_id = parse_delegate_permissions_key(str(row.get("key") or ""))
        if not user_id:
            continue
        if permission in await get_user_permissions(db, user_id):
            user_ids.add(int(user_id))
    return sorted(user_ids)


async def set_user_permissions(db: Database, user_id: int, permissions: set[str]) -> None:
    normalized = sorted({permission for permission in permissions if permission in ALL_PERMISSIONS})
    if is_owner_id(user_id):
        return
    if normalized:
        await db.set_setting(delegate_permissions_key(user_id), json.dumps(normalized, ensure_ascii=False))
    else:
        await db.delete_setting(delegate_permissions_key(user_id))
