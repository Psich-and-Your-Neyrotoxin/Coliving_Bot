from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import ADMIN_ID
from database import Database
from permissions import PERM_REPORTS_REVIEW, is_owner_id, list_permission_user_ids


REPORT_REVIEW_ROUTE_KEY = "report_review_route"
REPORT_ROUTE_OWNER_ONLY = "owner_only"
REPORT_ROUTE_REVIEWERS_ONLY = "reviewers_only"
REPORT_ROUTE_OWNER_AND_REVIEWERS = "owner_and_reviewers"

REPORT_ROUTE_LABELS = {
    REPORT_ROUTE_OWNER_ONLY: "Тільки власнику",
    REPORT_ROUTE_REVIEWERS_ONLY: "Тільки заступникам із правом перевірки",
    REPORT_ROUTE_OWNER_AND_REVIEWERS: "Власнику і заступникам із правом перевірки",
}

CRITICAL_DELEGATE_ACTION_LABELS = {
    "toggle_test_mode": "змінив тестовий режим",
    "delegate_permission_on": "увімкнув право заступнику",
    "delegate_permission_off": "вимкнув право заступнику",
    "delegate_permissions_clear": "забрав усі права у заступника",
    "add_skip_reminder_dates": "додав пропуск нагадувань",
    "remove_skip_reminder_date": "видалив дату пропуску нагадувань",
    "clear_skip_reminder_dates": "очистив пропуски нагадувань",
    "set_calendar_exception": "додав виняток у календар",
    "remove_calendar_exception": "видалив виняток із календаря",
    "deadline_extend_custom": "продовжив персональний дедлайн",
    "deadline_no_fine": "скасував штраф за дедлайн",
    "clear_deadline_user_override": "скасував персональний дедлайн",
    "clear_deadline_waive": "скасував режим без штрафу",
    "update_job_time": "змінив час нагадування",
    "update_deadline": "змінив час дедлайну",
    "update_bank_url": "оновив посилання на банку",
    "update_payment_folder": "оновив папку для оплати",
    "manual_override": "змінив ручне чергування",
    "clear_manual_override": "скасував ручне чергування",
    "test_override_set": "змінив тестове чергування",
    "test_override_clear": "скасував тестове чергування",
    "restore_backup": "відновив бекап",
    "issue_fine": "виписав штраф",
    "fine_after_reject": "виписав штраф після відхилення звіту",
    "deadline_bank_fine": "виписав грошовий штраф за дедлайн",
    "deadline_text_fine": "виписав текстовий штраф за дедлайн",
}


async def get_report_review_route(db: Database) -> str:
    value = str(await db.get_setting(REPORT_REVIEW_ROUTE_KEY, REPORT_ROUTE_OWNER_AND_REVIEWERS) or "").strip()
    if value not in REPORT_ROUTE_LABELS:
        return REPORT_ROUTE_OWNER_AND_REVIEWERS
    return value


async def get_report_review_recipient_ids(db: Database) -> list[int]:
    reviewer_ids = await list_permission_user_ids(db, PERM_REPORTS_REVIEW, include_owner=False)
    route = await get_report_review_route(db)
    if route == REPORT_ROUTE_OWNER_ONLY:
        return [int(ADMIN_ID)]
    if route == REPORT_ROUTE_REVIEWERS_ONLY:
        return reviewer_ids or [int(ADMIN_ID)]
    recipient_ids = {int(ADMIN_ID), *reviewer_ids}
    return sorted(recipient_ids)


async def get_report_review_watcher_ids(db: Database, actor_id: int) -> list[int]:
    watcher_ids = set(await list_permission_user_ids(db, PERM_REPORTS_REVIEW, include_owner=True))
    watcher_ids.discard(int(actor_id))
    return sorted(watcher_ids)


def report_review_route_kb(current_route: str) -> InlineKeyboardMarkup:
    rows = []
    for route, label in REPORT_ROUTE_LABELS.items():
        marker = "✅" if route == current_route else "☑️"
        rows.append([InlineKeyboardButton(text=f"{marker} {label}", callback_data=f"report_route:set:{route}")])
    rows.append([InlineKeyboardButton(text="⬅️ До системи", callback_data="admin:section_system")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def should_notify_owner_about_delegate_action(action_type: str) -> bool:
    return str(action_type) in CRITICAL_DELEGATE_ACTION_LABELS


async def notify_owner_about_delegate_action(
    bot,
    db: Database,
    *,
    actor_id: int,
    action_type: str,
    details: str = "",
    target_id: int | None = None,
) -> None:
    if is_owner_id(actor_id) or not should_notify_owner_about_delegate_action(action_type):
        return

    actor = await db.get_resident(int(actor_id))
    target = await db.get_resident(int(target_id)) if target_id else None
    actor_name = (actor or {}).get("full_name") or f"ID {actor_id}"
    target_name = (target or {}).get("full_name") or (f"ID {target_id}" if target_id else "—")
    action_label = CRITICAL_DELEGATE_ACTION_LABELS.get(str(action_type), str(action_type))

    lines = [
        "👀 <b>Дія заступника</b>",
        "",
        f"<b>{actor_name}</b> {action_label}.",
    ]
    if target_id:
        lines.append(f"Ціль: <b>{target_name}</b>.")
    if details:
        lines.append(f"Деталі: <code>{details}</code>")

    try:
        await bot.send_message(chat_id=int(ADMIN_ID), text="\n".join(lines))
    except Exception:
        pass
