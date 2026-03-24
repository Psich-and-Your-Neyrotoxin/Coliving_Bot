from __future__ import annotations

import json
import logging
import re
from io import BytesIO
from io import StringIO
import csv
from dataclasses import replace
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BufferedInputFile, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Message

from config import ADMIN_ID, APP_ENV, APP_VERSION, DB_PATH, GROUP_ID, LOG_FILE, RESIDENTS_JSON_PATH
from admin_alerts import (
    REPORT_REVIEW_ROUTE_KEY,
    REPORT_ROUTE_LABELS,
    get_report_review_route,
    notify_owner_about_delegate_action,
    report_review_route_kb,
)
from database import Database
from database import REPORT_RETENTION_DAYS
from deadline_policy import (
    DEADLINE_USER_OVERRIDE_PREFIX,
    DEADLINE_WAIVE_PREFIX,
    deadline_user_override_key,
    deadline_waive_key,
    get_deadline_due_at_for_user,
    parse_deadline_user_override_key,
    parse_deadline_waive_key,
)
from excel_schedule import Record as ExcelRecord
from excel_schedule import build_xlsx_bytes
from handlers.common import (
    CALENDAR_EXCEPTIONS_KEY,
    SUBMIT_REPORT_BUTTON,
    canonical_full_name,
    format_resident_mention,
    get_runtime_zone_assignments_for_date,
    get_zone_assignment_for_date,
    is_test_mode_enabled,
    kyiv_today,
    list_enabled_zone_choices,
    parse_date_period,
    parse_user_date,
    refresh_section_message,
    require_resident,
    zone_code_from_identifier,
    get_zone_title,
    zone_identifier_from_code,
)
from handlers.fines import ADMIN_FINE_BUTTON, start_admin_fine_flow
from handlers.duty import admin_moderation_kb
from instance_config import (
    InstanceDefinition,
    ZoneDefinition,
    default_feature_flags,
    instance_bundle_from_dict,
    instance_bundle_to_dict,
    load_instance_definition,
    store_instance_definition,
)
from permissions import (
    ALL_PERMISSIONS,
    PERM_BACKUPS_MANAGE,
    PERM_DELEGATES_MANAGE,
    PERM_EXCEPTIONS_MANAGE,
    PERM_FINES_MANAGE,
    PERM_HISTORY_VIEW,
    PERM_PAYMENTS_MANAGE,
    PERM_REMINDERS_MANAGE,
    PERM_REPORTS_REVIEW,
    PERM_SCHEDULE_MANAGE,
    PERM_SYSTEM_VIEW,
    PERM_TEST_MODE_MANAGE,
    PERMISSION_LABELS,
    can_access_admin_panel,
    get_user_permissions,
    has_permission,
    is_owner_id,
    set_user_permissions,
)
from scheduler import (
    JOB_BATH_PRIVATE,
    JOB_GENERAL_PRIVATE,
    JOB_GROUP_MORNING,
    JOB_KITCHEN_PRIVATE,
    JOB_MONTHLY_PAYMENT_REMINDER,
    REMINDER_SKIP_DATES_KEY,
    send_group_morning_reminder,
)


router = Router(name="admin")


class AdminFSM(StatesGroup):
    choosing_time_job = State()
    entering_time = State()
    choosing_deadline_zone = State()
    entering_deadline_time = State()
    confirming_restore = State()
    entering_bank_url = State()
    entering_export_period = State()
    entering_override_date = State()
    choosing_override_zone = State()
    choosing_override_first_user = State()
    choosing_override_second_user = State()
    confirming_reset_db = State()
    choosing_payment_resident = State()
    entering_payment_folder = State()
    entering_manual_override_date = State()
    choosing_manual_override_zone = State()
    choosing_manual_override_first_user = State()
    choosing_manual_override_second_user = State()
    entering_skip_reminder_dates = State()
    choosing_calendar_exception_kind = State()
    entering_calendar_exception_value = State()
    choosing_delegate_resident = State()
    entering_setup_coliving_name = State()
    entering_setup_timezone = State()
    entering_setup_group_id = State()
    entering_runtime_zone_code = State()
    entering_runtime_zone_title = State()
    entering_runtime_zone_new_pattern = State()
    entering_runtime_zone_new_every_days = State()
    entering_runtime_zone_new_deadline = State()
    entering_runtime_zone_new_private_time = State()
    entering_runtime_zone_new_members = State()
    entering_runtime_import_json = State()
    entering_runtime_zone_edit_title = State()
    entering_runtime_zone_edit_deadline = State()
    entering_runtime_zone_edit_private_time = State()
    entering_runtime_zone_edit_every_days = State()
    entering_runtime_zone_edit_pattern = State()
    entering_setup_residents = State()
    entering_runtime_zone_edit_members = State()


async def admin_panel_text(db: Database, user_id: int) -> str:
    setup_note = ""
    try:
        runtime_definition = await load_instance_definition(db)
        if is_owner_id(user_id) and not runtime_definition.settings.setup_complete:
            setup_note = "\n\n🧩 <b>Setup mode:</b> ще не завершено. Базові runtime-настройки варто пройти через wizard."
    except Exception:
        setup_note = ""
    permissions = await get_user_permissions(db, user_id)
    if is_owner_id(user_id):
        role_line = "Роль: <b>власник</b>"
        subtitle = "Повний доступ до всіх розділів."
    elif permissions:
        labels = [PERMISSION_LABELS[item] for item in ALL_PERMISSIONS if item in permissions]
        role_line = "Роль: <b>заступник</b>"
        subtitle = "Доступні права: " + ", ".join(labels)
    else:
        role_line = "Роль: <b>заступник</b>"
        subtitle = "Немає доступних дій."
    return f"🛠️ <b>Панель керування</b>\n\n{role_line}\n{subtitle}{setup_note}\n\nОберіть потрібний блок нижче."


def _admin_section_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ До розділів", callback_data="admin:back_to_panel")]]
    )


async def admin_help_text(db: Database) -> str:
    test_mode = await is_test_mode_enabled(db)
    test_mode_text = "увімкнено" if test_mode else "вимкнено"
    runtime_definition = await load_instance_definition(db)
    reminder_zone_titles = [
        zone.title
        for zone in sorted(runtime_definition.zones, key=lambda item: item.sort_order)
        if zone.enabled and zone.rotation_enabled and zone.private_reminder_enabled
    ]
    lines = [
        "ℹ️ <b>Довідка по панелі керування</b>",
        "",
        "Ось що робить кожна кнопка в адмін-панелі:",
        "",
        "📢 <b>Оголошення в групу зараз</b> — надсилає в групу повідомлення з графіком на сьогодні.",
        "📬 <b>Надіслати нагадування зараз</b> — одразу надсилає приватні нагадування всім сьогоднішнім черговим.",
        "💳 <b>Нагадування про оплату зараз</b> — надсилає мешканцям щомісячне нагадування з їхньою Google-папкою для квитанції.",
        "👤 <b>Статус контактів</b> — показує, хто вже почав діалог із ботом і кому бот може писати в ПП.",
        (
            "🔔 <b>Нагадування по зонах</b> — вручну надсилає окреме нагадування по активній зоні."
            if reminder_zone_titles
            else "🔔 <b>Нагадування по зонах</b> — з'являється, коли є активні runtime-зони з приватними нагадуваннями."
        ),
        "🗂 <b>Бекап зараз</b> — створює архів бази та надсилає його адміну в ПП.",
        "🧾 <b>Історія звітів</b> — показує останні подані, підтверджені та відхилені звіти.",
        "🔄 <b>Історія обмінів</b> — показує останні запити на обмін і їхній статус.",
        "📊 <b>Статистика</b> — коротка зведена статистика по звітах.",
        "🕘 <b>Лог дій</b> — журнал дій власника і заступників: підтвердження, відхилення, штрафи та інше.",
        f"🧪 <b>Тестовий режим</b> — зараз {test_mode_text}. У тестовому режимі можна окремо призначати тестові черги й тестувальників.",
        "⚖️ <b>Керування штрафами</b> — виписування штрафів і перегляд штрафних сценаріїв.",
        "📤 <b>Експорт таблиці чергувань</b> — формує Excel-файл за вказаний період.",
        "⏰ <b>Налаштувати час</b> — змінює час групового й приватних нагадувань, а також дедлайни звітів.",
        "🚫 <b>Пропуск нагадувань</b> — вимикає всі планові нагадування на вибрані дати без зміни самого розкладу.",
        "🗓 <b>Календар винятків</b> — задає свята, дні без чергувань і дні зі зміненими правилами.",
        "🕒 <b>Персональні дедлайни</b> — показує індивідуальні продовження дедлайнів і кейси без штрафу.",
        "🧾 <b>Маршрут звітів</b> — визначає, кому приходять нові звіти на перевірку: власнику, заступникам або всім разом.",
        "💳 <b>Папки оплат</b> — дозволяє прив'язати до кожного мешканця персональну Google-папку для квитанцій.",
        "🏦 <b>Змінити банку</b> — оновлює посилання на банку для грошових штрафів.",
    ]
    if test_mode:
        lines.extend(
            [
                "👥 <b>Тестувальники</b> — додає або прибирає мешканців із тестового доступу.",
                "🎯 <b>Тестові черги</b> — задає тимчасові тестові призначення на конкретну дату.",
            ]
        )
    return "\n".join(lines)


async def _has_actions_section(db: Database, user_id: int) -> bool:
    return any(
        [
            await has_permission(db, user_id, PERM_REMINDERS_MANAGE),
            await has_permission(db, user_id, PERM_SCHEDULE_MANAGE),
            await has_permission(db, user_id, PERM_FINES_MANAGE),
            await has_permission(db, user_id, PERM_TEST_MODE_MANAGE),
        ]
    )


async def _has_history_section(db: Database, user_id: int) -> bool:
    return any(
        [
            await has_permission(db, user_id, PERM_REPORTS_REVIEW),
            await has_permission(db, user_id, PERM_HISTORY_VIEW),
        ]
    )


async def _has_system_section(db: Database, user_id: int) -> bool:
    return any(
        [
            await has_permission(db, user_id, PERM_SYSTEM_VIEW),
            await has_permission(db, user_id, PERM_BACKUPS_MANAGE),
            await has_permission(db, user_id, PERM_SCHEDULE_MANAGE),
            await has_permission(db, user_id, PERM_EXCEPTIONS_MANAGE),
            await has_permission(db, user_id, PERM_PAYMENTS_MANAGE),
            await has_permission(db, user_id, PERM_DELEGATES_MANAGE),
            is_owner_id(user_id),
        ]
    )


def _back_to_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back_to_panel")]]
    )


async def _remember_admin_message(state: FSMContext, message: Message) -> None:
    await state.update_data(admin_chat_id=message.chat.id, admin_message_id=message.message_id)


async def _edit_admin_message(
    state: FSMContext,
    bot,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup | None,
) -> None:
    data = await state.get_data()
    chat_id = data.get("admin_chat_id")
    message_id = data.get("admin_message_id")
    if not chat_id or not message_id:
        return
    try:
        await bot.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(message_id),
            text=text,
            reply_markup=reply_markup,
        )
    except Exception:
        return


async def _safe_edit_callback_message(
    callback: CallbackQuery,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup | None,
) -> bool:
    if not callback.message:
        return False
    try:
        await callback.message.edit_text(text, reply_markup=reply_markup)
        return True
    except TelegramBadRequest:
        return False


async def _safe_refresh_callback_message(
    callback: CallbackQuery,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup | None,
) -> bool:
    ok = await refresh_section_message(callback, text=text, reply_markup=reply_markup)
    if ok:
        return True
    return await _safe_edit_callback_message(callback, text=text, reply_markup=reply_markup)


async def _cleanup_user_input(message: Message) -> None:
    try:
        await message.delete()
    except Exception:
        return


def _permission_for_admin_action(action: str) -> str | None:
    if action.startswith("remind_zone:"):
        return PERM_REMINDERS_MANAGE
    if action in {
        "group_now",
        "send_reminders_now",
        "payment_now",
    }:
        return PERM_REMINDERS_MANAGE
    if action in {"manual_override", "set_time", "export_duty"}:
        return PERM_SCHEDULE_MANAGE
    if action in {"skip_reminders", "calendar_exceptions", "deadline_controls"}:
        return PERM_EXCEPTIONS_MANAGE
    if action in {"payment_folders", "bank_url"}:
        return PERM_PAYMENTS_MANAGE
    if action in {"backup_now", "restore_menu"}:
        return PERM_BACKUPS_MANAGE
    if action in {"health", "version", "error_log", "runtime_config", "runtime_flags", "runtime_zones"}:
        return PERM_SYSTEM_VIEW
    if action in {"test_mode", "test_whitelist", "test_override"}:
        return PERM_TEST_MODE_MANAGE
    if action == "manage_fines":
        return PERM_FINES_MANAGE
    if action in {"contact_status", "report_history", "view_report"}:
        return PERM_REPORTS_REVIEW
    if action in {"swap_history", "stats", "action_log", "history_exports"}:
        return PERM_HISTORY_VIEW
    if action == "delegates":
        return PERM_DELEGATES_MANAGE
    return None


async def admin_kb(db: Database, user_id: int, *, include_back: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    top_row: list[InlineKeyboardButton] = []
    if await _has_actions_section(db, user_id):
        top_row.append(InlineKeyboardButton(text="⚡ Дії", callback_data="admin:section_actions"))
    if await _has_history_section(db, user_id):
        top_row.append(InlineKeyboardButton(text="🧾 Історія", callback_data="admin:section_history"))
    if top_row:
        rows.append(top_row)
    bottom_row: list[InlineKeyboardButton] = []
    if await _has_system_section(db, user_id):
        bottom_row.append(InlineKeyboardButton(text="🛠 Система", callback_data="admin:section_system"))
    bottom_row.append(InlineKeyboardButton(text="ℹ️ Допомога", callback_data="admin:help"))
    rows.append(bottom_row)
    if include_back:
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _pair_buttons(buttons: list[InlineKeyboardButton]) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    pending: list[InlineKeyboardButton] = []
    for button in buttons:
        pending.append(button)
        if len(pending) == 2:
            rows.append(pending)
            pending = []
    if pending:
        rows.append(pending)
    return rows


async def _admin_actions_kb(db: Database, user_id: int) -> InlineKeyboardMarkup:
    test_mode = await is_test_mode_enabled(db)
    toggle_label = "🧪 Тестовий режим: увімкнено" if test_mode else "🧪 Тестовий режим: вимкнено"
    runtime_definition = await load_instance_definition(db)
    flat_buttons: list[InlineKeyboardButton] = []
    if await has_permission(db, user_id, PERM_REMINDERS_MANAGE):
        flat_buttons.extend(
            [
                InlineKeyboardButton(text="📢 Група зараз", callback_data="admin:group_now"),
                InlineKeyboardButton(text="📬 Нагадати всім", callback_data="admin:send_reminders_now"),
                InlineKeyboardButton(text="💳 Оплата зараз", callback_data="admin:payment_now"),
            ]
        )
        for zone in sorted(runtime_definition.zones, key=lambda item: item.sort_order):
            if not zone.enabled or not zone.rotation_enabled or not zone.private_reminder_enabled:
                continue
            flat_buttons.append(
                InlineKeyboardButton(text=f"🔔 {zone.title}", callback_data=f"admin:remind_zone:{zone.code}")
            )
    if await has_permission(db, user_id, PERM_SCHEDULE_MANAGE):
        flat_buttons.extend(
            [
                InlineKeyboardButton(text="🔁 Ручна заміна", callback_data="admin:manual_override"),
                InlineKeyboardButton(text="📤 Експорт черг", callback_data="admin:export_duty"),
            ]
        )
    if await has_permission(db, user_id, PERM_FINES_MANAGE):
        flat_buttons.append(InlineKeyboardButton(text=ADMIN_FINE_BUTTON, callback_data="admin:manage_fines"))
    if await has_permission(db, user_id, PERM_TEST_MODE_MANAGE):
        flat_buttons.append(InlineKeyboardButton(text=toggle_label, callback_data="admin:test_mode"))
        if test_mode:
            flat_buttons.extend(
                [
                    InlineKeyboardButton(text="👥 Тестувальники", callback_data="admin:test_whitelist"),
                    InlineKeyboardButton(text="🎯 Тестові черги", callback_data="admin:test_override"),
                ]
            )
    rows = _pair_buttons(flat_buttons)
    rows.extend(_admin_section_back_kb().inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _admin_history_kb(db: Database, user_id: int) -> InlineKeyboardMarkup:
    flat_buttons: list[InlineKeyboardButton] = []
    if await has_permission(db, user_id, PERM_REPORTS_REVIEW):
        flat_buttons.extend(
            [
                InlineKeyboardButton(text="👤 Контакти", callback_data="admin:contact_status"),
                InlineKeyboardButton(text="🧾 Звіти", callback_data="admin:report_history"),
            ]
        )
    if await has_permission(db, user_id, PERM_HISTORY_VIEW):
        flat_buttons.extend(
            [
                InlineKeyboardButton(text="🔄 Обміни", callback_data="admin:swap_history"),
                InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
                InlineKeyboardButton(text="🕘 Лог дій", callback_data="admin:action_log"),
                InlineKeyboardButton(text="📦 Експорти", callback_data="admin:history_exports"),
            ]
        )
    rows = _pair_buttons(flat_buttons)
    rows.append([InlineKeyboardButton(text="⬅️ До розділів", callback_data="admin:back_to_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _admin_system_kb(db: Database, user_id: int) -> InlineKeyboardMarkup:
    version = await db.get_setting("app_version", "1.0.0")
    flat_buttons: list[InlineKeyboardButton] = []
    if await has_permission(db, user_id, PERM_SYSTEM_VIEW):
        flat_buttons.extend(
            [
                InlineKeyboardButton(text=f"🧬 Версія · {version}", callback_data="admin:version"),
                InlineKeyboardButton(text="❤️ Health-check", callback_data="admin:health"),
                InlineKeyboardButton(text="📜 Логи", callback_data="admin:error_log"),
                InlineKeyboardButton(text="⚙️ Runtime config", callback_data="admin:runtime_config"),
            ]
        )
    if is_owner_id(user_id):
        flat_buttons.append(InlineKeyboardButton(text="🧾 Маршрут звітів", callback_data="admin:report_review_route"))
    if await has_permission(db, user_id, PERM_BACKUPS_MANAGE):
        flat_buttons.extend(
            [
                InlineKeyboardButton(text="🗂 Бекап зараз", callback_data="admin:backup_now"),
                InlineKeyboardButton(text="♻️ Відновити", callback_data="admin:restore_menu"),
            ]
        )
    if is_owner_id(user_id):
        flat_buttons.append(InlineKeyboardButton(text="🧨 Очистити БД", callback_data="admin:reset_db_menu"))
    if await has_permission(db, user_id, PERM_SCHEDULE_MANAGE):
        flat_buttons.append(InlineKeyboardButton(text="⏰ Час і дедлайни", callback_data="admin:set_time"))
    if await has_permission(db, user_id, PERM_EXCEPTIONS_MANAGE):
        flat_buttons.extend(
            [
                InlineKeyboardButton(text="🚫 Пропуск дат", callback_data="admin:skip_reminders"),
                InlineKeyboardButton(text="🗓 Винятки", callback_data="admin:calendar_exceptions"),
                InlineKeyboardButton(text="🕒 Дедлайни", callback_data="admin:deadline_controls"),
            ]
        )
    if await has_permission(db, user_id, PERM_PAYMENTS_MANAGE):
        flat_buttons.extend(
            [
                InlineKeyboardButton(text="💳 Папки оплат", callback_data="admin:payment_folders"),
                InlineKeyboardButton(text="🏦 Банка", callback_data="admin:bank_url"),
            ]
        )
    if await has_permission(db, user_id, PERM_DELEGATES_MANAGE):
        flat_buttons.append(InlineKeyboardButton(text="👥 Заступники", callback_data="admin:delegates"))
    rows = _pair_buttons(flat_buttons)
    rows.append([InlineKeyboardButton(text="⬅️ До розділів", callback_data="admin:back_to_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


RUNTIME_FLAG_LABELS = {
    "reports": "Звіти",
    "fines": "Штрафи",
    "payments": "Оплати",
    "swaps": "Обміни",
    "deadlines": "Дедлайни",
    "reminders": "Нагадування",
    "delegates": "Заступники",
    "calendar_exceptions": "Винятки",
}


def _runtime_section_back_kb(target: str = "admin:runtime_config") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=target)]]
    )


def _runtime_bool(value: bool) -> str:
    return "увімкнено" if value else "вимкнено"


def _zone_pattern_label(zone: ZoneDefinition) -> str:
    return ",".join(str(item) for item in zone.rule.team_pattern) or "1"


def _validate_hhmm(value: str) -> None:
    parts = str(value).strip().split(":", 1)
    if len(parts) != 2:
        raise ValueError("Bad time")
    hour = int(parts[0])
    minute = int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("Bad time")


async def _runtime_config_text(db: Database) -> str:
    definition = await load_instance_definition(db)
    residents = await db.list_all_residents_full()
    enabled_zones = [zone.title for zone in definition.zones if zone.enabled]
    enabled_flags = [
        RUNTIME_FLAG_LABELS.get(key, key)
        for key, value in definition.feature_flags.items()
        if value
    ]
    lines = [
        "⚙️ <b>Runtime config</b>",
        "",
        f"Колів: <b>{definition.settings.coliving_name}</b>",
        f"Setup mode: <b>{'завершено' if definition.settings.setup_complete else 'ще триває'}</b>",
        f"Timezone: <b>{definition.settings.timezone}</b>",
        f"Owner ID: <code>{definition.settings.owner_id}</code>",
        f"Group ID: <code>{definition.settings.group_id}</code>",
        f"Мова: <b>{definition.settings.language}</b>",
        f"Мешканців у bundle: <b>{len(residents)}</b>",
        "",
        f"Модулі: <b>{len(enabled_flags)}/{len(definition.feature_flags)}</b> увімкнено",
        ("Активні: " + ", ".join(enabled_flags)) if enabled_flags else "Активні: —",
        "",
        f"Зони: <b>{len(enabled_zones)}/{len(definition.zones)}</b> активні",
        ("Активні зони: " + ", ".join(enabled_zones[:8])) if enabled_zones else "Активні зони: —",
    ]
    if len(enabled_zones) > 8:
        lines.append(f"Ще зон: <b>+{len(enabled_zones) - 8}</b>")
    return "\n".join(lines)


def _runtime_config_kb(definition: InstanceDefinition, *, can_manage: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="🧪 Модулі", callback_data="admin:runtime_flags"),
            InlineKeyboardButton(text="🗂 Зони", callback_data="admin:runtime_zones"),
        ],
        [
            InlineKeyboardButton(text="📤 JSON", callback_data="runtime_export:json"),
            InlineKeyboardButton(text="📤 YAML", callback_data="runtime_export:yaml"),
        ],
        [
            InlineKeyboardButton(text="📥 Імпорт bundle", callback_data="runtime_import:start"),
        ],
    ]
    if can_manage:
        rows.append(
            [
                InlineKeyboardButton(text="🧩 Setup wizard", callback_data="runtime_setup:start"),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text=("✅ Setup завершено" if definition.settings.setup_complete else "🧩 Позначити setup готовим"),
                    callback_data="runtime_setup:toggle",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ До системи", callback_data="admin:section_system")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _runtime_flags_text(db: Database) -> str:
    definition = await load_instance_definition(db)
    lines = ["🧪 <b>Runtime / Модулі</b>", "", "Увімкнення й вимкнення модулів інстансу:", ""]
    for key in default_feature_flags():
        label = RUNTIME_FLAG_LABELS.get(key, key)
        state = "✅" if definition.feature_flags.get(key, False) else "⛔"
        lines.append(f"{state} <b>{label}</b> — {_runtime_bool(definition.feature_flags.get(key, False))}")
    return "\n".join(lines)


def _runtime_flags_kb(
    definition: InstanceDefinition,
    *,
    can_manage: bool,
    back_target: str = "admin:runtime_config",
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if can_manage:
        buttons = [
            InlineKeyboardButton(
                text=f"{'✅' if definition.feature_flags.get(key, False) else '⛔'} {RUNTIME_FLAG_LABELS.get(key, key)}",
                callback_data=f"runtime_flag_toggle:{key}",
            )
            for key in default_feature_flags()
        ]
        rows.extend(_pair_buttons(buttons))
    rows.extend(_runtime_section_back_kb(back_target).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _yaml_available() -> bool:
    try:
        import yaml  # noqa: F401
    except Exception:
        return False
    return True


def _load_bundle_payload(raw: str) -> dict[str, object]:
    text = str(raw).strip()
    if not text:
        raise ValueError("Порожній payload")
    try:
        payload = json.loads(text)
    except Exception:
        if not _yaml_available():
            raise ValueError("JSON не розібрався, а YAML недоступний без PyYAML")
        import yaml

        payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError("Кореневий payload має бути object/map")
    return payload


async def _runtime_zones_text(db: Database) -> str:
    definition = await load_instance_definition(db)
    lines = ["🗂 <b>Runtime / Зони</b>", "", "Усі runtime-зони інстансу:", ""]
    for zone in definition.zones:
        marker = "✅" if zone.enabled else "⛔"
        lines.append(
            f"{marker} <b>{zone.title}</b> · code=<code>{zone.code}</code> · "
            f"pattern=<b>{_zone_pattern_label(zone)}</b> · every <b>{zone.rule.rotation_every_days}</b> дн."
        )
    return "\n".join(lines)


def _runtime_zones_kb(definition: InstanceDefinition, *, back_target: str = "admin:runtime_config") -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(text=f"{'✅' if zone.enabled else '⛔'} {zone.title}", callback_data=f"runtime_zone:{zone.code}")
        for zone in definition.zones
    ]
    rows = _pair_buttons(buttons)
    rows.append([InlineKeyboardButton(text="➕ Додати зону", callback_data="runtime_zone_add:start")])
    rows.extend(_runtime_section_back_kb(back_target).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _runtime_zone_kb(zone: ZoneDefinition, *, can_manage: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if can_manage:
        rows.extend(
            [
                [
                    InlineKeyboardButton(text="✏️ Назва", callback_data=f"runtime_zone_edit:{zone.code}:title"),
                    InlineKeyboardButton(text="⏱ Дедлайн", callback_data=f"runtime_zone_edit:{zone.code}:deadline"),
                ],
                [
                    InlineKeyboardButton(text="🔔 Private час", callback_data=f"runtime_zone_edit:{zone.code}:private_time"),
                    InlineKeyboardButton(text="📆 Крок днів", callback_data=f"runtime_zone_edit:{zone.code}:every_days"),
                ],
                [
                    InlineKeyboardButton(text="👥 Pattern", callback_data=f"runtime_zone_edit:{zone.code}:pattern"),
                    InlineKeyboardButton(text="🧑‍🤝‍🧑 Members", callback_data=f"runtime_zone_edit:{zone.code}:members"),
                ],
                [
                    InlineKeyboardButton(
                        text=("✅ Зона активна" if zone.enabled else "⛔ Зона вимкнена"),
                        callback_data=f"runtime_zone_toggle:{zone.code}:enabled",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text=("🧾 Звіт потрібен" if zone.report_required else "🚫 Без звітів"),
                        callback_data=f"runtime_zone_toggle:{zone.code}:report_required",
                    ),
                    InlineKeyboardButton(
                        text=("🔔 Приватні on" if zone.private_reminder_enabled else "🔕 Приватні off"),
                        callback_data=f"runtime_zone_toggle:{zone.code}:private_reminder_enabled",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text=("📢 Групові on" if zone.group_reminder_enabled else "📢 Групові off"),
                        callback_data=f"runtime_zone_toggle:{zone.code}:group_reminder_enabled",
                    ),
                    InlineKeyboardButton(
                        text=("🔁 Ротація on" if zone.rotation_enabled else "⏸ Ротація off"),
                        callback_data=f"runtime_zone_toggle:{zone.code}:rotation_enabled",
                    ),
                ],
            ]
        )
    rows.extend(_runtime_section_back_kb("admin:runtime_zones").inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _runtime_zone_text(db: Database, zone_code: str) -> str:
    definition = await load_instance_definition(db)
    zone = next((item for item in definition.zones if item.code == zone_code), None)
    if not zone:
        return "Зону не знайдено."
    report_offset_days = (zone.extra_config or {}).get("report_offset_days")
    residents = {int(item["telegram_id"]): item for item in await db.list_active_residents_full()}
    member_ids = list(zone.rule.member_order)
    if not member_ids:
        for group in zone.rule.member_groups:
            member_ids.extend(group)
    member_names = [
        canonical_full_name(
            residents.get(int(member_id), {}).get("full_name"),
            residents.get(int(member_id), {}).get("username"),
        )
        for member_id in member_ids[:8]
    ]
    lines = [
        f"🗂 <b>Зона: {zone.title}</b>",
        "",
        f"Code: <code>{zone.code}</code>",
        f"Стан: <b>{_runtime_bool(zone.enabled)}</b>",
        f"Team mode: <b>{zone.team_size_mode}</b>",
        f"Pattern: <b>{_zone_pattern_label(zone)}</b>",
        f"Крок ротації: <b>{zone.rule.rotation_every_days}</b> дн.",
        f"Звіти: <b>{_runtime_bool(zone.report_required)}</b>",
        f"Дедлайн звіту: <b>{zone.report_deadline_time or '—'}</b>",
        f"Private reminder: <b>{zone.private_reminder_time or '—'}</b> / {_runtime_bool(zone.private_reminder_enabled)}",
        f"Group reminder: <b>{_runtime_bool(zone.group_reminder_enabled)}</b>",
        f"Ротація: <b>{_runtime_bool(zone.rotation_enabled)}</b>",
        f"Report offset days: <b>{report_offset_days if report_offset_days is not None else '—'}</b>",
        f"Учасників у rule: <b>{len(zone.rule.member_order) or len(zone.rule.member_groups)}</b>",
        f"Members: <b>{', '.join(member_names) if member_names else '—'}</b>",
    ]
    if len(member_ids) > 8:
        lines.append(f"Ще учасників: <b>+{len(member_ids) - 8}</b>")
    return "\n".join(lines)


def _runtime_setup_wizard_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ До runtime config", callback_data="admin:runtime_config")]]
    )


async def _runtime_setup_summary_text(db: Database) -> str:
    definition = await load_instance_definition(db)
    enabled_zone_titles = [zone.title for zone in definition.zones if zone.enabled]
    active_residents = await db.list_active_residents_full()
    return (
        "🧩 <b>Setup wizard</b>\n\n"
        f"Назва: <b>{definition.settings.coliving_name}</b>\n"
        f"Timezone: <b>{definition.settings.timezone}</b>\n"
        f"Group ID: <code>{definition.settings.group_id}</code>\n"
        f"Owner ID: <code>{definition.settings.owner_id}</code>\n"
        f"Мова: <b>{definition.settings.language}</b>\n"
        f"Мешканців: <b>{len(active_residents)}</b>\n"
        f"Активні зони: <b>{', '.join(enabled_zone_titles) if enabled_zone_titles else '—'}</b>\n\n"
        "Після базових кроків можеш перейти в runtime-зони й модулі та довести інстанс до потрібної моделі."
    )


def _runtime_setup_summary_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Завести мешканців", callback_data="runtime_setup:residents")],
            [InlineKeyboardButton(text="🗂 Зони, учасники, правила", callback_data="runtime_setup:zones")],
            [InlineKeyboardButton(text="🧪 Налаштувати модулі", callback_data="runtime_setup:flags")],
            [InlineKeyboardButton(text="✅ Позначити setup готовим", callback_data="runtime_setup:toggle")],
            [InlineKeyboardButton(text="⬅️ До runtime config", callback_data="admin:runtime_config")],
        ]
    )


def _runtime_private_job_id(zone_code: str) -> str:
    mapping = {
        "kitchen": JOB_KITCHEN_PRIVATE,
        "bath": JOB_BATH_PRIVATE,
        "general": JOB_GENERAL_PRIVATE,
    }
    return mapping.get(str(zone_code), f"zone_private:{zone_code}")


async def _time_jobs_kb(db: Database) -> InlineKeyboardMarkup:
    definition = await load_instance_definition(db)
    rows = [[InlineKeyboardButton(text="☀️ Група (ранок)", callback_data=f"time_job:{JOB_GROUP_MORNING}")]]
    for zone in sorted(definition.zones, key=lambda item: item.sort_order):
        if not zone.enabled or not zone.rotation_enabled or not zone.private_reminder_enabled:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🔔 {zone.title} (приват)",
                    callback_data=f"time_job:{_runtime_private_job_id(zone.code)}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="💳 Оплата за місяць", callback_data=f"time_job:{JOB_MONTHLY_PAYMENT_REMINDER}")])
    rows.append([InlineKeyboardButton(text="⏳ Дедлайни звітів", callback_data="time_deadlines")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back_to_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _deadline_settings_kb(db: Database) -> InlineKeyboardMarkup:
    raw = await db.get_setting("deadline_defaults_json", "")
    defaults: dict[str, str] = {}
    try:
        parsed = json.loads(raw) if raw else {}
        if isinstance(parsed, dict):
            defaults.update({str(k): str(v) for k, v in parsed.items() if str(v).strip()})
    except Exception:
        pass
    definition = await load_instance_definition(db)
    rows: list[list[InlineKeyboardButton]] = []
    for zone in sorted(definition.zones, key=lambda item: item.sort_order):
        if not zone.enabled or not zone.report_required:
            continue
        current = defaults.get(zone_identifier_from_code(zone.code), zone.report_deadline_time or "—")
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{zone.title} · {current}",
                    callback_data=f"deadline_zone:{zone_identifier_from_code(zone.code)}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:set_time")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _time_settings_text(db: Database) -> str:
    definition = await load_instance_definition(db)
    group_time = await db.get_setting(f"time:{JOB_GROUP_MORNING}", "09:00")
    payment_time = await db.get_setting(f"time:{JOB_MONTHLY_PAYMENT_REMINDER}", "10:00")

    raw = await db.get_setting("deadline_defaults_json", "")
    defaults: dict[str, str] = {}
    try:
        parsed = json.loads(raw) if raw else {}
        if isinstance(parsed, dict):
            defaults.update({str(k): str(v) for k, v in parsed.items() if str(v).strip()})
    except Exception:
        pass

    reminder_lines = [f"• Групове оголошення: <b>{group_time}</b>"]
    for zone in sorted(definition.zones, key=lambda item: item.sort_order):
        if not zone.enabled or not zone.rotation_enabled or not zone.private_reminder_enabled:
            continue
        zone_job_id = _runtime_private_job_id(zone.code)
        zone_time = await db.get_setting(f"time:{zone_job_id}", zone.private_reminder_time or "—")
        reminder_lines.append(f"• {zone.title} (ПП): <b>{zone_time}</b>")
    reminder_lines.append(f"• Оплата за місяць: <b>{payment_time}</b> (1 і 4 число)")

    deadline_lines: list[str] = []
    for zone in sorted(definition.zones, key=lambda item: item.sort_order):
        if not zone.enabled or not zone.report_required:
            continue
        deadline_value = defaults.get(zone_identifier_from_code(zone.code), zone.report_deadline_time or "—")
        deadline_lines.append(f"• {zone.title}: <b>{deadline_value}</b>")
    if not deadline_lines:
        deadline_lines.append("• Немає активних зон зі звітами")

    return (
        "⏰ <b>Налаштування часу</b>\n\n"
        "Поточні значення:\n"
        f"{chr(10).join(reminder_lines)}\n"
        "\n"
        "Дедлайни:\n"
        f"{chr(10).join(deadline_lines)}\n"
        "\n"
        "Оберіть, що хочете змінити."
    )


async def _payment_folders_text(db: Database) -> str:
    residents = await db.list_active_residents_full()
    lines = [
        "💳 <b>Папки оплат</b>",
        "",
        "Оберіть мешканця, щоб додати або змінити його персональну Google-папку для квитанцій.",
        "Поточний статус:",
    ]
    for resident in residents:
        folder_url = ((await db.get_setting(f"payment_folder:{int(resident['telegram_id'])}", "")) or "").strip()
        status = "папка прив'язана" if folder_url else "папка не задана"
        lines.append(f"• {resident['full_name']} — {status}")
    return "\n".join(lines)


async def _report_review_route_text(db: Database) -> str:
    current_route = await get_report_review_route(db)
    return (
        "🧾 <b>Маршрут звітів</b>\n\n"
        "Тут задається, кому бот надсилає нові звіти на перевірку.\n"
        "Власник все одно зберігає повний перегляд через історію звітів.\n\n"
        f"Зараз: <b>{REPORT_ROUTE_LABELS[current_route]}</b>\n\n"
        "Після прийняття або відхилення інші перевіряючі все одно отримають коротке повідомлення,"
        " хто саме вже обробив звіт."
    )


async def _delegates_text(db: Database) -> str:
    residents = await db.list_active_residents_full()
    lines = [
        "👥 <b>Заступники</b>",
        "",
        "Тут можна дати мешканцю доступ лише до конкретних частин адмінки.",
        "Власник завжди має повний доступ і не редагується тут.",
        "",
        "Хто що може зараз:",
    ]
    has_any = False
    for resident in residents:
        user_id = int(resident["telegram_id"])
        if is_owner_id(user_id):
            lines.append(f"• <b>{resident['full_name']}</b> — власник")
            continue
        permissions = await get_user_permissions(db, user_id)
        if permissions:
            has_any = True
            labels = [PERMISSION_LABELS[item] for item in ALL_PERMISSIONS if item in permissions]
            lines.append(f"• <b>{resident['full_name']}</b> — {', '.join(labels)}")
    if not has_any:
        lines.append("Поки що заступників без власника немає.")
    return "\n".join(lines)


async def _delegates_kb(db: Database) -> InlineKeyboardMarkup:
    residents = await db.list_active_residents_full()
    rows: list[list[InlineKeyboardButton]] = []
    for resident in residents:
        user_id = int(resident["telegram_id"])
        if is_owner_id(user_id):
            continue
        permissions = await get_user_permissions(db, user_id)
        prefix = "✅" if permissions else "☑️"
        rows.append(
            [InlineKeyboardButton(text=f"{prefix} {resident['full_name']}", callback_data=f"delegate_pick:{user_id}")]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:section_system")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _delegate_permissions_text(db: Database, user_id: int) -> str:
    resident = await db.get_resident(user_id)
    name = resident["full_name"] if resident else f"ID {user_id}"
    permissions = await get_user_permissions(db, user_id)
    lines = [
        "👤 <b>Права заступника</b>",
        "",
        f"Мешканець: <b>{name}</b>",
        "",
        "Активні права:",
    ]
    if permissions:
        for permission in ALL_PERMISSIONS:
            if permission in permissions:
                lines.append(f"• {PERMISSION_LABELS[permission]}")
    else:
        lines.append("• Немає окремих прав")
    lines.extend(
        [
            "",
            "Увімкни лише потрібні блоки. Повний доступ лишається тільки у власника.",
        ]
    )
    return "\n".join(lines)


async def _delegate_permissions_kb(db: Database, user_id: int) -> InlineKeyboardMarkup:
    permissions = await get_user_permissions(db, user_id)
    rows: list[list[InlineKeyboardButton]] = []
    for permission in ALL_PERMISSIONS:
        marker = "✅" if permission in permissions else "☑️"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{marker} {PERMISSION_LABELS[permission]}",
                    callback_data=f"delegate_toggle:{user_id}:{permission}",
                )
            ]
        )
    if permissions:
        rows.append([InlineKeyboardButton(text="🧹 Забрати всі права", callback_data=f"delegate_clear:{user_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ До заступників", callback_data="admin:delegates")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _load_deadline_controls(db: Database) -> list[dict]:
    items: list[dict] = []
    stale_keys: list[str] = []
    current = datetime.now().astimezone()

    for row in await db.list_settings_by_prefix(DEADLINE_USER_OVERRIDE_PREFIX):
        parsed = parse_deadline_user_override_key(str(row["key"]))
        if not parsed:
            continue
        duty_date, zone, user_id = parsed
        try:
            due_at = await get_deadline_due_at_for_user(db, zone, duty_date, user_id)
        except Exception:
            due_at = None
        if due_at and due_at < current:
            stale_keys.append(str(row["key"]))
            continue
        resident = await db.get_resident(user_id)
        items.append(
            {
                "kind": "override",
                "zone": zone,
                "duty_date": duty_date,
                "user_id": user_id,
                "resident_name": canonical_full_name(
                    (resident or {}).get("full_name"),
                    (resident or {}).get("username"),
                ),
                "value": str(row["value"]),
            }
        )

    for row in await db.list_settings_by_prefix(DEADLINE_WAIVE_PREFIX):
        if str(row["value"]).strip() != "1":
            continue
        parsed = parse_deadline_waive_key(str(row["key"]))
        if not parsed:
            continue
        duty_date, zone, user_id = parsed
        try:
            due_at = await get_deadline_due_at_for_user(db, zone, duty_date, user_id)
        except Exception:
            due_at = None
        if due_at and due_at < current:
            stale_keys.append(str(row["key"]))
            continue
        resident = await db.get_resident(user_id)
        items.append(
            {
                "kind": "waive",
                "zone": zone,
                "duty_date": duty_date,
                "user_id": user_id,
                "resident_name": canonical_full_name(
                    (resident or {}).get("full_name"),
                    (resident or {}).get("username"),
                ),
                "value": "1",
            }
        )

    for key in stale_keys:
        await db.delete_setting(key)

    return sorted(items, key=lambda item: (item["duty_date"], item["zone"], item["resident_name"], item["kind"]))


async def _deadline_controls_text(db: Database) -> str:
    items = await _load_deadline_controls(db)
    lines = [
        "🕒 <b>Персональні дедлайни</b>",
        "",
        "Тут видно лише актуальні індивідуальні продовження дедлайнів і кейси без штрафу.",
        "Неактуальні записи автоматично прибираються звідси, а історія лишається в журналі дій.",
        "",
    ]
    if not items:
        lines.append("Активних персональних налаштувань зараз немає.")
        return "\n".join(lines)

    lines.append("Активні налаштування:")
    for item in items:
        zone_label = {"Kitchen": "Кухня", "Bath": "Ванна", "General": "Общак"}.get(str(item["zone"]), str(item["zone"]))
        duty_date = item["duty_date"].strftime("%d.%m.%Y")
        if item["kind"] == "override":
            try:
                due_at = datetime.fromisoformat(str(item["value"]))
                value_text = due_at.strftime("%d.%m.%Y %H:%M")
            except Exception:
                value_text = str(item["value"])
            lines.append(
                f"• <b>{item['resident_name']}</b> — {zone_label} за {duty_date}: дедлайн до <b>{value_text}</b>"
            )
        else:
            lines.append(f"• <b>{item['resident_name']}</b> — {zone_label} за {duty_date}: <b>без штрафу</b>")
    return "\n".join(lines)


async def _deadline_controls_kb(db: Database) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in await _load_deadline_controls(db):
        date_iso = item["duty_date"].isoformat()
        zone = str(item["zone"])
        user_id = int(item["user_id"])
        if item["kind"] == "override":
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"🗑 Скасувати дедлайн · {item['resident_name']}",
                        callback_data=f"deadline_control_clear:override:{date_iso}:{zone}:{user_id}",
                    )
                ]
            )
        else:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"🗑 Прибрати без штрафу · {item['resident_name']}",
                        callback_data=f"deadline_control_clear:waive:{date_iso}:{zone}:{user_id}",
                    )
                ]
            )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:section_system")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _load_skip_reminder_dates(db: Database) -> list[str]:
    raw = await db.get_setting(REMINDER_SKIP_DATES_KEY, "[]")
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return []
    today = kyiv_today()
    active_dates: list[str] = []
    changed = False
    for item in sorted({str(item) for item in parsed if str(item).strip()}):
        try:
            current_date = datetime.fromisoformat(item).date()
        except Exception:
            changed = True
            continue
        if current_date < today:
            changed = True
            continue
        active_dates.append(item)
    if changed:
        await db.set_setting(REMINDER_SKIP_DATES_KEY, json.dumps(active_dates, ensure_ascii=False))
    unique_dates = active_dates
    return unique_dates


async def _skip_reminders_text(db: Database) -> str:
    skip_dates = await _load_skip_reminder_dates(db)
    lines = [
        "🚫 <b>Пропуск нагадувань</b>",
        "",
        "У ці дні планові нагадування не надсилатимуться.",
        "Ручний запуск з адмінки при цьому все одно працює.",
        "Минулі дати автоматично прибираються зі списку, а історія лишається в журналі дій.",
        "",
    ]
    if skip_dates:
        lines.append("Поточні дати пропуску:")
        for iso_value in skip_dates:
            lines.append(f"• <b>{datetime.fromisoformat(iso_value).strftime('%d.%m.%Y')}</b>")
    else:
        lines.append("Поки що жодної дати не додано.")
    lines.extend(
        [
            "",
            "Надішли одну або кілька дат у форматі <b>ДД.ММ</b> або <b>ДД.ММ.РРРР</b>.",
            "Можна кілька через кому: <b>25.03, 31.03, 01.04</b>.",
        ]
    )
    return "\n".join(lines)


async def _skip_reminders_kb(db: Database) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for iso_value in await _load_skip_reminder_dates(db):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🗑 {datetime.fromisoformat(iso_value).strftime('%d.%m.%Y')}",
                    callback_data=f"skip_reminder_remove:{iso_value}",
                )
            ]
        )
    if rows:
        rows.append([InlineKeyboardButton(text="🧹 Очистити всі дати", callback_data="skip_reminder_clear_all")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:section_system")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _load_calendar_exceptions(db: Database) -> list[dict]:
    raw = await db.get_setting(CALENDAR_EXCEPTIONS_KEY, "[]")
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return []
    out: list[dict] = []
    today = kyiv_today()
    changed = False
    for item in parsed:
        if not isinstance(item, dict):
            changed = True
            continue
        iso_date = str(item.get("date") or "").strip()
        kind = str(item.get("kind") or "").strip()
        note = str(item.get("note") or "").strip()
        if not iso_date or kind not in {"holiday", "day_off", "special_rules"}:
            changed = True
            continue
        try:
            item_date = datetime.fromisoformat(iso_date).date()
        except Exception:
            changed = True
            continue
        if item_date < today:
            changed = True
            continue
        out.append({"date": iso_date, "kind": kind, "note": note})
    if changed:
        await db.set_setting(CALENDAR_EXCEPTIONS_KEY, json.dumps(out, ensure_ascii=False))
    return sorted(out, key=lambda item: item["date"])


def _calendar_exception_kind_label(kind: str) -> str:
    return {
        "holiday": "Свято / вихідний",
        "day_off": "День без чергувань",
        "special_rules": "Змінені правила",
    }.get(kind, kind)


async def _calendar_exceptions_text(db: Database) -> str:
    items = await _load_calendar_exceptions(db)
    lines = [
        "🗓 <b>Календар винятків</b>",
        "",
        "Тут можна позначати особливі дні для графіка.",
        "• <b>Свято / вихідний</b> — чергувань і дедлайнів на дату немає.",
        "• <b>День без чергувань</b> — теж повністю вимикає чергування на дату.",
        "• <b>Змінені правила</b> — лишає графік, але показує примітку в статусі.",
        "Минулі винятки автоматично прибираються зі списку, а історія лишається в журналі дій.",
        "",
    ]
    if items:
        lines.append("Поточні винятки:")
        for item in items:
            label = _calendar_exception_kind_label(str(item["kind"]))
            note = str(item.get("note") or "").strip()
            suffix = f" — {note}" if note else ""
            lines.append(f"• <b>{datetime.fromisoformat(str(item['date'])).strftime('%d.%m.%Y')}</b>: {label}{suffix}")
    else:
        lines.append("Поки що винятків немає.")
    return "\n".join(lines)


def _calendar_exception_kind_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎉 Свято / вихідний", callback_data="calendar_exception_kind:holiday")],
            [InlineKeyboardButton(text="😌 День без чергувань", callback_data="calendar_exception_kind:day_off")],
            [InlineKeyboardButton(text="📝 Змінені правила", callback_data="calendar_exception_kind:special_rules")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:section_system")],
        ]
    )


async def _calendar_exceptions_kb(db: Database) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="➕ Додати виняток", callback_data="calendar_exception_add")]]
    for item in await _load_calendar_exceptions(db):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🗑 {datetime.fromisoformat(str(item['date'])).strftime('%d.%m.%Y')} · {_calendar_exception_kind_label(str(item['kind']))}",
                    callback_data=f"calendar_exception_remove:{item['date']}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:section_system")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _payment_folders_kb(db: Database) -> InlineKeyboardMarkup:
    residents = await db.list_active_residents_full()
    rows = [
        [InlineKeyboardButton(text=f"💳 {resident['full_name']}", callback_data=f"payment_folder:{resident['telegram_id']}")]
        for resident in residents
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:section_system")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_contact_status_line(row: dict) -> str:
    mention = format_resident_mention(row, row.get("full_name"))
    started = "так" if row.get("has_started") else "ні"
    can_message = "так" if row.get("can_message") else "ні"
    last_seen = row.get("last_interaction_at") or "—"
    return f"• {mention} | старт: {started} | ПП: {can_message} | активність: {last_seen}"


def _zone_ua(zone_name: str) -> str:
    return {"Kitchen": "Кухня", "Bath": "Ванна", "General": "Общак"}.get(str(zone_name), str(zone_name))


def _report_history_days_label(days: int | None) -> str:
    if days is None:
        return "усе доступне"
    return f"останні {days} днів"


async def _report_history_text(db: Database, *, days: int | None = REPORT_RETENTION_DAYS) -> str:
    rows = await db.list_recent_duty_logs(limit=30, days=days)
    lines = ["🧾 <b>Історія звітів</b>", "", f"Поточний фільтр: <b>{_report_history_days_label(days)}</b>.", ""]
    if not rows:
        lines.append("Поки що звітів немає.")
        return "\n".join(lines)
    status_map = {"approved": "✅ Підтверджено", "pending": "⏳ На перевірці", "rejected": "❌ Відхилено"}
    for row in rows:
        mention = format_resident_mention(row, row.get("full_name"))
        duty_date = row.get("duty_date") or str(row.get("created_at", ""))[:10]
        line = (
            f"• <b>#{int(row['id'])}</b> · {mention}\n"
            f"  {_zone_ua(str(row['zone_name']))} · {duty_date} · {status_map.get(str(row['status']), row['status'])}"
        )
        lines.append(line)
        if row.get("admin_comment"):
            lines.append(f"  Причина: {row['admin_comment']}")
    lines.extend(
        [
            "",
            "Натисни кнопку нижче, щоб відкрити потрібний звіт.",
        ]
    )
    return "\n".join(lines)


def _report_history_filter_kb(days: int | None) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text=("✅ 7 днів" if days == 7 else "7 днів"), callback_data="report_history_filter:7"),
            InlineKeyboardButton(text=("✅ 30 днів" if days == 30 else "30 днів"), callback_data="report_history_filter:30"),
            InlineKeyboardButton(text=("✅ Усе" if days is None else "Усе"), callback_data="report_history_filter:all"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _report_history_kb(db: Database, *, days: int | None = REPORT_RETENTION_DAYS) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    status_map = {"approved": "✅", "pending": "⏳", "rejected": "❌"}
    rows.extend(_report_history_filter_kb(days).inline_keyboard)
    for row in await db.list_recent_duty_logs(limit=15, days=days):
        label = (
            f"{status_map.get(str(row['status']), '•')} "
            f"#{int(row['id'])} · {_zone_ua(str(row['zone_name']))} · "
            f"{canonical_full_name(row.get('full_name'), row.get('username'))}"
        )
        rows.append([InlineKeyboardButton(text=label, callback_data=f"admin:view_report:{int(row['id'])}")])
    rows.append([InlineKeyboardButton(text="⬅️ До історії", callback_data="admin:section_history")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _history_exports_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧾 Звіти 7д · CSV", callback_data="history_export:reports:csv:7")],
            [InlineKeyboardButton(text="🧾 Звіти 7д · JSON", callback_data="history_export:reports:json:7")],
            [InlineKeyboardButton(text="🧾 Звіти 7д · TXT", callback_data="history_export:reports:txt:7")],
            [InlineKeyboardButton(text="🧾 Звіти 30д · CSV", callback_data="history_export:reports:csv:30")],
            [InlineKeyboardButton(text="🧾 Звіти 30д · JSON", callback_data="history_export:reports:json:30")],
            [InlineKeyboardButton(text="🧾 Звіти 30д · TXT", callback_data="history_export:reports:txt:30")],
            [InlineKeyboardButton(text="🧾 Звіти всі · CSV", callback_data="history_export:reports:csv:all")],
            [InlineKeyboardButton(text="🧾 Звіти всі · JSON", callback_data="history_export:reports:json:all")],
            [InlineKeyboardButton(text="🧾 Звіти всі · TXT", callback_data="history_export:reports:txt:all")],
            [InlineKeyboardButton(text="🔄 Обміни · CSV", callback_data="history_export:swaps:csv")],
            [InlineKeyboardButton(text="🔄 Обміни · JSON", callback_data="history_export:swaps:json")],
            [InlineKeyboardButton(text="🔄 Обміни · TXT", callback_data="history_export:swaps:txt")],
            [InlineKeyboardButton(text="🕘 Дії · CSV", callback_data="history_export:actions:csv")],
            [InlineKeyboardButton(text="🕘 Дії · JSON", callback_data="history_export:actions:json")],
            [InlineKeyboardButton(text="🕘 Дії · TXT", callback_data="history_export:actions:txt")],
            [InlineKeyboardButton(text="⬅️ До історії", callback_data="admin:section_history")],
        ]
    )


async def _history_exports_text() -> str:
    return (
        "📦 <b>Експорти історій</b>\n\n"
        "Оберіть, що саме вивантажити і в якому форматі.\n"
        "Для звітів доступні фільтри: <b>7 днів</b>, <b>30 днів</b> або <b>усе доступне</b>.\n"
        "Журнали обмінів і дій вивантажуються з актуальної історії в базі."
    )


def _csv_bytes(rows: list[dict], headers: list[str]) -> bytes:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow({header: row.get(header, "") for header in headers})
    return buffer.getvalue().encode("utf-8")


def _txt_bytes(rows: list[dict], headers: list[str]) -> bytes:
    lines: list[str] = []
    for row in rows:
        lines.append(" | ".join(f"{header}: {row.get(header, '')}" for header in headers))
    return ("\n".join(lines) if lines else "Немає даних для експорту.").encode("utf-8")


async def _build_history_export(db: Database, export_kind: str, export_format: str, *, days: int | None = None) -> tuple[str, bytes]:
    now = kyiv_today()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if export_kind == "reports":
        if days is None:
            rows = await db.list_recent_duty_logs(limit=500, days=None)
        else:
            rows = await db.list_recent_duty_logs(limit=500, days=days)
        normalized = [
            {
                "id": int(row["id"]),
                "duty_date": str(row.get("duty_date") or ""),
                "zone_name": _zone_ua(str(row["zone_name"])),
                "resident": canonical_full_name(row.get("full_name"), row.get("username")),
                "status": str(row.get("status") or ""),
                "admin_comment": str(row.get("admin_comment") or ""),
                "created_at": str(row.get("created_at") or ""),
            }
            for row in rows
        ]
        headers = ["id", "duty_date", "zone_name", "resident", "status", "admin_comment", "created_at"]
        suffix = "all" if days is None else f"{days}d"
        base_name = f"reports_{suffix}_{stamp}"
    elif export_kind == "swaps":
        rows = await db.list_recent_swap_attempts(limit=200)
        normalized = [
            {
                "id": int(row["id"]),
                "created_at": str(row.get("created_at") or ""),
                "target_date": str(row.get("target_date") or ""),
                "zone": _zone_ua(str(row.get("zone") or "")),
                "from_name": canonical_full_name(row.get("from_name")),
                "to_name": canonical_full_name(row.get("to_name")),
                "status": str(row.get("status") or ""),
                "details": str(row.get("details") or ""),
            }
            for row in rows
        ]
        headers = ["id", "created_at", "target_date", "zone", "from_name", "to_name", "status", "details"]
        base_name = f"swaps_{stamp}"
    elif export_kind == "actions":
        rows = await db.list_admin_action_logs(limit=200)
        normalized = [
            {
                "id": int(row["id"]),
                "created_at": str(row.get("created_at") or ""),
                "admin_name": str(row.get("admin_name") or row.get("admin_id") or ""),
                "action_type": str(row.get("action_type") or ""),
                "target_name": str(row.get("target_name") or row.get("target_id") or ""),
                "details": str(row.get("details") or ""),
            }
            for row in rows
        ]
        headers = ["id", "created_at", "admin_name", "action_type", "target_name", "details"]
        base_name = f"actions_{stamp}"
    else:
        raise ValueError("Unknown export kind")

    if export_format == "json":
        return f"{base_name}.json", json.dumps(normalized, ensure_ascii=False, indent=2).encode("utf-8")
    if export_format == "txt":
        return f"{base_name}.txt", _txt_bytes(normalized, headers)
    return f"{base_name}.csv", _csv_bytes(normalized, headers)


async def _stats_text(db: Database) -> str:
    stats = await db.get_report_stats()
    totals = stats.get("totals") or {}
    lines = [
        "📊 <b>Статистика звітів</b>",
        "",
        f"Усього: <b>{int(totals.get('total') or 0)}</b>",
        f"Підтверджено: <b>{int(totals.get('approved') or 0)}</b>",
        f"На перевірці: <b>{int(totals.get('pending') or 0)}</b>",
        f"Відхилено: <b>{int(totals.get('rejected') or 0)}</b>",
        "",
        "Топ мешканців за підтвердженими звітами:",
    ]
    for row in (stats.get("by_resident") or [])[:10]:
        lines.append(
            f"• {row['full_name']} — підтверджено: {int(row.get('approved') or 0)}, "
            f"усього: {int(row.get('total') or 0)}, відхилено: {int(row.get('rejected') or 0)}"
        )
    return "\n".join(lines)


async def _swap_history_text(db: Database) -> str:
    rows = await db.list_recent_swap_attempts(limit=25)
    lines = ["🔄 <b>Історія обмінів</b>", ""]
    if not rows:
        lines.append("Поки що обмінів не було.")
        return "\n".join(lines)
    status_map = {
        "requested": "⏳ Запит",
        "accepted": "✅ Підтверджено",
        "declined": "❌ Відмовлено",
        "invalid_partner": "⚠️ Невалідно",
    }
    for row in rows:
        from_name = canonical_full_name(row.get("from_name"))
        to_name = canonical_full_name(row.get("to_name"))
        lines.append(
            f"• {row['created_at']} | {_zone_ua(str(row['zone']))} | "
            f"{from_name} → {to_name or '—'} | {status_map.get(str(row['status']), str(row['status']))}"
        )
        if row.get("details"):
            lines.append(f"Деталі: {row['details']}")
    return "\n".join(lines)


async def _action_log_text(db: Database) -> str:
    rows = await db.list_admin_action_logs(limit=25)
    lines = ["🕘 <b>Лог дій адміна</b>", ""]
    if not rows:
        lines.append("Поки що дій не зафіксовано.")
        return "\n".join(lines)
    for row in rows:
        admin_name = row.get("admin_name") or f"ID {row['admin_id']}"
        target_name = row.get("target_name") or (f"ID {row['target_id']}" if row.get("target_id") else "—")
        lines.append(_format_admin_action_entry(row, admin_name, target_name))
    return "\n".join(lines)


def _format_admin_action_entry(row: dict, admin_name: str, target_name: str) -> str:
    action_type = str(row.get("action_type") or "")
    created_at = str(row.get("created_at") or "—")
    details = str(row.get("details") or "")

    labels = {
        "add_skip_reminder_dates": "додав дати пропуску нагадувань",
        "remove_skip_reminder_date": "видалив дату пропуску нагадувань",
        "clear_skip_reminder_dates": "очистив усі дати пропуску нагадувань",
        "set_calendar_exception": "задав виняток у календарі",
        "remove_calendar_exception": "видалив виняток із календаря",
        "deadline_no_fine": "скасував штраф за дедлайн",
        "deadline_extend_custom": "продовжив персональний дедлайн",
        "clear_deadline_user_override": "скасував персональний дедлайн",
        "clear_deadline_waive": "скасував режим без штрафу",
        "update_job_time": "змінив час нагадування",
        "update_deadline": "змінив час дедлайну",
        "update_bank_url": "оновив посилання на банку",
        "update_payment_folder": "оновив папку для оплати",
        "manual_override": "змінив ручне чергування",
        "clear_manual_override": "скасував ручне чергування",
        "restore_backup": "відновив бекап",
        "issue_fine": "виписав штраф",
        "fine_after_reject": "виписав штраф після відхилення звіту",
        "deadline_bank_fine": "виписав грошовий штраф за дедлайн",
        "deadline_text_fine": "виписав текстовий штраф за дедлайн",
    }
    action_label = labels.get(action_type, action_type)

    if details:
        return f"• <b>{created_at}</b> — {admin_name} {action_label}. Ціль: <b>{target_name}</b>. <code>{details}</code>"
    return f"• <b>{created_at}</b> — {admin_name} {action_label}. Ціль: <b>{target_name}</b>."


async def _health_text(db: Database, scheduler_service=None, backup_service=None) -> str:
    db_path = await db.get_setting("db_path_shadow", "—")
    version = await db.get_setting("app_version", "1.0.0")
    env_name = await db.get_setting("app_env", "production")
    scheduler_running = "так" if getattr(getattr(scheduler_service, "scheduler", None), "running", False) else "ні"
    backup_enabled = "так" if getattr(backup_service, "enabled", False) else "ні"
    group_time = await db.get_setting(f"time:{JOB_GROUP_MORNING}", "09:00")
    definition = await load_instance_definition(db)
    reminder_lines = [f"• Група: <b>{group_time}</b>"]
    for zone in sorted(definition.zones, key=lambda item: item.sort_order):
        if not zone.enabled or not zone.private_reminder_enabled or not zone.rotation_enabled:
            continue
        zone_job_id = _runtime_private_job_id(zone.code)
        zone_time = await db.get_setting(f"time:{zone_job_id}", zone.private_reminder_time or "—")
        reminder_lines.append(f"• {zone.title}: <b>{zone_time}</b>")
    return (
        "❤️ <b>Health-check</b>\n\n"
        f"Середовище: <b>{env_name}</b>\n"
        f"Версія: <b>{version}</b>\n"
        f"База: <b>{db_path or '—'}</b>\n"
        f"Планувальник активний: <b>{scheduler_running}</b>\n"
        f"Автобекап активний: <b>{backup_enabled}</b>\n"
        "\n"
        "Поточні scheduler-часи:\n"
        f"{chr(10).join(reminder_lines)}\n"
        "Базова збірка та маршрути вже ініціалізовані."
    )


def _tail_error_log(log_path: str, max_lines: int = 20) -> str:
    from pathlib import Path

    path = Path(log_path)
    if not path.exists():
        return "Файл логів ще не створено."
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()[-max_lines:]
    if not lines:
        return "Лог поки що порожній."
    return "\n".join(lines)


async def _restore_menu_text(backup_service) -> tuple[str, InlineKeyboardMarkup]:
    backups = backup_service.list_backups(limit=8) if backup_service else []
    text_lines = [
        "♻️ <b>Відновлення з бекапу</b>",
        "",
        "Оберіть один із локальних бекапів. Перед відновленням бот автоматично створить страхувальну копію.",
    ]
    rows: list[list[InlineKeyboardButton]] = []
    if not backups:
        text_lines.append("")
        text_lines.append("Локальних бекапів не знайдено.")
    else:
        text_lines.append("")
        text_lines.append("Доступні бекапи:")
        for backup in backups:
            text_lines.append(f"• {backup.name}")
            rows.append([InlineKeyboardButton(text=backup.name, callback_data=f"restore_pick:{backup.name}")])
    rows.append([InlineKeyboardButton(text="⬅️ До системи", callback_data="admin:section_system")])
    return "\n".join(text_lines), InlineKeyboardMarkup(inline_keyboard=rows)


async def _test_whitelist_kb(db: Database) -> InlineKeyboardMarkup:
    whitelisted_ids = set(await db.list_test_whitelist_ids())
    residents = await db.list_active_residents_full()
    rows = []
    for resident in residents:
        mark = "✅" if int(resident["telegram_id"]) in whitelisted_ids else "☑️"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{mark} {resident['full_name']}",
                    callback_data=f"test_whitelist:{resident['telegram_id']}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back_to_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows[:95])


async def _override_zone_member_ids(db: Database, zone_identifier: str) -> tuple[int, ...]:
    definition = await load_instance_definition(db)
    zone_code = zone_code_from_identifier(zone_identifier)
    zone = next((item for item in definition.zones if item.code == zone_code), None)
    if not zone:
        return ()
    member_ids: list[int] = []
    if zone.rule.member_order:
        member_ids.extend(int(item) for item in zone.rule.member_order)
    for group in zone.rule.member_groups:
        member_ids.extend(int(item) for item in group)
    seen: set[int] = set()
    ordered: list[int] = []
    for member_id in member_ids:
        if member_id in seen:
            continue
        seen.add(member_id)
        ordered.append(member_id)
    return tuple(ordered)


async def _override_required_slots(db: Database, zone_identifier: str, target_date) -> int:
    assignment = await get_zone_assignment_for_date(db, zone_identifier, target_date)
    if assignment and assignment[1]:
        return max(1, len(tuple(assignment[1])))
    definition = await load_instance_definition(db)
    zone_code = zone_code_from_identifier(zone_identifier)
    zone = next((item for item in definition.zones if item.code == zone_code), None)
    if not zone:
        return 1
    if zone.rule.member_groups:
        return max(len(group) for group in zone.rule.member_groups if group) or 1
    return max(zone.rule.team_pattern or (1,))


async def _override_residents_kb(
    db: Database,
    *,
    callback_prefix: str,
    selected_ids: set[int] | None = None,
    zone_name: str | None = None,
) -> InlineKeyboardMarkup:
    selected = selected_ids or set()
    residents = await db.list_active_residents_full()
    eligible_ids = set(await _override_zone_member_ids(db, str(zone_name))) if zone_name else None
    rows = []
    for resident in residents:
        resident_id = int(resident["telegram_id"])
        if resident_id in selected:
            continue
        if eligible_ids is not None and eligible_ids and resident_id not in eligible_ids:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    text=resident["full_name"],
                    callback_data=f"{callback_prefix}:{resident['telegram_id']}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back_to_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows[:95])


async def _override_date_text(
    db: Database,
    target_date,
    *,
    title: str,
    intro: str,
    overrides: list[dict],
) -> str:
    zone_choices = await list_enabled_zone_choices(db)
    by_zone: dict[str, list[str]] = {}
    for row in overrides:
        by_zone.setdefault(str(row["zone_name"]), []).append(canonical_full_name(row.get("full_name"), row.get("username")))
    lines = [f"{title}\n", f"Дата: <b>{target_date.strftime('%d.%m.%Y')}</b>", intro]
    for zone_identifier, zone_title in zone_choices:
        names = by_zone.get(zone_identifier) or []
        lines.append(f"• {zone_title}: <b>{', '.join(names) if names else 'не задано'}</b>")
    lines.append("")
    lines.append("Оберіть зону нижче, щоб змінити призначення.")
    return "\n".join(lines)


async def _override_date_kb_for(
    db: Database,
    *,
    target_date,
    zone_callback_prefix: str,
    clear_callback: str,
    reopen_callback_prefix: str,
) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=zone_title, callback_data=f"{zone_callback_prefix}:{zone_identifier}")]
        for zone_identifier, zone_title in await list_enabled_zone_choices(db)
    ]
    rows.append([InlineKeyboardButton(text="🗑 Очистити дату", callback_data=clear_callback)])
    rows.append(
        [
            InlineKeyboardButton(
                text="🔁 Оновити екран",
                callback_data=f"{reopen_callback_prefix}:{target_date.isoformat()}",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back_to_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _test_override_date_text(db: Database, target_date) -> str:
    return await _override_date_text(
        db,
        target_date,
        title="🎯 <b>Тестові черги</b>",
        intro="Поточні тестові призначення:",
        overrides=await db.get_test_overrides_for_date(target_date),
    )


async def _manual_override_date_text(db: Database, target_date) -> str:
    return await _override_date_text(
        db,
        target_date,
        title="🔁 <b>Ручна заміна чергувань</b>",
        intro="Поточні ручні заміни:",
        overrides=await db.get_manual_overrides_for_date(target_date),
    )


async def _test_override_date_kb_for(db: Database, target_date) -> InlineKeyboardMarkup:
    return await _override_date_kb_for(
        db,
        target_date=target_date,
        zone_callback_prefix="test_override_zone",
        clear_callback="test_override_clear_date",
        reopen_callback_prefix="test_override_reopen",
    )


async def _manual_override_date_kb_for(db: Database, target_date) -> InlineKeyboardMarkup:
    return await _override_date_kb_for(
        db,
        target_date=target_date,
        zone_callback_prefix="manual_override_zone",
        clear_callback="manual_override_clear_date",
        reopen_callback_prefix="manual_override_reopen",
    )


async def _build_export_csv(db: Database, start_date, end_date) -> tuple[str, bytes]:
    logs = await db.list_duty_logs_between(start_date, end_date)
    logs_map: dict[tuple[str, str, int], list[dict]] = {}
    for log in logs:
        duty_key = str(log.get("duty_date") or "") or str(log["created_at"])[:10]
        key = (duty_key, str(log["zone_name"]), int(log["telegram_id"]))
        logs_map.setdefault(key, []).append(log)

    records: list[ExcelRecord] = []
    current = start_date
    while current <= end_date:
        definition, runtime_assignments = await get_runtime_zone_assignments_for_date(db, current)
        date_key = current.isoformat()
        for zone in sorted(definition.zones, key=lambda item: item.sort_order):
            assignment = runtime_assignments.get(zone.code)
            if not assignment:
                continue
            zone_keys = {zone_identifier_from_code(zone.code), zone.code}
            for resident_id, resident_name in zip(assignment.member_ids, assignment.member_names):
                resident_name = canonical_full_name(resident_name)
                status_rows: list[dict] = []
                for zone_key in zone_keys:
                    status_rows.extend(logs_map.get((date_key, zone_key, int(resident_id)), []))
                if not status_rows:
                    status = "Не здано"
                else:
                    statuses = {str(item["status"]) for item in status_rows}
                    if "approved" in statuses:
                        status = "Підтверджено"
                    elif "pending" in statuses:
                        status = "На перевірці"
                    else:
                        status = "Відхилено"
                records.append(
                    ExcelRecord(
                        day=current,
                        zone=zone.title,
                        person=resident_name,
                        status=status,
                    )
                )
        current = current.fromordinal(current.toordinal() + 1)

    xlsx_bytes = build_xlsx_bytes(records, title="Розклад чергувань")
    filename = f"cherguvannya_{start_date.isoformat()}_{end_date.isoformat()}.xlsx"
    return filename, xlsx_bytes

@router.message(lambda m: (m.text or "") == "/admin")
async def cmd_admin(message: Message, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return
    if not await can_access_admin_panel(db, int(resident["telegram_id"])):
        await message.answer("Доступ обмежено")
        return
    await message.answer(
        await admin_panel_text(db, int(resident["telegram_id"])),
        reply_markup=await admin_kb(db, int(resident["telegram_id"]), include_back=True),
    )


@router.message(lambda m: (m.text or "") == "/health")
async def cmd_health(message: Message, db: Database, scheduler_service=None, backup_service=None) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return
    if not await has_permission(db, int(resident["telegram_id"]), PERM_SYSTEM_VIEW):
        await message.answer("Доступ обмежено")
        return
    await message.answer(
        await _health_text(db, scheduler_service=scheduler_service, backup_service=backup_service),
        reply_markup=_back_to_panel_kb(),
    )


@router.message(lambda m: (m.text or "") == "/version")
async def cmd_version(message: Message, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return
    if not await has_permission(db, int(resident["telegram_id"]), PERM_SYSTEM_VIEW):
        await message.answer("Доступ обмежено")
        return
    env_name = await db.get_setting("app_env", "production")
    await message.answer(
        f"🧬 <b>Версія бота</b>\n\nВерсія: <b>{APP_VERSION}</b>\nСередовище: <b>{env_name}</b>",
        reply_markup=_back_to_panel_kb(),
    )


@router.callback_query(lambda c: c.data and c.data.startswith("admin:"))
async def admin_actions(
    callback: CallbackQuery,
    state: FSMContext,
    db: Database,
    bot,
    scheduler_service=None,
    backup_service=None,
) -> None:
    if not callback.from_user or not await can_access_admin_panel(db, int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    action = callback.data.split(":", 1)[1]
    required_permission = _permission_for_admin_action(action)
    if action == "reset_db_menu" and not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if action == "report_review_route" and not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if required_permission and not await has_permission(db, int(callback.from_user.id), required_permission):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    today = kyiv_today()
    logging.info(
        "Адмінська дія | action=%s | admin_id=%s | chat_id=%s",
        action,
        int(callback.from_user.id),
        int(callback.message.chat.id),
    )

    if action == "section_actions":
        ok = await _safe_refresh_callback_message(
            callback,
            text="🛠️ <b>Адмін / Дії</b>\n\nШвидкі дії для нагадувань, штрафів, експорту і тестового режиму.",
            reply_markup=await _admin_actions_kb(db, int(callback.from_user.id)),
        )
        if not ok:
            await callback.answer("Не вдалося оновити екран. Відкрий адмінку ще раз.", show_alert=True)
            return
        await callback.answer()
        return

    if action == "section_history":
        ok = await _safe_refresh_callback_message(
            callback,
            text="🛠️ <b>Адмін / Історія</b>\n\nТут можна переглядати звіти, обміни, статистику й журнал дій.",
            reply_markup=await _admin_history_kb(db, int(callback.from_user.id)),
        )
        if not ok:
            await callback.answer("Не вдалося оновити екран. Відкрий адмінку ще раз.", show_alert=True)
            return
        await callback.answer()
        return

    if action == "section_system":
        ok = await _safe_refresh_callback_message(
            callback,
            text="🛠️ <b>Адмін / Система</b>\n\nТут зібрані технічні інструменти: health-check, бекапи, логи, версія і налаштування.",
            reply_markup=await _admin_system_kb(db, int(callback.from_user.id)),
        )
        if not ok:
            await callback.answer("Не вдалося оновити екран. Відкрий адмінку ще раз.", show_alert=True)
            return
        await callback.answer()
        return

    if action == "report_review_route":
        await callback.message.edit_text(
            await _report_review_route_text(db),
            reply_markup=report_review_route_kb(await get_report_review_route(db)),
        )
        await callback.answer()
        return

    if action == "group_now":
        logging.info("Ручний запуск групового оголошення | admin_id=%s", int(callback.from_user.id))
        await send_group_morning_reminder(bot=bot, db=db, group_id=int(GROUP_ID), force=True)
        await callback.answer("Надіслано ✅", show_alert=True)
        return

    if action == "payment_now":
        if not scheduler_service:
            await callback.answer("Scheduler недоступний", show_alert=True)
            return
        logging.info("Ручний запуск нагадувань про оплату | admin_id=%s", int(callback.from_user.id))
        await scheduler_service.send_monthly_payment_reminders(force=True)
        await callback.answer("Нагадування про оплату надіслано ✅", show_alert=True)
        return

    if action == "manual_override":
        await state.set_state(AdminFSM.entering_manual_override_date)
        await _remember_admin_message(state, callback.message)
        await callback.message.edit_text(
            "🔁 <b>Ручна заміна чергувань</b>\n\n"
            "Введіть дату у форматі <b>ДД.ММ</b> або <b>ДД.ММ.РРРР</b>.",
            reply_markup=_back_to_panel_kb(),
        )
        await callback.answer()
        return

    if action == "send_reminders_now":
        logging.info("Ручний запуск приватних нагадувань | admin_id=%s | date=%s", int(callback.from_user.id), today.isoformat())
        if not scheduler_service:
            await callback.answer("Scheduler недоступний", show_alert=True)
            return
        await scheduler_service.send_all_private_zone_reminders(force=True)
        await callback.answer("Нагадування надіслано ✅", show_alert=True)
        return

    if action == "backup_now":
        if not backup_service:
            await callback.answer("Сервіс бекапу недоступний", show_alert=True)
            return
        logging.info("Ручний запуск бекапу | admin_id=%s", int(callback.from_user.id))
        backup_path = await backup_service.create_backup()
        await backup_service.send_backup_to_admin(backup_path, automatic=False)
        await db.log_admin_action(int(callback.from_user.id), "manual_backup", details=backup_path.name)
        await callback.answer("Бекап надіслано в ПП ✅", show_alert=True)
        return

    if action == "restore_menu":
        text, markup = await _restore_menu_text(backup_service)
        await callback.message.edit_text(text, reply_markup=markup)
        await callback.answer()
        return

    if action == "health":
        await callback.message.edit_text(
            await _health_text(db, scheduler_service=scheduler_service, backup_service=backup_service),
            reply_markup=_admin_section_back_kb(),
        )
        await callback.answer()
        return

    if action == "version":
        version = await db.get_setting("app_version", "1.0.0")
        env_name = await db.get_setting("app_env", "production")
        await callback.message.edit_text(
            f"🧬 <b>Версія бота</b>\n\nВерсія: <b>{version}</b>\nСередовище: <b>{env_name}</b>",
            reply_markup=_admin_section_back_kb(),
        )
        await callback.answer()
        return

    if action == "runtime_config":
        definition = await load_instance_definition(db)
        await callback.message.edit_text(
            await _runtime_config_text(db),
            reply_markup=_runtime_config_kb(definition, can_manage=is_owner_id(int(callback.from_user.id))),
        )
        await callback.answer()
        return

    if action == "runtime_flags":
        definition = await load_instance_definition(db)
        await callback.message.edit_text(
            await _runtime_flags_text(db),
            reply_markup=_runtime_flags_kb(definition, can_manage=is_owner_id(int(callback.from_user.id))),
        )
        await callback.answer()
        return

    if action == "runtime_zones":
        definition = await load_instance_definition(db)
        await callback.message.edit_text(
            await _runtime_zones_text(db),
            reply_markup=_runtime_zones_kb(definition),
        )
        await callback.answer()
        return

    if action == "error_log":
        from config import LOG_FILE

        log_tail = _tail_error_log(LOG_FILE)
        await callback.message.edit_text(
            f"📜 <b>Останні рядки логу</b>\n\n<pre>{log_tail}</pre>",
            reply_markup=_admin_section_back_kb(),
        )
        await callback.answer()
        return

    if action == "report_history":
        await callback.message.edit_text(
            await _report_history_text(db, days=REPORT_RETENTION_DAYS),
            reply_markup=await _report_history_kb(db, days=REPORT_RETENTION_DAYS),
        )
        await callback.answer()
        return

    if action == "history_exports":
        await callback.message.edit_text(
            await _history_exports_text(),
            reply_markup=_history_exports_kb(),
        )
        await callback.answer()
        return

    if action == "stats":
        await callback.message.edit_text(
            await _stats_text(db),
            reply_markup=_back_to_panel_kb(),
        )
        await callback.answer()
        return

    if action == "swap_history":
        await callback.message.edit_text(
            await _swap_history_text(db),
            reply_markup=_back_to_panel_kb(),
        )
        await callback.answer()
        return

    if action == "action_log":
        await callback.message.edit_text(
            await _action_log_text(db),
            reply_markup=_back_to_panel_kb(),
        )
        await callback.answer()
        return

    if action == "contact_status":
        rows = await db.list_contact_statuses()
        if not rows:
            await callback.message.edit_text(
                "👤 <b>Статус контактів</b>\n\nПоки що немає даних про контакти користувачів.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back_to_panel")]]
                ),
            )
            await callback.answer()
            return
        lines = ["👤 <b>Статус контактів</b>", "---"]
        for row in rows:
            lines.append(_format_contact_status_line(row))
            if row.get("last_delivery_error"):
                lines.append(f"Помилка доставки: {row['last_delivery_error']}")
        await callback.message.edit_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back_to_panel")]]
            ),
        )
        await callback.answer()
        return

    if action == "help":
        await callback.message.edit_text(
            await admin_help_text(db),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:back_to_panel")]]
            ),
        )
        await callback.answer()
        return

    if action == "test_mode":
        enabled = await is_test_mode_enabled(db)
        if enabled:
            await db.set_setting("test_mode", "0")
            await db.clear_test_mode_data()
        else:
            await db.set_setting("test_mode", "1")
        await db.log_admin_action(
            int(callback.from_user.id),
            "toggle_test_mode",
            details="on" if not enabled else "off",
        )
        await notify_owner_about_delegate_action(
            callback.bot,
            db,
            actor_id=int(callback.from_user.id),
            action_type="toggle_test_mode",
            details="on" if not enabled else "off",
        )
        status = "увімкнено" if not enabled else "вимкнено"
        ok = await _safe_edit_callback_message(
            callback,
            text="🛠️ <b>Адмін / Дії</b>\n\nШвидкі дії для нагадувань, штрафів, експорту і тестового режиму.",
            reply_markup=await _admin_actions_kb(db, int(callback.from_user.id)),
        )
        if not ok:
            await callback.answer("Тестовий режим змінено, але екран не оновився. Відкрий адмінку ще раз.", show_alert=True)
            return
        await callback.answer(f"Тестовий режим: {status}", show_alert=True)
        return

    if action == "test_whitelist":
        if not await is_test_mode_enabled(db):
            await callback.answer("Увімкніть тестовий режим спочатку.", show_alert=True)
            return
        await _remember_admin_message(state, callback.message)
        ok = await _safe_edit_callback_message(
            callback,
            text="👥 <b>Тестувальники</b>\n\nТут показані лише активні мешканці. "
            "Позначені люди матимуть доступ до тестових сценаріїв.",
            reply_markup=await _test_whitelist_kb(db),
        )
        if not ok:
            await callback.answer("Не вдалося відкрити екран тестувальників. Відкрий адмінку ще раз.", show_alert=True)
            return
        await callback.answer()
        return

    if action == "test_override":
        if not await is_test_mode_enabled(db):
            await callback.answer("Увімкніть тестовий режим спочатку.", show_alert=True)
            return
        await state.set_state(AdminFSM.entering_override_date)
        await _remember_admin_message(state, callback.message)
        ok = await _safe_edit_callback_message(
            callback,
            text="Введіть дату тестового оверрайду у форматі <b>ДД.ММ</b> або <b>ДД.ММ.РРРР</b>.",
            reply_markup=_back_to_panel_kb(),
        )
        if not ok:
            await callback.answer("Не вдалося відкрити екран тестових черг. Відкрий адмінку ще раз.", show_alert=True)
            return
        await callback.answer()
        return

    if action == "back_to_panel":
        ok = await _safe_refresh_callback_message(
            callback,
            text=await admin_panel_text(db, int(callback.from_user.id)),
            reply_markup=await admin_kb(db, int(callback.from_user.id), include_back=True),
        )
        if not ok:
            await callback.answer("Не вдалося оновити екран. Відкрий адмінку ще раз.", show_alert=True)
            return
        await callback.answer()
        return

    if action == "export_duty":
        await state.set_state(AdminFSM.entering_export_period)
        await _remember_admin_message(state, callback.message)
        await callback.answer()
        await callback.message.edit_text(
            "Введи період у форматі <b>ДД.ММ-ДД.ММ</b> або <b>ДД.ММ.РРРР-ДД.ММ.РРРР</b>.",
            reply_markup=_back_to_panel_kb(),
        )
        return

    if action == "manage_fines":
        await callback.answer()
        await start_admin_fine_flow(
            callback.message,
            state,
            db,
            actor_id=int(callback.from_user.id),
            reuse_message=True,
        )
        return

    if action.startswith("remind_zone:"):
        if not scheduler_service:
            await callback.answer("Scheduler недоступний", show_alert=True)
            return
        zone_code = action.split(":", 1)[1]
        await scheduler_service.send_zone_reminder(zone_code, force=True)
        zone_title = await get_zone_title(db, zone_code)
        await callback.answer(f"Нагадування по зоні «{zone_title}» надіслано ✅", show_alert=True)
        return

    if action == "set_time":
        await state.set_state(AdminFSM.choosing_time_job)
        await _remember_admin_message(state, callback.message)
        await callback.message.edit_text(await _time_settings_text(db), reply_markup=await _time_jobs_kb(db))
        await callback.answer()
        return

    if action == "payment_folders":
        await state.set_state(AdminFSM.choosing_payment_resident)
        await _remember_admin_message(state, callback.message)
        await callback.message.edit_text(
            await _payment_folders_text(db),
            reply_markup=await _payment_folders_kb(db),
        )
        await callback.answer()
        return

    if action == "skip_reminders":
        await state.set_state(AdminFSM.entering_skip_reminder_dates)
        await _remember_admin_message(state, callback.message)
        await callback.message.edit_text(
            await _skip_reminders_text(db),
            reply_markup=await _skip_reminders_kb(db),
        )
        await callback.answer()
        return

    if action == "calendar_exceptions":
        await state.set_state(AdminFSM.choosing_calendar_exception_kind)
        await _remember_admin_message(state, callback.message)
        await callback.message.edit_text(
            await _calendar_exceptions_text(db),
            reply_markup=await _calendar_exceptions_kb(db),
        )
        await callback.answer()
        return

    if action == "deadline_controls":
        await state.clear()
        await _remember_admin_message(state, callback.message)
        await callback.message.edit_text(
            await _deadline_controls_text(db),
            reply_markup=await _deadline_controls_kb(db),
        )
        await callback.answer()
        return

    if action == "delegates":
        await state.set_state(AdminFSM.choosing_delegate_resident)
        await _remember_admin_message(state, callback.message)
        await callback.message.edit_text(
            await _delegates_text(db),
            reply_markup=await _delegates_kb(db),
        )
        await callback.answer()
        return

    if action == "bank_url":
        await state.set_state(AdminFSM.entering_bank_url)
        await _remember_admin_message(state, callback.message)
        await callback.message.edit_text("🏦 Встав посилання на банку (fine_bank_url):", reply_markup=_back_to_panel_kb())
        await callback.answer()
        return

    if action == "reset_db_menu":
        await state.set_state(AdminFSM.confirming_reset_db)
        await callback.message.edit_text(
            "🧨 <b>Очистити базу даних</b>\n\n"
            "Це видалить поточні звіти, штрафи, обміни, тестові дані, логи дій і налаштування.\n"
            "Після очищення бот одразу заново створить таблиці та підтягне актуальних мешканців із <b>residents.json</b>.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Так, очистити", callback_data="reset_db_confirm")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:section_system")],
                ]
            ),
        )
        await callback.answer()
        return

    await callback.answer()


@router.callback_query(AdminFSM.choosing_payment_resident, lambda c: c.data and c.data.startswith("payment_folder:"))
async def choose_payment_folder_resident(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_PAYMENTS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    resident_id = int(callback.data.split(":", 1)[1])
    resident = await db.get_resident(resident_id)
    if not resident:
        await callback.answer("Мешканця не знайдено", show_alert=True)
        return

    current_url = ((await db.get_setting(f"payment_folder:{resident_id}", "")) or "").strip()
    await state.update_data(payment_resident_id=resident_id)
    await state.set_state(AdminFSM.entering_payment_folder)
    await callback.message.edit_text(
        "💳 <b>Папка оплат</b>\n\n"
        f"Мешканець: <b>{resident['full_name']}</b>\n"
        f"Поточне посилання: <code>{current_url or 'не задано'}</code>\n\n"
        "Вставте Google-посилання текстом.\n"
        "Щоб очистити посилання, надішліть <b>-</b>.",
        reply_markup=_back_to_panel_kb(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data and c.data.startswith("deadline_control_clear:"))
async def clear_deadline_control(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_EXCEPTIONS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    _, kind, duty_date_iso, zone, user_id_raw = callback.data.split(":", 4)
    duty_date = datetime.fromisoformat(duty_date_iso).date()
    user_id = int(user_id_raw)

    if kind == "override":
        await db.delete_setting(deadline_user_override_key(zone, duty_date, user_id))
        action_name = "clear_deadline_user_override"
        confirmation = "Персональний дедлайн скасовано ✅"
    else:
        await db.delete_setting(deadline_waive_key(zone, duty_date, user_id))
        action_name = "clear_deadline_waive"
        confirmation = "Режим без штрафу скасовано ✅"

    await db.log_admin_action(
        int(callback.from_user.id),
        action_name,
        target_id=user_id,
        details=f"zone={zone}|date={duty_date.isoformat()}",
    )
    await notify_owner_about_delegate_action(
        callback.bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type=action_name,
        details=f"zone={zone}|date={duty_date.isoformat()}",
        target_id=user_id,
    )
    await state.clear()
    await callback.message.edit_text(
        await _deadline_controls_text(db),
        reply_markup=await _deadline_controls_kb(db),
    )
    await callback.answer(confirmation, show_alert=True)


@router.callback_query(lambda c: c.data and c.data.startswith("admin:view_report:"))
async def view_report_from_history(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_REPORTS_REVIEW):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    log_id = int(callback.data.rsplit(":", 1)[1])
    log = await db.get_duty_log(log_id)
    if not log:
        await callback.answer("Звіт не знайдено", show_alert=True)
        return

    raw_photo_ids = str(log.get("photo_id") or "").strip()
    try:
        photo_ids = json.loads(raw_photo_ids) if raw_photo_ids.startswith("[") else [raw_photo_ids]
    except Exception:
        photo_ids = [raw_photo_ids] if raw_photo_ids else []
    photo_ids = [str(item) for item in photo_ids if str(item).strip()]
    if not photo_ids:
        await callback.answer("У звіті немає фото", show_alert=True)
        return

    resident_mention = format_resident_mention(log, log.get("full_name"))
    zone_label = _zone_ua(str(log["zone_name"]))
    duty_date = str(log.get("duty_date") or "—")
    status_label = {
        "pending": "⏳ На перевірці",
        "approved": "✅ Підтверджено",
        "rejected": "❌ Відхилено",
    }.get(str(log.get("status")), str(log.get("status") or "—"))

    media: list[InputMediaPhoto] = []
    for index, photo_id in enumerate(photo_ids):
        caption = None
        if index == 0:
            caption = (
                f"Звіт <b>#{log_id}</b>\n"
                f"Мешканець: {resident_mention}\n"
                f"Зона: <b>{zone_label}</b>\n"
                f"Дата чергування: <b>{duty_date}</b>\n"
                f"Статус: <b>{status_label}</b>\n"
                f"Фото: <b>{len(photo_ids)}</b>"
            )
        media.append(InputMediaPhoto(media=photo_id, caption=caption))

    try:
        await bot.send_media_group(chat_id=int(callback.from_user.id), media=media)
    except Exception:
        for index, photo_id in enumerate(photo_ids):
            caption = media[index].caption if index < len(media) else None
            await bot.send_photo(chat_id=int(callback.from_user.id), photo=photo_id, caption=caption)

    details_lines = [
        f"🧾 <b>Перегляд звіту #{log_id}</b>",
        "",
        f"Мешканець: {resident_mention}",
        f"Зона: <b>{zone_label}</b>",
        f"Дата чергування: <b>{duty_date}</b>",
        f"Статус: <b>{status_label}</b>",
        f"Надіслано: <b>{str(log.get('created_at') or '—')}</b>",
    ]
    if log.get("reviewed_at"):
        details_lines.append(f"Перевірено: <b>{str(log['reviewed_at'])}</b>")
    if log.get("admin_comment"):
        details_lines.append(f"Коментар: {str(log['admin_comment'])}")

    reply_markup = admin_moderation_kb(log_id) if str(log.get("status")) == "pending" else None
    await bot.send_message(
        chat_id=int(callback.from_user.id),
        text="\n".join(details_lines),
        reply_markup=reply_markup,
    )
    await callback.answer("Звіт відкрито ✅", show_alert=True)


@router.callback_query(lambda c: c.data and c.data.startswith("report_history_filter:"))
async def filter_report_history(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_REPORTS_REVIEW):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    raw_value = callback.data.split(":", 1)[1]
    days = None if raw_value == "all" else int(raw_value)
    await callback.message.edit_text(
        await _report_history_text(db, days=days),
        reply_markup=await _report_history_kb(db, days=days),
    )
    await callback.answer("Фільтр оновлено ✅")


@router.callback_query(lambda c: c.data and c.data.startswith("history_export:"))
async def export_history_snapshot(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_HISTORY_VIEW):
        await callback.answer("Доступ обмежено", show_alert=True)
        return

    try:
        parts = callback.data.split(":")
        _, export_kind, export_format = parts[:3]
        raw_days = parts[3] if len(parts) > 3 else None
    except ValueError:
        await callback.answer("Невірний експорт", show_alert=True)
        return

    days = None if raw_days in (None, "all") else int(raw_days)
    try:
        filename, payload = await _build_history_export(db, export_kind, export_format, days=days)
    except Exception:
        await callback.answer("Не вдалося зібрати експорт", show_alert=True)
        return

    await bot.send_document(
        chat_id=int(callback.from_user.id),
        document=BufferedInputFile(payload, filename=filename),
    )
    await callback.answer("Експорт надіслано ✅", show_alert=True)


@router.callback_query(AdminFSM.choosing_delegate_resident, lambda c: c.data and c.data.startswith("delegate_pick:"))
async def delegate_pick(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_DELEGATES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    user_id = int(callback.data.split(":", 1)[1])
    await callback.message.edit_text(
        await _delegate_permissions_text(db, user_id),
        reply_markup=await _delegate_permissions_kb(db, user_id),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data and c.data.startswith("delegate_toggle:"))
async def delegate_toggle(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_DELEGATES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    _, user_id_raw, permission = callback.data.split(":", 2)
    user_id = int(user_id_raw)
    permissions = await get_user_permissions(db, user_id)
    if permission in permissions:
        permissions.remove(permission)
        action = "delegate_permission_off"
    else:
        permissions.add(permission)
        action = "delegate_permission_on"
    await set_user_permissions(db, user_id, permissions)
    await db.log_admin_action(
        int(callback.from_user.id),
        action,
        target_id=user_id,
        details=f"permission={permission}",
    )
    await notify_owner_about_delegate_action(
        callback.bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type=action,
        details=f"permission={permission}",
        target_id=user_id,
    )
    await callback.message.edit_text(
        await _delegate_permissions_text(db, user_id),
        reply_markup=await _delegate_permissions_kb(db, user_id),
    )
    await callback.answer("Права оновлено ✅", show_alert=True)


@router.callback_query(lambda c: c.data and c.data.startswith("delegate_clear:"))
async def delegate_clear(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_DELEGATES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    user_id = int(callback.data.split(":", 1)[1])
    await set_user_permissions(db, user_id, set())
    await db.log_admin_action(
        int(callback.from_user.id),
        "delegate_permissions_clear",
        target_id=user_id,
        details="all",
    )
    await notify_owner_about_delegate_action(
        callback.bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type="delegate_permissions_clear",
        details="all",
        target_id=user_id,
    )
    await callback.message.edit_text(
        await _delegate_permissions_text(db, user_id),
        reply_markup=await _delegate_permissions_kb(db, user_id),
    )
    await callback.answer("Усі права прибрано ✅", show_alert=True)


@router.callback_query(lambda c: c.data and c.data.startswith("report_route:set:"))
async def set_report_review_route(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    route = callback.data.split(":", 2)[2]
    if route not in REPORT_ROUTE_LABELS:
        await callback.answer("Невідомий режим", show_alert=True)
        return

    await db.set_setting(REPORT_REVIEW_ROUTE_KEY, route)
    await callback.message.edit_text(
        await _report_review_route_text(db),
        reply_markup=report_review_route_kb(route),
    )
    await callback.answer("Маршрут звітів оновлено ✅", show_alert=True)


@router.callback_query(AdminFSM.entering_skip_reminder_dates, lambda c: c.data == "skip_reminder_clear_all")
async def clear_all_skip_reminder_dates(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_EXCEPTIONS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await db.set_setting(REMINDER_SKIP_DATES_KEY, "[]")
    await db.log_admin_action(int(callback.from_user.id), "clear_skip_reminder_dates", details="all")
    await notify_owner_about_delegate_action(
        callback.bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type="clear_skip_reminder_dates",
        details="all",
    )
    await callback.message.edit_text(await _skip_reminders_text(db), reply_markup=await _skip_reminders_kb(db))
    await callback.answer("Усі дати очищено ✅", show_alert=True)


@router.callback_query(AdminFSM.entering_skip_reminder_dates, lambda c: c.data and c.data.startswith("skip_reminder_remove:"))
async def remove_skip_reminder_date(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_EXCEPTIONS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    target_iso = callback.data.split(":", 1)[1]
    skip_dates = [value for value in await _load_skip_reminder_dates(db) if value != target_iso]
    await db.set_setting(REMINDER_SKIP_DATES_KEY, json.dumps(skip_dates, ensure_ascii=False))
    await db.log_admin_action(int(callback.from_user.id), "remove_skip_reminder_date", details=target_iso)
    await notify_owner_about_delegate_action(
        callback.bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type="remove_skip_reminder_date",
        details=target_iso,
    )
    await callback.message.edit_text(await _skip_reminders_text(db), reply_markup=await _skip_reminders_kb(db))
    await callback.answer("Дату прибрано ✅", show_alert=True)


@router.callback_query(AdminFSM.choosing_calendar_exception_kind, lambda c: c.data == "calendar_exception_add")
async def start_calendar_exception_add(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_EXCEPTIONS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await callback.message.edit_text(
        await _calendar_exceptions_text(db) + "\n\nОберіть тип винятку нижче.",
        reply_markup=_calendar_exception_kind_kb(),
    )
    await callback.answer()


@router.callback_query(AdminFSM.choosing_calendar_exception_kind, lambda c: c.data and c.data.startswith("calendar_exception_remove:"))
async def remove_calendar_exception(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_EXCEPTIONS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    target_iso = callback.data.split(":", 1)[1]
    items = [item for item in await _load_calendar_exceptions(db) if str(item["date"]) != target_iso]
    await db.set_setting(CALENDAR_EXCEPTIONS_KEY, json.dumps(items, ensure_ascii=False))
    await db.log_admin_action(int(callback.from_user.id), "remove_calendar_exception", details=target_iso)
    await notify_owner_about_delegate_action(
        callback.bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type="remove_calendar_exception",
        details=target_iso,
    )
    await callback.message.edit_text(await _calendar_exceptions_text(db), reply_markup=await _calendar_exceptions_kb(db))
    await callback.answer("Виняток видалено ✅", show_alert=True)


@router.callback_query(AdminFSM.choosing_calendar_exception_kind, lambda c: c.data and c.data.startswith("calendar_exception_kind:"))
async def choose_calendar_exception_kind(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_EXCEPTIONS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    kind = callback.data.split(":", 1)[1]
    await state.update_data(calendar_exception_kind=kind)
    await state.set_state(AdminFSM.entering_calendar_exception_value)
    title = _calendar_exception_kind_label(kind)
    prompt = (
        f"🗓 <b>{title}</b>\n\n"
        "Надішли дату у форматі <b>ДД.ММ</b> або <b>ДД.ММ.РРРР</b>."
    )
    if kind == "special_rules":
        prompt += "\nДля примітки використай формат: <b>ДД.ММ | текст</b>."
    else:
        prompt += "\nМожеш додати коротку примітку так: <b>ДД.ММ | текст</b>."
    await callback.message.edit_text(prompt, reply_markup=_back_to_panel_kb())
    await callback.answer()


@router.callback_query(lambda c: c.data and c.data.startswith("restore_pick:"))
async def restore_pick(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_BACKUPS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    backup_name = callback.data.split(":", 1)[1]
    await state.set_state(AdminFSM.confirming_restore)
    await state.update_data(restore_backup_name=backup_name)
    await callback.message.edit_text(
        "♻️ <b>Підтвердження відновлення</b>\n\n"
        f"Бекап: <b>{backup_name}</b>\n"
        "Перед відновленням буде створено страхувальну копію поточного стану.\n"
        "Після відновлення варто перезапустити бота.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Відновити", callback_data="restore_confirm")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:restore_menu")],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(AdminFSM.confirming_restore, lambda c: c.data == "restore_confirm")
async def restore_confirm(callback: CallbackQuery, state: FSMContext, db: Database, backup_service=None) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_BACKUPS_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    if not backup_service:
        await callback.answer("Сервіс бекапу недоступний", show_alert=True)
        return
    data = await state.get_data()
    backup_name = str(data.get("restore_backup_name") or "")
    if not backup_name:
        await state.clear()
        await callback.answer("Бекап не вибрано", show_alert=True)
        return
    safety_backup = await backup_service.restore_backup(backup_name)
    await db.log_admin_action(
        int(callback.from_user.id),
        "restore_backup",
        details=f"source={backup_name}|safety={safety_backup.name}",
    )
    await notify_owner_about_delegate_action(
        callback.bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type="restore_backup",
        details=f"source={backup_name}|safety={safety_backup.name}",
    )
    await state.clear()
    await callback.message.edit_text(
        "✅ <b>Відновлення завершено</b>\n\n"
        f"Відновлено з: <b>{backup_name}</b>\n"
        f"Страхувальний бекап: <b>{safety_backup.name}</b>\n"
        "Для повної надійності перезапусти бота.",
        reply_markup=_admin_section_back_kb(),
    )
    await callback.answer("Відновлено ✅", show_alert=True)


@router.callback_query(AdminFSM.confirming_reset_db, lambda c: c.data == "reset_db_confirm")
async def reset_db_confirm(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    await db.reset_database(RESIDENTS_JSON_PATH)
    await db.set_setting("app_env", APP_ENV)
    await db.set_setting("app_version", APP_VERSION)
    await db.set_setting("db_path_shadow", DB_PATH)
    await db.set_setting("test_mode", "0")
    await db.log_admin_action(int(callback.from_user.id), "reset_database", details="manual_reset")
    await state.clear()
    await callback.message.edit_text(
        "✅ <b>Базу очищено і створено заново</b>\n\n"
        "Схему перестворено, мешканців синхронізовано з <b>residents.json</b>, тестовий режим вимкнено.",
        reply_markup=_admin_section_back_kb(),
    )
    await callback.answer("Базу очищено ✅", show_alert=True)


@router.callback_query(AdminFSM.choosing_time_job, lambda c: c.data and c.data.startswith("time_job:"))
async def choose_time_job(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    job_id = callback.data.split(":", 1)[1]
    await state.update_data(job_id=job_id)
    await state.set_state(AdminFSM.entering_time)
    await callback.answer()
    await callback.message.edit_text("Введи час у форматі <b>HH:MM</b>.", reply_markup=_back_to_panel_kb())


@router.callback_query(AdminFSM.choosing_time_job, lambda c: c.data == "time_deadlines")
async def open_deadline_settings(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(AdminFSM.choosing_deadline_zone)
    await callback.message.edit_text(
        "⏳ <b>Дедлайни звітів</b>\n\nОберіть зону. "
        "Час до 12:00 вважається дедлайном наступного дня, а після 12:00 — того ж дня.",
        reply_markup=await _deadline_settings_kb(db),
    )
    await callback.answer()


@router.callback_query(
    lambda c: c.data == "time_deadlines"
)
async def open_deadline_settings_any(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(AdminFSM.choosing_deadline_zone)
    await callback.message.edit_text(
        "⏳ <b>Дедлайни звітів</b>\n\nОберіть зону. "
        "Час до 12:00 вважається дедлайном наступного дня, а після 12:00 — того ж дня.",
        reply_markup=await _deadline_settings_kb(db),
    )
    await callback.answer()


@router.callback_query(AdminFSM.choosing_deadline_zone, lambda c: c.data and c.data.startswith("deadline_zone:"))
async def choose_deadline_zone(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    zone_name = callback.data.split(":", 1)[1]
    allowed_zones = {zone_name for zone_name, _ in await list_enabled_zone_choices(db, report_required_only=True)}
    if zone_name not in allowed_zones:
        await callback.answer("Зона недоступна", show_alert=True)
        return
    await state.update_data(deadline_zone=zone_name)
    await state.set_state(AdminFSM.entering_deadline_time)
    zone_title = await get_zone_title(db, zone_name)
    await callback.message.edit_text(
        f"⏳ <b>Дедлайн для зони «{zone_title}»</b>\n\n"
        "Введіть час у форматі <b>HH:MM</b>.\n"
        "Приклади:\n"
        "• <b>01:00</b> — дедлайн вночі наступного дня\n"
        "• <b>23:59</b> — дедлайн увечері цього ж дня",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="time_deadlines")]]
        ),
    )
    await callback.answer()


@router.message(AdminFSM.entering_deadline_time)
async def enter_deadline_time(message: Message, state: FSMContext, db: Database, bot) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_SCHEDULE_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return

    hhmm = (message.text or "").strip()
    try:
        hour_text, minute_text = hhmm.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
    except Exception:
        await _cleanup_user_input(message)
        await message.answer("Невірний формат. Приклад: <b>01:00</b> або <b>23:59</b>.")
        return

    data = await state.get_data()
    zone_name = str(data.get("deadline_zone") or "")
    allowed_zones = {zone_name for zone_name, _ in await list_enabled_zone_choices(db, report_required_only=True)}
    if zone_name not in allowed_zones:
        await state.clear()
        await _cleanup_user_input(message)
        await message.answer("Контекст втрачено. Спробуйте ще раз через адмін-меню.")
        return

    raw = await db.get_setting("deadline_defaults_json", "")
    defaults: dict[str, str] = {}
    try:
        parsed = json.loads(raw) if raw else {}
        if isinstance(parsed, dict):
            defaults.update({str(k): str(v) for k, v in parsed.items()})
    except Exception:
        pass
    defaults[zone_name] = f"{hour:02d}:{minute:02d}"
    await db.set_setting("deadline_defaults_json", json.dumps(defaults, ensure_ascii=False))
    await db.log_admin_action(
        int(resident["telegram_id"]),
        "update_deadline",
        details=f"{zone_name}={defaults[zone_name]}",
    )
    await notify_owner_about_delegate_action(
        bot,
        db,
        actor_id=int(resident["telegram_id"]),
        action_type="update_deadline",
        details=f"{zone_name}={defaults[zone_name]}",
    )

    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.choosing_deadline_zone)
    await _edit_admin_message(
        state,
        bot,
        text="⏳ <b>Дедлайни звітів</b>\n\nОберіть зону. "
        "Час до 12:00 вважається дедлайном наступного дня, а після 12:00 — того ж дня.",
        reply_markup=await _deadline_settings_kb(db),
    )
    await message.answer(f"✅ Дедлайн для зони «{await get_zone_title(db, zone_name)}» оновлено: {defaults[zone_name]}")


@router.message(AdminFSM.entering_time)
async def enter_time(message: Message, state: FSMContext, db: Database, scheduler_service=None) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_SCHEDULE_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return

    hhmm = (message.text or "").strip()
    data = await state.get_data()
    job_id = str(data.get("job_id") or "")
    if not job_id or not scheduler_service:
        await state.clear()
        await _cleanup_user_input(message)
        await message.answer("> ⚠️ Scheduler не доступний.")
        return

    try:
        await scheduler_service.reschedule(job_id, hhmm)
    except Exception:
        await _cleanup_user_input(message)
        await message.answer("Невірний формат. Приклад: 09:30")
        return
    await db.log_admin_action(int(resident["telegram_id"]), "update_job_time", details=f"{job_id}={hhmm}")
    await notify_owner_about_delegate_action(
        message.bot,
        db,
        actor_id=int(resident["telegram_id"]),
        action_type="update_job_time",
        details=f"{job_id}={hhmm}",
    )
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await admin_panel_text(db, int(resident["telegram_id"])),
        reply_markup=await admin_kb(db, int(resident["telegram_id"]), include_back=True),
    )
    await state.clear()
    await message.answer(f"> ✅ Оновлено: {job_id} → {hhmm}")


@router.message(AdminFSM.entering_bank_url)
async def enter_bank_url(message: Message, state: FSMContext, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_PAYMENTS_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return
    url = (message.text or "").strip()
    if not url:
        await _cleanup_user_input(message)
        await message.answer("Встав посилання текстом.")
        return
    await db.set_setting("fine_bank_url", url)
    await db.log_admin_action(int(resident["telegram_id"]), "update_bank_url", details="updated")
    await notify_owner_about_delegate_action(
        message.bot,
        db,
        actor_id=int(resident["telegram_id"]),
        action_type="update_bank_url",
        details="updated",
    )
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await admin_panel_text(db, int(resident["telegram_id"])),
        reply_markup=await admin_kb(db, int(resident["telegram_id"]), include_back=True),
    )
    await state.clear()
    await message.answer("> ✅ Банку оновлено.")


@router.message(AdminFSM.entering_payment_folder)
async def enter_payment_folder(message: Message, state: FSMContext, db: Database, bot) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_PAYMENTS_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return

    data = await state.get_data()
    target_id = int(data.get("payment_resident_id") or 0)
    target_resident = await db.get_resident(target_id) if target_id else None
    if not target_resident:
        await state.clear()
        await _cleanup_user_input(message)
        await message.answer("Контекст втрачено. Спробуйте ще раз через адмінку.")
        return

    raw_value = (message.text or "").strip()
    if not raw_value:
        await _cleanup_user_input(message)
        await message.answer("Вставте посилання текстом або <b>-</b> для очищення.")
        return

    setting_key = f"payment_folder:{target_id}"
    normalized_value = "" if raw_value == "-" else raw_value
    await db.set_setting(setting_key, normalized_value)
    await db.log_admin_action(
        int(resident["telegram_id"]),
        "update_payment_folder",
        details=f"target_id={target_id}|cleared={1 if not normalized_value else 0}",
        target_id=target_id,
    )
    await notify_owner_about_delegate_action(
        bot,
        db,
        actor_id=int(resident["telegram_id"]),
        action_type="update_payment_folder",
        details=f"target_id={target_id}|cleared={1 if not normalized_value else 0}",
        target_id=target_id,
    )

    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.choosing_payment_resident)
    await _edit_admin_message(
        state,
        bot,
        text=await _payment_folders_text(db),
        reply_markup=await _payment_folders_kb(db),
    )
    if normalized_value:
        await message.answer(f"✅ Папку оплат для <b>{target_resident['full_name']}</b> збережено.")
    else:
        await message.answer(f"✅ Папку оплат для <b>{target_resident['full_name']}</b> очищено.")


@router.message(AdminFSM.entering_skip_reminder_dates)
async def enter_skip_reminder_dates(message: Message, state: FSMContext, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_EXCEPTIONS_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return

    raw_value = (message.text or "").strip()
    if not raw_value:
        await _cleanup_user_input(message)
        await message.answer("Надішли одну або кілька дат через кому.")
        return

    parts = [part.strip() for part in raw_value.replace("\n", ",").split(",") if part.strip()]
    if not parts:
        await _cleanup_user_input(message)
        await message.answer("Надішли одну або кілька дат через кому.")
        return

    existing_dates = set(await _load_skip_reminder_dates(db))
    added_dates: list[str] = []
    for part in parts:
        try:
            parsed_date = parse_user_date(part)
        except ValueError:
            await _cleanup_user_input(message)
            await message.answer(f"Не вдалося розібрати дату: <b>{part}</b>.")
            return
        iso_value = parsed_date.isoformat()
        if iso_value not in existing_dates:
            existing_dates.add(iso_value)
            added_dates.append(iso_value)

    await db.set_setting(REMINDER_SKIP_DATES_KEY, json.dumps(sorted(existing_dates), ensure_ascii=False))
    await db.log_admin_action(
        int(resident["telegram_id"]),
        "add_skip_reminder_dates",
        details=",".join(added_dates) if added_dates else "no_changes",
    )
    await notify_owner_about_delegate_action(
        message.bot,
        db,
        actor_id=int(resident["telegram_id"]),
        action_type="add_skip_reminder_dates",
        details=",".join(added_dates) if added_dates else "no_changes",
    )

    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _skip_reminders_text(db),
        reply_markup=await _skip_reminders_kb(db),
    )
    if added_dates:
        formatted = ", ".join(datetime.fromisoformat(value).strftime("%d.%m.%Y") for value in added_dates)
        await message.answer(f"✅ Додано дати пропуску: <b>{formatted}</b>.")
    else:
        await message.answer("ℹ️ Усі ці дати вже були в списку.")


@router.message(AdminFSM.entering_calendar_exception_value)
async def enter_calendar_exception_value(message: Message, state: FSMContext, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_EXCEPTIONS_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return

    data = await state.get_data()
    kind = str(data.get("calendar_exception_kind") or "")
    raw_value = (message.text or "").strip()
    if not raw_value or kind not in {"holiday", "day_off", "special_rules"}:
        await _cleanup_user_input(message)
        await message.answer("Контекст втрачено. Відкрий календар винятків ще раз.")
        return

    date_part, note_part = raw_value, ""
    if "|" in raw_value:
        date_part, note_part = [part.strip() for part in raw_value.split("|", 1)]
    try:
        target_date = parse_user_date(date_part)
    except ValueError:
        await _cleanup_user_input(message)
        await message.answer("Не вдалося розібрати дату. Приклад: <b>25.03</b> або <b>25.03 | Державне свято</b>.")
        return

    items = [item for item in await _load_calendar_exceptions(db) if str(item["date"]) != target_date.isoformat()]
    items.append({"date": target_date.isoformat(), "kind": kind, "note": note_part})
    items.sort(key=lambda item: str(item["date"]))
    await db.set_setting(CALENDAR_EXCEPTIONS_KEY, json.dumps(items, ensure_ascii=False))
    await db.log_admin_action(
        int(resident["telegram_id"]),
        "set_calendar_exception",
        details=f"date={target_date.isoformat()}|kind={kind}|note={note_part}",
    )
    await notify_owner_about_delegate_action(
        message.bot,
        db,
        actor_id=int(resident["telegram_id"]),
        action_type="set_calendar_exception",
        details=f"date={target_date.isoformat()}|kind={kind}|note={note_part}",
    )

    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.choosing_calendar_exception_kind)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _calendar_exceptions_text(db),
        reply_markup=await _calendar_exceptions_kb(db),
    )
    await message.answer(
        f"✅ Додано виняток: <b>{target_date.strftime('%d.%m.%Y')}</b> — <b>{_calendar_exception_kind_label(kind)}</b>."
    )


@router.message(AdminFSM.entering_export_period)
async def export_duty_period(message: Message, state: FSMContext, db: Database, bot) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_SCHEDULE_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return

    try:
        start_date, end_date = parse_date_period(message.text or "")
    except ValueError:
        await _cleanup_user_input(message)
        await message.answer("Невірний період. Приклад: <b>01.03-31.03</b>.")
        return

    filename, csv_bytes = await _build_export_csv(db, start_date, end_date)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        bot,
        text=await admin_panel_text(db, int(resident["telegram_id"])),
        reply_markup=await admin_kb(db, int(resident["telegram_id"]), include_back=True),
    )
    await state.clear()
    await bot.send_document(
        chat_id=message.chat.id,
        document=BufferedInputFile(csv_bytes, filename=filename),
        caption=f"📤 Експорт чергувань: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}",
    )


@router.callback_query(lambda c: c.data and c.data.startswith("test_whitelist:"))
async def toggle_test_whitelist(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_TEST_MODE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    if not await is_test_mode_enabled(db):
        await callback.answer("Увімкніть тестовий режим спочатку.", show_alert=True)
        return
    resident_id = int(callback.data.split(":", 1)[1])
    if not await db.is_active_resident(resident_id):
        await callback.answer("Доступний лише активний мешканець.", show_alert=True)
        return
    enabled = not await db.is_test_whitelisted(resident_id)
    await db.set_test_whitelist(resident_id, enabled)
    await db.log_admin_action(
        int(callback.from_user.id),
        "toggle_test_whitelist",
        target_id=resident_id,
        details="on" if enabled else "off",
    )
    await callback.message.edit_text(
        "👥 <b>Тестувальники</b>\n\nТут показані лише активні мешканці. "
        "Позначені люди матимуть доступ до тестових сценаріїв.",
        reply_markup=await _test_whitelist_kb(db),
    )
    await callback.answer("Тестувальника оновлено ✅")


@router.message(AdminFSM.entering_override_date)
async def receive_override_date(message: Message, state: FSMContext, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_TEST_MODE_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return
    try:
        target_date = parse_user_date(message.text or "")
    except ValueError:
        await _cleanup_user_input(message)
        await message.answer("Невірний формат дати. Приклад: <b>21.03</b>.")
        return
    await state.update_data(override_date=target_date.isoformat())
    await state.set_state(AdminFSM.choosing_override_zone)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _test_override_date_text(db, target_date),
        reply_markup=await _test_override_date_kb_for(db, target_date),
    )


@router.callback_query(AdminFSM.choosing_override_zone, lambda c: c.data and c.data.startswith("test_override_zone:"))
async def choose_override_zone(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_TEST_MODE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    zone_name = callback.data.split(":", 1)[1]
    await state.update_data(override_zone=zone_name)
    await state.set_state(AdminFSM.choosing_override_first_user)
    data = await state.get_data()
    from datetime import date

    target_date = date.fromisoformat(str(data.get("override_date")))
    zone_title = await get_zone_title(db, zone_name)
    await callback.answer()
    await callback.message.edit_text(
        f"🎯 <b>Тестові черги / {target_date.strftime('%d.%m.%Y')} / {zone_title}</b>\n\n"
        "Оберіть першого мешканця зі списку для цієї зони.",
        reply_markup=await _override_residents_kb(db, callback_prefix="test_override_user", zone_name=zone_name),
    )


@router.callback_query(AdminFSM.choosing_override_zone, lambda c: c.data == "test_override_clear_date")
async def clear_override_date(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_TEST_MODE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    data = await state.get_data()
    override_date_raw = str(data.get("override_date") or "")
    if not override_date_raw:
        await callback.answer("Дата не вибрана", show_alert=True)
        return
    from datetime import date

    target_date = date.fromisoformat(override_date_raw)
    await db.clear_test_overrides_for_date(target_date)
    await callback.message.edit_text(
        await _test_override_date_text(db, target_date),
        reply_markup=await _test_override_date_kb_for(db, target_date),
    )
    await callback.answer("Тестові черги за дату очищено ✅", show_alert=True)


@router.callback_query(lambda c: c.data and c.data.startswith("test_override_reopen:"))
async def reopen_override_date(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_TEST_MODE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    from datetime import date

    target_date = date.fromisoformat(callback.data.split(":", 1)[1])
    await state.update_data(override_date=target_date.isoformat())
    await state.set_state(AdminFSM.choosing_override_zone)
    await callback.message.edit_text(
        await _test_override_date_text(db, target_date),
        reply_markup=await _test_override_date_kb_for(db, target_date),
    )
    await callback.answer()


@router.callback_query(
    AdminFSM.choosing_override_first_user,
    lambda c: c.data and c.data.startswith("test_override_user:"),
)
async def choose_override_first_user(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_TEST_MODE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    resident_id = int(callback.data.split(":", 1)[1])
    data = await state.get_data()
    zone_name = str(data.get("override_zone") or "")
    from datetime import date

    await state.update_data(override_first_user_id=resident_id)
    target_date = date.fromisoformat(str(data.get("override_date")))
    selected_ids = [resident_id]
    required_slots = await _override_required_slots(db, zone_name, target_date)
    if len(selected_ids) < required_slots:
        await state.update_data(override_selected_user_ids=selected_ids)
        await state.set_state(AdminFSM.choosing_override_second_user)
        zone_title = await get_zone_title(db, zone_name)
        await callback.answer()
        await callback.message.edit_text(
            f"🎯 <b>Тестові черги / {zone_title}</b>\n\n"
            f"Оберіть ще мешканців для цієї зони. Потрібно: <b>{required_slots}</b>.",
            reply_markup=await _override_residents_kb(
                db,
                callback_prefix="test_override_user",
                selected_ids={resident_id},
                zone_name=zone_name,
            ),
        )
        return
    await _save_test_override(
        callback.message,
        state,
        db,
        zone_name=zone_name,
        resident_ids=[resident_id],
    )
    await callback.answer("Тестову чергу збережено ✅")


@router.callback_query(
    AdminFSM.choosing_override_second_user,
    lambda c: c.data and c.data.startswith("test_override_user:"),
)
async def choose_override_second_user(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_TEST_MODE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    second_id = int(callback.data.split(":", 1)[1])
    data = await state.get_data()
    selected_ids = [int(item) for item in (data.get("override_selected_user_ids") or []) if int(item)]
    first_id = int(data.get("override_first_user_id", 0))
    zone_name = str(data.get("override_zone") or "")
    from datetime import date

    target_date = date.fromisoformat(str(data.get("override_date")))
    if first_id and not selected_ids:
        selected_ids = [first_id]
    if second_id in set(selected_ids):
        await callback.answer("Оберіть іншого мешканця", show_alert=True)
        return
    selected_ids.append(second_id)
    required_slots = await _override_required_slots(db, zone_name, target_date)
    if len(selected_ids) < required_slots:
        await state.update_data(override_selected_user_ids=selected_ids)
        zone_title = await get_zone_title(db, zone_name)
        await callback.answer()
        await callback.message.edit_text(
            f"🎯 <b>Тестові черги / {zone_title}</b>\n\n"
            f"Оберіть ще мешканців для цієї зони. Уже вибрано: <b>{len(selected_ids)}/{required_slots}</b>.",
            reply_markup=await _override_residents_kb(
                db,
                callback_prefix="test_override_user",
                selected_ids=set(selected_ids),
                zone_name=zone_name,
            ),
        )
        return
    await _save_test_override(
        callback.message,
        state,
        db,
        zone_name=zone_name,
        resident_ids=selected_ids,
    )
    await callback.answer("Тестову чергу збережено ✅")


async def _save_test_override(
    message: Message,
    state: FSMContext,
    db: Database,
    *,
    zone_name: str,
    resident_ids: list[int],
) -> None:
    data = await state.get_data()
    override_date_raw = str(data.get("override_date") or "")
    from datetime import date

    target_date = date.fromisoformat(override_date_raw)
    await db.set_test_override(target_date, zone_name, resident_ids)
    residents = await db.get_residents_by_ids(resident_ids)
    resident_names = ", ".join(
        canonical_full_name(resident.get("full_name"), resident.get("username")) for resident in residents
    )
    zone_ua = await get_zone_title(db, zone_name)
    await message.edit_text(
        f"✅ <b>Тестову чергу збережено</b>\n\nДата: <b>{target_date.strftime('%d.%m.%Y')}</b>\n"
        f"Зона: <b>{zone_ua}</b>\n"
        f"Мешканці: <b>{resident_names}</b>\n\n"
        "Можна повернутися до цієї дати і змінити інші зони.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⬅️ До цієї дати",
                        callback_data=f"test_override_reopen:{target_date.isoformat()}",
                    )
                ],
                [InlineKeyboardButton(text="⬅️ До адмін-панелі", callback_data="admin:back_to_panel")],
            ]
        ),
    )
    await state.clear()


@router.message(AdminFSM.entering_manual_override_date)
async def receive_manual_override_date(message: Message, state: FSMContext, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_SCHEDULE_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return
    try:
        target_date = parse_user_date(message.text or "")
    except ValueError:
        await _cleanup_user_input(message)
        await message.answer("Невірний формат дати. Приклад: <b>21.03</b>.")
        return
    await state.update_data(manual_override_date=target_date.isoformat())
    await state.set_state(AdminFSM.choosing_manual_override_zone)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _manual_override_date_text(db, target_date),
        reply_markup=await _manual_override_date_kb_for(db, target_date),
    )


@router.callback_query(AdminFSM.choosing_manual_override_zone, lambda c: c.data and c.data.startswith("manual_override_zone:"))
async def choose_manual_override_zone(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    zone_name = callback.data.split(":", 1)[1]
    await state.update_data(manual_override_zone=zone_name)
    await state.set_state(AdminFSM.choosing_manual_override_first_user)
    data = await state.get_data()
    from datetime import date

    target_date = date.fromisoformat(str(data.get("manual_override_date")))
    zone_title = await get_zone_title(db, zone_name)
    await callback.answer()
    await callback.message.edit_text(
        f"🔁 <b>Ручна заміна / {target_date.strftime('%d.%m.%Y')} / {zone_title}</b>\n\n"
        "Оберіть першого мешканця зі списку для цієї зони.",
        reply_markup=await _override_residents_kb(db, callback_prefix="manual_override_user", zone_name=zone_name),
    )


@router.callback_query(AdminFSM.choosing_manual_override_zone, lambda c: c.data == "manual_override_clear_date")
async def clear_manual_override_date(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    data = await state.get_data()
    override_date_raw = str(data.get("manual_override_date") or "")
    if not override_date_raw:
        await callback.answer("Дата не вибрана", show_alert=True)
        return
    from datetime import date

    target_date = date.fromisoformat(override_date_raw)
    await db.clear_manual_overrides_for_date(target_date)
    await db.log_admin_action(
        int(callback.from_user.id),
        "clear_manual_override",
        details=f"date={target_date.isoformat()}",
    )
    await notify_owner_about_delegate_action(
        callback.bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type="clear_manual_override",
        details=f"date={target_date.isoformat()}",
    )
    await callback.message.edit_text(
        await _manual_override_date_text(db, target_date),
        reply_markup=await _manual_override_date_kb_for(db, target_date),
    )
    await callback.answer("Ручні заміни за дату очищено ✅", show_alert=True)


@router.callback_query(lambda c: c.data and c.data.startswith("manual_override_reopen:"))
async def reopen_manual_override_date(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    from datetime import date

    target_date = date.fromisoformat(callback.data.split(":", 1)[1])
    await state.update_data(manual_override_date=target_date.isoformat())
    await state.set_state(AdminFSM.choosing_manual_override_zone)
    await callback.message.edit_text(
        await _manual_override_date_text(db, target_date),
        reply_markup=await _manual_override_date_kb_for(db, target_date),
    )
    await callback.answer()


@router.callback_query(
    AdminFSM.choosing_manual_override_first_user,
    lambda c: c.data and c.data.startswith("manual_override_user:"),
)
async def choose_manual_override_first_user(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    resident_id = int(callback.data.split(":", 1)[1])
    data = await state.get_data()
    zone_name = str(data.get("manual_override_zone") or "")
    from datetime import date

    await state.update_data(manual_override_first_user_id=resident_id)
    target_date = date.fromisoformat(str(data.get("manual_override_date")))
    selected_ids = [resident_id]
    required_slots = await _override_required_slots(db, zone_name, target_date)
    if len(selected_ids) < required_slots:
        await state.update_data(manual_override_selected_user_ids=selected_ids)
        await state.set_state(AdminFSM.choosing_manual_override_second_user)
        zone_title = await get_zone_title(db, zone_name)
        await callback.answer()
        await callback.message.edit_text(
            f"🔁 <b>Ручна заміна / {zone_title}</b>\n\n"
            f"Оберіть ще мешканців для цієї зони. Потрібно: <b>{required_slots}</b>.",
            reply_markup=await _override_residents_kb(
                db,
                callback_prefix="manual_override_user",
                selected_ids={resident_id},
                zone_name=zone_name,
            ),
        )
        return
    await _save_manual_override(
        callback.message,
        state,
        db,
        zone_name=zone_name,
        resident_ids=[resident_id],
        admin_id=int(callback.from_user.id) if callback.from_user else None,
    )
    await callback.answer("Ручну заміну збережено ✅")


@router.callback_query(
    AdminFSM.choosing_manual_override_second_user,
    lambda c: c.data and c.data.startswith("manual_override_user:"),
)
async def choose_manual_override_second_user(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SCHEDULE_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    second_id = int(callback.data.split(":", 1)[1])
    data = await state.get_data()
    selected_ids = [int(item) for item in (data.get("manual_override_selected_user_ids") or []) if int(item)]
    first_id = int(data.get("manual_override_first_user_id", 0))
    zone_name = str(data.get("manual_override_zone") or "")
    from datetime import date

    target_date = date.fromisoformat(str(data.get("manual_override_date")))
    if first_id and not selected_ids:
        selected_ids = [first_id]
    if second_id in set(selected_ids):
        await callback.answer("Оберіть іншого мешканця", show_alert=True)
        return
    selected_ids.append(second_id)
    required_slots = await _override_required_slots(db, zone_name, target_date)
    if len(selected_ids) < required_slots:
        await state.update_data(manual_override_selected_user_ids=selected_ids)
        zone_title = await get_zone_title(db, zone_name)
        await callback.answer()
        await callback.message.edit_text(
            f"🔁 <b>Ручна заміна / {zone_title}</b>\n\n"
            f"Оберіть ще мешканців для цієї зони. Уже вибрано: <b>{len(selected_ids)}/{required_slots}</b>.",
            reply_markup=await _override_residents_kb(
                db,
                callback_prefix="manual_override_user",
                selected_ids=set(selected_ids),
                zone_name=zone_name,
            ),
        )
        return
    await _save_manual_override(
        callback.message,
        state,
        db,
        zone_name=zone_name,
        resident_ids=selected_ids,
        admin_id=int(callback.from_user.id) if callback.from_user else None,
    )
    await callback.answer("Ручну заміну збережено ✅")


async def _save_manual_override(
    message: Message,
    state: FSMContext,
    db: Database,
    *,
    zone_name: str,
    resident_ids: list[int],
    admin_id: int | None,
) -> None:
    data = await state.get_data()
    override_date_raw = str(data.get("manual_override_date") or "")
    from datetime import date

    target_date = date.fromisoformat(override_date_raw)
    await db.set_manual_override(target_date, zone_name, resident_ids)
    residents = await db.get_residents_by_ids(resident_ids)
    resident_names = ", ".join(
        canonical_full_name(resident.get("full_name"), resident.get("username")) for resident in residents
    )
    zone_ua = await get_zone_title(db, zone_name)
    if admin_id:
        await db.log_admin_action(
            admin_id,
            "manual_override",
            details=f"date={target_date.isoformat()}|zone={zone_name}|ids={','.join(map(str, resident_ids))}",
        )
        await notify_owner_about_delegate_action(
            message.bot,
            db,
            actor_id=admin_id,
            action_type="manual_override",
            details=f"date={target_date.isoformat()}|zone={zone_name}|ids={','.join(map(str, resident_ids))}",
        )
    await message.edit_text(
        f"✅ <b>Ручну заміну збережено</b>\n\nДата: <b>{target_date.strftime('%d.%m.%Y')}</b>\n"
        f"Зона: <b>{zone_ua}</b>\n"
        f"Мешканці: <b>{resident_names}</b>\n\n"
        "Ця заміна має пріоритет над звичайним графіком.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⬅️ До цієї дати",
                        callback_data=f"manual_override_reopen:{target_date.isoformat()}",
                    )
                ],
                [InlineKeyboardButton(text="⬅️ До адмін-панелі", callback_data="admin:back_to_panel")],
            ]
        ),
    )
    await state.clear()


@router.callback_query(lambda c: c.data == "runtime_setup:toggle")
async def toggle_runtime_setup(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    definition = await load_instance_definition(db)
    updated_definition = replace(
        definition,
        settings=replace(definition.settings, setup_complete=not definition.settings.setup_complete),
    )
    await store_instance_definition(db, updated_definition)
    await callback.message.edit_text(
        await _runtime_config_text(db),
        reply_markup=_runtime_config_kb(updated_definition, can_manage=True),
    )
    await callback.answer("Статус setup оновлено ✅", show_alert=True)


@router.callback_query(lambda c: c.data == "runtime_export:json")
async def export_runtime_config(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SYSTEM_VIEW):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    definition = await load_instance_definition(db)
    residents = await db.list_all_residents_full()
    payload = json.dumps(instance_bundle_to_dict(definition, residents), ensure_ascii=False, indent=2).encode("utf-8")
    filename = f"runtime_config_{kyiv_today().isoformat()}.json"
    await bot.send_document(
        chat_id=int(callback.from_user.id),
        document=BufferedInputFile(payload, filename=filename),
        caption="⚙️ Експорт повного instance bundle: settings, modules, zones і мешканці.",
    )
    await callback.answer("JSON надіслано в ПП ✅", show_alert=True)


@router.callback_query(lambda c: c.data == "runtime_export:yaml")
async def export_runtime_config_yaml(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SYSTEM_VIEW):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not _yaml_available():
        await callback.answer("Для YAML потрібен пакет PyYAML", show_alert=True)
        return
    import yaml

    definition = await load_instance_definition(db)
    residents = await db.list_all_residents_full()
    payload = yaml.safe_dump(
        instance_bundle_to_dict(definition, residents),
        allow_unicode=True,
        sort_keys=False,
    ).encode("utf-8")
    filename = f"runtime_config_{kyiv_today().isoformat()}.yaml"
    await bot.send_document(
        chat_id=int(callback.from_user.id),
        document=BufferedInputFile(payload, filename=filename),
        caption="⚙️ Експорт повного instance bundle у YAML.",
    )
    await callback.answer("YAML надіслано в ПП ✅", show_alert=True)


@router.callback_query(lambda c: c.data == "runtime_import:start")
async def start_runtime_import(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(AdminFSM.entering_runtime_import_json)
    await _remember_admin_message(state, callback.message)
    await callback.message.edit_text(
        "📥 <b>Імпорт instance bundle</b>\n\n"
        "Встав JSON/YAML-конфіг одним повідомленням або надішли `.json` / `.yaml` файл.\n"
        "Після імпорту instance settings, feature flags і зони буде замінено новими даними.",
        reply_markup=_runtime_setup_wizard_back_kb(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "runtime_setup:start")
async def start_runtime_setup(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(AdminFSM.entering_setup_coliving_name)
    await _remember_admin_message(state, callback.message)
    await callback.message.edit_text(
        "🧩 <b>Setup wizard / Крок 1</b>\n\n"
        "Введіть назву вашого coliving.\n"
        "Наприклад: <b>Kyiv Coliving 7</b>",
        reply_markup=_runtime_setup_wizard_back_kb(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "runtime_setup:residents")
async def start_runtime_setup_residents(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(AdminFSM.entering_setup_residents)
    await _remember_admin_message(state, callback.message)
    await callback.message.edit_text(
        "👥 <b>Setup wizard / Мешканці</b>\n\n"
        "Надішліть мешканців построково у форматі:\n"
        "<code>telegram_id | Повне ім'я | @username</code>\n\n"
        "Приклад:\n"
        "<code>123456789 | Іван Петренко | @ivan</code>\n"
        "<code>987654321 | Олена Іваненко</code>\n\n"
        "Усі мешканці, яких не буде в новому списку, стануть неактивними.",
        reply_markup=_runtime_section_back_kb("runtime_setup:summary"),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "runtime_setup:summary")
async def open_runtime_setup_summary(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await callback.message.edit_text(
        await _runtime_setup_summary_text(db),
        reply_markup=_runtime_setup_summary_kb(),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "runtime_setup:zones")
async def open_runtime_setup_zones(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    definition = await load_instance_definition(db)
    await callback.message.edit_text(
        "🗂 <b>Setup wizard / Зони, учасники, правила</b>\n\n"
        "Тут можна створювати зони, задавати pattern, крок днів і склад учасників для кожної зони.",
        reply_markup=_runtime_zones_kb(definition, back_target="runtime_setup:summary"),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "runtime_setup:flags")
async def open_runtime_setup_flags(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    definition = await load_instance_definition(db)
    await callback.message.edit_text(
        await _runtime_flags_text(db),
        reply_markup=_runtime_flags_kb(definition, can_manage=True, back_target="runtime_setup:summary"),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "runtime_zone_add:start")
async def start_runtime_zone_add(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(AdminFSM.entering_runtime_zone_code)
    await _remember_admin_message(state, callback.message)
    await callback.message.edit_text(
        "➕ <b>Майстер нової зони / Крок 1</b>\n\n"
        "Введіть технічний code нової зони.\n"
        "Формат: латиниця, цифри, `_` або `-`.\n"
        "Приклад: <b>laundry_2</b>",
        reply_markup=_runtime_section_back_kb("runtime_setup:zones"),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data and c.data.startswith("runtime_flag_toggle:"))
async def toggle_runtime_flag(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    flag_key = str(callback.data.split(":", 1)[1])
    if flag_key not in default_feature_flags():
        await callback.answer("Невідомий модуль", show_alert=True)
        return
    definition = await load_instance_definition(db)
    updated_flags = dict(definition.feature_flags)
    updated_flags[flag_key] = not updated_flags.get(flag_key, False)
    updated_definition = replace(definition, feature_flags=updated_flags)
    await store_instance_definition(db, updated_definition)
    await callback.message.edit_text(
        await _runtime_flags_text(db),
        reply_markup=_runtime_flags_kb(updated_definition, can_manage=True),
    )
    await callback.answer("Модуль оновлено ✅", show_alert=True)


@router.callback_query(lambda c: c.data and c.data.startswith("runtime_zone:"))
async def open_runtime_zone(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_SYSTEM_VIEW):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    zone_code = str(callback.data.split(":", 1)[1])
    definition = await load_instance_definition(db)
    zone = next((item for item in definition.zones if item.code == zone_code), None)
    if not zone:
        await callback.answer("Зону не знайдено", show_alert=True)
        return
    await callback.message.edit_text(
        await _runtime_zone_text(db, zone_code),
        reply_markup=_runtime_zone_kb(zone, can_manage=is_owner_id(int(callback.from_user.id))),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data and c.data.startswith("runtime_zone_toggle:"))
async def toggle_runtime_zone_field(callback: CallbackQuery, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    _, zone_code, field_name = str(callback.data).split(":", 2)
    allowed_fields = {
        "enabled",
        "report_required",
        "private_reminder_enabled",
        "group_reminder_enabled",
        "rotation_enabled",
    }
    if field_name not in allowed_fields:
        await callback.answer("Невідомий параметр", show_alert=True)
        return
    definition = await load_instance_definition(db)
    zones = list(definition.zones)
    zone_index = next((index for index, item in enumerate(zones) if item.code == zone_code), None)
    if zone_index is None:
        await callback.answer("Зону не знайдено", show_alert=True)
        return
    zone = zones[int(zone_index)]
    zones[int(zone_index)] = replace(zone, **{field_name: not bool(getattr(zone, field_name))})
    updated_definition = replace(definition, zones=tuple(zones))
    await store_instance_definition(db, updated_definition)
    updated_zone = zones[int(zone_index)]
    await callback.message.edit_text(
        await _runtime_zone_text(db, zone_code),
        reply_markup=_runtime_zone_kb(updated_zone, can_manage=True),
    )
    await callback.answer("Параметр зони оновлено ✅", show_alert=True)


@router.callback_query(lambda c: c.data and c.data.startswith("runtime_zone_edit:"))
async def start_runtime_zone_edit(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not is_owner_id(int(callback.from_user.id)):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return
    _, zone_code, field_name = str(callback.data).split(":", 2)
    definition = await load_instance_definition(db)
    zone = next((item for item in definition.zones if item.code == zone_code), None)
    if not zone:
        await callback.answer("Зону не знайдено", show_alert=True)
        return
    await state.update_data(runtime_edit_zone_code=zone_code, runtime_edit_field=field_name)
    await _remember_admin_message(state, callback.message)
    prompts = {
        "title": (
            AdminFSM.entering_runtime_zone_edit_title,
            f"✏️ <b>Нова назва для зони {zone.title}</b>\n\nПоточна назва: <b>{zone.title}</b>",
        ),
        "deadline": (
            AdminFSM.entering_runtime_zone_edit_deadline,
            f"⏱ <b>Новий дедлайн для {zone.title}</b>\n\n"
            f"Поточне значення: <b>{zone.report_deadline_time or '—'}</b>\n"
            "Введіть <b>HH:MM</b> або <b>-</b>, щоб вимкнути дедлайн.",
        ),
        "private_time": (
            AdminFSM.entering_runtime_zone_edit_private_time,
            f"🔔 <b>Новий private reminder time для {zone.title}</b>\n\n"
            f"Поточне значення: <b>{zone.private_reminder_time or '—'}</b>\n"
            "Введіть <b>HH:MM</b> або <b>-</b>, щоб прибрати час.",
        ),
        "every_days": (
            AdminFSM.entering_runtime_zone_edit_every_days,
            f"📆 <b>Новий крок ротації для {zone.title}</b>\n\n"
            f"Поточне значення: <b>{zone.rule.rotation_every_days}</b> дн.\n"
            "Введіть ціле число більше 0.",
        ),
        "pattern": (
            AdminFSM.entering_runtime_zone_edit_pattern,
            f"👥 <b>Новий pattern для {zone.title}</b>\n\n"
            f"Поточне значення: <b>{_zone_pattern_label(zone)}</b>\n"
            "Введіть шаблон через кому, наприклад: <b>2,3,2</b>",
        ),
        "members": (
            AdminFSM.entering_runtime_zone_edit_members,
            f"🧑‍🤝‍🧑 <b>Members для {zone.title}</b>\n\n"
            "Введіть Telegram ID через кому в порядку черги.\n"
            "Приклад: <code>123,456,789</code>\n"
            "Усі ID мають належати активним мешканцям.",
        ),
    }
    if field_name not in prompts:
        await callback.answer("Поле не підтримується", show_alert=True)
        return
    next_state, text = prompts[field_name]
    await state.set_state(next_state)
    await callback.message.edit_text(text, reply_markup=_runtime_section_back_kb("admin:runtime_zones"))
    await callback.answer()


@router.message(AdminFSM.entering_setup_coliving_name)
async def runtime_setup_coliving_name(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user or not is_owner_id(int(message.from_user.id)):
        await message.answer("Доступ обмежено")
        return
    value = (message.text or "").strip()
    if len(value) < 2:
        await message.answer("Назва має бути довшою.")
        return
    definition = await load_instance_definition(db)
    updated_definition = replace(definition, settings=replace(definition.settings, coliving_name=value))
    await store_instance_definition(db, updated_definition)
    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.entering_setup_timezone)
    await _edit_admin_message(
        state,
        message.bot,
        text=(
            "🧩 <b>Setup wizard / Крок 2</b>\n\n"
            f"Назву збережено: <b>{value}</b>\n\n"
            "Тепер введіть timezone.\n"
            "Приклад: <b>Europe/Kyiv</b>"
        ),
        reply_markup=_runtime_setup_wizard_back_kb(),
    )


@router.message(AdminFSM.entering_setup_timezone)
async def runtime_setup_timezone(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user or not is_owner_id(int(message.from_user.id)):
        await message.answer("Доступ обмежено")
        return
    value = (message.text or "").strip()
    try:
        ZoneInfo(value)
    except Exception:
        await message.answer("Некоректний timezone. Приклад: Europe/Kyiv")
        return
    definition = await load_instance_definition(db)
    updated_definition = replace(definition, settings=replace(definition.settings, timezone=value))
    await store_instance_definition(db, updated_definition)
    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.entering_setup_group_id)
    await _edit_admin_message(
        state,
        message.bot,
        text=(
            "🧩 <b>Setup wizard / Крок 3</b>\n\n"
            f"Timezone збережено: <b>{value}</b>\n\n"
            "Тепер введіть Telegram Group ID.\n"
            "Приклад: <code>-1001234567890</code>"
        ),
        reply_markup=_runtime_setup_wizard_back_kb(),
    )


@router.message(AdminFSM.entering_setup_group_id)
async def runtime_setup_group_id(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user or not is_owner_id(int(message.from_user.id)):
        await message.answer("Доступ обмежено")
        return
    raw = (message.text or "").strip()
    try:
        group_id = int(raw)
    except Exception:
        await message.answer("Group ID має бути цілим числом.")
        return
    definition = await load_instance_definition(db)
    updated_definition = replace(
        definition,
        settings=replace(
            definition.settings,
            group_id=group_id,
            owner_id=int(message.from_user.id),
        ),
    )
    await store_instance_definition(db, updated_definition)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _runtime_setup_summary_text(db),
        reply_markup=_runtime_setup_summary_kb(),
    )
    await state.clear()


@router.message(AdminFSM.entering_setup_residents)
async def runtime_setup_residents(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user or not is_owner_id(int(message.from_user.id)):
        await message.answer("Доступ обмежено")
        return
    lines = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
    if not lines:
        await message.answer("Надішліть хоча б одного мешканця.")
        return
    residents: list[dict] = []
    for index, line in enumerate(lines, start=1):
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 2:
            await message.answer(f"Рядок {index} має бути у форматі: telegram_id | Повне ім'я | @username")
            return
        try:
            telegram_id = int(parts[0])
        except Exception:
            await message.answer(f"Рядок {index}: telegram_id має бути числом.")
            return
        username = parts[2].lstrip("@") if len(parts) >= 3 and parts[2] else None
        residents.append(
            {
                "telegram_id": telegram_id,
                "full_name": parts[1],
                "username": username,
                "role": "resident",
                "is_active": True,
            }
        )
    await db.replace_residents_runtime(residents)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _runtime_setup_summary_text(db),
        reply_markup=_runtime_setup_summary_kb(),
    )
    await state.clear()


@router.message(AdminFSM.entering_runtime_zone_code)
async def runtime_zone_add_code(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user or not is_owner_id(int(message.from_user.id)):
        await message.answer("Доступ обмежено")
        return
    code = (message.text or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9_-]{2,40}", code):
        await message.answer("Code має містити 2-40 символів: латиниця, цифри, `_` або `-`.")
        return
    definition = await load_instance_definition(db)
    if any(zone.code == code for zone in definition.zones):
        await message.answer("Зона з таким code вже існує.")
        return
    await state.update_data(runtime_zone_code=code)
    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.entering_runtime_zone_title)
    await _edit_admin_message(
        state,
        message.bot,
        text=(
            "➕ <b>Майстер нової зони / Крок 2</b>\n\n"
            f"Code: <code>{code}</code>\n"
            "Тепер введіть відображувану назву.\n"
            "Приклад: <b>Пральня 2</b>"
        ),
        reply_markup=_runtime_section_back_kb("runtime_setup:zones"),
    )


@router.message(AdminFSM.entering_runtime_zone_title)
async def runtime_zone_add_title(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user or not is_owner_id(int(message.from_user.id)):
        await message.answer("Доступ обмежено")
        return
    title = (message.text or "").strip()
    if len(title) < 2:
        await message.answer("Назва має бути довшою.")
        return
    await state.update_data(runtime_zone_title=title)
    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.entering_runtime_zone_new_pattern)
    await _edit_admin_message(
        state,
        message.bot,
        text=(
            "➕ <b>Майстер нової зони / Крок 3</b>\n\n"
            f"Назва: <b>{title}</b>\n"
            "Введіть pattern команди через кому.\n"
            "Приклади: <code>1</code>, <code>2</code>, <code>2,3,2</code>"
        ),
        reply_markup=_runtime_section_back_kb("runtime_setup:zones"),
    )


@router.message(AdminFSM.entering_runtime_zone_new_pattern)
async def runtime_zone_add_pattern(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    try:
        parts = tuple(int(item.strip()) for item in raw.split(",") if item.strip())
        if not parts or any(item <= 0 for item in parts):
            raise ValueError
    except Exception:
        await message.answer("Введіть pattern у форматі 1 або 2,3,2")
        return
    await state.update_data(runtime_zone_pattern=list(parts))
    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.entering_runtime_zone_new_every_days)
    await _edit_admin_message(
        state,
        message.bot,
        text=(
            "➕ <b>Майстер нової зони / Крок 4</b>\n\n"
            f"Pattern: <b>{','.join(map(str, parts))}</b>\n"
            "Введіть крок ротації в днях.\n"
            "Приклад: <code>7</code> або <code>10</code>"
        ),
        reply_markup=_runtime_section_back_kb("runtime_setup:zones"),
    )


@router.message(AdminFSM.entering_runtime_zone_new_every_days)
async def runtime_zone_add_every_days(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    try:
        every_days = int(raw)
        if every_days <= 0:
            raise ValueError
    except Exception:
        await message.answer("Крок має бути цілим числом більше 0.")
        return
    await state.update_data(runtime_zone_every_days=every_days)
    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.entering_runtime_zone_new_deadline)
    await _edit_admin_message(
        state,
        message.bot,
        text=(
            "➕ <b>Майстер нової зони / Крок 5</b>\n\n"
            f"Крок ротації: <b>{every_days}</b> дн.\n"
            "Введіть дедлайн звіту у форматі <code>HH:MM</code> або <code>-</code>, якщо звіти не потрібні."
        ),
        reply_markup=_runtime_section_back_kb("runtime_setup:zones"),
    )


@router.message(AdminFSM.entering_runtime_zone_new_deadline)
async def runtime_zone_add_deadline(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    if raw != "-":
        try:
            _validate_hhmm(raw)
        except Exception:
            await message.answer("Формат часу: HH:MM або -")
            return
    await state.update_data(runtime_zone_deadline=None if raw == "-" else raw)
    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.entering_runtime_zone_new_private_time)
    await _edit_admin_message(
        state,
        message.bot,
        text=(
            "➕ <b>Майстер нової зони / Крок 6</b>\n\n"
            "Введіть час приватного нагадування у форматі <code>HH:MM</code> або <code>-</code>, якщо не треба."
        ),
        reply_markup=_runtime_section_back_kb("runtime_setup:zones"),
    )


@router.message(AdminFSM.entering_runtime_zone_new_private_time)
async def runtime_zone_add_private_time(message: Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    if raw != "-":
        try:
            _validate_hhmm(raw)
        except Exception:
            await message.answer("Формат часу: HH:MM або -")
            return
    await state.update_data(runtime_zone_private_time=None if raw == "-" else raw)
    await _cleanup_user_input(message)
    await state.set_state(AdminFSM.entering_runtime_zone_new_members)
    await _edit_admin_message(
        state,
        message.bot,
        text=(
            "➕ <b>Майстер нової зони / Крок 7</b>\n\n"
            "Введіть Telegram ID учасників через кому в порядку черги.\n"
            "Приклад: <code>123,456,789</code>"
        ),
        reply_markup=_runtime_section_back_kb("runtime_setup:zones"),
    )


@router.message(AdminFSM.entering_runtime_zone_new_members)
async def runtime_zone_add_members(message: Message, state: FSMContext, db: Database) -> None:
    raw = (message.text or "").strip()
    try:
        member_ids = tuple(int(item.strip()) for item in raw.split(",") if item.strip())
    except Exception:
        await message.answer("Введіть Telegram ID через кому.")
        return
    if not member_ids:
        await message.answer("Потрібен хоча б один Telegram ID.")
        return
    active_residents = {int(item["telegram_id"]) for item in await db.list_active_residents_full()}
    missing_ids = [member_id for member_id in member_ids if member_id not in active_residents]
    if missing_ids:
        await message.answer(f"Ці мешканці неактивні або відсутні: {', '.join(map(str, missing_ids))}")
        return

    data = await state.get_data()
    code = str(data.get("runtime_zone_code") or "").strip()
    title = str(data.get("runtime_zone_title") or "").strip()
    pattern = tuple(int(item) for item in (data.get("runtime_zone_pattern") or [1]))
    every_days = int(data.get("runtime_zone_every_days") or 7)
    deadline = data.get("runtime_zone_deadline")
    private_time = data.get("runtime_zone_private_time")
    if not code or not title:
        await state.clear()
        await message.answer("Контекст майстра втрачено.")
        return

    definition = await load_instance_definition(db)
    next_sort_order = max((zone.sort_order for zone in definition.zones), default=0) + 10
    from datetime import date as _date
    from rotation_engine import RotationRule

    new_zone = ZoneDefinition(
        code=code,
        title=title,
        enabled=True,
        sort_order=next_sort_order,
        team_size_mode="pattern" if len(pattern) > 1 else "fixed",
        report_required=deadline is not None,
        report_deadline_time=str(deadline) if deadline is not None else None,
        private_reminder_time=str(private_time) if private_time is not None else None,
        group_reminder_enabled=False,
        private_reminder_enabled=private_time is not None,
        rotation_enabled=True,
        rule=RotationRule(
            rotation_mode="ordered",
            rotation_every_days=every_days,
            team_pattern=pattern,
            anchor_date=_date(2026, 1, 1),
            member_order=member_ids,
            member_groups=(),
        ),
        extra_config={"report_offset_days": max(0, every_days - 1), "created_from_bot": True},
    )
    updated_definition = replace(
        definition,
        zones=tuple(sorted((*definition.zones, new_zone), key=lambda item: (item.sort_order, item.code))),
    )
    await store_instance_definition(db, updated_definition)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _runtime_zone_text(db, code),
        reply_markup=_runtime_zone_kb(new_zone, can_manage=True),
    )
    await state.clear()


@router.message(AdminFSM.entering_runtime_import_json, F.text)
async def runtime_import_json(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user or not is_owner_id(int(message.from_user.id)):
        await message.answer("Доступ обмежено")
        return
    raw = (message.text or "").strip()
    if not raw:
        await message.answer("Встав JSON або YAML одним повідомленням.")
        return
    try:
        payload = _load_bundle_payload(raw)
        definition, residents = instance_bundle_from_dict(payload)
    except Exception as exc:
        await message.answer(f"Не вдалося розібрати bundle: {exc}")
        return
    await db.replace_residents_runtime(residents)
    await store_instance_definition(db, definition)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _runtime_config_text(db),
        reply_markup=_runtime_config_kb(definition, can_manage=True),
    )
    await state.clear()


@router.message(AdminFSM.entering_runtime_import_json, F.document)
async def runtime_import_json_document(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user or not is_owner_id(int(message.from_user.id)):
        await message.answer("Доступ обмежено")
        return
    if not message.document:
        await message.answer("Надішліть JSON-файл.")
        return
    buffer = BytesIO()
    try:
        await message.bot.download(message.document, destination=buffer)
        buffer.seek(0)
        payload = _load_bundle_payload(buffer.read().decode("utf-8"))
        definition, residents = instance_bundle_from_dict(payload)
    except Exception as exc:
        await message.answer(f"Не вдалося імпортувати файл: {exc}")
        return
    await db.replace_residents_runtime(residents)
    await store_instance_definition(db, definition)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _runtime_config_text(db),
        reply_markup=_runtime_config_kb(definition, can_manage=True),
    )
    await state.clear()


async def _update_runtime_zone_and_render(
    message: Message,
    state: FSMContext,
    db: Database,
    transform,
) -> None:
    data = await state.get_data()
    zone_code = str(data.get("runtime_edit_zone_code") or "").strip()
    if not zone_code:
        await state.clear()
        await message.answer("Контекст втрачено.")
        return
    definition = await load_instance_definition(db)
    zones = list(definition.zones)
    zone_index = next((index for index, item in enumerate(zones) if item.code == zone_code), None)
    if zone_index is None:
        await state.clear()
        await message.answer("Зону не знайдено.")
        return
    updated_zone = transform(zones[int(zone_index)])
    zones[int(zone_index)] = updated_zone
    updated_definition = replace(definition, zones=tuple(sorted(zones, key=lambda item: (item.sort_order, item.code))))
    await store_instance_definition(db, updated_definition)
    await _cleanup_user_input(message)
    await _edit_admin_message(
        state,
        message.bot,
        text=await _runtime_zone_text(db, zone_code),
        reply_markup=_runtime_zone_kb(updated_zone, can_manage=True),
    )
    await state.clear()


@router.message(AdminFSM.entering_runtime_zone_edit_title)
async def runtime_zone_edit_title(message: Message, state: FSMContext, db: Database) -> None:
    title = (message.text or "").strip()
    if len(title) < 2:
        await message.answer("Назва має бути довшою.")
        return
    await _update_runtime_zone_and_render(
        message,
        state,
        db,
        lambda zone: replace(zone, title=title),
    )


@router.message(AdminFSM.entering_runtime_zone_edit_deadline)
async def runtime_zone_edit_deadline(message: Message, state: FSMContext, db: Database) -> None:
    raw = (message.text or "").strip()
    if raw != "-":
        try:
            _validate_hhmm(raw)
        except Exception:
            await message.answer("Формат часу: HH:MM або -")
            return
    value = None if raw == "-" else raw
    await _update_runtime_zone_and_render(
        message,
        state,
        db,
        lambda zone: replace(zone, report_deadline_time=value),
    )


@router.message(AdminFSM.entering_runtime_zone_edit_private_time)
async def runtime_zone_edit_private_time(message: Message, state: FSMContext, db: Database) -> None:
    raw = (message.text or "").strip()
    if raw != "-":
        try:
            _validate_hhmm(raw)
        except Exception:
            await message.answer("Формат часу: HH:MM або -")
            return
    value = None if raw == "-" else raw
    await _update_runtime_zone_and_render(
        message,
        state,
        db,
        lambda zone: replace(zone, private_reminder_time=value),
    )


@router.message(AdminFSM.entering_runtime_zone_edit_every_days)
async def runtime_zone_edit_every_days(message: Message, state: FSMContext, db: Database) -> None:
    raw = (message.text or "").strip()
    try:
        every_days = int(raw)
        if every_days <= 0:
            raise ValueError
    except Exception:
        await message.answer("Потрібне ціле число більше 0.")
        return
    await _update_runtime_zone_and_render(
        message,
        state,
        db,
        lambda zone: replace(zone, rule=replace(zone.rule, rotation_every_days=every_days)),
    )


@router.message(AdminFSM.entering_runtime_zone_edit_pattern)
async def runtime_zone_edit_pattern(message: Message, state: FSMContext, db: Database) -> None:
    raw = (message.text or "").strip()
    try:
        parts = tuple(int(item.strip()) for item in raw.split(",") if item.strip())
        if not parts or any(item <= 0 for item in parts):
            raise ValueError
    except Exception:
        await message.answer("Введіть pattern у форматі 1,2,3")
        return
    team_size_mode = "pattern" if len(parts) > 1 else "fixed"
    await _update_runtime_zone_and_render(
        message,
        state,
        db,
        lambda zone: replace(
            zone,
            team_size_mode=team_size_mode,
            rule=replace(zone.rule, team_pattern=parts),
        ),
    )


@router.message(AdminFSM.entering_runtime_zone_edit_members)
async def runtime_zone_edit_members(message: Message, state: FSMContext, db: Database) -> None:
    raw = (message.text or "").strip()
    try:
        member_ids = tuple(int(item.strip()) for item in raw.split(",") if item.strip())
    except Exception:
        await message.answer("Введіть Telegram ID через кому.")
        return
    if not member_ids:
        await message.answer("Потрібен хоча б один Telegram ID.")
        return
    active_residents = {int(item["telegram_id"]) for item in await db.list_active_residents_full()}
    missing_ids = [member_id for member_id in member_ids if member_id not in active_residents]
    if missing_ids:
        await message.answer(f"Ці мешканці неактивні або відсутні: {', '.join(map(str, missing_ids))}")
        return
    await _update_runtime_zone_and_render(
        message,
        state,
        db,
        lambda zone: replace(
            zone,
            rule=replace(zone.rule, member_order=member_ids, member_groups=()),
        ),
    )
