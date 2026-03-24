from __future__ import annotations

from datetime import date

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardRemove,
)

from database import Database
from handlers.admin import admin_kb, admin_panel_text
from handlers.common import (
    SUBMIT_REPORT_BUTTON,
    calendar_exception_blocks_duties,
    canonical_full_name,
    get_assignment_for_date,
    get_calendar_exception,
    get_runtime_zone_assignments_for_date,
    kyiv_today,
    parse_user_date,
    refresh_section_message,
    require_resident,
)
from handlers.fines import build_my_fines_text
from handlers.duty import open_report_menu_from_callback, open_report_menu_from_message
from handlers.swap import open_swap_menu_from_callback
from instance_config import load_instance_definition
from permissions import can_access_admin_panel


router = Router(name="core")


class CoreFSM(StatesGroup):
    waiting_status_date = State()


async def _safe_callback_answer(
    callback: CallbackQuery,
    text: str | None = None,
    *,
    show_alert: bool = False,
) -> None:
    try:
        await callback.answer(text, show_alert=show_alert)
    except TelegramBadRequest as exc:
        # Telegram rejects expired callback queries; this should not spam logs.
        if "query is too old" not in str(exc).lower() and "query id is invalid" not in str(exc).lower():
            raise


def status_nav_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⬅️ На 1 день назад", callback_data="status_shift:-1"),
                InlineKeyboardButton(text="📅 Ввести дату", callback_data="status_select_date"),
                InlineKeyboardButton(text="➡️ На 1 день вперед", callback_data="status_shift:1"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")],
        ]
    )


def home_kb(*, is_admin: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="📊 Статус", callback_data="nav:status"),
            InlineKeyboardButton(text="💸 Мої штрафи", callback_data="nav:fines"),
        ],
        [
            InlineKeyboardButton(text=SUBMIT_REPORT_BUTTON, callback_data="nav:report"),
            InlineKeyboardButton(text="🔄 Обмін", callback_data="nav:swap"),
        ],
        [InlineKeyboardButton(text="💳 Скинути оплату", callback_data="nav:payment")],
        [InlineKeyboardButton(text="❓ Допомога", callback_data="nav:help")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton(text="⚙️ Керування", callback_data="nav:admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def home_text(resident: dict) -> str:
    return (
        "🏠 <b>Головна / Меню</b>\n\n"
        "Тут можна переглянути чергування, здати звіт, перевірити штрафи, "
        "створити обмін, відкрити папку для оплати і відкрити службові інструменти.\n\n"
        "Коротко:\n"
        f"• {SUBMIT_REPORT_BUTTON} — доступне лише для своїх зон на актуальну дату.\n"
        "• 🔄 Обмін — можна запропонувати будь-кому, але бот перевіряє, чи обмін справді можливий.\n"
        "• 📊 Статус — показує, хто відповідає за кожну зону на вибрану дату.\n"
        "• 💳 Скинути оплату — відкриває твою Google-папку для квитанції."
    )


def setup_mode_kb(*, is_owner: bool) -> InlineKeyboardMarkup:
    rows = []
    if is_owner:
        rows.extend(
            [
                [InlineKeyboardButton(text="🧩 Setup wizard", callback_data="runtime_setup:start")],
                [InlineKeyboardButton(text="⚙️ Runtime config", callback_data="admin:runtime_config")],
                [InlineKeyboardButton(text="⚙️ Панель керування", callback_data="nav:admin")],
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def setup_mode_text(*, is_owner: bool, db: Database) -> str:
    definition = await load_instance_definition(db)
    active_residents = await db.list_active_residents()
    enabled_zones = [zone for zone in definition.zones if zone.enabled]
    feature_flags = definition.feature_flags if isinstance(definition.feature_flags, dict) else {}
    enabled_modules = [
        label
        for label, enabled in (
            ("Звіти", bool(feature_flags.get("reports", False))),
            ("Штрафи", bool(feature_flags.get("fines", False))),
            ("Оплати", bool(feature_flags.get("payments", False))),
            ("Обміни", bool(feature_flags.get("swaps", False))),
            ("Дедлайни", bool(feature_flags.get("deadlines", False))),
            ("Нагадування", bool(feature_flags.get("reminders", False))),
        )
        if enabled
    ]
    steps = [
        ("Назва", bool(definition.settings.coliving_name.strip())),
        ("Timezone", bool(definition.settings.timezone.strip())),
        ("Group ID", bool(definition.settings.group_id)),
        ("Мешканці", len(active_residents) > 0),
        ("Зони", len(enabled_zones) > 0),
        ("Setup complete", definition.settings.setup_complete),
    ]
    done_count = sum(1 for _, done in steps if done)
    progress_lines = [f"{'✅' if done else '▫️'} {label}" for label, done in steps]
    if is_owner:
        return (
            "🧩 <b>Початкове налаштування</b>\n\n"
            "Бот ще не завершив стартове налаштування.\n"
            "Поки setup не завершено, звичайні розділи приховані.\n\n"
            f"Прогрес: <b>{done_count}/{len(steps)}</b>\n"
            f"{chr(10).join(progress_lines)}\n\n"
            f"Поточний coliving: <b>{definition.settings.coliving_name or 'не задано'}</b>\n"
            f"Timezone: <b>{definition.settings.timezone or 'не задано'}</b>\n"
            f"Group ID: <b>{definition.settings.group_id or 'не задано'}</b>\n"
            f"Активні мешканці: <b>{len(active_residents)}</b>\n"
            f"Активні зони: <b>{len(enabled_zones)}</b>\n"
            f"Модулі: <b>{', '.join(enabled_modules) if enabled_modules else 'усі вимкнені'}</b>\n\n"
            "Що далі:\n"
            "• пройти Setup wizard\n"
            "• перевірити runtime config\n"
            "• позначити setup завершеним"
        )
    return (
        "🧩 <b>Бот ще налаштовується</b>\n\n"
        "Власник ще не завершив початкове налаштування.\n"
        "Основні розділи тимчасово недоступні.\n\n"
        "Спробуй трохи пізніше."
    )


async def _get_setup_mode_state(db: Database) -> tuple[bool, int | None]:
    definition = await load_instance_definition(db)
    return (not definition.settings.setup_complete, definition.settings.owner_id)


async def _setup_guard_message(message: Message, db: Database) -> bool:
    setup_required, owner_id = await _get_setup_mode_state(db)
    if not setup_required:
        return False
    is_owner = bool(message.from_user and owner_id and int(message.from_user.id) == int(owner_id))
    await message.answer(
        await setup_mode_text(is_owner=is_owner, db=db),
        reply_markup=setup_mode_kb(is_owner=is_owner),
    )
    return True


async def _setup_guard_callback(callback: CallbackQuery, db: Database) -> bool:
    setup_required, owner_id = await _get_setup_mode_state(db)
    if not setup_required:
        return False
    is_owner = bool(callback.from_user and owner_id and int(callback.from_user.id) == int(owner_id))
    if callback.message:
        ok = await refresh_section_message(
            callback,
            text=await setup_mode_text(is_owner=is_owner, db=db),
            reply_markup=setup_mode_kb(is_owner=is_owner),
        )
        if not ok:
            await callback.message.edit_text(
                await setup_mode_text(is_owner=is_owner, db=db),
                reply_markup=setup_mode_kb(is_owner=is_owner),
            )
    await _safe_callback_answer(
        callback,
        None if is_owner else "Бот ще налаштовується",
        show_alert=not is_owner,
    )
    return True


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext, db: Database) -> None:
    if await _setup_guard_message(message, db):
        return
    resident = await require_resident(message, db)
    if not resident:
        return

    start_parts = (message.text or "").split(maxsplit=1)
    if len(start_parts) > 1 and start_parts[1].strip().lower() == "report":
        await open_report_menu_from_message(message, state, db)
        return

    remove_message = await message.answer(
        home_text(resident),
        reply_markup=ReplyKeyboardRemove(),
    )
    await message.answer(
        home_text(resident),
        reply_markup=home_kb(is_admin=await can_access_admin_panel(db, int(resident["telegram_id"]))),
    )
    try:
        await remove_message.delete()
    except Exception:
        pass


@router.message(Command("help"))
async def cmd_help(message: Message, db: Database) -> None:
    if await _setup_guard_message(message, db):
        return
    resident = await require_resident(message, db)
    if not resident:
        return
    await message.answer(
        "❓ <b>Головна / Допомога</b>\n"
        "---\n"
        "• <b>📊 Статус</b> — показує актуальних чергових і дає вибрати дату вручну.\n"
        f"• <b>{SUBMIT_REPORT_BUTTON}</b> — відкриває подання фото-звіту лише для твоєї зони на дозволену дату.\n"
        "• <b>🔄 Обмін</b> — можна запропонувати будь-якому мешканцю, але бот перевіряє, чи хтось із вас реально прив'язаний до вибраної зони сьогодні.\n"
        "• <b>💸 Мої штрафи</b> — показує поточний баланс і історію штрафів.\n"
        "• <b>⚙️ Адмін</b> — нагадування, тестовий режим, експорт таблиці та інші адмін-дії.\n"
        "---\n"
        "<b>Правила звітів</b>\n"
        "• Кожна активна зона може мати свій дедлайн, свій цикл і власний склад команди.\n"
        "• Для зон із кількома учасниками кожен здає свій окремий звіт.\n"
        "• У тестовому режимі звіт може піти на ручну перевірку адміну.\n"
        "---\n"
        "Усі основні дії виконуються через кнопки меню."
    )


async def _build_status_text(db: Database, target: date) -> str:
    calendar_exception = await get_calendar_exception(db, target)
    if calendar_exception_blocks_duties(calendar_exception):
        kind_titles = {"holiday": "Свято / вихідний", "day_off": "День без чергувань"}
        title = kind_titles.get(str(calendar_exception.get("kind")), "Особливий день")
        note = str(calendar_exception.get("note") or "").strip()
        text = (
            f"📍 <b>Головна / Статус</b> ({target.strftime('%d.%m.%Y')})\n"
            "---\n"
            f"🗓 <b>{title}</b>\n"
            "На цю дату чергувань немає.\n"
        )
        if note:
            text += f"ℹ️ {note}\n"
        text += f"---\nКнопка <b>{SUBMIT_REPORT_BUTTON}</b> для цієї дати недоступна."
    else:
        definition, runtime_assignments = await get_runtime_zone_assignments_for_date(db, target)
        zone_lines: list[str] = []
        for zone in sorted(definition.zones, key=lambda item: item.sort_order):
            if not zone.enabled or not zone.rotation_enabled:
                continue
            assignment = runtime_assignments.get(zone.code)
            if not assignment or not assignment.member_names:
                continue
            zone_lines.append(f"• <b>{zone.title}:</b> {' + '.join(assignment.member_names)}")
        text = (
            f"📍 <b>Головна / Статус</b> ({target.strftime('%d.%m.%Y')})\n"
            "---\n"
            f"{chr(10).join(zone_lines) if zone_lines else 'Сьогодні немає активних призначень.'}\n"
        )
        if calendar_exception and str(calendar_exception.get("kind")) == "special_rules":
            note = str(calendar_exception.get("note") or "").strip()
            if note:
                text += f"---\nℹ️ <b>Змінені правила:</b> {note}\n"
        text += f"---\nЩоб здати звіт, натисни кнопку <b>{SUBMIT_REPORT_BUTTON}</b>."
    return text


async def _render_status(message_or_cb, db: Database, target: date) -> None:
    text = await _build_status_text(db, target)

    if isinstance(message_or_cb, Message):
        await message_or_cb.answer(text, reply_markup=status_nav_kb())
        return

    from aiogram.exceptions import TelegramBadRequest

    try:
        await message_or_cb.message.edit_text(text, reply_markup=status_nav_kb())
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            await _safe_callback_answer(message_or_cb)
            return
        raise


@router.message(Command("status"))
async def cmd_status(message: Message, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return
    await _render_status(message, db, kyiv_today())


@router.callback_query(F.data == "nav:home")
async def nav_home(callback: CallbackQuery, db: Database) -> None:
    if await _setup_guard_callback(callback, db):
        return
    if not callback.message or not callback.from_user:
        await _safe_callback_answer(callback)
        return
    resident = await db.get_resident(int(callback.from_user.id))
    can_use_admin = await can_access_admin_panel(db, int(callback.from_user.id))
    if not resident and not can_use_admin:
        await _safe_callback_answer(callback, "Доступ обмежено", show_alert=True)
        return
    resident = resident or {
        "telegram_id": callback.from_user.id,
        "full_name": callback.from_user.full_name or "Адмін",
        "username": callback.from_user.username,
        "role": "admin",
    }
    ok = await refresh_section_message(
        callback,
        text=home_text(resident),
        reply_markup=home_kb(
            is_admin=can_use_admin,
        ),
    )
    if not ok:
        await callback.message.edit_text(
            home_text(resident),
            reply_markup=home_kb(
                is_admin=can_use_admin,
            ),
        )
    await _safe_callback_answer(callback)


@router.callback_query(F.data == "nav:status")
async def nav_status(callback: CallbackQuery, db: Database) -> None:
    if await _setup_guard_callback(callback, db):
        return
    await _render_status(callback, db, kyiv_today())
    await _safe_callback_answer(callback)


@router.callback_query(F.data == "nav:help")
async def nav_help(callback: CallbackQuery, db: Database) -> None:
    if await _setup_guard_callback(callback, db):
        return
    if not callback.message:
        await _safe_callback_answer(callback)
        return
    text = (
        "❓ <b>Головна / Допомога</b>\n"
        "---\n"
        "• <b>📊 Статус</b> — показує актуальних чергових і дає вибрати дату вручну.\n"
        f"• <b>{SUBMIT_REPORT_BUTTON}</b> — відкриває подання фото-звіту лише для твоєї зони на дозволену дату.\n"
        "• <b>🔄 Обмін</b> — можна запропонувати будь-якому мешканцю, але бот перевіряє, чи хтось із вас реально прив'язаний до вибраної зони сьогодні.\n"
        "• <b>💸 Мої штрафи</b> — показує поточний баланс і історію штрафів.\n"
        "• <b>⚙️ Керування</b> — службові інструменти для власника і заступників.\n"
        "---\n"
        "<b>Правила звітів</b>\n"
        "• Кожна активна зона може мати свій дедлайн, свій цикл і власний склад команди.\n"
        "• Якщо зона має кількох відповідальних, кожен учасник здає свій окремий звіт.\n"
        "• Кожен звіт завжди йде адміну на перевірку: він або підтверджує, або відхиляє його з причиною.\n"
        "• Якщо звіт відхилено, його треба перездати заново після виправлення.\n"
        "• Якщо звіт не здано до дедлайну, адміну приходить окреме повідомлення і він може виписати грошовий або текстовий штраф.\n"
    )
    reply_markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
    )
    ok = await refresh_section_message(callback, text=text, reply_markup=reply_markup)
    if not ok:
        await callback.message.edit_text(text, reply_markup=reply_markup)
    await _safe_callback_answer(callback)


@router.callback_query(F.data == "nav:fines")
async def nav_fines(callback: CallbackQuery, db: Database) -> None:
    if await _setup_guard_callback(callback, db):
        return
    if not callback.message or not callback.from_user:
        await _safe_callback_answer(callback)
        return
    text = await build_my_fines_text(int(callback.from_user.id), db)
    section_text = f"💸 <b>Головна / Мої штрафи</b>\n\n{text}"
    reply_markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
    )
    ok = await refresh_section_message(callback, text=section_text, reply_markup=reply_markup)
    if not ok:
        await callback.message.edit_text(section_text, reply_markup=reply_markup)
    await _safe_callback_answer(callback)


@router.callback_query(F.data == "nav:admin")
async def nav_admin(callback: CallbackQuery, db: Database) -> None:
    setup_required, owner_id = await _get_setup_mode_state(db)
    if setup_required:
        if not callback.from_user or not owner_id or int(callback.from_user.id) != int(owner_id):
            await _setup_guard_callback(callback, db)
            return
    if not callback.message or not callback.from_user:
        await _safe_callback_answer(callback)
        return
    if not await can_access_admin_panel(db, int(callback.from_user.id)):
        await _safe_callback_answer(callback, "Доступ обмежено", show_alert=True)
        return
    ok = await refresh_section_message(
        callback,
        text=await admin_panel_text(db, int(callback.from_user.id)),
        reply_markup=await admin_kb(db, int(callback.from_user.id), include_back=True),
    )
    if not ok:
        await callback.message.edit_text(
            await admin_panel_text(db, int(callback.from_user.id)),
            reply_markup=await admin_kb(db, int(callback.from_user.id), include_back=True),
        )
    await _safe_callback_answer(callback)


@router.callback_query(F.data == "nav:report")
async def nav_report(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if await _setup_guard_callback(callback, db):
        return
    await open_report_menu_from_callback(callback, state, db)


@router.callback_query(F.data == "nav:swap")
async def nav_swap(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if await _setup_guard_callback(callback, db):
        return
    await open_swap_menu_from_callback(callback, state, db)


@router.callback_query(F.data == "nav:payment")
async def nav_payment(callback: CallbackQuery, db: Database) -> None:
    if await _setup_guard_callback(callback, db):
        return
    if not callback.message or not callback.from_user:
        await _safe_callback_answer(callback)
        return
    resident = await db.get_resident(int(callback.from_user.id))
    folder_url = ((await db.get_setting(f"payment_folder:{int(callback.from_user.id)}", "")) or "").strip()
    text = (
        "💳 <b>Головна / Скинути оплату</b>\n\n"
        "Тут можна відкрити свою Google-папку і скинути квитанцію про оплату.\n\n"
        "Підписуйте квитанцію так: <b>Ім'я Прізвище (місяць рік)</b>.\n"
        "Приклад: <b>Ярослав Шарга (Січень 2026)</b>."
    )
    if not folder_url:
        text += "\n\nПосилання на папку ще не задано. Напишіть адміну."
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
        )
    else:
        text += "\n\nНатисни кнопку нижче, щоб відкрити папку."
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📁 Відкрити папку для оплати", url=folder_url)],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")],
            ]
        )
    ok = await refresh_section_message(callback, text=text, reply_markup=reply_markup)
    if not ok:
        await callback.message.edit_text(text, reply_markup=reply_markup)
    await _safe_callback_answer(callback)


@router.callback_query(F.data == "status_select_date")
async def select_date_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message:
        await _safe_callback_answer(callback)
        return
    await state.set_state(CoreFSM.waiting_status_date)
    await state.update_data(status_message_chat_id=callback.message.chat.id, status_message_id=callback.message.message_id)
    await _safe_callback_answer(callback)
    await callback.message.edit_text(
        "Введи дату у форматі <b>ДД.ММ</b> або <b>ДД.ММ.РРРР</b> за київським часом.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:status")]]
        ),
    )


@router.callback_query(F.data.startswith("status_shift:"))
async def shift_status_date(callback: CallbackQuery, db: Database) -> None:
    if not callback.message:
        await _safe_callback_answer(callback)
        return
    try:
        shift = int(callback.data.split(":", 1)[1])
    except ValueError:
        await _safe_callback_answer(callback, "Помилка формату", show_alert=True)
        return
    current_text = callback.message.text or ""
    target = kyiv_today()
    import re

    match = re.search(r"\((\d{2}\.\d{2}\.\d{4})\)", current_text)
    if match:
        try:
            target = parse_user_date(match.group(1))
        except ValueError:
            target = kyiv_today()
    target = target.fromordinal(target.toordinal() + shift)
    await _safe_callback_answer(callback)
    await _render_status(callback, db, target)


@router.message(CoreFSM.waiting_status_date)
async def receive_status_date(message: Message, state: FSMContext, db: Database, bot) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return

    try:
        target = parse_user_date(message.text or "")
    except ValueError:
        try:
            await message.delete()
        except Exception:
            pass
        await message.answer("Помилка формату дати. Приклад: <b>21.03</b> або <b>21.03.2026</b>.")
        return

    data = await state.get_data()
    try:
        await message.delete()
    except Exception:
        pass
    await state.clear()
    chat_id = data.get("status_message_chat_id")
    message_id = data.get("status_message_id")
    if chat_id and message_id:
        await bot.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(message_id),
            text=await _build_status_text(db, target),
            reply_markup=status_nav_kb(),
        )
        return
    await _render_status(message, db, target)
