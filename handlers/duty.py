from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date as date_type, datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Message,
)

from admin_alerts import (
    get_report_review_recipient_ids,
    get_report_review_watcher_ids,
    notify_owner_about_delegate_action,
)
from config import ADMIN_ID
from database import Database
from deadline_policy import deadline_user_override_key, deadline_waive_key, get_deadline_due_at
from handlers.common import (
    SUBMIT_REPORT_BUTTON,
    format_resident_mention,
    get_assignment_for_date,
    get_runtime_zone_titles,
    get_zone_assignment_for_date,
    get_zone_title,
    get_user_report_options,
    is_admin,
    is_admin_id,
    is_test_user,
    kyiv_today,
    require_resident,
)
from permissions import PERM_FINES_MANAGE, PERM_REPORTS_REVIEW, PERM_TEST_MODE_MANAGE, has_permission


router = Router(name="duty")
ALBUM_FINALIZERS: dict[int, asyncio.Task] = {}
KYIV_TZ = ZoneInfo("Europe/Kyiv")


class ReportFSM(StatesGroup):
    choosing_zone = State()
    waiting_photo = State()


class AdminRejectFSM(StatesGroup):
    waiting_reason = State()
    waiting_fine_amount = State()
    waiting_deadline_bank_amount = State()
    waiting_deadline_text_reason = State()
    waiting_deadline_extend_until = State()


@dataclass(frozen=True)
class RejectContext:
    log_id: int
    resident_id: int


def zone_kb(options: list[tuple[str, str]] | None = None) -> InlineKeyboardMarkup:
    allowed_options = options or []
    rows = []
    for zone_name, label in allowed_options:
        rows.append([InlineKeyboardButton(text=label, callback_data=f"report_zone:{zone_name}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _report_option_buttons(options: list[tuple[str, date_type]]) -> list[tuple[str, str]]:
    today = kyiv_today()
    rows: list[tuple[str, str]] = []
    for zone_name, target_date in options:
        label = str(zone_name)
        if target_date != today:
            label = f"{label} · {target_date.strftime('%d.%m')}"
        rows.append((f"{zone_name}:{target_date.isoformat()}", label))
    return rows


def admin_moderation_kb(log_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Підтвердити", callback_data=f"duty_ok:{log_id}"),
                InlineKeyboardButton(text="❌ Відхилити", callback_data=f"duty_no:{log_id}"),
            ],
        ]
    )


def deadline_moderation_kb(zone: str, resident_id: int, duty_date: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💸 Грошовий штраф",
                    callback_data=f"deadline_bank:{resident_id}:{zone}:{duty_date}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📝 Текстовий штраф",
                    callback_data=f"deadline_text:{resident_id}:{zone}:{duty_date}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🕒 Продовжити дедлайн",
                    callback_data=f"deadline_extend:{resident_id}:{zone}:{duty_date}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="👌 Без штрафу",
                    callback_data=f"deadline_no_fine:{resident_id}:{zone}:{duty_date}",
                )
            ],
        ]
    )

def _parse_deadline_datetime_input(value: str) -> datetime:
    raw = " ".join((value or "").strip().split())
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m %H:%M"):
        try:
            parsed = datetime.strptime(raw, fmt)
            if fmt == "%d.%m %H:%M":
                parsed = parsed.replace(year=kyiv_today().year)
            return parsed.replace(tzinfo=KYIV_TZ)
        except ValueError:
            continue
    raise ValueError("Bad deadline datetime")


def _cancel_album_finalizer(user_id: int) -> None:
    task = ALBUM_FINALIZERS.pop(user_id, None)
    if task and not task.done():
        task.cancel()


async def _send_report_to_admin(
    bot,
    db: Database,
    resident: dict,
    zone_name: str,
    log_id: int,
    photo_ids: list[str],
    *,
    duty_date: str | None = None,
) -> None:
    zone_ua = await get_zone_title(db, zone_name)
    resident_mention = format_resident_mention(resident)
    media = []
    for index, photo_id in enumerate(photo_ids):
        caption = None
        if index == 0:
            caption = (
                f"Звіт від: {resident_mention}\n"
                f"Зона: {zone_ua}\n"
                f"Дата чергування: {duty_date or '—'}\n"
                f"Фото: {len(photo_ids)}"
            )
        media.append(InputMediaPhoto(media=photo_id, caption=caption))
    for recipient_id in await get_report_review_recipient_ids(db):
        await bot.send_media_group(chat_id=int(recipient_id), media=media)
        await bot.send_message(
            chat_id=int(recipient_id),
            text=f"Перевірка звіту: {resident_mention} • <b>{zone_ua}</b>",
            reply_markup=admin_moderation_kb(log_id),
        )


async def _finalize_report_submission(
    message: Message,
    state: FSMContext,
    db: Database,
    bot,
    *,
    expected_group_id: str | None,
) -> None:
    if not message.from_user:
        return

    user_id = int(message.from_user.id)
    data = await state.get_data()
    state_group_id = data.get("media_group_id")
    if expected_group_id != state_group_id:
        return

    zone_name = data.get("zone_name")
    duty_date_raw = str(data.get("duty_date") or "")
    photo_ids = list(dict.fromkeys(data.get("photo_ids") or []))
    if not zone_name or not photo_ids or not duty_date_raw:
        return

    resident = await require_resident(message, db)
    if not resident:
        return

    from datetime import date

    duty_date = date.fromisoformat(duty_date_raw)
    log_id = await db.create_duty_log(
        resident["telegram_id"],
        zone_name,
        json.dumps(photo_ids, ensure_ascii=False),
        duty_date=duty_date,
    )
    await _send_report_to_admin(
        bot,
        db,
        resident,
        zone_name,
        log_id,
        photo_ids,
        duty_date=duty_date.strftime("%d.%m.%Y"),
    )
    await message.answer("> ✅ Звіт надіслано на перевірку адміну.")

    _cancel_album_finalizer(user_id)
    await state.clear()


def _schedule_album_finalizer(
    message: Message,
    state: FSMContext,
    db: Database,
    bot,
    media_group_id: str,
) -> None:
    user_id = int(message.from_user.id)
    _cancel_album_finalizer(user_id)

    async def _runner() -> None:
        try:
            await asyncio.sleep(1.2)
            await _finalize_report_submission(
                message,
                state,
                db,
                bot,
                expected_group_id=media_group_id,
            )
        except asyncio.CancelledError:
            return

    ALBUM_FINALIZERS[user_id] = asyncio.create_task(_runner())


@router.message(Command("report"))
async def cmd_report(message: Message, state: FSMContext, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return

    report_options = await get_user_report_options(db, int(resident["telegram_id"]))
    if not report_options:
        await message.answer("Сьогодні не твоя черга.")
        return

    zone_titles = await get_runtime_zone_titles(db)
    await state.set_state(ReportFSM.choosing_zone)
    await message.answer(
        f"📸 <b>{SUBMIT_REPORT_BUTTON}</b>\nОберіть зону:",
        reply_markup=zone_kb(
            [
                (
                    f"{zone_name}:{target_date.isoformat()}",
                    f"{zone_titles.get(zone_name, zone_name)}"
                    + (f" · {target_date.strftime('%d.%m')}" if target_date != kyiv_today() else ""),
                )
                for zone_name, target_date in report_options
            ]
        ),
    )


async def open_report_menu_from_callback(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return
    resident = await db.get_resident(int(callback.from_user.id))
    if is_admin_id(int(callback.from_user.id)):
        resident = resident or {
            "telegram_id": callback.from_user.id,
            "full_name": callback.from_user.full_name or "Адмін",
            "username": callback.from_user.username,
            "role": "admin",
            "is_active": 1,
        }
    if not resident:
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    report_options = await get_user_report_options(db, int(callback.from_user.id))
    if not report_options:
        await callback.message.edit_text(
            "📸 <b>Здати звіт</b>\n\nСьогодні не твоя черга.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        await callback.answer()
        return
    zone_titles = await get_runtime_zone_titles(db)
    await state.set_state(ReportFSM.choosing_zone)
    await callback.message.edit_text(
        f"📸 <b>{SUBMIT_REPORT_BUTTON}</b>\nОберіть зону:",
        reply_markup=zone_kb(
            [
                (
                    f"{zone_name}:{target_date.isoformat()}",
                    f"{zone_titles.get(zone_name, zone_name)}"
                    + (f" · {target_date.strftime('%d.%m')}" if target_date != kyiv_today() else ""),
                )
                for zone_name, target_date in report_options
            ]
        ),
    )
    await callback.answer()


async def open_report_menu_from_message(message: Message, state: FSMContext, db: Database) -> None:
    if not message.from_user:
        return
    resident = await db.get_resident(int(message.from_user.id))
    if is_admin_id(int(message.from_user.id)):
        resident = resident or {
            "telegram_id": message.from_user.id,
            "full_name": message.from_user.full_name or "Адмін",
            "username": message.from_user.username,
            "role": "admin",
            "is_active": 1,
        }
    if not resident:
        await message.answer("Доступ обмежено")
        return
    report_options = await get_user_report_options(db, int(message.from_user.id))
    if not report_options:
        await message.answer(
            "📸 <b>Здати звіт</b>\n\nСьогодні не твоя черга.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ До меню", callback_data="nav:home")]]
            ),
        )
        return
    zone_titles = await get_runtime_zone_titles(db)
    await state.set_state(ReportFSM.choosing_zone)
    await message.answer(
        f"📸 <b>{SUBMIT_REPORT_BUTTON}</b>\nОберіть зону:",
        reply_markup=zone_kb(
            [
                (
                    f"{zone_name}:{target_date.isoformat()}",
                    f"{zone_titles.get(zone_name, zone_name)}"
                    + (f" · {target_date.strftime('%d.%m')}" if target_date != kyiv_today() else ""),
                )
                for zone_name, target_date in report_options
            ]
        ),
    )

@router.message(Command("force_report"))
async def cmd_force_report(message: Message, state: FSMContext, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return
    if not await has_permission(db, int(resident["telegram_id"]), PERM_TEST_MODE_MANAGE):
        await message.answer("Доступ обмежено")
        return
    zone_titles = await get_runtime_zone_titles(db)
    await state.set_state(ReportFSM.choosing_zone)
    await message.answer(
        f"> 🧪 Тестовий режим: оберіть зону для {SUBMIT_REPORT_BUTTON}.",
        reply_markup=zone_kb([(zone_name, zone_titles.get(zone_name, zone_name)) for zone_name in zone_titles]),
    )


@router.callback_query(F.data.startswith("report_zone:"))
async def on_zone_chosen(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    message = callback.message
    if not message or not callback.from_user:
        await callback.answer()
        return

    resident = await db.get_resident(int(callback.from_user.id))
    if is_admin_id(callback.from_user.id):
        resident = resident or {
            "telegram_id": callback.from_user.id,
            "full_name": callback.from_user.full_name or "Адмін",
            "username": callback.from_user.username,
            "role": "admin",
            "is_active": 1,
        }
    if not resident or (
        not resident.get("is_active")
        and not is_admin_id(callback.from_user.id)
        and not await is_test_user(db, callback.from_user.id)
    ):
        await message.edit_text(
            "Доступ обмежено",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        await callback.answer()
        return

    if await db.get_resident(int(callback.from_user.id)):
        await db.update_resident_profile(
            callback.from_user.id,
            str(resident["full_name"]),
            callback.from_user.username,
        )
        resident = await db.get_resident(int(callback.from_user.id)) or resident

    parts = callback.data.split(":")
    zone_name = parts[1]
    duty_date_raw = parts[2] if len(parts) > 2 else kyiv_today().isoformat()

    report_options = await get_user_report_options(db, int(resident["telegram_id"]))
    allowed_pairs = {(zone, target_date.isoformat()) for zone, target_date in report_options}
    target_date = date_type.fromisoformat(duty_date_raw)
    allowed = (zone_name, duty_date_raw) in allowed_pairs
    zone_ua = await get_zone_title(db, zone_name)
    assignment_info = await get_zone_assignment_for_date(db, zone_name, target_date)
    responsible_name = ""
    if assignment_info:
        _, _, member_names = assignment_info
        responsible_name = " + ".join(member_names)

    if not allowed:
        await state.clear()
        await message.edit_text(
            "⚠️ <b>Сьогодні не твоя черга.</b>\n"
            f"Зона: <b>{zone_ua}</b>\n"
            f"Згідно з графіком, зараз чергує <b>{responsible_name}</b>.\n"
            "Якщо ви помінялися, скористайтеся кнопкою <b>🔄 Обмін</b>.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        await callback.answer()
        return

    await state.update_data(zone_name=zone_name, duty_date=target_date.isoformat(), photo_ids=[], media_group_id=None)
    await state.set_state(ReportFSM.waiting_photo)
    await callback.answer()
    await message.edit_text(
        "📸 <b>Здати звіт</b>\n\nНадішли одне або кілька фото звіту. Якщо це альбом, надішли його одним повідомленням.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
        ),
    )


@router.message(ReportFSM.waiting_photo, F.photo)
async def on_report_photo(message: Message, state: FSMContext, db: Database, bot: object) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return

    data = await state.get_data()
    zone_name = data.get("zone_name")
    if not zone_name:
        await state.clear()
        await message.answer("Сталася помилка. Спробуй ще раз через кнопку «Здати звіт».")
        return

    photo_ids = list(data.get("photo_ids") or [])
    photo_ids.append(message.photo[-1].file_id)
    media_group_id = message.media_group_id
    await state.update_data(photo_ids=photo_ids, media_group_id=media_group_id)

    if media_group_id:
        _schedule_album_finalizer(message, state, db, bot, media_group_id)
        return

    await _finalize_report_submission(message, state, db, bot, expected_group_id=None)


@router.message(ReportFSM.waiting_photo)
async def on_report_non_photo(message: Message) -> None:
    await message.answer("Потрібно надіслати фото або альбом із фото.")


@router.callback_query(F.data.startswith("duty_ok:"))
async def on_admin_approve(callback: CallbackQuery, state: FSMContext, db: Database, bot: object) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_REPORTS_REVIEW):
        await callback.answer("Доступ обмежено", show_alert=True)
        return

    log_id = int(callback.data.split(":", 1)[1])
    current_log = await db.get_duty_log(log_id)
    if not current_log:
        await callback.answer("Не знайдено запис", show_alert=True)
        return
    if str(current_log.get("status")) != "pending":
        if callback.message:
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
        await callback.answer("Звіт уже оброблено", show_alert=True)
        return
    log = await db.set_duty_status(log_id, "approved")
    if not log:
        await callback.answer("Не знайдено запис", show_alert=True)
        return
    await db.log_admin_action(
        int(callback.from_user.id),
        "approve_report",
        target_id=int(log["telegram_id"]),
        details=f"log={log_id}|zone={log['zone_name']}|date={log.get('duty_date') or ''}",
    )

    await bot.send_message(chat_id=int(log["telegram_id"]), text="> ✅ Звіт прийнято! Дякую.")
    actor = await db.get_resident(int(callback.from_user.id))
    actor_name = str((actor or {}).get("full_name") or callback.from_user.full_name or callback.from_user.id)
    zone_ua = await get_zone_title(db, str(log["zone_name"]))
    for watcher_id in await get_report_review_watcher_ids(db, int(callback.from_user.id)):
        try:
            await bot.send_message(
                chat_id=int(watcher_id),
                text=(
                    f"✅ Звіт <b>#{log_id}</b> по зоні <b>{zone_ua}</b> "
                    f"за <b>{log.get('duty_date') or '—'}</b> прийняв(ла) <b>{actor_name}</b>."
                ),
            )
        except Exception:
            pass
    if callback.message:
        await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer("Прийнято ✅")


@router.callback_query(F.data.startswith("duty_no:"))
async def on_admin_reject_start(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_REPORTS_REVIEW):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    log_id = int(callback.data.split(":", 1)[1])
    log = await db.get_duty_log(log_id)
    if not log:
        await callback.answer("Не знайдено запис", show_alert=True)
        return

    await state.set_state(AdminRejectFSM.waiting_reason)
    await state.update_data(reject_ctx={"log_id": log_id, "resident_id": int(log["telegram_id"])})
    await callback.answer()
    await callback.message.answer("Напишіть причину відхилення (текстом).")


@router.message(AdminRejectFSM.waiting_reason)
async def on_admin_reject_reason(message: Message, state: FSMContext, db: Database, bot: object) -> None:
    if not message.from_user or not await has_permission(db, int(message.from_user.id), PERM_REPORTS_REVIEW):
        await message.answer("Доступ обмежено")
        return

    reason = (message.text or "").strip()
    if not reason:
        await message.answer("Будь ласка, напишіть текст причини.")
        return

    data = await state.get_data()
    ctx = data.get("reject_ctx") or {}
    log_id = int(ctx.get("log_id", 0))
    resident_id = int(ctx.get("resident_id", 0))
    fine_mode = bool(ctx.get("fine_mode", False))
    if not log_id or not resident_id:
        await state.clear()
        await message.answer("Контекст втрачено. Спробуйте відхилити ще раз з кнопки.")
        return

    current_log = await db.get_duty_log(log_id)
    if not current_log or str(current_log.get("status")) != "pending":
        await state.clear()
        await message.answer("Цей звіт уже оброблено.")
        return

    await db.set_duty_status(log_id, "rejected", admin_comment=reason)
    log = await db.get_duty_log(log_id)
    await db.log_admin_action(
        int(message.from_user.id),
        "reject_report",
        target_id=resident_id,
        details=f"log={log_id}|reason={reason}",
    )
    await bot.send_message(
        chat_id=resident_id,
        text=(
            f"> ❌ Звіт відхилено!\nПричина: {reason}\n\n"
            f"Будь ласка, перероби його та знову натисни кнопку <b>{SUBMIT_REPORT_BUTTON}</b>."
        ),
    )
    actor = await db.get_resident(int(message.from_user.id))
    actor_name = str((actor or {}).get("full_name") or message.from_user.full_name or message.from_user.id)
    zone_ua = await get_zone_title(db, str((log or {}).get("zone_name") or ""))
    for watcher_id in await get_report_review_watcher_ids(db, int(message.from_user.id)):
        try:
            await bot.send_message(
                chat_id=int(watcher_id),
                text=(
                    f"❌ Звіт <b>#{log_id}</b> по зоні <b>{zone_ua or '—'}</b> "
                    f"за <b>{(log or {}).get('duty_date') or '—'}</b> відхилив(ла) <b>{actor_name}</b>.\n"
                    f"Причина: {reason}"
                ),
            )
        except Exception:
            pass
    if fine_mode:
        await state.set_state(AdminRejectFSM.waiting_fine_amount)
        await state.update_data(fine_ctx={"resident_id": resident_id, "reason": reason})
        await message.answer("💰 Введіть суму штрафу (число, грн).")
        return

    await state.clear()
    await message.answer("Причину надіслано мешканцю.")


@router.message(AdminRejectFSM.waiting_fine_amount)
async def on_admin_fine_amount(message: Message, state: FSMContext, db: Database, bot) -> None:
    if not message.from_user or not await has_permission(db, int(message.from_user.id), PERM_FINES_MANAGE):
        await message.answer("Доступ обмежено")
        return
    txt = (message.text or "").strip()
    try:
        amount = int(txt)
        if amount <= 0:
            raise ValueError
    except Exception:
        await message.answer("Сума має бути числом більше 0.")
        return

    data = await state.get_data()
    fine_ctx = data.get("fine_ctx") or {}
    resident_id = int(fine_ctx.get("resident_id", 0))
    reason = str(fine_ctx.get("reason", "")).strip()
    if not resident_id or not reason:
        await state.clear()
        await message.answer("Контекст втрачено.")
        return

    fine_id = await db.create_fine(
        user_id=resident_id,
        reason=reason,
        amount=amount,
        fine_date=kyiv_today(),
        fine_type="Штраф за відхилений звіт",
        issued_by=int(message.from_user.id),
    )
    await db.log_admin_action(
        int(message.from_user.id),
        "fine_after_reject",
        target_id=resident_id,
        details=f"fine_id={fine_id}|amount={amount}|reason={reason}",
    )
    await notify_owner_about_delegate_action(
        bot,
        db,
        actor_id=int(message.from_user.id),
        action_type="fine_after_reject",
        details=f"fine_id={fine_id}|amount={amount}|reason={reason}",
        target_id=resident_id,
    )
    bank_url = await db.get_setting("fine_bank_url", "—")
    fined = await db.get_resident(resident_id)
    fined_mention = format_resident_mention(fined)

    from config import GROUP_ID
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    pay_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📸 Надіслати чек про оплату", callback_data=f"fine_pay:{fine_id}")]]
    )

    await bot.send_message(
        chat_id=int(GROUP_ID),
        text=(
            "🔔 <b>ШТРАФ ВИПИСАНО!</b>\n"
            f"👤 Порушник: {fined_mention}\n"
            f"📝 Причина: {reason}\n"
            f"💰 Сума: <b>{amount}</b> грн\n"
            f"🔗 Оплатити: {bank_url}"
        ),
    )
    try:
        await bot.send_message(
            chat_id=int(resident_id),
            text=(
                "> ⚠️ Тобі виписали штраф.\n"
                f"Причина: {reason}\n"
                f"Сума: {amount} грн\n"
                f"Оплатити: {bank_url}"
            ),
            reply_markup=pay_kb,
        )
    except Exception:
        pass

    await state.clear()
    await message.answer("> ✅ Штраф виписано.")


@router.callback_query(F.data.startswith("duty_fine:"))
async def on_duty_fine(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    log_id = int(callback.data.split(":", 1)[1])
    log = await db.get_duty_log(log_id)
    if not log:
        await callback.answer("Не знайдено запис", show_alert=True)
        return
    await state.set_state(AdminRejectFSM.waiting_reason)
    await state.update_data(reject_ctx={"log_id": log_id, "resident_id": int(log["telegram_id"]), "fine_mode": True})
    await callback.answer()
    await callback.message.answer("📝 Введіть причину штрафу (текстом).")


@router.callback_query(F.data.startswith("deadline_bank:"))
async def on_deadline_bank(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    _, resident_id, zone_name, duty_date = callback.data.split(":", 3)
    await state.set_state(AdminRejectFSM.waiting_deadline_bank_amount)
    await state.update_data(
        deadline_fine_ctx={
            "resident_id": int(resident_id),
            "zone_name": zone_name,
            "duty_date": duty_date,
            "kind": "bank",
        }
    )
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.answer()
    await callback.message.answer("💸 Введіть суму грошового штрафу в гривнях.")


@router.callback_query(F.data.startswith("deadline_text:"))
async def on_deadline_text(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    _, resident_id, zone_name, duty_date = callback.data.split(":", 3)
    await state.set_state(AdminRejectFSM.waiting_deadline_text_reason)
    await state.update_data(
        deadline_fine_ctx={
            "resident_id": int(resident_id),
            "zone_name": zone_name,
            "duty_date": duty_date,
            "kind": "text",
        }
    )
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.answer()
    await callback.message.answer("📝 Введіть текстовий штраф або опис порушення.")


@router.callback_query(F.data.startswith("deadline_no_fine:"))
async def on_deadline_no_fine(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    _, resident_id, zone_name, duty_date = callback.data.split(":", 3)
    resident_id_int = int(resident_id)
    duty_date_obj = date_type.fromisoformat(duty_date)
    fined = await db.get_resident(resident_id_int)
    resident_name = format_resident_mention(fined) if fined else f"<b>{resident_id_int}</b>"
    zone_ua = await get_zone_title(db, zone_name)
    await db.set_setting(deadline_waive_key(zone_name, duty_date_obj, resident_id_int), "1")
    await db.clear_deadline_alert(resident_id_int, zone_name, duty_date_obj)
    await db.clear_deadline_user_reminders(resident_id_int, zone_name, duty_date_obj)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await db.log_admin_action(
        int(callback.from_user.id),
        "deadline_no_fine",
        target_id=resident_id_int,
        details=f"zone={zone_name}|date={duty_date}",
    )
    await notify_owner_about_delegate_action(
        bot,
        db,
        actor_id=int(callback.from_user.id),
        action_type="deadline_no_fine",
        details=f"zone={zone_name}|date={duty_date}",
        target_id=resident_id_int,
    )
    await callback.answer("Штраф не буде виписано ✅", show_alert=True)
    try:
        await bot.send_message(
            chat_id=resident_id_int,
            text=(
                f"👌 Для тебе не буде штрафу за прострочений звіт по зоні <b>{zone_ua}</b> "
                f"за <b>{duty_date}</b>."
            ),
        )
    except Exception:
        pass
    await callback.message.answer(
        f"👌 Для мешканця {resident_name} штраф за дедлайн по зоні <b>{zone_ua}</b> за <b>{duty_date}</b> скасовано."
    )


@router.callback_query(F.data.startswith("deadline_extend:"))
async def on_deadline_extend(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    if not callback.message:
        await callback.answer()
        return

    _, resident_id, zone_name, duty_date = callback.data.split(":", 3)
    resident_id_int = int(resident_id)
    fined = await db.get_resident(resident_id_int)
    resident_name = format_resident_mention(fined) if fined else f"<b>{resident_id_int}</b>"
    await state.set_state(AdminRejectFSM.waiting_deadline_extend_until)
    await state.update_data(
        deadline_extend_ctx={
            "resident_id": resident_id_int,
            "zone_name": zone_name,
            "duty_date": duty_date,
        }
    )
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.answer()
    await callback.message.answer(
        f"🕒 Введіть новий дедлайн для {resident_name}.\n"
        "Формат: <b>ДД.ММ ГГ:ХХ</b> або <b>ДД.ММ.РРРР ГГ:ХХ</b>.\n"
        "Приклад: <b>24.03 12:00</b>"
    )


@router.message(AdminRejectFSM.waiting_deadline_bank_amount)
async def on_deadline_bank_amount(message: Message, state: FSMContext, db: Database, bot) -> None:
    if not message.from_user or not await has_permission(db, int(message.from_user.id), PERM_FINES_MANAGE):
        await message.answer("Доступ обмежено")
        return

    txt = (message.text or "").strip()
    try:
        amount = int(txt)
        if amount <= 0:
            raise ValueError
    except Exception:
        await message.answer("Сума має бути числом більше 0.")
        return

    data = await state.get_data()
    ctx = data.get("deadline_fine_ctx") or {}
    resident_id = int(ctx.get("resident_id", 0))
    zone_name = str(ctx.get("zone_name", ""))
    duty_date = str(ctx.get("duty_date", "")).strip()
    if not resident_id or not zone_name or not duty_date:
        await state.clear()
        await message.answer("Контекст втрачено.")
        return

    zone_ua = await get_zone_title(db, zone_name)
    fine_reason = f"Не здано звіт: {zone_ua} за {duty_date}"
    fine_id = await db.create_fine(
        user_id=resident_id,
        reason=fine_reason,
        amount=amount,
        fine_date=date_type.fromisoformat(duty_date),
        fine_type="Грошовий штраф",
        issued_by=int(message.from_user.id),
        requires_proof=True,
    )
    await db.log_admin_action(
        int(message.from_user.id),
        "deadline_bank_fine",
        target_id=resident_id,
        details=f"fine_id={fine_id}|amount={amount}|zone={zone_name}|date={duty_date}",
    )
    await notify_owner_about_delegate_action(
        bot,
        db,
        actor_id=int(message.from_user.id),
        action_type="deadline_bank_fine",
        details=f"fine_id={fine_id}|amount={amount}|zone={zone_name}|date={duty_date}",
        target_id=resident_id,
    )
    bank_url = await db.get_setting("fine_bank_url", "—")
    fined = await db.get_resident(resident_id)
    fined_mention = format_resident_mention(fined)

    from config import GROUP_ID
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    pay_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📸 Надіслати чек про оплату", callback_data=f"fine_pay:{fine_id}")]]
    )

    await bot.send_message(
        chat_id=int(GROUP_ID),
        text=(
            "🔔 <b>ШТРАФ ВИПИСАНО</b>\n"
            f"Порушник: {fined_mention}\n"
            f"Причина: {fine_reason}\n"
            f"Сума: <b>{amount}</b> грн\n"
            f"Оплатити: {bank_url}"
        ),
    )
    try:
        await bot.send_message(
            chat_id=int(resident_id),
            text=(
                "> ⚠️ Вам виписали штраф.\n"
                f"Причина: {fine_reason}\n"
                f"Сума: {amount} грн\n"
                f"Оплатити: {bank_url}"
            ),
            reply_markup=pay_kb,
        )
    except Exception:
        pass

    await state.clear()
    await message.answer("✅ Грошовий штраф виписано.")


@router.message(AdminRejectFSM.waiting_deadline_text_reason)
async def on_deadline_text_reason(message: Message, state: FSMContext, db: Database, bot) -> None:
    if not message.from_user or not await has_permission(db, int(message.from_user.id), PERM_FINES_MANAGE):
        await message.answer("Доступ обмежено")
        return

    text_reason = (message.text or "").strip()
    if not text_reason:
        await message.answer("Введіть текст штрафу.")
        return

    data = await state.get_data()
    ctx = data.get("deadline_fine_ctx") or {}
    resident_id = int(ctx.get("resident_id", 0))
    zone_name = str(ctx.get("zone_name", ""))
    duty_date = str(ctx.get("duty_date", "")).strip()
    if not resident_id or not zone_name or not duty_date:
        await state.clear()
        await message.answer("Контекст втрачено.")
        return

    zone_ua = await get_zone_title(db, zone_name)
    fine_reason = f"Не здано звіт: {zone_ua} за {duty_date}"
    fined = await db.get_resident(resident_id)
    fined_mention = format_resident_mention(fined)
    await db.create_fine(
        user_id=resident_id,
        reason=text_reason,
        amount=0,
        fine_date=date_type.fromisoformat(duty_date),
        fine_type=f"Текстовий штраф • {fine_reason}",
        issued_by=int(message.from_user.id),
        requires_proof=False,
    )
    await db.log_admin_action(
        int(message.from_user.id),
        "deadline_text_fine",
        target_id=resident_id,
        details=f"zone={zone_name}|date={duty_date}|text={text_reason}",
    )
    await notify_owner_about_delegate_action(
        bot,
        db,
        actor_id=int(message.from_user.id),
        action_type="deadline_text_fine",
        details=f"zone={zone_name}|date={duty_date}|text={text_reason}",
        target_id=resident_id,
    )

    from config import GROUP_ID

    await bot.send_message(
        chat_id=int(GROUP_ID),
        text=(
            "📝 <b>ТЕКСТОВИЙ ШТРАФ ВИПИСАНО</b>\n"
            f"Порушник: {fined_mention}\n"
            f"Причина: {fine_reason}\n"
            f"Опис: {text_reason}"
        ),
    )
    try:
        await bot.send_message(
            chat_id=int(resident_id),
            text=(
                "> ⚠️ Вам виписали текстовий штраф.\n"
                f"Причина: {fine_reason}\n"
                f"Опис: {text_reason}\n"
                "Підтвердження оплати не потрібне."
            ),
        )
    except Exception:
        pass

    await state.clear()
    await message.answer("✅ Текстовий штраф виписано.")


@router.message(AdminRejectFSM.waiting_deadline_extend_until)
async def on_deadline_extend_until(message: Message, state: FSMContext, db: Database, bot) -> None:
    if not message.from_user or not await has_permission(db, int(message.from_user.id), PERM_FINES_MANAGE):
        await message.answer("Доступ обмежено")
        return

    raw_value = (message.text or "").strip()
    try:
        extended_due_at = _parse_deadline_datetime_input(raw_value)
    except ValueError:
        await message.answer("Невірний формат. Приклад: <b>24.03 12:00</b> або <b>24.03.2026 12:00</b>.")
        return

    data = await state.get_data()
    ctx = data.get("deadline_extend_ctx") or {}
    resident_id = int(ctx.get("resident_id", 0))
    zone_name = str(ctx.get("zone_name", ""))
    duty_date = str(ctx.get("duty_date", "")).strip()
    if not resident_id or not zone_name or not duty_date:
        await state.clear()
        await message.answer("Контекст втрачено.")
        return

    zone_ua = await get_zone_title(db, zone_name)

    duty_date_obj = date_type.fromisoformat(duty_date)
    current_due_at = await get_deadline_due_at(db, zone_name, duty_date_obj)
    if extended_due_at <= current_due_at:
        await message.answer(
            f"Новий дедлайн має бути пізніше за поточний: <b>{current_due_at.strftime('%d.%m %H:%M')}</b>."
        )
        return

    fined = await db.get_resident(resident_id)
    resident_name = format_resident_mention(fined) if fined else f"<b>{resident_id}</b>"
    await db.set_setting(
        deadline_user_override_key(zone_name, duty_date_obj, resident_id),
        extended_due_at.isoformat(),
    )
    await db.delete_setting(deadline_waive_key(zone_name, duty_date_obj, resident_id))
    await db.clear_deadline_alert(resident_id, zone_name, duty_date_obj)
    await db.clear_deadline_user_reminders(resident_id, zone_name, duty_date_obj)
    await db.log_admin_action(
        int(message.from_user.id),
        "deadline_extend_custom",
        target_id=resident_id,
        details=f"zone={zone_name}|date={duty_date}|due_at={extended_due_at.isoformat()}",
    )
    await notify_owner_about_delegate_action(
        bot,
        db,
        actor_id=int(message.from_user.id),
        action_type="deadline_extend_custom",
        details=f"zone={zone_name}|date={duty_date}|due_at={extended_due_at.isoformat()}",
        target_id=resident_id,
    )
    await state.clear()
    try:
        await bot.send_message(
            chat_id=resident_id,
            text=(
                f"🕒 Адмін продовжив дедлайн для твого звіту по зоні <b>{zone_ua}</b> "
                f"за <b>{duty_date}</b> до <b>{extended_due_at.strftime('%d.%m.%Y %H:%M')}</b>."
            ),
        )
    except Exception:
        pass
    await message.answer(
        f"🕒 Дедлайн для мешканця {resident_name} по зоні <b>{zone_ua}</b> за <b>{duty_date}</b> "
        f"продовжено до <b>{extended_due_at.strftime('%d.%m.%Y %H:%M')}</b>."
    )
