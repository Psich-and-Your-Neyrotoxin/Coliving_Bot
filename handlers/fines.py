from __future__ import annotations

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from admin_alerts import notify_owner_about_delegate_action
from config import ADMIN_ID, GROUP_ID
from database import Database
from handlers.common import canonical_full_name, format_resident_mention, parse_user_date, require_resident
from permissions import PERM_FINES_MANAGE, has_permission


router = Router(name="fines")

MY_FINES_BUTTON = "💸 Мої штрафи"
ADMIN_FINE_BUTTON = "⚖️ Керування штрафами"


class FineFSM(StatesGroup):
    choosing_user = State()
    entering_type = State()
    entering_amount = State()
    entering_date = State()


class FinePayFSM(StatesGroup):
    waiting_photo = State()


def fine_control_kb(*, back: str | None = None) -> InlineKeyboardMarkup:
    row = []
    if back:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=back))
    row.append(InlineKeyboardButton(text="✖️ Скасувати", callback_data="fine_flow:cancel"))
    return InlineKeyboardMarkup(inline_keyboard=[row])


def residents_kb(residents: list[dict]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=canonical_full_name(resident["full_name"], resident.get("username")),
                callback_data=f"fine_user:{resident['telegram_id']}",
            )
        ]
        for resident in residents
    ][:89]
    rows.append([InlineKeyboardButton(text="✖️ Скасувати", callback_data="fine_flow:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def pay_kb(fine_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📸 Надіслати чек про оплату", callback_data=f"fine_pay:{fine_id}")]]
    )


def admin_pay_review_kb(fine_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Оплачено", callback_data=f"fine_ok:{fine_id}"),
                InlineKeyboardButton(text="❌ Фейк", callback_data=f"fine_fake:{fine_id}"),
            ]
        ]
    )


async def render_my_fines(message: Message, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return
    text = await build_my_fines_text(int(resident["telegram_id"]), db)
    await message.answer(text)


async def build_my_fines_text(user_id: int, db: Database) -> str:
    resident = await db.get_resident(int(user_id))
    if not resident:
        return "💸 <b>Мої штрафи</b>\n---\nДані не знайдено."

    balance = await db.get_user_fines_balance(int(resident["telegram_id"]))
    fines = await db.list_fines_for_user(int(resident["telegram_id"]))

    lines = [
        "💸 <b>Мої штрафи</b>",
        "---",
        f"Поточний баланс: <b>{balance}</b> грн",
        "",
        "<b>Історія штрафів</b>",
    ]
    if not fines:
        lines.append("Штрафів поки немає.")
    else:
        for fine in fines:
            fine_date = fine["fine_date"] or str(fine["created_at"])[:10]
            fine_type = fine["fine_type"] or fine["reason"]
            if not int(fine.get("requires_proof", 1)):
                status = "Без оплати"
            else:
                status = {
                    "pending": "Не сплачено",
                    "paid": "Сплачено",
                    "cancelled": "Скасовано",
                }.get(str(fine["status"]), str(fine["status"]))
            fine_date_ua = ".".join(reversed(str(fine_date).split("-"))) if "-" in str(fine_date) else str(fine_date)
            lines.append(f"• {fine_date_ua} | {fine_type} | {fine['amount']} грн | {status}")
    return "\n".join(lines)


async def _update_fine_control_message(
    state: FSMContext,
    bot,
    *,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    data = await state.get_data()
    chat_id = data.get("fine_control_chat_id")
    message_id = data.get("fine_control_message_id")
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


async def start_admin_fine_flow(
    message: Message,
    state: FSMContext,
    db: Database,
    *,
    actor_id: int | None = None,
    reuse_message: bool = False,
) -> None:
    resident = None
    if actor_id is not None:
        if await has_permission(db, int(actor_id), PERM_FINES_MANAGE):
            resident = await db.get_resident(int(actor_id))
            if resident:
                resident = dict(resident)
            else:
                resident = {
                    "telegram_id": int(actor_id),
                    "full_name": "Модератор",
                    "role": "admin",
                    "is_active": 1,
                }
    else:
        resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_FINES_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return
    residents = await db.list_active_residents()
    await state.set_state(FineFSM.choosing_user)
    text = "⚖️ <b>Керування штрафами</b>\n\nОберіть мешканця для штрафу:"
    if reuse_message:
        await state.update_data(
            fine_control_chat_id=message.chat.id,
            fine_control_message_id=message.message_id,
        )
        await message.edit_text(text, reply_markup=residents_kb(residents))
        return
    sent = await message.answer(text, reply_markup=residents_kb(residents))
    await state.update_data(
        fine_control_chat_id=sent.chat.id,
        fine_control_message_id=sent.message_id,
    )


@router.callback_query(FineFSM.choosing_user, F.data.startswith("fine_user:"))
async def fine_choose_user(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.message:
        await callback.answer()
        return
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    to_id = int(callback.data.split(":", 1)[1])
    await state.update_data(user_id=to_id)
    await state.set_state(FineFSM.entering_type)
    await callback.answer()
    await callback.message.edit_text(
        "⚖️ <b>Керування штрафами</b>\n\nВведіть тип штрафу або причину.\n"
        "Приклад: <b>Кухня</b>, <b>Ванна</b>, <b>Прострочений звіт</b>.",
        reply_markup=fine_control_kb(back="fine_flow:users"),
    )


@router.callback_query(F.data == "fine_flow:users")
async def fine_back_to_users(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.message or not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    residents = await db.list_active_residents()
    await state.set_state(FineFSM.choosing_user)
    await state.update_data(user_id=None, fine_type=None, amount=None)
    await callback.message.edit_text(
        "⚖️ <b>Керування штрафами</b>\n\nОберіть мешканця для штрафу:",
        reply_markup=residents_kb(residents),
    )
    await callback.answer()


@router.callback_query(F.data == "fine_flow:cancel")
async def fine_cancel(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.message:
        await callback.answer()
        return
    await state.clear()
    from handlers.admin import admin_kb, admin_panel_text

    await callback.message.edit_text(
        await admin_panel_text(db, int(callback.from_user.id) if callback.from_user else 0),
        reply_markup=await admin_kb(db, int(callback.from_user.id) if callback.from_user else 0, include_back=True),
    )
    await callback.answer("Скасовано")


@router.message(FineFSM.entering_type)
async def fine_type(message: Message, state: FSMContext, db: Database, bot) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_FINES_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return
    fine_type = (message.text or "").strip()
    if not fine_type:
        await message.answer("Введіть тип штрафу текстом.")
        return
    await state.update_data(fine_type=fine_type)
    await state.set_state(FineFSM.entering_amount)
    await _update_fine_control_message(
        state,
        bot,
        text="⚖️ <b>Керування штрафами</b>\n\nВведіть суму штрафу в гривнях.",
        reply_markup=fine_control_kb(back="fine_flow:type"),
    )


@router.callback_query(F.data == "fine_flow:type")
async def fine_back_to_type(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(FineFSM.entering_type)
    await callback.message.edit_text(
        "⚖️ <b>Керування штрафами</b>\n\nВведіть тип штрафу або причину.",
        reply_markup=fine_control_kb(back="fine_flow:users"),
    )
    await callback.answer()


@router.message(FineFSM.entering_amount)
async def fine_amount(message: Message, state: FSMContext, db: Database, bot) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_FINES_MANAGE):
        await state.clear()
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
    await state.update_data(amount=amount)
    await state.set_state(FineFSM.entering_date)
    await _update_fine_control_message(
        state,
        bot,
        text="⚖️ <b>Керування штрафами</b>\n\nВведіть дату штрафу у форматі <b>ДД.ММ</b> або <b>ДД.ММ.РРРР</b>.",
        reply_markup=fine_control_kb(back="fine_flow:amount"),
    )


@router.callback_query(F.data == "fine_flow:amount")
async def fine_back_to_amount(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(FineFSM.entering_amount)
    await callback.message.edit_text(
        "⚖️ <b>Керування штрафами</b>\n\nВведіть суму штрафу в гривнях.",
        reply_markup=fine_control_kb(back="fine_flow:type"),
    )
    await callback.answer()


@router.message(FineFSM.entering_date)
async def fine_date(message: Message, state: FSMContext, db: Database, bot) -> None:
    resident = await require_resident(message, db)
    if not resident or not await has_permission(db, int(resident["telegram_id"]), PERM_FINES_MANAGE):
        await state.clear()
        await message.answer("Доступ обмежено")
        return
    try:
        fine_date_value = parse_user_date(message.text or "")
    except ValueError:
        await message.answer("Невірний формат дати. Приклад: <b>21.03</b>.")
        return

    data = await state.get_data()
    user_id = int(data["user_id"])
    fine_type = str(data["fine_type"])
    amount = int(data["amount"])
    fine_id = await db.create_fine(
        user_id=user_id,
        reason=fine_type,
        amount=amount,
        fine_date=fine_date_value,
        fine_type=fine_type,
        issued_by=int(resident["telegram_id"]),
    )
    await db.log_admin_action(
        int(resident["telegram_id"]),
        "issue_fine",
        target_id=user_id,
        details=f"fine_id={fine_id}|type={fine_type}|amount={amount}|date={fine_date_value.isoformat()}",
    )
    await notify_owner_about_delegate_action(
        message.bot,
        db,
        actor_id=int(resident["telegram_id"]),
        action_type="issue_fine",
        details=f"fine_id={fine_id}|type={fine_type}|amount={amount}|date={fine_date_value.isoformat()}",
        target_id=user_id,
    )

    bank_url = await db.get_setting("fine_bank_url", "—")
    fined = await db.get_resident(user_id)
    fined_mention = format_resident_mention(fined)
    await bot.send_message(
        chat_id=int(GROUP_ID),
        text=(
            "🔔 <b>Штраф виписано</b>\n"
            f"Порушник: {fined_mention}\n"
            f"Дата: <b>{fine_date_value.strftime('%d.%m.%Y')}</b>\n"
            f"Тип: {fine_type}\n"
            f"Сума: <b>{amount}</b> грн\n"
            f"Оплатити: {bank_url}"
        ),
    )
    try:
        await bot.send_message(
            chat_id=int(user_id),
            text=(
                "> ⚠️ Вам виписали штраф.\n"
                f"Дата: {fine_date_value.strftime('%d.%m.%Y')}\n"
                f"Тип: {fine_type}\n"
                f"Сума: {amount} грн\n"
                f"Оплатити: {bank_url}"
            ),
            reply_markup=pay_kb(fine_id),
        )
    except Exception:
        pass
    await _update_fine_control_message(
        state,
        bot,
        text="✅ Штраф успішно створено.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ До адмін-панелі", callback_data="admin:back_to_panel")]]
        ),
    )
    await state.clear()
    await message.answer("> ✅ Штраф створено.")


@router.callback_query(F.data.startswith("fine_pay:"))
async def fine_pay_start(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return
    fine_id = int(callback.data.split(":", 1)[1])
    fine = await db.get_fine(fine_id)
    if (
        not fine
        or int(fine["user_id"]) != int(callback.from_user.id)
        or fine["status"] != "pending"
        or not int(fine.get("requires_proof", 1))
        or int(fine.get("amount", 0)) <= 0
    ):
        await callback.answer("Недоступно", show_alert=True)
        return
    await state.set_state(FinePayFSM.waiting_photo)
    await state.update_data(fine_id=fine_id)
    await callback.answer()
    await callback.message.answer("Надішліть фото чека про оплату.")


@router.message(FinePayFSM.waiting_photo)
async def fine_pay_photo(message: Message, state: FSMContext, db: Database, bot) -> None:
    if not message.from_user:
        return
    data = await state.get_data()
    fine_id = int(data.get("fine_id", 0))
    fine = await db.get_fine(fine_id)
    if not fine or int(fine["user_id"]) != int(message.from_user.id):
        await state.clear()
        return
    if not message.photo:
        await message.answer("Потрібно надіслати фото.")
        return

    photo_id = message.photo[-1].file_id
    await db.set_fine_proof(fine_id, photo_id)
    await state.clear()
    await message.answer("> ✅ Чек надіслано адміну на перевірку.")
    await bot.send_photo(
        chat_id=int(ADMIN_ID),
        photo=photo_id,
        caption=(
            "Чек оплати штрафу\n"
            f"Порушник: {fine['user_name']}\n"
            f"Тип: {fine['fine_type'] or fine['reason']}\n"
            f"Дата: {fine['fine_date'] or str(fine['created_at'])[:10]}\n"
            f"Сума: {fine['amount']} грн"
        ),
        reply_markup=admin_pay_review_kb(fine_id),
    )


@router.callback_query(F.data.startswith("fine_ok:"))
async def fine_ok(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    fine_id = int(callback.data.split(":", 1)[1])
    fine = await db.get_fine(fine_id)
    if not fine:
        await callback.answer("Не знайдено", show_alert=True)
        return
    await db.set_fine_status(fine_id, "paid")
    await bot.send_message(chat_id=int(GROUP_ID), text=f"✅ Штраф {fine['user_name']} оплачено.")
    await callback.answer("Підтверджено ✅", show_alert=True)


@router.callback_query(F.data.startswith("fine_fake:"))
async def fine_fake(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not await has_permission(db, int(callback.from_user.id), PERM_FINES_MANAGE):
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    fine_id = int(callback.data.split(":", 1)[1])
    fine = await db.get_fine(fine_id)
    if not fine:
        await callback.answer("Не знайдено", show_alert=True)
        return
    try:
        await bot.send_message(chat_id=int(fine["user_id"]), text="> ❌ Чек не підтверджено. Надішліть його ще раз.")
    except Exception:
        pass
    await callback.answer("Відхилено ❌", show_alert=True)
