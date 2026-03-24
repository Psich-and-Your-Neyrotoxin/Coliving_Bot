from __future__ import annotations

from dataclasses import dataclass

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import GROUP_ID
from database import Database
from handlers.common import (
    SUBMIT_REPORT_BUTTON,
    canonical_full_name,
    format_resident_mention,
    get_zone_assignment_for_date,
    get_zone_title,
    list_enabled_zone_choices,
    kyiv_today,
    require_resident,
)


router = Router(name="swap")


class SwapFSM(StatesGroup):
    choosing_zone = State()
    choosing_person = State()


@dataclass(frozen=True)
class PendingSwap:
    from_id: int
    to_id: int
    zone: str


def residents_kb(residents: list[dict], exclude_id: int) -> InlineKeyboardMarkup:
    buttons = []
    for resident in residents:
        if int(resident["telegram_id"]) == int(exclude_id):
            continue
        buttons.append(
            [
                InlineKeyboardButton(
                    text=canonical_full_name(resident["full_name"], resident.get("username")),
                    callback_data=f"swap_to:{resident['telegram_id']}",
                )
            ]
        )
    buttons = buttons[:89]
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="swap_back:zones")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def zone_kb(options: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            *[
                [InlineKeyboardButton(text=label, callback_data=f"swap_zone:{zone_name}")]
                for zone_name, label in options
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")],
        ]
    )


def approve_kb(*, zone: str, requester_id: int, approver_id: int, swap_from_id: int, swap_to_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Згоден",
                    callback_data=f"swap_ok:{zone}:{requester_id}:{approver_id}:{swap_from_id}:{swap_to_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Відмовити",
                    callback_data=f"swap_no:{zone}:{requester_id}:{approver_id}:{swap_from_id}:{swap_to_id}",
                ),
            ]
        ]
    )


@router.message(Command("swap"))
async def cmd_swap(message: Message, state: FSMContext, db: Database) -> None:
    resident = await require_resident(message, db)
    if not resident:
        return
    if message.chat.type != "private":
        await message.answer("> ⚠️ /swap працює лише в особистих повідомленнях боту.")
        return

    await state.set_state(SwapFSM.choosing_zone)
    await message.answer("За яку зону хочеш помінятися?", reply_markup=zone_kb(await list_enabled_zone_choices(db)))


async def open_swap_menu_from_callback(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.message or not callback.from_user:
        await callback.answer()
        return
    resident = await db.get_resident(int(callback.from_user.id))
    if not resident:
        await callback.answer("Доступ обмежено", show_alert=True)
        return
    await state.set_state(SwapFSM.choosing_zone)
    await callback.message.edit_text(
        "🔄 <b>Обмін</b>\n\nОберіть зону, для якої хочете запропонувати обмін.\n"
        "Бот сам перевірить, чи такий обмін можливий.",
        reply_markup=zone_kb(await list_enabled_zone_choices(db)),
    )
    await callback.answer()

@router.callback_query(SwapFSM.choosing_zone, F.data.startswith("swap_zone:"))
async def on_swap_choose_zone(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    from_id = int(callback.from_user.id)
    zone = callback.data.split(":", 1)[1]

    allowed_zone_names = {zone_name for zone_name, _ in await list_enabled_zone_choices(db)}
    if zone not in allowed_zone_names:
        await state.clear()
        await callback.message.edit_text(
            "Сталася помилка. Спробуй ще раз з головного меню.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        await callback.answer()
        return

    await state.update_data(zone=zone)
    await state.set_state(SwapFSM.choosing_person)

    residents = await db.list_active_residents()
    await callback.answer()
    await callback.message.edit_text(
        "🔄 <b>Обмін</b>\n\nОберіть мешканця, з яким хочете помінятися.\n"
        "Він не зобов'язаний чергувати сьогодні, але має підтвердити запит.",
        reply_markup=residents_kb(residents, exclude_id=from_id),
    )


@router.callback_query(SwapFSM.choosing_person, F.data == "swap_back:zones")
async def on_swap_back_zones(callback: CallbackQuery, state: FSMContext, db: Database) -> None:
    if not callback.message:
        await callback.answer()
        return
    await state.set_state(SwapFSM.choosing_zone)
    zone_options = await list_enabled_zone_choices(db)
    await callback.message.edit_text(
        "🔄 <b>Обмін</b>\n\nЗа яку зону хочеш помінятися?",
        reply_markup=zone_kb(zone_options),
    )
    await callback.answer()


@router.callback_query(SwapFSM.choosing_person, F.data.startswith("swap_to:"))
async def on_swap_choose_person(callback: CallbackQuery, state: FSMContext, db: Database, bot) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    from_id = int(callback.from_user.id)
    to_id = int(callback.data.split(":", 1)[1])
    data = await state.get_data()
    zone = data.get("zone")

    if not to_id or not zone:
        await state.clear()
        await callback.message.edit_text(
            "Сталася помилка. Спробуй ще раз з головного меню.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        await callback.answer()
        return
    if to_id == from_id:
        today = kyiv_today()
        await db.log_swap_attempt(
            from_id=from_id,
            to_id=to_id,
            zone=str(zone),
            target_date=today,
            status="invalid_partner",
            details="self_selected",
        )
        await state.clear()
        await callback.answer("Не можна обмінюватися із собою.", show_alert=True)
        await callback.message.edit_text(
            "⚠️ Ви не можете обмінятися цією зоною.\nПовертаю в головне меню.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        return

    from_res = await db.get_resident(from_id)
    to_res = await db.get_resident(to_id)
    if not from_res or not to_res or not to_res.get("is_active"):
        today = kyiv_today()
        await db.log_swap_attempt(
            from_id=from_id,
            to_id=to_id,
            zone=str(zone),
            target_date=today,
            status="invalid_partner",
            details="target_unavailable",
        )
        await state.clear()
        await callback.message.edit_text(
            "Цей мешканець недоступний або сталася помилка.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        await callback.answer()
        return

    today = kyiv_today()
    assignment_info = await get_zone_assignment_for_date(db, str(zone), today)
    assignees = set(int(item) for item in assignment_info[1]) if assignment_info else set()
    from_on_zone = int(from_id) in assignees
    to_on_zone = int(to_id) in assignees
    if not from_on_zone and not to_on_zone:
        await db.log_swap_attempt(
            from_id=from_id,
            to_id=to_id,
            zone=str(zone),
            target_date=today,
            status="invalid_partner",
            details="no_one_assigned_to_zone",
        )
        await state.clear()
        await callback.answer("Для цієї зони потрібен хоча б один черговий учасник.", show_alert=True)
        await callback.message.edit_text(
            "⚠️ Ви не можете обмінятися цією зоною.\n"
            "Для обміну хоча б один із двох мешканців має бути черговим у вибраній зоні сьогодні.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        return
    if from_on_zone and to_on_zone:
        await db.log_swap_attempt(
            from_id=from_id,
            to_id=to_id,
            zone=str(zone),
            target_date=today,
            status="invalid_partner",
            details="both_already_assigned_to_zone",
        )
        await state.clear()
        await callback.answer("Обидва вже стоять у цій зоні.", show_alert=True)
        await callback.message.edit_text(
            "⚠️ Обидва мешканці вже закріплені за цією зоною сьогодні.\n"
            "Оберіть іншого учасника обміну.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
            ),
        )
        return

    swap_from_id = from_id if from_on_zone else to_id
    swap_to_id = to_id if from_on_zone else from_id

    zone_ua = await get_zone_title(db, str(zone))
    from_mention = format_resident_mention(from_res)

    await bot.send_message(
        chat_id=int(to_id),
        text=(
            f"🔄 <b>Запит на обмін</b>\n"
            f"{from_mention} просить підмінити його в зоні <b>{zone_ua}</b> на сьогодні.\n\n"
            "Якщо погоджуєшся, натисни кнопку нижче."
        ),
        reply_markup=approve_kb(
            zone=str(zone),
            requester_id=from_id,
            approver_id=to_id,
            swap_from_id=swap_from_id,
            swap_to_id=swap_to_id,
        ),
    )
    await db.log_swap_attempt(
        from_id=from_id,
        to_id=to_id,
        zone=str(zone),
        target_date=today,
        status="requested",
    )
    await state.clear()
    await callback.message.edit_text(
        "✅ Запит на обмін надіслано.\n\n"
        "Тепер очікуємо підтвердження від іншого мешканця.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:home")]]
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("swap_ok:"))
async def on_swap_ok(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    parts = callback.data.split(":")
    zone = parts[1]
    requester_id = int(parts[2])
    approver_id = int(parts[3])
    swap_from_id = int(parts[4])
    swap_to_id = int(parts[5])

    if int(callback.from_user.id) != int(approver_id):
        await callback.answer("Доступ обмежено", show_alert=True)
        return

    today = kyiv_today()
    await db.create_swap(zone=zone, from_id=swap_from_id, to_id=swap_to_id, for_date=today)
    await db.log_swap_attempt(
        from_id=requester_id,
        to_id=approver_id,
        zone=zone,
        target_date=today,
        status="accepted",
    )

    zone_ua = await get_zone_title(db, str(zone))
    from_res = await db.get_resident(swap_from_id)
    to_res = await db.get_resident(swap_to_id)
    from_mention = format_resident_mention(from_res)
    to_mention = format_resident_mention(to_res)

    await bot.send_message(chat_id=int(requester_id), text="> ✅ Обмін підтверджено.")
    await callback.message.answer(
        f"> ✅ Прийнято. Тепер можна натиснути кнопку <b>{SUBMIT_REPORT_BUTTON}</b>."
    )
    await bot.send_message(
        chat_id=int(GROUP_ID),
        text=(
            f"🔄 Оновлення! Сьогодні {to_mention} замість {from_mention} "
            f"у зоні <b>{zone_ua}</b>."
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("swap_no:"))
async def on_swap_no(callback: CallbackQuery, db: Database, bot) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    parts = callback.data.split(":")
    zone = parts[1]
    requester_id = int(parts[2])
    approver_id = int(parts[3])
    if int(callback.from_user.id) != int(approver_id):
        await callback.answer("Доступ обмежено", show_alert=True)
        return

    await db.log_swap_attempt(
        from_id=requester_id,
        to_id=approver_id,
        zone=zone,
        target_date=kyiv_today(),
        status="declined",
    )
    await bot.send_message(chat_id=int(requester_id), text="> ❌ Обмін відхилено.")
    await callback.message.answer("> ❌ Відмовлено.")
    await callback.answer()
