from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import ADMIN_ID
from database import Database
from deadline_policy import deadline_user_override_key, deadline_waive_key, get_deadline_due_at, get_deadline_due_at_for_user
from handlers.common import (
    SUBMIT_REPORT_BUTTON,
    calendar_exception_blocks_duties,
    format_resident_name_plain,
    get_assignment_for_date,
    get_calendar_exception,
    get_runtime_zone_assignments_for_date,
    get_zone_title,
    kyiv_now,
)
from handlers.duty import deadline_moderation_kb
from instance_config import get_legacy_zone_from_definition, is_zone_report_day, load_instance_definition

JOB_GROUP_MORNING = "group_morning"
JOB_KITCHEN_PRIVATE = "kitchen_private"
JOB_BATH_PRIVATE = "bath_private"
JOB_GENERAL_PRIVATE = "general_private"
JOB_DEADLINES = "deadlines_0105"
JOB_PENDING_REVIEW = "pending_review_reminder"
JOB_REJECTED_FOLLOWUP = "rejected_followup_reminder"
JOB_MONTHLY_PAYMENT_REMINDER = "monthly_payment_reminder"
JOB_RUNTIME_ZONE_PRIVATE_PREFIX = "zone_private:"
REMINDER_SKIP_DATES_KEY = "reminder_skip_dates_json"
KYIV_WALL_TZ = ZoneInfo("Europe/Kyiv")
KYIV_TZ = KYIV_WALL_TZ


def _parse_hhmm(value: str) -> tuple[int, int]:
    v = value.strip()
    hh, mm = v.split(":", 1)
    h = int(hh)
    m = int(mm)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError("Bad time")
    return h, m


def _fmt_date_ua(d: date) -> str:
    return d.strftime("%d.%m.%Y")


def _now_kyiv_wall() -> datetime:
    return datetime.now(KYIV_WALL_TZ)


def _fmt_kyiv_wall(dt: datetime | None = None) -> str:
    current = (dt or _now_kyiv_wall()).astimezone(KYIV_WALL_TZ)
    return current.strftime("%d.%m.%Y %H:%M:%S %Z")


async def should_skip_scheduled_reminders(db: Database, target_date: date | None = None) -> bool:
    check_date = target_date or kyiv_now().date()
    raw = await db.get_setting(REMINDER_SKIP_DATES_KEY, "[]")
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return False
    return check_date.isoformat() in {str(item) for item in parsed}


async def should_skip_any_scheduled_reminder_date(db: Database, *dates_to_check: date) -> bool:
    for check_date in dates_to_check:
        if await should_skip_scheduled_reminders(db, check_date):
            return True
    return False


def _format_job_next_run(job) -> str:
    next_run = getattr(job, "next_run_time", None)
    if not next_run:
        return "not scheduled"
    return _fmt_kyiv_wall(next_run)


def _parse_deadline_hhmm(value: str, default: str = "01:00") -> tuple[int, int]:
    try:
        return _parse_hhmm(value)
    except Exception:
        return _parse_hhmm(default)


async def _get_plain_name_text(db: Database, telegram_id: int, fallback_name: str | None = None) -> str:
    resident = await db.get_resident(telegram_id)
    return format_resident_name_plain(resident, fallback_name or f"Користувач {telegram_id}")


async def _get_bot_username_cached(bot) -> str | None:
    cached = getattr(bot, "_cached_public_username", None)
    if cached:
        return str(cached)
    try:
        me = await bot.get_me()
    except Exception:
        return None
    username = getattr(me, "username", None)
    if username:
        setattr(bot, "_cached_public_username", str(username))
        return str(username)
    return None


async def _open_report_bot_kb(bot) -> InlineKeyboardMarkup | None:
    username = await _get_bot_username_cached(bot)
    if not username:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=SUBMIT_REPORT_BUTTON, url=f"https://t.me/{username}?start=report")]
        ]
    )


async def send_group_morning_reminder(bot, db: Database, group_id: int, *, force: bool = False) -> None:
    logging.info("Групове оголошення стартувало | Kyiv=%s", _fmt_kyiv_wall())
    today = kyiv_now().date()
    if not force and await should_skip_scheduled_reminders(db, today):
        logging.info("Групове оголошення пропущено | Kyiv=%s | date=%s | reason=skip_date", _fmt_kyiv_wall(), today.isoformat())
        return
    calendar_exception = await get_calendar_exception(db, today)
    if calendar_exception_blocks_duties(calendar_exception):
        title_map = {"holiday": "Свято / вихідний", "day_off": "День без чергувань"}
        title = title_map.get(str(calendar_exception.get("kind")), "Особливий день")
        note = str(calendar_exception.get("note") or "").strip()
        text = (
            f"🗓 <b>ОСОБЛИВИЙ ДЕНЬ ({_fmt_date_ua(today)})</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"📌 <b>{title}</b>\n"
            "Сьогодні чергувань немає.\n"
        )
        if note:
            text += f"ℹ️ {note}\n"
        text += "━━━━━━━━━━━━━━"
        await bot.send_message(chat_id=int(group_id), text=text)
        logging.info("Групове оголошення про виняток надіслано | Kyiv=%s | group_id=%s | kind=%s", _fmt_kyiv_wall(), int(group_id), str(calendar_exception.get("kind")))
        return
    try:
        definition, runtime_assignments = await get_runtime_zone_assignments_for_date(db, today)
        zone_lines: list[str] = []
        for zone in sorted(definition.zones, key=lambda item: item.sort_order):
            if not zone.enabled or not zone.rotation_enabled:
                continue
            assignment = runtime_assignments.get(zone.code)
            if not assignment or not assignment.member_ids:
                continue
            name_texts = [
                await _get_plain_name_text(db, int(member_id), member_name)
                for member_id, member_name in zip(assignment.member_ids, assignment.member_names)
            ]
            zone_lines.append(f"• <b>{zone.title}:</b> {' &amp; '.join(name_texts)}")
        if not zone_lines:
            raise ValueError("No runtime zone assignments available")
        text = (
            f"🗓 <b>ГРАФІК НА СЬОГОДНІ ({_fmt_date_ua(today)})</b>\n"
            "━━━━━━━━━━━━━━\n"
            + "\n".join(zone_lines)
            + "\n"
        )
        log_details = " | ".join(zone_lines)
    except Exception:
        assignment = await get_assignment_for_date(db, today)
        kitchen_text = await _get_plain_name_text(db, int(assignment.kitchen_id), assignment.kitchen_name)
        bath_text = await _get_plain_name_text(db, int(assignment.bath_id), assignment.bath_name)
        general_text_1 = await _get_plain_name_text(db, int(assignment.general_ids[0]), assignment.general_names[0])
        general_text_2 = await _get_plain_name_text(db, int(assignment.general_ids[1]), assignment.general_names[1])

        text = (
            f"🗓 <b>ГРАФІК НА СЬОГОДНІ ({_fmt_date_ua(today)})</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"🍴 <b>Кухня:</b> {kitchen_text}\n"
            f"🚿 <b>Ванна:</b> {bath_text}\n"
            f"📦 <b>Общак:</b> {general_text_1} &amp; {general_text_2}\n"
        )
        log_details = f"kitchen={kitchen_text} | bath={bath_text} | general={general_text_1} & {general_text_2}"
    if calendar_exception and str(calendar_exception.get("kind")) == "special_rules":
        note = str(calendar_exception.get("note") or "").strip()
        if note:
            text += f"━━━━━━━━━━━━━━\nℹ️ <b>Змінені правила:</b> {note}\n"
    text += "━━━━━━━━━━━━━━\nЩоб здати звіт, натисни кнопку нижче."

    msg = await bot.send_message(chat_id=int(group_id), text=text, reply_markup=await _open_report_bot_kb(bot))
    logging.info(
        "Групове оголошення надіслано | Kyiv=%s | group_id=%s | details=%s",
        _fmt_kyiv_wall(),
        int(group_id),
        log_details,
    )

    try:
        await bot.unpin_all_chat_messages(chat_id=int(group_id))
        await bot.pin_chat_message(chat_id=int(group_id), message_id=msg.message_id, disable_notification=True)
    except Exception:
        logging.exception("Немає прав на закріплення")


@dataclass
class SchedulerService:
    bot: object
    db: Database
    group_id: int
    timezone: str = "Europe/Kyiv"

    def __post_init__(self) -> None:
        self.scheduler = AsyncIOScheduler(timezone=self.timezone)
        self.tzinfo = ZoneInfo(self.timezone)

    def _log_job_state(self, job_id: str) -> None:
        job = self.scheduler.get_job(job_id)
        if not job:
            logging.info("Scheduler job status | job_id=%s | state=missing", job_id)
            return
        logging.info(
            "Scheduler job status | job_id=%s | next_run_kyiv=%s",
            job_id,
            _format_job_next_run(job),
        )

    def _log_all_job_states(self) -> None:
        for job in sorted(self.scheduler.get_jobs(), key=lambda item: str(item.id)):
            self._log_job_state(str(job.id))

    def _private_job_id_for_zone(self, zone_code: str) -> str:
        mapping = {
            "kitchen": JOB_KITCHEN_PRIVATE,
            "bath": JOB_BATH_PRIVATE,
            "general": JOB_GENERAL_PRIVATE,
        }
        return mapping.get(str(zone_code), f"{JOB_RUNTIME_ZONE_PRIVATE_PREFIX}{zone_code}")

    async def _safe_pm(self, telegram_id: int, text: str) -> None:
        await self._safe_pm_with_markup(telegram_id, text, reply_markup=None)

    async def _safe_pm_with_markup(
        self,
        telegram_id: int,
        text: str,
        *,
        reply_markup: InlineKeyboardMarkup | None,
    ) -> None:
        try:
            await self.bot.send_message(chat_id=int(telegram_id), text=text, reply_markup=reply_markup)
            await self.db.mark_message_delivery(int(telegram_id), True)
            logging.info("Приватне нагадування надіслано | Kyiv=%s | telegram_id=%s", _fmt_kyiv_wall(), int(telegram_id))
        except Exception:
            await self.db.mark_message_delivery(int(telegram_id), False, "Користувач заблокував бота або не почав діалог")
            logging.info("Приватне нагадування не доставлено | Kyiv=%s | telegram_id=%s", _fmt_kyiv_wall(), telegram_id)

    async def _get_plain_name_text(self, telegram_id: int, fallback_name: str | None = None) -> str:
        return await _get_plain_name_text(self.db, telegram_id, fallback_name)

    async def _should_skip_today(self) -> bool:
        today = kyiv_now().date()
        if await should_skip_scheduled_reminders(self.db, today):
            logging.info("Планові нагадування пропущено | Kyiv=%s | date=%s | reason=skip_date", _fmt_kyiv_wall(), today.isoformat())
            return True
        calendar_exception = await get_calendar_exception(self.db, today)
        if calendar_exception_blocks_duties(calendar_exception):
            logging.info(
                "Планові нагадування по чергуваннях пропущено | Kyiv=%s | date=%s | reason=calendar_exception | kind=%s",
                _fmt_kyiv_wall(),
                today.isoformat(),
                str(calendar_exception.get("kind")),
            )
            return True
        return False

    async def _get_runtime_legacy_zone(self, legacy_zone_name: str):
        try:
            definition = await load_instance_definition(self.db)
            return get_legacy_zone_from_definition(definition, legacy_zone_name)
        except Exception:
            return None

    async def _is_legacy_report_day(self, legacy_zone_name: str, target_date: date) -> bool:
        zone = await self._get_runtime_legacy_zone(legacy_zone_name)
        if zone:
            return bool(zone.enabled and zone.report_required and is_zone_report_day(zone, target_date))
        if legacy_zone_name == "Kitchen":
            return True
        if legacy_zone_name == "Bath":
            return target_date.weekday() == 6
        if legacy_zone_name == "General":
            return target_date.weekday() == 2
        return False

    async def _private_time_default(self, legacy_zone_name: str, fallback: str) -> str:
        zone = await self._get_runtime_legacy_zone(legacy_zone_name)
        if zone and zone.private_reminder_time:
            return str(zone.private_reminder_time)
        return fallback

    async def _is_private_reminder_enabled(self, legacy_zone_name: str) -> bool:
        zone = await self._get_runtime_legacy_zone(legacy_zone_name)
        if zone is None:
            return True
        return bool(zone.enabled and zone.private_reminder_enabled and zone.rotation_enabled)

    async def _send_runtime_zone_reminder(self, zone_code: str, *, force: bool = False) -> None:
        logging.info("Старт нагадування по runtime-зоні | Kyiv=%s | zone=%s", _fmt_kyiv_wall(), zone_code)
        if not force and await self._should_skip_today():
            return
        today = kyiv_now().date()
        definition, runtime_assignments = await get_runtime_zone_assignments_for_date(self.db, today)
        zone = next((item for item in definition.zones if item.code == zone_code), None)
        if not zone:
            logging.info("Нагадування по runtime-зоні пропущено | zone=%s | reason=missing_zone", zone_code)
            return
        if not zone.enabled or not zone.rotation_enabled:
            logging.info("Нагадування по runtime-зоні пропущено | zone=%s | reason=zone_disabled", zone_code)
            return
        if not force and not zone.private_reminder_enabled:
            logging.info("Нагадування по runtime-зоні пропущено | zone=%s | reason=private_disabled", zone_code)
            return
        if not force and zone.report_required and not is_zone_report_day(zone, today):
            logging.info("Нагадування по runtime-зоні пропущено | zone=%s | reason=not_report_day", zone_code)
            return
        assignment = runtime_assignments.get(zone_code)
        if not assignment or not assignment.member_ids:
            logging.info("Нагадування по runtime-зоні пропущено | zone=%s | reason=no_assignment", zone_code)
            return

        for member_id, member_name in zip(assignment.member_ids, assignment.member_names):
            name_text = await self._get_plain_name_text(int(member_id), member_name)
            logging.info(
                "Підготовлено приватне нагадування по runtime-зоні | Kyiv=%s | zone=%s | zone_title=%s | resident=%s | telegram_id=%s",
                _fmt_kyiv_wall(),
                zone.code,
                zone.title,
                name_text,
                int(member_id),
            )
            if zone.report_required and is_zone_report_day(zone, today):
                text = (
                    f"⏰ Нагадування: сьогодні день звіту по зоні <b>{zone.title}</b>.\n"
                    f"Щоб здати звіт, відкрий бот і натисни <b>{SUBMIT_REPORT_BUTTON}</b>."
                )
            else:
                text = f"⏰ Нагадування: сьогодні твоя черга в зоні <b>{zone.title}</b>."
            await self._safe_pm(int(member_id), text)

    async def send_kitchen_reminder(self, *, force: bool = False) -> None:
        await self._send_runtime_zone_reminder("kitchen", force=force)

    async def send_bathroom_reminder(self, *, force: bool = False) -> None:
        await self._send_runtime_zone_reminder("bath", force=force)

    async def send_common_reminder(self, *, force: bool = False) -> None:
        await self._send_runtime_zone_reminder("general", force=force)

    async def send_zone_reminder(self, zone_code: str, *, force: bool = False) -> None:
        await self._send_runtime_zone_reminder(str(zone_code), force=force)

    async def send_all_private_zone_reminders(self, *, force: bool = False) -> None:
        today = kyiv_now().date()
        definition, _ = await get_runtime_zone_assignments_for_date(self.db, today)
        for zone in sorted(definition.zones, key=lambda item: item.sort_order):
            if not zone.enabled or not zone.rotation_enabled or not zone.private_reminder_enabled:
                continue
            await self._send_runtime_zone_reminder(zone.code, force=force)

    async def send_monthly_payment_reminders(self, *, force: bool = False) -> None:
        now = kyiv_now()
        period_label = now.strftime("%m.%Y")
        logging.info("Старт нагадувань про оплату | Kyiv=%s | period=%s", _fmt_kyiv_wall(now), period_label)
        if not force and await should_skip_scheduled_reminders(self.db, now.date()):
            logging.info(
                "Нагадування про оплату пропущено | Kyiv=%s | period=%s | reason=skip_date",
                _fmt_kyiv_wall(now),
                period_label,
            )
            return
        residents = await self.db.list_active_residents_full()
        sent_count = 0
        skipped_count = 0

        for resident in residents:
            telegram_id = int(resident["telegram_id"])
            name_text = await self._get_plain_name_text(telegram_id, resident.get("full_name"))
            folder_url = ((await self.db.get_setting(f"payment_folder:{telegram_id}", "")) or "").strip()
            if not folder_url:
                skipped_count += 1
                logging.info(
                    "Нагадування про оплату пропущено | Kyiv=%s | telegram_id=%s | resident=%s | reason=no_folder_url",
                    _fmt_kyiv_wall(now),
                    telegram_id,
                    name_text,
                )
                continue

            await self._safe_pm_with_markup(
                telegram_id,
                (
                    f"💳 Нагадування про оплату за <b>{period_label}</b>.\n"
                    "Будь ласка, до <b>5 числа</b> завантаж квитанцію про оплату у свою Google-папку.\n"
                    "Підписуйте квитанцію так: <b>Ім'я Прізвище (місяць рік)</b>.\n"
                    "Приклад: <b>Ярослав Шарга (Січень 2026)</b>."
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="📁 Відкрити папку для квитанції", url=folder_url)]
                    ]
                ),
            )
            sent_count += 1
            logging.info(
                "Нагадування про оплату підготовлено | Kyiv=%s | telegram_id=%s | resident=%s | folder_url_set=1",
                _fmt_kyiv_wall(now),
                telegram_id,
                name_text,
            )

        logging.info(
            "Нагадування про оплату завершено | Kyiv=%s | period=%s | sent=%s | skipped=%s",
            _fmt_kyiv_wall(now),
            period_label,
            sent_count,
            skipped_count,
        )

    async def _kitchen_private(self) -> None:
        await self.send_kitchen_reminder()

    async def _bath_private(self) -> None:
        await self.send_bathroom_reminder()

    async def _general_private(self) -> None:
        await self.send_common_reminder()

    async def _monthly_payment_reminder(self) -> None:
        await self.send_monthly_payment_reminders()

    async def _send_deadline_user_reminder(
        self,
        *,
        zone: str,
        zone_title: str,
        user_id: int,
        name: str,
        duty_date: date,
        due_at: datetime,
        stage: str,
    ) -> None:
        if await self.db.has_deadline_user_reminder(int(user_id), zone, duty_date, stage):
            return
        name_text = await self._get_plain_name_text(user_id, name)
        if stage == "before_1h":
            text = (
                f"⏰ Нагадування: до дедлайну по зоні <b>{zone_title}</b> лишилась приблизно <b>1 година</b>.\n"
                f"Дата чергування: <b>{_fmt_date_ua(duty_date)}</b>\n"
                f"Дедлайн: <b>{due_at.strftime('%d.%m %H:%M')}</b>\n"
                f"Якщо ти ще не здав звіт, відкрий бот і натисни <b>{SUBMIT_REPORT_BUTTON}</b>."
            )
        elif stage == "at_deadline":
            text = (
                f"🚨 Дедлайн по зоні <b>{zone_title}</b> вже настав.\n"
                f"Дата чергування: <b>{_fmt_date_ua(duty_date)}</b>\n"
                f"Дедлайн був: <b>{due_at.strftime('%d.%m %H:%M')}</b>\n"
                f"Будь ласка, якнайшвидше відкрий бот і натисни <b>{SUBMIT_REPORT_BUTTON}</b>."
            )
        else:
            text = (
                f"⚠️ Звіт по зоні <b>{zone_title}</b> все ще не здано вже кілька годин після дедлайну.\n"
                f"Дата чергування: <b>{_fmt_date_ua(duty_date)}</b>\n"
                f"Дедлайн був: <b>{due_at.strftime('%d.%m %H:%M')}</b>\n"
                f"Якщо це ще не зроблено, відкрий бот і натисни <b>{SUBMIT_REPORT_BUTTON}</b>."
            )
        logging.info(
            "Підготовлено дедлайн-нагадування мешканцю | Kyiv=%s | zone=%s | resident=%s | duty_date=%s | stage=%s",
            _fmt_kyiv_wall(),
            zone,
            name_text,
            duty_date.isoformat(),
            stage,
        )
        await self._safe_pm(int(user_id), text)
        await self.db.mark_deadline_user_reminder(int(user_id), zone, duty_date, stage)

    async def _deadlines(self) -> None:
        logging.info("Перевірка дедлайнів стартувала | Kyiv=%s", _fmt_kyiv_wall())
        now = kyiv_now()
        today = now.date()
        if await should_skip_scheduled_reminders(self.db, today):
            logging.info("Перевірка дедлайнів пропущена | Kyiv=%s | date=%s | reason=skip_date", _fmt_kyiv_wall(now), today.isoformat())
            return
        # Не спамимо адміна історичними прострочками після рестарту:
        # алерт має приходити лише в "свіжому" вікні після дедлайну.
        fresh_alert_window = timedelta(hours=12)
        for shift in range(0, 8):
            duty_date = today - timedelta(days=shift)
            calendar_exception = await get_calendar_exception(self.db, duty_date)
            if calendar_exception_blocks_duties(calendar_exception):
                logging.info(
                    "Перевірка дедлайнів для дати пропущена | Kyiv=%s | duty_date=%s | reason=calendar_exception | kind=%s",
                    _fmt_kyiv_wall(now),
                    duty_date.isoformat(),
                    str(calendar_exception.get("kind")),
                )
                continue
            definition, runtime_assignments = await get_runtime_zone_assignments_for_date(self.db, duty_date)
            checks: list[tuple[str, str, int, str]] = []
            for zone in sorted(definition.zones, key=lambda item: item.sort_order):
                if not zone.enabled or not zone.rotation_enabled or not zone.report_required:
                    continue
                if not is_zone_report_day(zone, duty_date):
                    continue
                assignment = runtime_assignments.get(zone.code)
                if not assignment:
                    continue
                for user_id, name in zip(assignment.member_ids, assignment.member_names):
                    checks.append((zone.code, zone.title, int(user_id), str(name)))

            for zone, zone_title, user_id, name in checks:
                waived = (await self.db.get_setting(deadline_waive_key(zone, duty_date, int(user_id)), "0")) == "1"
                if waived:
                    continue
                due_at = await self._deadline_due_at_for_user(zone, duty_date, int(user_id))
                if await should_skip_any_scheduled_reminder_date(self.db, duty_date, due_at.date()):
                    logging.info(
                        "Перевірка дедлайну пропущена | Kyiv=%s | duty_date=%s | due_date=%s | zone=%s | reason=skip_date",
                        _fmt_kyiv_wall(now),
                        duty_date.isoformat(),
                        due_at.date().isoformat(),
                        zone,
                    )
                    continue
                ok = await self.db.has_duty_submission(zone_name=zone, user_id=int(user_id), for_date=duty_date)
                if ok:
                    continue
                if due_at - timedelta(hours=1) <= now < due_at:
                    await self._send_deadline_user_reminder(
                        zone=zone,
                        zone_title=zone_title,
                        user_id=int(user_id),
                        name=name,
                        duty_date=duty_date,
                        due_at=due_at,
                        stage="before_1h",
                    )
                if now < due_at:
                    continue
                if now <= due_at + timedelta(hours=1):
                    await self._send_deadline_user_reminder(
                        zone=zone,
                        zone_title=zone_title,
                        user_id=int(user_id),
                        name=name,
                        duty_date=duty_date,
                        due_at=due_at,
                        stage="at_deadline",
                    )
                if now >= due_at + timedelta(hours=3):
                    await self._send_deadline_user_reminder(
                        zone=zone,
                        zone_title=zone_title,
                        user_id=int(user_id),
                        name=name,
                        duty_date=duty_date,
                        due_at=due_at,
                        stage="after_3h",
                    )
                if now - due_at > fresh_alert_window:
                    continue
                if await self.db.has_deadline_alert(int(user_id), zone, duty_date):
                    continue
                mention_text = await self._get_plain_name_text(user_id, name)
                logging.info(
                    "Знайдено прострочений звіт | Kyiv=%s | zone=%s | resident=%s | duty_date=%s | due_at=%s",
                    _fmt_kyiv_wall(),
                    zone,
                    mention_text,
                    duty_date.isoformat(),
                    due_at.isoformat(),
                )
                await self.bot.send_message(
                    chat_id=int(ADMIN_ID),
                    text=(
                        f"🚨 <b>Увага!</b> {mention_text} не здав(ла) звіт "
                        f"за <b>{zone_title}</b> за <b>{_fmt_date_ua(duty_date)}</b> до дедлайну "
                        f"<b>{due_at.strftime('%d.%m %H:%M')}</b>.\n"
                        "Оберіть тип штрафу нижче."
                    ),
                    reply_markup=deadline_moderation_kb(zone, int(user_id), duty_date.isoformat()),
                )
                await self.db.mark_deadline_alert(int(user_id), zone, duty_date)

    async def _deadline_due_at_for_user(self, zone: str, duty_date: date, user_id: int) -> datetime:
        try:
            return await get_deadline_due_at_for_user(self.db, zone, duty_date, user_id)
        except Exception:
            override_key = deadline_user_override_key(zone, duty_date, user_id)
            raw_override = (await self.db.get_setting(override_key, "")) or ""
            logging.info(
                "Некоректний персональний дедлайн override | key=%s | value=%s",
                override_key,
                raw_override,
            )
            return await get_deadline_due_at(self.db, zone, duty_date)

    async def _deadline_due_at(self, zone: str, duty_date: date) -> datetime:
        return await get_deadline_due_at(self.db, zone, duty_date)

    async def _pending_review_reminder(self) -> None:
        logging.info("Перевірка pending-звітів стартувала | Kyiv=%s", _fmt_kyiv_wall())
        if await should_skip_scheduled_reminders(self.db, kyiv_now().date()):
            logging.info("Нагадування по pending review пропущено | Kyiv=%s | reason=skip_date", _fmt_kyiv_wall())
            return
        logs = await self.db.list_pending_review_logs()
        for log in logs:
            zone_ua = await get_zone_title(self.db, str(log["zone_name"]))
            name_text = await self._get_plain_name_text(int(log["telegram_id"]), log.get("full_name"))
            await self.bot.send_message(
                chat_id=int(ADMIN_ID),
                text=(
                    "⏳ <b>Є неперевірений звіт</b>\n"
                    f"Мешканець: {name_text}\n"
                    f"Зона: <b>{zone_ua}</b>\n"
                    f"Дата чергування: <b>{log.get('duty_date') or '—'}</b>\n"
                    "Звіт усе ще очікує перевірки."
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Відкрити в адмінці", callback_data="admin:report_history")]
                    ]
                ),
            )
            await self.db.mark_admin_reminder_sent(int(log["id"]))

    async def _rejected_followup_reminder(self) -> None:
        logging.info("Перевірка відхилених звітів стартувала | Kyiv=%s", _fmt_kyiv_wall())
        if await should_skip_scheduled_reminders(self.db, kyiv_now().date()):
            logging.info("Нагадування по rejected follow-up пропущено | Kyiv=%s | reason=skip_date", _fmt_kyiv_wall())
            return
        logs = await self.db.list_rejected_logs_for_user_reminder()
        for log in logs:
            zone_ua = await get_zone_title(self.db, str(log["zone_name"]))
            reason = str(log.get("admin_comment") or "Причину дивись у попередньому повідомленні.")
            await self._safe_pm(
                int(log["telegram_id"]),
                (
                    "🔁 Нагадування про перездачу звіту.\n"
                    f"Зона: <b>{zone_ua}</b>\n"
                    f"Дата: <b>{log.get('duty_date') or '—'}</b>\n"
                    f"Причина відхилення: {reason}\n"
                    f"Будь ласка, знову відкрий бот і натисни <b>{SUBMIT_REPORT_BUTTON}</b>."
                ),
            )
            await self.db.mark_user_reminder_sent(int(log["id"]))

    def start(self) -> None:
        async def _load_time(key: str) -> tuple[int, int]:
            defaults = {
                JOB_GROUP_MORNING: "09:00",
                JOB_MONTHLY_PAYMENT_REMINDER: "10:00",
            }
            raw = await self.db.get_setting(f"time:{key}", defaults[key])
            return _parse_hhmm(raw or defaults[key])

        async def _setup() -> None:
            definition = await load_instance_definition(self.db)
            group_hour, group_minute = await _load_time(JOB_GROUP_MORNING)
            payment_hour, payment_minute = await _load_time(JOB_MONTHLY_PAYMENT_REMINDER)

            self.scheduler.add_job(
                send_group_morning_reminder,
                CronTrigger(hour=group_hour, minute=group_minute, timezone=self.tzinfo),
                id=JOB_GROUP_MORNING,
                kwargs={"bot": self.bot, "db": self.db, "group_id": self.group_id},
                replace_existing=True,
            )
            for zone in sorted(definition.zones, key=lambda item: item.sort_order):
                if not zone.enabled or not zone.rotation_enabled or not zone.private_reminder_enabled:
                    continue
                if not zone.private_reminder_time:
                    continue
                hour, minute = _parse_hhmm(str((await self.db.get_setting(f"time:{self._private_job_id_for_zone(zone.code)}", zone.private_reminder_time)) or zone.private_reminder_time))
                self.scheduler.add_job(
                    self._send_runtime_zone_reminder,
                    CronTrigger(hour=hour, minute=minute, timezone=self.tzinfo),
                    id=self._private_job_id_for_zone(zone.code),
                    kwargs={"zone_code": zone.code},
                    replace_existing=True,
                )
            self.scheduler.add_job(
                self._deadlines,
                CronTrigger(minute="*/15", timezone=self.tzinfo),
                id=JOB_DEADLINES,
                replace_existing=True,
            )
            self.scheduler.add_job(
                self._pending_review_reminder,
                CronTrigger(minute="*/30", timezone=self.tzinfo),
                id=JOB_PENDING_REVIEW,
                replace_existing=True,
            )
            self.scheduler.add_job(
                self._rejected_followup_reminder,
                CronTrigger(minute="*/45", timezone=self.tzinfo),
                id=JOB_REJECTED_FOLLOWUP,
                replace_existing=True,
            )
            self.scheduler.add_job(
                self._monthly_payment_reminder,
                CronTrigger(day="1,4", hour=payment_hour, minute=payment_minute, timezone=self.tzinfo),
                id=JOB_MONTHLY_PAYMENT_REMINDER,
                replace_existing=True,
            )

            self.scheduler.start()
            self._log_all_job_states()

        asyncio.create_task(_setup())

    async def reschedule(self, job_id: str, hhmm: str) -> None:
        hour, minute = _parse_hhmm(hhmm)
        if job_id == JOB_MONTHLY_PAYMENT_REMINDER:
            trigger = CronTrigger(day="1,4", hour=hour, minute=minute, timezone=self.tzinfo)
        else:
            trigger = CronTrigger(hour=hour, minute=minute, timezone=self.tzinfo)
        self.scheduler.reschedule_job(job_id, trigger=trigger)
        await self.db.set_setting(f"time:{job_id}", hhmm)
        logging.info("Job %s rescheduled to %s", job_id, hhmm)
        self._log_job_state(job_id)
