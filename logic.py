from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Literal, TypedDict


RESIDENTS_JSON_PATH = Path(__file__).with_name("residents.json")


@dataclass(frozen=True)
class DutyStatus:
    kitchen_today: str
    bath_this_week: str
    common_pair_this_week: str


ZoneName = Literal["Kitchen", "Bath", "General"]


class SwapRecord(TypedDict):
    zone: ZoneName
    from_id: int
    to_id: int
    date: str


@dataclass(frozen=True)
class DutyAssignment:
    kitchen_id: int
    kitchen_name: str
    bath_id: int
    bath_name: str
    general_ids: tuple[int, int]
    general_names: tuple[str, str]


@dataclass(frozen=True)
class DutyResident:
    telegram_id: int
    full_name: str


@dataclass(frozen=True)
class ScheduleConfig:
    kitchen_ids: tuple[int, ...]
    bath_ids: tuple[int, ...]
    general_pairs: tuple[tuple[int, int], ...]


def _resident_map(residents: list[dict]) -> dict[int, dict]:
    return {int(r["telegram_id"]): r for r in residents}


def _apply_swaps(zone: ZoneName, ids: list[int], swaps: list[SwapRecord]) -> list[int]:
    out = ids[:]
    for swap in swaps:
        if swap["zone"] != zone:
            continue
        try:
            index = out.index(int(swap["from_id"]))
        except ValueError:
            continue
        out[index] = int(swap["to_id"])
    return out


@lru_cache(maxsize=4)
def _load_schedule_config_cached(cache_key: tuple[str, int]) -> ScheduleConfig:
    path = Path(cache_key[0])
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("residents.json must be a JSON list")

    kitchen_entries: list[tuple[int, int]] = []
    bath_entries: list[tuple[int, int]] = []
    general_entries: dict[int, dict[int, int]] = {}

    for resident in data:
        telegram_id = int(resident["telegram_id"])

        kitchen_order = resident.get("kitchen_order")
        if kitchen_order is not None:
            kitchen_entries.append((int(kitchen_order), telegram_id))

        bath_order = resident.get("bath_order")
        if bath_order is not None:
            bath_entries.append((int(bath_order), telegram_id))

        pair_order = resident.get("general_pair_order")
        pair_slot = resident.get("general_pair_slot")
        if pair_order is not None or pair_slot is not None:
            if pair_order is None or pair_slot is None:
                raise ValueError(
                    "For General schedule each resident must have both "
                    "general_pair_order and general_pair_slot."
                )
            pair_order_int = int(pair_order)
            pair_slot_int = int(pair_slot)
            if pair_slot_int not in (1, 2):
                raise ValueError("general_pair_slot must be 1 or 2.")
            general_entries.setdefault(pair_order_int, {})[pair_slot_int] = telegram_id

    kitchen_ids = tuple(telegram_id for _, telegram_id in sorted(kitchen_entries))
    bath_ids = tuple(telegram_id for _, telegram_id in sorted(bath_entries))

    general_pairs_list: list[tuple[int, int]] = []
    for _, pair in sorted(general_entries.items()):
        if set(pair) != {1, 2}:
            raise ValueError("Each General pair must contain exactly two residents with slots 1 and 2.")
        general_pairs_list.append((int(pair[1]), int(pair[2])))

    if not kitchen_ids:
        raise ValueError("residents.json must define at least one kitchen_order.")
    if not bath_ids:
        raise ValueError("residents.json must define at least one bath_order.")
    if not general_pairs_list:
        raise ValueError("residents.json must define at least one General pair.")

    return ScheduleConfig(
        kitchen_ids=kitchen_ids,
        bath_ids=bath_ids,
        general_pairs=tuple(general_pairs_list),
    )


def load_schedule_config(path: Path | None = None) -> ScheduleConfig:
    residents_path = path or RESIDENTS_JSON_PATH
    stat = residents_path.stat()
    return _load_schedule_config_cached((str(residents_path.resolve()), stat.st_mtime_ns))


def calculate_duties(today: date | None = None) -> DutyStatus:
    if today is None:
        today = date.today()

    schedule = load_schedule_config()
    day_of_year = int(today.timetuple().tm_yday)
    week_of_year = int(today.isocalendar()[1])

    kitchen = f"ID:{schedule.kitchen_ids[(day_of_year - 1) % len(schedule.kitchen_ids)]}"
    bath = f"ID:{schedule.bath_ids[(week_of_year - 1) % len(schedule.bath_ids)]}"
    g1, g2 = schedule.general_pairs[(week_of_year - 1) % len(schedule.general_pairs)]
    common = f"ID:{g1} + ID:{g2}"

    return DutyStatus(kitchen_today=kitchen, bath_this_week=bath, common_pair_this_week=common)


def get_resident_by_duty(
    residents: list[dict],
    zone: ZoneName,
    target_date: date,
    swaps_for_date: list[SwapRecord] | None = None,
) -> DutyResident | tuple[DutyResident, DutyResident]:
    swaps_for_date = swaps_for_date or []
    resident_map = _resident_map(residents)
    schedule = load_schedule_config()
    day_of_year = int(target_date.timetuple().tm_yday)
    week_of_year = int(target_date.isocalendar()[1])

    if zone == "Kitchen":
        base_id = schedule.kitchen_ids[(day_of_year - 1) % len(schedule.kitchen_ids)]
        duty_id = _apply_swaps("Kitchen", [base_id], swaps_for_date)[0]
        resident = resident_map[duty_id]
        return DutyResident(telegram_id=int(resident["telegram_id"]), full_name=str(resident["full_name"]))

    if zone == "Bath":
        base_id = schedule.bath_ids[(week_of_year - 1) % len(schedule.bath_ids)]
        duty_id = _apply_swaps("Bath", [base_id], swaps_for_date)[0]
        resident = resident_map[duty_id]
        return DutyResident(telegram_id=int(resident["telegram_id"]), full_name=str(resident["full_name"]))

    g1, g2 = schedule.general_pairs[(week_of_year - 1) % len(schedule.general_pairs)]
    duty_ids = _apply_swaps("General", [g1, g2], swaps_for_date)
    resident_1 = resident_map[duty_ids[0]]
    resident_2 = resident_map[duty_ids[1]]
    return (
        DutyResident(telegram_id=int(resident_1["telegram_id"]), full_name=str(resident_1["full_name"])),
        DutyResident(telegram_id=int(resident_2["telegram_id"]), full_name=str(resident_2["full_name"])),
    )


def calculate_assignment(
    residents: list[dict],
    swaps_for_date: list[SwapRecord] | None = None,
    today: date | None = None,
) -> DutyAssignment:
    if today is None:
        today = date.today()
    swaps_for_date = swaps_for_date or []

    kitchen_resident = get_resident_by_duty(residents, "Kitchen", today, swaps_for_date)
    bath_resident = get_resident_by_duty(residents, "Bath", today, swaps_for_date)
    general_residents = get_resident_by_duty(residents, "General", today, swaps_for_date)
    assert isinstance(kitchen_resident, DutyResident)
    assert isinstance(bath_resident, DutyResident)
    assert isinstance(general_residents, tuple)
    general_1, general_2 = general_residents

    return DutyAssignment(
        kitchen_id=int(kitchen_resident.telegram_id),
        kitchen_name=str(kitchen_resident.full_name),
        bath_id=int(bath_resident.telegram_id),
        bath_name=str(bath_resident.full_name),
        general_ids=(int(general_1.telegram_id), int(general_2.telegram_id)),
        general_names=(str(general_1.full_name), str(general_2.full_name)),
    )


def tomorrow(d: date | None = None) -> date:
    if d is None:
        d = date.today()
    return d + timedelta(days=1)
