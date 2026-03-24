from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from instance_config import InstanceDefinition
from rotation_engine import compute_zone_rotation


LEGACY_ZONE_NAME_BY_CODE = {
    "kitchen": "Kitchen",
    "bath": "Bath",
    "general": "General",
}
LEGACY_ZONE_CODE_BY_NAME = {value: key for key, value in LEGACY_ZONE_NAME_BY_CODE.items()}


@dataclass(frozen=True)
class RuntimeZoneAssignment:
    code: str
    title: str
    member_ids: tuple[int, ...]
    member_names: tuple[str, ...]


def assignment_for_zone(assignments: dict[str, RuntimeZoneAssignment], zone_code: str) -> RuntimeZoneAssignment | None:
    return assignments.get(str(zone_code))


def _apply_swap_ids(member_ids: tuple[int, ...], *, from_id: int, to_id: int) -> tuple[int, ...]:
    mutable = list(member_ids)
    try:
        index = mutable.index(int(from_id))
    except ValueError:
        return member_ids
    mutable[index] = int(to_id)
    return tuple(mutable)


def build_zone_assignments(
    definition: InstanceDefinition,
    resident_map: dict[int, dict],
    target_date: date,
) -> dict[str, RuntimeZoneAssignment]:
    assignments: dict[str, RuntimeZoneAssignment] = {}
    for zone in sorted(definition.zones, key=lambda item: item.sort_order):
        if not zone.enabled or not zone.rotation_enabled:
            continue
        member_ids = tuple(int(member_id) for member_id in compute_zone_rotation(zone.rule, target_date))
        if not member_ids:
            continue
        member_names = tuple(
            str(resident_map.get(member_id, {}).get("full_name") or f"ID:{member_id}")
            for member_id in member_ids
        )
        assignments[zone.code] = RuntimeZoneAssignment(
            code=zone.code,
            title=zone.title,
            member_ids=member_ids,
            member_names=member_names,
        )
    return assignments


def apply_legacy_swaps_to_assignments(
    assignments: dict[str, RuntimeZoneAssignment],
    resident_map: dict[int, dict],
    swaps_for_date: list[dict],
) -> dict[str, RuntimeZoneAssignment]:
    updated = dict(assignments)
    for swap in swaps_for_date:
        zone_name = str(swap.get("zone") or "")
        zone_code = LEGACY_ZONE_CODE_BY_NAME.get(zone_name)
        if not zone_code or zone_code not in updated:
            continue
        current = updated[zone_code]
        member_ids = _apply_swap_ids(
            current.member_ids,
            from_id=int(swap["from_id"]),
            to_id=int(swap["to_id"]),
        )
        member_names = tuple(
            str(resident_map.get(member_id, {}).get("full_name") or f"ID:{member_id}")
            for member_id in member_ids
        )
        updated[zone_code] = RuntimeZoneAssignment(
            code=current.code,
            title=current.title,
            member_ids=member_ids,
            member_names=member_names,
        )
    return updated


def apply_zone_overrides_to_assignments(
    assignments: dict[str, RuntimeZoneAssignment],
    resident_map: dict[int, dict],
    overrides_for_date: list[dict],
) -> dict[str, RuntimeZoneAssignment]:
    updated = dict(assignments)
    grouped: dict[str, list[tuple[int, int]]] = {}
    for override in overrides_for_date:
        zone_raw = str(override.get("zone_name") or "")
        zone_code = LEGACY_ZONE_CODE_BY_NAME.get(zone_raw, zone_raw)
        telegram_id = int(override["telegram_id"])
        slot_index = int(override.get("slot_index") or 0)
        grouped.setdefault(zone_code, []).append((slot_index, telegram_id))

    for zone_code, slots in grouped.items():
        current = updated.get(zone_code)
        if not current:
            continue
        ordered_ids = tuple(telegram_id for _, telegram_id in sorted(slots, key=lambda item: item[0]))
        if not ordered_ids:
            continue
        member_names = tuple(
            str(resident_map.get(member_id, {}).get("full_name") or f"ID:{member_id}")
            for member_id in ordered_ids
        )
        updated[zone_code] = RuntimeZoneAssignment(
            code=current.code,
            title=current.title,
            member_ids=ordered_ids,
            member_names=member_names,
        )
    return updated


def get_assigned_zone_codes_for_user(
    definition: InstanceDefinition,
    assignments: dict[str, RuntimeZoneAssignment],
    user_id: int,
    *,
    report_day_predicate,
) -> set[str]:
    matched: set[str] = set()
    for zone in definition.zones:
        if not zone.enabled:
            continue
        if zone.report_required and not report_day_predicate(zone):
            continue
        current = assignments.get(zone.code)
        if not current:
            continue
        if int(user_id) in {int(member_id) for member_id in current.member_ids}:
            matched.add(zone.code)
    return matched
