from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class RotationRule:
    rotation_mode: str
    rotation_every_days: int
    team_pattern: tuple[int, ...]
    anchor_date: date
    member_order: tuple[int, ...] = ()
    member_groups: tuple[tuple[int, ...], ...] = ()


def _days_between(anchor_date: date, target_date: date) -> int:
    return (target_date - anchor_date).days


def _cycle_index(rule: RotationRule, target_date: date) -> int:
    interval = max(1, int(rule.rotation_every_days))
    return _days_between(rule.anchor_date, target_date) // interval


def _pattern_value(pattern: tuple[int, ...], index: int) -> int:
    if not pattern:
        return 1
    return int(pattern[index % len(pattern)])


def _ordered_assignment(rule: RotationRule, target_date: date) -> tuple[int, ...]:
    members = tuple(int(member_id) for member_id in rule.member_order)
    if not members:
        return ()

    cycle_index = _cycle_index(rule, target_date)
    team_size = max(1, _pattern_value(rule.team_pattern, cycle_index))
    consumed = 0
    if cycle_index > 0:
        for step in range(cycle_index):
            consumed += max(1, _pattern_value(rule.team_pattern, step))
    start_offset = consumed % len(members)
    return tuple(members[(start_offset + idx) % len(members)] for idx in range(team_size))


def _grouped_assignment(rule: RotationRule, target_date: date) -> tuple[int, ...]:
    groups = tuple(tuple(int(member_id) for member_id in group) for group in rule.member_groups if group)
    if not groups:
        return ()
    cycle_index = _cycle_index(rule, target_date)
    return groups[cycle_index % len(groups)]


def compute_zone_rotation(rule: RotationRule, target_date: date) -> tuple[int, ...]:
    mode = str(rule.rotation_mode).strip().lower()
    if mode == "grouped":
        return _grouped_assignment(rule, target_date)
    return _ordered_assignment(rule, target_date)
