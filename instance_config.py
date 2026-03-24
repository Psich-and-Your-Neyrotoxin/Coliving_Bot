from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from config import ADMIN_ID, GROUP_ID, RESIDENTS_JSON_PATH
from database import Database
from rotation_engine import RotationRule


DEFAULT_TIMEZONE = "Europe/Kyiv"
DEFAULT_LANGUAGE = "uk"
DEFAULT_COLIVING_NAME = "Coliving"


@dataclass(frozen=True)
class InstanceSettings:
    coliving_name: str
    timezone: str
    owner_id: int
    group_id: int
    language: str
    setup_complete: bool


@dataclass(frozen=True)
class ZoneDefinition:
    code: str
    title: str
    enabled: bool
    sort_order: int
    team_size_mode: str
    report_required: bool
    report_deadline_time: str | None
    private_reminder_time: str | None
    group_reminder_enabled: bool
    private_reminder_enabled: bool
    rotation_enabled: bool
    rule: RotationRule
    extra_config: dict[str, object] | None = None


@dataclass(frozen=True)
class InstanceDefinition:
    settings: InstanceSettings
    feature_flags: dict[str, bool]
    zones: tuple[ZoneDefinition, ...]


def instance_definition_to_dict(definition: InstanceDefinition) -> dict[str, object]:
    return {
        "settings": {
            "coliving_name": definition.settings.coliving_name,
            "timezone": definition.settings.timezone,
            "owner_id": definition.settings.owner_id,
            "group_id": definition.settings.group_id,
            "language": definition.settings.language,
            "setup_complete": definition.settings.setup_complete,
        },
        "feature_flags": dict(definition.feature_flags),
        "zones": [
            {
                "code": zone.code,
                "title": zone.title,
                "enabled": zone.enabled,
                "sort_order": zone.sort_order,
                "team_size_mode": zone.team_size_mode,
                "report_required": zone.report_required,
                "report_deadline_time": zone.report_deadline_time,
                "private_reminder_time": zone.private_reminder_time,
                "group_reminder_enabled": zone.group_reminder_enabled,
                "private_reminder_enabled": zone.private_reminder_enabled,
                "rotation_enabled": zone.rotation_enabled,
                "rule": {
                    "rotation_mode": zone.rule.rotation_mode,
                    "rotation_every_days": zone.rule.rotation_every_days,
                    "team_pattern": list(zone.rule.team_pattern),
                    "anchor_date": zone.rule.anchor_date.isoformat(),
                    "member_order": list(zone.rule.member_order),
                    "member_groups": [list(group) for group in zone.rule.member_groups],
                },
                "extra_config": dict(zone.extra_config or {}),
            }
            for zone in definition.zones
        ],
    }


def instance_bundle_to_dict(definition: InstanceDefinition, residents: list[dict]) -> dict[str, object]:
    payload = instance_definition_to_dict(definition)
    payload["residents"] = [
        {
            "telegram_id": int(resident["telegram_id"]),
            "full_name": str(resident["full_name"]),
            "username": str(resident["username"]).lstrip("@") if resident.get("username") else None,
            "role": str(resident.get("role", "resident")),
            "is_active": bool(resident.get("is_active", True)),
        }
        for resident in residents
    ]
    return payload


def instance_definition_from_dict(payload: dict[str, object]) -> InstanceDefinition:
    settings_raw = payload.get("settings") if isinstance(payload, dict) else {}
    flags_raw = payload.get("feature_flags") if isinstance(payload, dict) else {}
    zones_raw = payload.get("zones") if isinstance(payload, dict) else []

    if not isinstance(settings_raw, dict):
        raise ValueError("settings must be an object")
    if not isinstance(flags_raw, dict):
        raise ValueError("feature_flags must be an object")
    if not isinstance(zones_raw, list):
        raise ValueError("zones must be a list")

    settings = InstanceSettings(
        coliving_name=str(settings_raw.get("coliving_name") or DEFAULT_COLIVING_NAME),
        timezone=str(settings_raw.get("timezone") or DEFAULT_TIMEZONE),
        owner_id=int(settings_raw.get("owner_id") or ADMIN_ID),
        group_id=int(settings_raw.get("group_id") or GROUP_ID),
        language=str(settings_raw.get("language") or DEFAULT_LANGUAGE),
        setup_complete=bool(settings_raw.get("setup_complete", False)),
    )
    feature_flags = {
        key: bool(flags_raw.get(key, default))
        for key, default in default_feature_flags().items()
    }

    zones: list[ZoneDefinition] = []
    seen_codes: set[str] = set()
    for index, zone_raw in enumerate(zones_raw):
        if not isinstance(zone_raw, dict):
            raise ValueError("Each zone must be an object")
        code = str(zone_raw.get("code") or "").strip()
        if not code:
            raise ValueError("Zone code is required")
        if code in seen_codes:
            raise ValueError(f"Duplicate zone code: {code}")
        seen_codes.add(code)

        rule_raw = zone_raw.get("rule") or {}
        if not isinstance(rule_raw, dict):
            raise ValueError("Zone rule must be an object")
        team_pattern_raw = rule_raw.get("team_pattern") or [1]
        if not isinstance(team_pattern_raw, list) or not team_pattern_raw:
            team_pattern_raw = [1]
        member_order_raw = rule_raw.get("member_order") or []
        member_groups_raw = rule_raw.get("member_groups") or []

        anchor_date_raw = str(rule_raw.get("anchor_date") or date(2026, 1, 1).isoformat())
        try:
            anchor_date = date.fromisoformat(anchor_date_raw)
        except ValueError as exc:
            raise ValueError(f"Invalid anchor_date for zone {code}") from exc

        zone = ZoneDefinition(
            code=code,
            title=str(zone_raw.get("title") or code),
            enabled=bool(zone_raw.get("enabled", True)),
            sort_order=int(zone_raw.get("sort_order", (index + 1) * 10)),
            team_size_mode=str(zone_raw.get("team_size_mode") or "fixed"),
            report_required=bool(zone_raw.get("report_required", True)),
            report_deadline_time=(
                str(zone_raw.get("report_deadline_time"))
                if zone_raw.get("report_deadline_time") is not None
                else None
            ),
            private_reminder_time=(
                str(zone_raw.get("private_reminder_time"))
                if zone_raw.get("private_reminder_time") is not None
                else None
            ),
            group_reminder_enabled=bool(zone_raw.get("group_reminder_enabled", False)),
            private_reminder_enabled=bool(zone_raw.get("private_reminder_enabled", True)),
            rotation_enabled=bool(zone_raw.get("rotation_enabled", True)),
            rule=RotationRule(
                rotation_mode=str(rule_raw.get("rotation_mode") or "ordered"),
                rotation_every_days=max(1, int(rule_raw.get("rotation_every_days") or 1)),
                team_pattern=tuple(max(1, int(item)) for item in team_pattern_raw),
                anchor_date=anchor_date,
                member_order=tuple(int(item) for item in member_order_raw),
                member_groups=tuple(
                    tuple(int(member_id) for member_id in group)
                    for group in member_groups_raw
                    if isinstance(group, list)
                ),
            ),
            extra_config=dict(zone_raw.get("extra_config") or {}) if isinstance(zone_raw.get("extra_config"), dict) else {},
        )
        zones.append(zone)

    return InstanceDefinition(
        settings=settings,
        feature_flags=feature_flags,
        zones=tuple(sorted(zones, key=lambda item: (item.sort_order, item.code))),
    )


def instance_bundle_from_dict(payload: dict[str, object]) -> tuple[InstanceDefinition, list[dict]]:
    definition = instance_definition_from_dict(payload)
    residents_raw = payload.get("residents") if isinstance(payload, dict) else []
    if residents_raw is None:
        residents_raw = []
    if not isinstance(residents_raw, list):
        raise ValueError("residents must be a list")
    residents: list[dict] = []
    seen_ids: set[int] = set()
    for index, resident_raw in enumerate(residents_raw, start=1):
        if not isinstance(resident_raw, dict):
            raise ValueError(f"resident #{index} must be an object")
        try:
            telegram_id = int(resident_raw.get("telegram_id"))
        except Exception as exc:
            raise ValueError(f"resident #{index} must contain numeric telegram_id") from exc
        if telegram_id in seen_ids:
            raise ValueError(f"duplicate resident telegram_id: {telegram_id}")
        seen_ids.add(telegram_id)
        full_name = str(resident_raw.get("full_name") or "").strip()
        if not full_name:
            raise ValueError(f"resident #{index} must contain full_name")
        residents.append(
            {
                "telegram_id": telegram_id,
                "full_name": full_name,
                "username": str(resident_raw.get("username")).lstrip("@") if resident_raw.get("username") else None,
                "role": str(resident_raw.get("role") or "resident"),
                "is_active": bool(resident_raw.get("is_active", True)),
            }
        )
    return definition, residents


def default_zone_templates() -> tuple[ZoneDefinition, ...]:
    return (
        ZoneDefinition(
            code="laundry",
            title="Пральня",
            enabled=False,
            sort_order=40,
            team_size_mode="fixed",
            report_required=True,
            report_deadline_time="20:00",
            private_reminder_time="18:00",
            group_reminder_enabled=False,
            private_reminder_enabled=True,
            rotation_enabled=True,
            rule=RotationRule(
                rotation_mode="ordered",
                rotation_every_days=10,
                team_pattern=(1,),
                anchor_date=date(2026, 1, 1),
            ),
            extra_config={"report_offset_days": 9, "template_only": True},
        ),
        ZoneDefinition(
            code="hall",
            title="Коридор",
            enabled=False,
            sort_order=50,
            team_size_mode="fixed",
            report_required=True,
            report_deadline_time="22:00",
            private_reminder_time="20:00",
            group_reminder_enabled=False,
            private_reminder_enabled=True,
            rotation_enabled=True,
            rule=RotationRule(
                rotation_mode="ordered",
                rotation_every_days=7,
                team_pattern=(1,),
                anchor_date=date(2026, 1, 5),
            ),
            extra_config={"report_offset_days": 6, "template_only": True},
        ),
        ZoneDefinition(
            code="trash",
            title="Сміття",
            enabled=False,
            sort_order=60,
            team_size_mode="fixed",
            report_required=False,
            report_deadline_time=None,
            private_reminder_time="19:00",
            group_reminder_enabled=False,
            private_reminder_enabled=True,
            rotation_enabled=True,
            rule=RotationRule(
                rotation_mode="ordered",
                rotation_every_days=3,
                team_pattern=(1,),
                anchor_date=date(2026, 1, 1),
            ),
            extra_config={"report_offset_days": 0, "template_only": True},
        ),
        ZoneDefinition(
            code="fridge",
            title="Холодильник",
            enabled=False,
            sort_order=70,
            team_size_mode="pattern",
            report_required=True,
            report_deadline_time="21:00",
            private_reminder_time="18:30",
            group_reminder_enabled=False,
            private_reminder_enabled=True,
            rotation_enabled=True,
            rule=RotationRule(
                rotation_mode="ordered",
                rotation_every_days=7,
                team_pattern=(2, 3, 2),
                anchor_date=date(2026, 1, 5),
            ),
            extra_config={"report_offset_days": 6, "template_only": True},
        ),
    )


def default_feature_flags() -> dict[str, bool]:
    return {
        "reports": True,
        "fines": True,
        "payments": True,
        "swaps": True,
        "deadlines": True,
        "reminders": True,
        "delegates": True,
        "calendar_exceptions": True,
    }


def _load_residents_json(path: Path | str) -> list[dict]:
    residents = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(residents, list):
        raise ValueError("residents.json must be a JSON list")
    return residents


def _sorted_zone_members(residents: list[dict], order_key: str) -> tuple[int, ...]:
    ordered: list[tuple[int, int]] = []
    for resident in residents:
        if resident.get(order_key) is None:
            continue
        ordered.append((int(resident[order_key]), int(resident["telegram_id"])))
    return tuple(member_id for _, member_id in sorted(ordered))


def _sorted_general_groups(residents: list[dict]) -> tuple[tuple[int, int], ...]:
    grouped: dict[int, dict[int, int]] = {}
    for resident in residents:
        pair_order = resident.get("general_pair_order")
        pair_slot = resident.get("general_pair_slot")
        if pair_order is None and pair_slot is None:
            continue
        if pair_order is None or pair_slot is None:
            raise ValueError("General pairs in residents.json require both general_pair_order and general_pair_slot")
        grouped.setdefault(int(pair_order), {})[int(pair_slot)] = int(resident["telegram_id"])

    pairs: list[tuple[int, int]] = []
    for _, slots in sorted(grouped.items()):
        if set(slots) != {1, 2}:
            raise ValueError("Each general pair must contain two members with slots 1 and 2")
        pairs.append((int(slots[1]), int(slots[2])))
    return tuple(pairs)


def build_legacy_instance_definition(
    *,
    residents_path: Path | str = RESIDENTS_JSON_PATH,
    owner_id: int = ADMIN_ID,
    group_id: int = GROUP_ID,
    timezone: str = DEFAULT_TIMEZONE,
    coliving_name: str = DEFAULT_COLIVING_NAME,
    language: str = DEFAULT_LANGUAGE,
) -> InstanceDefinition:
    residents = _load_residents_json(residents_path)
    kitchen_ids = _sorted_zone_members(residents, "kitchen_order")
    bath_ids = _sorted_zone_members(residents, "bath_order")
    general_groups = _sorted_general_groups(residents)

    zones = (
        ZoneDefinition(
            code="kitchen",
            title="Кухня",
            enabled=True,
            sort_order=10,
            team_size_mode="fixed",
            report_required=True,
            report_deadline_time="01:00",
            private_reminder_time="23:00",
            group_reminder_enabled=False,
            private_reminder_enabled=True,
            rotation_enabled=True,
            rule=RotationRule(
                rotation_mode="ordered",
                rotation_every_days=1,
                team_pattern=(1,),
                anchor_date=date(2026, 1, 1),
                member_order=kitchen_ids,
            ),
            extra_config={"report_offset_days": 0},
        ),
        ZoneDefinition(
            code="bath",
            title="Ванна",
            enabled=True,
            sort_order=20,
            team_size_mode="fixed",
            report_required=True,
            report_deadline_time="01:00",
            private_reminder_time="23:00",
            group_reminder_enabled=False,
            private_reminder_enabled=True,
            rotation_enabled=True,
            rule=RotationRule(
                rotation_mode="ordered",
                rotation_every_days=7,
                team_pattern=(1,),
                anchor_date=date(2025, 12, 29),
                member_order=bath_ids,
            ),
            extra_config={"report_offset_days": 6},
        ),
        ZoneDefinition(
            code="general",
            title="Общак",
            enabled=True,
            sort_order=30,
            team_size_mode="fixed",
            report_required=True,
            report_deadline_time="01:00",
            private_reminder_time="23:00",
            group_reminder_enabled=False,
            private_reminder_enabled=True,
            rotation_enabled=True,
            rule=RotationRule(
                rotation_mode="grouped",
                rotation_every_days=7,
                team_pattern=(2,),
                anchor_date=date(2025, 12, 29),
                member_groups=general_groups,
            ),
            extra_config={"report_offset_days": 2},
        ),
    ) + default_zone_templates()

    settings = InstanceSettings(
        coliving_name=str(coliving_name or DEFAULT_COLIVING_NAME),
        timezone=str(timezone or DEFAULT_TIMEZONE),
        owner_id=int(owner_id),
        group_id=int(group_id),
        language=str(language or DEFAULT_LANGUAGE),
        setup_complete=False,
    )
    return InstanceDefinition(settings=settings, feature_flags=default_feature_flags(), zones=zones)


async def store_instance_definition(db: Database, definition: InstanceDefinition) -> None:
    await db.set_instance_setting("coliving_name", definition.settings.coliving_name)
    await db.set_instance_setting("timezone", definition.settings.timezone)
    await db.set_instance_setting("owner_id", str(definition.settings.owner_id))
    await db.set_instance_setting("group_id", str(definition.settings.group_id))
    await db.set_instance_setting("language", definition.settings.language)
    await db.set_instance_setting("setup_complete", "1" if definition.settings.setup_complete else "0")
    await db.replace_feature_flags(definition.feature_flags)
    for zone in definition.zones:
        await db.upsert_zone(
            code=zone.code,
            title=zone.title,
            enabled=zone.enabled,
            sort_order=zone.sort_order,
            team_size_mode=zone.team_size_mode,
            report_required=zone.report_required,
            report_deadline_time=zone.report_deadline_time,
            private_reminder_time=zone.private_reminder_time,
            group_reminder_enabled=zone.group_reminder_enabled,
            private_reminder_enabled=zone.private_reminder_enabled,
            rotation_enabled=zone.rotation_enabled,
        )
        members: list[dict] = []
        if zone.rule.member_order:
            members = [
                {
                    "telegram_id": int(member_id),
                    "sort_order": index,
                    "group_index": None,
                    "slot_index": None,
                    "is_active": True,
                }
                for index, member_id in enumerate(zone.rule.member_order)
            ]
        elif zone.rule.member_groups:
            for group_index, group in enumerate(zone.rule.member_groups):
                for slot_index, member_id in enumerate(group):
                    members.append(
                        {
                            "telegram_id": int(member_id),
                            "sort_order": group_index * 10 + slot_index,
                            "group_index": group_index,
                            "slot_index": slot_index,
                            "is_active": True,
                        }
                    )
        await db.replace_zone_members(zone.code, members)
        await db.replace_zone_rule(
            zone_code=zone.code,
            rotation_mode=zone.rule.rotation_mode,
            rotation_every_days=zone.rule.rotation_every_days,
            team_pattern_json=json.dumps(list(zone.rule.team_pattern)),
            anchor_date=zone.rule.anchor_date.isoformat(),
            config_json=json.dumps(
                {
                    "member_order": list(zone.rule.member_order),
                    "member_groups": [list(group) for group in zone.rule.member_groups],
                    "extra_config": dict(zone.extra_config or {}),
                },
                ensure_ascii=False,
            ),
        )


def _parse_team_pattern(raw: str) -> tuple[int, ...]:
    try:
        parsed = json.loads(raw or "[1]")
    except Exception:
        parsed = [1]
    if not isinstance(parsed, list) or not parsed:
        return (1,)
    return tuple(max(1, int(item)) for item in parsed)


def _parse_rule(row: dict) -> RotationRule:
    try:
        config = json.loads(str(row.get("config_json") or "{}"))
    except Exception:
        config = {}
    member_order = tuple(int(item) for item in config.get("member_order", []) or [])
    member_groups = tuple(tuple(int(member_id) for member_id in group) for group in config.get("member_groups", []) or [])
    return RotationRule(
        rotation_mode=str(row.get("rotation_mode") or "ordered"),
        rotation_every_days=max(1, int(row.get("rotation_every_days") or 1)),
        team_pattern=_parse_team_pattern(str(row.get("team_pattern_json") or "[1]")),
        anchor_date=date.fromisoformat(str(row.get("anchor_date"))),
        member_order=member_order,
        member_groups=member_groups,
    )


def _parse_rule_config(row: dict) -> dict[str, object]:
    try:
        parsed = json.loads(str(row.get("config_json") or "{}"))
    except Exception:
        parsed = {}
    if not isinstance(parsed, dict):
        return {}
    extra = parsed.get("extra_config")
    if isinstance(extra, dict):
        return dict(extra)
    return {}


async def load_instance_definition(
    db: Database,
    *,
    residents_path: Path | str = RESIDENTS_JSON_PATH,
    owner_id: int = ADMIN_ID,
    group_id: int = GROUP_ID,
) -> InstanceDefinition:
    if not await db.has_dynamic_zones():
        return build_legacy_instance_definition(
            residents_path=residents_path,
            owner_id=owner_id,
            group_id=group_id,
        )

    raw_settings = await db.list_instance_settings()
    raw_flags = await db.list_feature_flags()
    raw_zones = await db.list_zone_records()
    raw_members = await db.list_all_zone_members()
    raw_rules = {str(item["zone_code"]): item for item in await db.list_all_zone_rules()}
    members_by_zone: dict[str, list[dict]] = {}
    for member in raw_members:
        members_by_zone.setdefault(str(member["zone_code"]), []).append(member)

    zones: list[ZoneDefinition] = []
    for zone_row in raw_zones:
        code = str(zone_row["code"])
        rule = _parse_rule(raw_rules[code]) if code in raw_rules else RotationRule(
            rotation_mode="ordered",
            rotation_every_days=1,
            team_pattern=(1,),
            anchor_date=date(2026, 1, 1),
        )
        zone_members = members_by_zone.get(code, [])
        if zone_members and not rule.member_order and not rule.member_groups:
            if any(member.get("group_index") is not None for member in zone_members):
                grouped: dict[int, list[tuple[int, int]]] = {}
                for member in zone_members:
                    group_index = int(member.get("group_index") or 0)
                    grouped.setdefault(group_index, []).append(
                        (int(member.get("slot_index") or 0), int(member["telegram_id"]))
                    )
                rule = RotationRule(
                    rotation_mode=rule.rotation_mode,
                    rotation_every_days=rule.rotation_every_days,
                    team_pattern=rule.team_pattern,
                    anchor_date=rule.anchor_date,
                    member_groups=tuple(
                        tuple(member_id for _, member_id in sorted(group))
                        for _, group in sorted(grouped.items())
                    ),
                )
            else:
                rule = RotationRule(
                    rotation_mode=rule.rotation_mode,
                    rotation_every_days=rule.rotation_every_days,
                    team_pattern=rule.team_pattern,
                    anchor_date=rule.anchor_date,
                    member_order=tuple(int(member["telegram_id"]) for member in sorted(zone_members, key=lambda item: int(item["sort_order"]))),
                )

        zones.append(
            ZoneDefinition(
                code=code,
                title=str(zone_row["title"]),
                enabled=bool(int(zone_row["enabled"])),
                sort_order=int(zone_row["sort_order"]),
                team_size_mode=str(zone_row["team_size_mode"]),
                report_required=bool(int(zone_row["report_required"])),
                report_deadline_time=str(zone_row["report_deadline_time"]) if zone_row.get("report_deadline_time") else None,
                private_reminder_time=str(zone_row["private_reminder_time"]) if zone_row.get("private_reminder_time") else None,
                group_reminder_enabled=bool(int(zone_row["group_reminder_enabled"])),
                private_reminder_enabled=bool(int(zone_row["private_reminder_enabled"])),
                rotation_enabled=bool(int(zone_row["rotation_enabled"])),
                rule=rule,
                extra_config=_parse_rule_config(raw_rules[code]) if code in raw_rules else {},
            )
        )

    settings = InstanceSettings(
        coliving_name=str(raw_settings.get("coliving_name", DEFAULT_COLIVING_NAME)),
        timezone=str(raw_settings.get("timezone", DEFAULT_TIMEZONE)),
        owner_id=int(raw_settings.get("owner_id", owner_id or 0)),
        group_id=int(raw_settings.get("group_id", group_id or 0)),
        language=str(raw_settings.get("language", DEFAULT_LANGUAGE)),
        setup_complete=raw_settings.get("setup_complete", "0") == "1",
    )
    return InstanceDefinition(
        settings=settings,
        feature_flags=raw_flags or default_feature_flags(),
        zones=tuple(zones),
    )


async def seed_runtime_config_if_empty(
    db: Database,
    *,
    residents_path: Path | str = RESIDENTS_JSON_PATH,
    owner_id: int = ADMIN_ID,
    group_id: int = GROUP_ID,
) -> None:
    if await db.has_dynamic_zones():
        return
    definition = build_legacy_instance_definition(
        residents_path=residents_path,
        owner_id=owner_id,
        group_id=group_id,
    )
    await store_instance_definition(db, definition)


LEGACY_ZONE_NAME_BY_CODE = {
    "kitchen": "Kitchen",
    "bath": "Bath",
    "general": "General",
}
LEGACY_ZONE_CODE_BY_NAME = {value: key for key, value in LEGACY_ZONE_NAME_BY_CODE.items()}


def get_legacy_zone_from_definition(definition: InstanceDefinition, legacy_zone_name: str) -> ZoneDefinition | None:
    zone_code = LEGACY_ZONE_CODE_BY_NAME.get(str(legacy_zone_name))
    if not zone_code:
        return None
    for zone in definition.zones:
        if zone.code == zone_code:
            return zone
    return None


def zone_report_offset_days(zone: ZoneDefinition) -> int:
    try:
        return int((zone.extra_config or {}).get("report_offset_days", 0))
    except Exception:
        return 0


def is_zone_report_day(zone: ZoneDefinition, target_date: date) -> bool:
    interval = max(1, int(zone.rule.rotation_every_days))
    offset = zone_report_offset_days(zone) % interval
    delta_days = (target_date - zone.rule.anchor_date).days
    return delta_days % interval == offset
