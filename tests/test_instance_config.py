from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from datetime import date

from instance_config import (
    build_legacy_instance_definition,
    instance_bundle_from_dict,
    instance_bundle_to_dict,
    instance_definition_from_dict,
    instance_definition_to_dict,
    is_zone_report_day,
    load_instance_definition,
    seed_runtime_config_if_empty,
)


class _FakeDb:
    def __init__(self) -> None:
        self.instance_settings: dict[str, str] = {}
        self.feature_flags: dict[str, bool] = {}
        self.zones: dict[str, dict] = {}
        self.zone_members: dict[str, list[dict]] = {}
        self.zone_rules: dict[str, dict] = {}

    async def has_dynamic_zones(self) -> bool:
        return bool(self.zones)

    async def set_instance_setting(self, key: str, value: str) -> None:
        self.instance_settings[str(key)] = str(value)

    async def list_instance_settings(self) -> dict[str, str]:
        return dict(self.instance_settings)

    async def replace_feature_flags(self, flags: dict[str, bool]) -> None:
        self.feature_flags = dict(flags)

    async def list_feature_flags(self) -> dict[str, bool]:
        return dict(self.feature_flags)

    async def upsert_zone(self, **kwargs) -> None:
        self.zones[str(kwargs["code"])] = dict(kwargs)

    async def replace_zone_members(self, zone_code: str, members: list[dict]) -> None:
        self.zone_members[str(zone_code)] = [dict(item) for item in members]

    async def replace_zone_rule(self, **kwargs) -> None:
        self.zone_rules[str(kwargs["zone_code"])] = dict(kwargs)

    async def list_zone_records(self) -> list[dict]:
        return [
            {
                "code": zone["code"],
                "title": zone["title"],
                "enabled": 1 if zone["enabled"] else 0,
                "sort_order": zone["sort_order"],
                "team_size_mode": zone["team_size_mode"],
                "report_required": 1 if zone["report_required"] else 0,
                "report_deadline_time": zone["report_deadline_time"],
                "private_reminder_time": zone["private_reminder_time"],
                "group_reminder_enabled": 1 if zone["group_reminder_enabled"] else 0,
                "private_reminder_enabled": 1 if zone["private_reminder_enabled"] else 0,
                "rotation_enabled": 1 if zone["rotation_enabled"] else 0,
            }
            for zone in sorted(self.zones.values(), key=lambda item: int(item["sort_order"]))
        ]

    async def list_all_zone_members(self) -> list[dict]:
        rows: list[dict] = []
        for zone_code in sorted(self.zone_members):
            for row in self.zone_members[zone_code]:
                rows.append({"zone_code": zone_code, **row})
        return rows

    async def list_all_zone_rules(self) -> list[dict]:
        return [dict(item) for _, item in sorted(self.zone_rules.items())]


class InstanceConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.residents_path = Path(self.tmpdir.name) / "residents.json"
        self.residents_path.write_text(
            json.dumps(
                [
                    {"telegram_id": 1, "full_name": "Альфа Один", "kitchen_order": 1, "general_pair_order": 1, "general_pair_slot": 1},
                    {"telegram_id": 2, "full_name": "Бета Два", "kitchen_order": 2, "general_pair_order": 1, "general_pair_slot": 2},
                    {"telegram_id": 3, "full_name": "Гамма Три", "bath_order": 1, "general_pair_order": 2, "general_pair_slot": 1},
                    {"telegram_id": 4, "full_name": "Дельта Чотири", "bath_order": 2, "general_pair_order": 2, "general_pair_slot": 2},
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        self.db = _FakeDb()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_legacy_definition_extracts_three_default_zones(self) -> None:
        definition = build_legacy_instance_definition(
            residents_path=self.residents_path,
            owner_id=100,
            group_id=-200,
        )

        self.assertEqual(definition.settings.owner_id, 100)
        self.assertEqual(definition.settings.group_id, -200)
        self.assertEqual([zone.code for zone in definition.zones[:3]], ["kitchen", "bath", "general"])
        self.assertEqual(definition.zones[0].rule.member_order, (1, 2))
        self.assertEqual(definition.zones[1].rule.member_order, (3, 4))
        self.assertEqual(definition.zones[2].rule.member_groups, ((1, 2), (3, 4)))
        self.assertIn("laundry", [zone.code for zone in definition.zones])
        self.assertIn("hall", [zone.code for zone in definition.zones])
        self.assertIn("trash", [zone.code for zone in definition.zones])
        self.assertIn("fridge", [zone.code for zone in definition.zones])

    def test_seed_runtime_config_populates_runtime_adapter(self) -> None:
        asyncio.run(
            seed_runtime_config_if_empty(
                self.db,
                residents_path=self.residents_path,
                owner_id=555,
                group_id=-777,
            )
        )

        loaded = asyncio.run(
            load_instance_definition(
                self.db,
                residents_path=self.residents_path,
                owner_id=555,
                group_id=-777,
            )
        )

        self.assertTrue(asyncio.run(self.db.has_dynamic_zones()))
        self.assertEqual(loaded.settings.owner_id, 555)
        self.assertEqual(loaded.settings.group_id, -777)
        self.assertEqual([zone.code for zone in loaded.zones[:3]], ["kitchen", "bath", "general"])
        self.assertEqual(loaded.zones[0].rule.member_order, (1, 2))
        self.assertEqual(loaded.zones[2].rule.member_groups, ((1, 2), (3, 4)))
        self.assertIn("laundry", [zone.code for zone in loaded.zones])
        self.assertIn("hall", [zone.code for zone in loaded.zones])

    def test_legacy_zones_keep_report_day_offsets(self) -> None:
        definition = build_legacy_instance_definition(
            residents_path=self.residents_path,
            owner_id=100,
            group_id=-200,
        )
        by_code = {zone.code: zone for zone in definition.zones}

        self.assertTrue(is_zone_report_day(by_code["kitchen"], date(2026, 1, 1)))
        self.assertTrue(is_zone_report_day(by_code["bath"], date(2026, 1, 4)))
        self.assertFalse(is_zone_report_day(by_code["bath"], date(2026, 1, 3)))
        self.assertTrue(is_zone_report_day(by_code["general"], date(2025, 12, 31)))
        self.assertFalse(is_zone_report_day(by_code["general"], date(2026, 1, 1)))

    def test_instance_definition_to_dict_contains_settings_flags_and_rules(self) -> None:
        definition = build_legacy_instance_definition(
            residents_path=self.residents_path,
            owner_id=100,
            group_id=-200,
        )

        exported = instance_definition_to_dict(definition)

        self.assertEqual(exported["settings"]["owner_id"], 100)
        self.assertIn("reports", exported["feature_flags"])
        kitchen = next(zone for zone in exported["zones"] if zone["code"] == "kitchen")
        self.assertEqual(kitchen["rule"]["rotation_every_days"], 1)
        self.assertEqual(kitchen["rule"]["member_order"], [1, 2])

    def test_instance_definition_from_dict_roundtrip(self) -> None:
        definition = build_legacy_instance_definition(
            residents_path=self.residents_path,
            owner_id=100,
            group_id=-200,
        )

        restored = instance_definition_from_dict(instance_definition_to_dict(definition))

        self.assertEqual(restored.settings.owner_id, 100)
        self.assertEqual(restored.settings.group_id, -200)
        self.assertEqual(restored.zones[0].code, "kitchen")
        self.assertEqual(restored.zones[0].rule.member_order, (1, 2))

    def test_instance_bundle_roundtrip_keeps_residents(self) -> None:
        definition = build_legacy_instance_definition(
            residents_path=self.residents_path,
            owner_id=100,
            group_id=-200,
        )
        residents = [
            {
                "telegram_id": 1,
                "full_name": "Альфа Один",
                "username": "alpha",
                "role": "resident",
                "is_active": True,
            },
            {
                "telegram_id": 2,
                "full_name": "Бета Два",
                "username": None,
                "role": "delegate",
                "is_active": False,
            },
        ]

        restored_definition, restored_residents = instance_bundle_from_dict(instance_bundle_to_dict(definition, residents))

        self.assertEqual(restored_definition.settings.owner_id, 100)
        self.assertEqual(len(restored_residents), 2)
        self.assertEqual(restored_residents[0]["telegram_id"], 1)
        self.assertEqual(restored_residents[1]["role"], "delegate")
