from __future__ import annotations

import json
import unittest

from permissions import PERM_REPORTS_REVIEW, can_access_admin_panel, delegate_permissions_key, get_user_permissions


class _FakeDb:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        return self.values.get(key, default)


class PermissionsTests(unittest.IsolatedAsyncioTestCase):
    async def test_delegate_permissions_are_loaded_from_settings(self) -> None:
        db = _FakeDb()
        db.values[delegate_permissions_key(777)] = json.dumps([PERM_REPORTS_REVIEW])

        permissions = await get_user_permissions(db, 777)

        self.assertEqual(permissions, {PERM_REPORTS_REVIEW})

    async def test_delegate_with_any_permission_can_open_admin_panel(self) -> None:
        db = _FakeDb()
        db.values[delegate_permissions_key(777)] = json.dumps([PERM_REPORTS_REVIEW])

        self.assertTrue(await can_access_admin_panel(db, 777))
