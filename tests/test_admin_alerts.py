import json
import unittest

from admin_alerts import (
    REPORT_REVIEW_ROUTE_KEY,
    REPORT_ROUTE_OWNER_AND_REVIEWERS,
    REPORT_ROUTE_REVIEWERS_ONLY,
    get_report_review_recipient_ids,
    get_report_review_watcher_ids,
)
from permissions import PERM_REPORTS_REVIEW, delegate_permissions_key


class _FakeDb:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        return self.values.get(key, default)

    async def list_settings_by_prefix(self, prefix: str) -> list[dict]:
        return [{"key": key, "value": value} for key, value in self.values.items() if key.startswith(prefix)]


class AdminAlertsTests(unittest.IsolatedAsyncioTestCase):
    async def test_report_recipients_include_owner_and_reviewers(self) -> None:
        db = _FakeDb()
        db.values[REPORT_REVIEW_ROUTE_KEY] = REPORT_ROUTE_OWNER_AND_REVIEWERS
        db.values[delegate_permissions_key(777)] = json.dumps([PERM_REPORTS_REVIEW])

        recipients = await get_report_review_recipient_ids(db)

        self.assertIn(777, recipients)
        self.assertTrue(recipients)
        self.assertEqual(len(recipients), 2)

    async def test_reviewers_only_falls_back_to_owner(self) -> None:
        db = _FakeDb()
        db.values[REPORT_REVIEW_ROUTE_KEY] = REPORT_ROUTE_REVIEWERS_ONLY

        recipients = await get_report_review_recipient_ids(db)

        self.assertEqual(len(recipients), 1)

    async def test_watchers_exclude_actor(self) -> None:
        db = _FakeDb()
        db.values[delegate_permissions_key(777)] = json.dumps([PERM_REPORTS_REVIEW])
        db.values[delegate_permissions_key(888)] = json.dumps([PERM_REPORTS_REVIEW])

        watchers = await get_report_review_watcher_ids(db, 777)

        self.assertIn(888, watchers)
        self.assertNotIn(777, watchers)

