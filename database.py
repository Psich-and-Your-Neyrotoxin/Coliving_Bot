from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import aiosqlite
from config import RESIDENTS_JSON_PATH as CONFIG_RESIDENTS_JSON_PATH


DB_PATH = Path(os.getenv("DB_PATH", str(Path(__file__).with_name("coliving.db"))))
RESIDENTS_JSON_PATH = Path(CONFIG_RESIDENTS_JSON_PATH)
REPORT_RETENTION_DAYS = 7


def now_iso() -> str:
    kyiv_tz = ZoneInfo("Europe/Kyiv")
    return datetime.now(kyiv_tz).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Database:
    db_path: Path | str = DB_PATH

    async def init_schema(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS residents (
                    telegram_id   INTEGER PRIMARY KEY,
                    full_name     TEXT NOT NULL,
                    username      TEXT,
                    role          TEXT NOT NULL CHECK(role IN ('admin','resident')),
                    is_active     INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS duty_logs (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id   INTEGER NOT NULL,
                    zone_name     TEXT NOT NULL,
                    duty_date     TEXT NOT NULL DEFAULT '',
                    photo_id      TEXT NOT NULL,
                    status        TEXT NOT NULL CHECK(status IN ('pending','approved','rejected')) DEFAULT 'pending',
                    admin_comment TEXT,
                    reviewed_at   TEXT,
                    user_reminded_at TEXT,
                    admin_reminded_at TEXT,
                    created_at    TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id)
                );

                CREATE TABLE IF NOT EXISTS swaps (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    zone      TEXT NOT NULL,
                    from_id   INTEGER NOT NULL,
                    to_id     INTEGER NOT NULL,
                    date      TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (from_id) REFERENCES residents(telegram_id),
                    FOREIGN KEY (to_id) REFERENCES residents(telegram_id),
                    UNIQUE(zone, from_id, date)
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS instance_settings (
                    key        TEXT PRIMARY KEY,
                    value      TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS feature_flags (
                    key        TEXT PRIMARY KEY,
                    enabled    INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS zones (
                    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                    code                     TEXT NOT NULL UNIQUE,
                    title                    TEXT NOT NULL,
                    enabled                  INTEGER NOT NULL DEFAULT 1,
                    sort_order               INTEGER NOT NULL DEFAULT 0,
                    team_size_mode           TEXT NOT NULL DEFAULT 'fixed',
                    report_required          INTEGER NOT NULL DEFAULT 1,
                    report_deadline_time     TEXT,
                    private_reminder_time    TEXT,
                    group_reminder_enabled   INTEGER NOT NULL DEFAULT 0,
                    private_reminder_enabled INTEGER NOT NULL DEFAULT 1,
                    rotation_enabled         INTEGER NOT NULL DEFAULT 1,
                    created_at               TEXT NOT NULL,
                    updated_at               TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS zone_members (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    zone_code   TEXT NOT NULL,
                    telegram_id INTEGER NOT NULL,
                    sort_order  INTEGER NOT NULL DEFAULT 0,
                    group_index INTEGER,
                    slot_index  INTEGER,
                    is_active   INTEGER NOT NULL DEFAULT 1,
                    created_at  TEXT NOT NULL,
                    FOREIGN KEY (zone_code) REFERENCES zones(code) ON DELETE CASCADE,
                    FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                    UNIQUE(zone_code, telegram_id)
                );

                CREATE TABLE IF NOT EXISTS zone_rules (
                    zone_code            TEXT PRIMARY KEY,
                    rotation_mode        TEXT NOT NULL DEFAULT 'ordered',
                    rotation_every_days  INTEGER NOT NULL DEFAULT 1,
                    team_pattern_json    TEXT NOT NULL DEFAULT '[1]',
                    anchor_date          TEXT NOT NULL,
                    config_json          TEXT NOT NULL DEFAULT '{}',
                    created_at           TEXT NOT NULL,
                    updated_at           TEXT NOT NULL,
                    FOREIGN KEY (zone_code) REFERENCES zones(code) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS fines (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id        INTEGER NOT NULL,
                    fine_date      TEXT NOT NULL DEFAULT '',
                    fine_type      TEXT NOT NULL DEFAULT '',
                    reason         TEXT NOT NULL,
                    amount         INTEGER NOT NULL,
                    requires_proof INTEGER NOT NULL DEFAULT 1,
                    status         TEXT NOT NULL CHECK(status IN ('pending','paid','cancelled')) DEFAULT 'pending',
                    photo_proof_id TEXT,
                    issued_by      INTEGER,
                    created_at     TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES residents(telegram_id),
                    FOREIGN KEY (issued_by) REFERENCES residents(telegram_id)
                );

                CREATE TABLE IF NOT EXISTS test_mode_whitelist (
                    telegram_id INTEGER PRIMARY KEY,
                    created_at  TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id)
                );

                CREATE TABLE IF NOT EXISTS test_duty_overrides (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    date        TEXT NOT NULL,
                    zone_name   TEXT NOT NULL,
                    slot_index  INTEGER NOT NULL,
                    telegram_id INTEGER NOT NULL,
                    created_at  TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                    UNIQUE(date, zone_name, slot_index)
                );

                CREATE TABLE IF NOT EXISTS manual_duty_overrides (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    date        TEXT NOT NULL,
                    zone_name   TEXT NOT NULL,
                    slot_index  INTEGER NOT NULL,
                    telegram_id INTEGER NOT NULL,
                    created_at  TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                    UNIQUE(date, zone_name, slot_index)
                );

                CREATE TABLE IF NOT EXISTS swap_attempts (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_id       INTEGER NOT NULL,
                    to_id         INTEGER,
                    zone          TEXT NOT NULL,
                    target_date   TEXT NOT NULL,
                    status        TEXT NOT NULL,
                    details       TEXT,
                    created_at    TEXT NOT NULL,
                    FOREIGN KEY (from_id) REFERENCES residents(telegram_id),
                    FOREIGN KEY (to_id) REFERENCES residents(telegram_id)
                );

                CREATE TABLE IF NOT EXISTS user_contacts (
                    telegram_id         INTEGER PRIMARY KEY,
                    has_started         INTEGER NOT NULL DEFAULT 1,
                    can_message         INTEGER NOT NULL DEFAULT 0,
                    last_interaction_at TEXT NOT NULL,
                    last_delivery_at    TEXT,
                    last_delivery_error TEXT,
                    FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id)
                );

                CREATE TABLE IF NOT EXISTS admin_action_logs (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id    INTEGER NOT NULL,
                    action_type TEXT NOT NULL,
                    target_id   INTEGER,
                    details     TEXT,
                    created_at  TEXT NOT NULL,
                    FOREIGN KEY (admin_id) REFERENCES residents(telegram_id),
                    FOREIGN KEY (target_id) REFERENCES residents(telegram_id)
                );

                CREATE TABLE IF NOT EXISTS deadline_alerts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    zone_name   TEXT NOT NULL,
                    duty_date   TEXT NOT NULL,
                    created_at  TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                    UNIQUE(telegram_id, zone_name, duty_date)
                );

                CREATE TABLE IF NOT EXISTS deadline_user_reminders (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    zone_name   TEXT NOT NULL,
                    duty_date   TEXT NOT NULL,
                    stage       TEXT NOT NULL,
                    created_at  TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                    UNIQUE(telegram_id, zone_name, duty_date, stage)
                );
                """
            )
            columns = await (await conn.execute("PRAGMA table_info(residents);")).fetchall()
            column_names = {str(row["name"]) for row in columns}
            if "username" not in column_names:
                await conn.execute("ALTER TABLE residents ADD COLUMN username TEXT;")
            duty_log_columns = await (await conn.execute("PRAGMA table_info(duty_logs);")).fetchall()
            duty_log_column_names = {str(row["name"]) for row in duty_log_columns}
            if "duty_date" not in duty_log_column_names:
                await conn.execute("ALTER TABLE duty_logs ADD COLUMN duty_date TEXT NOT NULL DEFAULT '';")
            if "reviewed_at" not in duty_log_column_names:
                await conn.execute("ALTER TABLE duty_logs ADD COLUMN reviewed_at TEXT;")
            if "user_reminded_at" not in duty_log_column_names:
                await conn.execute("ALTER TABLE duty_logs ADD COLUMN user_reminded_at TEXT;")
            if "admin_reminded_at" not in duty_log_column_names:
                await conn.execute("ALTER TABLE duty_logs ADD COLUMN admin_reminded_at TEXT;")
            fine_columns = await (await conn.execute("PRAGMA table_info(fines);")).fetchall()
            fine_column_names = {str(row["name"]) for row in fine_columns}
            if "fine_date" not in fine_column_names:
                await conn.execute("ALTER TABLE fines ADD COLUMN fine_date TEXT NOT NULL DEFAULT '';")
            if "fine_type" not in fine_column_names:
                await conn.execute("ALTER TABLE fines ADD COLUMN fine_type TEXT NOT NULL DEFAULT '';")
            if "issued_by" not in fine_column_names:
                await conn.execute("ALTER TABLE fines ADD COLUMN issued_by INTEGER;")
            if "requires_proof" not in fine_column_names:
                await conn.execute("ALTER TABLE fines ADD COLUMN requires_proof INTEGER NOT NULL DEFAULT 1;")
            await self._migrate_legacy_zone_check_tables(conn)
            await conn.commit()

    async def _migrate_legacy_zone_check_tables(self, conn) -> None:
        await conn.execute("PRAGMA foreign_keys = OFF;")

        async def _table_sql(name: str) -> str:
            row = await (
                await conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?;",
                    (name,),
                )
            ).fetchone()
            return str(row["sql"] or "") if row else ""

        async def _rebuild(table_name: str, create_sql: str, columns: list[str]) -> None:
            sql = await _table_sql(table_name)
            if "CHECK(zone_name IN ('Kitchen','Bath','General'))" not in sql and "CHECK(zone IN ('Kitchen','Bath','General'))" not in sql:
                return
            temp_name = f"{table_name}__legacy_zone_backup"
            cols = ", ".join(columns)
            await conn.execute(f"ALTER TABLE {table_name} RENAME TO {temp_name};")
            await conn.execute(create_sql)
            await conn.execute(f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM {temp_name};")
            await conn.execute(f"DROP TABLE {temp_name};")

        await _rebuild(
            "duty_logs",
            """
            CREATE TABLE duty_logs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id   INTEGER NOT NULL,
                zone_name     TEXT NOT NULL,
                duty_date     TEXT NOT NULL DEFAULT '',
                photo_id      TEXT NOT NULL,
                status        TEXT NOT NULL CHECK(status IN ('pending','approved','rejected')) DEFAULT 'pending',
                admin_comment TEXT,
                reviewed_at   TEXT,
                user_reminded_at TEXT,
                admin_reminded_at TEXT,
                created_at    TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id)
            );
            """,
            [
                "id",
                "telegram_id",
                "zone_name",
                "duty_date",
                "photo_id",
                "status",
                "admin_comment",
                "reviewed_at",
                "user_reminded_at",
                "admin_reminded_at",
                "created_at",
            ],
        )
        await _rebuild(
            "swaps",
            """
            CREATE TABLE swaps (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                zone      TEXT NOT NULL,
                from_id   INTEGER NOT NULL,
                to_id     INTEGER NOT NULL,
                date      TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (from_id) REFERENCES residents(telegram_id),
                FOREIGN KEY (to_id) REFERENCES residents(telegram_id),
                UNIQUE(zone, from_id, date)
            );
            """,
            ["id", "zone", "from_id", "to_id", "date", "created_at"],
        )
        await _rebuild(
            "test_duty_overrides",
            """
            CREATE TABLE test_duty_overrides (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT NOT NULL,
                zone_name   TEXT NOT NULL,
                slot_index  INTEGER NOT NULL,
                telegram_id INTEGER NOT NULL,
                created_at  TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                UNIQUE(date, zone_name, slot_index)
            );
            """,
            ["id", "date", "zone_name", "slot_index", "telegram_id", "created_at"],
        )
        await _rebuild(
            "manual_duty_overrides",
            """
            CREATE TABLE manual_duty_overrides (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT NOT NULL,
                zone_name   TEXT NOT NULL,
                slot_index  INTEGER NOT NULL,
                telegram_id INTEGER NOT NULL,
                created_at  TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                UNIQUE(date, zone_name, slot_index)
            );
            """,
            ["id", "date", "zone_name", "slot_index", "telegram_id", "created_at"],
        )
        await _rebuild(
            "deadline_alerts",
            """
            CREATE TABLE deadline_alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                zone_name   TEXT NOT NULL,
                duty_date   TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                UNIQUE(telegram_id, zone_name, duty_date)
            );
            """,
            ["id", "telegram_id", "zone_name", "duty_date", "created_at"],
        )
        await _rebuild(
            "deadline_user_reminders",
            """
            CREATE TABLE deadline_user_reminders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                zone_name   TEXT NOT NULL,
                duty_date   TEXT NOT NULL,
                stage       TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES residents(telegram_id),
                UNIQUE(telegram_id, zone_name, duty_date, stage)
            );
            """,
            ["id", "telegram_id", "zone_name", "duty_date", "stage", "created_at"],
        )
        await conn.execute("PRAGMA foreign_keys = ON;")

    async def get_instance_setting(self, key: str, default: str | None = None) -> str | None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (await conn.execute("SELECT value FROM instance_settings WHERE key = ?;", (str(key),))).fetchone()
        return default if row is None else str(row["value"])

    async def set_instance_setting(self, key: str, value: str) -> None:
        timestamp = now_iso()
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                """
                INSERT INTO instance_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at;
                """,
                (str(key), str(value), timestamp),
            )
            await conn.commit()

    async def list_instance_settings(self) -> dict[str, str]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (await conn.execute("SELECT key, value FROM instance_settings ORDER BY key ASC;")).fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    async def replace_feature_flags(self, flags: dict[str, bool]) -> None:
        timestamp = now_iso()
        payload = [(str(key), 1 if bool(value) else 0, timestamp) for key, value in sorted(flags.items())]
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("DELETE FROM feature_flags;")
            if payload:
                await conn.executemany(
                    """
                    INSERT INTO feature_flags (key, enabled, updated_at)
                    VALUES (?, ?, ?);
                    """,
                    payload,
                )
            await conn.commit()

    async def list_feature_flags(self) -> dict[str, bool]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (await conn.execute("SELECT key, enabled FROM feature_flags ORDER BY key ASC;")).fetchall()
        return {str(row["key"]): bool(int(row["enabled"])) for row in rows}

    async def has_dynamic_zones(self) -> bool:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (await conn.execute("SELECT 1 AS ok FROM zones LIMIT 1;")).fetchone()
        return bool(row)

    async def upsert_zone(
        self,
        *,
        code: str,
        title: str,
        enabled: bool,
        sort_order: int,
        team_size_mode: str,
        report_required: bool,
        report_deadline_time: str | None,
        private_reminder_time: str | None,
        group_reminder_enabled: bool,
        private_reminder_enabled: bool,
        rotation_enabled: bool,
    ) -> None:
        timestamp = now_iso()
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                """
                INSERT INTO zones (
                    code, title, enabled, sort_order, team_size_mode, report_required,
                    report_deadline_time, private_reminder_time, group_reminder_enabled,
                    private_reminder_enabled, rotation_enabled, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    title = excluded.title,
                    enabled = excluded.enabled,
                    sort_order = excluded.sort_order,
                    team_size_mode = excluded.team_size_mode,
                    report_required = excluded.report_required,
                    report_deadline_time = excluded.report_deadline_time,
                    private_reminder_time = excluded.private_reminder_time,
                    group_reminder_enabled = excluded.group_reminder_enabled,
                    private_reminder_enabled = excluded.private_reminder_enabled,
                    rotation_enabled = excluded.rotation_enabled,
                    updated_at = excluded.updated_at;
                """,
                (
                    str(code),
                    str(title),
                    1 if enabled else 0,
                    int(sort_order),
                    str(team_size_mode),
                    1 if report_required else 0,
                    str(report_deadline_time) if report_deadline_time else None,
                    str(private_reminder_time) if private_reminder_time else None,
                    1 if group_reminder_enabled else 0,
                    1 if private_reminder_enabled else 0,
                    1 if rotation_enabled else 0,
                    timestamp,
                    timestamp,
                ),
            )
            await conn.commit()

    async def replace_zone_members(self, zone_code: str, members: list[dict]) -> None:
        timestamp = now_iso()
        payload = [
            (
                str(zone_code),
                int(item["telegram_id"]),
                int(item.get("sort_order", 0)),
                int(item["group_index"]) if item.get("group_index") is not None else None,
                int(item["slot_index"]) if item.get("slot_index") is not None else None,
                1 if bool(item.get("is_active", True)) else 0,
                timestamp,
            )
            for item in members
        ]
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("DELETE FROM zone_members WHERE zone_code = ?;", (str(zone_code),))
            if payload:
                await conn.executemany(
                    """
                    INSERT INTO zone_members (
                        zone_code, telegram_id, sort_order, group_index, slot_index, is_active, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    payload,
                )
            await conn.commit()

    async def replace_zone_rule(
        self,
        *,
        zone_code: str,
        rotation_mode: str,
        rotation_every_days: int,
        team_pattern_json: str,
        anchor_date: str,
        config_json: str,
    ) -> None:
        timestamp = now_iso()
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                """
                INSERT INTO zone_rules (
                    zone_code, rotation_mode, rotation_every_days, team_pattern_json,
                    anchor_date, config_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(zone_code) DO UPDATE SET
                    rotation_mode = excluded.rotation_mode,
                    rotation_every_days = excluded.rotation_every_days,
                    team_pattern_json = excluded.team_pattern_json,
                    anchor_date = excluded.anchor_date,
                    config_json = excluded.config_json,
                    updated_at = excluded.updated_at;
                """,
                (
                    str(zone_code),
                    str(rotation_mode),
                    int(rotation_every_days),
                    str(team_pattern_json),
                    str(anchor_date),
                    str(config_json),
                    timestamp,
                    timestamp,
                ),
            )
            await conn.commit()

    async def list_zone_records(self) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT code, title, enabled, sort_order, team_size_mode, report_required,
                           report_deadline_time, private_reminder_time, group_reminder_enabled,
                           private_reminder_enabled, rotation_enabled
                    FROM zones
                    ORDER BY sort_order ASC, id ASC;
                    """
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def list_all_zone_members(self) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT zone_code, telegram_id, sort_order, group_index, slot_index, is_active
                    FROM zone_members
                    ORDER BY zone_code ASC, sort_order ASC, id ASC;
                    """
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def list_all_zone_rules(self) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT zone_code, rotation_mode, rotation_every_days, team_pattern_json, anchor_date, config_json
                    FROM zone_rules
                    ORDER BY zone_code ASC;
                    """
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def sync_residents_from_json(self, json_path: Path | str = RESIDENTS_JSON_PATH) -> None:
        """
        Звіряє residents.json з базою.
        - вставляє відсутніх
        - оновлює full_name/role/is_active при зміні (по telegram_id)
        - мешканців, яких більше немає у residents.json, переводить у неактивні
        """
        path = Path(json_path)
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("residents.json must be a JSON list")

        rows = []
        active_ids: list[int] = []
        for r in data:
            telegram_id = int(r["telegram_id"])
            active_ids.append(telegram_id)
            rows.append(
                (
                    telegram_id,
                    str(r["full_name"]),
                    str(r["username"]).lstrip("@") if r.get("username") else None,
                    str(r.get("role", "resident")),
                    1 if bool(r.get("is_active", True)) else 0,
                )
            )

        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.executemany(
                """
                INSERT INTO residents (telegram_id, full_name, username, role, is_active)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    full_name = excluded.full_name,
                    username = COALESCE(excluded.username, residents.username),
                    role = excluded.role,
                    is_active = excluded.is_active;
                """,
                rows,
            )
            if active_ids:
                placeholders = ",".join(["?"] * len(active_ids))
                await conn.execute(
                    f"""
                    UPDATE residents
                    SET is_active = 0
                    WHERE telegram_id NOT IN ({placeholders});
                    """,
                    tuple(active_ids),
                )
                await conn.execute(
                    f"""
                    DELETE FROM test_mode_whitelist
                    WHERE telegram_id NOT IN ({placeholders});
                    """,
                    tuple(active_ids),
                )
                await conn.execute(
                    f"""
                    DELETE FROM test_duty_overrides
                    WHERE telegram_id NOT IN ({placeholders});
                    """,
                    tuple(active_ids),
                )
                await conn.execute(
                    f"""
                    DELETE FROM manual_duty_overrides
                    WHERE telegram_id NOT IN ({placeholders});
                    """,
                    tuple(active_ids),
                )
            await conn.commit()

    async def seed_residents_if_empty(self, json_path: Path | str = RESIDENTS_JSON_PATH) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (await conn.execute("SELECT COUNT(*) AS c FROM residents;")).fetchone()
            if int(row["c"]) > 0:
                return

        # якщо порожня — просто синхронізуємо
        await self.sync_residents_from_json(json_path)

    async def replace_residents_runtime(self, residents: list[dict]) -> None:
        rows = []
        active_ids: list[int] = []
        for resident in residents:
            telegram_id = int(resident["telegram_id"])
            active_ids.append(telegram_id)
            rows.append(
                (
                    telegram_id,
                    str(resident["full_name"]),
                    str(resident["username"]).lstrip("@") if resident.get("username") else None,
                    str(resident.get("role", "resident")),
                    1 if bool(resident.get("is_active", True)) else 0,
                )
            )

        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            if rows:
                await conn.executemany(
                    """
                    INSERT INTO residents (telegram_id, full_name, username, role, is_active)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(telegram_id) DO UPDATE SET
                        full_name = excluded.full_name,
                        username = COALESCE(excluded.username, residents.username),
                        role = excluded.role,
                        is_active = excluded.is_active;
                    """,
                    rows,
                )
            if active_ids:
                placeholders = ",".join(["?"] * len(active_ids))
                await conn.execute(
                    f"""
                    UPDATE residents
                    SET is_active = 0
                    WHERE telegram_id NOT IN ({placeholders});
                    """,
                    tuple(active_ids),
                )
            else:
                await conn.execute("UPDATE residents SET is_active = 0;")
            await conn.commit()

    async def get_resident(self, telegram_id: int) -> dict | None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT telegram_id, full_name, username, role, is_active
                    FROM residents
                    WHERE telegram_id = ?;
                    """,
                    (int(telegram_id),),
                )
            ).fetchone()
        return dict(row) if row else None

    async def get_residents_by_ids(self, telegram_ids: list[int]) -> list[dict]:
        if not telegram_ids:
            return []
        placeholders = ",".join(["?"] * len(telegram_ids))
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    f"""
                    SELECT telegram_id, full_name, username, role, is_active
                    FROM residents
                    WHERE telegram_id IN ({placeholders});
                    """,
                    tuple(int(x) for x in telegram_ids),
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def list_active_residents(self) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT telegram_id, full_name, username
                    FROM residents
                    WHERE is_active = 1
                    ORDER BY full_name ASC;
                    """
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def list_active_residents_full(self) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT telegram_id, full_name, username, role, is_active
                    FROM residents
                    WHERE is_active = 1
                    ORDER BY full_name ASC;
                    """
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def list_all_residents_full(self) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT telegram_id, full_name, username, role, is_active
                    FROM residents
                    ORDER BY full_name ASC;
                    """
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def update_resident_profile(self, telegram_id: int, full_name: str, username: str | None) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                """
                UPDATE residents
                SET full_name = ?, username = ?
                WHERE telegram_id = ?;
                """,
                (str(full_name), username.lstrip("@") if username else None, int(telegram_id)),
            )
            await conn.commit()

    async def create_duty_log(self, telegram_id: int, zone_name: str, photo_id: str, *, duty_date: date_type | None = None) -> int:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            cur = await conn.execute(
                """
                INSERT INTO duty_logs (telegram_id, zone_name, duty_date, photo_id, status, created_at)
                VALUES (?, ?, ?, ?, 'pending', ?);
                """,
                (int(telegram_id), str(zone_name), duty_date.isoformat() if duty_date else "", str(photo_id), now_iso()),
            )
            await conn.commit()
            return int(cur.lastrowid)

    async def purge_old_duty_logs(self, retention_days: int = REPORT_RETENTION_DAYS) -> int:
        keep_days = max(1, int(retention_days))
        cutoff_date = (datetime.now(ZoneInfo("Europe/Kyiv")).date() - timedelta(days=keep_days - 1)).isoformat()
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            cur = await conn.execute(
                """
                DELETE FROM duty_logs
                WHERE COALESCE(NULLIF(duty_date, ''), substr(created_at, 1, 10)) < ?;
                """,
                (cutoff_date,),
            )
            await conn.commit()
            return int(cur.rowcount or 0)

    async def set_duty_status(self, log_id: int, status: str, admin_comment: str | None = None) -> dict | None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            reviewed_at = now_iso() if str(status) in {"approved", "rejected"} else None
            await conn.execute(
                "UPDATE duty_logs SET status = ?, admin_comment = ?, reviewed_at = COALESCE(?, reviewed_at) WHERE id = ?;",
                (str(status), admin_comment, reviewed_at, int(log_id)),
            )
            await conn.commit()
            row = await (
                await conn.execute(
                    """
                    SELECT id, telegram_id, zone_name, duty_date, photo_id, status, admin_comment,
                           reviewed_at, user_reminded_at, admin_reminded_at, created_at
                    FROM duty_logs WHERE id = ?;
                    """,
                    (int(log_id),),
                )
            ).fetchone()
        return dict(row) if row else None

    async def get_duty_log(self, log_id: int) -> dict | None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT dl.id, dl.telegram_id, r.full_name, r.username, dl.zone_name, dl.duty_date, dl.photo_id,
                           dl.status, dl.admin_comment, dl.reviewed_at, dl.user_reminded_at,
                           dl.admin_reminded_at, dl.created_at
                    FROM duty_logs dl
                    JOIN residents r ON r.telegram_id = dl.telegram_id
                    WHERE dl.id = ?;
                    """,
                    (int(log_id),),
                )
            ).fetchone()
        return dict(row) if row else None

    async def create_swap(self, zone: str, from_id: int, to_id: int, for_date: date_type) -> int:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            cur = await conn.execute(
                """
                INSERT INTO swaps (zone, from_id, to_id, date, created_at)
                VALUES (?, ?, ?, ?, ?);
                """,
                (str(zone), int(from_id), int(to_id), for_date.isoformat(), now_iso()),
            )
            await conn.commit()
            return int(cur.lastrowid)

    async def list_swaps_for_date(self, for_date: date_type) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT s.id, s.zone, s.from_id, rf.full_name AS from_name,
                           s.to_id, rt.full_name AS to_name, s.date, s.created_at
                    FROM swaps s
                    JOIN residents rf ON rf.telegram_id = s.from_id
                    JOIN residents rt ON rt.telegram_id = s.to_id
                    WHERE s.date = ?
                    ORDER BY s.zone ASC, s.id DESC;
                    """,
                    (for_date.isoformat(),),
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def list_recent_swap_attempts(self, limit: int = 50) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT sa.id, sa.zone, sa.target_date, sa.status, sa.details, sa.created_at,
                           sa.from_id, rf.full_name AS from_name,
                           sa.to_id, rt.full_name AS to_name
                    FROM swap_attempts sa
                    LEFT JOIN residents rf ON rf.telegram_id = sa.from_id
                    LEFT JOIN residents rt ON rt.telegram_id = sa.to_id
                    ORDER BY sa.id DESC
                    LIMIT ?;
                    """,
                    (int(limit),),
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def get_swap_for_date_zone(self, for_date: date_type, zone: str) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT id, zone, from_id, to_id, date
                    FROM swaps
                    WHERE date = ? AND zone = ?;
                    """,
                    (for_date.isoformat(), str(zone)),
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def set_setting(self, key: str, value: str) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
                (str(key), str(value)),
            )
            await conn.commit()

    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (await conn.execute("SELECT value FROM settings WHERE key = ?;", (str(key),))).fetchone()
        return str(row["value"]) if row else default

    async def delete_setting(self, key: str) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("DELETE FROM settings WHERE key = ?;", (str(key),))
            await conn.commit()

    async def list_settings_by_prefix(self, prefix: str) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    "SELECT key, value FROM settings WHERE key LIKE ? ORDER BY key ASC;",
                    (f"{str(prefix)}%",),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def create_fine(
        self,
        user_id: int,
        reason: str,
        amount: int,
        *,
        fine_date: date_type | None = None,
        fine_type: str = "",
        issued_by: int | None = None,
        requires_proof: bool = True,
    ) -> int:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            cur = await conn.execute(
                """
                INSERT INTO fines (user_id, fine_date, fine_type, reason, amount, requires_proof, status, issued_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?);
                """,
                (
                    int(user_id),
                    (fine_date.isoformat() if fine_date else ""),
                    str(fine_type),
                    str(reason),
                    int(amount),
                    1 if requires_proof else 0,
                    int(issued_by) if issued_by else None,
                    now_iso(),
                ),
            )
            await conn.commit()
            return int(cur.lastrowid)

    async def set_fine_proof(self, fine_id: int, photo_id: str) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "UPDATE fines SET photo_proof_id = ? WHERE id = ?;",
                (str(photo_id), int(fine_id)),
            )
            await conn.commit()

    async def set_fine_status(self, fine_id: int, status: str) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("UPDATE fines SET status = ? WHERE id = ?;", (str(status), int(fine_id)))
            await conn.commit()

    async def get_fine(self, fine_id: int) -> dict | None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT f.id, f.user_id, r.full_name AS user_name, f.fine_date, f.fine_type,
                           f.reason, f.amount, f.requires_proof, f.status, f.photo_proof_id, f.issued_by, f.created_at
                    FROM fines f
                    JOIN residents r ON r.telegram_id = f.user_id
                    WHERE f.id = ?;
                    """,
                    (int(fine_id),),
                )
            ).fetchone()
        return dict(row) if row else None

    async def list_unpaid_fines(self, limit: int = 20) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT f.id, f.user_id, r.full_name AS user_name, f.fine_date, f.fine_type,
                           f.reason, f.amount, f.requires_proof, f.created_at
                    FROM fines f
                    JOIN residents r ON r.telegram_id = f.user_id
                    WHERE f.status = 'pending'
                    ORDER BY f.id DESC
                    LIMIT ?;
                    """,
                    (int(limit),),
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def list_fines_for_user(self, user_id: int, limit: int = 100) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT id, user_id, fine_date, fine_type, reason, amount, requires_proof, status, created_at
                    FROM fines
                    WHERE user_id = ?
                    ORDER BY COALESCE(NULLIF(fine_date, ''), substr(created_at, 1, 10)) DESC, id DESC
                    LIMIT ?;
                    """,
                    (int(user_id), int(limit)),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def get_user_fines_balance(self, user_id: int) -> int:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT COALESCE(SUM(amount), 0) AS balance
                    FROM fines
                    WHERE user_id = ? AND status = 'pending';
                    """,
                    (int(user_id),),
                )
            ).fetchone()
        return int(row["balance"]) if row else 0

    async def log_swap_attempt(
        self,
        *,
        from_id: int,
        zone: str,
        target_date: date_type,
        status: str,
        to_id: int | None = None,
        details: str | None = None,
    ) -> int:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            cur = await conn.execute(
                """
                INSERT INTO swap_attempts (from_id, to_id, zone, target_date, status, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    int(from_id),
                    int(to_id) if to_id is not None else None,
                    str(zone),
                    target_date.isoformat(),
                    str(status),
                    str(details) if details else None,
                    now_iso(),
                ),
            )
            await conn.commit()
            return int(cur.lastrowid)

    async def has_duty_submission(self, zone_name: str, user_id: int, for_date: date_type) -> bool:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT 1 AS ok
                    FROM duty_logs
                    WHERE zone_name = ?
                      AND telegram_id = ?
                      AND status IN ('pending', 'approved')
                      AND COALESCE(NULLIF(duty_date, ''), substr(created_at, 1, 10)) = ?
                    LIMIT 1;
                    """,
                    (str(zone_name), int(user_id), for_date.isoformat()),
                )
            ).fetchone()
        return bool(row)

    async def list_duty_logs_between(self, start_date: date_type, end_date: date_type) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT dl.id, dl.telegram_id, r.full_name, r.username, dl.zone_name, dl.duty_date, dl.photo_id,
                           dl.status, dl.admin_comment, dl.created_at
                    FROM duty_logs dl
                    JOIN residents r ON r.telegram_id = dl.telegram_id
                    WHERE COALESCE(NULLIF(dl.duty_date, ''), substr(dl.created_at, 1, 10)) BETWEEN ? AND ?
                    ORDER BY COALESCE(NULLIF(dl.duty_date, ''), substr(dl.created_at, 1, 10)) ASC, dl.created_at ASC, dl.id ASC;
                    """,
                    (start_date.isoformat(), end_date.isoformat()),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def list_recent_duty_logs(self, limit: int = 50, *, days: int | None = REPORT_RETENTION_DAYS) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            if days is None:
                rows = await (
                    await conn.execute(
                        """
                        SELECT dl.id, dl.telegram_id, r.full_name, r.username, dl.zone_name, dl.duty_date, dl.status,
                               dl.admin_comment, dl.reviewed_at, dl.created_at
                        FROM duty_logs dl
                        JOIN residents r ON r.telegram_id = dl.telegram_id
                        ORDER BY dl.id DESC
                        LIMIT ?;
                        """,
                        (int(limit),),
                    )
                ).fetchall()
            else:
                cutoff_date = (datetime.now(ZoneInfo("Europe/Kyiv")).date() - timedelta(days=max(1, int(days)) - 1)).isoformat()
                rows = await (
                    await conn.execute(
                        """
                        SELECT dl.id, dl.telegram_id, r.full_name, r.username, dl.zone_name, dl.duty_date, dl.status,
                               dl.admin_comment, dl.reviewed_at, dl.created_at
                        FROM duty_logs dl
                        JOIN residents r ON r.telegram_id = dl.telegram_id
                        WHERE COALESCE(NULLIF(dl.duty_date, ''), substr(dl.created_at, 1, 10)) >= ?
                        ORDER BY dl.id DESC
                        LIMIT ?;
                        """,
                        (cutoff_date, int(limit)),
                    )
                ).fetchall()
        return [dict(row) for row in rows]

    async def list_pending_review_logs(self, older_than_minutes: int = 30, limit: int = 20) -> list[dict]:
        cutoff = (datetime.fromisoformat(now_iso()) - timedelta(minutes=int(older_than_minutes))).isoformat(timespec="seconds")
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT dl.id, dl.telegram_id, r.full_name, r.username, dl.zone_name, dl.duty_date,
                           dl.created_at, dl.admin_reminded_at
                    FROM duty_logs dl
                    JOIN residents r ON r.telegram_id = dl.telegram_id
                    WHERE dl.status = 'pending'
                      AND dl.created_at <= ?
                      AND (dl.admin_reminded_at IS NULL OR dl.admin_reminded_at = '')
                    ORDER BY dl.created_at ASC
                    LIMIT ?;
                    """,
                    (cutoff, int(limit)),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def mark_admin_reminder_sent(self, log_id: int) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "UPDATE duty_logs SET admin_reminded_at = ? WHERE id = ?;",
                (now_iso(), int(log_id)),
            )
            await conn.commit()

    async def list_rejected_logs_for_user_reminder(self, older_than_minutes: int = 120, limit: int = 20) -> list[dict]:
        cutoff = (datetime.fromisoformat(now_iso()) - timedelta(minutes=int(older_than_minutes))).isoformat(timespec="seconds")
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT dl.id, dl.telegram_id, r.full_name, r.username, dl.zone_name, dl.duty_date,
                           dl.admin_comment, dl.reviewed_at
                    FROM duty_logs dl
                    JOIN residents r ON r.telegram_id = dl.telegram_id
                    WHERE dl.status = 'rejected'
                      AND COALESCE(dl.reviewed_at, dl.created_at) <= ?
                      AND (dl.user_reminded_at IS NULL OR dl.user_reminded_at = '')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM duty_logs newer
                          WHERE newer.telegram_id = dl.telegram_id
                            AND newer.zone_name = dl.zone_name
                            AND COALESCE(NULLIF(newer.duty_date, ''), substr(newer.created_at, 1, 10)) =
                                COALESCE(NULLIF(dl.duty_date, ''), substr(dl.created_at, 1, 10))
                            AND newer.id > dl.id
                            AND newer.status IN ('pending', 'approved')
                      )
                    ORDER BY COALESCE(dl.reviewed_at, dl.created_at) ASC
                    LIMIT ?;
                    """,
                    (cutoff, int(limit)),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def mark_user_reminder_sent(self, log_id: int) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "UPDATE duty_logs SET user_reminded_at = ? WHERE id = ?;",
                (now_iso(), int(log_id)),
            )
            await conn.commit()

    async def get_report_stats(self) -> dict:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            totals_row = await (
                await conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
                        SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected
                    FROM duty_logs;
                    """
                )
            ).fetchone()
            resident_rows = await (
                await conn.execute(
                    """
                    SELECT r.telegram_id, r.full_name,
                           COUNT(dl.id) AS total,
                           SUM(CASE WHEN dl.status = 'approved' THEN 1 ELSE 0 END) AS approved,
                           SUM(CASE WHEN dl.status = 'rejected' THEN 1 ELSE 0 END) AS rejected
                    FROM residents r
                    LEFT JOIN duty_logs dl ON dl.telegram_id = r.telegram_id
                    WHERE r.is_active = 1
                    GROUP BY r.telegram_id, r.full_name
                    ORDER BY approved DESC, total DESC, r.full_name ASC;
                    """
                )
            ).fetchall()
        return {"totals": dict(totals_row) if totals_row else {}, "by_resident": [dict(r) for r in resident_rows]}

    async def log_admin_action(
        self,
        admin_id: int,
        action_type: str,
        *,
        target_id: int | None = None,
        details: str | None = None,
    ) -> int:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            cur = await conn.execute(
                """
                INSERT INTO admin_action_logs (admin_id, action_type, target_id, details, created_at)
                VALUES (?, ?, ?, ?, ?);
                """,
                (
                    int(admin_id),
                    str(action_type),
                    int(target_id) if target_id is not None else None,
                    str(details) if details else None,
                    now_iso(),
                ),
            )
            await conn.commit()
            return int(cur.lastrowid)

    async def list_admin_action_logs(self, limit: int = 50) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT aal.id, aal.action_type, aal.details, aal.created_at,
                           aal.admin_id, a.full_name AS admin_name,
                           aal.target_id, t.full_name AS target_name
                    FROM admin_action_logs aal
                    LEFT JOIN residents a ON a.telegram_id = aal.admin_id
                    LEFT JOIN residents t ON t.telegram_id = aal.target_id
                    ORDER BY aal.id DESC
                    LIMIT ?;
                    """,
                    (int(limit),),
                )
            ).fetchall()
        return [dict(r) for r in rows]

    async def has_deadline_alert(self, telegram_id: int, zone_name: str, duty_date: date_type) -> bool:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT 1 AS ok
                    FROM deadline_alerts
                    WHERE telegram_id = ? AND zone_name = ? AND duty_date = ?
                    LIMIT 1;
                    """,
                    (int(telegram_id), str(zone_name), duty_date.isoformat()),
                )
            ).fetchone()
        return bool(row)

    async def mark_deadline_alert(self, telegram_id: int, zone_name: str, duty_date: date_type) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                """
                INSERT INTO deadline_alerts (telegram_id, zone_name, duty_date, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(telegram_id, zone_name, duty_date) DO NOTHING;
                """,
                (int(telegram_id), str(zone_name), duty_date.isoformat(), now_iso()),
            )
            await conn.commit()

    async def clear_deadline_alert(self, telegram_id: int, zone_name: str, duty_date: date_type) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "DELETE FROM deadline_alerts WHERE telegram_id = ? AND zone_name = ? AND duty_date = ?;",
                (int(telegram_id), str(zone_name), duty_date.isoformat()),
            )
            await conn.commit()

    async def has_deadline_user_reminder(
        self,
        telegram_id: int,
        zone_name: str,
        duty_date: date_type,
        stage: str,
    ) -> bool:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT 1 AS ok
                    FROM deadline_user_reminders
                    WHERE telegram_id = ? AND zone_name = ? AND duty_date = ? AND stage = ?
                    LIMIT 1;
                    """,
                    (int(telegram_id), str(zone_name), duty_date.isoformat(), str(stage)),
                )
            ).fetchone()
        return bool(row)

    async def mark_deadline_user_reminder(
        self,
        telegram_id: int,
        zone_name: str,
        duty_date: date_type,
        stage: str,
    ) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                """
                INSERT INTO deadline_user_reminders (telegram_id, zone_name, duty_date, stage, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(telegram_id, zone_name, duty_date, stage) DO NOTHING;
                """,
                (int(telegram_id), str(zone_name), duty_date.isoformat(), str(stage), now_iso()),
            )
            await conn.commit()

    async def clear_deadline_user_reminders(self, telegram_id: int, zone_name: str, duty_date: date_type) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "DELETE FROM deadline_user_reminders WHERE telegram_id = ? AND zone_name = ? AND duty_date = ?;",
                (int(telegram_id), str(zone_name), duty_date.isoformat()),
            )
            await conn.commit()

    async def touch_user_contact(self, telegram_id: int) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            resident_row = await (
                await conn.execute(
                    "SELECT 1 FROM residents WHERE telegram_id = ?;",
                    (int(telegram_id),),
                )
            ).fetchone()
            if not resident_row:
                return
            await conn.execute(
                """
                INSERT INTO user_contacts (telegram_id, has_started, can_message, last_interaction_at)
                VALUES (?, 1, 0, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    has_started = 1,
                    last_interaction_at = excluded.last_interaction_at;
                """,
                (int(telegram_id), now_iso()),
            )
            await conn.commit()

    async def mark_message_delivery(self, telegram_id: int, success: bool, error_text: str | None = None) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            resident_row = await (
                await conn.execute(
                    "SELECT 1 FROM residents WHERE telegram_id = ?;",
                    (int(telegram_id),),
                )
            ).fetchone()
            if not resident_row:
                return
            await conn.execute(
                """
                INSERT INTO user_contacts (
                    telegram_id, has_started, can_message, last_interaction_at, last_delivery_at, last_delivery_error
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    has_started = CASE
                        WHEN excluded.can_message = 1 THEN 1
                        ELSE user_contacts.has_started
                    END,
                    can_message = excluded.can_message,
                    last_delivery_at = excluded.last_delivery_at,
                    last_delivery_error = excluded.last_delivery_error;
                """,
                (
                    int(telegram_id),
                    1 if success else 0,
                    1 if success else 0,
                    now_iso(),
                    now_iso(),
                    None if success else str(error_text or "Невідома помилка"),
                ),
            )
            await conn.commit()

    async def list_contact_statuses(self) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT r.telegram_id, r.full_name, r.username, r.role, r.is_active,
                           uc.has_started, uc.can_message, uc.last_interaction_at,
                           uc.last_delivery_at, uc.last_delivery_error
                    FROM residents r
                    LEFT JOIN user_contacts uc ON uc.telegram_id = r.telegram_id
                    WHERE r.is_active = 1
                    ORDER BY COALESCE(uc.has_started, 0) DESC, COALESCE(uc.can_message, 0) DESC, r.full_name ASC;
                    """
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def list_test_whitelist_ids(self) -> list[int]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT tw.telegram_id
                    FROM test_mode_whitelist tw
                    JOIN residents r ON r.telegram_id = tw.telegram_id
                    WHERE r.is_active = 1
                    ORDER BY tw.telegram_id ASC;
                    """
                )
            ).fetchall()
        return [int(row["telegram_id"]) for row in rows]

    async def is_test_whitelisted(self, telegram_id: int) -> bool:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT 1 AS ok
                    FROM test_mode_whitelist tw
                    JOIN residents r ON r.telegram_id = tw.telegram_id
                    WHERE tw.telegram_id = ?
                      AND r.is_active = 1
                    LIMIT 1;
                    """,
                    (int(telegram_id),),
                )
            ).fetchone()
        return bool(row)

    async def is_active_resident(self, telegram_id: int) -> bool:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            row = await (
                await conn.execute(
                    """
                    SELECT 1 AS ok
                    FROM residents
                    WHERE telegram_id = ?
                      AND is_active = 1
                    LIMIT 1;
                    """,
                    (int(telegram_id),),
                )
            ).fetchone()
        return bool(row)

    async def set_test_whitelist(self, telegram_id: int, enabled: bool) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            if enabled:
                resident_row = await (
                    await conn.execute(
                        "SELECT 1 FROM residents WHERE telegram_id = ? AND is_active = 1;",
                        (int(telegram_id),),
                    )
                ).fetchone()
                if not resident_row:
                    return
                await conn.execute(
                    """
                    INSERT INTO test_mode_whitelist (telegram_id, created_at)
                    VALUES (?, ?)
                    ON CONFLICT(telegram_id) DO NOTHING;
                    """,
                    (int(telegram_id), now_iso()),
                )
            else:
                await conn.execute(
                    "DELETE FROM test_mode_whitelist WHERE telegram_id = ?;",
                    (int(telegram_id),),
                )
            await conn.commit()

    async def clear_test_mode_data(self) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("DELETE FROM test_mode_whitelist;")
            await conn.execute("DELETE FROM test_duty_overrides;")
            await conn.commit()

    async def get_manual_overrides_for_date(self, for_date: date_type) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT mdo.id, mdo.date, mdo.zone_name, mdo.slot_index, mdo.telegram_id,
                           r.full_name, r.username, r.is_active
                    FROM manual_duty_overrides mdo
                    JOIN residents r ON r.telegram_id = mdo.telegram_id
                    WHERE mdo.date = ?
                      AND r.is_active = 1
                    ORDER BY mdo.zone_name ASC, mdo.slot_index ASC;
                    """,
                    (for_date.isoformat(),),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def set_manual_override(self, for_date: date_type, zone_name: str, telegram_ids: list[int]) -> None:
        zone = str(zone_name)
        ids = []
        seen: set[int] = set()
        for telegram_id in telegram_ids:
            resident_id = int(telegram_id)
            if resident_id in seen:
                continue
            if not await self.is_active_resident(resident_id):
                continue
            seen.add(resident_id)
            ids.append(resident_id)
        if not ids:
            raise ValueError(f"{zone} override requires at least one active resident")
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "DELETE FROM manual_duty_overrides WHERE date = ? AND zone_name = ?;",
                (for_date.isoformat(), zone),
            )
            for index, telegram_id in enumerate(ids):
                await conn.execute(
                    """
                    INSERT INTO manual_duty_overrides (date, zone_name, slot_index, telegram_id, created_at)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (for_date.isoformat(), zone, int(index), telegram_id, now_iso()),
                )
            await conn.commit()

    async def clear_manual_overrides_for_date(self, for_date: date_type) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "DELETE FROM manual_duty_overrides WHERE date = ?;",
                (for_date.isoformat(),),
            )
            await conn.commit()

    async def get_test_overrides_for_date(self, for_date: date_type) -> list[dict]:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            rows = await (
                await conn.execute(
                    """
                    SELECT tdo.id, tdo.date, tdo.zone_name, tdo.slot_index, tdo.telegram_id,
                           r.full_name, r.username, r.is_active
                    FROM test_duty_overrides tdo
                    JOIN residents r ON r.telegram_id = tdo.telegram_id
                    WHERE tdo.date = ?
                      AND r.is_active = 1
                    ORDER BY tdo.zone_name ASC, tdo.slot_index ASC;
                    """,
                    (for_date.isoformat(),),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def set_test_override(self, for_date: date_type, zone_name: str, telegram_ids: list[int]) -> None:
        zone = str(zone_name)
        ids = []
        seen: set[int] = set()
        for telegram_id in telegram_ids:
            resident_id = int(telegram_id)
            if resident_id in seen:
                continue
            if not await self.is_active_resident(resident_id):
                continue
            seen.add(resident_id)
            ids.append(resident_id)
        if not ids:
            raise ValueError(f"{zone} override requires at least one active resident")
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "DELETE FROM test_duty_overrides WHERE date = ? AND zone_name = ?;",
                (for_date.isoformat(), zone),
            )
            for index, telegram_id in enumerate(ids):
                await conn.execute(
                    """
                    INSERT INTO test_duty_overrides (date, zone_name, slot_index, telegram_id, created_at)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (for_date.isoformat(), zone, int(index), telegram_id, now_iso()),
                )
            await conn.commit()

    async def clear_test_overrides_for_date(self, for_date: date_type) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "DELETE FROM test_duty_overrides WHERE date = ?;",
                (for_date.isoformat(),),
            )
            await conn.commit()

    async def reset_database(self, json_path: Path | str = RESIDENTS_JSON_PATH) -> None:
        async with aiosqlite.connect(str(self.db_path)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys = OFF;")
            await conn.executescript(
                """
                DROP TABLE IF EXISTS deadline_user_reminders;
                DROP TABLE IF EXISTS deadline_alerts;
                DROP TABLE IF EXISTS admin_action_logs;
                DROP TABLE IF EXISTS user_contacts;
                DROP TABLE IF EXISTS swap_attempts;
                DROP TABLE IF EXISTS manual_duty_overrides;
                DROP TABLE IF EXISTS test_duty_overrides;
                DROP TABLE IF EXISTS test_mode_whitelist;
                DROP TABLE IF EXISTS fines;
                DROP TABLE IF EXISTS swaps;
                DROP TABLE IF EXISTS duty_logs;
                DROP TABLE IF EXISTS settings;
                DROP TABLE IF EXISTS residents;
                """
            )
            await conn.commit()

        await self.init_schema()
        await self.seed_residents_if_empty(json_path)
        await self.sync_residents_from_json(json_path)
