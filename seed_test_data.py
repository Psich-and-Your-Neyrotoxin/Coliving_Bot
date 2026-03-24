from __future__ import annotations

import argparse
import asyncio
import shutil
from pathlib import Path

from database import Database


DEFAULT_TARGET_RESIDENTS = Path("residents.test.runtime.json")
DEFAULT_TARGET_DB = Path("coliving.test.db")


async def _seed(target_db: Path, source_residents: Path, target_residents: Path) -> None:
    shutil.copyfile(source_residents, target_residents)
    db = Database(target_db)
    await db.init_schema()
    await db.seed_residents_if_empty(target_residents)
    await db.sync_residents_from_json(target_residents)


def main() -> None:
    parser = argparse.ArgumentParser(description="Створює локальну test/dev базу з demo-даними.")
    parser.add_argument("--db", default=str(DEFAULT_TARGET_DB), help="Куди створити test SQLite DB.")
    parser.add_argument(
        "--residents",
        default=str(DEFAULT_TARGET_RESIDENTS),
        help="Куди скопіювати test residents.json.",
    )
    parser.add_argument(
        "--source",
        default="residents.test.json",
        help="Джерельний файл із тестовими мешканцями.",
    )
    args = parser.parse_args()

    target_db = Path(args.db)
    target_residents = Path(args.residents)
    source_residents = Path(args.source)
    target_db.parent.mkdir(parents=True, exist_ok=True)
    target_residents.parent.mkdir(parents=True, exist_ok=True)

    asyncio.run(_seed(target_db, source_residents, target_residents))
    print(f"Готово: DB={target_db} | residents={target_residents}")


if __name__ == "__main__":
    main()
