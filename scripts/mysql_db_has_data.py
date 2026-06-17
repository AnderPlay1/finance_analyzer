"""
Проверяет, что MySQL-база уже содержит основные данные приложения.

Exit code:
0 - таблицы users и transactions существуют и не пустые.
1 - таблиц нет, они пустые или проверка не удалась.
"""

import sys
from pathlib import Path

from sqlalchemy import inspect, text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.init_db import engine
from scripts.db_functions import sync_category_catalog


SETUP_MARKER = "demo_import_complete"


def ensure_setup_status_table() -> None:
    """
    Создаёт таблицу маркера импорта, если её ещё нет.
    :return: None
    """
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE IF NOT EXISTS setup_status ("
            "name VARCHAR(80) PRIMARY KEY, "
            "value VARCHAR(80) NOT NULL"
            ")"
        ))


def mark_setup_complete() -> None:
    """
    Ставит маркер завершённого демо-импорта.
    :return: None
    """
    with engine.begin() as connection:
        connection.execute(
            text(
                "REPLACE INTO setup_status (name, value) "
                "VALUES (:name, '1')"
            ),
            {"name": SETUP_MARKER},
        )


def table_has_rows(table_name: str) -> bool:
    """
    Проверяет, есть ли хотя бы одна строка в таблице.
    :param table_name: str
    :return: bool
    """
    with engine.connect() as connection:
        result = connection.execute(
            text(f"SELECT 1 FROM {table_name} LIMIT 1")
        ).first()
        return result is not None


def main() -> int:
    """
    Возвращает 0, если база уже готова к запуску приложения.
    :return: int
    """
    try:
        ensure_setup_status_table()
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())
        required_tables = {"users", "transactions", "categories", "setup_status"}
        if not required_tables.issubset(table_names):
            print("Database setup is required: missing tables.")
            return 1

        has_users = table_has_rows("users")
        has_transactions = table_has_rows("transactions")
        has_categories = table_has_rows("categories")
        if not has_users:
            print("Database setup is required: users table is empty.")
            return 1
        if not has_transactions:
            print("Database setup is required: transactions table is empty.")
            return 1
        if not has_categories:
            sync_category_catalog()
            has_categories = table_has_rows("categories")
            if not has_categories:
                print("Database setup is required: categories table is empty.")
                return 1

        with engine.connect() as connection:
            setup_marker = connection.execute(
                text(
                    "SELECT 1 FROM setup_status "
                    "WHERE name = :name LIMIT 1"
                ),
                {"name": SETUP_MARKER},
            ).first()
            if setup_marker is None:
                mark_setup_complete()
                print("Existing data found. Setup marker repaired.")
                return 0

        print("Existing database with data found.")
        return 0
    except Exception as exc:
        print(f"Database setup check failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
