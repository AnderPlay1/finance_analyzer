"""
Создаёт и наполняет MySQL-базу демо-данными из CSV.

По умолчанию скрипт не пересоздаёт уже заполненные таблицы:
он создаёт недостающую схему и импортирует только пустые users/transactions.
Для полного сброса перед запуском задайте RESET_DB=1.
"""

import os
import sys
from pathlib import Path

from sqlalchemy import inspect, text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.init_db import (
    Base,
    engine,
    ensure_performance_indexes,
    ensure_schema_columns,
)
from scripts.db_functions import sync_category_catalog
from scripts.parser import parse_cities, parse_transactions


SETUP_MARKER = "demo_import_complete"


def ensure_setup_status_table() -> None:
    """
    Создаёт служебную таблицу с маркером успешного импорта демо-данных.
    :return: None
    """
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE IF NOT EXISTS setup_status ("
            "name VARCHAR(80) PRIMARY KEY, "
            "value VARCHAR(80) NOT NULL"
            ")"
        ))


def setup_marker_exists() -> bool:
    """
    Проверяет, был ли демо-импорт успешно завершён ранее.
    :return: bool
    """
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT 1 FROM setup_status WHERE name = :name LIMIT 1"),
            {"name": SETUP_MARKER},
        ).first() is not None


def mark_setup_complete() -> None:
    """
    Записывает маркер успешного демо-импорта.
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


def truncate_table(table_name: str) -> None:
    """
    Быстро очищает таблицу MySQL.
    :param table_name: str
    :return: None
    """
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {table_name}"))


def table_has_rows(table_name: str) -> bool:
    """
    Проверяет, есть ли в таблице хотя бы одна строка.
    :param table_name: str
    :return: bool
    """
    with engine.connect() as connection:
        return connection.execute(
            text(f"SELECT 1 FROM {table_name} LIMIT 1")
        ).first() is not None


def main() -> None:
    """
    Создаёт таблицы, импортирует отсутствующие CSV-данные и создаёт индексы.
    :return: None
    """
    if not engine.url.drivername.startswith("mysql"):
        raise RuntimeError("setup_mysql_db.py работает только с MySQL.")

    if os.getenv("RESET_DB") == "1":
        print("RESET_DB=1: пересоздание таблиц MySQL...")
        Base.metadata.drop_all(engine)

    print("Создание недостающих таблиц MySQL...")
    Base.metadata.create_all(engine)
    ensure_schema_columns()
    ensure_setup_status_table()

    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())

    if "users" not in table_names or not table_has_rows("users"):
        print("Импорт пользователей из CSV...")
        parse_cities()
    else:
        print("Таблица users уже содержит данные. Пропускаю импорт пользователей.")

    import_is_complete = setup_marker_exists()
    if import_is_complete and table_has_rows("transactions"):
        print("Таблица transactions уже содержит завершённый импорт. Пропускаю.")
    else:
        if table_has_rows("transactions"):
            print(
                "Транзакции уже есть, но маркер импорта отсутствует. "
                "Синхронизирую категории и восстанавливаю маркер..."
            )
            sync_category_catalog()
            mark_setup_complete()
        else:
            print("Импорт транзакций из CSV...")
            parse_transactions()
            mark_setup_complete()

    sync_category_catalog()

    print("Создание индексов...")
    ensure_performance_indexes()
    print("MySQL-база готова.")


if __name__ == "__main__":
    main()
