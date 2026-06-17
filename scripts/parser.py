# -*- coding: utf-8 -*-
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.init_db import engine
from scripts.db_functions import sync_category_catalog

TRANSACTION_CHUNK_SIZE = 100_000
SQL_INSERT_CHUNK_SIZE = 10_000


def get_engine() -> Engine:
    """
    Получение SQLAlchemy Engine для подключения к базе данных.
    :return: SQLAlchemy Engine
    """
    return engine


def trim_quantile(df, col, q) -> pd.DataFrame:
    '''
    Чистит выбросы датасета, убирая верхние и нижние 5% данных.

    :param df: Датафрейм для очистки -> pd.DataFrame
    :param col: Название колонки, по который производится выброс -> string
    :param q: Процент выброса (в десятичном виде) -> float

    :return: Очищенный датафрейм
    :rtype: DataFrame
    '''
    low = df[col].quantile(q)
    high = df[col].quantile(1 - q)
    return df[(df[col] >= low) & (df[col] <= high)]


def clean_amount_series(series: pd.Series) -> pd.Series:
    """
    Приводит строковую колонку суммы к числовому формату.
    :param series: pd.Series
    :return: pd.Series
    """
    cleaned = (
        series
        .astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace("—", "", regex=False)
        .str.replace("None", "", regex=False)
        .str.replace("nan", "", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def parse_cities() -> None:
    '''
    Читает данные из users_data.csv, форматирует и добавляет в базу данных.
    Пользователи из CSV не содержат пароль, поэтому поле password_hash остаётся пустым.
    '''
    engine = get_engine()
    users = pd.read_csv("data/users_data.csv", encoding="cp1251", sep=';')
    users["monthly_income_amt"] = clean_amount_series(users["monthly_income_amt"])
    users = users[users["monthly_income_amt"] >= 0]
    users = users[users["citizenship_country_nm"] == "РФ"]
    users = users[users["lvn_state_nm"] != "0"]
    users = users.drop(columns=[
        "Unnamed: 0",
        "gender_cd",
        "citizenship_country_nm",
        "first_bank_product_date",
        "first_session_dttm",
        "risk_level_cd",
    ])

    users = trim_quantile(users, "monthly_income_amt", 0.05)
    with open("data/all_cities.txt", "r", encoding="utf-8") as file:
        all_cities = file.read().splitlines()

    for index, row in users.iterrows():
        for j in all_cities:
            if j in row["lvn_state_nm"]:
                users.at[index, "lvn_state_nm"] = j

    users.rename(
        columns={
            "party_rk": "user_id",
            "monthly_income_amt": "income",
            "lvn_state_nm": "region",
        },
        inplace=True,
    )
    users.to_sql(
        "users",
        engine,
        if_exists="append",
        index=False,
        chunksize=SQL_INSERT_CHUNK_SIZE,
        method="multi",
    )


def get_transaction_amount_bounds() -> tuple[float, float]:
    """
    Считает границы квантильной очистки по суммам транзакций без загрузки
    всего CSV в память.
    :return: tuple[float, float]
    """
    amounts = []
    for chunk in pd.read_csv(
        "data/all_user_transactions.csv",
        encoding="cp1251",
        sep=';',
        dtype=str,
        usecols=["transaction_amt_rur"],
        chunksize=TRANSACTION_CHUNK_SIZE,
    ):
        chunk_amounts = clean_amount_series(chunk["transaction_amt_rur"])
        chunk_amounts = chunk_amounts.dropna()
        chunk_amounts = chunk_amounts[chunk_amounts > 0]
        if not chunk_amounts.empty:
            amounts.append(chunk_amounts)

    if not amounts:
        return 0.0, 0.0

    all_amounts = pd.concat(amounts, ignore_index=True)
    return (
        float(all_amounts.quantile(0.05)),
        float(all_amounts.quantile(0.95)),
    )


def prepare_transactions_chunk(
    transactions: pd.DataFrame,
    low_amount: float,
    high_amount: float,
) -> pd.DataFrame:
    """
    Очищает и форматирует один chunk транзакций.
    :param transactions: pd.DataFrame
    :param low_amount: float
    :param high_amount: float
    :return: pd.DataFrame
    """
    transactions["transaction_amt_rur"] = clean_amount_series(
        transactions["transaction_amt_rur"]
    )
    transactions = transactions[
        transactions["loyalty_cashback_category_nm"] != "0"
    ]
    transactions = transactions.dropna(subset=["transaction_amt_rur"])
    transactions = transactions[transactions["transaction_amt_rur"] > 0]
    transactions = transactions[
        transactions["transaction_amt_rur"].between(low_amount, high_amount)
    ]

    transactions["real_transaction_dttm"] = pd.to_datetime(
        transactions["real_transaction_dttm"],
        errors="coerce",
    ).dt.date
    transactions = transactions.dropna(subset=["real_transaction_dttm"])

    transactions.rename(
        columns={
            "party_rk": "user_id",
            "transaction_amt_rur": "amount",
            "loyalty_cashback_category_nm": "category",
            "real_transaction_dttm": "transaction_date",
        },
        inplace=True,
    )
    return transactions[["user_id", "amount", "category", "transaction_date"]]


def parse_transactions() -> None:
    '''
    Читает данные из all_user_transactions.csv, форматирует и добавляет в базу данных.
    '''
    engine = get_engine()
    low_amount, high_amount = get_transaction_amount_bounds()
    if low_amount == 0.0 and high_amount == 0.0:
        print("Нет транзакций для импорта.")
        return

    drop_columns = [
        "Unnamed: 0",
        "account_rk",
        "financial_account_type_cd",
        "financial_account_subtype_cd",
        "transaction_type_cd",
        "brand_nm",
        "loyalty_accrual_rub_amt",
        "utilization_flg",
    ]

    imported_rows = 0
    for chunk in pd.read_csv(
        "data/all_user_transactions.csv",
        encoding="cp1251",
        sep=';',
        dtype=str,
        chunksize=TRANSACTION_CHUNK_SIZE,
    ):
        chunk = chunk.drop(columns=drop_columns)
        prepared = prepare_transactions_chunk(chunk, low_amount, high_amount)
        if prepared.empty:
            continue
        prepared.to_sql(
            "transactions",
            engine,
            if_exists="append",
            index=False,
            chunksize=SQL_INSERT_CHUNK_SIZE,
            method="multi",
        )
        imported_rows += len(prepared)
        print(f"Импортировано транзакций: {imported_rows}")

    sync_category_catalog()


def parse_all() -> None:
    """
    Функция, вызов которой запускает парсер.
    """
    parse_cities()
    parse_transactions()
