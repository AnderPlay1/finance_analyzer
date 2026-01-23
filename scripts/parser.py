# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine


def trim_quantile(df, col, q) -> pd.DataFrame:
    low = df[col].quantile(q)
    high = df[col].quantile(1 - q)
    return df[(df[col] >= low) & (df[col] <= high)]


def parse_cities() -> None:
    engine = create_engine(
        "mysql+pymysql://user:password@localhost:3306/mydb",
        echo=True,
        pool_pre_ping=True
    )
    users = pd.read_csv("data/users_data.csv", encoding="cp1251", sep=';')
    users["monthly_income_amt"] = (
        users["monthly_income_amt"]
        .astype(str)
        .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.replace("—", "", regex=False)
            .str.replace("None", "", regex=False)
            .str.replace("nan", "", regex=False)
    )
    users["monthly_income_amt"] = pd.to_numeric(
        users["monthly_income_amt"], errors="coerce"
    )
    users = users[users["monthly_income_amt"] >= 0]
    users = users[users["citizenship_country_nm"] == "РФ"]
    users = users[users["lvn_state_nm"] != "0"]
    users = users.drop(columns=["Unnamed: 0", "gender_cd", "citizenship_country_nm",
                       "first_bank_product_date", "first_session_dttm", "risk_level_cd"])

    users = trim_quantile(users, "monthly_income_amt", 0.05)
    with open("data/all_cities.txt", "r", encoding="utf-8") as file:
        all_cities = file.read().splitlines()

    for index, row in users.iterrows():
        for j in all_cities:
            if j in row["lvn_state_nm"]:
                users.at[index, "lvn_state_nm"] = j

    users.rename(columns={"party_rk": "user_id", "monthly_income_amt": "income",
                 "lvn_state_nm": "region"}, inplace=True)
    print(users)
    users.to_sql("users", engine, if_exists="append", index=False)


def parse_transactions() -> None:
    engine = create_engine(
        "mysql+pymysql://user:password@localhost:3306/mydb",
        echo=True,
        pool_pre_ping=True
    )

    transactions = pd.read_csv(
        "data/all_user_transactions.csv",
        encoding="cp1251",
        sep=';',
        dtype=str
    )
    transactions["transaction_amt_rur"] = (
        transactions["transaction_amt_rur"]
        .astype(str)
        .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.replace("—", "", regex=False)
            .str.replace("None", "", regex=False)
            .str.replace("nan", "", regex=False)
    )
    transactions = transactions.drop(columns=[
        "Unnamed: 0", "account_rk", "financial_account_type_cd",
        "financial_account_subtype_cd", "transaction_type_cd",
        "brand_nm", "loyalty_accrual_rub_amt", "utilization_flg"
    ])

    transactions = transactions[transactions["loyalty_cashback_category_nm"] != "0"]

    transactions["transaction_amt_rur"] = pd.to_numeric(
        transactions["transaction_amt_rur"], errors="coerce"
    )

    transactions = transactions.dropna(subset=["transaction_amt_rur"])
    transactions = transactions[transactions["transaction_amt_rur"] > 0]
    transactions = trim_quantile(transactions, "transaction_amt_rur", 0.05)

    transactions["real_transaction_dttm"] = pd.to_datetime(
        transactions["real_transaction_dttm"],
        errors="coerce"
    ).dt.date
    transactions = transactions.dropna(subset=["real_transaction_dttm"])

    transactions.rename(columns={
        "party_rk": "user_id",
        "transaction_amt_rur": "amount",
        "loyalty_cashback_category_nm": "category",
        "real_transaction_dttm": "transaction_date"
    }, inplace=True)

    print(transactions.dtypes)
    print(transactions.head())

    transactions.to_sql("transactions", engine,
                        if_exists="append", index=False)


def parse_all() -> None:
    parse_cities()
    parse_transactions()
