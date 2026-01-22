# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine

def parse_cities():
    engine = create_engine(
            "mysql+pymysql://user:password@localhost:3306/mydb",
            echo=True,
            pool_pre_ping=True
            )
    users = pd.read_csv("../data/users_data.csv", encoding="cp1251", sep=';')
    users = users[users["citizenship_country_nm"] == "РФ"]
    users = users[users["lvn_state_nm"] != "0"]
    users = users.drop(columns=["gender_cd", "citizenship_country_nm", "first_bank_product_date", "first_session_dttm", "risk_level_cd"])
    with open("../data/all_cities.txt", "r") as file:
        all_cities = file.read().splitlines()
    for index, row in users.iterrows():
        for j in all_cities:
            if j in row["lvn_state_nm"]: users.at[index, "lvn_state_nm"] = j
    users.rename({"party_rk": "user_id", "monthly_income_amt": "income", "lvn_state_nm": "region"}, axis="columns")
    users.to_sql("users", engine, if_exists="append")

