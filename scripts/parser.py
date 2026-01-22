# -*- coding: utf-8 -*-
import pandas as pd

def parse_cities(users):
    with open("../data/all_cities.txt", "r") as file: 
        all_cities = file.read().splitlines()
    for index, row in users.iterrows():
        for j in all_cities:
            if j in row["lvn_state_nm"]: users.at[index, "lvn_state_nm"] = j

users = pd.read_csv("../data/users_data.csv", encoding="cp1251", sep=';') 
users = users[users["citizenship_country_nm"] == "РФ"]
users = users[users["lvn_state_nm"] != "0"]
users = users.drop(columns=["gender_cd", "citizenship_country_nm", "first_bank_product_date", "first_session_dttm", "risk_level_cd"])

parse_cities(users)
print(users)
