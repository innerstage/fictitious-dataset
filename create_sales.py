import pandas as pd
import numpy as np
import sys
import csv


N = 1000000
year = int(sys.argv[1])

ALL_DATES = pd.date_range(start=str(year), end=str(year+1))[:-1]

country_df = pd.read_csv("source_data/country_list.csv")
product_df = pd.read_csv("source_data/product_list.csv")

ALL_COUNTRIES = country_df["iso2_code"].to_list()
ALL_PRODUCTS = product_df["product_name"].to_list()

df = pd.DataFrame({
    "date": np.random.choice(ALL_DATES, size=N),
    "origin_country": np.random.choice(ALL_COUNTRIES + ["XX", "-", np.nan], size=N),
    "destination_country": np.random.choice(ALL_COUNTRIES + ["XX", "-", np.nan], size=N),
    "product": np.random.choice(ALL_PRODUCTS + [np.nan], size=N),
    "duration": np.round(100 * np.random.random(N), decimals=2),
    "units": np.random.randint(low=1, high=100, size=N)
})

df["amount"] = np.round(df["units"] * np.random.randint(low=5, high=16, size=N), decimals=2)
df = df.sort_values(by="date")

df.to_csv("source_data/sales_{}.csv".format(year), index=False, quoting=csv.QUOTE_NONNUMERIC)