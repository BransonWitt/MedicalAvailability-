import os
import csv
from pandas import DataFrame as df



with open("C:\\Users\\brans\\Documents\\DataStructuresFinal\\data\\worcester_county_healthcare_facilities.csv", "r") as f:
    f = csv.reader(f)
    data = list(f)
    DF = df(data, columns=data[0])


shortened_df = DF.loc[DF["Medical"] == "Yes"]

shortened_df.add(DF.loc[DF["Surgical"] == "Yes"])

shortened_df_np = shortened_df.to_numpy()

active = 0

for i in shortened_df_np:
    print(i)