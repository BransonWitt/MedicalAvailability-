import os
import csv
from pandas import DataFrame as df



with open("C:\\Users\\brans\\Documents\\DataStructuresFinal\\data\\POS_File_Hospital_Non_Hospital_Facilities_Q1_2024.csv", "r") as f:
    f = csv.reader(f)
    data = list(f)
    DF = df(data, columns=data[0])

test = DF.loc[(DF["FIPS_CNTY_CD"] == "027") & (DF["FIPS_STATE_CD"] == "25")]

test.to_csv("C:\\Users\\brans\\Documents\\DataStructuresFinal\\data\\expandedWorcesterMedicalCenters.csv", index=False)