import os
import csv
from pandas import DataFrame as df



with open("C:\\Users\\brans\\Documents\\DataStructuresFinal\\data\\expandedWorcesterMedicalCenters.csv", "r") as f:
    f = csv.reader(f)
    data = list(f)
    DF = df(data, columns=data[0])

DF = DF.iloc[1:]

hospital_df = DF.loc[((DF["PRVDR_CTGRY_SBTYP_CD"] == "01") | (DF["PRVDR_CTGRY_SBTYP_CD"] == "") | (DF["PRVDR_CTGRY_SBTYP_CD"] == "02") \
                        | (DF["PRVDR_CTGRY_SBTYP_CD"] == "11") \
                      ) & (DF["PRVDR_CTGRY_CD"] == "01")]

with open("C:\\Users\\brans\\Documents\\DataStructuresFinal\\data\\hospital_services\\MUP_IHP_RY23_P03_V10_DY21_PRVSVC.CSV", "r") as w:
    w = csv.reader(w)
    newData = list(w)
    ccnDF = df(newData, columns=newData[0])
    ccnDF = ccnDF.iloc[1:]

validCCNs = set(ccnDF["Rndrng_Prvdr_CCN"])

hospital_np = hospital_df.to_numpy()

for i in hospital_np:
    if i[17] not in validCCNs:
        print(i[17])
        print(i[11])
        print(i[10])