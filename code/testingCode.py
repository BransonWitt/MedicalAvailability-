import csv
import os
from pandas import DataFrame as df

cwd = os.getcwd()

def cleanAddress(addrs:str) -> str:
    addrs = addrs.split(',')[0].upper()
    addrs = addrs.split(" SUITE")[0].upper()
    
    if addrs[-1] == "*" or addrs[-1] == "^":
        addrs = addrs[:-1]
    elif addrs[-1] == "~":
        addrs = ""#Non-licensed practices with little info
    return addrs 

def CheckDifferentStreets(address:str, dataset:df, columnName:str, beenDone = False) -> df:
    if any(abbv in address for abbv in ["ST", "BLVD", "RD", "AVE"]):
        address.replace("ST", "STREET")
        address.replace("BLVD", "BOULEVARD")
        address.replace("RD","ROAD")
        address.replace("AVE", "AVENUE")
        
        row = dataset.loc[dataset[columnName] == address]
        
    elif any(abbv in address for abbv in ["STREET", "BOULEVARD", "ROAD", "AVENUE"]):
        address.replace("STREET", "ST")
        address.replace("BOULEVARD", "BLVD")
        address.replace("ROAD", "RD")
        address.replace("AVENUE", "AVE")
        
        row = dataset.loc[dataset[columnName] == address]
    
    else:
        row = dataset.loc[dataset[columnName] == address]
    
    if len(row.index) != 0:
        return row
    
    elif beenDone == True:
        print("Couldn't find")
        print(address)
        return row
    
    else:
        CheckDifferentStreets(address, dataset, columnName, beenDone=True)
        
    

def checkAddressMatches(SmallDataFrame:df, CheckDataFrame:df, ColumnName1:str, ColumnName2:str, CheckColumn1:str, key:str) -> df:
    returnDF = df(columns=CheckDataFrame.iloc[:1])
    
    for i in SmallDataFrame[ColumnName1]:
        i = cleanAddress(i)
        try:
            row = CheckDifferentStreets(i, CheckDataFrame, ColumnName2)
            
            print(str(row[CheckColumn1])[-2:])
            if row[CheckColumn1] == key:
                print("GOOD")
                returnDF.add(row) 
        except:
            print("FAILED")
            pass
            
        
    
    return returnDF

with open(cwd + "\\data\\worcester_county_healthcare_facilities.csv", "r") as file:
    file = csv.reader(file)
    data = list(file)

care_data = df(data, columns=data[0])
npData = care_data.to_numpy()

primary_care = care_data.loc[care_data["Medical"] == "Yes"]
secondary_care = care_data.loc[care_data["Surgical"] == "Yes"]

with open(cwd + "\\data\\POS_File_Hospital_Non_Hospital_Facilities_Q1_2024.csv", "r") as US_file:
    US_file = csv.reader(US_file)
    US_data = list(US_file)

US_medical_facilities = df(US_data, columns=US_data[0])

compared = checkAddressMatches(primary_care, US_medical_facilities, "Street", "ST_ADR", "STATE_CD", "MA")
print(compared)



