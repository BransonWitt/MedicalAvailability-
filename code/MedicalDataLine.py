from pandas import DataFrame as df
import pandas as pd
import os

class MedicalDataFrameLine:
    def __init__(self, line:df):
        #Asserting a valid, single line data frame from the medical data frame entry
        assert(line.shape == (473,))
        
        #Setting name
        self.name = line["FAC_NAME"]
        
        #Setting provider number
        self.providerNum = line["PRVDR_NUM"] #Note to self, loads in as a string
        self.crssRefProviderNum = line["CROSS_REF_PROVIDER_NUMBER"]
        
        #Setting geographic information
        self.street = line["ST_ADR"]
        self.city = line["CITY_NAME"]
        self.zipcode = line["ZIP_CD"]
        
        #Calling to finish setting up attributes
        self.getGeographicInformation()
    
    def getGeographicInformation(self):
        """sets the geographic information of facility"""
        #Loading the csv data frame
        latlngDF = pd.read_csv(f"{os.getcwd()}\\data\\medical_lat_long.csv")
        
        #Searching and finding a matching line
        matchingLine = latlngDF.loc[(latlngDF["Name"] == self.name) & (latlngDF["Provider Number"] == self.providerNum)]
        
        #Setting lat and long attributes
        self.lat = matchingLine["Lat"].values[0]
        self.lng = matchingLine["Long"].values[0]
    
    def findServiceData(self):
        """Returns a DataFrame of all the proceedure costs and descriptions for the specified medical center. Data comes from CMS, and some medical centers have no returns"""
        #Loading the CSV file
        costDataFrame = pd.read_csv(f"{os.getcwd()}\\data\\hospital_services\\MUP_IHP_RY23_P03_V10_DY21_PRVSVC.csv",encoding='cp1252')
        #Finding a matching dataFrame
        matchingDF = costDataFrame.loc[costDataFrame["Rndrng_Prvdr_CCN"] == int(self.providerNum)]
        return matchingDF
            