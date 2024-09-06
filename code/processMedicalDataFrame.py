from enum import Enum, unique
import pandas as pd
from pandas import DataFrame as df
import os
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import googlemaps as gc
from openrouteservice import client as orsClient
from MedicalDataLine import MedicalDataFrameLine
import json
import time
from shapely.geometry import Point, Polygon
from descartes import PolygonPatch
from matplotlib.patches import Patch

#Class to process a dataframe of medical data
class MedicalCenterDataFrame:
    #Enum for map data based of the TIGER Data. Source found here: https://www2.census.gov/geo/pdfs/reference/mtfccs2022.pdf
    @unique
    class MTFCC(Enum):
        MOUNTAIN_PEAK = "C3022"
        ISLAND = "C3023"
        DAM = "C3027"
        LIGHTHOUSE = "C3074"
        TOWER = "C3071"
        WINDMILL_FARM = "C3076"
        SOLAR_FARM = "C3077"
        MONUMENT = "C3078"
        METRO_DIVISION = "G3120"
        COMBINED_NEW_ENGLAND_CITY_OR_TOWN_AREA = "G3200"
        URBAN_AREA = "G3500"
        STATE = "G4000"
        COUNTY = "G4020"
        COUNTY_SUBDIVISION = "G4040"
        ESTATE = "G4050"
        INCORP_PLACE = "G4110"
        CONGRESS_DISTRICT = "G5200"
        UPPER_CHAMBER_STATE_DIVISION = "G5210"
        LOWER_CHAMBER_STATE_DIVISION = "G5220"
        VOTING_DISTRICT = "G5240"
        ELEMENTARY_SCHOOL_DISTRICT = "G5400"
        SECONDARY_SCHOOL_DISTRICT = "G5410"
        UNIFIED_SCHOOL_DISTRICT = "G5420"
        URBAN_GROWTH_ZONE = "G6330"
        FIVE_DIGIT_ZIPCODE_DIVISION = "G6350"
        LAKE_OR_POND = "H2030"
        RESERVOIR = "H2040"
        TREATMENT_POND = "H2041"
        BAY_OR_SIMILAR = "H2051"
        OCEAN = "H2053"
        GLACIER = "H2081"
        STREAM_OR_RIVER = "H3010"
        ARTIFICAL_WATERWAY = "H3020"
        APARTMENT_COMPLEX = "K1121"
        MOBILE_HOME_PARK = "K1223"
        HOTEL = "K1227"
        CAMPGROUND = "K1228"
        SHELTER = "K1229"
        MEDICAL_CENTER = "K1231"
        NURSING_HOME = "K1233"
        JUVINILE_INSTITUTE = "K1235"
        JAIL = "K1236"
        PRISION = "K1237"
        OTHER_CORRECTIONAL_INSITUTE = "K1238"
        RELIGIOUS_QUARTERS = "K1239"
        GOVERNMENTAL_LOCATION = "K2100"
        MILITARY_LOCATION = "K2110"
        PARK = "K2180"
        NATIONAL_PARK = "K2181"
        NATIONAL_FOREST = "K2182"
        TRIBAL_PARK = "K2183"
        STATE_PARK = "K2184"
        REGIONAL_PARK = "K2185"
        COUNTY_PARK = "K2186"
        INCORPORATED_PARK = "K2188"
        PRIVATE_PARK = "K2189"
        OTHER_PARK = "K2190"
        POST_OFFICE = "K2191"
        FIRE_DEPARTMENT = "K2193"
        POLICE_STATION = "K2194"
        LIBRARY = "K2195"
        CITY_HALL = "K2196"
        COMMERCIAL_WORKPLACE = "K2300"
        SHOPPING_CENTER = "K2361"
        INDUSTRIAL_BUILDING = "K2362"
        OFFICE_BUILDING = "K2363"
        FARM = "K2364"
        TRANSPORT_TERMINAL = "K2400"
        MARINA = "K2424"
        PIER = "K2432"
        AIRPORT = "K2451"
        TRAIN_STATION = "K2452"
        BUS_TERMINAL = "K2453"
        UNIVERSITY = "K2540"
        SCHOOL = "K2543"
        MUSEUM = "K2545"
        PLACE_OF_WORSHIP = "K3544"
        PIPLINE = "L4010"
        POWERLINE = "L4020"
        COASTLINE = "L4150"
        RAIL_FEATURE = "R1011"
        PRIMARY_ROAD = "S1100" #These are smaller roads
        SECONDARY_ROAD = "S1200" #More like highways
        LOCAL_ROAD = "S1400"
        PRIVATE_ROAD = "S1740"
    
    #Sources for the following two enums: 
    # https://data.cms.gov/sites/default/files/2023-07/0ca58d5d-7914-4532-b22d-41741d3e6151/P.QWB.POSQ.OTHER.LAYOUT.MAR23.pdf
    # https://www.hhs.gov/guidance/document/2019-provider-services-pos-file-0 
    
    #Enums for provider subtypes
    @unique
    class PRVDR_CTGRY_SBTYP_CD(Enum):
        SHORT_TERM = 1
        LONG_TERM = 2
        RELIGIOUS_NONMEDICAL = 3
        PSYCHIATRIC = 4
        REHABILITATION = 5
        CHILDRENS_HOSPITAL = 6
        DISTINCT_PSYCH_HOSP = 7
        CRITICAL_ACCESS_HOSP = 11
        TRANSPLANT_HOSP = 20
        MEDICAID_ONLY_SHORT_TERM = 22
        MEDICAID_ONLY_CHILDREN = 23
        MEDICAID_ONLY_CHILDREN_PSYCH = 24
        MEDICAID_ONLY_PSYCH = 25
        MEDICAID_ONLY_REHAB = 26
        MEDICAID_ONLY_LONG_TERM = 27
        RURAL_EMERGENCY_HOSP = 28
    
    #Enums for provider categories
    @unique
    class PRVDR_CTGRY_CD(Enum):
        HOSPITAL = 1
        DUALLY_SKILLED_NURSING_FACILITY = 2
        DISTINCT_PART_NURSING_FACILITY = 3
        SKILLED_NURSING_FACILITY = 4
        HOME_HEALTH_AGENCY = 5
        PYSCH_RESIDENTIAL_TREATMENT_FACILITY = 6
        PORTABLE_XRAY_SUPPLIER = 7
        OUTPATIENT_PHYSICAL_THERAPY_OR_SPEECH_PATHOLOGY = 8
        END_STAGE_RENAL_DISEASE_FACILITY = 9
        NURSING_FACILITY = 10
        INTERMEDIATE_CARE_FACILITY_FOR_MENTALLY_HANDICAPPED = 11
        RURAL_HEALTH_CENTER = 12
        COMPREHENSIVE_OUTPATIENT_REHAB = 14
        ABULATORY_SURGICAL_CENTER = 15
        HOSPICE_CENTER = 16
        ORGAN_PROCUREMENT_ORG = 17
        COMMUNITY_MENTAL_HEALTH_CENTER = 19
        FEDERALLY_QUALIFIED_HEALTH_CENTER = 21
        CLIA88_LAB = 22
    
    
    def __init__(self, mDataFrame:df):
        """Initializing object"""
        
        #Asserting a proper data frame
        if mDataFrame.shape[1] != 473:
            Exception("Not a supported data frame. Only supports 473 column data frames from CMS.gov")
        
        self.Data = mDataFrame
        self.name = "MedicalDataFrame"
        self.cwd = os.getcwd()
        
        self.appliedCategories = []
        self.appliefSubtypes = []
    
    def returnMTFCCFeatures(self):
        """Prints a list of valid and supported MTFCC features for the code (these are not all of them)"""
        print("MTFCC Features: " + str(self.MTFCC._member_names_))
    
    def returnViableCategories(self):
        """Prints a string of all viable categories to include in sortProviderCategory"""
        print("Viable Categories: " + str(self.PRVDR_CTGRY_CD._member_names_))
        
    def returnViableSubtypes(self):
        """Prints a string of all viable subtypes that are included in the PRVDR_CTGRY_SUBTYP_CD"""
        print("Viable Subtypes: " + str(self.PRVDR_CTGRY_SBTYP_CD._member_names_))
    
    
    def sortByCategory(self, category:str, **kwargs):
        """Takes in a list of variable Key Word Arguments and sorts the dataFrame by them"""
        """Proper kwarg input should be a dict of String values corresponding to viableSubtypes with bool values for wether they should be included"""
        """Only supported categories are PRVDR_CTGRY_SBTYP_CD or PRVDR_CTGRY_CD"""
        
        #Only supports two category filters
        assert(category == "PRVDR_CTGRY_SBTYP_CD" or category == "PRVDR_CTGRY_CD")
        
        #Assigning an enum class based upon entry
        if category == "PRVDR_CTGRY_CD":
            enumCat = self.PRVDR_CTGRY_CD
        elif category == "PRVDR_CTGRY_SBTYP_CD":
            enumCat = self.PRVDR_CTGRY_SBTYP_CD
        
        #Creating a new dataframe to replace the only one when done
        newDataFrame = df(columns = list(self.Data.columns.values))
        
        for key, value in kwargs.items():
            try:
                #Asserting a proper entry
                assert(key in enumCat._member_names_ and type(value) == bool)
                        
                #Only working with entries determined defined to be included in the scope 
                if value == True:
                    #Adding new rows to the newDataFrame depending on the key
                    enumValue = enumCat[key].value
                    
                    #Updating applied names
                    if enumCat == self.PRVDR_CTGRY_SBTYP_CD:
                        self.appliefSubtypes.append(enumValue)
                    elif enumCat == self.PRVDR_CTGRY_CD:
                        self.appliedCategories.append(enumValue)
                    
                    #Repeatedly Adding onto the data frame until all dataFrames generate by the key are concated into the newDataFrame
                    newDataFrame = pd.concat([newDataFrame, self.Data.loc[self.Data[category] == enumValue]], ignore_index=True)           
                    
            except:
                #Printing an error statement
                print(f"Error encountered with {key}, {value} key-value pair")

        #Switching old data frame with a new one
        self.Data = newDataFrame
    
    def getProceedureData(self, providerNum:int) -> df:
        """returns a dataframe with proceedure names and costs based off of a CMS provider number"""
        target_facility = self.Data.loc[self.Data["PRVDR_NUM"] == str(providerNum)]
        mdf = MedicalDataFrameLine(target_facility.iloc[0])
        return mdf.findServiceData()
    
    def __updateName(self):
        """updates the name to get ready for file storage"""
        
        #Adding sorted categories
        self.name += "_Categories"
        for category in self.appliedCategories:
            self.name += f"_{category}"
            
        #Adding sorted subtypes
        self.name += "_Subtypes"
        for styp in self.appliefSubtypes:
            self.name += f"_{styp}"
    
    def save_file(self):
        self.__updateName()
        
        """if haven't seen this type of data frame before, this method creates a folder of directories and saves the new dataframe as a csv"""
        if (self.name) not in os.listdir(f"{self.cwd}\\previous_sims"):
            #Creating the folders
            os.mkdir(f"{self.cwd}\\previous_sims\\{self.name}")
            os.mkdir(f"{self.cwd}\\previous_sims\\{self.name}\\data")
            os.mkdir(f"{self.cwd}\\previous_sims\\{self.name}\\images")
            
            workAround = open(f"{self.cwd}\\previous_sims\\{self.name}\\data\\{self.name}.csv", "w")
            
            #Saving the csv
            self.Data.to_csv(f"{self.cwd}\\previous_sims\\{self.name}\\data\\{self.name}.csv")
            
            print(f"CSV file save at {self.cwd}\\previous_sims\\{self.name}\\data")
    
    def generate_isochrone_GeoJSON_files(self):
        """Generates a folder full of geoJSON files in the data folder, uses OpenRouteService API to generate isochrones"""
        """Must have saved the file before attempting to generate isochrones"""
        
        if self.name == "MedicalDataFrame":
            self.__updateName()
        
        #Making sure that save_file method has been used before 
        try:
            pe = os.path.exists(f"{self.cwd}\\previous_sims\\{self.name}")
            if pe == False:
                raise FileExistsError
        except FileExistsError:
            print("File not found, make sure to use the save_file method before calling generate_isochrone_GeoJSON_files \n \n")
        
        
        #creating a json dir
        dir = f"{self.cwd}\\previous_sims\\{self.name}\\jsons"
        
        if "jsons" not in os.listdir(f"{self.cwd}\\previous_sims\\{self.name}"):
            os.mkdir(dir)
        
        if(len(os.listdir(f"{self.cwd}\\previous_sims\\{self.name}\\jsons\\")) == 0):
            #iterrating over the data
            for index, row in self.Data.iterrows():
                isoClient = orsClient.Client(key="5b3ce3597851110001cf6248164cba3fefd74520b8bc0de03f42f25a")
                currentRow = MedicalDataFrameLine(row)
                
                #Parameters for the API
                iso_params = {
                    "profile" : "driving-car", #Driving in a car
                    "intervals" : [300, 600, 900, 1800], #5, 10, 15, 30 minutes
                    "locations" : [[currentRow.lng, currentRow.lat]]
                }
                

                #Calling the API
                clientResponse = isoClient.isochrones(**iso_params)
                    
                #Modifying the API response so all the deatures have the name and provder number of the medical center
                for feature in clientResponse['features']:
                    feature["properties"]["name"] = currentRow.name
                    feature["properties"]["Provider Number"] = currentRow.providerNum
                    
                #Dumping the response into the JSON folder as a geoJSON
                with open(f"{dir}\\{(currentRow.name).replace(' ', '')}.json", "w") as file:
                    file.write(json.dumps(clientResponse))

                
                #Sleeping, the program is eepy
                time.sleep(0.5)
    
    def __loadFeatureDataset(self, features:list):
        """This private method loads the datasets in order to generate the ischrones, mainly the features and geometry"""
        """This takes a significant amount of time and memory, however will create incredibly accurate maps"""
        
        #Nested DataFrameToFindLineWidth
        def findWidth(mtfcc:str) -> float:
            if mtfcc == "S1200":
                return 1
            elif mtfcc == "S1100":
                return 0.5
            else:
                return 1
        
        #Loading in a large file that contains the geo survery of Worcester County 
        beginningFeatureDataFrame = gpd.read_file(f"{self.cwd}\\data\\worcester_county_shapefiles\\tl_2023_25027_edges.shp")
        
        #List to store sub data frames generated by each feature
        listFeatureDataFrame = []
        
        #Creating a new data frame based off every feature included
        for feature in features: #Making sure that no member error occurs
            if feature in self.MTFCC._member_names_:
                listFeatureDataFrame.append(beginningFeatureDataFrame.loc[beginningFeatureDataFrame["MTFCC"] == self.MTFCC[feature].value])
        
        #Merging the small feature generated data frames into one big data frame
        self.__featureDataFrame = pd.concat(listFeatureDataFrame, ignore_index=True)
        self.__featureDataFrame["linewidth"] = self.__featureDataFrame["MTFCC"].apply(findWidth)
        
        #Opening the file of geometries for all of MA and then sorting that down by county into just Worcester County
        maGeometry = gpd.read_file(f"{self.cwd}\\townssurvey_shp\\TOWNSSURVEY_POLYM.shp")
        self.__worcesterCoGeometry = maGeometry.loc[maGeometry["COUNTY"] == "WORCESTER"]
        
        #Changing the Projection to a mercador projection from a state elevation projection
        self.__worcesterCoGeometry = self.__worcesterCoGeometry.to_crs("4326")
        
        #Creating an outline polygon in a GeoDataFrame by merging the worcester county dataframe by county (only one to choose)
        #The last bits are to go into the geometry and find the polygon on the first line
        self.__countyOutline = self.__worcesterCoGeometry.dissolve("COUNTY")["geometry"][0]
        
    def __figureIsochroneColor(self, distanceInSec:int) -> str:
        """Private method to return a hex value based off time away from point"""
        if distanceInSec == 300:
            return "#38A3A5"
        elif distanceInSec == 600:
            return "#57CC99"
        elif distanceInSec == 900:
            return "#80ED99"
        elif distanceInSec == 1800:
            return "#391463"
                
    def returnFilterByValue(self, value:int, procedure:str) -> str:
        """returns a string based off the code provided and the proceedure implemented"""
        """example code: 01,  example proceedure:PRVDR_CTGRY_CD or PRVDR_CTGRY_SBTYP_CD"""
        if procedure == "PRVDR_CTGRY_CD":
            enumCat = self.PRVDR_CTGRY_CD
        elif procedure == "PRVDR_CTGRY_SBTYP_CD":
            enumCat = self.PRVDR_CTGRY_SBTYP_CD
            
        return enumCat(value).name
    
    def __findFilters(self) -> str:
        """returns a string of all the filters applied"""
        filters = "Filters: "
        
        #Looping backwards in both subtype
        for v in self.appliedCategories:
            if filters == "Filters: ":
                filters += str(self.returnFilterByValue(v, "PRVDR_CTGRY_CD"))
            else:
                filters += (", " + str(self.returnFilterByValue(v, "PRVDR_CTGRY_CD")))
            
        for i in self.appliefSubtypes:
            if filters == "Filters: ":
                filters += str(self.returnFilterByValue(i, "PRVDR_CTGRY_SBTYP_CD"))
            else:
                filters += (", " + str(self.returnFilterByValue(i, "PRVDR_CTGRY_SBTYP_CD")))
       
        
        return filters
    
    def generate_isochrone(self, features:list):
        """Generates a ischrone image under the images folder"""
        """Takes in a list of features to be included in the map based off the MTFCC features list"""
        #Inspiration taken from https://www.linkedin.com/pulse/isochrones-geopandas-paul-whiteside
        
        #Making sure that the folders needed exist
        assert((self.name) in os.listdir(f"{self.cwd}\\previous_sims"))
        assert("jsons" in os.listdir(f"{self.cwd}\\previous_sims\\{self.name}"))
        
        #calling the private method
        self.__loadFeatureDataset(features)

        #creating a path variable to save time
        path = f"{self.cwd}\\previous_sims\\{self.name}\\jsons\\"
        
        #List to hold all isochrone GeoDataFrames to eventually merge
        gdfList = []
        
        #Looping over every file in the json dir
        for file in os.listdir(path):
            with open(f"{path}{file}") as f:
                data = json.load(f) #opening the file as a json
                gpdData = gpd.GeoDataFrame.from_features(data) #creating a gpddf based off the json
                gdfList.append(gpdData) #adding the gpddf into the list
        
        #merging the dataframes
        finalIsochroneDF = pd.concat(gdfList, ignore_index=True)
        
        #Creating a new column for color based off the distance values
        finalIsochroneDF["color"] = finalIsochroneDF["value"].apply(self.__figureIsochroneColor)
        
        #Sorting Isochrone
        finalIsochroneDF =finalIsochroneDF.sort_values("value", ascending=False)
        finalIsochroneDF =finalIsochroneDF.reset_index(drop = True)
        
        #Turning the center values in the dataframe into shapely points so that GeoPandas can actually plot them when we break it off
        finalIsochroneDF["center"] = finalIsochroneDF["center"].apply(Point)
        
        #Creating a new dataframe for center points
        centerPointPlotDF = gpd.GeoDataFrame(finalIsochroneDF[["Provider Number","name","center"]].copy(), geometry = "center")
        
        #finally plotting
        fig, ax = plt.subplots(figsize = (17,20), layout = 'constrained')

        #Setting axis limits and variables, currenly optimized for Worcester County
        ax.axis("scaled")
        ax.set_xlim(-72.35, -71.45)
        ax.set_ylim(42,42.75)
        
        #Variables for the axis limits
        xlim = ax.get_xlim()
        ylim = ax.get_ylim()

        #Plotting the points
        finalIsochroneDF.plot(ax=ax, color=finalIsochroneDF["color"]) #Plotting the isochrone polygons
        centerPointPlotDF.plot(ax=ax, color='#8C001A') #Plotting the points included in the dataframe
        self.__worcesterCoGeometry.plot(ax=ax, color='#FF000000', edgecolor='black')#Plotting the municipalities with a black border
        self.__featureDataFrame.plot(ax=ax, color='#e4eaed', linewidth=self.__featureDataFrame['linewidth'])#plotting a grayish color for all the features

        #Creating inner and outerbounds
        outer_bound = Polygon([(xlim[0], ylim[0]), (xlim[0], ylim[1]), (xlim[1], ylim[1]), (xlim[1], ylim[0]), (xlim[0], ylim[0])])
        inner_bound = self.__countyOutline

        #Creating a white patch to go around the plot
        patchPoly = outer_bound.symmetric_difference(inner_bound)
        ax.add_patch(PolygonPatch(patchPoly, facecolor='w', zorder=1))
        
        #Adding a title
        ax.set_title(f"Isochrone Graph for Worcester County | {self.__findFilters()} ", family=["monospace"], size=17, backgroundcolor="w")
        ax.axis('off')
        
        #Custom Legend
        legend_elements = [Patch(facecolor="#38A3A5", edgecolor="#38A3A5", label = "5 min Drive Time"), Patch(facecolor="#57CC99", edgecolor="#57CC99",\
            label="10 min Drive Time"), Patch(facecolor="#80ED99", edgecolor="#80ED99", label="15 min Drive Time"), Patch(facecolor="#391463", edgecolor="#391463",\
                label="30 min Drive Time")]
        
        ax.legend(handles=legend_elements, loc="upper right")
        
        #Saving
        previous_figs = len(os.listdir(f"{self.cwd}\\previous_sims\\{self.name}\\images"))
        fig.savefig(f"{self.cwd}\\previous_sims\\{self.name}\\images\\isochrone{previous_figs}.png")
        
        #Giving the User a message
        print("Image Saved")

