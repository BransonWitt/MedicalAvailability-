from processMedicalDataFrame import MedicalCenterDataFrame
from MedicalDataLine import MedicalDataFrameLine
import pandas as pd
import os

#PRVDR_CTGRY_SBTYP_CD
subtype_filter = {
    "SHORT_TERM" : True, #
    "LONG_TERM" : False, #
    "RELIGIOUS_NONMEDICAL" : False, #NR
    "PSYCHIATRIC" : False, #
    "REHABILITATION" : True, #
    "CHILDRENS_HOSPITAL" : False,
    "DISTINCT_PSYCH_HOSP" : False,
    "CRITICAL_ACCESS_HOSP" : True, #
    "TRANSPLANT_HOSP" : True, #
    "RURAL_EMERGENCY_HOSP" : False
}

#PRVDR_CTGRY_CD
category_filter = {
    "HOSPITAL" : True, #
    "DUALLY_SKILLED_NURSING_FACILITY" : False, #NR
    "DISTINCT_PART_NURSING_FACILITY" : True, #
    "SKILLED_NURSING_FACILITY": True, #
    "HOME_HEALTH_AGENCY" : False,
    "PYSCH_RESIDENTIAL_TREATMENT_FACILITY" : False,
    "PORTABLE_XRAY_SUPPLIER" : False,
    "OUTPATIENT_PHYSICAL_THERAPY_OR_SPEECH_PATHOLOGY" : False, #
    "END_STAGE_RENAL_DISEASE_FACILITY" : False, #
    "NURSING_FACILITY" : False, #
    "INTERMEDIATE_CARE_FACILITY_FOR_MENTALLY_HANDICAPPED" : False, #
    "RURAL_HEALTH_CENTER" : False, #
    "COMPREHENSIVE_OUTPATIENT_REHAB" : False,
    "ABULATORY_SURGICAL_CENTER" : False,
    "HOSPICE_CENTER" : False,
    "ORGAN_PROCUREMENT_ORG" : False,
    "COMMUNITY_MENTAL_HEALTH_CENTER" : False, #
    "FEDERALLY_QUALIFIED_HEALTH_CENTER" : False #NR
}
cwd = os.getcwd()
myDataFrame = pd.read_csv(f"{cwd}\\data\\expandedWorcesterMedicalCenters.csv")

workingData = MedicalCenterDataFrame(myDataFrame)
workingData.sortByCategory("PRVDR_CTGRY_CD", **category_filter)
workingData.sortByCategory("PRVDR_CTGRY_SBTYP_CD", **subtype_filter)



workingData.save_file()
workingData.generate_isochrone_GeoJSON_files()
workingData.generate_isochrone(["SECONDARY_ROAD", "PRIMARY_ROAD"])


