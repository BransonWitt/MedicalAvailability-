import csv
import os

cwd = os.getcwd()

with open(cwd + "\\data\\worcester_county_zipcodes.csv", "r") as f:
    zip = csv.reader(f)
    zipcodes = list(zip)
    for i in zipcodes:
        try:
            zipcodes.append(int(i[0]))
            zipcodes = zipcodes[1:]
        except:
            zipcodes = zipcodes[1:]
        
with open(cwd + "\\data\\licensed-clinic-services-nov-2023.csv", "r") as facilities:
    facilities = csv.reader(facilities)
    data = list(facilities)

header = data[11]

with open("worcester_county_healthcare_facilities.csv", "w") as worcester_facilities:
    write = csv.writer(worcester_facilities, quoting=csv.QUOTE_ALL)
    write.writerow(header)
    
    for line in data[12:]:
        if int(line[3]) in zipcodes:
            write.writerow(line)
        else:
            pass

