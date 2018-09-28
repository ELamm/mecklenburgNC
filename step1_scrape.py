import sys
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import numpy
import pandas
import pandas.io.sql
from sqlalchemy import create_engine


# This script accepts one or two arguments. If one, gets data for single property. If two, gets data for range of properties (inclusive).

# This data is then written to the mysql table 'scraped' 

##########################
# check input arguments  # 
##########################

if len(sys.argv)==1:
    print('No account numbers requested')
    exit(1)
else:
    try:
        start = int(sys.argv[1])
    except ValueError:
        print(str(sys.argv[1])+' is not an integer.')
        exit(1)

if len(sys.argv)==2:
    end = start
else:
    try:
        end = int(sys.argv[2])
    except ValueError:
        print(str(sys.argv[2])+' is not an integer.')
        exit(1)
        
if end<start:
    start = int(sys.argv[2])
    end = int(sys.argv[1])

# Display message to console:
print('starting to scrape for account numbers '+str(start)+' to '+str(end)+' at '+datetime.now().strftime("%m/%d/%Y %H:%M:%S"))


##################
# Open webdriver #
##################

scrapeUrl = 'https://property.spatialest.com/nc/mecklenburg/'
options = Options()
options.set_headless(headless=True)
driver = webdriver.Firefox(firefox_options=options)

##############################################
# function to get data for an account number #
##############################################

def getDataByAccountNo(accountNo):
    driver.get(scrapeUrl+'#/property/'+str(accountNo))
    try:
        element = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.ID, 'propertyMapHolder')))            
    except TimeoutException:
        return
        
    bs = BeautifulSoup(driver.page_source, 'lxml')
   
    # check that accountNo exists
    if len(bs.findAll(text='Oops!'))>0 and len(bs.findAll(text='No Record Found'))>0:
        return

    # data is in <strong> tags
    rec = [x.text for x in bs.findAll('strong')]

    # check that land use description is 'Single Family Residential'
    if 'SFR' not in rec or 'R100' not in rec:
        return

    else:
        rec.remove('BuildingDetails')
        return rec[0:30]

##############################################
# Get data for all account numbers requested #
##############################################

results = []
for i in range(start, end+1):
    rec = getDataByAccountNo(i)
    if rec is not None:
        results.append(rec)

driver.close()

# put into a DataFrame 
output = pandas.DataFrame(results, columns=['parcelID', 'accountNo',
    'locationAddress', 'currentOwner1', 'currentOwner2', 'mailingAddress',
    'landUseCode', 'landUseDesc', 'exemptDefer', 'neighborhood', 'legalDesc',
    'land', 'lastSaleDate', 'lastSalePrice', 'landValue', 'buildingValue',
    'features', 'heatedArea', 'heat', 'yearBuilt', 'story', 'builtUseStyle',
    'fuel', 'foundation', 'externalWall', 'fireplaces', 'halfBaths', 'fullBaths',
    'bedrooms', 'totalSqFt'])

print('Found data for '+str(len(results))+' out of '+str(end-start+1)+' records.')

################################################################################
# Format values/types in DataFrame so will be correct when written to database #
################################################################################

# Unknown values on website are "-" - replace with Null
output.replace({'-':numpy.nan}, inplace=True)

# type conversions (text/object to numeric/datetime)
for col in ['lastSalePrice', 'landValue', 'buildingValue', 'features', 'heatedArea', 'totalSqFt']:
    output[col] = pandas.to_numeric(output[col].str.replace("$", "").str.replace(",", ""))

for col in ['accountNo', 'yearBuilt', 'fireplaces', 'halfBaths', 'fullBaths', 'bedrooms']:
    output[col] = pandas.to_numeric(output[col])

output['lastSaleDate'] = pandas.to_datetime(output['lastSaleDate'], format='%m/%d/%Y')

# change index
output.set_index('accountNo', inplace=True)


#######################################
# Write to a mysql database and table #
#######################################
print('Writing '+str(len(results))+' records to mysql at '+datetime.now().strftime("%m/%d/%Y %H:%M:%S"))

dbName = 'mecklenburgNC'
cnxHost = 'localhost'
cnxPort = 3306
cnxUser = 'elamm'
cnxPasswd = 'guest'

engine = create_engine('mysql+mysqlconnector://'+cnxUser+':'+cnxPasswd+'@'+cnxHost+'/'+dbName)
with engine.connect() as conn, conn.begin():
    output.to_sql('scraped', engine, if_exists='append', chunksize=1000)


print('Finished at '+datetime.now().strftime("%m/%d/%Y %H:%M:%S"))
