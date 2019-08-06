"""
08/05/2019
Stacy Bridges

rg_sharepoint_etl.py

"""
# IMPORTS  =====================================
import json
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np

# FUNCTIONS  ===================================

# MAIN  ========================================
def main():

    # import xls


    # case logic
    '''
    1	ID
    2	GeoMarket
    3	Country
    4	Region
    5	Product Line
    6	IncidentType
    7	FormStatus
    8	Description
    9	IncidentDate
    10	EmploymentType
    11	InjuryNature
    12	RiskRanking
    13	RiskRating
    14	Root Cause(5 Why's)
    15	Created By
    16	FormSubmittedBy
    17	QHSE Report Workflow
    18	InjuryLocation
    19	InjuryNatureMechanism
    20	Primary Root Cause
    21	NonProductiveTime
    22	Test XML
    23	PINType
    24	Cost of Poor Quality (USD)
    25	Job Number
    26	Item Type
    27	Path
    '''

    # import xls file
    data = pd.read_excel(r'C:\Users\stacy\My WrWx\00_projects\reservoirGroup\Adam\Oil and Gas PIN System Summary Dashboard.xlsx', sheet_name='PIN Data')
    print (data)  # print a summary table of the xlsx contents
    print('Col Headers:\n', data.columns)  # print a list of the headers
    print(data['Region'])  # print all rows within a column as a list

    # iterate over the region list from above using a loop
    for i in data.index:
        print(data['Region'][i])


    # take entire columns from the sheet and put into lists
    region = data['Region']
    company = data['Company']
    raisedBy = data['RaisedBy']

    nu_region = []
    nu_company = []
    nu_raisedBy = []

    i = 0
    for item in region:
        if i == 10:
            nu_region.append('Bananas')
            nu_company.append('Apples')
            nu_raisedBy.append ('ORanges')
        else:
            nu_region.append(region[i])
            nu_company.append(company[i])
            nu_raisedBy.append(raisedBy[i])
        i += 1

    # show a subtable of the imported excel file
    df = pd.DataFrame(data, columns = ['PinID',	'Risk',	'Region', 'Company'])
    print(df)

    # pandas output
    pandas_file = 'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/pandas_test.xlsx'
    pandas_file_2 = 'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/pandas_test_apples.xlsx'
    pandas_file_3 = 'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/historical_pin_hse.xlsx'

    writer = pd.ExcelWriter(pandas_file)
    df.to_excel(writer,'PIN_Data', index=False)
    writer.save()

    df2 = pd.DataFrame({ 'Region':nu_region, 'Company':nu_company, 'RaisedBy':nu_raisedBy})
    writer2 = pd.ExcelWriter(pandas_file_2)
    df2.to_excel(writer2, 'Historical PIN and HSE', index=False)
    writer2.save()




    print('Done.')


if __name__ == '__main__': main()
