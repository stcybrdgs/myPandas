"""
08/05/2019
Stacy Bridges

rg_sharepoint_etl.py

import cols from OG:
'Risk'  # map to sp c12
'Region'  # use for sp c2/c3/c5
'Company'  # concat with region to get sp c5 'Product Line'
'Details'  # use for sp c8 'Description'
'RaisedBy'  # use for sp c15 'Created By'
'OccuranceDate'  # use for sp c9 'Incident Date'
'RootCauseName'  # use for sp 14 'Root Cause(5 Why's)'
'NonProductiveTime'  # use for sp 21 'NonProductiveTime' (precision 0 -> precision 2)
'Status'  # sp 7 'FormStatus' (see logic below)

export arrays:
id = []  # c1: leave blank (there are dupe ids btw sp and og)
geoMarket = []  # c2: use lookups from ps geomarket worksheet
country  = []  # c3: use lookups from ps geomarket worksheet
region = []  # c4: blank
productLine = []  # c5: concat from ogcompany (6): ogregion (3) (may affect dashboard)
incidentType = []  # c6: blank
formStatus = []  # c7: direct map to OG71, should some OPENS be closed (now that they are old)?
description = []  # c8: og8 Details
incidentDate = []  # c9: see date overlaps in notes sheet
employmentType = []  # c10: no direct mapping / leave blank
injuryNature = []  # c11: no direct mapping / leave blank
riskRanking = []  # c12: og 2 Risk maps to SP 12
riskRating = []  # c13: if riskRanking is L, M, H and if L=0-4, M=6-9, H=10-15, how to determine #? (for now, use min # in each rank cat)
rootCause = []  # c14: use og 58 (note: not all descriptions in og line up with descriptions in sp)
createdBy = []  # c15: og 10 raised by
formSubmittedBy = []  # c16: blank
qhseReportWorkflow = []  # c17: use og 7:
                        # og Closed = sp Completed
                        # og In progress = sp In Progress
                        # og Error, For Action = sp Waiting For closed
injuryLocation = []  # c18: no direct mapping, leave blank
injuryNatureMechanism = []  # c19: no direct mapping, leave blank
primaryRootCause = []  # c20: all values null, no mapping
nonProductiveTime = []  # c21: clock hours in 60/100 format, precision = 2
testXML = []  # c22: blank
pinType = []  # c23: an issue type / see sheet PS PINType ; no direct mapping
costOfPoorQuality = []  # c24: dollar cost / float value, precision = 2 / use OG 18 or 19 or leave blank?
jobNumber = []  # c25: no direct mapping --> leave blank
itemType = []  # c26: all fields say 'item' (leave blank or fill in?) / no mapping
path = []  # c27: all fields say 'sites/TheRigUp/Lists/IncidentReports' (leave blank or fill in?) / no mapping

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
    # define imports
    og_file = r'C:\Users\stacy\My WrWx\00_projects\reservoirGroup\Adam\Oil and Gas PIN System Summary Dashboard.xlsx'
    sheet_name='PIN Data'

    # perform imports
    data = pd.read_excel(og_file, sheet_name)
    p# rint (data)  # print a summary table of the xlsx contents
    # print('Col Headers:\n', data.columns)  # print a list of the headers
    # print(data['Region'])  # print all rows within a column as a list

    # take the target og cols and put them into lists
    risk = data['Risk']  # map to sp c12
    region = data['Region']  # use for sp c2/c3/c5
    company = data['Company'] # concat with region to get sp c5 'Product Line'
    details = data['Details']  # use for sp c8 'Description'
    reaisedBy = data['RaisedBy']  # use for sp c15 'Created By'
    occuranceDate = data['OccuranceDate']  # use for sp c9 'Incident Date'
    rootCauseName = data['RootCauseName'] # use for sp 14 'Root Cause(5 Why's)'
    nonProductiveTime = data['NonProductiveTime']  # use for sp 21 'NonProductiveTime' (precision 0 -> precision 2)
    status = data['Status']  # sp 7 'FormStatus' (see logic below)

    # create target cols fro final data DataFrame
    id = []  # c1: leave blank (there are dupe ids btw sp and og)
    geoMarket = []  # c2: use lookups from ps geomarket worksheet
    country  = []  # c3: use lookups from ps geomarket worksheet
    region = []  # c4: blank
    productLine = []  # c5: concat from ogcompany (6): ogregion (3) (may affect dashboard)
    incidentType = []  # c6: blank
    formStatus = []  # c7: direct map to OG71, should some OPENS be closed (now that they are old)?
    description = []  # c8: og8 Details
    incidentDate = []  # c9: see date overlaps in notes sheet
    employmentType = []  # c10: no direct mapping / leave blank
    injuryNature = []  # c11: no direct mapping / leave blank
    riskRanking = []  # c12: og 2 Risk maps to SP 12
    riskRating = []  # c13: if riskRanking is L, M, H and if L=0-4, M=6-9, H=10-15, how to determine #? (for now, use min # in each rank cat)
    rootCause = []  # c14: use og 58 (note: not all descriptions in og line up with descriptions in sp)
    createdBy = []  # c15: og 10 raised by
    formSubmittedBy = []  # c16: blank
    qhseReportWorkflow = []  # c17: use og 7:
                             # og Closed = sp Completed
                             # og In progress = sp In Progress
                             # og Error, For Action = sp Waiting For closed
    injuryLocation = []  # c18: no direct mapping, leave blank
    injuryNatureMechanism = []  # c19: no direct mapping, leave blank
    primaryRootCause = []  # c20: all values null, no mapping
    nonProductiveTime = []  # c21: clock hours in 60/100 format, precision = 2
    testXML = []  # c22: blank
    pinType = []  # c23: an issue type / see sheet PS PINType ; no direct mapping
    costOfPoorQuality = []  # c24: dollar cost / float value, precision = 2 / use OG 18 or 19 or leave blank?
    jobNumber = []  # c25: no direct mapping --> leave blank
    itemType = []  # c26: all fields say 'item' (leave blank or fill in?) / no mapping
    path = []  # c27: all fields say 'sites/TheRigUp/Lists/IncidentReports' (leave blank or fill in?) / no mapping

    # iterate over the region list from above using a loop
    for i in data.index:
        print(data['Region'][i])

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
