"""
compatible with Python 3+

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
import datetime

# MAIN  ========================================
def main():
    # define imports
    og_file = r'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/Oil and Gas PIN System Summary Dashboard.xlsx'
    sheet_name='PIN Data'
    pandas_file = r'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/pinSys_to_sharePoint.xlsx'

    # perform imports
    data = pd.read_excel(og_file, sheet_name)
    # print (data)  # print a summary table of the xlsx contents
    # print('Col Headers:\n', data.columns)  # print a list of the headers
    # print(data['Region'])  # print all rows within a column as a list

    # take the target og cols and put them into lists
    pinId = data['PinID']  # make unique or leave blank
    risk = data['Risk']  # map to sp c12
    region_pin = data['Region']  # use for sp c2/c3/c5
    company = data['Company'] # concat with region to get sp c5 'Product Line'
    details = data['Details']  # use for sp c8 'Description'
    raisedBy = data['RaisedBy']  # use for sp c15 'Created By'
    occuranceDate = data['OccuranceDate']  # use for sp c9 'Incident Date'
    dollarCost = data['DollarCost']  # use for sp c24 'Cost of Poor Quality (USD)'
    rootCause_pin = data['RootCause'] # use for sp 14 'Root Cause(5 Why's)'
    nonProductiveTime_pin = data['NonProductiveTime']  # use for sp 21 'NonProductiveTime' (precision 0 -> precision 2)
    status = data['Status']  # sp 7 'FormStatus' (see logic below)

    # create target cols fro final data DataFrame
    id = []  # c1: make unique or leave blank (there are dupe ids btw sp and og)
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

    # iterate over the og lists, perform transformations, and load into sp lists
    j = 0
    for i in data.index:
        # ID
        # I assume you will create a unique ID for each record when you import
        # into SharePoint. I noticed there are duplicative IDs between the SharePoint
        # query and the PINSys data, so I for now I simply created a unique ID based on the PinID.
        # The other option was to leave the ID blank.
        nuId = str(pinId[i]) + ':PIN'
        id.append(nuId)  # c1: make unique or leave blank (there are dupe ids btw sp and og)

        # GeoMarket
        geoMarket.append(str(j))

        # Country
        country.append(str(j))

        # Region
        # c4: blank
        region.append('')

        # Product Line
        productLine.append(str(j))

        # IncidentType
        incidentType.append(str(j))

        # FormStatus
        # This field in the SharePoint query has been mapped directly to the
        # field 'Status' in the PINSys data (Closed, Error, For Action, In Progress);
        # observed that some of the Open status may need closure.
        formStatus.append(status[j])

        # Description
        description.append(str(j))

        # IncidentDate
        # his field in the SharePoint query has been mapped directly to the
        # field 'OccuranceDate' in the PINSys data
        # mm/dd/yyyy
        x =occuranceDate[j]
        incidentDate.append(x.strftime('%m/%d/%Y'))

        # EmploymentType
        employmentType.append(str(j))

        # InjuryNature
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed.
        injuryNature.append('')

        # RiskRanking
        riskRanking.append(str(j))

        # RiskRating
        riskRating.append(str(j))

        # Root Cause(5 Why's)
        # This field in the SharePoint query has been mapped directly to the
        # field 'RootCause' in the PINSys data
        rootCause.append(rootCause_pin[j])

        # Created By
        # This field in the SharePoint query has been mapped directly to
        # the field 'Raised By' in the PINSys data
        createdBy.append(raisedBy[j].title())  # ensure all names have title case

        # FormSubmittedBy
        # Did not see a direct mapping between SharePoint query and PINSys data.
        # Can easily do something different if needed
        formSubmittedBy.append('')

        # QHSE Report Workflow
        # For this field, I used the PINSys data field 'Status' by mapping the
        # PIN term 'Closed' to the SharePoint term 'Completed' and by mapping
        # the PIN term'Open' to the SharePoint term 'Waiting for Closed.'
        if status[j] == 'Open':
            qhseReportWorkflow.append('Waiting for Closed')
        elif status[j] == 'Closed':
            qhseReportWorkflow.append('Completed')

        # InjuryLocation
        # Did not see a direct mapping between SharePoint query and PINSys data.
        # Can easily do something different if needed
        injuryLocation.append('')

        # InjuryNatureMechanism
        # Did not see a direct mapping between SharePoint query and PINSys data.
        # Can easily do something different if needed
        injuryNatureMechanism.append('')

        # Primary Root Cause
        # All records for this field are blank in the SharePoint query,
        # so I left them blank for the PINSys Data as well.
        primaryRootCause.append('')

        # NonProductiveTime
        # This field in the SharePoint query has been mapped directly to the
        # field 'NonProductiveTime' in the PINSys data.
        # (Clock hours in 100ths, precision = 2)
        nonProductiveTime.append(nonProductiveTime_pin[j])

        # Test XML
        # All records for this field are blank in the SharePoint query,
        # so I left them blank for the PINSys Data as well.
        testXML.append('')

        # PINType
        pinType.append(str(j))

        # Cost of Poor Quality (USD)
        # This field in the SharePoint query has been mapped directly to the
        # field 'DollarCost' in the PINSys data, with a type conversion from
        # precision 0 to precision 2
        costOfPoorQuality.append(dollarCost[j])

        # Job Number
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed.
        jobNumber.append('')

        # Item Type
        # All records for this field in the SharePoint query say 'Item,' so I
        # continued the pattern with the PINSys Data. No other mapping option was observed.
        itemType.append(str(j))

        # Path
        # All records for this field in the SharePoint query say
        # 'sites/TheRigUp/Lists/IncidentReports'. I did not observe any other
        # mapping option, so I chose to leave this one blank. Can easily do
        # something different if needed.
        path.append('')

        j += 1

    # populate data frame columns
    keys = [
        'ID','GeoMarket','Country','Region','Product Line','IncidentType',
        'FormStatus','Description','IncidentDate','EmploymentType','InjuryNature',
        'RiskRanking','RiskRating','Root Cause(5 Why\'s)','Created By',
        'FormSubmittedBy','QHSE Report Workflow','InjuryLocation',
        'InjuryNatureMechanism','Primary Root Cause','NonProductiveTime',
        'Test XML','PINType','Cost of Poor Quality (USD)','Job Number',
        'Item Type','Path'
    ]
    values = [
        id,geoMarket,country,region,productLine,incidentType,
        formStatus,description,incidentDate,employmentType,injuryNature,
        riskRanking,riskRating,rootCause,createdBy,formSubmittedBy,
        qhseReportWorkflow,injuryLocation,injuryNatureMechanism,
        primaryRootCause,nonProductiveTime,testXML,pinType,
        costOfPoorQuality,jobNumber,itemType,path
    ]

    df_dict = {}
    i = 0
    for k in keys:
        df_dict.update({keys[i]:values[i]})
        i += 1

    df = pd.DataFrame(df_dict)  # create dataframe
    writer = pd.ExcelWriter(pandas_file)  # create excel writer
    df.to_excel(writer, 'Historical PIN Sys Data', index=False)  # convert dataframe to xlswriter excel object
    writer.save()  # close the writer and export the excel file

    print(df_dict)
    print('Done.')

if __name__ == '__main__': main()
