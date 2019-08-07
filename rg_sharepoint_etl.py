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
    # og refers to 'Oil & Gas,' or the PINSys xlsx
    og_file = r'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/Oil and Gas PIN System Summary Dashboard.xlsx'
    sheet_name='PIN Data'  # worksheet name in PINSys xlsx
    pandas_file = r'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/pinSys_to_sharePoint.xlsx'

    # perform imports
    data = pd.read_excel(og_file, sheet_name)

    # take the data from target PINSys columns and put them into lists
    pinId = data['PinID']
    risk = data['Risk']
    region_pin = data['Region']
    company = data['Company']
    details = data['Details']
    raisedBy = data['RaisedBy']
    occuranceDate = data['OccuranceDate']
    dollarCost = data['DollarCost']
    rootCause_pin = data['RootCause']
    nonProductiveTime_pin = data['NonProductiveTime']
    status = data['Status']

    # create arrays to contain column data for output file in the sharepoint format
    id = []
    geoMarket = []
    country = []
    region = []
    productLine = []
    incidentType = []
    formStatus = []
    description = []
    incidentDate = []
    employmentType = []
    injuryNature = []
    riskRanking = []
    riskRating = []
    rootCause = []
    createdBy = []
    formSubmittedBy = []
    qhseReportWorkflow = []
    injuryLocation = []
    injuryNatureMechanism = []
    primaryRootCause = []
    nonProductiveTime = []
    testXML = []
    pinType = []
    costOfPoorQuality = []
    jobNumber = []
    itemType = []
    path = []

    # iterate over the og lists, perform transformations, and load into sp lists
    j = 0
    for i in data.index:
        # ID
        # I assume you will create a unique ID for each record when you import
        # into SharePoint. I noticed there are duplicative IDs between the SharePoint
        # query and the PINSys data, so I for now I simply created a unique ID based on the PinID.
        # The other option was to leave the ID blank.
        nuId = str(pinId[i]) + ':PIN'
        id.append(nuId)

        # GeoMarket
        ogRegion = [
            'Africa', 'Blackburn - UK', 'Brazil', 'Canada',
            'Caribbean', 'Columbia', 'East Australia',
            'Europe, Caspian, Russia', 'Holland, Assen', 'KSA',
            'KSA - Dammam', 'Kuwait', 'Middle East', 'Peru',
            'Singapore', 'South Australia', 'UAE', 'UK, Inverkeithing',
            'USA - General', 'Vietnam', 'West  Australia'
        ]
        spGeoMarket = [
            'Africa', 'Europe - CIS', 'Latin America', 'North America',
            'Latin America', 'Latin America', 'Asia Pacific',
            'Europe - CIS', 'Europe - CIS', 'Middle East', 'Middle East',
            'Middle East', 'Middle East', 'Latin America', 'Asia Pacific',
            'Asia Pacific', 'Middle East', 'Europe - CIS', 'North America',
            'Asia Pacific', 'Asia Pacific'
        ]
        if region_pin[j] != '':
            # find the region string in ogRegion and return the string from the
            # same position in spGeoMarket
            gm_index = ogRegion.index(region_pin[j])
            geoMarket.append(spGeoMarket[gm_index])
        else:
            # if the region is empty, put null in geoMarket
            geoMarket.append('')

        # Country
        spCountry = [
            '', 'UK', 'Brazil', 'Canada', 'Caribbean', 'Colombia', 'Australia',
            '', 'Netherlands', 'Saudi Arabia', 'Saudi Arabia', 'Kuwait', '',
            'Peru', 'Singapore', 'Australia', 'UAE', 'Inverkeithing', 'USA',
            'Vietnam', 'Australia'
        ]
        if region_pin[j] != '':
            # find the region string in ogRegion and return the string from the
            # same position in spGeoMarket
            country_index = ogRegion.index(region_pin[j])
            country.append(spCountry[country_index])
        else:
            # if the region is empty, put null in geoMarket
            country.append('')

        # Region
        # I noticed the Region designation in SharePoint data is at the city
        # level, but in the PINSys data it is not, with the exception of
        # Assen, Holland. As a result, there is no way to consistently map
        # Region from PINSys to SharePoint data. I opted to leave all Region
        # records blank with the exception of Assen. Can easily do something
        # different if needed.
        if region[j] == 'Assen':
            region.append('Assen')
        else:
            region.append('')

        # Product Line
        productLine.append(str(j))

        # IncidentType
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed.
        incidentType.append('')

        # FormStatus
        # This field in the SharePoint query has been mapped directly to the
        # field 'Status' in the PINSys data (Closed, Error, For Action, In Progress);
        # observed that some of the Open status may need closure.
        formStatus.append(status[j])

        # Description
        # This field in the SharePoint query has been mapped directly to the field
        # 'Details' in the PINSys data
        description.append(details[j])

        # IncidentDate
        # his field in the SharePoint query has been mapped directly to the
        # field 'OccuranceDate' in the PINSys data
        # mm/dd/yyyy
        x =occuranceDate[j]
        incidentDate.append(x.strftime('%m/%d/%Y'))

        # EmploymentType
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed.
        employmentType.append('')

        # InjuryNature
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed.
        injuryNature.append('')

        # RiskRanking
        # This field in the SharePoint query has been mapped directly to the
        # field 'Risk' in the PINSys data.
        riskRanking.append(risk[j])

        # RiskRating
        # No equivalent field observed in PINSys data for this SharePoint field.
        # So, based on the existing Risk Rating ranges Low (0-4), Medium (5-9),
        # High (10-15), I interpreted a Risk Rating by using the lowest number
        # of the range for each Risk Rank (ie, Low=0, Medium=5, High=10).
        if risk[j] == 'Low':
            riskRating.append('0')
        elif risk[j] == 'Medium':
            riskRating.append('5')
        elif risk[j] == 'High':
            riskRating.append('10')
        else:
            riskRating.append('')

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
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed.
        pinType.append('')

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
