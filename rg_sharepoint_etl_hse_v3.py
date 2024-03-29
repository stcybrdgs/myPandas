"""
08/05/19
WrWx ETL Script
This script is compatible with Python 3+
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
    # define import/export files/worksheets
    # rem hse refers to the 'RG HSE Dashboard' xlsx
    hse_file = r'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/original files from Adam/RG HSE Dashboard.xlsx'
    sheet_name='New Data'  # worksheet name in PINSys xlsx
    pandas_file = r'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/hse_to_sharePoint_output_v3.xlsx'

    # perform imports
    data = pd.read_excel(hse_file, sheet_name)

    # take the data from target HSE columns and put them into lists
    division_hse = data['Division']
    incidentId = data['Incident ID']
    region_hse = data['Region']
    injuryNature_hse = data['Injury Nature']
    incidentType_hse = data['Incident Type']
    incidentDate_hse = data['Incident Date']
    incidentTypeOther = data['Incident Type Other']
    employmentType_hse = data['Employment Type']
    formClosed = data['Form Closed']
    injuryLocation_hse = data['Injury Location']
    injuryMechanism = data['Injury Mechanism']
    riskRanking_hse = data['Risk Ranking']
    riskRating_hse = data['Risk Rating']
    createdBy_hse = data['Created By']
    incidentTypeOther = data['Incident Type Other']
    incidentDescription = data['Incident Description']
    rootCause_hse = data['Root Cause']
    itemType_hse = data['Item Type']
    path_qse = data['Path']

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
    customerSupplier = []

    # iterate over the hse lists, perform transformations, and load into sp lists
    j = 0
    for i in data.index:
        # ----------------------------------------------------------------------
        # ID
        # Mapped directly to 'Incident ID' in the HSE data, with the ID
        # concatenated to an HSE prefix. Can do something different if needed.
        nuId = 'HSE:' + str(incidentId[j])
        id.append(nuId)


        # ----------------------------------------------------------------------
        # GeoMarket
        # Filled in GeoMarket based upon patterns observed in SharePoint data.
        '''
        - Stacy will update the extract tol to include the following mapping in the
        - next extract:
        North America - USA - Edmond
        Middle East - Ira - Erbil
        North America - USA - Grand Junction
        North America - Mexico - Hermosillo
        Europe - CIS - Scotland - Kinellar
        North America - USA - Rosharon
        North America - Mexico - Villa Hermosa
        '''
        hseRegion = [
            'Aberdeen','Abu Dhabi','Adelaide','Algiers','Assen','Astana','Berlin','Bogota',
            'Brisbane','Calgary','Cape Town','Dammam','Denver','Dubai','Edmond','Edmonton',
            'Erbil','Grand Junction','Hermosillo','Houston','Inverkeithing','Kinellar',
            'Kuwait City','Lima','Luanda','Macae','Midland','Perth','Rosharon','Singapore',
            'Stavanger','Villa Hermosa','Vung Tao','Youngstown'
        ]
        spGeoMarket = [
            'Europe - CIS','Middle East','Asia Pacific','Africa','Europe - CIS',
            'Europe - CIS','Europe - CIS','Latin America','Australia','North America',
            'Africa','Middle East','North America','Middle East','North America','North America',
            'Middle East','North America','North America','North America','Europe - CIS','Europe - CIS',
            'Middle East','Latin America','Africa','Latin America','North America','Asia Pacific','North America',
            'Asia Pacific','Europe - CIS','North America','Asia Pacific','North America'
        ]
        if region_hse[j] != '':
            # find the region string in hseRegion and return the string from the
            # same position in spGeoMarket
            gm_index = hseRegion.index(region_hse[j])
            geoMarket.append(spGeoMarket[gm_index])
        else:
            # if the region is empty, put null in geoMarket
            geoMarket.append('')


        # ----------------------------------------------------------------------
        # Country
        # Filled in Country based upon patterns observed in SharePoint data.
        spCountry = [
            'Scotland','UAE','Australia','Algeria','Netherlands','Kazakhstan',
            'Germany','Colombia','Queenssland','Canada','South Africa',
            'Saudi Arabia','USA','UAE','USA','Canada','Iraq','USA','Mexico','USA','Scotland','Scotland',
            'Kuwait','Peru','South Africa','Brazil','USA','Australia','USA','Singapore',
            'Norway','Mexico','Vietnam','USA'
        ]
        if region_hse[j] != '':
            # find the region string in hseRegion and return the string from the
            # same position in spGeoMarket
            country_index = hseRegion.index(region_hse[j])
            country.append(spCountry[country_index])
        else:
            # if the region is empty, put null in geoMarket
            country.append('')

        # ----------------------------------------------------------------------
        # Region
        # Mapped directly to 'Region' in the HSE data.
        region.append(region_hse[j])

        # ----------------------------------------------------------------------
        # Product Line
        # No direct mapping observed, so left this field blank. Can do something different if needed.
        # UPDATE 10/17/2019
        #productLine.append('')
        '''
        Stacy update extract with the following mapping:
        TGD = Surface Logging
        Omega = Well Monitoring
        XDT = Drilling Tools
        MSI = Surface Logging
        Empirica = Surface Logging
        Coring = Coring / Corpro = Coring
        Welltools = Well Intervention
        Wellvention = Well Intervention

        '''
        if division_hse[j] == 'TGD': productLine.append('Surface Logging')
        elif division_hse[j] == 'Omega': productLine.append('Well Monitoring')
        elif division_hse[j] == 'XDT': productLine.append('Drilling Tools')
        elif division_hse[j] == 'MSI': productLine.append('Surface Logging')
        elif division_hse[j] == 'Empirica': productLine.append('Surface Logging')
        elif division_hse[j] == 'Coring': productLine.append('Coring')
        elif division_hse[j] == 'Corpro': productLine.append('Coring')
        elif division_hse[j] == 'Welltools': productLine.append('Well Intervention')
        elif division_hse[j] == 'Wellvention': productLine.append('Well Intervention')

        # ----------------------------------------------------------------------
        # IncidentType
        # Mapped directly to 'Incident Type' in HSE data, but if the type was
        # 'Other (specify)', then used the value in 'Incident Type Other' instead.
        if incidentType_hse[j] == 'Other (specify)':
            incidentType.append(incidentTypeOther[j])
        elif incidentType_hse[j] == 'Hazard Report':
            incidentType.append('Observation Card')
        else:
            incidentType.append(incidentType_hse[j])

        # ----------------------------------------------------------------------
        # FormStatus
        # Mapped this field to 'Form Closed' in HSE data, such that HSE 'True' = 'Closed' and HSE 'False' = 'In Progress'
        if formClosed[j] == True:
            formStatus.append('Closed')
        elif formClosed[j] == False:
            formStatus.append('In Progress')
        else:
            formStatus.append('')

        # ----------------------------------------------------------------------
        # Description
        # Mapped directly to 'Incident Description' in the HSE data.
        description.append(incidentDescription[j])

        # ----------------------------------------------------------------------
        # IncidentDate
        # Mapped directly to 'Incident Date' in HSE data.
        # mm/dd/yyyy
        x = incidentDate_hse[j]
        incidentDate.append(x.strftime('%m/%d/%Y'))

        # ----------------------------------------------------------------------
        # EmploymentType
        # Mapped directly to 'Employment Type' in the HSE data.
        employmentType.append(employmentType_hse[j])

        # ----------------------------------------------------------------------
        # InjuryNature
        # Mapped directly to 'Injury Nature' in the HSE data.
        injuryNature.append(injuryNature_hse[j])

        # ----------------------------------------------------------------------
        # RiskRanking
        # Mapped directly to 'Risk Ranking' in the HSE data.
        riskRanking.append(riskRanking_hse[j])

        # ----------------------------------------------------------------------
        # RiskRating
        # Mapped directly to 'Risk Rating' in the HSE data.
        riskRating.append(riskRating_hse[j])

        # ----------------------------------------------------------------------
        # Root Cause(5 Why's)
        # Mapped directly to 'Root Cause' in the HSE data.
        rootCause.append(rootCause_hse[j])

        # ----------------------------------------------------------------------
        # Created By
        # Mapped directly to 'Created By' in the HSE data.
        createdBy.append(createdBy_hse[j].title())  # ensure all names have title case

        # ----------------------------------------------------------------------
        # FormSubmittedBy
        # Did not see a direct mapping between SharePoint query and HSE data,
        # so left this field blank. Can easily do something different if needed.
        formSubmittedBy.append('')

        # ----------------------------------------------------------------------
        # QHSE Report Workflow
        # Did not see a direct mapping between SharePoint query and HSE data, and
        # could not determine a value based upon the action steps provided in HSE data.
        # As a result, left this field blank. Can easily do something different if needed.
        qhseReportWorkflow.append('')

        # ----------------------------------------------------------------------
        # InjuryLocation
        # Mapped directly to 'Injury Location' in the HSE data.
        injuryLocation.append(injuryLocation_hse[j])

        # ----------------------------------------------------------------------
        # InjuryNatureMechanism
        # Mapped directly to 'Injury Mechanism' in the HSE data.
        injuryNatureMechanism.append(injuryMechanism[j])

        # ----------------------------------------------------------------------
        # Primary Root Cause
        # All records for this field are blank in the SharePoint query,
        # so I left them blank for the HSE Data as well. Can do something else
        # if needed. (Perhaps 'Nature of Hazard or Observation' might be a fit?).
        #primaryRootCause.append('')
        # UPDATE 10/17/2019
        # this field is OPEN TEXT in SharePoint,
        # so the HSE 'Root Cause' field == SharePoint 'Primary Root Cause' field
        # ie., primaryRootCause == rootCause_hse
        primaryRootCause.append(rootCause_hse[j])

        # ----------------------------------------------------------------------
        # NonProductiveTime
        # Did not see a direct mapping between SharePoint query and HSE data,
        # so left this field blank. Can easily do something different if needed.
        nonProductiveTime.append('')

        # ----------------------------------------------------------------------
        # Test XML
        # All records for this field are blank in the SharePoint query,
        # so I left them blank for the HSE data as well.
        testXML.append('')

        # ----------------------------------------------------------------------
        # PINType
        # Did not see a direct mapping between SharePoint query and HSE data,
        # so left this field blank. Can easily do something different if needed.
        pinType.append('')

        # ----------------------------------------------------------------------
        # Cost of Poor Quality (USD)
        # Did not see a direct mapping between SharePoint query and HSE data,
        # so left this field blank. Can easily do something different if needed.
        costOfPoorQuality.append('')

        # ----------------------------------------------------------------------
        # Job Number
        # Did not see a direct mapping between SharePoint query and HSE data,
        # so left this field blank. Can easily do something different if needed.
        jobNumber.append('')

        # ----------------------------------------------------------------------
        # Item Type
        # Mapped directly to 'Item' in the HSE data.
        itemType.append(itemType_hse[j])

        # ----------------------------------------------------------------------
        # Path
        # Mapped directly to 'Path' in the HSE data.
        path.append(path_qse[j])

        # ----------------------------------------------------------------------
        # Customer/Supplier
        # all blank for HSE data
        customerSupplier.append('')

        j += 1

    # populate pandas data frame columns
    keys = [
        'ID','GeoMarket','Country','Region','Product Line','IncidentType',
        'FormStatus','Description','IncidentDate','EmploymentType','InjuryNature',
        'RiskRanking','RiskRating','Root Cause(5 Why\'s)','Created By',
        'FormSubmittedBy','QHSE Report Workflow','InjuryLocation',
        'InjuryNatureMechanism','Primary Root Cause','NonProductiveTime',
        'Test XML','PINType','Cost of Poor Quality (USD)','Job Number',
        'Item Type','Path', 'Customer/Supplier'
    ]
    values = [
        id,geoMarket,country,region,productLine,incidentType,
        formStatus,description,incidentDate,employmentType,injuryNature,
        riskRanking,riskRating,rootCause,createdBy,formSubmittedBy,
        qhseReportWorkflow,injuryLocation,injuryNatureMechanism,
        primaryRootCause,nonProductiveTime,testXML,pinType,
        costOfPoorQuality,jobNumber,itemType,path,customerSupplier
    ]

    df_dict = {}
    i = 0
    for k in keys:
        df_dict.update({keys[i]:values[i]})
        i += 1

    df = pd.DataFrame(df_dict)  # create dataframe
    writer = pd.ExcelWriter(pandas_file)  # create excel writer
    df.to_excel(writer, 'Historical HSE Sys Data', index=False)  # convert dataframe to xlswriter excel object
    writer.save()  # close the writer and export the excel file

    #print(df_dict)
    print('Done.')

if __name__ == '__main__': main()
