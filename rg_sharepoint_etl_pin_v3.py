"""
08/05/19
WrWx ETL Script
This script is compatible with Python 3+
rg_sharepoint_etl.py

# TEST
sp_list_names = [
    id,
    geoMarket,
    country,
    region,
    productLine,
    incidentType,
    formStatus,
    description,
    incidentDate,
    employmentType,
    injuryNature,
    riskRanking,
    riskRating,
    rootCause,
    createdBy,
    formSubmittedBy,
    qhseReportWorkflow,
    injuryLocation,
    injuryNatureMechanism,
    primaryRootCause,
    nonProductiveTime,
    testXML,
    pinType,
    costOfPoorQuality,
    jobNumber,
    itemType,
    path,
    customerSupplier
]
print('LIST LENGTHS:')
for name in sp_list_names:
    print(len(name))

"""
# IMPORTS  =====================================
import json
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import datetime
import numpy as np


# FUNCTIONS  ===================================
def is_nan(x):
    return (x is np.nan or x != x)


# MAIN  ========================================
def main():
    # define import/export files/worksheets
    # rem og refers to 'Oil & Gas,' or the PINSys xlsx
    og_file = r'C:/Users/stacy/My WrWx/00_projects/reservoirGroup/Adam/original files from Adam/Oil and Gas PIN System Summary Dashboard.xlsx'
    sheet_name='PIN Data'  # worksheet name in PINSys xlsx
    pandas_file = r'C:\Users\stacy\My WrWx\00_projects\reservoirGroup\Adam\pinSys_to_sharePoint_output_v3.xlsx'

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
    rootCauseName_pin = data['RootCauseName']
    nonProductiveTime_pin = data['NonProductiveTime']
    status = data['Status']
    customer = data['Customer']
    supplierName = data['SupplierName']
    auditInspection_pin = data['AuditInspection']
    customerComplaint_pin = data['CustomerComplaint']
    logistics_pin = data['Logistics']
    operations_pin = data['Operations']
    qaqc_pin = data['QAQC']
    supplierVendor_pin = data['SupplierVendor']

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

    # define containers needed for mapping
    pinSys_co_tag = [
        'PS - Well Monitoring',
        'RC - Surface Logging',
        'RC- Reservoir Laboratories',
        'DS - Coring',
        'DS - Drilling Tools',
        'PS - Well Intervention Products',
        'PS - Well Intervention Services'
    ]
    sp_pl_tag = [
        'Well Monitoring',
        'Surface Logging',
        'Reservoir Group',
        'Coring',
        'Drilling Tools',
        'Well Intervention',
        'Well Intervention'
    ]
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
    spCountry = [
        'South Africa', 'Scotland', 'Brazil', 'Canada', 'Caribbean', 'Colombia', 'Australia',
        'Scotland', 'Netherlands', 'Saudi Arabia', 'Saudi Arabia', 'Kuwait', 'UAE',
        'Peru', 'Thailand', 'Australia', 'UAE', 'Scotland', 'USA',
        'Vietnam', 'Australia'
    ]
    spRegion = [
        'Durbanville',
        'Aberdeen',
        'Macae',
        'Calgary',
        'Trinidad',
        'Cota',
        'Roma',
        'Whitecarns',
        'Assen',
        'Al Khobar',
        'Dammam',
        'Kuwait City',
        'Dubai',
        'Lima',
        'Songkhla',
        'Forrestfield',
        'Abu Dhabi',
        'Inverkeithing',
        'Houston',
        'Vung Tau',
        'Forrestfield',
    ]

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
        if region_pin[j] != '':
            # find the region string in ogRegion and return the string from the
            # same position in spGeoMarket
            gm_index = ogRegion.index(region_pin[j])
            geoMarket.append(spGeoMarket[gm_index])
        else:
            # if the region is empty, put null in geoMarket
            geoMarket.append('')

        # Country
        if region_pin[j] != '':
            # find the region string in ogRegion and return the string from the
            # same position in spGeoMarket
            country_index = ogRegion.index(region_pin[j])
            country.append(spCountry[country_index])
        else:
            # if the region is empty, put null in country
            country.append('')

        # Region
        # I noticed the Region designation in SharePoint data is at the city
        # level, but in the PINSys data it is not, with the exception of
        # Assen, Holland. As a result, there is no way to consistently map
        # Region from PINSys to SharePoint data. I opted to go ahead and use the
        # PINSys Region designation despite the inconsistency because it can
        # be easily removed if it causes an issue.
        #region.append(region_pin[j])
        # based on updated mapping information:
        if region_pin[j] != '':
            # find the region string in ogRegion and return the string from the
            # same position in spGeoMarket
            region_index = ogRegion.index(region_pin[j])
            region.append(spRegion[region_index])
        else:
            # if the region is empty, put null in region
            region.append('')

        # Product Line
        # As with Region, there is no way to consistently map PINSys data to
        # SharePoint data because SharePoint designation for Region is at the
        # city level and PINSys data is not. For Product Line, I opted to concatenate
        # PINSys Region and Company despite the inconsistency, again feeling
        # that the records can be easily removed if they cause an  issue due to
        # the inconsistency.
        '''
        trunc_company = ''
        if company[j] == 'RC- Reservoir Laboratories':
            trunc_company = company[j][4:len(company[j])]
        else:
            trunc_company = company[j][5:len(company[j])]
        pl_string = trunc_company + '-' + region[j]
        productLine.append(pl_string)
        '''
        # code updated to reflect updated mapping per method:
        #   get PINSys Region from region[]
        #   get PINSys Company from company[]
        #   pl_val = concatenate region[n] + '-' + company[n]
        #   productLine.append(pl_val)
        pl_val = ''
        sp_pl_val = ''
        pinSys_co_index = 0
        pinSys_co_val = ''

        for item in pinSys_co_tag:
            if item == company[i]:
                pinSys_co_index = pinSys_co_tag.index(company[i])
                sp_pl_val = sp_pl_tag[pinSys_co_index]
                pl_val = sp_pl_val + '-' + region[i]
                continue
        productLine.append(pl_val)

        # IncidentType
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed.
        incidentType.append('PIN (Quality)')

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
            riskRating.append('2')
        elif risk[j] == 'Medium':
            riskRating.append('7')
        elif risk[j] == 'High':
            riskRating.append('12')
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
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed
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
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed
        injuryLocation.append('')

        # InjuryNatureMechanism
        # Did not see a direct mapping between SharePoint query and PINSys data,
        # so left this field blank. Can easily do something different if needed
        injuryNatureMechanism.append('')

        # Primary Root Cause
        # All records for this field are blank in the SharePoint query,
        # so I left them blank for the PINSys Data as well.
        #primaryRootCause.append('')
        # UPDATE 10/17/2019
        # The SharePoint field is Open text, so
        # SharePoint 'Primary Root Cause' == PINSys 'RootCauseName'
        # ie, primaryRootCause == rootCauseName_pin
        primaryRootCause.append(rootCauseName_pin[j])

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
        #pinType.append('')
        # UPDATE 10/17/2019
        '''
        USE THESE PINSYS COLS TO GET MAPPING TO PINType:
        Operations --> operations_pin[j] == 'Yes': pinType.append('Operational Issue')
        QAQC --> qaqc_pin[j] == 'Yes': pinType.append('QC Issue')
        SupplierVendor --> supplierVendor_pin[j] == : pinType.append('Supplier / Vendor Concern')
        CustomerComplaint --> customerComplaint_pin[j] == 'Yes': pinType.append('Customer Complaint')
        AuditInspection --> auditInspection_pin[j] == 'Yes': pinType.append('Audit/Inspection')
        LogisticalIssue --> logistics_pin[j] == 'Yes': pinType.append('Logistical Issue')

        '''
        if operations_pin[j] == 'Yes': pinType.append('Operational Issue')
        elif qaqc_pin[j] == 'Yes': pinType.append('QC Issue')
        elif supplierVendor_pin[j] == 'Yes': pinType.append('Supplier / Vendor Concern')
        elif customerComplaint_pin[j] == 'Yes': pinType.append('Customer Complaint')
        elif auditInspection_pin[j] == 'Yes': pinType.append('Audit/Inspection')
        elif logistics_pin[j] == 'Yes': pinType.append('Logistical Issue')
        else: pinType.append('')

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
        itemType.append('Item')

        # Path
        # All records for this field in the SharePoint query say
        # 'sites/TheRigUp/Lists/IncidentReports'. I did not observe any other
        # mapping option, so I chose to leave this one blank. Can easily do
        # something different if needed.
        path.append('')

        # Customer/Supplier
        # combine Customer field with SupplierName field as Customer/SupplierName
        # get values for Customer and SupplierName
        cust = str(customer[j])
        supp = str(supplierName[j])

        # if value is nan, make value blank
        if is_nan(cust): cust = ''
        if is_nan(supp): supp = ''

        custSupp = cust + '/' + supp
        customerSupplier.append(custSupp)

        j += 1

    # populate pandas data frame columns
    keys = [
        'ID','GeoMarket','Country','Region','Product Line','IncidentType',
        'FormStatus','Description','IncidentDate','EmploymentType','InjuryNature',
        'RiskRanking','RiskRating','Root Cause(5 Why\'s)','Created By',
        'FormSubmittedBy','QHSE Report Workflow','InjuryLocation',
        'InjuryNatureMechanism','Primary Root Cause','NonProductiveTime',
        'Test XML','PINType','Cost of Poor Quality (USD)','Job Number',
        'Item Type','Path','Customer/Supplier'
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
    df.to_excel(writer, 'Historical PIN Sys Data', index=False)  # convert dataframe to xlswriter excel object
    writer.save()  # close the writer and export the excel file

    #print(df_dict)
    print('Done.')

if __name__ == '__main__': main()
