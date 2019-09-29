"""
prep the dirty descriptions file by:
- saving description in a single-column xlsx
- sort the column
- delete duplicates
- make sure there are no empty records in dirty descriptions

prep stop words file by:
- make sure there are no empty records in stop brands
- make sure all records are lower case
"""

# IMPORTS  ---------------------------------------------------------
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np

# define input files/sheets
dirty_desc_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\in_description_dirty.xlsx'
dirty_desc_sheet = 'Sheet1'

stop_brands_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\in_stop_brands.xlsx'
stop_brands_sheet = 'Sheet1'

# read input files/sheets
dirty_desc_data = pd.read_excel(dirty_desc_file, dirty_desc_sheet)
stop_brands_data = pd.read_excel(stop_brands_file, stop_brands_sheet)

# store col data from input files/sheets
dirty_desc = dirty_desc_data['dirtyDescription']
stop_brands = stop_brands_data['stopBrands']
fin_description = []

# define output file
output_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\out_description_clean.xlsx'

# remove chars array
stop_chars = [
    '?', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '+', '=',
    '\\', '/', '[', ']', ':', ';', '"', '\'', '<', '>', '|', '.'
    ]
# remove numbers array
stop_nums = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

# remove strings array
stop_strings = ['na','n/a','unknown','tbd','tba','unbranded', 'mm', '(dscntd)']

# clean up the dirty description
j = 0
for item in dirty_desc:
    item = item.lower()  # convert description record to lowercase
    for char in stop_chars:
        item = item.replace(char, '')  # replace special characters
    for num in stop_nums:
        item = item.replace(num, '')  # replace numbers
    for str in stop_strings:
        item = item.replace(str, '')  # replace strings
    #i = 0
    #for brand in stop_brands:
        #print(i, '-', type(brand))
        #brand = brand.strip()
        #item = item.replace(brand, '')  # replace brands
        #i += 1

    item = item.replace('  ', ' ')  # replace double white space
    item = item.replace('  ', ' ') # replace double white space
    item = item.strip()  # strip leading and trailing white space

    fin_description.append(item)  # save the clean description

    print(j)
    j += 1

# test
#for record in fin_description:
#    print(record)

df = pd.DataFrame({'cleanDescription':fin_description})
writer = pd.ExcelWriter(output_file)
df.to_excel(writer,'Clean Description', index=False)
writer.save()

# end program
print('Done.')
