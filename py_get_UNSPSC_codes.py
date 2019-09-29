"""
Map UNSPSC codes into those records that don't
have a code yet.

"""
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np

# define input files/sheets
map_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\20190820_260_ProjectInsight_MaterialData_MechPT_unspsc_v1.xlsx'
map_sheet = 'UNSPSC Map'
dirty_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\in_description_dirty.xlsx'
dirty_sheet = 'Dirty Description'

# read input files/sheets
map_data = pd.read_excel(map_file, map_sheet)
dirty_data = pd.read_excel(dirty_file, dirty_sheet)

# store col data from input files/sheets
keyword_low = map_data['KEYWORD LOW']
unspsc = map_data['UNSPSC']
dirty_description = dirty_data['dirtyDescription']
fin_hi_keyword = []
fin_low_keyword = []
fin_unspsc = []

# define output file
outfile = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\out_unspsc_mapped.xlsx'

i = 0
print('BEGIN ---------------')
for record in dirty_description:
    if record.find('bearing') >= 0:
        fin_hi_keyword.append('bearing')
        j = 0
        for lowkey in keyword_low:
            if record.find(lowkey) >= 0:
                fin_low_keyword.append(keyword_low[j])
                fin_unspsc.append(unspsc[j])
                print(i, ' : ', j, ' : ', 'bearing', ' : ', lowkey, ' : ', unspsc[j])
                break
            else:
                j += 1
    i += 1
print('END ---------------')
print(len(fin_hi_keyword), len(fin_low_keyword), len(fin_unspsc))
# print to pandas file
df3 = pd.DataFrame({    'hiKeyword':fin_hi_keyword,
                        'lowKeyword':fin_low_keyword,
                        'UNSPSC':fin_unspsc})

writer3 = pd.ExcelWriter(outfile)
df3.to_excel(writer3,'UNSPSC Data', index=False)
writer3.save()

# end program
print('Done.')
