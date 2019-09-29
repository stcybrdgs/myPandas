"""
After pulling down the most recent project file,
join map the UNSPSC codes to the full project file so that
you can copy the data into the full project file.
Afterward, you can map UNSPSC into those records that don't
have a code yet.
 
"""
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np

# define input files/sheets
new_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\new_data.xlsx'
old_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\20190820_260_ProjectInsight_MaterialData_MechPT_unspsc_v1.xlsx'

new_sheet = 'MechPT data'
old_sheet = 'data'

# read input files/sheets
new_data = pd.read_excel(new_file, new_sheet)
old_data = pd.read_excel(old_file, old_sheet)

# store col data from input files/sheets
new_cid = new_data['CI_ID']
new_comCode = new_data['UNSPSC']
old_cid = old_data['CI_ID']
old_comCode = old_data['comCode']
fin_cid = []
fin_comCode = []

# define output file
outfile = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\out_unspsc_codes.xlsx'

i = 0
print('BEGIN ---------------')
for old_id in old_cid:
    old_id = str(old_id)
    j = 0
    match = False
    for new_id in new_cid:
        new_id = str(new_id)
        if old_id == new_id:
            fin_cid.append(new_cid[j])
            fin_comCode.append(new_comCode[j])
            match = True
            print(i, "- MATCH")
            break
        j += 1
    if match == False:
        fin_cid.append(old_cid[i])
        fin_comCode.append('miss')
        print(i, "- ...")
    i += 1

# test
i = 0
for item in fin_cid:
    print(str(fin_comCode[i]))
    i += 1
print('END ---------------')

# print to pandas file
df3 = pd.DataFrame({ 'CI_IC':fin_cid,
                    'UNSPSC':fin_comCode})

writer3 = pd.ExcelWriter(outfile)
df3.to_excel(writer3,'UNSPSC Data', index=False)
writer3.save()

# compare lengths of input col to length of output col
#print(len(new_cid), len(old_cid), len(fin_cid))

# end program
print('Done.')
