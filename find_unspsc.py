# find unspsc
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
desc_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\FluidPower UNSPSC\in_description.xlsx'
kw_file = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\FluidPower UNSPSC\in_keyword_map.xlsx'

desc_sheet = 'Sheet1'
kw_sheet = 'Sheet1'

# read input files/sheets
key_data = pd.read_excel(kw_file, kw_sheet)
desc_data = pd.read_excel(desc_file, desc_sheet)

# store col data from input files/sheets
description = desc_data['description']
words1 = key_data['words1']
words2 = key_data['words2']
codes = key_data['codes']
fin_words = []
fin_codes = []

# define output file
outfile = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\FluidPower UNSPSC\out_codes.xlsx'

i = 0
match_count = 0
for row in description:
    row=str(row)
    j = 0
    fin_word = ''
    fin_code = ''
    match = False
    for word in words1:
        loc1 = row.find(words1[j])
        loc2 = row.find(words2[j])
        if loc1 < 0:
            # case: no match to first key word
            #       so continue the loop
            j += 1
            continue
        if loc1 > -1:
            if words2[j] == 'ignore':
                # case: only one word needs to match and does
                #       so print unspsc record
                fin_word = words1[j]
                fin_code = codes[j]
                match = True
            elif loc2 > -1:
                # case: both words need to match and do
                #       so print unspsc record
                fin_word = words1[j] + ' ' + words2[j]
                fin_code = codes[j]
                match = True
        if match == True:
            fin_words.append(fin_word)
            fin_codes.append(fin_code)
            match_count += 1
            print('{}  |  {} : {}'.format(match_count, fin_word, fin_code))
            j += 1
            break
        j += 1
    if match == False:
        # if no match is found, print an empty record
        fin_words.append('')
        fin_codes.append('')
    i += 1

df = pd.DataFrame({ 'UNSPSC Title':fin_words,
                    'UNSPSC Code':fin_codes})
writer = pd.ExcelWriter(outfile)
df.to_excel(writer,'NERS_Cmmdtys', index=False)
writer.save()

# end program
print('{} records, {} UNSPSC Codes matched'.format(i, match_count))
print('Done.')
