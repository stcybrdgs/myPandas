import pandas as pd
from pandas import ExcelWriter
#from pandas import ExcelFile
import numpy as np

'''
import pandas
writer = pandas.ExcelWriter('Masterfile.xlsx')
data_filtered.to_excel(writer, "Main", cols=['Diff1', 'Diff2'])
writer.save()

# -------------------------
import pandas
from openpyxl import load_workbook

book = load_workbook('Masterfile.xlsx')
writer = pandas.ExcelWriter('Masterfile.xlsx', engine='openpyxl')
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
data_filtered.to_excel(writer, "Main", cols=['Diff1', 'Diff2'])
writer.save()

'''
s1 = '1001:fgwukjoipqvujse0xfhybd5h6200,Pallet Rexroth circulation roller bearing NP60 NP60 EC'
s2 = '1002:fgwukjoipqvujse0xfhybd5h6200, FAG Pallet stop arm bearing with RHP or Rexworth MNR R162472220'
s3 = '1003:y1mowwdcd1wwng3pajoxjbff68700,TIMKEN BEARING (WHEEL) Q2019-058 Sheen Spark Q2019-058'
s4 = '1004:fi2r11ncqz3ldgo2gfryducd67104,FYTWK30YTH-* SKF cancelled po RHP, NSK do not use this line cancelled po do not use this line cancelled po do not use this line'
s5 = '1005:0M80-8KDJ93,7216 bep Bearing   N/A N/A or maybe TIMKEN'
s6 = '1005:0M80-8KDJ93,7216 bert Bearing   N/A N/A or maybe TUMKEN'
s7 = 'Gimme some of that SKF !'
strings = [s1, s2, s3, s4, s5, s6, s7]
manuf_list = ['Rexroth','SKF', 'NSK', 'RHP', 'TIMKEN', 'FAG', 'Sheen Spark', 'bep']
w_Manufs = []
w_Manuf_Alts = []
manuf = ''
alts = ''

j = 1
for str in strings:
    i = 0
    for m in manuf_list:
        # case: if there's a manuf in the string
        loc = str.find(m, 0)
        if loc > 0:
            if i == 0:
                # if it's the first manuf in the sentence, put it in w_Manufs
                manuf = m
                i += 1
            else:
                # if it's not the first manuf in the sentence, put it in alts
                if alts == '':
                    alts = m
                else:
                    alts = alts + ', ' + m
        else:
            continue

    w_Manufs.append(manuf.upper())
    w_Manuf_Alts.append(alts.upper())

    # test ---------------
    print('str ', j, 'w_Manufs: ', w_Manufs)
    print('str ', j, 'w_Manuf_Alts: ', w_Manuf_Alts)
    # test ---------------

    manuf = ''
    alts = ''
    j += 1

df = pd.DataFrame({'w_Manuf':w_Manufs,
                   'w_Manuf_Alt':w_Manuf_Alts})

writer = pd.ExcelWriter('demo_pandas_manufs.xlsx')
df.to_excel(writer,'NERS_Manufs', index=False)
writer.save()
