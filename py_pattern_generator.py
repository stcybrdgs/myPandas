#!/usr/bin/env python
# coding: utf8
# Compatible with: spaCy v2.0.0+
'''
Sunday, July 19, 2019
Stacy Bridges

'''
# IMPORT LIBS  =================================
import spacy
import csv
import os, sys, shutil

# FUNCTIONS  ===================================
def import_csv(d):
    global row_heads
    doc = ''
    with open(d, encoding = 'utf-8', mode = 'r', errors = 'ignore') as data:
        csv_reader = csv.reader(data, delimiter='|')
        i = 0
        for row in csv_reader:
            # populate row_heads[]
            #if i > 0:  # skip header row
            # row = row.rstrip(string.punctuation)
            row_head = row[0]
            row_heads.append(row_head)
            # populate txt obj
            doc = doc + ('|'.join(row) + '\n')
            i += 1
    return doc
    # end function //

# MAIN  ========================================
def main():
    # CONFIG  -------------------------------------------------- \\
    # ------------------------------------------------------------ \\

    # set parameters to run either MPN or BRND (only one choice per run)
    dataLabel = 'CMMDTY'  # BRND, MPN, CLSS, CMMDTY


    pattern_generator_inpath = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\test'
    pattern_generator_outpath = r'C:\Users\stacy\Desktop\IESA Project - Europe\NERS_Projects_IESA\tempFiles\test'

    # ------------------------------------------------------------ //
    # ---------------------------------------------------------- //

    if dataLabel == 'MPN':
        dataIn = r'\in_mpn.csv'
        ners_patterns = r'\ners_patterns_mpn.jsonl'
    elif dataLabel == 'BRND':
        dataIn = r'\in_brnd.csv'
        ners_patterns = r'\ners_patterns_brnd.jsonl'
    elif dataLabel == 'SPPLR':
        dataIn = r'\in_spplr.csv'
        ners_patterns = r'\ners_patterns_spplr.jsonl'
    elif dataLabel == 'CMMDTY':
        dataIn = r'\in_cmmdty.csv'
        ners_patterns = r'\cmmdty_ners_patterns.jsonl'

    # parameters
    infile  =  pattern_generator_inpath + dataIn
    outfile =  pattern_generator_outpath + ners_patterns

    # declare containers
    patterns = []
    special_chars = ['/','\\','+','-','.','&',',','(',')']
    doc = ''
    outer_prefix = '{"label":"' + dataLabel + '", "pattern":['
    outer_suffix = ']}'
    inner_prefix = '{"lower":"'
    inner_suffix = '"}'
    inner_delimiter = ','

    # import erp csv
    with open(infile) as data:
        csv_reader = csv.reader(data, delimiter = '|')
        i = 0
        for line in csv_reader:
            if i > 0:  # skip header row
                # replace special characters
                #print('before: {}'.format(line))
                for char in special_chars:
                    if line[0].find(char, 0) >= 0:
                        #print(char)
                        line[0] = line[0].replace(char, ' ')
                #print('after: {}'.format(line))

                #if line[0].find(" ", 0) < 0:
                # define pattern pieces
                # ex: {"label":"BRND", "pattern":[{"lower":"timken"}]}
                # ex: {"label":"BRND", "pattern":[{"lower":"stanley"},{"lower":"power"},{"lower":"tools"}]}
                pattern = outer_prefix + inner_prefix
                tokens = []
                token = ''

                # add the tokens to the pattern
                for char in line[0]:
                    if char != ' ':
                        token = token + char
                    if char == ' ':
                        pattern = pattern + token.lower() + inner_suffix + inner_delimiter + inner_prefix
                        token = ''

                pattern = pattern + token.lower() + inner_suffix + outer_suffix + '\n'

                # detect duplicates and append only unique patterns
                # to the patterns list
                pattern_exists = False
                for p in patterns:
                    if p == pattern:
                        pattern_exists = True
                if not pattern_exists:
                    patterns.append(pattern)

            #else:
                #print('processing complex string: {}'.format(line))
                #{"label":"BRND", "pattern":[{"lower":"stanley"}, {"lower":"tools"}]}
            i += 1

    # create pattern files using jsonl
    # supplier patterns:
    with open(outfile, 'w') as outfile:
        for line in patterns:
            outfile.write(line)
            print(line)

    # clean up goes here...
    # rem open file again to:
    #   replace {"lower":""} with ''
    #   replace ',,' with ','

    # test
    #mkpath = os.path.abspath(__file__)  # = (os.path.abspath('.'))
    #projectsDir = os.path.dirname(mkpath)
    #templateDir = os.path.dirname(projectsDir) + r'\NERS_Template'  # relative path
    #print (mkpath, projectsDir, templateDir)

    # end program
    print('Done.')

if __name__ == '__main__' : main()
