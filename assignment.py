#!/usr/bin/env python3
import sys
import os
from datetime import datetime
import pandas as pd

NEW_SEC_FILE = 'new_securities.csv'
NEW_SECDATA_FILE = 'security_data.csv'
START_OF_FIELD = 'START-OF-FIELDS'
END_OF_FIELD = 'END-OF-FIELDS'
START_OF_DATA = 'START-OF-DATA'
END_OF_DATA = 'END-OF-DATA'
IDBBKEYSTR = '﻿id_bb_global'
refsec_df = pd.read_csv('reference_securities.csv', index_col = False)
reffield_df = pd.read_csv('reference_fields.csv', index_col = False)

newdf = pd.DataFrame()
reading_columns = True
start_of_fields = False
start_of_data = False

column_name_stack = []

with open('corp_pfd.dif') as reader:
    for curline in open('corp_pfd.dif'):
        curline = curline.strip()
        if reading_columns:
            if(len(curline)>0):
                if(curline[0]!='#'):
                    if start_of_fields:
                        if curline.upper() == END_OF_FIELD:
                            reading_columns = False
                            newdf = pd.DataFrame(columns=column_name_stack)
                        else:
                            column_name_stack.append(curline)
                    elif curline.upper() == START_OF_FIELD:
                        start_of_fields = True
        else:
            if start_of_data:
                if curline.upper() == END_OF_DATA:
                    start_of_data = False
                else:
                    curlinelist = curline.split('|')
                    newdf.loc[len(newdf)] = curlinelist[0:len(column_name_stack)]
            else:
                if curline.upper() == START_OF_DATA: #start reading data
                    start_of_data = True
columns_drop_stack = []
for (colname, coldata) in newdf.iteritems():
    fieldexists = colname in reffield_df.values
    if fieldexists == False:
        columns_drop_stack.append(colname)

newdf2 = newdf.drop(columns = columns_drop_stack)
newdf2_columns = []
for (colname, coldata) in newdf2.iteritems():
    newdf2_columns.append(colname)

new_securities_df = pd.DataFrame(columns=refsec_df.columns)
new_securities_cols = []
new_sec_col_idx = []
for (colname, coldata) in refsec_df.iteritems():
    uppercolname = colname.upper()
    new_securities_cols.append(uppercolname)
    new_sec_col_idx.append(newdf2.columns.get_loc(uppercolname))


now = datetime.now()
date_time = now.strftime("%Y-%m-%d %H:%M:%S")
security_data_df = pd.DataFrame(columns=['ID_BB_GLOBAL','FIELD','VALUE','SOURCE','TSTAMP'])

for index, row in newdf2.iterrows():
    idbbgstr = row[new_sec_col_idx[0]]
    secexists = idbbgstr in refsec_df.values
    if secexists == False:
        newvalues = []
        for ii in new_sec_col_idx:
            newvalues.append(row[ii])
        new_securities_df.loc[len(new_securities_df)] = newvalues
        cnt = 0
        for r in row:
            secrow = [idbbgstr, newdf2_columns[cnt], r, 'corp_pdf.diff', date_time]
            cnt = cnt + 1
            security_data_df.loc[len(security_data_df)] = secrow

if len(new_securities_df.index) > 0:
    new_securities_df.to_csv(NEW_SEC_FILE, index=False)
if len(security_data_df.index) > 0:
    security_data_df.to_csv(NEW_SECDATA_FILE, index=False)
