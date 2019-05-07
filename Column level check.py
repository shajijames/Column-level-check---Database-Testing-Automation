# -*- coding: utf-8 -*-
"""
Created on Mon May  6 16:10:48 2019

@author: Shaji
"""

import pandas as pd
import pyodbc

con=pyodbc.connect('Driver={**********};'
                'Server=************;'
                'Database=********;'
                'uid=**********;'
                'pwd=**********')    
cursor=con.cursor()

src_d=  input('Enter source database: ')
src_s=  input('Enter source schema: ')
src_t = input('Enter source table/query: ')
src_t=src_t.lstrip()
if src_t[0:3]=='sel':
    src_t='('+src_t+')'


tgt_d=  input('Enter target database: ')
tgt_s=  input('Enter target schema: ')
tgt_t = input('Enter target table: ')

cursor.execute('use '+tgt_d)

if src_t[1:4]=='sel':
    df_src=pd.read_sql(src_t,con)
    df_src_c=pd.DataFrame(df_src.columns,columns=['COLUMN_NAME'])
else:
    src_c_sql= 'select COLUMN_NAME from information_schema.columns where TABLE_NAME=\''+src_t+'\''+' and TABLE_SCHEMA=\''+src_s+'\''
    df_src_c=pd.read_sql(src_c_sql,con)

tgt_c_sql= 'select COLUMN_NAME from information_schema.columns where TABLE_NAME=\''+tgt_t+'\''+' and TABLE_SCHEMA=\''+tgt_s+'\''

df_tgt_c=pd.read_sql(tgt_c_sql,con)

print('\nSource columns:')
for i in range(len(df_src_c['COLUMN_NAME'])):
    print(str(i+1) +' : '+ df_src_c['COLUMN_NAME'][i])

print('\nTarget columns:')
for i in range(len(df_tgt_c['COLUMN_NAME'])):
    print(str(i+1) +' : '+ df_tgt_c['COLUMN_NAME'][i])

key_list=[]
x='Y'
while x=='Y' or x=='y':
    l=[]
    l.append(int(input('Enter source key column: '))-1)
    l.append(int(input('Enter target key column: '))-1)
    key_list.append(l)
    x=input('Do you want to add more key columns (y/n): ')
else:
    print('Thanks')

key_list[0][0]
  
on=''
for i in range(len(key_list)):
    on+=' a.'+df_src_c['COLUMN_NAME'][key_list[i][0]]+'='+' b.'+df_tgt_c['COLUMN_NAME'][key_list[i][1]]+' and '
on=on[:-5]

src_k=[]
for i in range(len(key_list)):
    src_k.append(key_list[i][0])
    
tgt_k=[]
for i in range(len(key_list)):
    tgt_k.append(key_list[i][1])

print('\nSelect columns to be matched:')
print('\nSource columns:')
for i in range(len(df_src_c['COLUMN_NAME'])):
    if i not in src_k:
        print(str(i+1) +' : '+ df_src_c['COLUMN_NAME'][i])

print('\nTarget columns:')
for i in range(len(df_tgt_c['COLUMN_NAME'])):
    if i not in tgt_k:
        print(str(i+1) +' : '+ df_tgt_c['COLUMN_NAME'][i])

match_list=[]
x='Y'
while x=='Y' or x=='y':
    l=[]
    l.append(int(input('Enter source column: '))-1)
    l.append(int(input('Enter target column to match: '))-1)
    match_list.append(l)
    x=input('Do you want to add more columns (y/n): ')
else:
    print('Thanks')
    
    
if src_t[1:4]=='sel':
    q='select * from '+tgt_d+'.'+tgt_s+'.'+tgt_t +' a left join '+src_t+' b on '+on+' where '
    for i in range(len(match_list)):
        q+=' a.'+df_tgt_c['COLUMN_NAME'][match_list[i][0]]+' != b.'+df_tgt_c['COLUMN_NAME'][match_list[i][1]]+' or '
    q=q[:-4]
else:
    q='select * from '+tgt_d+'.'+tgt_s+'.'+tgt_t +' a left join '+src_d+'.'+src_s+'.'+src_t+' b on '+on+' where '
    for i in range(len(match_list)):
        q+=' a.'+df_tgt_c['COLUMN_NAME'][match_list[i][0]]+' != b.'+df_tgt_c['COLUMN_NAME'][match_list[i][1]]+' or '
    q=q[:-4]

df=pd.read_sql(q,con)

con.commit() 
con.close()

if df.shape[0]==0:
    print('Column level check passed')
else:
    print(str(df.shape[0])+' unmatched records')