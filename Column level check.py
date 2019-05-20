# -*- coding: utf-8 -*-
"""
Created on Mon May  6 16:10:48 2019

@author: Shaji
"""

import pandas as pd
import pyodbc

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

con=pyodbc.connect('Driver={****};'
                'Server=****;'
                'Database=****;'
                'uid=****;'
                'pwd=****')    
cursor=con.cursor()

src_d=  input('Enter source database: ')
src_s=  input('Enter source schema: ')
src_t = input('Enter source table/query: ')  

src_t=str.lower(src_t.lstrip())
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

ma_list=[]    
au=input('\nDo you want to proceed with auto-match (y/n): ')
if au=='Y' or au=='y':
    for i in range(len(df_tgt_c['COLUMN_NAME'])):
        if i not in tgt_k:
            for j in range(len(df_src_c['COLUMN_NAME'])):
                if i not in src_k:
                    if str.lower(df_tgt_c['COLUMN_NAME'][i])==str.lower(df_src_c['COLUMN_NAME'][j]):
                        ma_list.append([j,i])                        
    for i in range(len(ma_list)):
        print(str(i+1)+': '+df_tgt_c['COLUMN_NAME'][ma_list[i][1]]+' = '+df_src_c['COLUMN_NAME'][ma_list[i][0]])
    
    dr=input('\nDo you want to drop any match (y/n): ')
    if dr=='Y' or dr=='y':
        dr_s=input('\nEnter the matches to be dropped separated by comma: ')
        dr_l=[int(i)-1 for i in dr_s.split(',')]
        for i in sorted(dr_l, reverse=True):
            del ma_list[i]
else:
    print('')
        
for i in range(len(ma_list)):
    src_k.append(ma_list[i][0])
for i in range(len(ma_list)):
    tgt_k.append(ma_list[i][1])

print('\nColumns in target that are left to be matched:')    
for i in range(len(df_tgt_c['COLUMN_NAME'])):
    if i not in tgt_k:
        print('\t'+df_tgt_c['COLUMN_NAME'][i])

ma=input('\nDo you want to proceed with manual-match (y/n): ')
if ma=='Y' or ma=='y':
    print('\nSelect columns to be matched:')
    print('\nSource columns:')
    for i in range(len(df_src_c['COLUMN_NAME'])):
        if i not in src_k:
            print(str(i+1) +' : '+ df_src_c['COLUMN_NAME'][i])
    
    print('\nTarget columns:')
    for i in range(len(df_tgt_c['COLUMN_NAME'])):
        if i not in tgt_k:
            print(str(i+1) +' : '+ df_tgt_c['COLUMN_NAME'][i])
    
    x='Y'
    while x=='Y' or x=='y':
        l=[]
        l.append(int(input('Enter source column: '))-1)
        l.append(int(input('Enter target column to match: '))-1)
        ma_list.append(l)
        x=input('Do you want to add more columns (y/n): ')
    else:
        print('Thanks')
    
    
if str.lower(src_t[1:4])=='sel':
    q='select * from '+tgt_d+'.'+tgt_s+'.'+tgt_t +' a left join '+src_t+' b on '+on+' where '
    for i in range(len(ma_list)):
        q+=' a.'+df_tgt_c['COLUMN_NAME'][ma_list[i][1]]+' != b.'+df_src_c['COLUMN_NAME'][ma_list[i][0]]+' or '
    q=q[:-4]
else:
    q='select * from '+tgt_d+'.'+tgt_s+'.'+tgt_t +' a left join '+src_d+'.'+src_s+'.'+src_t+' b on '+on+' where '
    for i in range(len(ma_list)):
        q+=' a.'+df_tgt_c['COLUMN_NAME'][ma_list[i][1]]+' != b.'+df_src_c['COLUMN_NAME'][ma_list[i][0]]+' or '
    q=q[:-4]

df=pd.read_sql(q,con)

con.commit() 
con.close()

from datetime import datetime
time=datetime.now()
file='CLC_'+tgt_t+'_'+time.strftime("%j")+time.strftime("%Y")+time.strftime("%H")+time.strftime("%M")+time.strftime("%S")+'.csv'
file_path='C:\\'+file

log=str(time)+'\n \n'+'Source: '+src_t+'\n \n'+'Target: '+tgt_d+'.'+tgt_s+'.'+tgt_t+'\n \n'+'Test query: \n'+q

log_path='C:\\log_'+tgt_t+'_'+time.strftime("%j")+time.strftime("%Y")+time.strftime("%H")+time.strftime("%M")+time.strftime("%S")+'.txt'
f=open(log_path,'w')
f.write(log)
f.close()
print('Log file saved in '+log_path)

if df.shape[0]==0:
    print('Column level check passed')
else:
    print(str(df.shape[0])+' unmatched records')
    out=input('Do you want the unmatched records to be stored locally (y/n): ')
    if out=='y' or out=='Y':
        df.to_csv(file_path)
        print('File saved successfully '+file_path)
    else:
        print('')
