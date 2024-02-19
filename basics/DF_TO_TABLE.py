# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 15:24:49 2023

@author: 2076284
"""

import pandas as pd
import cx_Oracle
#cx_Oracle.init_oracle_client(lib_dir= r"C:\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7")

DATABASE =  'oridb-wc-stga.sys.comcast.net'
PORT = 1555
SID = 'ORIONSTGA'
SCHEMA = 'DA_USER'
PASSWORD = 'DA_Prod123'

connstr = f"{DATABASE}:{PORT}/{SID}"
con = cx_Oracle.connect(SCHEMA, PASSWORD, connstr,encoding="UTF-8")
cursor=con.cursor()
dataset = {
   'name': ['A', 'A', 'A', 'B', 'B', 'B'],
   'address': ['Bangalore', 'Bangalore', 'Bangalore', 'Bangalore', 'Bangalore', 'Bangalore'],
   'email': ['A@gmail.com', 'A1@gmail.com', 'A2@gmail.com', 'B@gmail.com', 'B1@gmail.com', 'B2@gmail.com'],
   'floor': [1, 1, 2, 2, 2, 1]
}

dataset = pd.DataFrame(dataset)

# datatype=dataset.dtypes
# datatype
#cursor.execute('CREATE TABLE X (NAME VARCHAR2(10),ADDRESS VARCHAR2(10),EMAIL VARCHAR2(50),FLOOR NUMBER)')
sql='INSERT INTO X  VALUES (:1,:2,:3,:4)'
df_list = dataset.values.tolist()
n = 0
for i in dataset.iterrows():
    cursor.execute(sql,df_list[n])
    n += 1

con.commit()
cursor.close
con.close   