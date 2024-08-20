# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 17:08:03 2023

@author: 2076284
"""
import pandas as pd

import cx_Oracle
#cx_Oracle.init_oracle_client(lib_dir= r"C:\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7")

try:

    oracle_table_name = "SALES" # table name 

    connection = cx_Oracle.connect('USER/PWD@HOST/SERVICE_NAME')  # oracle connection

    cursor = connection.cursor()

    df = pd.read_excel("Sales-Distribution-Practice-File.xlsx",'Input Data')   # excel file path

    df.fillna('',inplace=True)

    insert_query = f"INSERT INTO SALES VALUES (TO_DATE(':1','DD-MM-YYYY'), :2,:3,:4,:5,:6,:7,:8,:9,:10)"  # no of coloumns

    cursor.execute(insert_query)

 

    connection.commit()

 

    print("Data loaded into Oracle table successfully!")

 

except Exception as e:

    print(f"An error occurred: {str(e)}")

    connection.rollback()

 

finally:

    cursor.close()

    connection.close()
