#!/usr/bin/env python
# coding: utf-8

#-------------------------------------------------------------------------------------------------------------------#
"""RECON Autoamtion Frameowrk.py: This python script will automate the RCA creation for MRC difference file."""
__author__      = "Shiva Durai"
__copyright__   = "Copyright 2019| Comcast-Orion Project | Cognizant Technology Solutions "
__maintainer__ = "Shiva Durai"
__status__ = "Stage or Production, based out of Config File"
#-------------------------------------------------------------------------------------------------------------------#

# Importing all Libraries
import pandas as pd
import numpy as np
import cx_Oracle
import sqlalchemy
import os
import platform
from collections import Counter
from win32com.client import Dispatch
from glob import glob
import time
start = time.time()


#from datetime import date
import datetime
from itertools import chain


# Import the Config Files
import Config_MRC as Config


# Information on the environments we are using in the PY framework
# Test to see if it will print the version of sqlalchemy
print(f'SQL Alchemy Version : {sqlalchemy.__version__}')   
# Test to see if the cx_Oracle is recognized
print(f'CX Oracle PiPy lib Version : {cx_Oracle.version}')  
# This fails for me at this point but will succeed after the solution described below
print(f'Oracle insta client lib Version : {cx_Oracle.clientversion()}')
# Diagnostic output to verify 64 bit arch and list files
print("SYS ARCH:", platform.architecture())
# print("FILES AT lib_dir:")
# for name in os.listdir(lib_dir):
#     print(name)


# Set database parameters
# DB DETAILS
DATABASE = Config.DATABASE
PORT = Config.PORT
SID = Config.SID
connstr = Config.connstr

# USER DETAILS - Bhoomi - commenting STAGE_SCHEMA AND STAGE_PASSWORD -07/05/2022
SCHEMA   = Config.SCHEMA
PASSWORD = Config.PASSWORD
STAGE_SCHEMA   = Config.STAGE_SCHEMA
STAGE_PASSWORD = Config.STAGE_PASSWORD


# Display the connection strings for the framework
print(f'The Connect String/TNS going to be used is : {connstr}')


# Creating STG and DA USER connections
# Create Connection to Oracle
connection = cx_Oracle.connect(SCHEMA, PASSWORD, connstr,encoding="UTF-8")
conx_stage = cx_Oracle.connect(STAGE_SCHEMA, STAGE_PASSWORD, connstr,encoding="UTF-8")


# Display the connection strings for the framework
print(f'Active Connection Established with Database  : {connection}')


# SQL to test connection to database
TestSQL =Config.TestSQL
TestSQL
df_ora = pd.read_sql(TestSQL, con=connection)
df_ora


#Get the Local variables data to suffix the table and file name
FILE_NAME=Config.FILE_NAME
RCA_TABLE_NAME=Config.CM030_RCA_TABLE
RCA_FILE_NAME=Config.RCA_FILE_NAME
CM030_DIFF_TABLE=Config.CM030_DIFF_TABLE


#Printing the file and database objects name for reference
print("FILE_NAME is: "+FILE_NAME)
print("RCA_FILE_NAME is: "+RCA_FILE_NAME)
print("RCA_TABLE_NAME is: "+RCA_TABLE_NAME)
print("CM030_DIFF_TABLE is: "+CM030_DIFF_TABLE)



# Query_CM030_P_DIFF_SQL =Config.Query_CM030_P_DIFF_SQL
# df = pd.read_sql(Query_CM030_P_DIFF_SQL, con=conx_stage)
# df
#FILE_NAME
#Query_CM030_P_DIFF_SQL


#Pass the File name to a local variable
output_path = FILE_NAME

#Start the writing of the difference file details to a temp file
writer = pd.ExcelWriter(output_path)
#df = pd.read_csv ('DIFF_CM030-P.csv')
Query_CM030_P_DIFF_SQL =Config.Query_CM030_P_DIFF_SQL
df = pd.read_sql(Query_CM030_P_DIFF_SQL, con=conx_stage)
df['MRC_DIFF'] = df['SV_MRC'] - df['UF_MRC']
df['Len'] = df['LEGACY_CUST_ID'].astype(str).map(len)

#Mapping and scoring the length of the CSG accounts
def map_score(score):
    if score >= 16:
        return "Y"
    else:
        return "N"
#Applying the Filter wiht the above UDF
df["CSG(Y/N)"] = df["Len"].apply(lambda score: map_score(score))

#df['LEGACY_CUST_ID']=df['LEGACY_CUST_ID'].astype(str)
df['LEGACY_CUST_ID']=df['LEGACY_CUST_ID'].astype('int64')
pd.set_option('display.precision', 0)
ColumnOrder = ['CHECK_ID', 'LEGACY_CUST_ID', 'PRODUCT_OFFER', 'SV_MRC', 'UF_MRC', 'CSG(Y/N)', 'MRC_DIFF']
df[ColumnOrder].to_excel(writer,sheet_name='CM030-P DIFF File', index=False)
writer.save()
#writer.close()


TruncateRCASQL = Config.TruncateRCASQL
TruncateRCASQL
cursor = conx_stage.cursor()
cursor.execute(TruncateRCASQL)
conx_stage.commit()


#Display the Table anme for your reference
print(RCA_TABLE_NAME)


#Passing the file name to the local variable
file = FILE_NAME     
tab_name = "CM030-P DIFF File"
# tab_exists = """
# DECLARE
#   v_exst INT;
# BEGIN
#   SELECT COUNT(*) 
#     INTO v_exst 
#     FROM cat 
#    WHERE table_name = '"""+tab_name+"""' 
#      AND table_type = 'TABLE';
#   IF v_exst = 1 THEN
#      EXECUTE IMMEDIATE('DROP TABLE """+tab_name+"""');
#   END IF;   
# END;
# """
# cursor.execute(tab_exists)    
# create_table = """
# CREATE TABLE """+tab_name+""" (
#        col1 VARCHAR2(50) NOT NULL,
#        col2 VARCHAR2(50) NOT NULL,
#        col3 VARCHAR2(50) NOT NULL,
#        col4 VARCHAR2(50) NOT NULL,
#        col5 VARCHAR2(50) NOT NULL,
#        col6 VARCHAR2(50) NOT NULL,
#        col7 VARCHAR2(50) NOT NULL
# )    """    
# cursor.execute(create_table)     
df_rca_tbl_load = pd.read_excel(file, engine='openpyxl') 
df_rca_tbl_load.drop(columns= ['CSG(Y/N)'], inplace = True)
df_list = df_rca_tbl_load.fillna('').values.tolist()   
#start = timeit.timeit()


#Inserting the DIFF data to the RCa table
insert_table = "INSERT INTO "+RCA_TABLE_NAME+" VALUES (:1,:2,:3,:4,:5,:6)"    
cursor = conx_stage.cursor()   
cursor.executemany(insert_table,df_list)    


#end = timeit.timeit()
#print(end - start)
cursor.close()
conx_stage.commit()
#conx_stage.close()
#Need the connection open for one another database query.


#insert_table
#output_path
#RCA_TABLE_NAME

#The below SQL is from Config file to get the Std. Address Mismatch
Query_Std_Address_Mismatch = Config.Query_Std_Address_Mismatch
#Query_Std_Address_Mismatch
#Get the DataFrame from the above SQL
df_Std_Addr_Mismatch = pd.read_sql(Query_Std_Address_Mismatch, con=connection)
#df_Std_Addr_Mismatch
#df_Std_Addr_Mismatch

# Accounts which has address mismatch, returned by the Std Mismatch query
df_Addr_Mismatch_Accounts = df_Std_Addr_Mismatch[['ACCOUNTNUMBER']]

# Remove duplicates from the list of Accounts
df_Addr_Mismatch_Accounts.drop_duplicates(inplace=True)

#The below SQL is from Config file to get the missing services
Query_CM030_P_UF832 = Config.Query_CM030_P_UF832

#Get the DataFrame from the above SQL
df_Missing_Services = pd.read_sql(Query_CM030_P_UF832, con=connection)

#Query_CM030_P_UF832

#The below SQL is from Config file to get the DUP EQPs
Query_Duplicate_Address = Config.Query_Duplicate_Address

#Get the DataFrame from the above SQL
df_Duplicate_Equipment = pd.read_sql(Query_Duplicate_Address, con=connection)

#df_Duplicate_Equipment

#The below SQL is from Config file 
Query_Incorrect_Service_Address = Config.Query_Incorrect_Service_Address

#Get the DataFrame from the above SQL
df_Incorrect_SV_Add = pd.read_sql(Query_Incorrect_Service_Address, con=connection)
#df_Incorrect_SV_Add

#The below SQL is from Config file 
Query_Site_Address_Standardization = Config.Query_Site_Address_Standardization

#Get the DataFrame from the above SQL
df_Site_Address_Std = pd.read_sql(Query_Site_Address_Standardization, con=connection)
#df_Site_Address_Std
#Query_Site_Address_Standardization


Query_UF_541 = Config.Query_UF_541

#Get the DataFrame from the above SQL
df_UF_541 = pd.read_sql(Query_UF_541, con=connection)
#df_UF_541
#df_UF_541.shape
Query_Missing_BI_Equipment=Config.Query_Missing_BI_Equipment

#Get the DataFrame from the above SQL
df_Missing_BI_Equipment = pd.read_sql(Query_Missing_BI_Equipment, con=connection)

#Get the DataFrame from the above SQL
#Query_Missing_BI_Equipment
#df_Missing_BI_Equipment
#df_Missing_BI_Equipment.shape
Query_Circuit_Account_Mismatch = Config.Query_Circuit_Account_Mismatch

#Get the DataFrame from the above SQL
df_DNORM = pd.read_sql(Query_Circuit_Account_Mismatch, con=connection)
#Query_Circuit_Account_Mismatch
#df_DNORM.shape
#df_DNORM

#Get the DataFrame from the above SQL
# 08-26-2021: Fill Na with NULL so that the filters can be applied in the next step correctly
df_DNORM.fillna("NULL",inplace=True)
#df_DNORM
#df_DNORM[df_DNORM['CUSTOMER_ACCOUT_NUMBER']=="939042552"]
#df_DNORM[df_DNORM.SV_CLIPS_VALIDATION == 0]


# Contains all IPV4 accounts where SV_CLIPS_VALIDATION fails ie Circuit Mismatch
#Get the DataFrame from the above SQL
#df_Circuit_Mismatch = df_DNORM[df_DNORM.SV_CLIPS_VALIDATION == 0]
df_Circuit_Mismatch = df_DNORM[df_DNORM.SV_CLIPS_VALIDATION == 0]

#df_Circuit_Mismatch

# Contains all IPV4 accounts where SV_CLIPS_ACC_VALIDATION fails ie Account Mismatch
# df_Account_Mismatch = df_DNORM[df_DNORM.SV_CLIPS_ACC_VALIDATION == 0]
# New Requirement--> SV_CLIPS_ACC_VALIDATION is null (then account mismatch flag should be set to 1)
df_Account_Mismatch = df_DNORM[df_DNORM.SV_CLIPS_ACC_VALIDATION == "NULL"]
df_Circuit_Mismatch.shape, df_Account_Mismatch.shape


#Get the DataFrame from the above SQL
Query_Ethernet_RCA_Reasons = Config.Query_Ethernet_RCA_Reasons

#Get the DataFrame from the above SQL
#Query_Ethernet_RCA_Reasons
df_Ethernet_RCA_Reasons = pd.read_sql(Query_Ethernet_RCA_Reasons, con=connection)
#df_Ethernet_RCA_Reasons.shape
#df_Ethernet_RCA_Reasons.head()

#Bhoomi - Get the DataFrame from the above SQL
Query_Activecore_Missing_Base_Product = Config.Query_Activecore_Missing_Base_Product

#Bhoomi - Get the DataFrame from the above SQL
df_Activecore_Missing_Base_Product = pd.read_sql(Query_Activecore_Missing_Base_Product, con=connection)


#Get the DataFrame from the above SQL
Query_Root_Account_List = Config.Query_Root_Account_List

#Get the DataFrame from the above SQL
df_Root_Account_List = pd.read_sql(Query_Root_Account_List, con=connection)


#Query_Root_Account_List
df_Root_Account_List.shape
#Get the DataFrame from the above SQL
Query_CM_AM_ADD_FILTER = Config.Query_CM_AM_ADD_FILTER


#df_CM_AM_ADD_FILTER =  pd.DataFrame(columns = ['LEGACY_CUST_ID', 'LEGACY_ENTITY_ID', 'SERVICE_CODE', 'LEGACY_PRODUCT_OFFER'])
#print(df_CM_AM_ADD_FILTER)

#Get the DataFrame from the above SQL
df_CM_AM_ADD_FILTER = pd.read_sql(Query_CM_AM_ADD_FILTER, con=connection)

#Query_CM_AM_ADD_FILTER
#Get the DataFrame from the above SQL
df_CM_AM_ADD_FILTER.shape

#Get the DataFrame from the above SQL
Query_CSG_MRC_EXCLUSIONS_SQL = Config.Query_CSG_MRC_EXCLUSIONS_SQL

#Query_CSG_MRC_EXCLUSIONS_SQL
#Get the DataFrame from the above SQL
df_CSG_MRC_EXLUSIONS = pd.read_sql(Query_CSG_MRC_EXCLUSIONS_SQL, con=connection)

ColumnOrder = ['ACCOUNT_NAME', 'PRODUCT_NAME', 'EXCLUSION', 'MRC']

#df_CSG_MRC_EXLUSIONS[ColumnOrder].drop_duplicates()
#df_CSG_MRC_EXLUSIONS[ColumnOrder]

#Get the DataFrame from the above SQL --Bhoomi - Comment Out Query_UNI_EQP_DIFF_INVC_IN_SV logic as of now(05/04/2022)
#Query_UNI_EQP_DIFF_INVC_IN_SV = Config.Query_UNI_EQP_DIFF_INVC_IN_SV

#Get the starting time
#Query_UNI_EQP_DIFF_INVC_IN_SV -  --Bhoomi - Comment Out Query_UNI_EQP_DIFF_INVC_IN_SV logic as of now(05/04/2022)
#start_UNI_EQP_DIFF_SQL = time.time()


#Get the DataFrame from the above SQL --Bhoomi - Comment Out Query_UNI_EQP_DIFF_INVC_IN_SV logic as of now(05/04/2022)
#df_UNI_EQP_DIFF_INVC_IN_SV = pd.read_sql(Query_UNI_EQP_DIFF_INVC_IN_SV, con=connection)

#Get the end time -- Bhoomi - remove time becuase we are no longer using Query_UNI_EQP_DIFF_INVC_IN_SV- 05-09-2022
#end_UNI_EQP_DIFF_SQL = time.time()
#hours, rem = divmod(end_UNI_EQP_DIFF_SQL-start_UNI_EQP_DIFF_SQL, 3600)
#minutes, seconds = divmod(rem, 60)
#print("Time Taken for UNI EPQ SQL is: {:0>2} hours : {:0>2} minutes : {:05.2f} seconds".format(int(hours),int(minutes),seconds))


#Query_UNI_EQP_DIFF_INVC_IN_SV  --Bhoomi - Comment Out Query_UNI_EQP_DIFF_INVC_IN_SV logic as of now(05/04/2022)
#df_UNI_EQP_DIFF_INVC_IN_SV.shape

#df_Root_Account_List.head()

#Get the DataFrame from the above SQL
Query_Multiple_AAN_tagged_same_BVE_service = Config.Query_Multiple_AAN_tagged_same_BVE_service


#Query_Multiple_AAN_tagged_same_BVE_service
df_Multiple_AAN_tagged_same_BVE_service = pd.read_sql(Query_Multiple_AAN_tagged_same_BVE_service, con=connection)

#
#df_Multiple_AAN_tagged_same_BVE_service

#Query Multiple BI on same site SQL
Query_Multiple_BI_On_Same_Site = Config.Query_Multiple_BI_On_Same_Site

#Get Multiple BI on same site in dataframe
df_Multiple_BI_On_Same_Site = pd.read_sql(Query_Multiple_BI_On_Same_Site, con=connection)

#Query Missing Wifi Pro Access Point Equipment SQL
Query_Missing_Wifi_Pro_Access_Point_Eqp = Config.Query_Missing_Wifi_Pro_Access_Point_Eqp

#Get Missing Wifi Pro Access Point Equipment in dataframe
df_Missing_Wifi_Pro_Access_Point_Eqp = pd.read_sql(Query_Missing_Wifi_Pro_Access_Point_Eqp, con=connection)



#Get the DataFrame from the above SQL
#Bhoomi - Added Query_Missing_PRI_Service(05/02)
Query_Missing_PRI_Service = Config.Query_Missing_PRI_Service

#Query Multiple PRI Services
#Bhoomi - Added Query_Missing_PRI_Service in DF(05/02)
df_Missing_PRI_Service = pd.read_sql(Query_Missing_PRI_Service, con= connection)

#Bhoomi - Added Query_Multiple_Voice_To_One_AAN - 05/03/2022
Query_Multiple_Voice_To_One_AAN = Config.Query_Multiple_Voice_To_One_AAN

#Bhoomi - Added Multiple_Voice_To_One_AAN in DF - 05/03/2022
df_Multiple_Voice_To_One_AAN = pd.read_sql(Query_Multiple_Voice_To_One_AAN, con=connection)

#Bhoomi - Get PRI Accounts - test-050922
Query_PRI_Accounts = Config.Query_PRI_Accounts

#Bhoomi - Get PRI Account in DF - test - 05022022
df_Query_PRI_Accounts = pd.read_sql(Query_PRI_Accounts, con=connection)

#Bhoomi - Get CSG has only BI service but eqp tagged as "Equipment Fee - Voice" - 05132022
Query_Incorrect_EQP_Fee_Tagging_in_CSG = Config.Query_Incorrect_EQP_Fee_Tagging_in_CSG

#Bhoomi - Get CSG has only BI service but eqp tagged as "Equipment Fee - Voice"  - 05132022
df_Query_Incorrect_EQP_Fee_Tagging_in_CSG = pd.read_sql(Query_Incorrect_EQP_Fee_Tagging_in_CSG, con=connection)

#Bhoomi - Get -	Only BI Service but having ‘BV Voicemail’ MRC - CSG MRC Cleanup RCA - 05132022
Query_CSG_MRC_Cleanup = Config.Query_CSG_MRC_Cleanup

#Bhoomi - Get -	Only BI Service but having ‘BV Voicemail’ MRC - CSG MRC Cleanup RCA - 05132022
df_Query_CSG_MRC_Cleanup = pd.read_sql(Query_CSG_MRC_Cleanup, con=connection)


#Bhoomi - Get those account where we have same site but we are getting same MRC for both 'Equipment Fee - Voice' and 'Equipment Fee - Data' from source. In Extract flow, we are deleting one of the MRC as per our current logic.
#Bhoomi _ Query to get double billing - 05-17-2022
Query_Equipment_Double_Billing = Config.Query_Equipment_Double_Billing 

#Bhoomi - read the query to get double billing accounts - 05-17-2022
df_Query_Equipment_Double_Billing = pd.read_sql(Query_Equipment_Double_Billing, con=connection)


#Get the DataFrame from the above SQL
Query_ME_SERVICES = Config.Query_ME_SERVICES
#
#df_ME_SERVICES
#Get the DataFrame from the above SQL
df_ME_SERVICES = pd.read_sql(Query_ME_SERVICES, con=connection)


#Get the DataFrame from the above SQL
#df_ME_SERVICES

#Get the DataFrame from the above SQL
Query_BI_SERVICES = Config.Query_BI_SERVICES
#
#Query_BI_SERVICES
#Get the DataFrame from the above SQL
df_BI_SERVICES = pd.read_sql(Query_BI_SERVICES, con=connection)
#

#df_BI_SERVICES
#Get the DataFrame from the above SQL
Query_BCV_SERVICES = Config.Query_BCV_SERVICES
#
#Query_BCV_SERVICES
#Get the DataFrame from the above SQL
df_BCV_SERVICES = pd.read_sql(Query_BCV_SERVICES, con=connection)
#


#df_BCV_SERVICES
#Get the DataFrame from the above SQL
Query_BVE_SERVICES = Config.Query_BVE_SERVICES
#
#Query_BVE_SERVICES
#Get the DataFrame from the above SQL
df_BVE_SERVICES = pd.read_sql(Query_BVE_SERVICES, con=connection)
#


#df_BVE_SERVICES
#Get the DataFrame from the above SQL
Query_PRI_SERVICES = Config.Query_PRI_SERVICES
#
#Query_PRI_SERVICES
#Get the DataFrame from the above SQL
df_PRI_SERVICES = pd.read_sql(Query_PRI_SERVICES, con=connection)
#
#df_PRI_SERVICES


#Get the DataFrame from the above SQL
Query_SIP_SERVICES = Config.Query_SIP_SERVICES
#
#Query_SIP_SERVICES
#Get the DataFrame from the above SQL
df_SIP_SERVICES = pd.read_sql(Query_SIP_SERVICES, con=connection)
#


#df_SIP_SERVICES
#Get the DataFrame from the above SQL
Query_Same_ActiveCore_Mult_EQPs = Config.Query_Same_ActiveCore_Mult_EQPs
#
#Query_Same_ActiveCore_Mult_EQPs
#Get the DataFrame from the above SQL
df_Same_ActiveCore_Mult_EQPs = pd.read_sql(Query_Same_ActiveCore_Mult_EQPs, con=connection)
#
#df_Same_ActiveCore_Mult_EQPs


#Get the DataFrame from the above SQL
Query_SV_EQP_MRC_Underlay_Switch = Config.Query_SV_EQP_MRC_Underlay_Switch
#
#Query_SV_EQP_MRC_Underlay_Switch
#Get the DataFrame from the above SQL
Query_PRD_IND_List = Config.Query_PRD_IND_List
#


#Query_PRD_IND_List
#Get the DataFrame from the above SQL
df_PRD_IND_List = pd.read_sql(Query_PRD_IND_List, con=connection)
#Get the DataFrame from the above SQL
df_PRD_IND_List

#Get the Dataframe for the below SQL --Bhoomi Added Missing AAN for BVE Services -08/21/23
Query_Missing_AAN_BVE_Services = Config.Query_Missing_AAN_BVE_Services

df_Missing_AAN_BVE_Services = pd.read_sql(Query_Missing_AAN_BVE_Services, con=connection)


#
# #df_RCA_Ethernet_Combined
#df_PRD_IND_List['LEGACY_ACCOUNT_NO'] = df_PRD_IND_List['LEGACY_ACCOUNT_NO'].astype('int64')
#df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'] = df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].astype('int64')
#df_RCA_Ethernet_Combined = pd.merge(df_RCA_Ethernet_Combined,df_PRD_IND_List,how = 'left', 
#left_on= ['LEGACY_ACCOUNT_NO'], right_on=['LEGACY_ACCOUNT_NO'] , suffixes=('_Source','_PRD_IND'))
#
#df_RCA_Ethernet_Combined['SV_PRODUCT_INDICATOR']
#
#connection.close()
#Need the connection open for one another database query.
#
#df_Diff_Src = pd.read_csv(r'R11.MR1 - Cycle 2_DIFF_CM030-P.csv')
#df_Diff_Src=pd.read_excel(open('R11.MR1 - Cycle 2_DIFF_CM030-P.xlsx', 'rb'),
#              sheet_name='CM030-P DIFF File')  

#Get the MRC diff data from the file
df_Diff_Src=pd.read_excel(open(FILE_NAME, 'rb'),
              sheet_name='CM030-P DIFF File', engine='openpyxl')  
#
df_Diff_Src.shape
#
#df_Diff_Src.head()
#P2P_FILTER



#Prepare the P2P consolidation process
#P2P = glob(r'C:\Users\Bsiddh212\Documents\bsiddh212\Recon\Recon_New_2022\Python_Code\MRC_Py_Code\R22\R22_FVT_8\*P2P*')
P2P = glob(r'C:\Recon MRC Mismatches\Recon_MR2_Testing_11_03\*P2P*')
# # initializing start Prefix
start_letter = 'P2P_'
# # Remove the path from the LIST
#P2P=[s.replace('C:\\Users\\Bsiddh212\\Documents\\bsiddh212\\Recon\\Recon_New_2022\\Python_Code\\MRC_Py_Code\\R22\\R22_FVT_8\\', '') for s in P2P] 
P2P=[s.replace('C:\\Recon MRC Mismatches\\Recon_MR2_Testing_11_03\\', '') for s in P2P] 
# # Filter the LIST thathas only P2P as the start string
P2P_FILTER = [idx for idx in P2P if idx.lower().startswith(start_letter.lower())]


# # P2P_ME=str(P2P_FILTER)
# # P2P_BI=str(P2P_FILTER)
# # P2P_AV=str(P2P_FILTER)
# # P2P_BV=str(P2P_FILTER)
# p2p_columns = ["LEGACY_CUST_ID","LEGACY_ENTITY_ID","SERVICE_CODE","SERVICE_SEQ","MANUAL_OVERRIDE_AMOUNT","LEGACY_ACCOUNT_NO"]
# #Get the ME/AV/BVE P2P files
# try:
#     P2P_ME=str(P2P_FILTER)
#     df_P2P_fallout_details_ME = pd.read_excel(P2P_ME, engine='openpyxl')
# except (IndexError, ValueError):
#     df_P2P_fallout_details_ME=pd.DataFrame(columns = p2p_columns)
#     pass
# try:
#     P2P_BI=str(P2P_FILTER)
#     df_P2P_fallout_details_BI = pd.read_excel(P2P_BI, engine='openpyxl')
# except (IndexError, ValueError):
#     df_P2P_fallout_details_BI=pd.DataFrame(columns = p2p_columns)
#     pass
# try:
#     P2P_AV=str(P2P_FILTER)
#     df_P2P_fallout_details_AV = pd.read_excel(P2P_AV, engine='openpyxl')
# except (IndexError, ValueError):
#     df_P2P_fallout_details_AV=pd.DataFrame(columns = p2p_columns)
#     pass
# try:
#     P2P_BV=str(P2P_FILTER)
#     df_P2P_fallout_details_BV = pd.read_excel(P2P_BV, engine='openpyxl')
# except (IndexError, ValueError):
#     df_P2P_fallout_details_BV=pd.DataFrame(columns = p2p_columns)
#     pass
#
#xls_file_list
#

#Get the P2P columns that is needed for comparison
p2p_columns = ["LEGACY_CUST_ID","LEGACY_ENTITY_ID","SERVICE_CODE","SERVICE_SEQ","MANUAL_OVERRIDE_AMOUNT","LEGACY_ACCOUNT_NO"]

P2P_SUB_LIST=[s.split('.') for s in P2P_FILTER]
xls_files = ([i for i in P2P_SUB_LIST if i!="xlsx"])
xls_files_merge = [i[:1] for i in xls_files]
xls_files_merge=list(chain.from_iterable(xls_files_merge))
xlsx =str('.xlsx')
xls =str('.xls')
xlsx_file_list = list(map(lambda orig_string: str(orig_string) + xlsx, xls_files_merge))
xls_file_list = list(map(lambda orig_string: str(orig_string) + xls, xls_files_merge))

#Create a UDF
def last_letter(word):
    return word[:-1]
sorted(xls_files_merge, key=last_letter)
#Get the ME/AV/BVE P2P files
try:
    for f in xlsx_file_list:
        os.remove(f)
except OSError:
    pass
xls_file_list.sort()
xlsx_file_list
i=0


for f in xlsx_file_list:
    output_path = f
    writer = pd.ExcelWriter(output_path)
    df = pd.read_excel(xls_file_list[i])
    xlsx_sheet_name = str(xls_file_list[i]).split('.')[0]
    xlsx_sheet_name=str(xlsx_sheet_name[7:])
    i=i+1
    df.to_excel(writer,sheet_name=xlsx_sheet_name, index=False)
    writer.save()
    #writer.close()
#Get the ME/AV/BVE P2P files


try:
    P2P_ME=str(xlsx_file_list[0])
    df_P2P_fallout_details_ME = pd.read_excel(P2P_ME, engine='openpyxl')
except (IndexError, ValueError):
    df_P2P_fallout_details_ME=pd.DataFrame(columns = p2p_columns)
    pass
    
    
#try:
#    P2P_BI=str(xlsx_file_list[2])
#    df_P2P_fallout_details_BI = pd.read_excel(P2P_BI, engine='openpyxl')
#except (IndexError, ValueError):
#    df_P2P_fallout_details_BI=pd.DataFrame(columns = p2p_columns)
#    pass
    
    
try:
    P2P_AV=str(xlsx_file_list[1])
    df_P2P_fallout_details_AV = pd.read_excel(P2P_AV, engine='openpyxl')
except (IndexError, ValueError):
    df_P2P_fallout_details_AV=pd.DataFrame(columns = p2p_columns)
    pass
    
    
try:
    P2P_BV=str(xlsx_file_list[2])
    df_P2P_fallout_details_BV = pd.read_excel(P2P_BV, engine='openpyxl')
except (IndexError, ValueError):
    df_P2P_fallout_details_BV=pd.DataFrame(columns = p2p_columns)
    pass
    
    
#
# p2p_columns = ["LEGACY_CUST_ID","LEGACY_ENTITY_ID","SERVICE_CODE","SERVICE_SEQ","MANUAL_OVERRIDE_AMOUNT","LEGACY_ACCOUNT_NO"]
# #Get the ME/AV/BVE P2P files
# try:
#     P2P_ME=str(P2P_FILTER)
#     fname = P2P_ME
#     excel = win32.gencache.EnsureDispatch('Excel.Application')
#     path =  os.getcwd().replace('\'','\\') + '\\'
#     xlsx_format=".xlsx"
#     P2P_ME_xlsx = P2P_ME.split(".")
#     P2P_ME_xlsx=str(P2P_ME_xlsx)
#     wb = excel.Workbooks.Open(path+fname) 
#     wb.SaveAs(path+P2P_ME_xlsx, FileFormat = 51)    #FileFormat = 51 is for .xlsx extension
#     wb.Close()                               #FileFormat = 56 is for .xls extension
#     excel.Application.Quit()
#     P2P_ME_xlsx=path+P2P_ME_xlsx+xlsx_format
#     df_P2P_fallout_details_ME = pd.read_excel(P2P_ME_xlsx, engine='openpyxl')
# except (IndexError, ValueError):
#     df_P2P_fallout_details_ME=pd.DataFrame(columns = p2p_columns)
#     pass


# try:
#     P2P_BI=str(P2P_FILTER)
#     fname = P2P_BI
#     excel = win32.gencache.EnsureDispatch('Excel.Application')
#     path =  os.getcwd().replace('\'','\\') + '\\'
#     xlsx_format=".xlsx"
#     P2P_BI_xlsx = P2P_BI.split(".")
#     P2P_BI_xlsx=str(P2P_BI_xlsx)
#     wb = excel.Workbooks.Open(path+fname)
#     wb.SaveAs(path+P2P_BI_xlsx, FileFormat = 51)    #FileFormat = 51 is for .xlsx extension
#     wb.Close()                               #FileFormat = 56 is for .xls extension
#     excel.Application.Quit()
#     P2P_BI_xlsx=path+P2P_BI_xlsx+xlsx_format    
#     df_P2P_fallout_details_BI = pd.read_excel(P2P_BI_xlsx, engine='openpyxl')
# except (IndexError, ValueError):
#     df_P2P_fallout_details_BI=pd.DataFrame(columns = p2p_columns)
#     pass


# try:
#     P2P_AV=str(P2P_FILTER)
#     fname = P2P_AV
#     excel = win32.gencache.EnsureDispatch('Excel.Application')
#     path =  os.getcwd().replace('\'','\\') + '\\'
#     xlsx_format=".xlsx"
#     P2P_AV_xlsx = P2P_AV.split(".")
#     P2P_AV_xlsx=str(P2P_AV_xlsx)
#     wb = excel.Workbooks.Open(path+fname)
#     wb.SaveAs(path+P2P_AV_xlsx, FileFormat = 51)    #FileFormat = 51 is for .xlsx extension
#     wb.Close()                               #FileFormat = 56 is for .xls extension
#     excel.Application.Quit()
#     P2P_AV_xlsx=path+P2P_AV_xlsx+xlsx_format      
#     df_P2P_fallout_details_AV = pd.read_excel(P2P_AV_xlsx, engine='openpyxl')
# except (IndexError, ValueError):
#     df_P2P_fallout_details_AV=pd.DataFrame(columns = p2p_columns)
#     pass


# try:
#     P2P_BV=str(P2P_FILTER)
#     fname = P2P_BV
#     excel = win32.gencache.EnsureDispatch('Excel.Application')
#     path =  os.getcwd().replace('\'','\\') + '\\'
#     xlsx_format=".xlsx"
#     P2P_BV_xlsx = P2P_BV.split(".")
#     P2P_BV_xlsx=str(P2P_AV_xlsx)
#     wb = excel.Workbooks.Open(path+fname)
#     wb.SaveAs(path+P2P_BV_xlsx, FileFormat = 51)    #FileFormat = 51 is for .xlsx extension
#     wb.Close()                               #FileFormat = 56 is for .xls extension
#     excel.Application.Quit()   
#     P2P_BV_xlsx=path+P2P_BV_xlsx+xlsx_format        
#     df_P2P_fallout_details_BV = pd.read_excel(P2P_BV_xlsx, engine='openpyxl')
# except (IndexError, ValueError):
#     df_P2P_fallout_details_BV=pd.DataFrame(columns = p2p_columns)
#     pass


#
#df_P2P_fallout_details_ME
#
#df_P2P_fallout_details_ME = pd.read_excel(P2P_ME, engine='openpyxl')
#df_P2P_fallout_details_BI = pd.read_excel(P2P_BI, engine='openpyxl')
#df_P2P_fallout_details_AV = pd.read_excel(P2P_AV)
#df_P2P_fallout_details_BV = pd.read_excel(P2P_BV)
#


# df_P2P_fallout_details_1 = pd.read_excel(r'P2P_MIG6_fallout_details.xls')
# df_P2P_fallout_details_2 = pd.read_excel(r'P2P_MIG8_fallout_details.xls')
# df_P2P_fallout_details_3 = pd.read_excel(r'P2P_MIG9_fallout_details.xls')
#


#MERGE THE P2P FILES ---removed - df_P2P_fallout_details_BI - Bhoomi - 06082022
df_P2P_Consolidated = pd.concat([df_P2P_fallout_details_ME,df_P2P_fallout_details_AV,df_P2P_fallout_details_BV], axis = 0)
#


# Then drop duplicates
df_P2P_Consolidated.drop_duplicates(inplace=True)
#


# Check if the row count matches ---removed - df_P2P_fallout_details_BI.shape,- Bhoomi - 06082022
df_P2P_Consolidated.shape,df_P2P_fallout_details_ME.shape,df_P2P_fallout_details_AV.shape,df_P2P_fallout_details_BV.shape
#


# EXPORT COMBINED P2P TO A FILE
output_path = r'Export_P2P_CONSOLIDATED.xlsx'
writer = pd.ExcelWriter(output_path)
df_P2P_Consolidated.to_excel(writer,sheet_name='P2P_CONSOLIDATED')
writer.save()
#

df_Diff_Src['Std Address Mismatch'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].astype('int64').isin(df_Addr_Mismatch_Accounts['ACCOUNTNUMBER'].astype('int64'))) & ((df_Diff_Src['PRODUCT_OFFER'] == 'Ethernet Equipment Fee') | (df_Diff_Src['PRODUCT_OFFER'] == 'Equipment Fee')),1,0)

#

# Ethernet
#Shiva: 09-01-21 Std Address Mismatch FLAG is not anymore Ethernet EQP
#df_Diff_Src['Std Address Mismatch'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].astype('int64').isin(df_Addr_Mismatch_Accounts['ACCOUNTNUMBER'].astype('int64'))) & (df_Diff_Src['PRODUCT_OFFER'] == 'Ethernet Equipment Fee'),1,0)
df_Diff_Src['Duplicate Equipment Fee in SingleView'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].astype('int64').isin(df_Duplicate_Equipment['ACCOUNTNUMBER'].astype('int64'))) & (df_Diff_Src['PRODUCT_OFFER'] == 'Ethernet Equipment Fee'),1,0)
df_Diff_Src['Incorrect_Service_Address'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].astype('int64').isin(df_Incorrect_SV_Add['ACCT_NUM'].astype('int64'))) & (df_Diff_Src['PRODUCT_OFFER'] == 'Ethernet Equipment Fee'),1,0)
df_Diff_Src['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].astype('int64').isin(df_UF_541['LEGACY_CUST_ID'].astype('int64'))) & (df_Diff_Src['PRODUCT_OFFER'] == 'Ethernet Equipment Fee'),1,0)
df_Diff_Src['Site Address Standardization issue'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].astype('int64').isin(df_Site_Address_Std['LEGACY_CUST_ID'].astype('int64'))) & (df_Diff_Src['PRODUCT_OFFER'] == 'Ethernet Equipment Fee'),1,0)

#

#df_Diff_Src[(df_Diff_Src['Incorrect_Service_Address'] == 1) & (df_Diff_Src['PRODUCT_OFFER'] == 'Ethernet Equipment Fee')]
#df_Diff_Src[df_Diff_Src['LEGACY_CUST_ID']==939743950]
#

#Missing BI Equipment
#Shiva: 09-02--> Needs to be mapped for ROOT account, not CHILD account
#df_Diff_Src['Missing BI Equipment'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].astype('int64').isin(df_Missing_BI_Equipment['LEGACY_CUST_ID'].astype('int64'))) & (df_Diff_Src['PRODUCT_OFFER'] == 'Equipment Fee'),1,0)
#

#df_Diff_Src[df_Diff_Src['Missing BI Equipment'] == 1]
#df_Missing_BI_Equipment
#

#Missing Services in UF_SUBSCRIBER
df_Diff_Src['Missing services in UF_SUBSCRIBER'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].isin(df_Missing_Services['LEGACY_CUST_ID'].astype('int64'))) ,1,0)

#

# IPV4 P2P
df_Diff_Src['IPV4 P2P FALLOUT'] = np.where((df_Diff_Src['LEGACY_CUST_ID'].isin(df_P2P_Consolidated['LEGACY_ACCOUNT_NO'].astype('int64'))) & (df_Diff_Src['PRODUCT_OFFER'].str.startswith('IPv4',na=False)),1,0)

#

# UNI and EQP Fee on different invoice account no's in SingleView  --Bhoomi - Comment Out Query_UNI_EQP_DIFF_INVC_IN_SV logic as of now(05/04/2022)
#df_Diff_Src['UNI and EQP Fee on different invoice account nos in SingleView'] = np.where(df_Diff_Src['LEGACY_CUST_ID'].isin(df_UNI_EQP_DIFF_INVC_IN_SV['EQP_CHILD_BAN'].astype('int64')),1,0)

#

# Account Mismatch--> OPEN ITEM ALWAYS RPT goes ROOT A/C
#df_Diff_Src['ACCT MISMATCH'] = np.where(df_Diff_Src['LEGACY_CUST_ID'].isin(df_Account_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64')),1,0)

#

# # Circuit Mismatch

# df_Diff_Src['CIRCUIT MISMATCH'] = np.where(df_Diff_Src['LEGACY_CUST_ID'].isin(df_Circuit_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64')),1,0)
# df_Diff_Src['CIRCUIT MISMATCH'] = np.where(df_Diff_Src['LEGACY_CUST_ID'].isin(df_Circuit_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64')),1,0)

#

# In general if UF MRC>SV MRC -> it need to be marked for Extract review in RCA column as these are incorrect cartesian joins in UF load causing the multiples in UF
df_Diff_Src['MRC Outliers for extract review'] = np.where(df_Diff_Src['UF_MRC'] > df_Diff_Src['SV_MRC'],1,0) | np.where(df_Diff_Src['MRC_DIFF'] == 5 ,1,0) | np.where(df_Diff_Src['MRC_DIFF'] == 10 ,1,0) | np.where(df_Diff_Src['MRC_DIFF'] == -5 ,1,0) | np.where(df_Diff_Src['MRC_DIFF'] == -10 ,1,0)

#

#df_Diff_Src['SV_MRC'].dtypes
#df_Diff_Src

#
#output_path = 'R13.MR1 - Cycle-2_DIFF_CM030-P_STD_ADDR.xlsx'
#writer = pd.ExcelWriter(output_path)
#df_Diff_Src.to_excel(writer,sheet_name='RCA', index=False)
#writer.save()
#


#df_Diff_Src['Rounding Issue'] = np.where(df_Diff_Src['MRC_DIFF'].between(-0.03, 0.03, inclusive=False), 0,1)
#df_Diff_Src['Rounding Issue'] = np.where(df_Diff_Src['MRC_DIFF'].between(-0.03, 0.03, inclusive=False) == True, 0, 1) 
#df_Diff_Src[df_Diff_Src['MRC_DIFF'].between(-0.03, 0.03, inclusive=False)]
#df_Diff_Src[df_Diff_Src['MRC_DIFF'].between(-0.03, 0.03, inclusive=False)]
# conditions = [
#     (df_Diff_Src['MRC_DIFF'] >= -0.03) & (df_Diff_Src['MRC_DIFF'] <= 0.03)
#     ]
# values = ['1']
# df_Diff_Src['Rounding Issue'] = np.select(conditions, values)
#


#Flag rounding issue
#df_Diff_Src['Rounding Issue'] =  np.where((df_Diff_Src['MRC_DIFF'] >= -0.03) & (df_Diff_Src['MRC_DIFF'] >= 0.03),0,1) | np.where(df_Diff_Src['MRC_DIFF'] == 0.01 ,1,0) | np.where(df_Diff_Src['MRC_DIFF'] == 0.02 ,1,0) | np.where(df_Diff_Src['MRC_DIFF'] == 0.03 ,1,0) | np.where(df_Diff_Src['MRC_DIFF'] == -0.01 ,1,0) | np.where(df_Diff_Src['MRC_DIFF'] == -0.02 ,1,0) | np.where(df_Diff_Src['MRC_DIFF'] == -0.03 ,1,0)
#df_Diff_Src[(df_Diff_Src['MRC_DIFF'] >= -0.03) & (df_Diff_Src['MRC_DIFF'] <= 0.03)]


df_Diff_Src['Rounding Issue'] = [1 if x >= -0.03 and x <= 0.03 else 0 for x in df_Diff_Src['MRC_DIFF']]


#
# Calculate total count of mismatches
#df_Diff_Src[df_Diff_Src['PRODUCT_OFFER']=="Static IP Address Block"]
#df_Diff_Src['MRC_DIFF'] 
# df_Diff_Src['Count of Mismatches'] = df_Diff_Src['Std Address Mismatch'] + df_Diff_Src['Duplicate Equipment Fee in SingleView']  + df_Diff_Src['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS']  + df_Diff_Src['IPV4 P2P FALLOUT'] + df_Diff_Src['ACCT MISMATCH'] + df_Diff_Src['CIRCUIT MISMATCH']
#df_Diff_Src[df_Diff_Src['MRC_DIFF'] == -0.01]
#df_Diff_Src[df_Diff_Src['Rounding Issue']==1]
#


df_Diff_Src.info()


#
df_Ethernet_RCA_Reasons.info()
#


df_Ethernet_RCA_Reasons['CUST_ID_INT'] = df_Ethernet_RCA_Reasons['CUST_ID'].astype('int64')

#

df_RCA_Ethernet_Combined = pd.merge(df_Diff_Src,df_Ethernet_RCA_Reasons,how = 'left', 
left_on= ['LEGACY_CUST_ID'], right_on=['CUST_ID_INT'] , suffixes=('_Source','_RCA_Reason'))

#

df_RCA_Ethernet_Combined.shape

#

#df_RCA_Ethernet_Combined.head()
#
# def StandardAddressMismatch(param_Cust_ID):
#     SV_Address = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['LEGACY_CUST_ID'] == param_Cust_ID]['SV_ADDRESS'].astype('str').iloc
#     L_SV_Address = SV_Address.split('\n')
#     UF_Address = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['LEGACY_CUST_ID'] == param_Cust_ID]['UF_ADDRESS'].astype('str').iloc
#     L_UF_Address = UF_Address.split('\n')
# # ------------------- STANDARD ADDRESS MISMATCH -----------------------#
#     # Get Addresses that are in SV but not in UF
#     Set_Only_In_SV = set(L_SV_Address) - set(L_UF_Address)
#     # Get Addresses that are in UF but not in SV : This is common scenario and not required
#     # Set_Only_In_UF = set(L_UF_Address) - set(L_SV_Address)
#     # format the values as it needs to appear in excel
#     Set_Only_In_SV_Disp_Txt = set([x.replace(' | ','') + ' appears only in SV \n' for x in Set_Only_In_SV])
#     # format the values as it needs to appear in excel
#     # Set_Only_In_UF_Disp_Txt = set([x.replace(' | ','') + ' appears in UF \n' for x in Set_Only_In_UF])
#     # Union the Standard Address Mismatch Errors
#     # Set_Address_diff = Set_Only_In_SV_Disp_Txt.union(Set_Only_In_UF_Disp_Txt)
#     # Convert the set to a list
#     List_Std_Address_Mismatch = list(Set_Only_In_SV_Disp_Txt)
# # ------------------- DUPLICATE EQUIPMENT FEES -----------------------#
#     # Create counters for each list 
#     SV_Address_Counter = Counter(L_SV_Address)
#     # Create counters for each list 
#     UF_Address_Counter = Counter(L_UF_Address)
#     # format the values as it needs to appear in excel
#     L_SV_Duplicate_Addr = [x.replace(' | ','') + f' has {SV_Address_Counter[x]} EQP in SV' for x in SV_Address_Counter if SV_Address_Counter[x] >= 2]
#     # format the values as it needs to appear in excel
#     L_UF_Duplicate_Addr = [x.replace(' | ','') + f' has {UF_Address_Counter[x]} EQP in UF/CLIPS' for x in UF_Address_Counter if UF_Address_Counter[x] >= 2]
#     # Merge the two list
#     List_Duplicate_Address = L_SV_Duplicate_Addr + L_UF_Duplicate_Addr
#     Final_List_Issues = List_Std_Address_Mismatch+ List_Duplicate_Address
#     ReturnString = '\n'.join(Final_List_Issues)
#     return(ReturnString)
#


def StandardAddressMismatch(param_Cust_ID):
    SV_Address = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['LEGACY_CUST_ID'] == param_Cust_ID]['SV_ADDRESS'].astype('str').iloc
    #L_SV_Address = SV_Address.split('\n')
    L_SV_Address = SV_Address
    UF_Address = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['LEGACY_CUST_ID'] == param_Cust_ID]['UF_ADDRESS'].astype('str').iloc
    #L_UF_Address = UF_Address.split('\n')
    L_UF_Address = UF_Address
# ------------------- STANDARD ADDRESS MISMATCH -----------------------#
    # Get Addresses that are in SV but not in UF
    Set_Only_In_SV = set(L_SV_Address) - set(L_UF_Address)
    # Get Addresses that are in UF but not in SV : This is common scenario and not required
    # Set_Only_In_UF = set(L_UF_Address) - set(L_SV_Address)
    # format the values as it needs to appear in excel
    Set_Only_In_SV_Disp_Txt = set([x.replace(' | ','') + ' appears only in SV \n' for x in Set_Only_In_SV])
    # format the values as it needs to appear in excel
    # Set_Only_In_UF_Disp_Txt = set([x.replace(' | ','') + ' appears in UF \n' for x in Set_Only_In_UF])
    # Union the Standard Address Mismatch Errors
    # Set_Address_diff = Set_Only_In_SV_Disp_Txt.union(Set_Only_In_UF_Disp_Txt)
    # Convert the set to a list
    List_Std_Address_Mismatch = list(Set_Only_In_SV_Disp_Txt)
# ------------------- DUPLICATE EQUIPMENT FEES -----------------------#
    # Create counters for each list 
    SV_Address_Counter = Counter(L_SV_Address)
    # Create counters for each list 
    UF_Address_Counter = Counter(L_UF_Address)
    # SV Addresses where count of SV > count of CLIPS with a minimum count of 1 ie items that are common to both SV and UF
    dict_SV_Address_gt_UF_Address = {k: SV_Address_Counter[k] for k in SV_Address_Counter if k in UF_Address_Counter and SV_Address_Counter[k] > UF_Address_Counter[k] and SV_Address_Counter[k] > 1}
    # format the values as it needs to appear in excel
    L_SV_Duplicate_Addr = [x.replace(' | ','') + f' has {dict_SV_Address_gt_UF_Address[x]} EQP in SV' for x in dict_SV_Address_gt_UF_Address if dict_SV_Address_gt_UF_Address[x] >= 2]
    # format the values as it needs to appear in excel
    L_UF_Duplicate_Addr = [x.replace(' | ','') + f' has {UF_Address_Counter[x]} EQP in UF/CLIPS' for x in UF_Address_Counter if UF_Address_Counter[x] >= 2]
    # Merge the two list
    List_Duplicate_Address = L_SV_Duplicate_Addr + L_UF_Duplicate_Addr
    Final_List_Issues = List_Std_Address_Mismatch+ List_Duplicate_Address
    ReturnString = '\n'.join(Final_List_Issues)
    return(ReturnString)
    
    
#


df_RCA_Ethernet_Combined['RCA_COMMENTS'] = df_RCA_Ethernet_Combined['LEGACY_CUST_ID'].apply(StandardAddressMismatch) 
#
# def CheckUFAddrStandardization(param_Cust_ID):
#     ReturnVal = 0
#     UF_Address = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['LEGACY_CUST_ID'] == param_Cust_ID]['UF_ADDRESS'].astype('str').iloc
#     L_UF_Address = UF_Address.split('\n')
#     # print(L_UF_Address)
#     # Std_Add_Mismatch_Reason = [x for x in L_UF_Address if x.endswith('-  | ')]
#     count_Std_Add_Mismatch = sum([1 for x in L_UF_Address if x.endswith('-  | ')])
#     if count_Std_Add_Mismatch >=1:
#         ReturnVal = 1
#     return(count_Std_Add_Mismatch)
#
#df_RCA_Ethernet_Combined['Site Address Standardization issue'] = df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee') & (df_RCA_Ethernet_Combined['Std Address Mismatch'] == 0 ) & (df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 0 ) &  (df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 0 )]['LEGACY_CUST_ID'].apply(CheckUFAddrStandardization)
#
#df_RCA_Ethernet_Combined['Site Address Standardization issue'].fillna(0,inplace = True)
#
df_RCA_Ethernet_debug = df_RCA_Ethernet_Combined.copy(deep =True)
#
df_RCA_Ethernet_Combined.drop(columns= ['SV_ADDRESS','UF_ADDRESS','CUST_ID','CUST_ID_INT'], inplace = True)
#
df_RCA_Ethernet_Combined.columns


#
#df_RCA_Ethernet_Combined

#

#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains(" -  appears only in SV ")] 
#df_RCA_Ethernet_Combined.query('RCA_COMMENTS.str.contains("appears only in SV")')
#df_RCA_Ethernet_Combined[~df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV ")]
#test_df[test_df['RCA_COMMENTS'].str.contains("appears only in SV ")]
#output_path = r'R11.FVT5 - Cycle 2_DIFF_CM030-P_Test.xlsx'
#test_df=
#df_RCA_Ethernet_Combined[~df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV ")].query('RCA_COMMENTS.str.contains("appears only in SV")', engine='python')
#writer = pd.ExcelWriter(output_path)
#test_df.to_excel(writer,sheet_name='RCA')
#test_df.to_excel(writer, 'Sheet 1', index=False) 
#writer.save()
#writer.close()
#df_RCA_Ethernet_Combined[~df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV ")].query('RCA_COMMENTS.str.contains("appears only in SV")', engine='python')
#df_RCA_Ethernet_Combined.query('RCA_COMMENTS.str.contains("appears only in SV")', engine='python')
#df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("appears only in SV") == True) & (df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV") == False)]

print("Out of UDF ADDRESS abd Comments!")

df_RCA_Ethernet_Combined['Incorrect_Service_Address']=np.where(
    (df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("appears only in SV") == True) & (df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV") == False), 1, df_RCA_Ethernet_Combined['Incorrect_Service_Address']
)


# test_df=df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("appears only in SV") == True) & (df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV") == False)]
# output_path = r'R11.FVT5 - Cycle 2_DIFF_CM030-P_Test.xlsx'
# writer = pd.ExcelWriter(output_path)
# test_df.to_excel(writer,sheet_name='RCA')
# test_df.to_excel(writer, 'Sheet 1', index=False) 
# writer.save()
# writer.close()
#
#df_RCA_Ethernet_Combined
df_RCA_Ethernet_Combined['RCA_COMMENTS']=np.where(
    (df_RCA_Ethernet_Combined['PRODUCT_OFFER'].str.contains("Equipment Fee") == False) , '', df_RCA_Ethernet_Combined['RCA_COMMENTS']
)


df_RCA_Ethernet_Combined['COMBINED_ADDRESS']=np.where(
    (df_RCA_Ethernet_Combined['PRODUCT_OFFER'].str.contains("Equipment Fee") == False) , '', df_RCA_Ethernet_Combined['COMBINED_ADDRESS']
)


df_RCA_Ethernet_Combined['Incorrect_Service_Address']=np.where(
    (df_RCA_Ethernet_Combined['PRODUCT_OFFER'].str.contains("Equipment Fee") == False) , 0, df_RCA_Ethernet_Combined['Incorrect_Service_Address']
)


# df_RCA_Ethernet_Combined['Std Address Mismatch']=np.where(
#     (df_RCA_Ethernet_Combined['Incorrect_Service_Address']==1 ) , 0, df_RCA_Ethernet_Combined['Std Address Mismatch']
# )
# df_RCA_Ethernet_Combined['Std Address Mismatch']=np.where(
#     (df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView']==1 ) , 0, df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView']
# )
#


df_RCA_Ethernet_Combined['Incorrect_Service_Address']=np.where(
    (df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 1) & ~(df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee') , 0, df_RCA_Ethernet_Combined['Incorrect_Service_Address']
)


#

#Bhoomi - commented UNI and EQP Fee on different invoice account nos in SingleView - 05-09-2022
#df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView']=np.where(
 #   (df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 1) & ~(df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee') , 0, df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView']
#)


#


#df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 1) & (df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee')]
#df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 1) & ~(df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee')]
#df_RCA_Ethernet_Combined

#

#output_path = 'R13.MR1 - Cycle-2_DIFF_CM030-P_STD_ADDR.xlsx'
#writer = pd.ExcelWriter(output_path)
#df_RCA_Ethernet_Combined.to_excel(writer,sheet_name='RCA', index=False)
#writer.save()
#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'] == '900021287']
#

df_RCA_Ethernet_Combined.loc[df_RCA_Ethernet_Combined.Incorrect_Service_Address == 1, 'Std Address Mismatch'] = 0

#

# SearchString="Ethernet"
# df_RCA_Ethernet_Combined[~df_RCA_Ethernet_Combined["PRODUCT_OFFER"].str.contains(SearchString)]
#df_RCA_Ethernet_Combined

df_RCA_Ethernet_Combined.loc[df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 1, 'Std Address Mismatch'] = 0

#
#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['Incorrect_Service_Address']==1]
#.str.contains(" -  appears only in SV ")
# df_RCA_Ethernet_Combined['Incorrect_Service_Address']=np.where(
#     (df_RCA_Ethernet_Combined['Incorrect_Service_Address'].str.contains("1") == True) & (df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV") == True), 0, df_RCA_Ethernet_Combined['Incorrect_Service_Address']
# )


#test_df= df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['Incorrect_Service_Address']==1) & (df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV") == True)]
# df_RCA_Ethernet_Combined['Incorrect_Service_Address']=np.where(
# (df_RCA_Ethernet_Combined['Incorrect_Service_Address']==1) & 
# (df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV") == True)
#     , 0, df_RCA_Ethernet_Combined['Incorrect_Service_Address']
# )


# test_df = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV") == True]
# output_path = r'R11.FVT5 - Cycle 2_DIFF_CM030-P_01.xlsx'
# writer = pd.ExcelWriter(output_path)
# test_df.to_excel(writer,sheet_name='RCA')
# #test_df.to_excel(writer, 'Sheet 1', index=False) 
# writer.save()
# writer.close()
#


df_RCA_Ethernet_Combined['RCA_COMMENT_WORD_COUNT'] = df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.len()

#

df_RCA_Ethernet_Combined['Incorrect_Service_Address']=np.where(
(df_RCA_Ethernet_Combined['Incorrect_Service_Address']==1) & 
#(df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.contains("-  appears only in SV") == True)
(df_RCA_Ethernet_Combined['RCA_COMMENT_WORD_COUNT'] == 24)
    , 0, df_RCA_Ethernet_Combined['Incorrect_Service_Address']
)


#
#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['RCA_COMMENT_WORD_COUNT'] == 24]
#

#df_Root_Account_List
#

df_RCA_Ethernet_Combined['LEGACY_CUST_ID']=df_RCA_Ethernet_Combined['LEGACY_CUST_ID'].astype('int64')
df_Root_Account_List['LEGACY_CUST_ID']=df_Root_Account_List['LEGACY_CUST_ID'].astype('int64')

#df_RCA_Ethernet_Combined["SV_ROOT_ACCOUNT_NO"] = ""

df_RCA_Ethernet_Combined = pd.merge(df_RCA_Ethernet_Combined,df_Root_Account_List,
                                   on="LEGACY_CUST_ID",
                                   how="left")
                                   
#
# df_RCA_Ethernet_Combined.rename(columns={'LEGACY_CUST_ID':'LEGACY_ACCOUNT_NO '}, 
#                  inplace=True)


#df_RCA_Ethernet_Combined
#df_Root_Account_List
#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO']=="939042552"]
#df_Circuit_Mismatch[df_Circuit_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64').astype(str)==939042552]


df_RCA_Ethernet_Combined.rename(columns = {'LEGACY_CUST_ID':'LEGACY_ACCOUNT_NO'}, inplace = True)
df_RCA_Ethernet_Combined.rename(columns = {'SV_MRC':'SV/CSG MRC'}, inplace = True)


#
#df_RCA_Ethernet_Combined

#
# Circuit Mismatch

df_RCA_Ethernet_Combined['CIRCUIT MISMATCH'] = np.where(df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].isin(df_Circuit_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64')),1,0) | np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].isin(df_Circuit_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64').astype(str)),1,0)
#



# Circuit Mismatch for ME products
df_RCA_Ethernet_Combined['CIRCUIT MISMATCH'] = np.where(
                                                        (df_RCA_Ethernet_Combined['CIRCUIT MISMATCH'] == 1) &  
                                                        df_RCA_Ethernet_Combined['PRODUCT_OFFER'].isin(df_ME_SERVICES['SERVICE_CODE'])
                                                   ,1,0)
                                                   
                                                   
#
# Account Mismatch--> OPEN ITEM ALWAYS RPT goes ROOT A/C
#df_Diff_Src['ACCT MISMATCH'] = np.where(df_Diff_Src['LEGACY_CUST_ID'].isin(df_Account_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64')),1,0)
df_RCA_Ethernet_Combined['ACCOUNT MISMATCH'] = np.where(df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].isin(df_Account_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64')),1,0) | np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].isin(df_Account_Mismatch['CUSTOMER_ACCOUT_NUMBER'].astype('int64').astype(str)),1,0)


#
# Account Mismatch for ME products
df_RCA_Ethernet_Combined['ACCOUNT MISMATCH'] = np.where(
                                                        (df_RCA_Ethernet_Combined['ACCOUNT MISMATCH'] == 1) &  
                                                        df_RCA_Ethernet_Combined['PRODUCT_OFFER'].isin(df_ME_SERVICES['SERVICE_CODE'])
                                                   ,1,0)
#


#print(df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].isnull()])
sv_root_zero=df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].isnull()]


#
sv_zero_row_count=sv_root_zero.shape
#


#df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'] = pd.to_numeric(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'], errors='coerce')
#print(df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO']==0])
#


if sv_zero_row_count[1] > 0:
    time_desc = "/*MRC Execution date: " + str(datetime.datetime.now()) + "*/" + str("\n")
    with open(r'MRC_LogFile.txt', 'w') as logfile:
         logfile.write(time_desc)
    logfile.close()
    
    desc = "SingleView records with 0 as account #!" + str("\n")
    
    with open(r'MRC_LogFile.txt', 'a') as logfile:
         logfile.write(desc)
    logfile.close()
    
    with open(r'MRC_LogFile.txt', 'a') as logfile:
         logfile.write(str(sv_root_zero))
    logfile.close()
    
print("Log files done!")  
#
df_RCA_Ethernet_Combined["SV_ROOT_ACCOUNT_NO"]= df_RCA_Ethernet_Combined["SV_ROOT_ACCOUNT_NO"].fillna(0).astype('int64')

#

df_Missing_BI_Equipment["LEGACY_CUST_ID"]= df_Missing_BI_Equipment["LEGACY_CUST_ID"].fillna(0).astype('int64')

#

df_RCA_Ethernet_Combined['Missing BI Equipment'] = np.where((df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Missing_BI_Equipment['LEGACY_CUST_ID'].astype('int64'))) & (df_Diff_Src['PRODUCT_OFFER'] == 'Equipment Fee'),1,0)

#

df_RCA_Ethernet_Combined['Non-Flagged']=0

#
#Shiva: 09-13-2021 We will Flag "Non-Flagged" column only for the below columns:
#Rounding Issue|Missing BI Equipment|Missing services in UF_SUBSCRIBER|Std Address Mismatch|Incorrect_Service_Address|
#Duplicate Equipment Fee in SingleView|MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS|Site Address Standardization issue|
#IPV4 P2P FALLOUT|UNI and EQP Fee on different invoice account nos in SingleView
# df_RCA-Ethernet_Combined['Non-Flagged']=np.where(
#     (df_RCA_Ethernet_Combined['Rounding Issue'] == 0) & 	(df_RCA_Ethernet_Combined['Missing BI Equipment'] == 0) & 	(df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 0) & 	(df_RCA_Ethernet_Combined['Std Address Mismatch'] == 0) & 	(df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 0) & 	(df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 0) & 	(df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 0) & 	(df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 0) & 	(df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 0) & 	(df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 0) & 	(df_RCA_Ethernet_Combined['ACCOUNT MISMATCH'] == 0) & 	(df_RCA_Ethernet_Combined['CIRCUIT MISMATCH'] == 0) & 	(df_RCA_Ethernet_Combined['EXTRACT REVIEW'] == 0)
#     ,1,df_RCA_Ethernet_Combined['Non-Flagged'])
#-->Removed : & 	(df_RCA_Ethernet_Combined['ACCOUNT MISMATCH'] == 0) & 	(df_RCA_Ethernet_Combined['CIRCUIT MISMATCH'] == 0) & 	(df_RCA_Ethernet_Combined['EXTRACT REVIEW'] == 0)


df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Rounding Issue'] == 0) & 	(df_RCA_Ethernet_Combined['Missing BI Equipment'] == 0) & 	(df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 0) & 	(df_RCA_Ethernet_Combined['Std Address Mismatch'] == 0) & 	(df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 0) & 	(df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 0) & 	(df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 0) & 	(df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 0) & 	(df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 0) 
    ,1,df_RCA_Ethernet_Combined['Non-Flagged'])
    
#Bhoomi - & (df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 0)   -- removed from above condition  
    
    
#
#Get the names of indexes for the below condition and drop in the dataset
df_RCA_Ethernet_Combined_DELETED=df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Installation Fee']
df_RCA_Ethernet_Combined.drop(df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Installation Fee'].index, inplace = True)


#
#If none of THE BELOW COLUMNS are flagged, and if RCA comments has only "-  appears only in SV" then flag as Std Address Mismatch
#Rounding Issue|Missing BI Equipment|Missing services in UF_SUBSCRIBER|Std Address Mismatch|Incorrect_Service_Address|
#Duplicate Equipment Fee in SingleView|MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS|Site Address Standardization issue|
#IPV4 P2P FALLOUT|UNI and EQP Fee on different invoice account nos in SingleView


df_RCA_Ethernet_Combined['Std Address Mismatch']=np.where(
    (df_RCA_Ethernet_Combined['RCA_COMMENT_WORD_COUNT'] == 24) & (df_RCA_Ethernet_Combined['Non-Flagged'] == 1)
    & (df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == "Ethernet Equipment Fee")
    ,1,df_RCA_Ethernet_Combined['Std Address Mismatch'])
    
#

#The above will set "nan appears only in SV" RCA Comments as well, so resetting it below:
df_RCA_Ethernet_Combined['Std Address Mismatch']=np.where(
    (df_RCA_Ethernet_Combined['RCA_COMMENTS'].str.match('nan appears only in SV', na=False))
    ,0,df_RCA_Ethernet_Combined['Std Address Mismatch'])
    
#

#Create a new column 'Missing Equipment from CLIPS' for setting the below filter
df_RCA_Ethernet_Combined['Missing Equipment from CLIPS']=0

#

#Create a new column 'Multiple BI Equipment for BI service/same AAN' for setting the below filter
df_RCA_Ethernet_Combined['Multiple BI Equipment for BI service or same AAN']=0


#
#



#
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Rounding Issue'] == 0) & 	(df_RCA_Ethernet_Combined['Missing BI Equipment'] == 0) & 	(df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 0) & 	(df_RCA_Ethernet_Combined['Std Address Mismatch'] == 0) & 	(df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 0) & 	(df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 0) & 	(df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 0) & 	(df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 0) & 	(df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 0) 
    ,1,df_RCA_Ethernet_Combined['Non-Flagged'])
#Bhoomi - Removed from above condition &(df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 0) - 05/09/2022
    
#
#If none of THE BELOW COLUMNS are flagged, and if RCA comments is blank  then flag as Missing Equipment from CLIPS
#Shiva: 09-30-2021 Disabling this FLAG, as it is not getting flagged as expected.
# df_RCA_Ethernet_Combined['Missing Equipment from CLIPS']=np.where(
#  (df_RCA_Ethernet_Combined['RCA_COMMENT_WORD_COUNT'] == 0) & (df_RCA_Ethernet_Combined['RCA_COMMENTS']== "") 
#  & (df_RCA_Ethernet_Combined['Non-Flagged'] == 1) 
#  & (df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee') 
#  ,1,df_RCA_Ethernet_Combined['Missing Equipment from CLIPS'])
#


df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Rounding Issue'] == 0) & 	(df_RCA_Ethernet_Combined['Missing BI Equipment'] == 0) &  	(df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 0) & 	(df_RCA_Ethernet_Combined['Std Address Mismatch'] == 0) & 	(df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 0) & 	(df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 0) & 	(df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 0) & 	(df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 0) & 	(df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 0) 
    | (df_RCA_Ethernet_Combined['Missing Equipment from CLIPS']== 0)
    ,1,df_RCA_Ethernet_Combined['Non-Flagged'])
    
#Bhoomi - removed from above condition & (df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 0)

df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Rounding Issue'] == 1) | 	(df_RCA_Ethernet_Combined['Missing BI Equipment'] == 1) 
    | 	(df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 1) | 	(df_RCA_Ethernet_Combined['Std Address Mismatch'] == 1) 
    | 	(df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 1) | 	(df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 1) 
    | 	(df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 1) 
   # |(df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 1)
    | 	(df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 1) | 	(df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 1) 
    | (df_RCA_Ethernet_Combined['Missing Equipment from CLIPS']== 1) 
    ,0,df_RCA_Ethernet_Combined['Non-Flagged'])
    


#f_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['Non-Flagged'] == 1]
#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['Missing Equipment from CLIPS']== 1]

#

# df_RCA_Ethernet_Combined[
# (df_RCA_Ethernet_Combined['Rounding Issue'] == 0) & 	(df_RCA_Ethernet_Combined['Missing BI Equipment'] == 0) & 	(df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 0) & 	(df_RCA_Ethernet_Combined['Std Address Mismatch'] == 0) & 	(df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 0) & 	(df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 0) & 	(df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 0) & 	(df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 0) & 	(df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 0) & 	(df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 0) 
# ]

#

#Shiva: 09-30-2021 For ALL 'Missing BI Equipment' flagged, we need to set flag as '1' to all -ve rows to 
#the 'Multiple BI Equipment for BI service/same AAN' flag
df_RCA_Ethernet_Combined['Multiple BI Equipment for BI service or same AAN']=np.where(
    (df_RCA_Ethernet_Combined['Missing BI Equipment'] == 1) & (df_RCA_Ethernet_Combined['MRC_DIFF'] < 0)
    ,1,df_RCA_Ethernet_Combined['Multiple BI Equipment for BI service or same AAN'])
    
  


  
#df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['Missing BI Equipment'] == 1) & (df_RCA_Ethernet_Combined['MRC_DIFF'] < 0)]

#

#Shiva: 09-30-2021 RESETTING 'Missing BI Equipment' flagged to remove the above conflict
df_RCA_Ethernet_Combined['MRC Outliers for extract review']=np.where(
    (df_RCA_Ethernet_Combined['Rounding Issue'] == 1) & 
    (df_RCA_Ethernet_Combined['MRC Outliers for extract review'] == 1)
    ,0,df_RCA_Ethernet_Combined['MRC Outliers for extract review'])
    
#

df_AM = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['ACCOUNT MISMATCH']==1]
Cols_AM=['SV_ROOT_ACCOUNT_NO','PRODUCT_OFFER','ACCOUNT MISMATCH']
df_AM=df_AM[Cols_AM]

#

#Cols_CM_AM_FLTR=['LEGACY_CUST_ID','SERVICE_CODE']


#Shiva 10/25: Add only SERVICE_CODE as a part of filter
Cols_CM_AM_FLTR=['SERVICE_CODE']
df_CM_AM_ADD_FLTR=df_CM_AM_ADD_FILTER[Cols_CM_AM_FLTR]


#df_CM_AM_ADD_FLTR
#


Excluded_Codes=['Security Edge','Cloud Solutions – Webhost','Cloud Solutions - Office 365',
                'Cloud Solutions - E-FAX','Cloud Solutions - Norton','Unreturned DCT','Unreturned DTA',
                'WifiPro Access Point Expanded Coverage''Cloud Solutions - Office 365',
                'Cloud Solutions - E-FAX','Web Hosting Business','WifiPro Access Point Expanded Coverage',
                'Cloud Solutions - Webhost','Cloud Solutions - Norton',
                'Hospitality Full Fetaured Line','Hospitality Internet Deluxe100+',
                'Hospitality Voice Equipment Fee','Cloud Solutions - Carbonite','TV Additional Outlet',
                'Hospitality Internet Equipment Fee','Cloud Solutions - VISIO','Cloud Solutions - Docusign',
                'Cloud Solutions - Carbonite Po','Wifi Pro','VES Desk Phone','Cloud Solutions -  F-Secure',
                'Unreturned Modem Fee','HD Technology Fee','Unreturned Equipment Security Equipment','Bandwidth - Basic']
                
#Excluded_Codes

#

df_RCA_Ethernet_Combined['Unsupported product'] = np.where((df_RCA_Ethernet_Combined.isin(Excluded_Codes).any(1).astype(int) & df_RCA_Ethernet_Combined['CSG(Y/N)'].str.contains("Y") == True),1,0)

#

Unsupported_SV_Prds=[
'SIP Trunk',
'BTV SIP Trunk Group',
'BTV PRI Trunk Group',
'BTV Vanity Numbers',
'Trunk BTN',
'SIP BTN',
'CDR'
]


#Unsupported_SV_Prds

#

df_RCA_Ethernet_Combined['Unsupported SV product'] = df_RCA_Ethernet_Combined.isin(Unsupported_SV_Prds).any(1).astype(int)

#


#Shiva 10/25: Add only SERVICE_CODE as a part of filter
#df_RCA_Ethernet_Combined['ACC_SER_CNCT'] = df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype(str)+'_'+df_RCA_Ethernet_Combined['PRODUCT_OFFER']
#


#Shiva 10/25: Add only SERVICE_CODE as a part of filter
#df_CM_AM_ADD_FLTR['ACC_SER_CNCT'] = df_CM_AM_ADD_FLTR['LEGACY_CUST_ID'].astype(str)+'_'+df_CM_AM_ADD_FLTR['SERVICE_CODE']
#


#df_RCA_Ethernet_Combined['ACC_SER_CNCT']
#df_CM_AM_ADD_FLTR['ACC_SER_CNCT']
#df_CM_AM_ADD_FLTR
#


#Shiva 10/25: Add only SERVICE_CODE as a part of filter
#df_RCA_Ethernet_Combined['ACCOUNT_MISMATCH'] = np.where((df_RCA_Ethernet_Combined.ACC_SER_CNCT.isin(df_CM_AM_ADD_FLTR.ACC_SER_CNCT)) & (df_RCA_Ethernet_Combined['ACCOUNT MISMATCH'] == 1),1,0)
df_RCA_Ethernet_Combined['ACCOUNT_MISMATCH'] = np.where((df_RCA_Ethernet_Combined.PRODUCT_OFFER.isin(df_CM_AM_ADD_FLTR.SERVICE_CODE)) & (df_RCA_Ethernet_Combined['ACCOUNT MISMATCH'] == 1),1,0)

#


#Shiva 10/25: Add only SERVICE_CODE as a part of filter
#df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] = np.where((df_RCA_Ethernet_Combined.ACC_SER_CNCT.isin(df_CM_AM_ADD_FLTR.ACC_SER_CNCT)) & (df_RCA_Ethernet_Combined['CIRCUIT MISMATCH'] == 1),1,0)
df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] = np.where((df_RCA_Ethernet_Combined.PRODUCT_OFFER.isin(df_CM_AM_ADD_FLTR.SERVICE_CODE)) & (df_RCA_Ethernet_Combined['CIRCUIT MISMATCH'] == 1),1,0)

#

df_RCA_Ethernet_Combined["LEGACY_ACCOUNT_NO"]= df_RCA_Ethernet_Combined["LEGACY_ACCOUNT_NO"].fillna(0).astype('int64')
df_Same_ActiveCore_Mult_EQPs["LEGACY_ACCOUNT_NO"]= df_Same_ActiveCore_Mult_EQPs["LEGACY_ACCOUNT_NO"].fillna(0).astype('int64')

#

ActiveCore_SRV_Codes=['Router Equipment Fee - Large', 'Router Equipment Fee - Medium', 'Router Equipment Fee - Small',
                 'uCPE Equipment Fee - Large', 'uCPE Equipment Fee - Medium', 'uCPE Equipment Fee - Small']
#


#Shiva: 01/28/2022 For the same activecore service, we have multiple EQPs from CLIPS
df_RCA_Ethernet_Combined['Same ActiveCore services, with multiple EQPs from CLIPS'] = np.where(
    (df_RCA_Ethernet_Combined.LEGACY_ACCOUNT_NO.isin(df_Same_ActiveCore_Mult_EQPs.LEGACY_ACCOUNT_NO)) & 
    (df_RCA_Ethernet_Combined.PRODUCT_OFFER.isin(ActiveCore_SRV_Codes)) & 
    (df_RCA_Ethernet_Combined['MRC_DIFF'] < 0) & 
    (df_RCA_Ethernet_Combined['Non-Flagged'] == 1),1,0)
    
    
#


#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['Same ActiveCore services, with multiple EQPs from CLIPS']==1]
#df_Same_ActiveCore_Mult_EQPs[df_Same_ActiveCore_Mult_EQPs['LEGACY_ACCOUNT_NO']==932763759]
#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO']==932763759]
#df_RCA_Ethernet_Combined
#


#df_RCA_Ethernet_Combined['ACCOUNT_MISMATCH'] = np.where((df_RCA_Ethernet_Combined.SV_ROOT_ACCOUNT_NO.isin(df_CM_AM_ADD_FLTR.LEGACY_CUST_ID)) & (df_RCA_Ethernet_Combined.PRODUCT_OFFER.isin(df_CM_AM_ADD_FLTR.SERVICE_CODE)) & (df_RCA_Ethernet_Combined['ACCOUNT MISMATCH'] == 1),1,0)
#


#df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] = np.where((df_RCA_Ethernet_Combined.SV_ROOT_ACCOUNT_NO.isin(df_CM_AM_ADD_FLTR.LEGACY_CUST_ID)) & (df_RCA_Ethernet_Combined.PRODUCT_OFFER.isin(df_CM_AM_ADD_FLTR.SERVICE_CODE)) & (df_RCA_Ethernet_Combined['CIRCUIT MISMATCH'] == 1),1,0)
#


#df_CM_AM_ADD_FLTR[df_CM_AM_ADD_FLTR['LEGACY_CUST_ID']=='932768674']
#
#df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] == 1)] 
#





#Shiva: 12-17-2021 For Unsupported product for '1' --> RESET the non-flag to '0'
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Unsupported product'] == 1)
    ,0,df_RCA_Ethernet_Combined['Non-Flagged'])
    
    
#


#Shiva: 12-17-2021 For CSG EXCLUSIONS
df_CSG_MRC_EXLUSIONS['ACCOUNT_NAME'] = df_CSG_MRC_EXLUSIONS['ACCOUNT_NAME'].astype('int64')


# df_RCA_Ethernet_Combined = pd.merge(df_RCA_Ethernet_Combined,df_CSG_MRC_EXLUSIONS,how = 'left', 
# left_on= ['LEGACY_ACCOUNT_NO','PRODUCT_OFFER'], right_on=['ACCOUNT_NAME','PRODUCT_NAME'] , suffixes=('_Source','_EX_PRD'))


df_RCA_Ethernet_Combined = pd.merge(df_RCA_Ethernet_Combined,df_CSG_MRC_EXLUSIONS,how = 'left', 
left_on= ['LEGACY_ACCOUNT_NO'], right_on=['ACCOUNT_NAME'] , suffixes=('_Source','_EX_PRD'))

# df_RCA_Ethernet_Combined = pd.merge(df_RCA_Ethernet_Combined,df_CSG_MRC_EXLUSIONS,how = 'left', 
# left_on= ['LEGACY_ACCOUNT_NO'], right_on=['ACCOUNT_NAME'] , suffixes=('_Source','_EX_PRD'))

#

CSG_Package = ['Package']


#CSG_Package
#
#df_pkgd_aan
#


#Shiva: 12-17-2021 Setting package exclusions
#Removing the below filter to map only for AAN's
#df_RCA_Ethernet_Combined['Package exclusions for CSG'] = df_RCA_Ethernet_Combined.isin(CSG_Package).any(1).astype(int)
#df_pkgd_aan = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['EXCLUSION'] == 'Package']
#
#df_pkgd_aan_leg_accs
#df_pkgd_aan
df_RCA_Ethernet_Combined['EXCLUSION'] 
filtered_df = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['EXCLUSION'].notnull()]
filtered_df[filtered_df['EXCLUSION'].str.contains("Package")]
filtered_df


#

df_pkgd_aan_leg_accs = filtered_df[['LEGACY_ACCOUNT_NO']]
#

# Check for P2P : The sum in P2P Consolidated should match with P2P values in RCA files
# df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['RCA_IPV4_P2P_Fallout'] ==1]
#df_RCA_Ethernet_Combined
df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO']=df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].astype(str)
df_pkgd_aan_leg_accs['LEGACY_ACCOUNT_NO']=df_pkgd_aan_leg_accs['LEGACY_ACCOUNT_NO'].astype(str)
#


#df_pkgd_aan_leg_accs
#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['Package exclusions for CSG'] == 1]

#

#Shiva: Addded this new feature on 01-05-2022 x--x Package exclusions for CSG
df_RCA_Ethernet_Combined['Package exclusions for CSG'] = np.where(
    (df_RCA_Ethernet_Combined.LEGACY_ACCOUNT_NO.isin(df_pkgd_aan_leg_accs.LEGACY_ACCOUNT_NO))
#01-10-2022 Applying the Package across any other filters
#    & (df_RCA_Ethernet_Combined['Non-Flagged'] == 1)
    ,1,0)
    
#

#Shiva: 12-17-2021 Re-setting package exclusions
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Package exclusions for CSG'] == 1)
    ,0,df_RCA_Ethernet_Combined['Non-Flagged'])
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Unsupported product'] == 1)
    ,0,df_RCA_Ethernet_Combined['Non-Flagged'])
    
#Bhoomi - Multiple BI on Same Site
df_RCA_Ethernet_Combined['Multiple BI on Same Site'] = np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Multiple_BI_On_Same_Site['LEGACY_CUST_ID'].astype('int64')),1,0)

#Bhoomi - Missing Wifi Pro Access Point Equipment
df_RCA_Ethernet_Combined['Missing Wifi Pro Access Point Equipment'] = np.where(df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].astype('int64').isin(df_Missing_Wifi_Pro_Access_Point_Eqp['LEGACY_ACCOUNT_NO'].astype('int64')),1,0)


#Bhoomi - flag Equipment Double Billing - 05172022
df_RCA_Ethernet_Combined['Equipment Double Billing'] = np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Query_Equipment_Double_Billing['ROOT_ACCOUNT_NAME'].astype('int64')),1,0) & np.where((df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Equipment Fee'),1,0)



#Bhoomi - Rename Missing PRI Service ROOT_ACCOUNT_NAME to ACCOUNT_NAME
df_Missing_PRI_Service.rename(columns = {'ROOT_ACCOUNT_NAME':'ACCOUNT_NAME'}, inplace = True)

#Bhoomi - merge Missing PRI Service SQL resultset and PRI Accounts query(join Cmo30 union table and base.list_of_acc table)
df_PRI_Merge_Accounts = pd.merge(df_Missing_PRI_Service,df_Query_PRI_Accounts, how='outer', indicator=True)

#Bhoomi - finding df_Query_PRI_Accounts only using right only
df_right = df_PRI_Merge_Accounts[df_PRI_Merge_Accounts['_merge']=='right_only'][df_Query_PRI_Accounts.columns]

#Bhoomi - Flag Missing PRI Service
df_RCA_Ethernet_Combined['Missing PRI Service'] = np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_right['ACCOUNT_NAME'].astype('int64')),1,0) & np.where((df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Business TFN'),1,0)



#Bhoomi - if the below condition will not flag the RCA column then use above 3 condition - 05-09-2022
#df_RCA_Ethernet_Combined['Missing PRI Service'] = np.where(~df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Missing_PRI_Service['ROOT_ACCOUNT_NAME'].astype('int64')) & (df_RCA_Ethernet_Combined['SV_PRODUCT_INDICATOR'].str.contains("PRI") == True),1,0)

#Bhoomi - Flag Incorrect EQP fee tagging in CSG - 05132022
df_RCA_Ethernet_Combined['Incorrect EQP fee tagging in CSG'] = np.where(df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].astype('int64').isin(df_Query_Incorrect_EQP_Fee_Tagging_in_CSG['SUB_ACCT_NO_SBB'].astype('int64')),1,0) 


#Bhoomi - Query_CSG_MRC_Cleanup
df_RCA_Ethernet_Combined['CSG MRC Cleanup'] = np.where(df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].astype('int64').isin(df_Query_CSG_MRC_Cleanup['SUB_ACCT_NO_SBB'].astype('int64')),1,0)




 




df_RCA_Ethernet_Combined.rename(columns={'EXCLUSION': 'EXCLUSION(CSG)','Unsupported product':'Unsupported product(CSG)'}, inplace=True)

#
#Bhoomi - 04-27-2022 - added LEGACY_ACCOUNT_NO and SV_ROOT_ACCOUNT_NO in one condition so we can cover all the scenario
#df_RCA_Ethernet_Combined['Multiple AAN tagged to same BVE service'] = np.where(df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].isin(df_Multiple_AAN_tagged_same_BVE_service['LEGACY_CUST_ID'].astype('int64')),1,0)| np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].isin(df_Multiple_AAN_tagged_same_BVE_service['LEGACY_CUST_ID'].astype('int64')),1,0)

#Bhoomi - 06-17-2022 - moving below logic to after belo condition so can add filter on SV product indicator
#df_RCA_Ethernet_Combined['Multiple AAN tagged to same BVE service'] = np.where(df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].isin(df_Multiple_AAN_tagged_same_BVE_service['LEGACY_CUST_ID'].astype('int64')),1,0)| np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].isin(df_Multiple_AAN_tagged_same_BVE_service['LEGACY_CUST_ID'].astype('int64')),1,0) & np.where((df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'BVE Equipment Fee') | (df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'BVE Station'),1,0)


#
#df_RCA_Ethernet_Combined
df_PRD_IND_List['CUSTOMER_ACCOUT_NUMBER'] = df_PRD_IND_List['CUSTOMER_ACCOUT_NUMBER'].astype('int64')
df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'] = df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64')
df_RCA_Ethernet_Combined = pd.merge(df_RCA_Ethernet_Combined,df_PRD_IND_List,how = 'left', 
left_on= ['SV_ROOT_ACCOUNT_NO'], right_on=['CUSTOMER_ACCOUT_NUMBER'] , suffixes=('_Source','_PRD_IND'))
#

#Shiva: Addded this new feature on 01-05-2022 x--x Multiple AAN tagged to same BVE service, 
#Bhoomi -04-21-2022 - used df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'] instead of df_RCA_Ethernet_Combined.SV_ROOT_ACCOUNT_NO. 
#Bhoomi - not using below condition anymore
#df_RCA_Ethernet_Combined['Multiple AAN tagged to same BVE service'] = np.where(
#(df_RCA_Ethernet_Combined.SV_ROOT_ACCOUNT_NO.isin(df_Multiple_AAN_tagged_same_BVE_service.LEGACY_CUST_ID)) & 
#(df_RCA_Ethernet_Combined['Non-Flagged'] == 1),1,0)



#Bhoomi - 05-03-2022 - Adding New Multiple Voice to one AAN -> 06-16-2022 - updated logic and addded filter condition on SV Product indicator and flagging only for negative MRC Diff accounts - Multiple Voice to one AAN
df_RCA_Ethernet_Combined['Negative_MRC_Account_No_multiple_voice'] = df_RCA_Ethernet_Combined.SV_ROOT_ACCOUNT_NO[df_RCA_Ethernet_Combined['MRC_DIFF'] < 0] 

df_Negative_MRC_Acc_No_multiple_voice_without_NAN = df_RCA_Ethernet_Combined['Negative_MRC_Account_No_multiple_voice'].dropna()

df_Negative_MRC_Acc_No_multiple_voice_without_dup = df_Negative_MRC_Acc_No_multiple_voice_without_NAN.drop_duplicates()

df_RCA_Ethernet_Combined['Multiple Voice to one AAN']=np.where((df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Multiple_Voice_To_One_AAN['SV_ROOT_BAN'].astype('int64')) & df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Negative_MRC_Acc_No_multiple_voice_without_dup.astype('int64')) & df_RCA_Ethernet_Combined['SV_PRODUCT_INDICATOR'].str.contains('PRI|SIP|BVE') & df_RCA_Ethernet_Combined['UF_MRC'] != 0),1,0)


#Bhoomi - 06-16-2022 - Upaded logic and addded filter condition on SV Product indicator and flagging only for negative MRC Diff accounts --Multiple AAN tagged to same Voice services
#df_RCA_Ethernet_Combined['Multiple AAN tagged to same BVE service'] = np.where(df_RCA_Ethernet_Combined['LEGACY_ACCOUNT_NO'].isin(df_Multiple_AAN_tagged_same_BVE_service['LEGACY_CUST_ID'].astype('int64')),1,0)| np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].isin(df_Multiple_AAN_tagged_same_BVE_service['LEGACY_CUST_ID'].astype('int64')),1,0) & np.where((df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'BVE Equipment Fee') | (df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'BVE Station'),1,0)
df_RCA_Ethernet_Combined['Negative_MRC_Account_No_multiple_AAN'] = df_RCA_Ethernet_Combined.SV_ROOT_ACCOUNT_NO[df_RCA_Ethernet_Combined['MRC_DIFF'] < 0] 

df_Negative_MRC_Acc_No_multiple_AAN_without_NAN = df_RCA_Ethernet_Combined['Negative_MRC_Account_No_multiple_AAN'].dropna()

df_Negative_MRC_Acc_No_multiple_AAN_without_dup = df_Negative_MRC_Acc_No_multiple_AAN_without_NAN.drop_duplicates()


df_RCA_Ethernet_Combined['Multiple AAN tagged to same BVE service']=np.where((df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Multiple_AAN_tagged_same_BVE_service['SV_ROOT_BAN'].astype('int64')) & df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Negative_MRC_Acc_No_multiple_AAN_without_dup.astype('int64')) & df_RCA_Ethernet_Combined['SV_PRODUCT_INDICATOR'].str.contains('PRI|SIP|BVE') & df_RCA_Ethernet_Combined['UF_MRC'] != 0),1,0)

#Bhoomi - 12-2-2022 - Add Missing base product flag for Activecore
df_RCA_Ethernet_Combined['Missing Activecore Base Product/Other Cleanup Required']=np.where((df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Activecore_Missing_Base_Product['ACCOUNT_NAME'].astype('int64')) & df_RCA_Ethernet_Combined['SV_PRODUCT_INDICATOR'].str.contains('ACTIVECORE') & ~(df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee') & ~(df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Equipment Fee')),1,0)

#Bhoomi - 08/21/2023 - Add Missing AAN for BVE service
df_RCA_Ethernet_Combined['Missing AAN For BVE Services']=np.where(df_RCA_Ethernet_Combined['SV_ROOT_ACCOUNT_NO'].astype('int64').isin(df_Missing_AAN_BVE_Services['ROOT_ACCOUNT_NAME'].astype('int64')),1,0)



#Bhoomi -05-02-2022 -- Added Missing PRI service set to 0 if it flagged as 1, added 'Incorrect EQP fee tagging in CSG', Added Equipment Double Billing, CSG MRC Cleanup
#Added Multiple Voice to one AAN set to 0 if it flagged as 1
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Rounding Issue'] == 1) | 	(df_RCA_Ethernet_Combined['Missing BI Equipment'] == 1) 
    | 	(df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 1) | 	(df_RCA_Ethernet_Combined['Std Address Mismatch'] == 1) 
    | 	(df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 1) | 	(df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 1) 
    | 	(df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 1) 
    | 	(df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 1) | 	(df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 1) 
    | (df_RCA_Ethernet_Combined['Missing Equipment from CLIPS']== 1) | (df_RCA_Ethernet_Combined['Unsupported product(CSG)']== 1) 
    | (df_RCA_Ethernet_Combined['Package exclusions for CSG']== 1) | (df_RCA_Ethernet_Combined['Multiple BI Equipment for BI service or same AAN']== 1) 
    | (df_RCA_Ethernet_Combined['Multiple AAN tagged to same BVE service']== 1) | (df_RCA_Ethernet_Combined['Same ActiveCore services, with multiple EQPs from CLIPS']== 1)
    | (df_RCA_Ethernet_Combined['Unsupported SV product']==1)
    | (df_RCA_Ethernet_Combined['Missing PRI Service']==1)
    | (df_RCA_Ethernet_Combined['Incorrect EQP fee tagging in CSG']==1)
    | (df_RCA_Ethernet_Combined['CSG MRC Cleanup']==1)
    | (df_RCA_Ethernet_Combined['Multiple BI on Same Site']==1)
    | (df_RCA_Ethernet_Combined['Missing Wifi Pro Access Point Equipment']==1)
    | (df_RCA_Ethernet_Combined['Equipment Double Billing']==1)
   # | (df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 1)
    | (df_RCA_Ethernet_Combined['Multiple Voice to one AAN']==1)
    | (df_RCA_Ethernet_Combined['Missing Activecore Base Product/Other Cleanup Required']==1)
    | (df_RCA_Ethernet_Combined['Missing AAN For BVE Services']==1)
    ,0,df_RCA_Ethernet_Combined['Non-Flagged'])
#Bhoomi - Removed from above condition - 


#Shiva: Addded this new feature on 01-05-2022 
#Identify all switch connected to only Underlay UNI (uf_subscriber and uf_sub_associaiton)
#Flag this as separate category for Ethernet EQP Fee mismatch


#Mismatch Category -> SV has EQP MRC for Underlay Switch
df_non_flgd_accs=[]
df_non_flgd_accs = df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['Non-Flagged'] == 1]
#


df_non_flgd_root_accs = df_non_flgd_accs[['SV_ROOT_ACCOUNT_NO']]
#


df_nf_list = df_non_flgd_root_accs.fillna('').values.tolist()   
#


# Create Connection to Oracle
# connection = cx_Oracle.connect(SCHEMA, PASSWORD, connstr,encoding="UTF-8")
# conx_stage = cx_Oracle.connect(STAGE_SCHEMA, STAGE_PASSWORD, connstr,encoding="UTF-8")
#


file = "NON-Flagged Accounts.xlsx"     
tab_name = "NON-Flagged Accounts"
NF_TABLE_NAME = 'X_TEMP_NON_FLGD_ACCT_NOS'
insert_table = "INSERT INTO "+NF_TABLE_NAME+" VALUES (:1)"    
cursor = conx_stage.cursor()   
cursor.executemany(insert_table,df_nf_list)    
cursor.close()
conx_stage.commit()
#


df_SV_EQP_MRC_Underlay_Switch = pd.read_sql(Query_SV_EQP_MRC_Underlay_Switch, con=connection)

#

#df_SV_EQP_MRC_Underlay_Switch

#


#Shiva: Addded this new feature on 01-05-2022 x--x SingleView has EQP MRC for Underlay Switch
df_RCA_Ethernet_Combined['SingleView has EQP MRC for Underlay Switch'] = np.where(
    (df_RCA_Ethernet_Combined.SV_ROOT_ACCOUNT_NO.astype('int64').isin(df_SV_EQP_MRC_Underlay_Switch.LEGACY_CUST_ID.astype('int64'))) & 
    (df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee'),1,0)
    #& (df_RCA_Ethernet_Combined['Non-Flagged'] == 1),1,0)
#Bhoomi - Commented Non_Flagged==1 to flag all SingleView has EQP MRC for Underlay Switch- 05-12-2022



cur = conx_stage.cursor()
cur.execute("TRUNCATE TABLE STAGE.X_TEMP_NON_FLGD_ACCT_NOS")
conx_stage.commit()
#

#Bhoomi -05-02-2022 -- added Missing PRI service set to 0 if it flagged as 1, added 'Incorrect EQP fee tagging in CSG', added CSG MRC Cleanup, Added 'Equipment Double Billing'
# Added Multiple Voice to one AAN set to 0 if it is flagged as 1
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Rounding Issue'] == 1) | (df_RCA_Ethernet_Combined['Missing BI Equipment'] == 1) 
    | (df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 1) 
    | (df_RCA_Ethernet_Combined['Std Address Mismatch'] == 1) 
    | (df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 1) 
    | (df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 1) 
    | (df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 1) 
    | (df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 1) 
    #| (df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 1)
    | (df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 1) 
#    | (df_RCA_Ethernet_Combined['Missing Equipment from CLIPS']== 1) 
    | (df_RCA_Ethernet_Combined['Unsupported product(CSG)']== 1) 
    | (df_RCA_Ethernet_Combined['Package exclusions for CSG']== 1) 
    | (df_RCA_Ethernet_Combined['Multiple BI Equipment for BI service or same AAN']== 1) 
    | (df_RCA_Ethernet_Combined['Multiple AAN tagged to same BVE service']== 1) 
    | (df_RCA_Ethernet_Combined['SingleView has EQP MRC for Underlay Switch']== 1)
    | (df_RCA_Ethernet_Combined['Same ActiveCore services, with multiple EQPs from CLIPS']== 1)
    | (df_RCA_Ethernet_Combined['Incorrect EQP fee tagging in CSG']== 1)
    | (df_RCA_Ethernet_Combined['CSG MRC Cleanup']== 1)
    | (df_RCA_Ethernet_Combined['Multiple BI on Same Site']==1)
    | (df_RCA_Ethernet_Combined['Missing Wifi Pro Access Point Equipment']==1)
    | (df_RCA_Ethernet_Combined['Equipment Double Billing']==1)
    | (df_RCA_Ethernet_Combined['Unsupported SV product']==1)
    | (df_RCA_Ethernet_Combined['Missing PRI Service']==1)
    | (df_RCA_Ethernet_Combined['Multiple Voice to one AAN']==1)
    | (df_RCA_Ethernet_Combined['Missing Activecore Base Product/Other Cleanup Required']==1)
    | (df_RCA_Ethernet_Combined['Missing AAN For BVE Services']==1)
    ,0,df_RCA_Ethernet_Combined['Non-Flagged'])


#Bhoomi -05-02-2022 -- added Missing PRI service set to 1 if it flagged as 0, Added CSG MRC Cleanup, Added Incorrect EQP fee tagging in CSG, Added Multiple Voice to one AAN, Added Equipment Double Billing
#Added Multiple Voice to one AAN set to 1 if it flagged as 0
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['Rounding Issue'] == 0) & (df_RCA_Ethernet_Combined['Missing BI Equipment'] == 0) & 
    (df_RCA_Ethernet_Combined['Missing services in UF_SUBSCRIBER'] == 0) & 
    (df_RCA_Ethernet_Combined['Std Address Mismatch'] == 0) & 
    (df_RCA_Ethernet_Combined['Incorrect_Service_Address'] == 0) & 
    (df_RCA_Ethernet_Combined['Duplicate Equipment Fee in SingleView'] == 0) & 
    (df_RCA_Ethernet_Combined['MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS'] == 0) & 
    (df_RCA_Ethernet_Combined['Site Address Standardization issue'] == 0) & 
    (df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 0) & 
# (df_RCA_Ethernet_Combined['UNI and EQP Fee on different invoice account nos in SingleView'] == 0) &
#    (df_RCA_Ethernet_Combined['Missing Equipment from CLIPS']== 0) &
    (df_RCA_Ethernet_Combined['Unsupported product(CSG)']== 0) &
    (df_RCA_Ethernet_Combined['Package exclusions for CSG']== 0) &
    (df_RCA_Ethernet_Combined['Multiple BI Equipment for BI service or same AAN']== 0) &
    (df_RCA_Ethernet_Combined['Multiple AAN tagged to same BVE service']== 0) &
    (df_RCA_Ethernet_Combined['SingleView has EQP MRC for Underlay Switch']== 0) &
    (df_RCA_Ethernet_Combined['Incorrect EQP fee tagging in CSG']== 0) &
    (df_RCA_Ethernet_Combined['Same ActiveCore services, with multiple EQPs from CLIPS']== 0) &
    (df_RCA_Ethernet_Combined['Unsupported SV product']==0) &
    (df_RCA_Ethernet_Combined['Missing PRI Service']==0) &
    (df_RCA_Ethernet_Combined['CSG MRC Cleanup']==0) &
    (df_RCA_Ethernet_Combined['Multiple BI on Same Site']==0) &
    (df_RCA_Ethernet_Combined['Missing Wifi Pro Access Point Equipment']==0) &
    (df_RCA_Ethernet_Combined['Equipment Double Billing']==0) &
    (df_RCA_Ethernet_Combined['Multiple Voice to one AAN']==0) &
    (df_RCA_Ethernet_Combined['Missing Activecore Base Product/Other Cleanup Required']==0) &
    (df_RCA_Ethernet_Combined['Missing AAN For BVE Services']==0)
    ,1,df_RCA_Ethernet_Combined['Non-Flagged'])
#


#Shiva: 12-17-2021 For P2P non-flagged, check for CM's flag for '1' --> RESET the non-flag to '0'
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['IPV4 P2P FALLOUT'] == 0) & (df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] == 1)
    ,0,df_RCA_Ethernet_Combined['Non-Flagged'])
#


#Shiva: 12-17-2021 Anything that has %BANDWITH% or %UNI% and has CM as '1' rest the NF to'0'
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['PRODUCT_OFFER'].str.upper().str.contains("BANDWIDTH") == True) & 
    (df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] == 1), 0, df_RCA_Ethernet_Combined['Non-Flagged']
)
#XXX 12/23


df_RCA_Ethernet_Combined['MRC Outliers for extract review']=np.where(
    (df_RCA_Ethernet_Combined['PRODUCT_OFFER'].str.upper().str.contains("BANDWIDTH") == True) & 
    (df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] == 1), 0, df_RCA_Ethernet_Combined['MRC Outliers for extract review']
)
#


#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['PRODUCT_OFFER'].str.upper().str.contains("BANDWIDTH") == True]

#

#Shiva: 12-17-2021 Anything that has %BANDWITH% or %UNI% and has CM as '1' rest the NF to'0'
df_RCA_Ethernet_Combined['Non-Flagged']=np.where(
    (df_RCA_Ethernet_Combined['PRODUCT_OFFER'].str.upper().str.contains("UNI") == True) & 
    (df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] == 1), 0, df_RCA_Ethernet_Combined['Non-Flagged']
)
#XXX 12/23


df_RCA_Ethernet_Combined['MRC Outliers for extract review']=np.where(
    (df_RCA_Ethernet_Combined['PRODUCT_OFFER'].str.upper().str.contains("UNI") == True) & 
    (df_RCA_Ethernet_Combined['CIRCUIT_MISMATCH'] == 1), 0, df_RCA_Ethernet_Combined['MRC Outliers for extract review']
)
#
#Bhoomi - Rename "Missing services in UF_SUBSCRIBER","MRC Outliers for extract review" --05-11-2022
df_RCA_Ethernet_Combined.rename(columns = {'Missing services in UF_SUBSCRIBER':'Legacy Products or Future Activation Date'}, inplace = True)
df_RCA_Ethernet_Combined.rename(columns = {'MRC Outliers for extract review':'Negative MRC diff or $5/10 diff'}, inplace = True)
#Bhoomi - Rename " Duplicate Equipment Fee in Singleviw" - 05192022
df_RCA_Ethernet_Combined.rename(columns = {'Duplicate Equipment Fee in SingleView':'Equipment Double Billing - Ethernet Equipment'}, inplace = True)
df_RCA_Ethernet_Combined.rename(columns = {'Equipment Double Billing':'Equipment Double Billing For BI and BV'}, inplace = True)


conx_stage.close()
connection.close()
#
print("Flags done!")  

print("Starting to write to file!!")  

ColumnOrder = ['CHECK_ID', 'LEGACY_ACCOUNT_NO', 'SV_ROOT_ACCOUNT_NO', 
#Shiva: 02-24-2022 Adding the product indicators
    'TOTAL_PRODUCT_INDICATOR', 'SV_PRODUCT_INDICATOR',
    'PRODUCT_OFFER','EXCLUSION(CSG)',
    'SV/CSG MRC','UF_MRC','MRC_DIFF','CSG(Y/N)','Unsupported product(CSG)','Package exclusions for CSG',
#Bhoomi - -05-02-2022 - Bhoomi - Added Missing PRI service, Added Multiple Voice to one AAN
    'Multiple BI Equipment for BI service or same AAN','Rounding Issue','Missing BI Equipment', 'Missing PRI Service', 'Multiple Voice to one AAN', 'Incorrect EQP fee tagging in CSG',
    'Legacy Products or Future Activation Date','CSG MRC Cleanup', 'Multiple BI on Same Site' , 'Missing Wifi Pro Access Point Equipment' ,'Equipment Double Billing For BI and BV', 'Missing Activecore Base Product/Other Cleanup Required', 'Missing AAN For BVE Services',
#Shiva: 01-05-2022 Adding Multiple AAN tagged to same BVE service and SingleView has EQP MRC for Underlay Switch FLAG
    'Multiple AAN tagged to same BVE service',
    'SingleView has EQP MRC for Underlay Switch',
#Shiva: 09-30-2021 Disabling this FLAG, as it is not getting flagged as expected.
    #'Missing Equipment from CLIPS',
    'Std Address Mismatch','Incorrect_Service_Address',
    'Equipment Double Billing - Ethernet Equipment',
    'MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS','Site Address Standardization issue',
    'IPV4 P2P FALLOUT',
#Bhoomi - Disabling this Flag, as it can flag manually    
    #'UNI and EQP Fee on different invoice account nos in SingleView',
#Shiva: 01-28-2022 For the same activecore service, we have multiple EQPs from CLIPS
    'Same ActiveCore services, with multiple EQPs from CLIPS',
#Shiva: 02-22-2022 Adding flag for un-supported SV products               
    'Unsupported SV product',               
    #'ACCOUNT MISMATCH',--> Pre additional filter
    'ACCOUNT_MISMATCH',
    #'CIRCUIT MISMATCH',--> Pre additional filter
    'CIRCUIT_MISMATCH',
    'Negative MRC diff or $5/10 diff',
    'Non-Flagged','COMBINED_ADDRESS','RCA_COMMENTS']
#


#RCA_FILE_NAME='R11.RO2 - Cycle-1_MRC_RCA.xlsx'
#df_RCA_Ethernet_Combined[ColumnOrder]
#

#Get the assigned POC's
assigned_data = {'ASSIGNED_TO':[' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ','Exclusion','Exclusion','Jefry','Non-Issue','Jefry','Jefry',
        'Jefry','Jefry','Teja','Jefry','Jefry','Jefry','Jefry','Non-Issue','Jefry','Jefry','Jefry','Sylin','Sylin']}
#


df_assigned_data = pd.DataFrame(assigned_data)
#df


#
df_assigned_row = df_assigned_data.transpose()
#df_assigned_row
#


#'CHECK_ID','LEGACY_ACCOUNT_NO','SV_ROOT_ACCOUNT_NO','PRODUCT_OFFER','SV/CSG MRC','UF_MRC','CSG(Y/N)','MRC_DIFF','Std Address Mismatch','Duplicate Equipment Fee in SingleView','Incorrect_Service_Address','MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS','Site Address Standardization issue','Missing services in UF_SUBSCRIBER','IPV4 P2P FALLOUT','EXTRACT REVIEW','Rounding Issue','COMBINED_ADDRESS','RCA_COMMENTS','RCA_COMMENT_WORD_COUNT','CIRCUIT MISMATCH','ACCOUNT MISMATCH','Missing BI Equipment','Non-Flagged'
del_ColumnOrder = ['CHECK_ID','LEGACY_ACCOUNT_NO','SV_ROOT_ACCOUNT_NO','PRODUCT_OFFER','SV/CSG MRC','UF_MRC','CSG(Y/N)','MRC_DIFF','Std Address Mismatch','Duplicate Equipment Fee in SingleView','Incorrect_Service_Address','MULTIPLE SITE ID FOR SAME SITE ADDRESS IN CLIPS','Site Address Standardization issue','Missing services in UF_SUBSCRIBER','IPV4 P2P FALLOUT','MRC Outliers for extract review','Rounding Issue','COMBINED_ADDRESS','RCA_COMMENTS','CIRCUIT MISMATCH','ACCOUNT MISMATCH','Missing BI Equipment','Non-Flagged']
#Bhoomi - Removed -,'UNI and EQP Fee on different invoice account nos in SingleView' from above condition --05092022


#df_RCA_Ethernet_Combined
#df_assigned_row
#



# Get the EXCEL OUTPUT populated opst all the above data processing
#output_path = r'R11.MR1 - Cycle 2_DIFF_CM030-P_RCA.xlsx'
output_path = RCA_FILE_NAME
writer = pd.ExcelWriter(output_path)
df_RCA_Ethernet_Combined[ColumnOrder].to_excel(writer,sheet_name='Automated RCA', index=False,startrow=1)

#df_RCA_Ethernet_debug.to_excel(writer,sheet_name='Debug', index=False)   

df_RCA_Ethernet_Combined_DELETED[del_ColumnOrder].to_excel(writer,sheet_name='DELETED Records', index=False)   
df_RCA_Ethernet_Combined.to_excel(writer,sheet_name='RCA_ALL', index=False)


# Get the xlsxwriter workbook and worksheet objects.
workbook = writer.book
worksheet = writer.sheets['Automated RCA']
#worksheet2 = writer.sheets['Debug']
worksheet3 = writer.sheets['DELETED Records']


# cell_format = workbook.add_format()
# cell_format.set_font_name('Arial')
# cell_format.set_font_size(8)
# cell_format.set_align('center')
# cell_format.set_align('vcenter')
# worksheet.set_row(0, 42.75)  # Set the height of Row 1 to 20.
# worksheet2.set_row(0, 42.75)  # Set the height of Row 1 to 20.
# #header_format = workbook.add_format({'text_wrap': True})
# #worksheet.write(0,header_format)
# worksheet.set_column('A:Z', None, cell_format)
# worksheet2.set_column('A:Z', None, cell_format)
# #worksheet.set_column('E:S', None, cell_format)
# #worksheet2.set_column('E:S', None, cell_format)
# worksheet.set_column('A:Z', 15)
# worksheet2.set_column('A:Z', 15)
# wrap_format = workbook.add_format({'text_wrap': True})
# worksheet.set_column('A:Z', None, wrap_format)
# worksheet.set_column('U:V', 45)
# worksheet2.set_column('U:V', 45)
# cell_format.set_align('left')
# worksheet.set_column('D:D', None, cell_format)
# worksheet2.set_column('D:D', None, cell_format)
# cell_format.set_align('left')
# worksheet.set_column('U:V', None, cell_format)
# cell_format.set_align('top')
# worksheet2.set_column('U:V', None, cell_format)
# worksheet.set_default_row(42.75)
writer.save()
#
#writer.close()
#RCA_FILE_NAME
#df_RCA_Ethernet_Combined[ColumnOrder]
#

print("Wriring done!")  

# #Get the column width set
# excel = Dispatch('Excel.Application')
# #output_path = r'R11.MR1 - Cycle 2_DIFF_CM030-P_RCA.xlsx'
# #temp_file='C:\Users\Bsiddh212\Documents\bsiddh212\Recon\Recon_New_2022\Python_Code_V2\R22\'+str(RCA_FILE_NAME)
# wb = excel.Workbooks.Open(RCA_FILE_NAME)
# #path =  os.getcwd().replace('\'','\\') + '\\'
# #wb = excel.Workbooks.Open(path+RCA_FILE_NAME)
# ws = wb.Worksheets("DELETED Records")
# #Activate second sheet
# excel.Worksheets(3).Activate()
# # #Autofit column in active sheet
# excel.ActiveSheet.Columns.AutoFit()
# ws = wb.Worksheets("Automated RCA")
# #Activate second sheet
# excel.Worksheets(1).Activate()
# # #Autofit column in active sheet
# excel.ActiveSheet.Columns.AutoFit()
# ws.Range("B:B").ColumnWidth = 16.57
# ws.Range("C:C").ColumnWidth = 17.43
# ws.Range("D:F").ColumnWidth = 26
# ws.Range("G:G").ColumnWidth = 14.44
# ws.Range("H:J").ColumnWidth = 9
# ws.Range("J:AF").ColumnWidth = 14
# ws.Range("AG:AH").ColumnWidth = 50
# ws.Range("R:R").ColumnWidth = 16.29
# ws.Range("W:W").ColumnWidth = 15.14
# ws.Range("N:N").ColumnWidth = 17.86
# ws.Range("Z:Z").ColumnWidth = 20.14
# ws.Range("AA:AA").ColumnWidth = 18.72
# #ws.Range("E:H").ColumnWidth = 11
# #Save changes in a new file
# #wb.SaveAs("D:\\output_fit.xlsx")
# #Or simply save changes in a current file
# wb.Save()
# wb.Close()
# excel.Application.Quit()
# #


# excel = Dispatch('Excel.Application')
# path =  os.getcwd().replace('\'','\\') + '\\'
# wb = excel.Workbooks.Open(path+RCA_FILE_NAME)
# ws = wb.Worksheets("DELETED Records")
# #Activate second sheet
# excel.Worksheets(3).Activate()
# # #Autofit column in active sheet
# excel.ActiveSheet.Columns.AutoFit()
# #Or simply save changes in a current file
# wb.Save()
# wb.Close()
# excel.Application.Quit()
#


#Calcukate the end time for the automation process
end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
#today = date.today()
today_ts = datetime.datetime.now()
print(f"Last run was on {today_ts}")
print("Time Taken for E2E is: {:0>2} hours : {:0>2} minutes : {:05.2f} seconds".format(int(hours),int(minutes),seconds))
#


# from openpyxl import load_workbook
# writer = pd.ExcelWriter(RCA_FILE_NAME, engine='openpyxl')
# # try to open an existing workbook
# writer.book = load_workbook("C:\\Users\\Bsiddh212\\Documents\\bsiddh212\\Recon\\Recon_New_2022\\Python_Code_V2\\R22\\RECON_R22_FV3 - Cycle-1_MRC_RCA.xlsx")
# # copy existing sheets
# writer.sheets = dict((ws.title, ws) for ws in writer.book.worksheets)
# # read existing file
# reader = pd.read_excel(RCA_FILE_NAME)
# # write out the new sheet
# df_assigned_row.to_excel(writer, sheet_name='Automated RCA',index=False,header=False,startrow=0)
# writer.close()
#


#RCA_FILE_NAME
#Query_Circuit_Account_Mismatch
#


#RCA_TABLE_NAME
#


#df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['Rounding Issue'] == 0)]
#df['NAME_Count'] = df['NAME'].str.len()
#df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Installation Fee']
#


#Config.CM030_DIFF_TABLE
#Get the names of indexes for the below condition and drop in the dataset
#df_RCA_Ethernet_Combined.drop(df_RCA_Ethernet_Combined[df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Installation Fee'].index, inplace = True)
#df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['RCA_COMMENT_WORD_COUNT'] == 24) & (df_RCA_Ethernet_Combined['Non-Flagged'] == 1) ]
#


#  df_RCA_Ethernet_Combined[(df_RCA_Ethernet_Combined['RCA_COMMENT_WORD_COUNT'] == 0) & (df_RCA_Ethernet_Combined['Non-Flagged'] == 1) & (df_RCA_Ethernet_Combined['RCA_COMMENTS']== "") & (df_RCA_Ethernet_Combined['PRODUCT_OFFER'] == 'Ethernet Equipment Fee') ] 
#Query_CM_AM_ADD_FILTER
#df_ME_SERVICES
#Query_PRD_IND_List
