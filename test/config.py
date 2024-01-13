import pandas as pd
import cx_Oracle

# ORACLE CLIENT LIB files
# lib_dir = r"C:\Development_Avecto\OracleClient\instantclient_19_9"
lib_dir = r"C:\Development_Avecto\OracleClient\instantclient-basic-nt-19.10.0.0.0dbru_32\instantclient_19_10"


#env='STG'#For Stage runs
env='PROD'#For PROD  runs

if env == 'STG':

   with open("Stage_Parameter.txt") as f:
    lines = f.readlines()
    DATABASE =  lines[0].strip()
    PORT = lines[1].strip()
    SID = lines[2].strip()
    SCHEMA = lines[3].strip()
    PASSWORD = lines[4].strip()
    STAGE_SCHEMA  = lines[5].strip()
    STAGE_PASSWORD  = lines[6].strip()
    #print(f"DATABASE={DATABASE}, PORT={PORT}, SID={SID}, SCHEMA={SCHEMA}, PASSWORD={PASSWORD}, STAGE_SCHEMA={STAGE_SCHEMA}, STAGE_PASSWORD={STAGE_PASSWORD}")

    connstr = f"{DATABASE}:{PORT}/{SID}"

    
elif env == 'PROD':

   with open("Prod_Parameter.txt") as f:
    lines = f.readlines()
    DATABASE =  lines[0].strip()
    PORT = lines[1].strip()
    SID = lines[2].strip()
    SCHEMA = lines[3].strip()
    PASSWORD = lines[4].strip()
    STAGE_SCHEMA  = lines[5].strip()
    STAGE_PASSWORD  = lines[6].strip()
    print(f"DATABASE={DATABASE}, PORT={PORT}, SID={SID}, SCHEMA={SCHEMA}, PASSWORD={PASSWORD}, STAGE_SCHEMA={STAGE_SCHEMA}, STAGE_PASSWORD={STAGE_PASSWORD}")

    connstr = f"{DATABASE}:{PORT}/{SID}"

Query_UF_Release_SQL = fr"""
/*SQL to get the UF release data*/
SELECT VARIABLE_VALUE FROM "OPS"."LOCAL_VARIABLE" WHERE VARIABLE_NM = 'UF_CYCLE_DATE' AND PROGRAM_NM = 'validation'
"""
Query_RECON_Release_SQL = fr"""
/*SQL to get the RECON 1release data*/
SELECT VARIABLE_VALUE FROM "OPS"."LOCAL_VARIABLE" WHERE VARIABLE_NM = 'RELEASE' AND PROGRAM_NM = 'validation'
"""

Query_Bill_Run_SQL = fr"""
/*SQL to get the Bill run data*/
SELECT VARIABLE_VALUE FROM "OPS"."LOCAL_VARIABLE" WHERE program_nm = 'validation' and variable_nm='BILL_RUN'
"""

Query_MIG_SQL = fr"""
/*SQL to get the migration data*/
SELECT distinct VARIABLE_VALUE FROM "OPS"."LOCAL_VARIABLE" where program_nm='validation' and VARIABLE_NM='RECON_UF_SCHEMA'
"""

Query_CONSOLIDATED_MIG_SQL = fr"""
/*SQL to get the migration data*/
SELECT distinct VARIABLE_VALUE FROM "OPS"."LOCAL_VARIABLE" where program_nm='validation' and VARIABLE_NM='CONSOLIDATED_SCHEMA'
"""

# Create Connection to Oracle
connection = cx_Oracle.connect(SCHEMA, PASSWORD, connstr,encoding="UTF-8")
conx_stage = cx_Oracle.connect(STAGE_SCHEMA, STAGE_PASSWORD, connstr,encoding="UTF-8")

df_Query_UF_Release = pd.read_sql(Query_UF_Release_SQL, con=connection)
UF_release =str(df_Query_UF_Release.VARIABLE_VALUE[0])

df_Query_RECON_Release = pd.read_sql(Query_RECON_Release_SQL, con=connection)
release =str(df_Query_RECON_Release.VARIABLE_VALUE[0])

df_Query_Bill_Run = pd.read_sql(Query_Bill_Run_SQL, con=connection)
cycle =str(df_Query_Bill_Run.VARIABLE_VALUE[0])

df_Query_MIG = pd.read_sql(Query_MIG_SQL, con=connection)
#MIG =str(df_Query_MIG.VARIABLE_VALUE[0])
MIG=df_Query_MIG.VARIABLE_VALUE[0]
MIG_ARR = MIG.split(',')

df_Consolidated_Mig_Sch = pd.read_sql(Query_CONSOLIDATED_MIG_SQL, con=connection)
Consolidated_Mig_Sch = str(df_Consolidated_Mig_Sch.VARIABLE_VALUE[0])


# SET RELEASE AND CYCLE
CYCLE = cycle
#RELEASE = f'MR1' 
#FILE_RELEASE = f'R13.{RELEASE}'
#DB_RELEASE = f'R13_{RELEASE}'

FILE_RELEASE = release
DB_RELEASE = release

# SCHEMA NAMES
STAGE = 'STAGE'
# MIGRATION_ME = 'MIGRATION_6'
# MIGRATION_BI = 'MIGRATION_7'
# MIGRATION_AV = 'MIGRATION_8'
# MIGRATION_BV = 'MIGRATION_9'
# MIGRATION_CNV = 'MIGRATION_7'

#MIGRATION_ME = str(MIG[0])
#MIGRATION_BI = str(MIG[1])
#MIGRATION_AV = str(MIG[2])
#MIGRATION_BV = str(MIG[3])

MIGRATION_ME = str(MIG_ARR[0])
MIGRATION_BI = str(MIG_ARR[1])
MIGRATION_AV = str(MIG_ARR[2])
MIGRATION_BV = str(MIG_ARR[3])



# SET RELEASE AND CYCLE
#CYCLE = 2
#RELEASE = f'R15' 
#VERSION = f'FV2' 
#FILE_RELEASE = f'{RELEASE}.{VERSION}'
#DB_RELEASE   = f'{RELEASE}_{VERSION}'

# SET APP AND BASE SCHEMA
if CYCLE == '1':
    APP  = 'APP_1'
    BASE = 'BASE_1'
    REF  = 'REF_1' 
    RPT  = 'RPT_1'
elif CYCLE == '2':
    APP  = 'APP_2'
    BASE = 'BASE_2'
    REF  = 'REF_2'
    RPT  = 'RPT_2'    
    
# R11.RELEASE3 - Cycle 2 --> MIGRATION_1,MIGRATION_4,MIGRATION_7,MIGRATION_10
# R11.RELEASE4 - Cycle 2 --> MIGRATION_3,MIGRATION_6,MIGRATION_9,MIGRATION_12

#FILE NAME
FILE_NAME       = fr'RECON_{FILE_RELEASE} - Cycle-{CYCLE}_DIFF_CM030-P.xlsx'
RCA_FILE_NAME   = fr'RECON_{FILE_RELEASE} - Cycle-{CYCLE}_MRC_RCA.xlsx'

# TABLES

#CM030_RCA_TABLE = f'X_TMP_RECON_CM030_RCA_{RELEASE}'
#CM030_DIFF_TABLE = f'X_TMP_RECON_CM030_RCA_{RELEASE}'

CM030_RCA_TABLE  = f'RECON_CM030_RCA_{DB_RELEASE}'
CM030_DIFF_TABLE = f'RECON_CM030_RCA_{DB_RELEASE}'
CM030_UNION_SQL  = f'RECON_CM030UNIONSQL_{DB_RELEASE}'

CNV_ENTITIES_MAPPING = f'CNV_ENTITIES_MAPPING_{DB_RELEASE}'
UF_ACCOUNT = f'UF_ACCOUNT_{DB_RELEASE}'
UF_ADDRESS = f'UF_ADDRESS_{DB_RELEASE}'
UF_ARM_ENTITIES= f'UF_ARM_ENTITIES_{DB_RELEASE}'
UF_CDL_RELEASE = f'UF_CDL_{DB_RELEASE}'
UF_CHARGE = f'UF_CHARGE_{DB_RELEASE}'
UF_CONTACT_EXT = f'UF_CONTACT_EXT_{DB_RELEASE}'
UF_CONTACT = f'UF_CONTACT_{DB_RELEASE}'
UF_CON_ROLES = f'UF_CON_ROLES_{DB_RELEASE}'
UF_CORPORATE = f'UF_CORPORATE_{DB_RELEASE}'
UF_CREDIT = f'UF_CREDIT_{DB_RELEASE}'
UF_CUSTOMER = f'UF_CUSTOMER_{DB_RELEASE}'
UF_DEPOSITS = f'UF_DEPOSITS_{DB_RELEASE}'
UF_INVOICE = f'UF_INVOICE_{DB_RELEASE}'
UF_MEMO = f'UF_MEMO_{DB_RELEASE}'
UF_PAYMENT = f'UF_PAYMENT_{DB_RELEASE}'
UF_SERVICES = f'UF_SERVICES_{DB_RELEASE}'
UF_SITE = f'UF_SITE_{DB_RELEASE}'
UF_SRV_PARAM = f'UF_SRV_PARAM_{DB_RELEASE}'
UF_SUBSCRIBER = f'UF_SUBSCRIBER_{DB_RELEASE}'
UF_SUB_ASSOCIATION = f'UF_SUB_ASSOCIATION_{DB_RELEASE}'


# SQL FILES BELOW


# SQL to test connection to database
TestSQL = fr'SELECT COUNT(1) FROM {STAGE}.{CM030_DIFF_TABLE}'


# CM 030 SQLS

TruncateRCASQL = fr'TRUNCATE TABLE {STAGE}.{CM030_DIFF_TABLE}'

SQL_COMMIT = 'COMMIT'

#CM030_P_DIFF_SQL
Query_CM030_P_DIFF_SQL = fr"""
SELECT NVL(RES1.CHECK_ID,RES2.CHECK_ID) CHECK_ID, NVL(RES1.LEGACY_CUST_ID, RES2.LEGACY_CUST_ID) LEGACY_CUST_ID
, NVL(RES1.PRODUCT_OFFER, RES2.PRODUCT_OFFER) PRODUCT_OFFER
, NVL(RES2.SV_MRC,0) SV_MRC, NVL(RES1.UF_MRC,0) UF_MRC 
FROM 
  (SELECT CHECK_ID, LEGACY_CUST_ID, PRODUCT_OFFER, UF_MRC FROM
      (
      SELECT CHECK_ID, LEGACY_CUST_ID,  FIELD1 PRODUCT_OFFER, ROUND(TO_NUMBER(FIELD2),2) UF_MRC FROM RECON_UF_RESULT WHERE CHECK_ID = 'CM030-P'
      MINUS
      SELECT CHECK_ID, LEGACY_CUST_ID, FIELD1, ROUND(SV_MRC,2) SV_MRC  FROM (
    select  CHECK_ID, LEGACY_CUST_ID , FIELD1, SUM(TO_NUMBER(FIELD2)) SV_MRC from RECON_SV_RESULT WHERE  CHECK_ID = 'CM030-P'
    GROUP BY CHECK_ID,LEGACY_CUST_ID, FIELD1
    ) WHERE SV_MRC <> 0
   )
   ) RES1  
FULL OUTER JOIN 

 (SELECT CHECK_ID, LEGACY_CUST_ID, PRODUCT_OFFER, SV_MRC FROM
      ( SELECT CHECK_ID, LEGACY_CUST_ID, FIELD1 PRODUCT_OFFER, ROUND(SV_MRC,2) SV_MRC  FROM (
    select CHECK_ID, LEGACY_CUST_ID , FIELD1, SUM(TO_NUMBER(FIELD2)) SV_MRC from RECON_SV_RESULT WHERE  CHECK_ID = 'CM030-P'
    GROUP BY CHECK_ID,LEGACY_CUST_ID, FIELD1
    ) WHERE SV_MRC <> 0
  MINUS
  SELECT CHECK_ID, LEGACY_CUST_ID,  FIELD1 PRODUCT_OFFER, ROUND(TO_NUMBER(FIELD2),2) UF_MRC FROM RECON_UF_RESULT WHERE CHECK_ID = 'CM030-P'
   )
   ) RES2 
  
  ON (RES1.LEGACY_CUST_ID = RES2.LEGACY_CUST_ID
  AND RES1.PRODUCT_OFFER = RES2.PRODUCT_OFFER
  AND ROUND(RES1.UF_MRC,2)  <> ROUND(RES2.SV_MRC,2)
  ) ORDER BY LEGACY_CUST_ID
"""

Query_CSG_MRC_EXCLUSIONS_SQL = fr"""
SELECT DISTINCT ACCOUNT_NAME, LISTAGG(EXCLUSION, '|') WITHIN GROUP (ORDER BY ACCOUNT_NAME) AS EXCLUSION  FROM
(SELECT DISTINCT 'CSG Product MRC' AS SQL_SOURCE
,cp_dnorm.SUB_ACCT_NO_SBB as ACCOUNT_NAME
,CSG_REF.CSG_SYS_PRIN_CODE,CSG_REF.service_code AS PRODUCT_NAME 
/*Shiva 12-09-2021 Added the below code to incorporate the TN's charges*/
,cp_dnorm.SERV_ID_OCI
, EXCLUSION
,cp_dnorm.charge_amt_oci as MRC
FROM {BASE}.CSG_SUB_CUST_LOC_PROD_DNRM CP_DNORM
INNER JOIN {REF}.REF_CSG_SERV_CDE_MAPPING CSG_REF
ON (CP_DNORM.SYS_SBB||'_'||CP_DNORM.PRIN_SBB||'_'||CP_DNORM.SERV_CDE_OCI)=CSG_REF.CSG_SYS_PRIN_CODE
AND CP_DNORM.EQP_STAT_EQP = 'H'
AND SERVICE_CODE IS NOT NULL
JOIN {BASE}.CSG_MRC_LATEST_REPORT CSG_MRC
/*Shiva: 09-08-2021 Removed the Logic to match based on the description*/
/*ON CSG_MRC.ONLINE_DESC_ALA=CSG_REF.CSG_DESC */
/*Shiva: 09-08-2021 Added Logic to match based on the Service Code OCI*/
ON CP_DNORM.SERV_CDE_OCI= CSG_MRC.SERV_CDE_OCI
AND CP_DNORM.AAN_INPUT=CSG_MRC.AAN_INPUT
JOIN {APP}.APP_CSG_BILLED_AAN_INPUT AAN
ON CSG_MRC.AAN_INPUT = AAN.CSG_AAN
WHERE CSG_REF.EXCLUSION IS NOT NULL) CSG_PRD_TBL 
--WHERE EXCLUSION in 'Package'
GROUP BY ACCOUNT_NAME
--ORDER BY ACCOUNT_NAME
"""

#CM030_P_DIFF_SQL
Query_CSG_MRC_EXCLUSIONS_SQL_OLD = fr"""
SELECT 
--SQL_SOURCE, 
ACCOUNT_NAME
,CASE /*Shiva: Commenting the below for adding SERVICE CODE*/
--    WHEN CSG_REF.LEGACY_PRODUCT_OFFER = 'Equipment Fee - Data'  THEN 'Equipment Fee'
--    WHEN CSG_REF.LEGACY_PRODUCT_OFFER = 'Equipment Fee - Voice' THEN 'Equipment Fee'
--    WHEN CSG_REF.LEGACY_PRODUCT_OFFER IS NULL THEN CSG_MRC.ONLINE_DESC_ALA
--    ELSE CSG_REF.LEGACY_PRODUCT_OFFER
    WHEN PRODUCT_NAME = 'Equipment Fee - Data'  THEN 'Equipment Fee'
    WHEN PRODUCT_NAME = 'Equipment Fee - Voice' THEN 'Equipment Fee'
    ELSE PRODUCT_NAME
END AS PRODUCT_NAME
/*Shiva: 12-19-2021 Added Logic to extract the EXCLUSIONS for CSG prodcuts*/
, EXCLUSION
--,'N/A' AS PRODUCT_GROUP
, SUM(MRC) AS MRC FROM
(SELECT DISTINCT 'CSG Product MRC' AS SQL_SOURCE
,cp_dnorm.SUB_ACCT_NO_SBB as ACCOUNT_NAME
,CSG_REF.CSG_SYS_PRIN_CODE,CSG_REF.service_code AS PRODUCT_NAME 
/*Shiva 12-09-2021 Added the below code to incorporate the TN's charges*/
,cp_dnorm.SERV_ID_OCI
, EXCLUSION
,cp_dnorm.charge_amt_oci as MRC
FROM {BASE}.CSG_SUB_CUST_LOC_PROD_DNRM CP_DNORM
INNER JOIN {REF}.REF_CSG_SERV_CDE_MAPPING CSG_REF
ON (CP_DNORM.SYS_SBB||'_'||CP_DNORM.PRIN_SBB||'_'||CP_DNORM.SERV_CDE_OCI)=CSG_REF.CSG_SYS_PRIN_CODE
AND CP_DNORM.EQP_STAT_EQP = 'H'
AND SERVICE_CODE IS NOT NULL
JOIN {BASE}.CSG_MRC_LATEST_REPORT CSG_MRC
/*Shiva: 09-08-2021 Removed the Logic to match based on the description*/
/*ON CSG_MRC.ONLINE_DESC_ALA=CSG_REF.CSG_DESC */
/*Shiva: 09-08-2021 Added Logic to match based on the Service Code OCI*/
ON CP_DNORM.SERV_CDE_OCI= CSG_MRC.SERV_CDE_OCI
AND CP_DNORM.AAN_INPUT=CSG_MRC.AAN_INPUT
JOIN {APP}.APP_CSG_BILLED_AAN_INPUT AAN
ON CSG_MRC.AAN_INPUT = AAN.CSG_AAN
WHERE CSG_REF.EXCLUSION IS NOT NULL
--AND CP_DNORM.SUB_ACCT_NO_SBB='8155100030752194' 
)
GROUP BY SQL_SOURCE, ACCOUNT_NAME, PRODUCT_NAME, EXCLUSION, 'N/A'
"""

#Site Address Standardization
Query_Site_Address_Standardization = fr"""
select * from (
select sub.legacy_cust_id, sub.legacy_site_id,sub.legacy_sub_no,srv.manual_override_amount,srv.service_code,
ad.eloc_location_id, ad.adr_line_1, ad.unit_type, ad.unit_value, sub.legacy_product_offer
from {MIGRATION_ME}.uf_subscriber sub
left join {MIGRATION_ME}.uf_address ad
on sub.legacy_site_id=ad.legacy_address_id
left join {MIGRATION_ME}.uf_services srv
on sub.LEGACY_CUST_ID=srv.LEGACY_CUST_ID
and sub.legacy_sub_no=srv.legacy_entity_id
where 
sub.legacy_product_offer='SWITCH'

union

select sub.legacy_cust_id, sub.legacy_site_id,sub.legacy_sub_no,srv.manual_override_amount,srv.service_code,
ad.eloc_location_id, ad.adr_line_1, ad.unit_type, ad.unit_value, sub.legacy_product_offer
from {MIGRATION_BI}.uf_subscriber sub
left join {MIGRATION_BI}.uf_address ad
on sub.legacy_site_id=ad.legacy_address_id
left join {MIGRATION_BI}.uf_services srv
on sub.LEGACY_CUST_ID=srv.LEGACY_CUST_ID
and sub.legacy_sub_no=srv.legacy_entity_id
where 
sub.legacy_product_offer='SWITCH'

union

select sub.legacy_cust_id, sub.legacy_site_id,sub.legacy_sub_no,srv.manual_override_amount,srv.service_code,
ad.eloc_location_id, ad.adr_line_1, ad.unit_type, ad.unit_value, sub.legacy_product_offer
from {MIGRATION_AV}.uf_subscriber sub
left join {MIGRATION_AV}.uf_address ad
on sub.legacy_site_id=ad.legacy_address_id
left join {MIGRATION_AV}.uf_services srv
on sub.LEGACY_CUST_ID=srv.LEGACY_CUST_ID
and sub.legacy_sub_no=srv.legacy_entity_id
where 
sub.legacy_product_offer='SWITCH'

union

select sub.legacy_cust_id, sub.legacy_site_id,sub.legacy_sub_no,srv.manual_override_amount,srv.service_code,
ad.eloc_location_id, ad.adr_line_1, ad.unit_type, ad.unit_value, sub.legacy_product_offer
from {MIGRATION_BV}.uf_subscriber sub
left join {MIGRATION_BV}.uf_address ad
on sub.legacy_site_id=ad.legacy_address_id
left join {MIGRATION_BV}.uf_services srv
on sub.LEGACY_CUST_ID=srv.LEGACY_CUST_ID
and sub.legacy_sub_no=srv.legacy_entity_id
where 
sub.legacy_product_offer='SWITCH'
)
where adr_line_1 is null"""

# Query for Standard Address Mismatch
Query_Std_Address_Mismatch = fr"""
/*Query for Standard Address Mismatch for Equipment Fee MRC mismatches*/
SELECT INVOICE_ACCOUNT_NAME as ACCOUNTNUMBER, STD_LINE1 FROM
(SELECT	C.INVOICE_ACCOUNT_NAME, STD_LINE1
FROM 	{APP}.APP_CSG_SITE_ADDR_GLX_SUCESS CSG, {APP}.CUST_HIERARCHY C
WHERE	CSG.ACCOUNT = C.ACCOUNT_NAME
AND 	C.LOB LIKE '%BCI%'
MINUS
SELECT	C.INVOICE_ACCOUNT_NAME, SV.STD_LINE1
FROM	{APP}.APP_SV_SERV_ADD_ELOC_SUCESS SV, {APP}.CUST_HIERARCHY C
WHERE 	SV.BAN = C.ACCOUNT_NAME
AND 	SV.PRODUCT_NAME LIKE 'Equipment Fee')
WHERE INVOICE_ACCOUNT_NAME IN
( 
SELECT  DISTINCT LEGACY_CUST_ID FROM STAGE.{CM030_DIFF_TABLE}
WHERE   PRODUCT_OFFER='Equipment Fee'
)
UNION
/*Query for Standard Address Mismatch for Ethernet Equipment Fee MRC mismatches*/
SELECT ACCOUNTNUMBER, STD_LINE1 FROM (
SELECT DISTINCT CLIPS.ACCOUNTNUMBER, CLIPS.STD_LINE1 FROM (
SELECT DISTINCT CLIPS.ACCOUNTNUMBER, CLIPS.STD_LINE1, SLTV.DEV1FQDN FROM {APP}.APP_CLIPS_ADDRESS_ELOC_SUCESS CLIPS JOIN  {BASE}.SLTV SLTV
ON CLIPS.EPID = SLTV.UNIID 
) CLIPS 
MINUS
SELECT DISTINCT  BAN, STD_LINE1 FROM
(SELECT DISTINCT BAN, STD_LINE1, SERVICE_NAME, Service_name FROM {APP}.APP_SV_SERV_ADD_ELOC_SUCESS SV 
WHERE SERVICE_NAME LIKE 'EQP%')  SV 
)
WHERE ACCOUNTNUMBER IN 
(
SELECT DISTINCT LEGACY_CUST_ID FROM STAGE.{CM030_DIFF_TABLE}
WHERE UPPER(PRODUCT_OFFER) LIKE 'ETHER%'
)
"""

# Query for Standard Address Mismatch
Query_Std_Address_Mismatch_OLD = fr"""
SELECT * FROM (
SELECT distinct CLIPS.ACCOUNTNUMBER, CLIPS.STD_LINE1 FROM (
SELECT DISTINCT CLIPS.ACCOUNTNUMBER, CLIPS.STD_LINE1, SLTV.DEV1FQDN FROM {APP}.APP_CLIPS_ADDRESS_ELOC_SUCESS CLIPS JOIN  {BASE}.SLTV SLTV
ON CLIPS.EPID = SLTV.UNIID 
--WHERE CLIPS.ACCOUNTNUMBER = '900017754'
) CLIPS 

MINUS

SELECT distinct  BAN, STD_LINE1 FROM
(SELECT DISTINCT BAN, STD_LINE1, SERVICE_NAME, Service_name FROM {APP}.APP_SV_SERV_ADD_ELOC_SUCESS SV 
WHERE 
--BAN = 900017754 AND 
SERVICE_NAME LIKE 'EQP%')  SV 
)
where ACCOUNTNUMBER
in 
(
SELECT DISTINCT LEGACY_CUST_ID FROM "STAGE".{CM030_DIFF_TABLE}
where upper(PRODUCT_OFFER) like 'ETHER%'
)"""

# Query to get the root accounts
Query_Root_Account_List = fr"""
SELECT DISTINCT ACCOUNT_NAME as LEGACY_CUST_ID, ROOT_ACCOUNT_NAME as SV_ROOT_ACCOUNT_NO
FROM {APP}.CUST_HIERARCHY 
WHERE ACCOUNT_NAME IN 
(SELECT DISTINCT LEGACY_CUST_ID FROM STAGE.{CM030_DIFF_TABLE})
UNION
SELECT DISTINCT CSG_AAN AS LEGACY_CUST_ID, SV_ROOT_BAN as SV_ROOT_ACCOUNT_NO FROM {APP}.APP_CSG_BILLED_AAN_INPUT
where CSG_AAN in
(select distinct LEGACY_CUST_ID from STAGE.{CM030_DIFF_TABLE})
"""

# Query to get the root accounts Total PRD INDCTR and SV PRD INDCTR 
Query_PRD_IND_List = fr"""
SELECT CUSTOMER_ACCOUT_NUMBER, TOTAL_PRODUCT_INDICATOR, SV_PRODUCT_INDICATOR FROM {RPT}.RPT_MIGRATION_DNORM
WHERE CUSTOMER_ACCOUT_NUMBER IN 
(SELECT ROOT_ACCOUNT_NAME as SV_ROOT_ACCOUNT_NO 
FROM {APP}.CUST_HIERARCHY WHERE ACCOUNT_NAME IN 
(SELECT DISTINCT LEGACY_CUST_ID FROM STAGE.{CM030_DIFF_TABLE})
)

UNION

SELECT CUSTOMER_ACCOUT_NUMBER, TOTAL_PRODUCT_INDICATOR, SV_PRODUCT_INDICATOR FROM {RPT}.RPT_MIGRATION_DNORM
WHERE CUSTOMER_ACCOUT_NUMBER IN 
(
SELECT DISTINCT SV_ROOT_BAN AS SV_ROOT_ACCOUNT_NO FROM {APP}.APP_CSG_BILLED_AAN_INPUT
WHERE CSG_AAN IN
(SELECT DISTINCT LEGACY_CUST_ID FROM STAGE.{CM030_DIFF_TABLE})
)
"""

# Query for Duplicate Address SQL (For All Ethernet Product)
Query_Duplicate_Address = fr"""
SELECT * FROM (
SELECT BAN as ACCOUNTNUMBER, STD_LINE1, CNT FROM
(SELECT BAN, STD_LINE1, COUNT(*) AS CNT FROM 
(SELECT DISTINCT BAN, STD_LINE1, SERVICE_NAME, Service_name FROM {APP}.APP_SV_SERV_ADD_ELOC_SUCESS SV 
WHERE 
--BAN = 900011160 AND 
SERVICE_NAME LIKE 'EQP%')  SV 
GROUP BY BAN, STD_LINE1
) SV
WHERE               CNT >= 2

MINUS

SELECT CLIPS.ACCOUNTNUMBER, CLIPS.STD_LINE1, CLIPS.CNT FROM (
SELECT ACCOUNTNUMBER, STD_LINE1, COUNT(*) AS CNT FROM (
SELECT DISTINCT CLIPS.ACCOUNTNUMBER, CLIPS.STD_LINE1, SLTV.DEV1FQDN FROM {APP}.APP_CLIPS_ADDRESS_ELOC_SUCESS CLIPS JOIN  {BASE}.SLTV SLTV
ON CLIPS.EPID = SLTV.UNIID 
--WHERE CLIPS.ACCOUNTNUMBER = '900011160'
) CLIPS 
GROUP BY CLIPS.ACCOUNTNUMBER, CLIPS.STD_LINE1) CLIPS
WHERE               CNT >= 2
)

WHERE ACCOUNTNUMBER IN 
(
SELECT DISTINCT LEGACY_CUST_ID FROM "STAGE".{CM030_DIFF_TABLE}
where upper(PRODUCT_OFFER) like 'ETHER%'
)"""


# -- Incorrect Service address in SV
Query_Incorrect_Service_Address = fr"""
-- Incorrect Service address in SV
SELECT
    ACCT_NUM,
    ADDR
FROM
    (
        SELECT
            distinct BAN AS ACCT_NUM,
            STD_LINE1 AS ADDR
        FROM
            (
                SELECT
                    DISTINCT BAN,
                    STD_LINE1,
                    SERVICE_NAME,
                    Service_name
                FROM
                    {APP}.APP_SV_SERV_ADD_ELOC_SUCESS SV
                WHERE
                    -- BAN = 900013231
                    -- AND 
                    SERVICE_NAME LIKE 'EQP%'
            ) SV
        MINUS
        SELECT
            distinct CLIPS.ACCOUNTNUMBER AS ACCT_NUM,
            CLIPS.STD_LINE1 AS ADDR
        FROM
            (
                SELECT
                    DISTINCT CLIPS.ACCOUNTNUMBER,
                    CLIPS.STD_LINE1,
                    SLTV.DEV1FQDN
                FROM
                    {APP}.APP_CLIPS_ADDRESS_ELOC_SUCESS CLIPS
                    JOIN {BASE}.SLTV SLTV ON CLIPS.EPID = SLTV.UNIID -- WHERE
                    --     CLIPS.ACCOUNTNUMBER = '900013231'
            ) CLIPS
    ) OQ
WHERE
    OQ.ACCT_NUM IN (
        SELECT
            DISTINCT CAST(LEGACY_CUST_ID AS VARCHAR(20))
        FROM
            STAGE.{CM030_DIFF_TABLE}
        WHERE
            UPPER(PRODUCT_OFFER) LIKE 'ETHER%'
    )"""

#UF812 - Missing BI Equipment - 05-04-2022 - Bhoomi commented OR  LEGACY_PRODUCT_OFFER = 'BI Underlay Service'
Query_Missing_BI_Equipment = fr"""
SELECT distinct LEGACY_CUST_ID,LEGACY_SUB_NO,LEGACY_PRODUCT_OFFER, SUB.LEGACY_SITE_ID
FROM {MIGRATION_ME}.UF_SUBSCRIBER SUB
WHERE (LEGACY_PRODUCT_OFFER  LIKE '%Business Internet%' 
--OR LEGACY_PRODUCT_OFFER = 'BI Underlay Service'
OR UPPER(SUB.LEGACY_PRODUCT_OFFER) LIKE '%BASIC CONNECT%' 
OR UPPER(SUB.LEGACY_PRODUCT_OFFER) LIKE '%STANDARD CONNECT%')
AND  NOT EXISTS (SELECT 1 FROM {MIGRATION_ME}.UF_SRV_PARAM PAR WHERE SUB.LEGACY_SUB_NO = PAR.LEGACY_ENTITY_ID AND UPPER(PAR.PARAM_NAME) = 'MODEL')

UNION

SELECT distinct LEGACY_CUST_ID,LEGACY_SUB_NO,LEGACY_PRODUCT_OFFER, SUB.LEGACY_SITE_ID
FROM {MIGRATION_BI}.UF_SUBSCRIBER SUB
WHERE (LEGACY_PRODUCT_OFFER  LIKE '%Business Internet%' 
--OR LEGACY_PRODUCT_OFFER = 'BI Underlay Service'
OR UPPER(SUB.LEGACY_PRODUCT_OFFER) LIKE '%BASIC CONNECT%' 
OR UPPER(SUB.LEGACY_PRODUCT_OFFER) LIKE '%STANDARD CONNECT%')
AND  NOT EXISTS (SELECT 1 FROM {MIGRATION_BI}.UF_SRV_PARAM PAR WHERE SUB.LEGACY_SUB_NO = PAR.LEGACY_ENTITY_ID AND UPPER(PAR.PARAM_NAME) = 'MODEL')

UNION

SELECT distinct LEGACY_CUST_ID,LEGACY_SUB_NO,LEGACY_PRODUCT_OFFER, SUB.LEGACY_SITE_ID
FROM {MIGRATION_AV}.UF_SUBSCRIBER SUB
WHERE (LEGACY_PRODUCT_OFFER  LIKE '%Business Internet%' 
--OR LEGACY_PRODUCT_OFFER = 'BI Underlay Service'
OR UPPER(SUB.LEGACY_PRODUCT_OFFER) LIKE '%BASIC CONNECT%' 
OR UPPER(SUB.LEGACY_PRODUCT_OFFER) LIKE '%STANDARD CONNECT%')
AND  NOT EXISTS (SELECT 1 FROM {MIGRATION_AV}.UF_SRV_PARAM PAR WHERE SUB.LEGACY_SUB_NO = PAR.LEGACY_ENTITY_ID AND UPPER(PAR.PARAM_NAME) = 'MODEL')

UNION

SELECT distinct LEGACY_CUST_ID,LEGACY_SUB_NO,LEGACY_PRODUCT_OFFER, SUB.LEGACY_SITE_ID
FROM {MIGRATION_BV}.UF_SUBSCRIBER SUB
WHERE (LEGACY_PRODUCT_OFFER  LIKE '%Business Internet%' 
--OR LEGACY_PRODUCT_OFFER = 'BI Underlay Service'
OR UPPER(SUB.LEGACY_PRODUCT_OFFER) LIKE '%BASIC CONNECT%' 
OR UPPER(SUB.LEGACY_PRODUCT_OFFER) LIKE '%STANDARD CONNECT%')
AND  NOT EXISTS (SELECT 1 FROM {MIGRATION_BV}.UF_SRV_PARAM PAR WHERE SUB.LEGACY_SUB_NO = PAR.LEGACY_ENTITY_ID AND UPPER(PAR.PARAM_NAME) = 'MODEL')
"""

#UF525 - Missing BI Equipment --> Shiva: We are discontinuing the usage of this SQL, as it is not populating the entire missing BI EQP's
Query_Missing_BI_Equipment_OLD = fr"""
-- Missing BI Equipment
SELECT LEGACY_CUST_ID, LEGACY_SITE_ID FROM (
select /*+ parallel (t,$ORA_PARALLEL) */ legacy_cust_id, legacy_site_id from {MIGRATION_ME}.uf_subscriber t 
where legacy_product_offer not in ('Site Access', 'GTI') and 
not  exists ( select 1 from {MIGRATION_ME}.uf_srv_param , {MIGRATION_ME}.conv_tt
where legacy_sub_no =legacy_entity_id and  input_val1=comp_code and input_val2=param_name and input_val3=param_value and param_name ='Device_Acquisition_Type' and comp_code in ('Business_Internet_Main','BV_Main')
and output_val4='Bring_Your_Own')
minus
select /*+ parallel (sub,$ORA_PARALLEL )  parallel (tt,$ORA_PARALLEL ) */ distinct legacy_cust_id , legacy_site_id  from {MIGRATION_ME}.uf_subscriber sub , {MIGRATION_ME}.conv_tt tt
where sub.legacy_product_offer =input_val1 and  output_val6='Equipment' and trans_group ='TRANS_EPC_OFFER'

UNION

select /*+ parallel (t,$ORA_PARALLEL) */ legacy_cust_id, legacy_site_id from {MIGRATION_BI}.uf_subscriber t 
where legacy_product_offer not in ('Site Access', 'GTI') and 
not  exists ( select 1 from {MIGRATION_BI}.uf_srv_param , {MIGRATION_BI}.conv_tt
where legacy_sub_no =legacy_entity_id and  input_val1=comp_code and input_val2=param_name and input_val3=param_value and param_name ='Device_Acquisition_Type' and comp_code in ('Business_Internet_Main','BV_Main')
and output_val4='Bring_Your_Own')
minus
select /*+ parallel (sub,$ORA_PARALLEL )  parallel (tt,$ORA_PARALLEL ) */ distinct legacy_cust_id , legacy_site_id  from {MIGRATION_BI}.uf_subscriber sub , {MIGRATION_BI}.conv_tt tt
where sub.legacy_product_offer =input_val1 and  output_val6='Equipment' and trans_group ='TRANS_EPC_OFFER'

UNION

select /*+ parallel (t,$ORA_PARALLEL) */ legacy_cust_id, legacy_site_id from {MIGRATION_AV}.uf_subscriber t 
where legacy_product_offer not in ('Site Access', 'GTI') and 
not  exists ( select 1 from {MIGRATION_AV}.uf_srv_param , {MIGRATION_AV}.conv_tt
where legacy_sub_no =legacy_entity_id and  input_val1=comp_code and input_val2=param_name and input_val3=param_value and param_name ='Device_Acquisition_Type' and comp_code in ('Business_Internet_Main','BV_Main')
and output_val4='Bring_Your_Own')
minus
select /*+ parallel (sub,$ORA_PARALLEL )  parallel (tt,$ORA_PARALLEL ) */ distinct legacy_cust_id , legacy_site_id  from {MIGRATION_AV}.uf_subscriber sub , {MIGRATION_AV}.conv_tt tt
where sub.legacy_product_offer =input_val1 and  output_val6='Equipment' and trans_group ='TRANS_EPC_OFFER'

UNION

select /*+ parallel (t,$ORA_PARALLEL) */ legacy_cust_id, legacy_site_id from {MIGRATION_BV}.uf_subscriber t 
where legacy_product_offer not in ('Site Access', 'GTI') and 
not  exists ( select 1 from {MIGRATION_BV}.uf_srv_param , {MIGRATION_BV}.conv_tt
where legacy_sub_no =legacy_entity_id and  input_val1=comp_code and input_val2=param_name and input_val3=param_value and param_name ='Device_Acquisition_Type' and comp_code in ('Business_Internet_Main','BV_Main')
and output_val4='Bring_Your_Own')
minus
select /*+ parallel (sub,$ORA_PARALLEL )  parallel (tt,$ORA_PARALLEL ) */ distinct legacy_cust_id , legacy_site_id  from {MIGRATION_BV}.uf_subscriber sub , {MIGRATION_BV}.conv_tt tt
where sub.legacy_product_offer =input_val1 and  output_val6='Equipment' and trans_group ='TRANS_EPC_OFFER'
) UF525
WHERE LEGACY_CUST_ID IN (
SELECT DISTINCT LEGACY_CUST_ID FROM STAGE.{CM030_DIFF_TABLE}
)"""

# --UF-541 SQL - CAHNGE MIGRATION SCEMA BASED ON RELEASE
Query_UF_541 = fr"""
SELECT
    LEGACY_CUST_ID
FROM
    (
        (
            SELECT
                city,
                ADR_LINE_1,
                ZIPCODE,
                UNIT_TYPE,
                UNIT_VALUE,
                UNIT_TYPE_2,
                UNIT_VALUE_2,
                UNIT_TYPE_3,
                UNIT_VALUE_3,
                st.LEGACY_CUST_ID
            FROM
                {MIGRATION_ME}.uf_address ad,
                {MIGRATION_ME}.uf_site st
            WHERE
                ad.LEGACY_ADDRESS_ID = st.LEGACY_ADDR_ID
                AND st.LEGACY_CUST_ID IN (
                    SELECT
                        DISTINCT CAST(LEGACY_CUST_ID AS VARCHAR(20))
                    FROM
                        STAGE.{CM030_DIFF_TABLE}
                    WHERE
                        UPPER(PRODUCT_OFFER) LIKE 'ETHER%'
                )
            GROUP BY
                city,
                ADR_LINE_1,
                ZIPCODE,
                UNIT_TYPE,
                UNIT_VALUE,
                UNIT_TYPE_2,
                UNIT_VALUE_2,
                UNIT_TYPE_3,
                UNIT_VALUE_3,
                st.LEGACY_CUST_ID
            HAVING
                COUNT(*) > 1
        )
        UNION
        ALL 
        (
            SELECT
                city,
                ADR_LINE_1,
                ZIPCODE,
                UNIT_TYPE,
                UNIT_VALUE,
                UNIT_TYPE_2,
                UNIT_VALUE_2,
                UNIT_TYPE_3,
                UNIT_VALUE_3,
                st.LEGACY_CUST_ID
            FROM
                {MIGRATION_BI}.uf_address ad,
                {MIGRATION_BI}.uf_site st
            WHERE
                ad.LEGACY_ADDRESS_ID = st.LEGACY_ADDR_ID
                AND st.LEGACY_CUST_ID IN (
                    SELECT
                        DISTINCT CAST(LEGACY_CUST_ID AS VARCHAR(20))
                    FROM
                        STAGE.{CM030_DIFF_TABLE}
                    WHERE
                        UPPER(PRODUCT_OFFER) LIKE 'ETHER%'
                )
            GROUP BY
                city,
                ADR_LINE_1,
                ZIPCODE,
                UNIT_TYPE,
                UNIT_VALUE,
                UNIT_TYPE_2,
                UNIT_VALUE_2,
                UNIT_TYPE_3,
                UNIT_VALUE_3,
                st.LEGACY_CUST_ID
            HAVING
                COUNT(*) > 1
        )
        UNION
        ALL 
        (
            SELECT
                city,
                ADR_LINE_1,
                ZIPCODE,
                UNIT_TYPE,
                UNIT_VALUE,
                UNIT_TYPE_2,
                UNIT_VALUE_2,
                UNIT_TYPE_3,
                UNIT_VALUE_3,
                st.LEGACY_CUST_ID
            FROM
                {MIGRATION_AV}.uf_address ad,
                {MIGRATION_AV}.uf_site st
            WHERE
                ad.LEGACY_ADDRESS_ID = st.LEGACY_ADDR_ID
                AND st.LEGACY_CUST_ID IN (
                    SELECT
                        DISTINCT CAST(LEGACY_CUST_ID AS VARCHAR(20))
                    FROM
                        STAGE.{CM030_DIFF_TABLE}
                    WHERE
                        UPPER(PRODUCT_OFFER) LIKE 'ETHER%'
                )
            GROUP BY
                city,
                ADR_LINE_1,
                ZIPCODE,
                UNIT_TYPE,
                UNIT_VALUE,
                UNIT_TYPE_2,
                UNIT_VALUE_2,
                UNIT_TYPE_3,
                UNIT_VALUE_3,
                st.LEGACY_CUST_ID
            HAVING
                COUNT(*) > 1
        )
        UNION
        ALL 
        (
            SELECT
                city,
                ADR_LINE_1,
                ZIPCODE,
                UNIT_TYPE,
                UNIT_VALUE,
                UNIT_TYPE_2,
                UNIT_VALUE_2,
                UNIT_TYPE_3,
                UNIT_VALUE_3,
                st.LEGACY_CUST_ID
            FROM
                {MIGRATION_BV}.uf_address ad,
                {MIGRATION_BV}.uf_site st
            WHERE
                ad.LEGACY_ADDRESS_ID = st.LEGACY_ADDR_ID
                AND st.LEGACY_CUST_ID IN (
                    SELECT
                        DISTINCT CAST(LEGACY_CUST_ID AS VARCHAR(20))
                    FROM
                        STAGE.{CM030_DIFF_TABLE}
                    WHERE
                        UPPER(PRODUCT_OFFER) LIKE 'ETHER%'
                )
            GROUP BY
                city,
                ADR_LINE_1,
                ZIPCODE,
                UNIT_TYPE,
                UNIT_VALUE,
                UNIT_TYPE_2,
                UNIT_VALUE_2,
                UNIT_TYPE_3,
                UNIT_VALUE_3,
                st.LEGACY_CUST_ID
            HAVING
                COUNT(*) > 1
        )
    )"""

# QUERY TO RUN FOR GETTING DATA FROM DNORM TABLE
Query_Circuit_Account_Mismatch = fr"""
SELECT
    DISTINCT DNORM.CUSTOMER_ACCOUT_NUMBER,
    DNORM.SV_CLIPS_VALIDATION,
    -- CIRCUIT
    DNORM.SV_CLIPS_ACC_VALIDATION,
    -- ACCUNT
    DNORM.SV_CLIPS_ACC_SERV_VALIDATION -- ACCOUNT ASN CIRCUIT
FROM
    RPT_{CYCLE}.RPT_MIGRATION_DNORM DNORM
    JOIN
    (SELECT DISTINCT ROOT_ACCOUNT_NAME FROM APP_{CYCLE}.CUST_HIERARCHY CUST
    JOIN STAGE.RECON_CM030_RCA_{DB_RELEASE} CM030
    ON CUST.ACCOUNT_NAME=CM030.LEGACY_CUST_ID) ROOT_LKUP
ON DNORM.CUSTOMER_ACCOUT_NUMBER = ROOT_LKUP.ROOT_ACCOUNT_NAME
AND SV_PRODUCT_INDICATOR LIKE '%ME%' -->Shiva: Added on 08-13-2021; as CM/AM are applicable for ME prdts only
"""

#Query for PRI account (Missing PRI services)#Bhoomi - 05/09 - to get the PRI Accounts from List of accounts and RECON_CM030UNIONSQL table.
Query_PRI_Accounts = fr"""select distinct cm030.account_name from stage.RECON_CM030UNIONSQL_{DB_RELEASE} CM030
join  {BASE}.LIST_OF_ACCOUNTS acco
on acco.root_account_name  = cm030.account_name where acco.lob like '%PRI%'"""




# -- SQL FOR FETCHING THE SV AND UF ADDRESSES FOR EACH PRODUCT
Query_Ethernet_RCA_Reasons = fr"""
WITH LIST_ACCTS_QRY AS (
SELECT DISTINCT CAST(LEGACY_CUST_ID AS VARCHAR(20)) AS L_ACCOUNTS FROM STAGE.{CM030_DIFF_TABLE} WHERE UPPER(PRODUCT_OFFER) LIKE 'ETHER%'
)
SELECT DISTINCT
CUST_ID_FOR_GRP  AS CUST_ID,
    LISTAGG(TEXT, chr(10)  ON OVERFLOW TRUNCATE '***' WITH COUNT )  WITHIN GROUP (ORDER BY GROUPER,SRC, CUST_ID_FOR_GRP) OVER (PARTITION BY CUST_ID_FOR_GRP)
    AS COMBINED_ADDRESS,
    LISTAGG(SV_ADDRESS, chr(10)  ON OVERFLOW TRUNCATE '***' WITH COUNT )  WITHIN GROUP (ORDER BY GROUPER,SRC, CUST_ID_FOR_GRP) OVER (PARTITION BY CUST_ID_FOR_GRP)
    AS SV_ADDRESS,
    LISTAGG(UF_ADDRESS, chr(10)  ON OVERFLOW TRUNCATE '***' WITH COUNT )  WITHIN GROUP (ORDER BY GROUPER,SRC, CUST_ID_FOR_GRP) OVER (PARTITION BY CUST_ID_FOR_GRP)
    AS UF_ADDRESS
FROM
    (
        SELECT
            GROUPER,
            SRC,
            CUST_ID_FOR_GRP,            
            CONCAT(CONCAT(CONCAT(SRC, CUST_ID), MRC), ADDR) AS TEXT,
            CASE WHEN SRC LIKE 'SV |%' THEN ADDR ELSE NULL END AS SV_ADDRESS,
            CASE WHEN SRC LIKE 'UF |%' THEN ADDR ELSE NULL END AS UF_ADDRESS
--            CASE WHEN (CONCAT(CONCAT(CONCAT(SRC, CUST_ID), MRC), ADDR) LIKE 'SV |%' ) THEN  CONCAT(CONCAT(CONCAT(SRC, CUST_ID), MRC), ADDR) ELSE NULL END AS SV_ADDRESS,
--            CASE WHEN (CONCAT(CONCAT(CONCAT(SRC, CUST_ID), MRC), ADDR) LIKE 'UF |%' ) THEN  CONCAT(CONCAT(CONCAT(SRC, CUST_ID), MRC), ADDR) ELSE NULL END  AS UF_ADDRESS
        FROM
            (
                SELECT
                    GROUPER,
                    CONCAT(SRC, ' | ') SRC,
                    LEGACY_CUST_ID AS CUST_ID_FOR_GRP,
                    CONCAT(NVL(LEGACY_CUST_ID, ' - '), ' | ') CUST_ID,
                    CONCAT(NVL(LEGACY_SUB_NO, ' - '), ' | ') SUB_NO,
                    CONCAT(NVL(MRC, ' --- '), ' | ') MRC,
                    CONCAT(NVL(ADDR, ' - '), ' | ') ADDR
                FROM
                    (
                        SELECT
                            1 AS GROUPER,
                            'UF' AS SRC,
                            UF.LEGACY_CUST_ID AS LEGACY_CUST_ID,
                            UF.LEGACY_SUB_NO AS LEGACY_SUB_NO,
                            CAST(UF.MANUAL_OVERRIDE_AMOUNT AS VARCHAR(20)) AS MRC,
                            UF.ADR_LINE_1 AS ADDR
                        FROM
                            (
                                SELECT
                                    *
                                FROM
                                    (
                                        SELECT
                                            SUB.LEGACY_CUST_ID,
                                            SUB.LEGACY_SITE_ID
                                            /*, AD.LEGACY_ADDRESS_ID */
,
                                            SUB.LEGACY_SUB_NO,
                                            SERVICE_SEQ,
                                            SUB.AAN,
                                            SUB.LEGACY_PRODUCT_OFFER,
                                            SRV.SERVICE_EFF_DATE,
                                            AD.ELOC_LOCATION_ID,
                                            AD.UNIT_TYPE,
                                            AD.UNIT_VALUE,
                                            SRV.MANUAL_OVERRIDE_AMOUNT,
                                            SRV.SERVICE_CODE,
                                            AD.ADR_LINE_1
                                        FROM
                                            {MIGRATION_ME}.UF_SUBSCRIBER SUB
                                            LEFT JOIN {MIGRATION_ME}.UF_ADDRESS AD ON SUB.LEGACY_SITE_ID = AD.LEGACY_ADDRESS_ID
                                            LEFT JOIN {MIGRATION_ME}.UF_SERVICES SRV ON SUB.LEGACY_CUST_ID = SRV.LEGACY_CUST_ID
                                            AND SUB.LEGACY_SUB_NO = SRV.LEGACY_ENTITY_ID
                                        WHERE
                                            CAST(SUB.LEGACY_CUST_ID AS VARCHAR(20)) in (SELECT LIST_ACCTS_QRY.L_ACCOUNTS FROM LIST_ACCTS_QRY)
                                        UNION
                                        ALL
                                        SELECT
                                            SUB.LEGACY_CUST_ID,
                                            SUB.LEGACY_SITE_ID
                                            /*, AD.LEGACY_ADDRESS_ID */
,
                                            SUB.LEGACY_SUB_NO,
                                            SERVICE_SEQ,
                                            SUB.AAN,
                                            SUB.LEGACY_PRODUCT_OFFER,
                                            SRV.SERVICE_EFF_DATE,
                                            AD.ELOC_LOCATION_ID,
                                            AD.UNIT_TYPE,
                                            AD.UNIT_VALUE,
                                            SRV.MANUAL_OVERRIDE_AMOUNT,
                                            SRV.SERVICE_CODE,
                                            AD.ADR_LINE_1
                                        FROM
                                            {MIGRATION_BI}.UF_SUBSCRIBER SUB
                                            left JOIN {MIGRATION_BI}.UF_ADDRESS AD ON SUB.LEGACY_SITE_ID = AD.LEGACY_ADDRESS_ID
                                            LEFT JOIN {MIGRATION_BI}.UF_SERVICES SRV ON SUB.LEGACY_CUST_ID = SRV.LEGACY_CUST_ID
                                            AND SUB.LEGACY_SUB_NO = SRV.LEGACY_ENTITY_ID
                                        WHERE
                                            CAST(SUB.LEGACY_CUST_ID AS VARCHAR(20)) in (SELECT LIST_ACCTS_QRY.L_ACCOUNTS FROM LIST_ACCTS_QRY)
                                        UNION
                                        ALL
                                        SELECT
                                            SUB.LEGACY_CUST_ID,
                                            SUB.LEGACY_SITE_ID
                                            /*, AD.LEGACY_ADDRESS_ID */
,
                                            SUB.LEGACY_SUB_NO,
                                            SERVICE_SEQ,
                                            SUB.AAN,
                                            SUB.LEGACY_PRODUCT_OFFER,
                                            SRV.SERVICE_EFF_DATE,
                                            AD.ELOC_LOCATION_ID,
                                            AD.UNIT_TYPE,
                                            AD.UNIT_VALUE,
                                            SRV.MANUAL_OVERRIDE_AMOUNT,
                                            SRV.SERVICE_CODE,
                                            AD.ADR_LINE_1
                                        FROM
                                            {MIGRATION_AV}.UF_SUBSCRIBER SUB
                                            left JOIN {MIGRATION_AV}.UF_ADDRESS AD ON SUB.LEGACY_SITE_ID = AD.LEGACY_ADDRESS_ID
                                            LEFT JOIN {MIGRATION_AV}.UF_SERVICES SRV ON SUB.LEGACY_CUST_ID = SRV.LEGACY_CUST_ID
                                            AND SUB.LEGACY_SUB_NO = SRV.LEGACY_ENTITY_ID
                                        WHERE
                                            CAST(SUB.LEGACY_CUST_ID AS VARCHAR(20)) in (SELECT LIST_ACCTS_QRY.L_ACCOUNTS FROM LIST_ACCTS_QRY)
                                        UNION
                                        ALL
                                        SELECT
                                            SUB.LEGACY_CUST_ID,
                                            SUB.LEGACY_SITE_ID
                                            /*, AD.LEGACY_ADDRESS_ID */
,
                                            SUB.LEGACY_SUB_NO,
                                            SERVICE_SEQ,
                                            SUB.AAN,
                                            SUB.LEGACY_PRODUCT_OFFER,
                                            SRV.SERVICE_EFF_DATE,
                                            AD.ELOC_LOCATION_ID,
                                            AD.UNIT_TYPE,
                                            AD.UNIT_VALUE,
                                            SRV.MANUAL_OVERRIDE_AMOUNT,
                                            SRV.SERVICE_CODE,
                                            AD.ADR_LINE_1
                                        FROM
                                            {MIGRATION_BV}.UF_SUBSCRIBER SUB
                                            LEFT JOIN {MIGRATION_BV}.UF_ADDRESS AD ON SUB.LEGACY_SITE_ID = AD.LEGACY_ADDRESS_ID
                                            LEFT JOIN {MIGRATION_BV}.UF_SERVICES SRV ON SUB.LEGACY_CUST_ID = SRV.LEGACY_CUST_ID
                                            AND SUB.LEGACY_SUB_NO = SRV.LEGACY_ENTITY_ID
                                        WHERE
                                            CAST(SUB.LEGACY_CUST_ID AS VARCHAR(20)) in (SELECT LIST_ACCTS_QRY.L_ACCOUNTS FROM LIST_ACCTS_QRY)
                                    ) UF_ADDRESS
                                WHERE
                                    LEGACY_PRODUCT_OFFER = 'SWITCH'
                                ORDER BY
                                    ADR_LINE_1 ASC
                            ) UF
                        UNION
                        ALL
                        SELECT
                            1 AS GROUPER,
                            'SV' AS SRC,
                            SV.ACCOUNT_NUMBER AS LEGACY_CUST_ID,
                            SV.LEGACY_SUB_NO AS LEGACY_SUB_NO,
                            CAST(SV.MANUAL_OVERRIDE_AMOUNT AS VARCHAR(20)) AS MRC,
                            SV.SV_STD_ADD AS ADDR
                        FROM
                            (
                                SELECT
                                    distinct --'0070' AS RECORD_TYPE,
                                    --'SV' SOURCE_SYSTEM,
                                    ACCT.ACCOUNT_NAME AS ACCOUNT_NUMBER,
                                    SH.SERVICE_NAME as LEGACY_SUB_NO,
                                    --CHY.INVOICE_ACCOUNT_NAME,
                                    --PH.PRODUCT_NAME, -- service code
                                    --nvl(to_char(Sh.ACTIVE_DATE,'YYYYMMDDHHMISS'),'') AS SERVICE_EFF_DATE,
                                    --' ' as service_end_date,
                                    PIH.GENERAL_8 AS MANUAL_OVERRIDE_AMOUNT,
                                    --'ACTIVE' as service_status,
                                    --nvl(to_char(Sh.ACTIVE_DATE,'YYYYMMDDHHMISS'),'') as status_date ,
                                    --ah.line_1,
                                    --ah.line_2,
                                    --ah.suburb "CITY",
                                    --ah.state "COUNTY",
                                    --ah.country,
                                    ad.STD_LINE1 as SV_STD_ADD
                                from
                                    {BASE}.ACCOUNT acct
                                    LEFT JOIN {BASE}.CUSTOMER_NODE_HISTORY CH on CH.prime_account_id = acct.account_id
                                    LEFT JOIN {BASE}.PRODUCT_INSTANCE_HISTORY PIH ON CH.CUSTOMER_NODE_ID = PIH.CUSTOMER_NODE_ID
                                    LEFT JOIN {BASE}.SERVICE_HISTORY SH ON SH.BASE_PRODUCT_INSTANCE_ID = PIH.PRODUCT_INSTANCE_ID
                                    LEFT JOIN {BASE}.PRODUCT_HISTORY PH ON PIH.PRODUCT_ID = PH.PRODUCT_ID
                                    LEFT JOIN {BASE}.ADDRESS_HISTORY ah on ah.address_id = SH.A_ADDRESS_ID
                                    and sysdate between AH.EFFECTIVE_START_DATE
                                    and AH.EFFECTIVE_END_DATE
                                    LEFT JOIN {APP}.APP_SV_SERV_ADD_ELOC_SUCESS ad on ah.line_1 = ad.line_1
                                    and ad.MODIFY_DTS IS NULL --join &Enter_{APP}or2..CUST_HIERARCHY CHY
                                    --             on acct.account_name=CHY.ACCOUNT_NAME
                                WHERE
                                    sysdate BETWEEN CH.EFFECTIVE_START_DATE
                                    AND CH.EFFECTIVE_END_DATE
                                    AND sysdate BETWEEN PIH.EFFECTIVE_START_DATE
                                    AND PIH.EFFECTIVE_END_DATE
                                    AND sysdate BETWEEN PH.EFFECTIVE_START_DATE
                                    AND PH.EFFECTIVE_END_DATE
                                    AND SH.SERVICE_STATUS_CODE = '3'
                                    AND PIH.PRODUCT_INSTANCE_STATUS_CODE = '3'
                                    AND SYSDATE BETWEEN SH.EFFECTIVE_START_DATE
                                    AND SH.EFFECTIVE_END_DATE --AND PIH.PRODUCT_ID NOT IN ('8101200')
                                    AND PIH.PRODUCT_ID IN (8101200) -- EDI UNI, ENS UNI
                                    AND CAST(acct.account_name AS VARCHAR(20)) IN (SELECT LIST_ACCTS_QRY.L_ACCOUNTS FROM LIST_ACCTS_QRY)
                                    AND PH.PRODUCT_NAME IN ('Ethernet Equipment Fee')
                                ORDER BY
                                    SV_STD_ADD ASC
                            ) SV
                    ) UF_UNION_SV
            ) CONCAT_QRY
    ) MERGE_QRY
"""


# CHANNEL COUNT SQLS

# -- SQL FOR GETTING UF CHANNEL COUNTS

Query_UF_Channel_Counts = fr"""
select
    LEGACY_CUST_ID,
    LEGACY_SUB_NO,
    LEGACY_ACCOUNT_NO,
    LEGACY_PRODUCT_OFFER,
    sum(TWO_WAY_CHANNELS) AS CHANNEL_COUNT
from
    MIGRATION_BKP.{UF_SUBSCRIBER}
where
    LEGACY_PRODUCT_OFFER in ( 'Business Trunk Voice', 'Business SIP Trunk Voice', 'Business VoiceEdge', 'Business Voice' )
group by
    LEGACY_CUST_ID,
    LEGACY_ACCOUNT_NO,
    LEGACY_SUB_NO,
    LEGACY_PRODUCT_OFFER"""

# -- SQL FOR GETTING SV CHANNEL COUNTS

Query_SV_Channel_Counts = fr"""
SELECT
    LOB,
    root_ban,
    root_child_ban,
    base_service_name,
    SUM(fractional_count) fractional_count
FROM
    {APP}.app_sv_channel_count
WHERE invoice_cycle = 2
GROUP BY
    LOB,
    root_ban,
    root_child_ban,
    base_service_name
   """
#UF832: Missing services in UF_SUBSCRIBER
Query_CM030_P_UF832 = fr"""
(SELECT  /*+ PARALLEL (CUST ,$ORA_PARALLEL)   PARALLEL (A,$ORA_PARALLEL) */     CUST.LEGACY_CUST_ID FROM {MIGRATION_ME}.UF_CORPORATE A, {MIGRATION_ME}.UF_CUSTOMER CUST  WHERE IS_ROOT='Y' AND 
 NOT EXISTS ( SELECT 1 FROM {MIGRATION_ME}.UF_CORPORATE B  WHERE A.LEGACY_CORP_ID =B.LEGACY_CORP_ID AND  IS_ROOT<>'Y' )
 AND A.LEGACY_ENTITY_ID = CUST.PARENT_ID
UNION ALL
SELECT  /*+ PARALLEL (C,$ORA_PARALLEL)   PARALLEL (B,$ORA_PARALLEL) */   C.LEGACY_CUST_ID FROM {MIGRATION_ME}.UF_CUSTOMER C, {MIGRATION_ME}.UF_CORPORATE B
WHERE C.PARENT_ID=B.LEGACY_ENTITY_ID
AND (IS_ROOT = 'N' OR B.PARENT_ID IS NOT NULL)
)
 MINUS
SELECT LEGACY_CUST_ID FROM {MIGRATION_ME}.UF_SUBSCRIBER   

UNION

(SELECT  /*+ PARALLEL (CUST ,$ORA_PARALLEL)   PARALLEL (A,$ORA_PARALLEL) */     CUST.LEGACY_CUST_ID FROM {MIGRATION_BI}.UF_CORPORATE A, {MIGRATION_BI}.UF_CUSTOMER CUST  WHERE IS_ROOT='Y' AND 
 NOT EXISTS ( SELECT 1 FROM {MIGRATION_BI}.UF_CORPORATE B  WHERE A.LEGACY_CORP_ID =B.LEGACY_CORP_ID AND  IS_ROOT<>'Y' )
 AND A.LEGACY_ENTITY_ID = CUST.PARENT_ID
UNION ALL
SELECT  /*+ PARALLEL (C,$ORA_PARALLEL)   PARALLEL (B,$ORA_PARALLEL) */   C.LEGACY_CUST_ID FROM {MIGRATION_BI}.UF_CUSTOMER C, {MIGRATION_BI}.UF_CORPORATE B
WHERE C.PARENT_ID=B.LEGACY_ENTITY_ID
AND (IS_ROOT = 'N' OR B.PARENT_ID IS NOT NULL)
)
 MINUS
SELECT LEGACY_CUST_ID FROM {MIGRATION_BI}.UF_SUBSCRIBER   

UNION

(SELECT  /*+ PARALLEL (CUST ,$ORA_PARALLEL)   PARALLEL (A,$ORA_PARALLEL) */     CUST.LEGACY_CUST_ID FROM {MIGRATION_AV}.UF_CORPORATE A, {MIGRATION_AV}.UF_CUSTOMER CUST  WHERE IS_ROOT='Y' AND 
 NOT EXISTS ( SELECT 1 FROM {MIGRATION_AV}.UF_CORPORATE B  WHERE A.LEGACY_CORP_ID =B.LEGACY_CORP_ID AND  IS_ROOT<>'Y' )
 AND A.LEGACY_ENTITY_ID = CUST.PARENT_ID
UNION ALL
SELECT  /*+ PARALLEL (C,$ORA_PARALLEL)   PARALLEL (B,$ORA_PARALLEL) */   C.LEGACY_CUST_ID FROM {MIGRATION_AV}.UF_CUSTOMER C, {MIGRATION_AV}.UF_CORPORATE B
WHERE C.PARENT_ID=B.LEGACY_ENTITY_ID
AND (IS_ROOT = 'N' OR B.PARENT_ID IS NOT NULL)
)
 MINUS
SELECT LEGACY_CUST_ID FROM {MIGRATION_AV}.UF_SUBSCRIBER   

UNION

(SELECT  /*+ PARALLEL (CUST ,$ORA_PARALLEL)   PARALLEL (A,$ORA_PARALLEL) */     CUST.LEGACY_CUST_ID FROM {MIGRATION_BV}.UF_CORPORATE A, {MIGRATION_BV}.UF_CUSTOMER CUST  WHERE IS_ROOT='Y' AND 
 NOT EXISTS ( SELECT 1 FROM {MIGRATION_BV}.UF_CORPORATE B  WHERE A.LEGACY_CORP_ID =B.LEGACY_CORP_ID AND  IS_ROOT<>'Y' )
 AND A.LEGACY_ENTITY_ID = CUST.PARENT_ID
UNION ALL
SELECT  /*+ PARALLEL (C,$ORA_PARALLEL)   PARALLEL (B,$ORA_PARALLEL) */   C.LEGACY_CUST_ID FROM {MIGRATION_BV}.UF_CUSTOMER C, {MIGRATION_BV}.UF_CORPORATE B
WHERE C.PARENT_ID=B.LEGACY_ENTITY_ID
AND (IS_ROOT = 'N' OR B.PARENT_ID IS NOT NULL)
)
 MINUS
SELECT LEGACY_CUST_ID FROM {MIGRATION_BV}.UF_SUBSCRIBER   
"""

#Missing AAN for BVE Services --Bhoomi Added Missing AAN for BVE Service - 08/21/23
Query_Missing_AAN_BVE_Services = fr"""select root_account_name from {MIGRATION_AV}.BVE_SRV_ID
where legacy_product_offer = 'Business VoiceEdge' and aan is null"""




#UNI and EQP Fee on different invoice account no in SV -- Comment out the below logic for now.In future we will manual flagging of such accounts since it is only a handful
Query_UNI_EQP_DIFF_INVC_IN_SV_OLD = fr"""
SELECT S1.LEGACY_ACCOUNT_NO   EQP_CHILD_BAN
FROM {MIGRATION_ME}.UF_SUBSCRIBER S1, {MIGRATION_ME}.UF_SERVICES S2
WHERE S1.LEGACY_PRODUCT_OFFER IN ('SWITCH')
    AND S1.LEGACY_CUST_ID = S2.LEGACY_CUST_ID
    AND S1.LEGACY_SUB_NO = S2.LEGACY_ENTITY_ID
    AND S2.MANUAL_OVERRIDE_AMOUNT > 0
AND S1.LEGACY_CUST_ID IN
( SELECT LEGACY_CUST_ID FROM 
(SELECT LEGACY_CUST_ID,COUNT(DISTINCT LEGACY_ACCOUNT_NO) FROM {MIGRATION_ME}.UF_ACCOUNT 
	GROUP BY LEGACY_CUST_ID HAVING COUNT(DISTINCT LEGACY_ACCOUNT_NO)>1) )
MINUS
SELECT BAN FROM {APP}.APP_SV_SERV_ADD_ELOC_SUCESS
WHERE PRODUCT_NAME = 'Ethernet Equipment Fee'

UNION

SELECT S1.LEGACY_ACCOUNT_NO   EQP_CHILD_BAN
FROM {MIGRATION_BI}.UF_SUBSCRIBER S1, {MIGRATION_BI}.UF_SERVICES S2
WHERE S1.LEGACY_PRODUCT_OFFER IN ('SWITCH')
    AND S1.LEGACY_CUST_ID = S2.LEGACY_CUST_ID
    AND S1.LEGACY_SUB_NO = S2.LEGACY_ENTITY_ID
    AND S2.MANUAL_OVERRIDE_AMOUNT > 0
AND S1.LEGACY_CUST_ID IN
( SELECT LEGACY_CUST_ID FROM 
(SELECT LEGACY_CUST_ID,COUNT(DISTINCT LEGACY_ACCOUNT_NO) FROM {MIGRATION_BI}.UF_ACCOUNT 
	GROUP BY LEGACY_CUST_ID HAVING COUNT(DISTINCT LEGACY_ACCOUNT_NO)>1) )
MINUS
SELECT BAN FROM {APP}.APP_SV_SERV_ADD_ELOC_SUCESS
WHERE PRODUCT_NAME = 'Ethernet Equipment Fee'

UNION

SELECT S1.LEGACY_ACCOUNT_NO   EQP_CHILD_BAN
FROM {MIGRATION_AV}.UF_SUBSCRIBER S1, {MIGRATION_AV}.UF_SERVICES S2
WHERE S1.LEGACY_PRODUCT_OFFER IN ('SWITCH')
    AND S1.LEGACY_CUST_ID = S2.LEGACY_CUST_ID
    AND S1.LEGACY_SUB_NO = S2.LEGACY_ENTITY_ID
    AND S2.MANUAL_OVERRIDE_AMOUNT > 0
AND S1.LEGACY_CUST_ID IN
( SELECT LEGACY_CUST_ID FROM 
(SELECT LEGACY_CUST_ID,COUNT(DISTINCT LEGACY_ACCOUNT_NO) FROM {MIGRATION_AV}.UF_ACCOUNT 
	GROUP BY LEGACY_CUST_ID HAVING COUNT(DISTINCT LEGACY_ACCOUNT_NO)>1) )
MINUS
SELECT BAN FROM {APP}.APP_SV_SERV_ADD_ELOC_SUCESS
WHERE PRODUCT_NAME = 'Ethernet Equipment Fee'

UNION

SELECT S1.LEGACY_ACCOUNT_NO   EQP_CHILD_BAN
FROM {MIGRATION_BV}.UF_SUBSCRIBER S1, {MIGRATION_BV}.UF_SERVICES S2
WHERE S1.LEGACY_PRODUCT_OFFER IN ('SWITCH')
    AND S1.LEGACY_CUST_ID = S2.LEGACY_CUST_ID
    AND S1.LEGACY_SUB_NO = S2.LEGACY_ENTITY_ID
    AND S2.MANUAL_OVERRIDE_AMOUNT > 0
AND S1.LEGACY_CUST_ID IN
( SELECT LEGACY_CUST_ID FROM 
(SELECT LEGACY_CUST_ID,COUNT(DISTINCT LEGACY_ACCOUNT_NO) FROM {MIGRATION_BV}.UF_ACCOUNT 
	GROUP BY LEGACY_CUST_ID HAVING COUNT(DISTINCT LEGACY_ACCOUNT_NO)>1) )
MINUS
SELECT BAN FROM {APP}.APP_SV_SERV_ADD_ELOC_SUCESS
WHERE PRODUCT_NAME = 'Ethernet Equipment Fee'
"""

#Circuit Mismatch/Account mismatch MRC  report - additional filter
Query_CM_AM_ADD_FILTER = fr"""
SELECT 	DISTINCT 
		--SER.LEGACY_CUST_ID
        --,SER.LEGACY_ENTITY_ID,
		SER.SERVICE_CODE
        --, SUB.LEGACY_PRODUCT_OFFER
FROM 		{MIGRATION_ME}.UF_SERVICES 	SER
INNER JOIN 	{MIGRATION_ME}.UF_SUBSCRIBER SUB 
ON 			SUB.LEGACY_CUST_ID 		= SER.LEGACY_CUST_ID
AND 		SUB.LEGACY_ACCOUNT_NO 	= SER.LEGACY_ACCOUNT_NO
AND 		SUB.LEGACY_SUB_NO 		= SER.LEGACY_ENTITY_ID
INNER JOIN STAGE.{CM030_UNION_SQL}  RCA 
ON          SER.LEGACY_ACCOUNT_NO   = RCA.ACCOUNT_NAME
AND         SER.SERVICE_CODE        = RCA.PRODUCT_NAME 
WHERE	SUB.LEGACY_PRODUCT_OFFER IN (
        'EDI UNI',
        'ENS UNI',
        'EPL UNI',
        'EVC Endpoint',
        'EVC_EDI',
        'EVC_ENS',
        'EVC_EPL',
        'EVC_EVPL',
        'EVPL UNI',
        'SWITCH'
	)
UNION
SELECT 	DISTINCT 
		--SER.LEGACY_CUST_ID
        --,SER.LEGACY_ENTITY_ID,
		SER.SERVICE_CODE
        --,SUB.LEGACY_PRODUCT_OFFER
FROM 		{MIGRATION_BI}.UF_SERVICES SER
INNER JOIN 	{MIGRATION_BI}.UF_SUBSCRIBER SUB 
ON 			SUB.LEGACY_CUST_ID 		= SER.LEGACY_CUST_ID
AND 		SUB.LEGACY_ACCOUNT_NO 	= SER.LEGACY_ACCOUNT_NO
AND 		SUB.LEGACY_SUB_NO 		= SER.LEGACY_ENTITY_ID
INNER JOIN STAGE.{CM030_UNION_SQL}  RCA 
ON          SER.LEGACY_ACCOUNT_NO   = RCA.ACCOUNT_NAME
AND         SER.SERVICE_CODE        = RCA.PRODUCT_NAME 
WHERE	SUB.LEGACY_PRODUCT_OFFER IN (
        'EDI UNI',
        'ENS UNI',
        'EPL UNI',
        'EVC Endpoint',
        'EVC_EDI',
        'EVC_ENS',
        'EVC_EPL',
        'EVC_EVPL',
        'EVPL UNI',
        'SWITCH'
	)
UNION
SELECT 	DISTINCT 
		--SER.LEGACY_CUST_ID
        --,SER.LEGACY_ENTITY_ID,
        SER.SERVICE_CODE
        --,SUB.LEGACY_PRODUCT_OFFER
FROM 		{MIGRATION_AV}.UF_SERVICES SER
INNER JOIN 	{MIGRATION_AV}.UF_SUBSCRIBER SUB 
ON 			SUB.LEGACY_CUST_ID 		= SER.LEGACY_CUST_ID
AND 		SUB.LEGACY_ACCOUNT_NO 	= SER.LEGACY_ACCOUNT_NO
AND 		SUB.LEGACY_SUB_NO 		= SER.LEGACY_ENTITY_ID
INNER JOIN STAGE.{CM030_UNION_SQL}  RCA 
ON          SER.LEGACY_ACCOUNT_NO   = RCA.ACCOUNT_NAME
AND         SER.SERVICE_CODE        = RCA.PRODUCT_NAME 
WHERE	SUB.LEGACY_PRODUCT_OFFER IN (
        'EDI UNI',
        'ENS UNI',
        'EPL UNI',
        'EVC Endpoint',
        'EVC_EDI',
        'EVC_ENS',
        'EVC_EPL',
        'EVC_EVPL',
        'EVPL UNI',
        'SWITCH'
	)
UNION
SELECT 	DISTINCT 
		--SER.LEGACY_CUST_ID
        --,SER.LEGACY_ENTITY_ID,
        SER.SERVICE_CODE
        --,SUB.LEGACY_PRODUCT_OFFER
FROM 		{MIGRATION_BV}.UF_SERVICES SER
INNER JOIN 	{MIGRATION_BV}.UF_SUBSCRIBER SUB 
ON 			SUB.LEGACY_CUST_ID 		= SER.LEGACY_CUST_ID
AND 		SUB.LEGACY_ACCOUNT_NO 	= SER.LEGACY_ACCOUNT_NO
AND 		SUB.LEGACY_SUB_NO 		= SER.LEGACY_ENTITY_ID
INNER JOIN STAGE.{CM030_UNION_SQL}  RCA 
ON          SER.LEGACY_ACCOUNT_NO   = RCA.ACCOUNT_NAME
AND         SER.SERVICE_CODE        = RCA.PRODUCT_NAME 
WHERE	SUB.LEGACY_PRODUCT_OFFER IN (
        'EDI UNI',
        'ENS UNI',
        'EPL UNI',
        'EVC Endpoint',
        'EVC_EDI',
        'EVC_ENS',
        'EVC_EPL',
        'EVC_EVPL',
        'EVPL UNI',
        'SWITCH'
	)
"""

#Missing PRI Service - Bhoomi Adding this new RCA column 05/02
Query_Missing_PRI_Service = fr"""SELECT DISTINCT
    --ch.account_name,
    ch.root_account_name,
    ch.invoice_account_name,
	ch.lob,
    cnh.effective_start_date,
    cnh.effective_end_date,
    ph.product_name AS legacy_product_offer,
	pih.product_id,
    nvl(
        TO_CHAR(
            sh.active_date,
            'YYYYMMDDHHMISS'
        ),
        ''
    ) AS init_act_date,
    TO_CHAR(ltrim(rtrim(sh.service_status_code) ) ) AS sub_status,
    (
        CASE
            WHEN sh.service_status_code = 3  THEN nvl(
                TO_CHAR(
                    sh.active_date,
                    'YYYYMMDDHHMISS'
                ),
                ''
            )
            WHEN sh.service_status_code = 9   THEN nvl(
                TO_CHAR(
                    sh.effective_start_date,
                    'YYYYMMDDHHMISS'
                ),
                ''
            )
            ELSE NULL
        END
    ) AS sub_sts_change_date,
    'POST' payment_category,
    (
        CASE
            WHEN sh.service_status_code = 9   THEN nvl(
                TO_CHAR(
                    sh.effective_start_date,
                    'YYYYMMDDHHMISS'
                ),
                ''
            )
            ELSE NULL
        END
    ) AS sub_sts_exp_date,
    'C' AS product_type,
    DECODE(
        (
            CASE
                WHEN sh.service_status_code = 9   THEN nvl(
                    TO_CHAR(
                        sh.effective_start_date,
                        'YYYYMMDDHHMISS'
                    ),
                    ''
                )
                ELSE NULL
            END
        ),
        NULL,
        'N',
        'Y'
    ) AS hist_ind,
    'ENGLISH' AS sub_language,
    sh.service_name AS legacy_main_sub_no,
    case when csg.csg_aan like '8%' and length(csg.csg_aan) = 16 then csg.csg_aan else null end as aan,
    sh.general_3
FROM
    {BASE}.customer_node_history cnh
join {BASE}.customer_node_history cnh1
     on cnh.customer_node_id = cnh1.customer_node_id
      and sysdate between cnh1.EFFECTIVE_START_DATE and cnh1.EFFECTIVE_END_DATE
      and cnh1.CUSTOMER_NODE_STATUS_CODE = 3
join {BASE}.customer_node_history cnh2
      on nvl(cnh1.root_customer_node_id,cnh1.customer_node_id) = cnh2.customer_node_id
      and sysdate between cnh2.EFFECTIVE_START_DATE and cnh2.EFFECTIVE_END_DATE
      and cnh2.CUSTOMER_NODE_STATUS_CODE = 3
/*
left join {BASE}.customer_node_da_array cnda
      on cnda.customer_node_id  = cnh2.customer_node_id
      and  SYSDATE BETWEEN cnda.effective_start_date AND cnda.effective_end_date
      and cnda.derived_attribute_id = 8000208*/
join {BASE}.account a
      on cnh.customer_node_id = a.customer_node_id
      and a.ACCOUNT_TYPE_ID = 10000
join {BASE}.product_instance_history pih
      on cnh1.customer_node_id = pih.customer_node_id
      and pih.PRODUCT_INSTANCE_STATUS_CODE = 3
join {BASE}.service_history sh
      on sh.base_product_instance_id = nvl(pih.base_product_instance_id,pih.product_instance_id)
      and sh.SERVICE_STATUS_CODE = 3
      and sysdate between sh.EFFECTIVE_START_DATE and sh.EFFECTIVE_END_DATE
left join {APP}.app_sv_csg_assoc_dtls csg 
      on sh.service_name = csg.service_name
join {BASE}.product_history ph
     on  pih.product_id = ph.product_id
     AND SYSDATE BETWEEN ph.effective_start_date AND ph.effective_end_date
join {APP}.cust_hierarchy ch
    on ch.account_name = a.account_name
WHERE
     --cnda.INDEX1_VALUE is not null
      SYSDATE BETWEEN cnh.effective_start_date AND cnh.effective_end_date
    and cnh.customer_node_status_code = 3
    AND
        cnh.customer_node_status_code = 3
    AND cnh.schedule_id IN ( 5502499,5500387 )
    AND pih.product_id IN (
            '8000218'
        )"""


#Bhoomi - Adding Multiple Voice to one AAN(for PRI, SIP and BVE) - 05-03
Query_Multiple_Voice_To_One_AAN_OLD = fr"""SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_ME}.UF_SERVICES SER, {MIGRATION_ME}.UF_SUB_ASSOCIATION ASSO
WHERE  SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('Advanced_Voice_PRI_Gateway_Equipment_Fee')
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_PRI_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_BI}.UF_SERVICES SER, {MIGRATION_BI}.UF_SUB_ASSOCIATION ASSO
WHERE  SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('Advanced_Voice_PRI_Gateway_Equipment_Fee')
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_PRI_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_AV}.UF_SERVICES SER, {MIGRATION_AV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('Advanced_Voice_PRI_Gateway_Equipment_Fee')
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_PRI_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_BV}.UF_SERVICES SER, {MIGRATION_BV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('Advanced_Voice_PRI_Gateway_Equipment_Fee')
AND SER.LEGACY_ENTITY_ID  IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_PRI_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
------SIP Portion
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_ME}.UF_SERVICES SER, {MIGRATION_ME}.UF_SUB_ASSOCIATION ASSO
WHERE  SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('Advanced_Voice_SIP_Gateway_Equipment_Fee')
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_SIP_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_BI}.UF_SERVICES SER, {MIGRATION_BI}.UF_SUB_ASSOCIATION ASSO
WHERE  SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('Advanced_Voice_SIP_Gateway_Equipment_Fee')
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_SIP_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_AV}.UF_SERVICES SER, {MIGRATION_AV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('Advanced_Voice_SIP_Gateway_Equipment_Fee')
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_SIP_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_BV}.UF_SERVICES SER, {MIGRATION_BV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('Advanced_Voice_SIP_Gateway_Equipment_Fee')
AND SER.LEGACY_ENTITY_ID  IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_SIP_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
----BVE portion
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_ME}.UF_SERVICES SER, {MIGRATION_ME}.UF_SUB_ASSOCIATION ASSO
WHERE  SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') 
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_BVE_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_BI}.UF_SERVICES SER, {MIGRATION_BI}.UF_SUB_ASSOCIATION ASSO
WHERE  SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') 
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_BVE_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_AV}.UF_SERVICES SER, {MIGRATION_AV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') 
AND  SER.LEGACY_ENTITY_ID IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_BVE_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT,ASSO.INDEP_SUB_ID
FROM {MIGRATION_BV}.UF_SERVICES SER, {MIGRATION_BV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID AND SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') 
AND SER.LEGACY_ENTITY_ID  IN (SELECT EQP_SERIAL_EQP FROM {APP}.APP_BVE_OVER_LAY GROUP BY EQP_SERIAL_EQP HAVING COUNT(DISTINCT SERVICE_NAME)>1)"""

#Bhoomi - Adding Multiple Voice to one AAN(for PRI, SIP and BVE) - 06-15
Query_Multiple_Voice_To_One_AAN = fr"""select * from
(select distinct SV_ROOT_BAN, count(distinct service_name) over(partition by csg_aan) as voice_count from {APP}.app_sv_csg_assoc_dtls where PRODUCT_NAME in ('Business Trunk Voice','Business SIP Trunk Voice','Business VoiceEdge'))a
where a.voice_count>1"""


#For the same site, we are getting same MRC for both 'Equipment Fee - Voice' and 'Equipment Fee - Data' from source. In Extract flow, we are deleting one of the MRC as per our current logic."
#Bhoomi - Addded below query to flag above scenario - 05-17-2022 - Equipment Double billing
Query_Equipment_Double_Billing = fr"""
SELECT BV.ACCOUNT_NAME  , BI.ACCOUNT_NUMBER, BV.LEGACY_ACCOUNT_NO, BI.LEGACY_ACCOUNT_NAME , BI.ROOT_ACCOUNT_NAME , BV.MANUAL_OVERRIDE_AMOUNT , BV.PRODUCT_DISPLAY_NAME, BI.PRODUCT_NAME
FROM (SELECT * FROM (
select a.account_name,pih.product_id,ph.product_display_name,pih.general_7 as "QTY",pih.general_8 as MANUAL_OVERRIDE_AMOUNT,UF.LEGACY_ACCOUNT_NAME AS LEGACY_ACCOUNT_NO, UF.LEGACY_SUB_NO_BASE, UF.INIT_ACT_DATE,
case when pih.product_id in (8000125,8000125,8000971,8000972,8000973,8000974,8000975,8000976,8000977,8000978,8001031,8001032,8001033,8001034,8201371,8201381)
     then to_number(pih.general_7) * to_number(pih.general_8) 
     when pih.product_id in (6000233,6000231,6000226,8000351,8001481)
     then to_number(ch.general_2) 
       else to_number(pih.general_8) 
         end  "Total_MRC",
         ROOT_ACCOUNT_NAME
 from {BASE}.product_instance_history pih
join {BASE}.account a
     on a.account_type_id = 10000
     and a.customer_node_id = pih.customer_node_id
join {BASE}.product_history ph
     on ph.product_id = pih.product_id
     and sysdate between ph.effective_start_date and ph.effective_end_date
left join {BASE}.service_history sh
      on sh.base_product_instance_id = pih.product_instance_id
      and sh.service_status_code = 3
      and sysdate between sh.effective_start_date and sh.effective_end_date
left join {BASE}.contract_history ch
       on ch.contract_id = sh.contract_id
       and ch.contract_status_code in ( 3, 8)
       and sysdate between ch.effective_start_date and ch.effective_end_date
        JOIN {APP}.Cust_hierarchy Cust
ON cust.ACCOUNT_NAME = a.ACCOUNT_NAME
join (select distinct legacy_account_name, lEGACY_SUB_NO AS lEGACY_SUB_NO_BASE, INIT_ACT_DATE from {MIGRATION_BV}.UF_SUB_BV_STG_1 where PRODUCT_NAME IN ('Comcast Business Voice - Full Featured','Comcast Business Voice - Mobility','Comcast Business Voice - Basic')) UF
on a.account_name=UF.LEGACY_ACCOUNT_NAME --Performance tuning change for serial input trans (08-31-2021)
where pih.product_instance_status_code = 3
and sysdate between pih.effective_start_date and pih.effective_end_date
--and UF.PRODUCT_NAME IN ('Comcast Business Voice - Full Featured','Comcast Business Voice - Mobility','Comcast Business Voice - Basic') --Performance tuning change for serial input trans (08-31-2021)
and ph.product_id not in (8000390)--Discount
and ph.product_id in (select pig.product_id from {BASE}.product_in_product_group pig
where pig.product_group_id = 8000125
union
select pc.compatible_product_id from {BASE}.product_compatibility pc
where pc.product_id in (select pig.product_id "Base_product" from {BASE}.product_in_product_group pig
where pig.product_group_id = 8000125)))
WHERE PRODUCT_DISPLAY_NAME='Equipment Fee - Voice') BV
inner Join
(SELECT *  FROM 
(SELECT distinct 
'0070' AS RECORD_TYPE,
'SV' SOURCE_SYSTEM,
SH.SERVICE_NAME as LEGACY_SUB_NO,
ACCT.ACCOUNT_NAME AS ACCOUNT_NUMBER,
SUB.LEGACY_ACCOUNT_NAME,
PH.PRODUCT_NAME,
nvl(to_char(Sh.ACTIVE_DATE,'YYYYMMDDHHMISS'),'') AS SERVICE_EFF_DATE,
'' as service_end_date,
PIH.GENERAL_8 AS MANUAL_OVERRIDE_AMOUNT,
'ACTIVE' as service_status,
nvl(to_char(Sh.ACTIVE_DATE,'YYYYMMDDHHMISS'),'') AS STATUS_DATE,
CSG.UPD_CSG_AAN AS AAN,
ah.line_1,
ah.line_2,
ah.suburb "CITY",
ah.state "COUNTY",
ah.country,
ad.STD_LINE1 as SV_STD_ADD,
ROOT_ACCOUNT_NAME
from
{BASE}.CUSTOMER_NODE_HISTORY CH  
JOIN {BASE}.PRODUCT_INSTANCE_HISTORY PIH  
     ON CH.CUSTOMER_NODE_ID=PIH.CUSTOMER_NODE_ID
JOIN {BASE}.ACCOUNT acct 
     on CH.prime_account_id=acct.account_id
JOIN {BASE}.SERVICE_HISTORY SH  
     ON SH.BASE_PRODUCT_INSTANCE_ID = PIH.BASE_PRODUCT_INSTANCE_ID
JOIN {BASE}.PRODUCT_HISTORY PH   
     ON PH.PRODUCT_ID=PIH.PRODUCT_ID
LEFT JOIN {APP}.APP_SV_CSG_ASSOC_DTLS CSG 
	 ON CSG.SERVICE_NAME = SH.SERVICE_NAME
join {BASE}.ADDRESS_HISTORY ah
     on ah.address_id = SH.A_ADDRESS_ID
     and sysdate between AH.EFFECTIVE_START_DATE and AH.EFFECTIVE_END_DATE
left join {APP}.APP_SV_SERV_ADD_ELOC_SUCESS ad
     on ah.line_1=ad.line_1
	  and  ad.MODIFY_DTS IS NULL
JOIN {MIGRATION_BI}.UF_SUB_BI_TEMP_SRV_ID SUB        ---------------------------- 12/09/2020 :  Added this to replace DBjoin step to improve performance - N
	ON SUB.LEGACY_ACCOUNT_NAME = ACCT.ACCOUNT_NAME	  
JOIN {APP}.Cust_hierarchy Cust
ON cust.ACCOUNT_NAME = ACCT.ACCOUNT_NAME    
WHERE
sysdate BETWEEN CH.EFFECTIVE_START_DATE AND CH.EFFECTIVE_END_DATE
AND sysdate BETWEEN PIH.EFFECTIVE_START_DATE AND PIH.EFFECTIVE_END_DATE
AND sysdate BETWEEN PH.EFFECTIVE_START_DATE AND PH.EFFECTIVE_END_DATE
AND SH.SERVICE_STATUS_CODE='3'
AND PIH.PRODUCT_INSTANCE_STATUS_CODE = '3'
AND PH.COMPANION_IND_CODE = '1'
AND SYSDATE BETWEEN SH.EFFECTIVE_START_DATE AND SH.EFFECTIVE_END_DATE
AND exists (select 1 
              from {BASE}.PRODUCT_INSTANCE_HISTORY
             where pih.base_product_instance_id = product_instance_id
               and sysdate between effective_start_date and effective_end_date
               AND pih.PRODUCT_ID IN ('8000219'))) )BI
              on BV.ACCOUNT_NAME = BI.ACCOUNT_NUMBER and BV.LEGACY_ACCOUNT_NO = BI.LEGACY_ACCOUNT_NAME and BV.ROOT_ACCOUNT_NAME = BI.ROOT_ACCOUNT_NAME and BV.MANUAL_OVERRIDE_AMOUNT = BI.MANUAL_OVERRIDE_AMOUNT"""









#Multiple AAN tagged to same BVE service <Shiva: Addded this new feature on 01-05-2022> ---using below query instead of this one - Bhoomi - 05-31-2022
Query_Multiple_AAN_tagged_same_BVE_service_OLD = fr"""
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT
FROM {MIGRATION_ME}.UF_SERVICES SER, {MIGRATION_ME}.UF_SUB_ASSOCIATION ASSO
WHERE SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') AND SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID
AND ASSO.INDEP_SUB_ID IN (SELECT SERVICE_NAME FROM {APP}.APP_BVE_OVER_LAY GROUP BY SERVICE_NAME HAVING COUNT(DISTINCT AAN_CLEANSED)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT
FROM {MIGRATION_BI}.UF_SERVICES SER, {MIGRATION_BI}.UF_SUB_ASSOCIATION ASSO
WHERE SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') AND SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID
AND ASSO.INDEP_SUB_ID IN (SELECT SERVICE_NAME FROM {APP}.APP_BVE_OVER_LAY GROUP BY SERVICE_NAME HAVING COUNT(DISTINCT AAN_CLEANSED)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT
FROM {MIGRATION_AV}.UF_SERVICES SER, {MIGRATION_AV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') AND SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID
AND ASSO.INDEP_SUB_ID IN (SELECT SERVICE_NAME FROM {APP}.APP_BVE_OVER_LAY GROUP BY SERVICE_NAME HAVING COUNT(DISTINCT AAN_CLEANSED)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT
FROM {MIGRATION_BV}.UF_SERVICES SER, {MIGRATION_BV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') AND SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID
AND ASSO.INDEP_SUB_ID IN (SELECT SERVICE_NAME FROM {APP}.APP_BVE_OVER_LAY GROUP BY SERVICE_NAME HAVING COUNT(DISTINCT AAN_CLEANSED)>1)
"""


#Multiple AAN tagged to same BVE service <Bhoomi: Addded new app table on 05-31-2022>
Query_Multiple_AAN_tagged_same_BVE_service_OLD = fr"""
select * from (SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT
FROM {MIGRATION_ME}.UF_SERVICES SER, {MIGRATION_ME}.UF_SUB_ASSOCIATION ASSO
WHERE SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') AND SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID
AND ASSO.INDEP_SUB_ID IN (SELECT service_name FROM {APP}.app_sv_csg_assoc_dtls GROUP BY SERVICE_NAME HAVING COUNT(DISTINCT csg_aan)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT
FROM {MIGRATION_BI}.UF_SERVICES SER, {MIGRATION_BI}.UF_SUB_ASSOCIATION ASSO
WHERE SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') AND SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID
AND ASSO.INDEP_SUB_ID IN (SELECT service_name FROM {APP}.app_sv_csg_assoc_dtls GROUP BY SERVICE_NAME HAVING COUNT(DISTINCT csg_aan)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT
FROM {MIGRATION_AV}.UF_SERVICES SER, {MIGRATION_AV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') AND SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID
AND ASSO.INDEP_SUB_ID IN (SELECT SERVICE_NAME FROM {APP}.app_sv_csg_assoc_dtls GROUP BY SERVICE_NAME HAVING COUNT(DISTINCT csg_aan)>1)
UNION
SELECT DISTINCT SER.LEGACY_CUST_ID,SER.LEGACY_ACCOUNT_NO,SER.LEGACY_ENTITY_ID,SER.SERVICE_CODE,SER.MANUAL_OVERRIDE_AMOUNT
FROM {MIGRATION_BV}.UF_SERVICES SER, {MIGRATION_BV}.UF_SUB_ASSOCIATION ASSO
WHERE SER.SERVICE_CODE IN ('BVE Equipment Fee','BVE Equipment Fee Vintage') AND SER.LEGACY_ENTITY_ID = ASSO.DEP_SUB_ID
AND ASSO.INDEP_SUB_ID IN (SELECT SERVICE_NAME FROM {APP}.app_sv_csg_assoc_dtls GROUP BY SERVICE_NAME HAVING COUNT(DISTINCT csg_aan)>1))
"""


#Multiple AAN tagged to same BVE service <Bhoomi: using only app table on 06-17-2022>
Query_Multiple_AAN_tagged_same_BVE_service = fr"""select * from
(select distinct SV_ROOT_BAN, count(distinct csg_aan) over(partition by service_name) as aan_count from {APP}.app_sv_csg_assoc_dtls where PRODUCT_NAME in ('Business Trunk Voice','Business SIP Trunk Voice','Business VoiceEdge'))a
where a.aan_count>1"""





#SingleView has EQP MRC for Underlay Switch <Shiva: Addded this new feature on 01-05-2022>- Bhoomi - 05-04-2022 - we are no longer using this sql instead of this we are using new SQL
Query_SV_EQP_MRC_Underlay_Switch_OLD = fr"""
 )
SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 ( 
 /*EDI UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_ME}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EDI UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%ENS UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*ENS UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_ME}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%ENS UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*EPL UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_ME}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EPL UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%ENS UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*EVPL UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_ME}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%ENS UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 )
 
 UNION
 
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 ( 
 /*EDI UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_BI}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EDI UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%ENS UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*ENS UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_BI}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%ENS UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*EPL UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_BI}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EPL UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%ENS UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*EVPL UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_BI}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%ENS UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 )
  
 UNION
 
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 ( 
 /*EDI UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_AV}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EDI UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%ENS UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*ENS UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_AV}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%ENS UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*EPL UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_AV}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EPL UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%ENS UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*EVPL UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_AV}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%ENS UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 )
  
 UNION
 
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 ( 
 /*EDI UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_BV}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EDI UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%ENS UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*ENS UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_BV}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%ENS UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*EPL UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_BV}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EPL UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%ENS UNI%')  AND AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
 UNION 
 /*EVPL UNI*/
 SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, AGG_PRD FROM 
 (SELECT LEGACY_CUST_ID, LEGACY_SITE_ID, LISTAGG(LEGACY_PRODUCT_OFFER, '|') WITHIN GROUP (ORDER BY LEGACY_CUST_ID, LEGACY_SITE_ID, LEGACY_PRODUCT_OFFER) AS AGG_PRD 
 FROM {MIGRATION_BV}.UF_SUBSCRIBER --WHERE LEGACY_CUST_ID='934545244' 
 GROUP BY LEGACY_CUST_ID, LEGACY_SITE_ID) AGG_PRD_TBL 
 WHERE (AGG_PRD NOT LIKE ('%EVPL UNI%') AND AGG_PRD LIKE ('%Underlay%') ) 
 AND AGG_PRD NOT LIKE ('%EDI UNI%')  AND AGG_PRD NOT LIKE ('%EPL UNI%')  AND AGG_PRD NOT LIKE ('%ENS UNI%') AND AGG_PRD NOT LIKE ('%BI Underlay%')
 AND LEGACY_CUST_ID IN (SELECT DISTINCT ACCOUNT_NAME FROM STAGE.X_TEMP_NON_FLGD_ACCT_NOS)
"""

#SingleView has EQP MRC for Underlay Switch <Bhoomi: Addded this new feature on 05-04-2022>
Query_SV_EQP_MRC_Underlay_Switch = fr"""
SELECT
    *
FROM
    (
        SELECT
            s.legacy_cust_id,
            s.legacy_sub_no,
            LISTAGG(s1.legacy_product_offer, '|') WITHIN GROUP(
                ORDER BY
                    s1.legacy_product_offer
            ) OVER(
                PARTITION BY s.legacy_cust_id, s.legacy_sub_no
            ) AS eqp_uni
        FROM
            {MIGRATION_ME}.uf_subscriber s, --I used mig10 but while building in Recon framework pls use individual schemas and UNION
            {MIGRATION_ME}.uf_sub_association sa,
            {MIGRATION_ME}.uf_subscriber s1
        WHERE
            s.legacy_cust_id = sa.legacy_cust_id
            AND s.legacy_sub_no = sa.dep_sub_id
            AND s.legacy_product_offer = 'SWITCH'
            AND sa.legacy_cust_id = s1.legacy_cust_id
            AND sa.indep_sub_id = s1.legacy_sub_no
            AND s1.legacy_product_offer LIKE '%UNI%'
    )
WHERE
    eqp_uni = 'EDI Underlay UNI'
UNION
SELECT
    *
FROM
    (
        SELECT
            s.legacy_cust_id,
            s.legacy_sub_no,
            LISTAGG(s1.legacy_product_offer, '|') WITHIN GROUP(
                ORDER BY
                    s1.legacy_product_offer
            ) OVER(
                PARTITION BY s.legacy_cust_id, s.legacy_sub_no
            ) AS eqp_uni
        FROM
            {MIGRATION_BI}.uf_subscriber s, --I used mig10 but while building in Recon framework pls use individual schemas and UNION
            {MIGRATION_BI}.uf_sub_association sa,
            {MIGRATION_BI}.uf_subscriber s1
        WHERE
            s.legacy_cust_id = sa.legacy_cust_id
            AND s.legacy_sub_no = sa.dep_sub_id
            AND s.legacy_product_offer = 'SWITCH'
            AND sa.legacy_cust_id = s1.legacy_cust_id
            AND sa.indep_sub_id = s1.legacy_sub_no
            AND s1.legacy_product_offer LIKE '%UNI%'
    )
WHERE
    eqp_uni = 'EDI Underlay UNI'
UNION
SELECT
    *
FROM
    (
        SELECT
            s.legacy_cust_id,
            s.legacy_sub_no,
            LISTAGG(s1.legacy_product_offer, '|') WITHIN GROUP(
                ORDER BY
                    s1.legacy_product_offer
            ) OVER(
                PARTITION BY s.legacy_cust_id, s.legacy_sub_no
            ) AS eqp_uni
        FROM
            {MIGRATION_AV}.uf_subscriber s, --I used mig10 but while building in Recon framework pls use individual schemas and UNION
            {MIGRATION_AV}.uf_sub_association sa,
            {MIGRATION_AV}.uf_subscriber s1
        WHERE
            s.legacy_cust_id = sa.legacy_cust_id
            AND s.legacy_sub_no = sa.dep_sub_id
            AND s.legacy_product_offer = 'SWITCH'
            AND sa.legacy_cust_id = s1.legacy_cust_id
            AND sa.indep_sub_id = s1.legacy_sub_no
            AND s1.legacy_product_offer LIKE '%UNI%'
    )
WHERE
    eqp_uni = 'EDI Underlay UNI'
UNION
SELECT
    *
FROM
    (
        SELECT
            s.legacy_cust_id,
            s.legacy_sub_no,
            LISTAGG(s1.legacy_product_offer, '|') WITHIN GROUP(
                ORDER BY
                    s1.legacy_product_offer
            ) OVER(
                PARTITION BY s.legacy_cust_id, s.legacy_sub_no
            ) AS eqp_uni
        FROM
            {MIGRATION_BV}.uf_subscriber s, --I used mig10 but while building in Recon framework pls use individual schemas and UNION
            {MIGRATION_BV}.uf_sub_association sa,
            {MIGRATION_BV}.uf_subscriber s1
        WHERE
            s.legacy_cust_id = sa.legacy_cust_id
            AND s.legacy_sub_no = sa.dep_sub_id
            AND s.legacy_product_offer = 'SWITCH'
            AND sa.legacy_cust_id = s1.legacy_cust_id
            AND sa.indep_sub_id = s1.legacy_sub_no
            AND s1.legacy_product_offer LIKE '%UNI%'
    )
WHERE
    eqp_uni = 'EDI Underlay UNI'"""


#CSG has only BI service but eqp tagged as "Equipment Fee - Voice- Bhoomi added this feature to populate "Incorrect EQP fee tagging in CSG" - 05-11-2022
Query_Incorrect_EQP_Fee_Tagging_in_CSG = fr"""select distinct cp_dnorm.SUB_ACCT_NO_SBB,app_csg.sv_root_ban
                                 from {BASE}.CSG_SUB_CUST_LOC_PROD_DNRM cp_dnorm
                                 inner join {REF}.REF_CSG_SERV_CDE_MAPPING SERV on (cp_dnorm.sys_sbb||'_'||cp_dnorm.prin_sbb||'_'||cp_dnorm.SERV_CDE_OCI)=serv.CSG_SYS_PRIN_CODE
            join {APP}.app_csg_billed_aan_input app_csg on app_csg.csg_aan = cp_dnorm.SUB_ACCT_NO_SBB
    where cp_dnorm.EQP_STAT_EQP = 'H' 
    and (upper(legacy_product_offer) like '%CONNECT%' or upper(legacy_product_offer) like '%INTERNET%')
    and not exists (
                  select distinct cp_dnorm1.SUB_ACCT_NO_SBB,app_csg.sv_root_ban
                                 from {BASE}.CSG_SUB_CUST_LOC_PROD_DNRM cp_dnorm1
                                 inner join {REF}.REF_CSG_SERV_CDE_MAPPING SERV1 on (cp_dnorm1.sys_sbb||'_'||cp_dnorm1.prin_sbb||'_'||cp_dnorm1.SERV_CDE_OCI)=serv1.CSG_SYS_PRIN_CODE
            join {APP}.app_csg_billed_aan_input app_csg1 on app_csg1.csg_aan = cp_dnorm1.SUB_ACCT_NO_SBB
    and cp_dnorm1.EQP_STAT_EQP = 'H' 
    and (upper(legacy_product_offer) like '%BUSINESS VOICE%')
    and cp_dnorm.SUB_ACCT_NO_SBB = cp_dnorm1.SUB_ACCT_NO_SBB
    )
    and exists (
                  select distinct cp_dnorm2.SUB_ACCT_NO_SBB,app_csg.sv_root_ban
                                 from {BASE}.CSG_SUB_CUST_LOC_PROD_DNRM cp_dnorm2
                                 inner join {REF}.REF_CSG_SERV_CDE_MAPPING SERV2 on (cp_dnorm2.sys_sbb||'_'||cp_dnorm2.prin_sbb||'_'||cp_dnorm2.SERV_CDE_OCI)=serv2.CSG_SYS_PRIN_CODE
            join {APP}.app_csg_billed_aan_input app_csg2 on app_csg2.csg_aan = cp_dnorm2.SUB_ACCT_NO_SBB
    and cp_dnorm2.EQP_STAT_EQP = 'H' 
    and (upper(legacy_product_offer) like '%EQUIPMENT FEE - VOICE%')
    and cp_dnorm.SUB_ACCT_NO_SBB = cp_dnorm2.SUB_ACCT_NO_SBB)"""


#Only BI Service but having BV Voicemail MRC - Bhoomi added this feature to populate "CSG MRC Cleanup" RCA flag - 05-13-2022
Query_CSG_MRC_Cleanup = fr"""select distinct cp_dnorm.SUB_ACCT_NO_SBB,app_csg.sv_root_ban
                             from {BASE}.CSG_SUB_CUST_LOC_PROD_DNRM cp_dnorm
                             inner join {REF}.REF_CSG_SERV_CDE_MAPPING SERV on (cp_dnorm.sys_sbb||'_'||cp_dnorm.prin_sbb||'_'||cp_dnorm.SERV_CDE_OCI)=serv.CSG_SYS_PRIN_CODE
        join {APP}.app_csg_billed_aan_input app_csg on app_csg.csg_aan = cp_dnorm.SUB_ACCT_NO_SBB
where cp_dnorm.EQP_STAT_EQP = 'H' 
and (upper(legacy_product_offer) like '%CONNECT%' or upper(legacy_product_offer) like '%INTERNET%')
and not exists (
              select distinct cp_dnorm1.SUB_ACCT_NO_SBB,app_csg.sv_root_ban
                             from {BASE}.CSG_SUB_CUST_LOC_PROD_DNRM cp_dnorm1
                             inner join {REF}.REF_CSG_SERV_CDE_MAPPING SERV1 on (cp_dnorm1.sys_sbb||'_'||cp_dnorm1.prin_sbb||'_'||cp_dnorm1.SERV_CDE_OCI)=serv1.CSG_SYS_PRIN_CODE
        join {APP}.app_csg_billed_aan_input app_csg1 on app_csg1.csg_aan = cp_dnorm1.SUB_ACCT_NO_SBB
and cp_dnorm1.EQP_STAT_EQP = 'H' 
and (upper(legacy_product_offer) like '%BUSINESS VOICE%')
and cp_dnorm.SUB_ACCT_NO_SBB = cp_dnorm1.SUB_ACCT_NO_SBB
)
and exists (
              select distinct cp_dnorm2.SUB_ACCT_NO_SBB,app_csg.sv_root_ban
                             from {BASE}.CSG_SUB_CUST_LOC_PROD_DNRM cp_dnorm2
                             inner join {REF}.REF_CSG_SERV_CDE_MAPPING SERV2 on (cp_dnorm2.sys_sbb||'_'||cp_dnorm2.prin_sbb||'_'||cp_dnorm2.SERV_CDE_OCI)=serv2.CSG_SYS_PRIN_CODE
        join {APP}.app_csg_billed_aan_input app_csg2 on app_csg2.csg_aan = cp_dnorm2.SUB_ACCT_NO_SBB
and cp_dnorm2.EQP_STAT_EQP = 'H' 
and (upper(legacy_product_offer) like '%BV VOICEMAIL%')
and cp_dnorm.SUB_ACCT_NO_SBB = cp_dnorm2.SUB_ACCT_NO_SBB
)"""


#Added Multiple BI on same site - Bhoomi :- added this new feature on 06-03-2022
Query_Multiple_BI_On_Same_Site = fr"""select * from 
(select legacy_cust_id,legacy_site_id,legacy_product_offer,legacy_sub_no,count(distinct legacy_sub_no) over(partition by legacy_cust_id,legacy_site_id) as BI_count from
(select legacy_cust_id,legacy_site_id,legacy_sub_no,legacy_product_offer from {Consolidated_Mig_Sch}.uf_subscriber where legacy_product_offer in
(SELECT input_val3 FROM {Consolidated_Mig_Sch}.CONV_TT WHERE TRANS_GROUP='TRANS_EPC_OFFER' AND OUTPUT_VAL6='Business_Internet' and input_val3 not like '%Underlay%')))a
where a.BI_count>1
"""

#Added Missing Wifi Pro Access Point Equipment - Bhoomi :- Added this new feature on 06-03-2022
Query_Missing_Wifi_Pro_Access_Point_Eqp = fr"""SELECT * FROM {Consolidated_Mig_Sch}.UF_SUBSCRIBER S WHERE 
S.LEGACY_PRODUCT_OFFER IN ('Wifi Pro') AND NOT EXISTS (SELECT 1 FROM {Consolidated_Mig_Sch}.UF_SUB_ASSOCIATION SA,{Consolidated_Mig_Sch}.uf_subscriber s1 WHERE 
S.LEGACY_CUST_ID=SA.LEGACY_CUST_ID AND
S.LEGACY_SUB_NO=SA.INDEP_SUB_ID AND
S1.LEGACY_CUST_ID=SA.LEGACY_CUST_ID AND
S1.LEGACY_SUB_NO=SA.DEP_SUB_ID AND
s1.legacy_product_offer = 'Wifi Pro Access Point'
)"""




#Added filter for LOB type prodcuts <Shiva: Addded this new feature on 01-25-2022>
Query_ME_SERVICES = fr"""
--ME
SELECT DISTINCT 'METRO_E_SERVICES' AS SERVICE_TYPE, SERVICE_CODE 
--FROM {Consolidated_Mig_Sch}.UF_SERVICES WHERE LEGACY_CUST_ID IN
FROM {MIGRATION_ME}.UF_SERVICES WHERE LEGACY_CUST_ID IN
(
SELECT DISTINCT ROOT_ACCOUNT_NAME FROM {BASE}.LIST_OF_ACCOUNTS WHERE LOB = 'ME'
)
"""
Query_BI_SERVICES = fr"""
--BCI
SELECT DISTINCT 'Business_Internet_SERVICES' AS SERVICE_TYPE, SERVICE_CODE  
--FROM {Consolidated_Mig_Sch}.UF_SERVICES WHERE LEGACY_CUST_ID IN
FROM {MIGRATION_BI}.UF_SERVICES WHERE LEGACY_CUST_ID IN
(
SELECT DISTINCT ROOT_ACCOUNT_NAME FROM {BASE}.LIST_OF_ACCOUNTS WHERE LOB = 'BCI'
)
"""
Query_BCV_SERVICES = fr"""
--BCV
SELECT DISTINCT 'Business_Voice_SERVICES' AS SERVICE_TYPE, SERVICE_CODE   
--FROM {Consolidated_Mig_Sch}.UF_SERVICES WHERE LEGACY_CUST_ID IN
FROM {MIGRATION_BV}.UF_SERVICES WHERE LEGACY_CUST_ID IN
(
SELECT DISTINCT ROOT_ACCOUNT_NAME FROM {BASE}.LIST_OF_ACCOUNTS WHERE LOB = 'BCV'
)
"""
Query_BVE_SERVICES = fr"""
--BVE
SELECT DISTINCT 'AV_BusinessVoiceEdge_SERVICES' AS SERVICE_TYPE, SERVICE_CODE  
--FROM {Consolidated_Mig_Sch}.UF_SERVICES WHERE LEGACY_CUST_ID IN
FROM {MIGRATION_AV}.UF_SERVICES WHERE LEGACY_CUST_ID IN
(
SELECT DISTINCT ROOT_ACCOUNT_NAME FROM {BASE}.LIST_OF_ACCOUNTS WHERE LOB = 'BVE'
)
"""
Query_PRI_SERVICES = fr"""
--PRI
SELECT DISTINCT 'AV_PRI_SERVICES' AS SERVICE_TYPE, SERVICE_CODE   
--FROM {Consolidated_Mig_Sch}.UF_SERVICES WHERE LEGACY_CUST_ID IN
FROM {MIGRATION_AV}.UF_SERVICES WHERE LEGACY_CUST_ID IN
(
SELECT DISTINCT ROOT_ACCOUNT_NAME FROM {BASE}.LIST_OF_ACCOUNTS WHERE LOB = 'PRI'
)
"""
Query_SIP_SERVICES = fr"""
--SIP
SELECT DISTINCT 'AV_SIP_SERVICES' AS SERVICE_TYPE, SERVICE_CODE   
--FROM {Consolidated_Mig_Sch}.UF_SERVICES WHERE LEGACY_CUST_ID IN
FROM {MIGRATION_AV}.UF_SERVICES WHERE LEGACY_CUST_ID IN
(
SELECT DISTINCT ROOT_ACCOUNT_NAME FROM {BASE}.LIST_OF_ACCOUNTS WHERE LOB = 'SIP'
)
"""

Query_ACTIVECORE_SERVICES = fr"""
--SIP
SELECT DISTINCT DISTINCT 'ACTIVECORE_SERVICES' AS SERVICE_TYPE,SERVICE_CODE 
FROM {Consolidated_Mig_Sch}.UF_SERVICES WHERE SERVICE_CODE IN 
(
SELECT DISTINCT SERVICE_CODE FROM {Consolidated_Mig_Sch}.UF_SERVICES WHERE LEGACY_CUST_ID IN
	(SELECT DISTINCT ROOT_ACCOUNT_NAME FROM BASE_2.LIST_OF_ACCOUNTS WHERE LOB ='ACTIVECORE')
)
"""

#Shiva 01-28-2022: Get the duplicated MRC's for duplicated EQPs from CLIPS
#For the same activecore service, we have multiple EQPs from CLIPS
Query_Same_ActiveCore_Mult_EQPs = fr"""
SELECT LEGACY_CUST_ID,LEGACY_ACCOUNT_NO,LEGACY_SUB_NO,COUNT(DEP_SUB_ID) COUNT_AC_SUB_NO FROM
(
	SELECT
	SUB.LEGACY_CUST_ID,
	SUB.LEGACY_ACCOUNT_NO,
	SUB.LEGACY_PRODUCT_OFFER,
	SUB.LEGACY_SUB_NO,
	SUBA.INDEP_SUB_ID,
	SUBA.DEP_SUB_ID
	FROM {Consolidated_Mig_Sch}.UF_SUBSCRIBER SUB
	JOIN {Consolidated_Mig_Sch}.UF_SUB_ASSOCIATION SUBA 
		ON SUB.LEGACY_SUB_NO = SUBA.INDEP_SUB_ID AND SUB.LEGACY_CUST_ID = SUBA.LEGACY_CUST_ID
	JOIN {Consolidated_Mig_Sch}.UF_SUBSCRIBER SUB1 
		ON SUBA.DEP_SUB_ID = SUB1.LEGACY_SUB_NO
	WHERE SUB.LEGACY_CUST_ID IN (
	SELECT DISTINCT ROOT_ACCOUNT_NAME
	FROM {BASE}.LIST_OF_ACCOUNTS WHERE LOB LIKE '%ACTIVECORE%'
								)
	AND SUB.LEGACY_PRODUCT_OFFER IN (
	'Hardware_Managed_Router',
	'SD WAN Service',
	'Software_Managed_Router'
									)
	AND SUB1.LEGACY_PRODUCT_OFFER IN 	(
	'uCPE',
	'Hardware Router'
										)
)
GROUP BY LEGACY_CUST_ID,LEGACY_ACCOUNT_NO,LEGACY_SUB_NO
HAVING COUNT(DEP_SUB_ID)>1
"""





#Bhoomi 12-02-2022: Missing Base products for Activecore Products.
Query_Activecore_Missing_Base_Product = fr"""SELECT distinct ACCOUNT_NAME, legacy_sub_no FROM
    (SELECT DISTINCT 
    'CM030-P' as SQL_SOURCE
    /*Shiva:01-20-2022 Removing the Root Account, to include the child/invoicing accounts*/
    , ch.ROOT_ACCOUNT_NAME as ROOT_ACCOUNT_NAME
    , ch.invoice_account_name as ACCOUNT_NAME
    ,case when ph.product_display_name LIKE '%Security%' then sh.service_name||'_UTM'
    ELSE sh.service_name
    END AS legacy_sub_no
    ,ph.product_display_name AS PRODUCT_NAME
    ,'N/A' as PRODUCT_GROUP
    ,pih.general_8 as MRC ,
    pih.product_id
    from {BASE}.account a
    join {BASE}.product_instance_history pih
         on pih.customer_node_id = a.customer_node_id
         and pih.product_instance_status_code = 3
         and sysdate between pih.effective_start_date and pih.effective_end_date
    join {BASE}.service_history sh
         on sh.base_product_instance_id = nvl(pih.base_product_instance_id,pih.product_instance_id)
         and sh.service_status_code = 3
         and sysdate between sh.effective_start_date and sh.effective_end_date
    join {BASE}.product_history ph
         on ph.product_id = pih.product_id
    join {BASE}.product_instance_history pihbase
         on pihbase.product_instance_id = pih.base_product_instance_id
         and pihbase.product_instance_status_code = 3
         and sysdate between pihbase.effective_start_date and pihbase.effective_end_date
    --join RECON.all_account_list ch
    --  on ch.account_name = a.ACCOUNT_NAME
    --join APP_2.LIST_OF_ACCOUNTS ch
    --  on ch.ROOT_ACCOUNT_NAME = a.ACCOUNT_NAME
    /*Shiva:01-20-2022 Adding the below to include the child accounts*/
    JOIN {APP}.CUST_HIERARCHY CH
    ON CH.ACCOUNT_NAME = A.ACCOUNT_NAME
    where a.account_type_id = 10000
    /*Shiva:01-24-2022 Changed the below condition -->*/
    and (pih.product_id in ('8401540','8401676','8402052','8402084','8402085','8402276','8401534','8402048') 
    or pih.product_id in (SELECT pc.compatible_product_id FROM {BASE}.product_compatibility pc WHERE pc.product_id IN ('8401540','8401676','8402052','8402084','8402085','8402276','8401534','8402048'))
    )
    and ph.product_display_name not in ('Ethernet Service Discount')
    -->and pihbase.product_id in ('8401504') 
    /*Shiva:09-08-2021 Commenting it out to get only the list of correct products*/
    /*and pih.product_id in (8401532,8401540,8401521,8401524,8401525,8401743,8401744)*/
    -->and pih.product_id in (SELECT pc.compatible_product_id FROM {BASE}.product_compatibility pc
    -->WHERE pc.product_id IN (8401504))
    )
    Minus
select distinct LEGACY_CUST_ID, LEGACY_SUB_NO  from {Consolidated_Mig_Sch}.uf_subscriber  where (LEGACY_PRODUCT_OFFER like '%SD%WAN%' or
LEGACY_PRODUCT_OFFER like '%Managed%Router%' or LEGACY_PRODUCT_OFFER like '%Security%')"""

# Query_CM_AM_ADD_FILTER = fr"""
# SELECT 	DISTINCT SER.LEGACY_CUST_ID, SER.LEGACY_ENTITY_ID, SER.SERVICE_CODE,
		# SUB.LEGACY_PRODUCT_OFFER
# FROM 		MIGRATION_BKP.UF_SERVICES{UF_release} 	SER
# INNER JOIN 	MIGRATION_BKP.UF_SUBSCRIBER{UF_release} SUB 
# ON 			SUB.LEGACY_CUST_ID 		= SER.LEGACY_CUST_ID
# AND 		SUB.LEGACY_ACCOUNT_NO 	= SER.LEGACY_ACCOUNT_NO
# AND 		SUB.LEGACY_SUB_NO 		= SER.LEGACY_ENTITY_ID
# INNER JOIN STAGE.{CM030_UNION_SQL}  RCA 
# ON          SER.LEGACY_ACCOUNT_NO   = RCA.ACCOUNT_NAME
# AND         SER.SERVICE_CODE        = RCA.PRODUCT_NAME 
# WHERE	SUB.LEGACY_PRODUCT_OFFER IN (
        # 'EDI UNI',
        # 'ENS UNI',
        # 'EPL UNI',
        # 'EVC Endpoint',
        # 'EVC_EDI',
        # 'EVC_ENS',
        # 'EVC_EPL',
        # 'EVC_EVPL',
        # 'EVPL UNI',
        # 'SWITCH'
	# )
# """
