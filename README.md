Select /*+ PARALLEL(16) */ * From (
WITH Total_base AS (SELECT EDR_DATE , sum(QUOTA_USAGE) Usage , 'TOTAL' AS TYPE
FROM PCRF_EDRS
WHERE EDR_DATE >= TO_DATE('20261013','YYYYMMDD')
GROUP BY EDR_DATE
),

PRE AS (
SELECT EDR_DATE,
       SUM(QUOTA_USAGE) AS Usage,
       'PRE' AS TYPE
FROM PCRF_EDRS
WHERE EDR_DATE >= TO_DATE('20261013','YYYYMMDD')
  AND SERVICE_NAME IN ( 'Serv_DSL_Generic_Promo','Serv_DSL_Seasonal_Gift','Serv_DSL_Double_Quota_Pre','Serv_DSL_Compensation_Gift')
GROUP BY EDR_DATE),

POST as
(
SELECT EDR_DATE , sum(QUOTA_USAGE) Usage, 'POST' AS TYPE
FROM PCRF_EDRS
WHERE EDR_DATE >= TO_DATE('20261013','YYYYMMDD')
AND (QUOTA_NAME in('DSL_Compensation_Gift')or SERVICE_NAME IN ('Serv_DSL_DoubleQuota_300GB_POST','Serv_DSL_DoubleQuota_600GB_POST','Serv_DSL_DoubleQuota_200GB_POST',
'Serv_DSL_GC_Gift_3000G') )
GROUP BY EDR_DATE
)

SELECT /*+ PARALLEL(16) */
t.EDR_DATE Time, Round(t.USAGE/1024/1024/1024,2) Total_TB,
Round(pre.USAGE/1024/1024/1024,2) PRE_FREE_TRAFFIC_TB, (100- ROund((t.USAGE - pre.usage)/t.USAGE,4)*100) PRE_Percentage,
Round(POST.USAGE/1024/1024/1024,2) POST_FREE_TRAFFIC_TB,(100- ROund((t.USAGE - POST.usage)/t.USAGE,4)*100) POST_Percentage
FROM Total_base t JOIN PRE pre ON t.EDR_DATE = PRE.EDR_DATE
JOIN POST post ON t.EDR_DATE = post.EDR_DATE);


i want to convert this to duck dp query and selecr from the table "pcrf_edrs_src"    find the script i below i adjusted it untill hte query part 

"import os
import sys

PROJECT_ROOT = os.path.abspath(
os.path.join(os.path.dirname(__file__), "..")
)
sys.path.insert(0, PROJECT_ROOT)

import duckdb
import argparse
from datetime import datetime
from config.duckdb_connection import duckdb_connection
from utils.logger import get_logger
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr
from utils.dump_vs_database import validate_loaded_data
from flows.pcrf_edrs import load_pcrf_edrs



def export_free_traffic(run_date: str, con=None):

    logger = get_logger(
        flow="free_traffic_mismatch",
        log_dir=None,  
        run_date=run_date
    )


    try:
        if con is None:
            con = duckdb_connection()
    

        logger.info(f"###########################################################")
        logger.info(f"###### FREE TRAFFIC MISMATCH for run_date= {run_date} ######")
        logger.info(f"###########################################################")

        start_time = datetime.now()
        pcrf_edrs_dump = f"/RAIDdata/PI/PI_PRJ/exports/pcrf_edrs_{run_date}.csv"

        con.execute(f"""
            CREATE OR REPLACE TABLE pcrf_edrs_src AS
            SELECT EDR_DATE, QUOTA_USAGE, SERVICE_NAME,QUOTA_NAME
            FROM read_csv_auto('{pcrf_edrs_dump}', ignore_errors=True)
        """)


        con.execute(f""" 
        """ )

        count = con.execute("SELECT COUNT(*) FROM bscs_hlr_type_mismatch").fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        logger.info(f"Exporting bscs_hlr_type_mismatch_{run_date}.csv ..")

        con.execute(f"""COPY bscs_hlr_type_mismatch TO '/project/PLATFORM_INTEGRITY/exports/bscs_hlr_type_mismatch_{run_date}.csv' (HEADER, DELIMITER ',')""")

        logger.info(f"Inserting into PI.BSCS_HLR_TYPE_MISMATCH table ..")
        ctl_file = prepare_ctl("bscs_hlr_type_mismatch", run_date)
        run_sqlldr("bscs_hlr_type_mismatch",run_date, ctl_file=ctl_file)
        logger.info(f"Insertion done")

        logger.info(f"Validating inserted data ..")
        validate_loaded_data(
        flow_name="bscs_hlr_type_mismatch",
        run_date=run_date,
        expected_rows=count,
        logger=logger)
        logger.info("Inserted data validated successfully.")

        end_time = datetime.now()
        logger.info(f"bscs_hlr_type_mismatch job finished")
        logger.info(f"bscs_hlr_type_mismatch total duration: {end_time - start_time}")
    except Exception as e:
        logger.exception(f"bscs_hlr_type_mismatch failed for run_date= {run_date} | error= {str(e)}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()
    export_bscs_hlr_type(run_date=args.run_date)

"    also find beow a right script below just ignore logic take the script structure and functions 

'"import os 
import sys 
 
PROJECT_ROOT = os.path.abspath( 
    os.path.join(os.path.dirname(__file__), "..") 
) 
sys.path.insert(0, PROJECT_ROOT) 
 
import duckdb 
import argparse 
from datetime import datetime 
from config.duckdb_connection import duckdb_connection 
from utils.logger import get_logger 
from utils.export_to_oracle import prepare_ctl 
from utils.export_to_oracle import run_sqlldr 
from utils.dump_vs_database import validate_loaded_data 
 

 
def export_volte_mismatch(run_date: str, con=None): 
     
    logger = get_logger( 
        flow="volte_mismatch", 
        log_dir=None,   
        run_date=run_date 
    ) 
 

    try: 
        if con is None: 
            con = duckdb_connection() 
     
 
        logger.info(f"###################################################") 
        logger.info(f"###### VOLTE MISMATCH for run_date= {run_date} ######") 
        logger.info(f"###################################################") 
 

        start_time = datetime.now() 
        enum_dump = f"/RAIDdata/PI/PI_PRJ/exports/enum_{run_date}.csv" 
        hss_dump = f"/RAIDdata/PI/PI_PRJ/exports/hss_{run_date}.csv" 
        billing_dump = f"/RAIDdata/PI/PI_PRJ/exports/bscs_{run_date}.csv" 
        hlr_dump = f"/RAIDdata/PI/PI_PRJ/exports/hlr_{run_date}.csv" 
 

        con.execute(f""" 
            CREATE OR REPLACE TABLE hlr_src AS 
            SELECT RUN_DATE, MSISDN, IMSI, STATUS, refOickName , refTickHomeName, refTickRoamName
            FROM read_csv_auto('{hlr_dump}', ignore_errors=True) 
        """) 
 

        con.execute(f""" 
            CREATE OR REPLACE TABLE bscs_src AS 
            SELECT RUN_DATE, MSISDN, IMSI, STATUS, TYPE, TM_CODE, TM_CODE_DESC, ACTIVE_SERVICES, SERVICE_CODE, VOLTE
            FROM read_csv_auto('{billing_dump}', ignore_errors=True) 
        """) 
 

        con.execute(f""" 
            CREATE OR REPLACE TABLE enum_src AS 
            SELECT RUN_DATE, MSISDN
            FROM read_csv_auto('{enum_dump}', ignore_errors=True) 
        """) 
 

        con.execute(f""" 
            CREATE OR REPLACE TABLE hss_src AS 
            SELECT RUN_DATE, MSISDN, IMSI, HLR_STATUS AS HLR_HSS_DUMP_STATUS, VOLTE_4G_STATUS, IRS, RF, RO, APNS, VOLTE_EXIST, EXIST_4G
            FROM read_csv_auto('{hss_dump}', ignore_errors=True) 
        """) 

        con.execute(f""" 
            CREATE OR REPLACE TABLE base AS 
            SELECT RUN_DATE, MSISDN, IMSI FROM hlr_src
            UNION
            SELECT RUN_DATE, MSISDN, IMSI FROM bscs_src
            UNION
            SELECT RUN_DATE, MSISDN, NULL AS IMSI FROM enum_src
            UNION
            SELECT RUN_DATE, MSISDN, IMSI FROM hss_src
        """) 


        con.execute(f""" 
        CREATE OR REPLACE TABLE volte_mismatch AS 
        SELECT base.RUN_DATE,
        base.MSISDN,
        CASE WHEN hss_src.MSISDN IS NULL OR hss_src.VOLTE_EXIST != 1 OR hss_src.EXIST_4G != 1 THEN 'NOT EXIST' 
        ELSE 'EXIST'
        END HSS_EXISTENCE,
        hss_src.VOLTE_4G_STATUS, 
        bscs_src.VOLTE AS BILLING_VOLTE, 
        hlr_src.refTickHomeName AS TICK_HOME, 
        hlr_src.refTickRoamName AS TICK_ROAM,
        CASE WHEN hss_src.IRS IS NULL OR hss_src.APNS NOT IN ('hos|ims', 'ims|hos') OR hss_src.APNS IS NULL OR hss_src.RO IS NULL OR hss_src.RF IS NULL
        THEN 'Missing HSS Flag' 
        ELSE 'HSS Proper Flags'
        END AS HSS,
        CASE WHEN enum_src.MSISDN IS NULL
        THEN 'Missing ENUM Flag' 
        ELSE 'Exist'
        END AS ENUM,
        CASE
        WHEN bscs_src.VOLTE != 1 THEN 'FALSE'
        WHEN HSS_EXISTENCE = 'NOT EXIST' THEN 'Billing' 
        ELSE 'Network'
        END AS DEPROV,
        CASE
        WHEN bscs_src.VOLTE != 1
        THEN 'Billing'
        WHEN HSS_EXISTENCE = 'NOT EXIST'
        THEN 'FALSE' ELSE 'Network'
        END AS PROV
        FROM base
        LEFT JOIN hss_src
        ON base.IMSI = hss_src.IMSI
        LEFT JOIN hlr_src
        ON base.IMSI = hlr_src.IMSI
        LEFT JOIN bscs_src
        ON base.IMSI = bscs_src.IMSI
        LEFT JOIN enum_src
        ON base.MSISDN = enum_src.MSISDN
        WHERE (hlr_src.refTickHomeName <> 100 OR refTickRoamName <> 200 OR hss_src.IRS IS NULL OR enum_src.MSISDN IS NULL OR bscs_src.VOLTE <> 1 OR hss_src.APNS NOT IN ('hos|ims', 'ims|hos')
        OR hss_src.IMSI IS NULL OR hlr_src.IMSI IS NULL OR bscs_src.IMSI IS NULL OR enum_src.MSISDN IS NULL)
        AND (CAST(base.IMSI AS VARCHAR) NOT LIKE '602028%' AND CAST(base.IMSI AS VARCHAR) NOT IN ('602023381446246', '602023381446593'))
        AND base.MSISDN NOT IN(
        '201060053729',
        '201020039520',
        '201010965871',
        '201090138221',
        '201010135668',
        '201060973003',
        '201015199431',
        '201044832177',
        '201097002072',
        '201097005173',
        '201090051218',
        '201090064239',
        '201030016135',
        '201020039520',
        '201020010937',
        '201025832659',
        '201010965871',
        '201097533297',
        '201097524082',
        '201044839277',
        '201097534889',
        '201044832177',
        '201090138221',
        '201013377958',
        '201097598895',
        '201097530492',
        '201097590498',
        '201097505694',
        '201097554897',
        '201097589698',
        '201097562083',
        '201097531691',
        '201097190883',
        '201097031095',
        '201097554897',
        '201070217837',
        '201097015512',
        '201094074650',
        '201097530492',
        '201097598895')
        """ ) 
 
        count = con.execute("SELECT COUNT(*) FROM volte_mismatch").fetchone()[0] 
        logger.info(f"Total processed rows: {count}") 
        logger.info(f"Exporting volte_mismatch_{run_date}.csv ..") 
        con.execute(f"""COPY volte_mismatch TO '/project/PLATFORM_INTEGRITY/exports/VOLTE_MISMATCH_{run_date}.csv.gz' (HEADER, COMPRESSION gzip, DELIMITER ',')""") 
        end_time = datetime.now() 
        logger.info(f"volte_mismatch job finished") 
        logger.info(f"volte_mismatch total duration: {end_time - start_time}") 
    except Exception as e: 
        logger.exception(f"volte_mismatch failed for run_date= {run_date} | error= {str(e)}") 
        raise 
 

 
if __name__ == "__main__": 
    parser = argparse.ArgumentParser() 
    parser.add_argument("--run_date", required=True) 
    args = parser.parse_args() 
    export_volte_mismatch(run_date=args.run_date)"  ths volte is corect 
