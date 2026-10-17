
Adsl subs · PY
import os
import sys
 
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.insert(0, PROJECT_ROOT)
 
import argparse
from datetime import datetime, timedelta
from config.duckdb_connection import duckdb_connection
from utils.logger import get_logger
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr
 
 
def export_adsl_subs(run_date: str, con=None):
 
    logger = get_logger(
        flow="adsl_subs",
        log_dir=None,
        run_date=run_date
    )
 
    try:
        if con is None:
            con = duckdb_connection()
 
        logger.info("###########################################################")
        logger.info(f"###### ADSL SUBS for run_date= {run_date} ######")
        logger.info("###########################################################")
 
        start_time = datetime.now()
 
        prev_date = (datetime.strptime(run_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
        logger.info(f"Counting subscribers for RUN_DATE= {prev_date}")
 
        bscs_dump = f"/RAIDdata/PI/PI_PRJ/exports/bscs_profiles_adsl_{run_date}*"
        charging_dump = f"/RAIDdata/PI/PI_PRJ/exports/charging_adsl_{run_date}*"
 
 

        con.execute(f"""
            CREATE OR REPLACE TABLE bscs_adsl_src AS
            SELECT RUN_DATE, PCRF_SERVICE_NAME_EXPECTED, MSISDN, TYPE, TM_CODE_DESC
            FROM read_csv_auto('{bscs_dump}', all_varchar=true, ignore_errors=True)
        """)
 
        con.execute(f"""
            CREATE OR REPLACE TABLE charging_adsl_src AS
            SELECT RUN_DATE, PCRF_SERVICE_NAME as PCRF_SERVICE_ID_EXPECTED , MSISDN, PROD_TYPE_NAME as PACKAGE_TYPE
            FROM read_csv_auto('{charging_dump}', all_varchar=true, ignore_errors=True)
        """)
 

        con.execute(f"""
            CREATE OR REPLACE TABLE adsl_subs AS
            SELECT RUN_DATE, SERVICE_NAME, "COUNT"
            FROM (
                TRY_CAST(RUN_DATE AS DATE) AS RUN_DATE,
                       PCRF_SERVICE_NAME_EXPECTED AS SERVICE_NAME,
                       COUNT(DISTINCT MSISDN) AS "COUNT"
                FROM bscs_adsl_src
                WHERE TRY_CAST(RUN_DATE AS DATE) = strptime('{prev_date}', '%Y%m%d')
                  AND "TYPE" = 'POST'
                  AND TMCODE_DES NOT LIKE 'SDSL%'
                  AND TMCODE_DES NOT LIKE 'Cloud%'
                GROUP BY 1, 2
            )
            UNION ALL
            SELECT RUN_DATE, SERVICE_NAME, "COUNT"
            FROM (
                SELECT TRY_CAST(RUN_DATE AS DATE) AS RUN_DATE,
                       PCRF_SERVICE_ID_EXPECTED AS SERVICE_NAME,
                       COUNT(DISTINCT MSISDN) AS "COUNT"
                FROM charging_adsl_src
                WHERE TRY_CAST(RUN_DATE AS DATE) = strptime('{prev_date}', '%Y%m%d')
                  AND UPPER(PACKAGE_TYPE) = 'MAIN BUNDLE'
                GROUP BY 1, 2
            )
        """)
 
        count = con.execute("SELECT COUNT(*) FROM adsl_subs").fetchone()[0]
        logger.info(f"Total processed rows: {count}")
 
        if count == 0:
            raise ValueError(f"adsl_subs produced 0 rows for RUN_DATE= {prev_date}; check source files and filters")
 
        logger.info(f"Exporting adsl_subs_{run_date}.csv ..")
        con.execute(f"""COPY adsl_subs TO '/project/PLATFORM_INTEGRITY/exports/adsl_subs_{run_date}.csv' (HEADER, DELIMITER ',')""")
 
        logger.info("Inserting into RP7VFEPIDAT.ADSL_SUBS table ..")
        ctl_file = prepare_ctl("adsl_subs", run_date)
        run_sqlldr("adsl_subs", run_date, ctl_file=ctl_file)
        logger.info("Insertion done")
 
        end_time = datetime.now()
        logger.info("adsl_subs job finished")
        logger.info(f"adsl_subs total duration: {end_time - start_time}")
    except Exception as e:
        logger.exception(f"adsl_subs failed for run_date= {run_date} | error= {str(e)}")
        raise
 
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()
    export_adsl_subs(run_date=args.run_date)
 

