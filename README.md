import os 
import sys 
 
PROJECT_ROOT = os.path.abspath( 
os.path.join(os.path.dirname(__file__), "..") 
) 
sys.path.insert(0, PROJECT_ROOT) 
 
import duckdb 
import argparse 
from datetime import datetime, timedelta
from config.duckdb_connection import duckdb_connection 
from utils.logger import get_logger 
from utils.export_to_oracle import prepare_ctl 
from utils.export_to_oracle import run_sqlldr 
from utils.dump_vs_database import validate_loaded_data 
 
 
 
def export_procera_vs_edrs(run_date: str, con=None): 
 
    logger = get_logger( 
        flow="adsl_procera_vs_edrs", 
        log_dir=None,   
        run_date=run_date 
    ) 
 
 
    try: 
        if con is None: 
            con = duckdb_connection() 
     
 
        logger.info(f"################################################################") 
        logger.info(f"###### ADSL PROCERA VS EDRS for run_date= {run_date} ##############") 
        logger.info(f"################################################################") 
 
        start_time = datetime.now() 
        edrs_consum_dump = f"/RAIDdata/PI/PI_PRJ/exports/pcrf_edrs_*"
        procera_dump = f"/RAIDdata/PI/PI_PRJ/exports/procera_agg_*" 
 
        con.execute(f""" 
            CREATE OR REPLACE VIEW pcrf_edrs AS 
            SELECT RUN_DATE,EDR_DATE,TIME,MSISDN,TRIGGER_TYPE,SERVICE_NAME,QUOTA_NAME,QUOTA_STATUS,EXPECTED_ORG_QUOTA,EXPECTED_TAX_QUOTA,QUOTA_CONSUMPTION,QUOTA_USAGE,QUOTA_VALUE,ACTUAL_QUOTA_USAGE,SERVICE_EXTENDED_ATT1,SOURCE_FILENAME
            FROM read_csv_auto('{edrs_consum_dump}', ignore_errors=True, union_by_name=True) 
        """) 

        count = con.execute("SELECT COUNT(*) FROM pcrf_edrs").fetchone()[0] 
        logger.info(f"Total pcrf processed rows: {count}") 
 
        con.execute(f""" 
            CREATE OR REPLACE VIEW procera AS 
            SELECT RUN_DATE,START_DATE,LANDLINE,TOTAL_BYTES,FRAUD_FLAG,PCRF_RESULT_CODE,RULE_MONITORING_KEY,SESSION_ID,CLOSE_CAUSE,PCRF_RULES_LIST            
            FROM read_csv_auto('{procera_dump}', ignore_errors=True) 
        """) 

        count = con.execute("SELECT COUNT(*) FROM procera").fetchone()[0] 
        logger.info(f"Total procera processed rows: {count}") 
 
 
        con.execute(f""" 
       CREATE OR REPLACE TABLE procera_vs_edrs AS

        SELECT
            RUN_DATE,
            EDRS_LANDLINE,
            PROCERA_LANDLINE,
            ROUND(EDRS_CONSUMPTION, 3) AS EDRS_CONSUMPTION,
            ROUND(PROCERA_CONSUMPTION, 3) AS PROCERA_CONSUMPTION,
            ROUND(VARIANCE, 3) AS VARIANCE,
            ROUND(VARIANCE / NULLIF(PROCERA_CONSUMPTION, 0), 2) AS PERCENTAGE
        FROM
        (
            SELECT
                COALESCE(PROCERA.START_DATE, EDRS.EDR_DATE) AS RUN_DATE,
                EDRS.MSISDN AS EDRS_LANDLINE,
                PROCERA.LANDLINE AS PROCERA_LANDLINE,
                EDRS.QUOTA_CONSUMPTION AS EDRS_CONSUMPTION,
                (PROCERA.TOTAL_BYTES / 1024.0) AS PROCERA_CONSUMPTION,
                (
                    COALESCE((PROCERA.TOTAL_BYTES / 1024.0), 0)
                    - COALESCE(EDRS.QUOTA_CONSUMPTION, 0)
                ) AS VARIANCE
            FROM
            (
                SELECT
                    EDR_DATE,
                    MSISDN,
                    SUM(QUOTA_USAGE) AS QUOTA_CONSUMPTION
                FROM pcrf_edrs
                WHERE EDR_DATE = STRPTIME('{run_date}', '%Y%m%d')
                GROUP BY
                    EDR_DATE,
                    MSISDN
            ) EDRS

            FULL OUTER JOIN
            (
                SELECT
                    START_DATE,
                    LANDLINE,
                    SUM(TOTAL_BYTES) AS TOTAL_BYTES
                FROM procera
                WHERE START_DATE = STRPTIME('{run_date}', '%Y%m%d')
                GROUP BY
                    START_DATE,
                    LANDLINE
            ) PROCERA

            ON CAST(EDRS.MSISDN AS VARCHAR) = CAST(PROCERA.LANDLINE AS VARCHAR)

            WHERE
                (
                    COALESCE((PROCERA.TOTAL_BYTES / 1024.0), 0)
                    - COALESCE(EDRS.QUOTA_CONSUMPTION, 0)
                ) <> 0
        );
         """ ) 
 
     
        count = con.execute("SELECT COUNT(*) FROM procera_vs_edrs").fetchone()[0] 
        logger.info(f"Total processed rows: {count}") 
 
        logger.info(f"Exporting procera_vs_edrs_{run_date}.csv ..") 
 
        con.execute(f"""COPY procera_vs_edrs TO '/project/PLATFORM_INTEGRITY/exports/procera_vs_edrs_{run_date}.csv' (HEADER, DELIMITER ',')""") 
        # logger.info(f"Inserting into PI.BSCS_IN_HLR_STATUS_MISMATCH table ..") 
        # ctl_file = prepare_ctl("bscs_in_hlr_status_mismatch", run_date) 
        # run_sqlldr("bscs_in_hlr_status_mismatch",run_date, ctl_file=ctl_file) 
        # logger.info(f"Insertion done") 
 
        # logger.info(f"Validating inserted data ..") 
        # validate_loaded_data( 
        # flow_name="bscs_in_hlr_status_mismatch", 
        # run_date=run_date, 
        # expected_rows=count, 
        # logger=logger) 
        # logger.info("Inserted data validated successfully.") 
 
        end_time = datetime.now() 
        logger.info(f"procera_vs_edrs_consum job finished") 
        logger.info(f"procera_vs_edrs_consum total duration: {end_time - start_time}") 
    except Exception as e: 
        logger.exception(f"procera_vs_edrs_consum failed for run_date= {run_date} | error= {str(e)}") 
        raise 
 
 
if __name__ == "__main__": 
    parser = argparse.ArgumentParser() 
    parser.add_argument("--run_date", required=True) 
    args = parser.parse_args() 
    export_procera_vs_edrs(run_date=args.run_date) 



import os
import sys
import argparse
from datetime import datetime

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.insert(0, PROJECT_ROOT)

from config.duckdb_connection import duckdb_connection
from utils.logger import get_logger
from utils.send_mail import send_mail
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr
from utils.dump_vs_database import validate_loaded_data


def load_solarwinds(run_date: str, con=None):

    start_time = datetime.now()
    logger = get_logger(
        flow="solarwinds",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        solarwinds_date = f"{run_date[:4]}_{run_date[4:6]}_{run_date[6:]}"
        csv_path = f"/project/PLATFORM_INTEGRITY/exports/solarwinds_{run_date}.csv"
        file_path = f"/RAIDdata/PI/SOLARWINDS/out/{solarwinds_date}_*mod*"

        con.execute(f"""
            CREATE OR REPLACE TABLE solarwinds_data AS
            SELECT
                CAST("Timestamp" AS VARCHAR) AS RUN_DATE,

                TRIM("BNG Name") AS BNG_NAME,

                '_N' AS LAG,

                regexp_extract(filename, '[^/]+$') AS FILE_NAME,

                TRY_CAST(
                    regexp_extract(TRIM("Total Bytes Received"), '^([^.]*)')
                    AS BIGINT
                ) AS RECEIVED_VOLUME,

                TRY_CAST(
                    regexp_extract(TRIM("Total Bytes Transmitted"), '^([^.]*)')
                    AS BIGINT
                ) AS TRANSMITTED_VOLUME,

                TRY_CAST(
                    regexp_extract(TRIM("Total Bytes (Transmitted + Received)"), '^([^.]*)')
                    AS BIGINT
                ) AS TOTAL_VOLUME

            FROM read_csv_auto(
                '{file_path}',
                delim=',',
                header=TRUE,
                filename=TRUE,
                ignore_errors=TRUE,
                dateformat='%m/%d/%Y'
            );
        """)

        count = con.execute("SELECT COUNT(*) FROM solarwinds_data").fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        if count == 0:
            logger.info("No solarwinds data processed!")
            raise ValueError(f"No solarwinds data processed for run_date={run_date}")

        con.execute(f"""
            COPY (
                SELECT RUN_DATE, BNG_NAME, LAG, FILE_NAME,
                       RECEIVED_VOLUME, TRANSMITTED_VOLUME, TOTAL_VOLUME
                FROM solarwinds_data
            )
            TO '{csv_path}'
            (DELIMITER ',', HEADER TRUE);
        """)

#        logger.info("Inserting into PI.SOLARWINDS table ..")
#        ctl_file = prepare_ctl("solarwinds", run_date)
#        run_sqlldr("solarwinds", run_date, ctl_file=ctl_file)
#        logger.info("Insertion done")
#
#        logger.info("Validating inserted data ..")
#        validate_loaded_data(
#            flow_name="solarwinds",
#            run_date=run_date,
#            expected_rows=count,
#            logger=logger
#        )
#        logger.info("Inserted data validated successfully.")
#
#        end_time = datetime.now()
#        logger.info("solarwinds finished")
#        logger.info(f"Total duration: {end_time - start_time}")
#
#        send_mail(
#            subject="SOLARWINDS Flow Finished",
#            content=f"""
#Run Date      : {run_date}
#Rows          : {count}
#DB Validation : Passed
#Duration      : {end_time - start_time}
#"""
#        )
#
#        return con.table("solarwinds_data")

    except Exception as e:
#       logger.exception(f"SOLARWINDS failed for run_date= {run_date} | error= {str(e)}")
#        send_mail(
#            subject="SOLARWINDS Flow Failed",
#            content=f"""
#Run Date : {run_date}
#Error:
#{str(e)}
#"""
#        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()
    load_solarwinds(run_date=args.run_date)



    I WANT TO BUILD LOOKUP JUST LIKE THE ABOVE QUERIES 
    AND THE VALIDATION AS WELL 
    JUST ONE THING TO ADD TRIGGER THE LOOKUP IN THE EXPORTS     project/flow  first then read the files from exports as thers ok do u need anything fro me now?



