import os
import sys

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.insert(0, PROJECT_ROOT)

import duckdb
import argparse
import glob
from datetime import datetime
from config.duckdb_connection import duckdb_connection
from utils.logger import get_logger
from utils.send_mail import send_mail
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr
from utils.dump_vs_database import validate_loaded_data


# Positional mapping (read_csv with header=False -> column00 .. column19)
#    rdrCONTRACT_ID          (index 1)
#    rdrIMSI                 (index 2)
#    rdrRUNNING_NUMBER       (index 3)
#    rdrTM_CODE              (index 7)
#    rdrSUB_STATUS           (index 8)
#    rdrSUB_TYPE             (index 9)
#    rdrMSISDN               (index 10)
#    rdrSN_CODE              (index 11)
#    rdrBILL_CYCLE           (index 14)
#    TMCODE_DES              (index 16)
#    rdrACTIVE_SERVICE_LIST  (index 19)
#    rdrPRICE_GROUP          (index 20)


def load_bscs_profiles_adsl(run_date: str, con=None):

    start_time = datetime.now()
    logger = get_logger(
        flow="bscs_profiles_adsl",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        run_date_formatted = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:]}"
        csv_path = f"/project/PLATFORM_INTEGRITY/exports/bscs_profiles_adsl_{run_date}.csv"
        
        file_path = (
            f"/RAIDdata/PI/BSCS/raw/"
            f"CONTRACT_PROFILE_{run_date}.csv.gz"
        )

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Input file not found: {file_path}")

        logger.info(f"Reading {file_path} ..")

        con.execute(f"""
        CREATE OR REPLACE TABLE bscs_profiles_adsl_data AS
        WITH src AS (
        SELECT
            TRIM(column00) AS CONTRACT_ID,
            RTRIM(TRIM(column09), '`') AS MSISDN,
            TRIM(column07) AS SUB_STATUS,
            TRIM(column06) AS TM_CODE,
            TRIM(column08) AS SUB_TYPE,
            TRIM(column10) AS SN_CODE,
            TRIM(column13) AS BILL_CYCLE,
            TRIM(column15) AS TMCODE_DES,
            TRIM(column18) AS ACTIVE_SERVICE_LIST,
            TRIM(column19) AS PRICE_GROUP
        FROM read_csv_auto(
            '{file_path}',
            delim='|',
            header=FALSE,
            all_varchar=TRUE,
            quote='',
            escape='',
            null_padding=TRUE,
            ignore_errors=TRUE
        )
        ),

        -- one value per TM_CODE so the join can never duplicate rows
        lkp AS (
            SELECT
                TRIM(TRY_CAST(TMCODE AS VARCHAR)) AS TMCODE,
                ANY_VALUE(TRIM(TRY_CAST(PCRF_SERVICE_NAME AS VARCHAR))) AS PCRF_SERVICE_NAME
            FROM lkp17
            GROUP BY 1
        )

        SELECT
            TRY_CAST('{run_date_formatted}' AS DATE) AS RUN_DATE,

            src.CONTRACT_ID,
            src.MSISDN,
            src.TM_CODE,

            CASE
                WHEN UPPER(src.SUB_STATUS) = 'A' THEN 'ACTIVE'
                WHEN UPPER(src.SUB_STATUS) = 'S' THEN 'SUSPENDED'
                ELSE 'DEACTIVE'
            END AS STATUS,

            CASE
                WHEN src.SUB_TYPE = 'PREPAID'    THEN 'PRE'
                WHEN src.SUB_TYPE = 'POSTPAID'   THEN 'POST'
                WHEN src.SUB_TYPE = 'C2BCONTROL' THEN 'CTRL'
                WHEN src.SUB_TYPE = 'CONTROL'    THEN 'CTRL'
                ELSE src.SUB_TYPE
            END AS TYPE,

            src.BILL_CYCLE,
            src.TMCODE_DES AS TM_CODE_DESC,
            src.PRICE_GROUP,

            CASE
                WHEN l.TMCODE IS NOT NULL THEN l.PCRF_SERVICE_NAME
                ELSE '_N'
            END AS PCRF_SERVICE_NAME_EXPECTED,

            CASE
                WHEN (',' || src.ACTIVE_SERVICE_LIST || ',') LIKE '%,505,%'
                OR (',' || src.ACTIVE_SERVICE_LIST || ',') LIKE '%,507,%'
                THEN 1
                ELSE 0
            END AS AUTO_ACTIVATION_FLAG

        FROM src
        LEFT JOIN lkp l
            ON src.TM_CODE = l.TMCODE

        WHERE src.SN_CODE = '504' AND UPPER(src.SUB_STATUS) IN ('A','S');
        """)

        count = con.execute(
            "SELECT COUNT(*) FROM bscs_profiles_adsl_data"
        ).fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        con.execute(f"""
        COPY (
            SELECT RUN_DATE, CONTRACT_ID, MSISDN, TM_CODE, STATUS, TYPE,
                   BILL_CYCLE, TM_CODE_DESC, PRICE_GROUP,
                   PCRF_SERVICE_NAME_EXPECTED, AUTO_ACTIVATION_FLAG
            FROM bscs_profiles_adsl_data
        )
        TO '{csv_path}'
        (DELIMITER ',', HEADER TRUE, QUOTE '"', ESCAPE '"');
        """)

        print("Copied successfully..")

        if count == 0:
            logger.info("No bscs_profiles_adsl data processed!")
            raise ValueError("No bscs_profiles_adsl data processed (0 rows)")

        
        # logger.info("Inserting into PI.BSCS_PROFILES_ADSL table ..")
        # ctl_file = prepare_ctl("bscs_profiles_adsl", run_date)
        # run_sqlldr("bscs_profiles_adsl", run_date, ctl_file=ctl_file)
        # logger.info("Insertion done")
        # logger.info("Validating inserted data ..")

        # validate_loaded_data(
        #     flow_name="bscs_profiles_adsl",
        #     run_date=run_date,
        #     expected_rows=count,
        #     logger=logger)

        # logger.info("Inserted data validated successfully.")

        # end_time = datetime.now()
        # logger.info("bscs_profiles_adsl finished")
        # logger.info(f"Total duration: {end_time - start_time}")
        # send_mail(
        #     subject="BSCS_PROFILES_ADSL Flow Finished",
        #     content=f"""
        #         Run Date : {run_date}
        #         Rows      : {count}
        #         DB Validation : Passed
        #         Duration  : {end_time - start_time}""")

        return con.table("bscs_profiles_adsl_data")

    except Exception as e:
        logger.exception(
            f"BSCS_PROFILES_ADSL failed for run_date= {run_date} | error= {str(e)}"
        )
        send_mail(
            subject="BSCS_PROFILES_ADSL Flow Failed",
            content=f"""
            Run Date : {run_date}
            Error:
            {str(e)}
            """)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()
    load_bscs_profiles_adsl(run_date=args.run_date)
