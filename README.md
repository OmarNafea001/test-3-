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
#   column00 -> rdrCONTRACT_ID          (index 1)
#   column01 -> rdrIMSI                 (index 2)
#   column02 -> rdrRUNNING_NUMBER       (index 3)
#   column06 -> rdrTM_CODE              (index 7)
#   column07 -> rdrSUB_STATUS           (index 8)
#   column08 -> rdrSUB_TYPE             (index 9)
#   column09 -> rdrMSISDN               (index 10)
#   column10 -> rdrSN_CODE              (index 11)
#   column13 -> rdrBILL_CYCLE           (index 14)
#   column15 -> TMCODE_DES              (index 16)
#   column18 -> rdrACTIVE_SERVICE_LIST  (index 19)
#   column19 -> rdrPRICE_GROUP          (index 20)


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

        csv_path = (
            f"/project/PLATFORM_INTEGRITY/exports/"
            f"bscs_profiles_adsl_{run_date}.csv"
        )
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
        COALESCE(TRIM(TRY_CAST(column00 AS VARCHAR)), '') AS CONTRACT_ID,
        RTRIM(COALESCE(TRIM(TRY_CAST(column09 AS VARCHAR)), ''), '`') AS MSISDN,
        COALESCE(TRIM(TRY_CAST(column07 AS VARCHAR)), '') AS SUB_STATUS,
        COALESCE(TRIM(TRY_CAST(column06 AS VARCHAR)), '') AS TM_CODE,
        COALESCE(TRIM(TRY_CAST(column08 AS VARCHAR)), '') AS SUB_TYPE,
        COALESCE(TRIM(TRY_CAST(column10 AS VARCHAR)), '') AS SN_CODE,
        COALESCE(TRIM(TRY_CAST(column13 AS VARCHAR)), '') AS BILL_CYCLE,
        COALESCE(TRIM(TRY_CAST(column15 AS VARCHAR)), '') AS TMCODE_DES,
        COALESCE(TRIM(TRY_CAST(column18 AS VARCHAR)), '') AS ACTIVE_SERVICE_LIST,
        COALESCE(TRIM(TRY_CAST(column19 AS VARCHAR)), '') AS PRICE_GROUP,

        -- RUN_DATE extracted from the file name (XML: RegExpPart .*_.*_([0-9]*).*)
        regexp_extract(
            regexp_extract(filename, '[^/]+$'),
            '.*_.*_([0-9]*).*',
            1
        ) AS EXTRACT_DATE

    FROM read_csv_auto(
        '{file_path}',
        delim='|',
        header=FALSE,
        all_varchar=TRUE,
        quote='',
        escape='',
        null_padding=TRUE,
        filename=TRUE,
        ignore_errors=TRUE
    )
),

-- one value per TM_CODE so the join can never duplicate rows
lkp AS (
    SELECT
        TRIM(TRY_CAST(TMCODE AS VARCHAR)) AS TMCODE,
        ANY_VALUE(COALESCE(TRIM(TRY_CAST(PCRF_SERVICE_NAME AS VARCHAR)), '')) AS PCRF_SERVICE_NAME
    FROM lkp17
    GROUP BY 1
)

SELECT
    TRY_CAST(STRPTIME(src.EXTRACT_DATE, '%Y%m%d') AS DATE) AS RUN_DATE,

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

WHERE src.SN_CODE = '504';
""")

        count = con.execute(
            "SELECT COUNT(*) FROM bscs_profiles_adsl_data"
        ).fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        if count == 0:
            logger.info("No bscs_profiles_adsl data processed!")
            raise ValueError("No bscs_profiles_adsl data processed (0 rows)")

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

        logger.info("Inserting into PI.BSCS_PROFILES_ADSL table ..")
        ctl_file = prepare_ctl("bscs_profiles_adsl", run_date)
        run_sqlldr("bscs_profiles_adsl", run_date, ctl_file=ctl_file)
        logger.info("Insertion done")
        logger.info("Validating inserted data ..")

        validate_loaded_data(
            flow_name="bscs_profiles_adsl",
            run_date=run_date,
            expected_rows=count,
            logger=logger)

        logger.info("Inserted data validated successfully.")

        end_time = datetime.now()
        logger.info("bscs_profiles_adsl finished")
        logger.info(f"Total duration: {end_time - start_time}")
        send_mail(
            subject="BSCS_PROFILES_ADSL Flow Finished",
            content=f"""
                Run Date : {run_date}
                Rows      : {count}
                DB Validation : Passed
                Duration  : {end_time - start_time}""")

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
