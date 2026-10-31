import os
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
from utils.send_mail import send_mail
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr
from utils.dump_vs_database import validate_loaded_data


def load_bscs_profiles_adsl(run_date: str, con=None):

    logger = get_logger(
        flow="bscs_profiles_adsl",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        start_time = datetime.now()
        run_date_formatted = (
            f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:]}"
        )
        export_path = (
            f"/project/PLATFORM_INTEGRITY/exports/"
            f"bscs_profiles_adsl_{run_date}.csv"
        )
        file_path = (
            f"/RAIDdata/PI/BSCS/raw/"
            f"CONTRACT_PROFILE_{run_date}.csv.gz"
        )

        con.execute(f"""
        CREATE OR REPLACE TABLE bscs_profiles_adsl AS
        SELECT 
        TRY_CAST('{run_date_formatted}' AS DATE) AS RUN_DATE,
        TRY_CAST(src.column00 AS VARCHAR) AS CONTRACT_ID,
        '20' || TRY_CAST(src.column09 AS VARCHAR) AS MSISDN,
        TRY_CAST(src.column06 AS BIGINT) AS TM_CODE,
        CASE
            WHEN TRIM(UPPER(TRY_CAST(src.column07 AS VARCHAR))) = 'A'
                THEN 'ACTIVE'
            WHEN TRIM(UPPER(TRY_CAST(src.column07 AS VARCHAR))) = 'S'
                THEN 'SUSPENDED'
            ELSE 'DEACTIVE'
        END AS STATUS,

        CASE
            WHEN TRY_CAST(src.column08 AS VARCHAR) = 'PREPAID'
                THEN 'PRE'

            WHEN TRY_CAST(src.column08 AS VARCHAR) = 'POSTPAID'
                THEN 'POST'

            WHEN TRY_CAST(src.column08 AS VARCHAR)
                IN ('C2BCONTROL', 'CONTROL')
                THEN 'CTRL'

            ELSE TRY_CAST(src.column08 AS VARCHAR)
        END AS TYPE,

        CASE
            WHEN TRY_CAST(src.column13 AS VARCHAR) = '03'
                THEN 'PRE'
            ELSE 'POST'
        END AS BILL_CYCLE,

        TRY_CAST(src.column15 AS VARCHAR) AS TM_CODE_DESC,
        TRY_CAST(src.column19 AS VARCHAR) AS PRICE_GROUP,
        COALESCE(l.PCRF_SERVICE_NAME,'_N') AS PCRF_SERVICE_NAME_EXPECTED,
    
        CASE
            WHEN regexp_matches(
                concat(
                    ',',
                    trim(CAST(src.column18 AS VARCHAR)),
                    ','
                ),
                '.*,505,.*'
            )
            OR regexp_matches(
                concat(
                    ',',
                    trim(CAST(src.column18 AS VARCHAR)),
                    ','
                ),
                '.*,507,.*'
            )
            THEN 1
            ELSE 0
        END AS AUTO_ACTIVATION_FLAG

        FROM read_csv_auto('{file_path}',ignore_errors=True,delim='|',header=False) src

        LEFT JOIN lkp17 l
            ON TRY_CAST(src.column06 AS VARCHAR) = TRY_CAST(l.TMCODE AS VARCHAR)

        WHERE TRY_CAST(src.column10 AS VARCHAR) = '504'
        """)

        count = con.execute(
            "SELECT COUNT(*) FROM bscs_profiles_adsl"
        ).fetchone()[0]

        logger.info(f"Total processed rows: {count}")

        if count == 0:
            logger.info(
                "No bscs_profiles_adsl data processed!"
            )
            raise RuntimeError(
                "No bscs_profiles_adsl data processed: "
                "the resulting table contains zero rows."
            )

         con.execute(f"""
         COPY (
             SELECT *
             FROM bscs_profiles_adsl
         )
         TO '{export_path}'
         (DELIMITER ',', HEADER TRUE, QUOTE '"', ESCAPE '"');
         """)

         logger.info(
             "Inserting into PI.BSCS_PROFILES_ADSL table .."
         )
         ctl_file = prepare_ctl(
             "bscs_profiles_adsl",
             run_date
         )
         run_sqlldr(
             "bscs_profiles_adsl",
             run_date,
             ctl_file=ctl_file
         )
         logger.info("Insertion done")

         logger.info("Validating inserted data ..")
         validate_loaded_data(
             flow_name="bscs_profiles_adsl",
             run_date=run_date,
             expected_rows=count,
             logger=logger
         )
         logger.info(
             "Inserted data validated successfully."
         )

        end_time = datetime.now()
        logger.info("bscs_adsl job finished")
        logger.info(
            f"bscs_adsl total duration: "
            f"{end_time - start_time}"
        )

         send_mail(
             subject="BSCS_ADSL Flow Finished",
             content=f"""
             Run Date : {run_date}
             Rows      : {count}
             DB Validation : Passed
             Duration  : {end_time - start_time}"""
         )

        return con.table("bscs_profiles_adsl")

    except Exception as e:
        logger.exception(
            f"BSCS_ADSL failed for run_date= {run_date} | "
            f"error= {str(e)}"
        )

         send_mail(
            subject="BSCS_ADSL Flow Failed",
             content=f"""
                 Run Date : {run_date}
                 Error:
                 {str(e)}
             """
        )

        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()

    load_bscs_profiles_adsl(
        run_date=args.run_date
    )
