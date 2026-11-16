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

        logger.info("Inserting into PI.SOLARWINDS table ..")
        ctl_file = prepare_ctl("solarwinds", run_date)
        run_sqlldr("solarwinds", run_date, ctl_file=ctl_file)
        logger.info("Insertion done")

        logger.info("Validating inserted data ..")
        validate_loaded_data(
            flow_name="solarwinds",
            run_date=run_date,
            expected_rows=count,
            logger=logger
        )
        logger.info("Inserted data validated successfully.")

        end_time = datetime.now()
        logger.info("solarwinds finished")
        logger.info(f"Total duration: {end_time - start_time}")

        send_mail(
            subject="SOLARWINDS Flow Finished",
            content=f"""
Run Date      : {run_date}
Rows          : {count}
DB Validation : Passed
Duration      : {end_time - start_time}
"""
        )

        return con.table("solarwinds_data")

    except Exception as e:
        logger.exception(f"SOLARWINDS failed for run_date= {run_date} | error= {str(e)}")
        send_mail(
            subject="SOLARWINDS Flow Failed",
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
    load_solarwinds(run_date=args.run_date)









    --------------

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


def load_solarwinds_new(run_date: str, con=None):

    start_time = datetime.now()
    logger = get_logger(
        flow="solarwinds_new",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        solarwinds_date = f"{run_date[:4]}_{run_date[4:6]}_{run_date[6:]}"
        csv_path = f"/project/PLATFORM_INTEGRITY/exports/solarwinds_new_{run_date}.csv"
        file_path = f"/RAIDdata/PI/SOLARWINDS/out_2/*{solarwinds_date}*updated*"

        con.execute(f"""
            CREATE OR REPLACE TABLE solarwinds_new_data AS
            SELECT
                CAST("Timestamp" AS VARCHAR) AS RUN_DATE,

                TRIM("BNG Name") AS BNG_NAME,
                TRIM("Interface Name") AS INTERFACE_NAME,
                TRIM("Interface Caption") AS INTERFACE_CAPTION,

                regexp_extract(filename, '[^/]+$') AS FILE_NAME,

                TRY_CAST(
                    REGEXP_REPLACE(TRIM("Total Bytes Received"), '\\.[0-9]+[[:space:]]*B$', '')
                    AS BIGINT
                ) AS RECEIVED_VOLUME,

                TRY_CAST(
                    REGEXP_REPLACE(TRIM("Total Bytes Transmitted"), '\\.[0-9]+[[:space:]]*B$', '')
                    AS BIGINT
                ) AS TRANSMITTED_VOLUME,

                TRY_CAST(
                    REGEXP_REPLACE(TRIM("Total Bytes (Transmitted + Received)"), '\\.[0-9]+[[:space:]]*B$', '')
                    AS BIGINT
                ) AS TOTAL_VOLUME,

                CASE
                    WHEN UPPER(INTERFACE_CAPTION) LIKE '%HSUBBT%' THEN 'HSUBBT'
                    ELSE 'UBBT'
                END AS SPEED

            FROM read_csv_auto(
                '{file_path}',
                delim=',',
                header=TRUE,
                filename=TRUE,
                ignore_errors=TRUE,
                dateformat='%m/%d/%Y'
            );
        """)

        count = con.execute("SELECT COUNT(*) FROM solarwinds_new_data").fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        if count == 0:
            logger.info("No solarwinds_new data processed!")
            raise ValueError(f"No solarwinds_new data processed for run_date={run_date}")

        con.execute(f"""
            COPY (
                SELECT RUN_DATE, BNG_NAME, INTERFACE_NAME, INTERFACE_CAPTION, FILE_NAME,
                       RECEIVED_VOLUME, TRANSMITTED_VOLUME, TOTAL_VOLUME, SPEED
                FROM solarwinds_new_data
            )
            TO '{csv_path}'
            (DELIMITER ',', HEADER TRUE);
        """)

        logger.info("Inserting into PI.SOLARWINDS_NEW table ..")
        ctl_file = prepare_ctl("solarwinds_new", run_date)
        run_sqlldr("solarwinds_new", run_date, ctl_file=ctl_file)
        logger.info("Insertion done")

        logger.info("Validating inserted data ..")
        validate_loaded_data(
            flow_name="solarwinds_new",
            run_date=run_date,
            expected_rows=count,
            logger=logger
        )
        logger.info("Inserted data validated successfully.")

        end_time = datetime.now()
        logger.info("solarwinds_new finished")
        logger.info(f"Total duration: {end_time - start_time}")

        send_mail(
            subject="SOLARWINDS_NEW Flow Finished",
            content=f"""
Run Date      : {run_date}
Rows          : {count}
DB Validation : Passed
Duration      : {end_time - start_time}
"""
        )

        return con.table("solarwinds_new_data")

    except Exception as e:
        logger.exception(f"SOLARWINDS_NEW failed for run_date= {run_date} | error= {str(e)}")
        send_mail(
            subject="SOLARWINDS_NEW Flow Failed",
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
    load_solarwinds_new(run_date=args.run_date)

