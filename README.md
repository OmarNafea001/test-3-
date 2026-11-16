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
            

        run_date_formatted = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:]}"
        solarwinds_date = f"{run_date[:4]}_{run_date[4:6]}_{run_date[6:]}"
        csv_path = f"/project/PLATFORM_INTEGRITY/exports/solarwinds_{run_date}.csv"
        file_path = (f"/RAIDdata/PI/SOLARWINDS/out/{solarwinds_date}_*mod*")

        con.execute(f"""
        CREATE OR REPLACE TABLE solarwinds_data AS
        SELECT

            CASE
                WHEN TRY_CAST(CAST("Timestamp" AS VARCHAR) AS DATE) IS NOT NULL
                THEN CAST(TRY_CAST(CAST("Timestamp" AS VARCHAR) AS DATE) AS VARCHAR)
                ELSE ''
            END AS RUN_DATE,

            COALESCE(
                TRY_CAST(TRIM(CAST("BNG Name" AS VARCHAR)) AS VARCHAR),
                ''
            ) AS BNG_NAME,

            '_N' AS LAG,

            COALESCE(
                TRY_CAST(regexp_extract(filename, '[^/]+$') AS VARCHAR),
                ''
            ) AS FILE_NAME,

            TRY_CAST(
                regexp_extract(
                    TRIM(CAST("Total Bytes Received" AS VARCHAR)),
                    '^([^.]*)'
                ) AS BIGINT
            ) AS RECEIVED_VOLUME,

            TRY_CAST(
                regexp_extract(
                    TRIM(CAST("Total Bytes Transmitted" AS VARCHAR)),
                    '^([^.]*)'
                ) AS BIGINT
            ) AS TRANSMITTED_VOLUME,

            TRY_CAST(
                regexp_extract(
                    TRIM(CAST("Total Bytes (Transmitted + Received)" AS VARCHAR)),
                    '^([^.]*)'
                ) AS BIGINT
            ) AS TOTAL_VOLUME

        FROM read_csv_auto(
            '{file_path}',
            delim=',',
            header=TRUE,
            filename=TRUE,
            ignore_errors=TRUE
        );
        """)

        count = con.execute("SELECT COUNT(*) FROM solarwinds_data").fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        if count == 0:
            logger.info("No solarwinds data processed!")
            raise
        
        con.execute(f"""
        COPY (
            SELECT RUN_DATE, BNG_NAME , LAG , FILE_NAME , RECEIVED_VOLUME , TRANSMITTED_VOLUME , TOTAL_VOLUME
            FROM solarwinds_data 
        )
        TO '{csv_path}'
        (DELIMITER ',', HEADER TRUE,  ESCAPE '"');
        """)

        logger.info(f"Inserting into PI.SOLARWINDS table ..")
        ctl_file = prepare_ctl("solarwinds", run_date)
        run_sqlldr("solarwinds",run_date, ctl_file=ctl_file)
        logger.info(f"Insertion done")
        logger.info(f"Validating inserted data ..")

        validate_loaded_data(
        flow_name="solarwinds",
        run_date=run_date,
        expected_rows=count,
        logger=logger)

        logger.info("Inserted data validated successfully.")

        end_time = datetime.now()
        logger.info("solarwinds finished")
        logger.info(f"Total duration: {end_time - start_time}")
        send_mail(
                    subject="SOLARWINDS Flow Finished",
                    content=f"""
                Run Date : {run_date}
                Rows      : {count}
                DB Validation : Passed
                Duration  : {end_time - start_time}""")

        return con.table("solarwinds_data")

    except Exception as e:
        logger.exception( f"solarwinds failed for run_date= {run_date} | error= {str(e)}")
        send_mail(
        subject="SOLARWINDS Flow Failed",
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
    load_solarwinds(run_date=args.run_date)
  i want to remvowe unesaart parts why am i using quote in here  )
        TO '{csv_path}'
        (DELIMITER ',', HEADER TRUE, QUOTE '"', ESCAPE '"');
        """)






        so the same for this other script as well :

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
            

        run_date_formatted = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:]}"
        solarwinds_date = f"{run_date[:4]}_{run_date[4:6]}_{run_date[6:]}"
        csv_path = f"/project/PLATFORM_INTEGRITY/exports/solarwinds_new_{run_date}.csv"
        file_path = (f"/RAIDdata/PI/SOLARWINDS/out_2/*{solarwinds_date}*updated*")

        con.execute(f"""
CREATE OR REPLACE TABLE solarwinds_new_data AS
SELECT
    CASE
        WHEN TRY_CAST(TRIM(TRY_CAST("Timestamp" AS VARCHAR)) AS DATE) IS NOT NULL
        THEN TRY_CAST(
            TRY_CAST(TRIM(TRY_CAST("Timestamp" AS VARCHAR)) AS DATE)
            AS VARCHAR
        )
        ELSE ''
    END AS RUN_DATE,

    COALESCE(
        TRIM(TRY_CAST("BNG Name" AS VARCHAR)),
        ''
    ) AS BNG_NAME,

    COALESCE(
        TRIM(TRY_CAST("Interface Name" AS VARCHAR)),
        ''
    ) AS INTERFACE_NAME,

    COALESCE(
        TRIM(TRY_CAST("Interface Caption" AS VARCHAR)),
        ''
    ) AS INTERFACE_CAPTION,

     COALESCE(
        TRY_CAST(regexp_extract(filename, '[^/]+$') AS VARCHAR),
        ''
    ) AS FILE_NAME,

    TRY_CAST(
        REGEXP_REPLACE(
            TRIM(TRY_CAST("Total Bytes Received" AS VARCHAR)),
            '\.[0-9]+[[:space:]]*B$',
            ''
        ) AS BIGINT
    ) AS RECEIVED_VOLUME,

    TRY_CAST(
        REGEXP_REPLACE(
            TRIM(TRY_CAST("Total Bytes Transmitted" AS VARCHAR)),
            '\.[0-9]+[[:space:]]*B$',
            ''
        ) AS BIGINT
    ) AS TRANSMITTED_VOLUME,

    TRY_CAST(
        REGEXP_REPLACE(
            TRIM(TRY_CAST("Total Bytes (Transmitted + Received)" AS VARCHAR)),
            '\.[0-9]+[[:space:]]*B$',
            ''
        ) AS BIGINT
    ) AS TOTAL_VOLUME,

    CASE
        WHEN UPPER(
            COALESCE(
                TRIM(TRY_CAST("Interface Caption" AS VARCHAR)),
                ''
            )
        ) LIKE '%HSUBBT%'
        THEN 'HSUBBT'
        ELSE 'UBBT'
    END AS SPEED

FROM read_csv_auto(
    '{file_path}',
    delim=',',
    header=TRUE,
    filename=TRUE,
    ignore_errors=TRUE
);
""")

        count = con.execute("SELECT COUNT(*) FROM solarwinds_new_data").fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        if count == 0:
            logger.info("No solarwinds_new data processed!")
            raise
        
        con.execute(f"""
        COPY (
            SELECT RUN_DATE, BNG_NAME ,INTERFACE_NAME , INTERFACE_CAPTION , FILE_NAME , 
            RECEIVED_VOLUME , TRANSMITTED_VOLUME , TOTAL_VOLUME , SPEED
            FROM solarwinds_new_data 
        )
        TO '{csv_path}'
        (DELIMITER ',', HEADER TRUE, QUOTE '"', ESCAPE '"');
    """)

        logger.info(f"Inserting into PI.SOLARWINDS_NEW table ..")
        ctl_file = prepare_ctl("solarwinds_new", run_date)
        run_sqlldr("solarwinds_new",run_date, ctl_file=ctl_file)
        logger.info(f"Insertion done")
        logger.info(f"Validating inserted data ..")

        validate_loaded_data(
        flow_name="solarwinds_new",
        run_date=run_date,
        expected_rows=count,
        logger=logger)

        logger.info("Inserted data validated successfully.")

        end_time = datetime.now()
        logger.info("solarwinds_new finished")
        logger.info(f"Total duration: {end_time - start_time}")
        send_mail(
                    subject="SOLARWINDS_NEW Flow Finished",
                    content=f"""
                Run Date : {run_date}
                Rows      : {count}
                DB Validation : Passed
                Duration  : {end_time - start_time}""")

        return con.table("solarwinds_new_data")

    except Exception as e:
        logger.exception( f"SOLARWINDS_NEW failed for run_date= {run_date} | error= {str(e)}")
        send_mail(
        subject="SOLARWINDS_NEW Flow Failed",
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
    load_solarwinds_new(run_date=args.run_date)



why coalesce 
