import os
import sys
import argparse
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.insert(0, PROJECT_ROOT)

from config.duckdb_connection import duckdb_connection
from utils.logger import get_logger
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr
from utils.dump_vs_database import validate_loaded_data


def load_procera(run_date: str, con=None):

    logger = get_logger(
        flow="procera",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        start_time = datetime.now()
        run_dt = datetime.strptime(run_date, '%Y%m%d')          # fails fast on a bad --run_date
        data_dt = run_dt - timedelta(days=1)
        data_day = data_dt.strftime('%Y-%m-%d')                 # day the file covers = run_date - 1
        file_path = f"/RAIDdata/PI/PROCERA_CDR/raw/PROCERA_FIXED_CDRS_DATA_{data_dt.strftime('%Y%m%d')}.csv"
        output_path = f"/project/PLATFORM_INTEGRITY/exports/procera_{run_date}.csv"
        agg_output_path = f"/project/PLATFORM_INTEGRITY/exports/procera_agg_{run_date}.csv"

        # Row-level staging: one row per CDR line (no aggregates here).
        con.execute(f"""
            CREATE OR REPLACE TABLE procera AS
            SELECT
                CAST('{data_day}' AS DATE)                      AS RUN_DATE,
                TRIM(src.column6)                               AS START_DATE,
                '2' || TRIM(src.column7)                        AS LANDLINE,
                TRY_CAST(src.column4 AS DOUBLE)                 AS TOTAL_BYTES,
                src.column0                                     AS FRAUD_FLAG,
                src.column1                                     AS PCRF_RESULT_CODE,
                src.column2                                     AS RULE_MONITORING_KEY,
                src.column3                                     AS SESSION_ID,
                src.column5                                     AS CLOSE_CAUSE,
                src.column8                                     AS PCRF_RULES_LIST,
                regexp_extract(src.filename, '[^/]+$')          AS SOURCE_FILENAME
            FROM read_csv_auto(
                '{file_path}',
                delim = ',',
                header = false,
                all_varchar = true,
                filename = true,
                ignore_errors = true
            ) src
            WHERE src.column7 IS NOT NULL
              AND TRIM(src.column7) <> 'Subscriber_ID'
        """)

        procera_count = con.execute("SELECT COUNT(*) FROM procera").fetchall()[0][0]

        logger.info(f"Total processed rows of procera : {procera_count}")

        if procera_count == 0:
            logger.warning("No procera data processed!")
            return con.table("procera")

        # Daily aggregate (= RD_T_PROCERA_HISTORICAL_AGG): START_DATE cut to the day.
        con.execute("""
            CREATE OR REPLACE TABLE procera_historical_agg AS
            SELECT
                RUN_DATE,
                SUBSTRING(START_DATE, 1, 10)    AS START_DATE,
                LANDLINE,
                SUM(TOTAL_BYTES)                AS TOTAL_BYTES,
                FRAUD_FLAG,
                PCRF_RESULT_CODE,
                RULE_MONITORING_KEY,
                SESSION_ID,
                CLOSE_CAUSE,
                PCRF_RULES_LIST
            FROM procera
            GROUP BY
                RUN_DATE,
                SUBSTRING(START_DATE, 1, 10),
                LANDLINE,
                FRAUD_FLAG,
                PCRF_RESULT_CODE,
                RULE_MONITORING_KEY,
                SESSION_ID,
                CLOSE_CAUSE,
                PCRF_RULES_LIST
        """)

        agg_count = con.execute("SELECT COUNT(*) FROM procera_historical_agg").fetchall()[0][0]

        logger.info(f"Total processed rows OF procera_historical_agg: {agg_count}")

        if agg_count == 0:
            logger.warning("No procera data processed!")
            return con.table("procera_historical_agg")

        con.execute(f"""
            COPY (
                SELECT *
                FROM procera_historical_agg
            )
            TO '{output_path}'
            (
                DELIMITER ',',
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"'
            )
        """)

        # logger.info(f"procera data successfully exported to {output_path}")

        # logger.info(f"Inserting into PI.PROCERA table ..")
        # ctl_file = prepare_ctl("procera", run_date)
        # run_sqlldr("procera",run_date, ctl_file=ctl_file)
        # logger.info(f"Insertion done")

        # logger.info(f"Validating inserted data ..")
        # validate_loaded_data(
        # flow_name="procera",
        # run_date=run_date,
        # expected_rows=procera_count,      # rows in the exported file (procera)
        # logger=logger)
        # logger.info("Inserted data validated successfully.")

        end_time = datetime.now()
        logger.info(f"procera job finished")
        logger.info(f"procera total duration: {end_time - start_time}")
        # send_mail(   # needs: from utils.<module> import send_mail
        #             subject="procera Flow Finished",
        #             content=f"""
        #         Run Date : {run_date}
        #         Rows      : {procera_count}
        #         DB Validation : Passed
        #         Duration  : {end_time - start_time}""")

        return con.table("procera")

    except Exception as e:
        logger.exception(
            f"procera failed for run_date={run_date} | error={str(e)}"
        )
        raise


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True, help="YYYYMMDD")
    args = parser.parse_args()
    load_procera(run_date=args.run_date)










\


import os
import sys
import argparse
from datetime import datetime, timedelta

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


def load_procera(run_date: str, con=None):

    logger = get_logger(
        flow="procera",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        start_time = datetime.now()
        data_dt = datetime.strptime(run_date, '%Y%m%d') - timedelta(days=1)   # fails fast on a bad --run_date
        data_day = data_dt.strftime('%Y-%m-%d')                 # day the file covers = run_date - 1
        data_date = data_dt.strftime('%Y%m%d')                 # used for file names, sqlldr and validation
        file_path = f"/RAIDdata/PI/PROCERA_CDR/raw/PROCERA_FIXED_CDRS_DATA_{data_date}.csv"
        agg_path = "/RAIDdata/PI/PI_PRJ/exports/procera*"
        output_path = f"/project/PLATFORM_INTEGRITY/exports/procera_{data_date}.csv"
        agg_output_path = f"/project/PLATFORM_INTEGRITY/exports/procera_agg_{data_date}.csv"

        # Row-level staging: one row per CDR line (no aggregates here).
        con.execute(f"""
            CREATE OR REPLACE TABLE procera AS
            SELECT
                DATE '{data_day}'                               AS RUN_DATE,
                TRIM(src.column6)                               AS START_DATE,
                '2' || TRIM(src.column7)                        AS LANDLINE,
                TRY_CAST(src.column4 AS DOUBLE)                 AS TOTAL_BYTES,
                src.column0                                     AS FRAUD_FLAG,
                src.column1                                     AS PCRF_RESULT_CODE,
                src.column2                                     AS RULE_MONITORING_KEY,
                src.column3                                     AS SESSION_ID,
                src.column5                                     AS CLOSE_CAUSE,
                src.column8                                     AS PCRF_RULES_LIST,
                regexp_extract(src.filename, '[^/]+$')          AS SOURCE_FILENAME
            FROM read_csv_auto(
                '{file_path}',
                delim = ',',
                header = false,
                all_varchar = true,
                filename = true,
                ignore_errors = true
            ) src
            WHERE src.column7 IS NOT NULL
              AND TRIM(src.column7) <> 'Subscriber_ID'
        """)

        procera_count = con.execute("SELECT COUNT(*) FROM procera").fetchone()[0]
        logger.info(f"Total processed rows of procera : {procera_count}")

        if procera_count == 0:
            raise ValueError("No procera data processed!")

        # Export procera first, so the aggregate can read it
        con.execute(f"""
            COPY (SELECT * FROM procera)
            TO '{output_path}'
            (DELIMITER ',', HEADER TRUE)
        """)

        # Daily aggregate (= RD_T_PROCERA_HISTORICAL_AGG): today's procera export + old CDRs,
        # only sessions that started on data_day, START_DATE cut to the day.
        con.execute(f"""
            CREATE OR REPLACE TABLE procera_historical_agg AS
            SELECT
                DATE '{data_day}'               AS RUN_DATE,
                SUBSTRING(START_DATE, 1, 10)    AS START_DATE,
                LANDLINE,
                SUM(TRY_CAST(TOTAL_BYTES AS DOUBLE)) AS TOTAL_BYTES,
                FRAUD_FLAG,
                PCRF_RESULT_CODE,
                RULE_MONITORING_KEY,
                SESSION_ID,
                CLOSE_CAUSE,
                PCRF_RULES_LIST
            FROM read_csv_auto(
                ['{output_path}', '{agg_path}'],
                delim = ',',
                header = true,
                all_varchar = true,
                union_by_name = true,
                ignore_errors = true
            )
            WHERE SUBSTRING(START_DATE, 1, 10) = '{data_day}'
            GROUP BY ALL
        """)

        agg_count = con.execute("SELECT COUNT(*) FROM procera_historical_agg").fetchone()[0]
        logger.info(f"Total processed rows of procera_historical_agg : {agg_count}")

        if agg_count == 0:
            raise ValueError("No procera_historical_agg data processed!")

        con.execute(f"""
            COPY (SELECT * FROM procera_historical_agg)
            TO '{agg_output_path}'
            (DELIMITER ',', HEADER TRUE)
        """)

        # Load procera
    #  logger.info("Inserting into PI.PROCERA table ..")
    #  ctl_file = prepare_ctl("procera", data_date)
    #  run_sqlldr("procera", data_date, ctl_file=ctl_file)
    #  logger.info("Insertion done")

    #  logger.info("Validating inserted data ..")
    #  validate_loaded_data(
    #      flow_name="procera",
    #      run_date=data_date,
    #      expected_rows=procera_count,
    #      logger=logger)
    #  logger.info("Inserted data validated successfully.")

    #  # Load procera_agg
    #  logger.info("Inserting into PI.PROCERA_AGG table ..")
    #  ctl_file = prepare_ctl("procera_agg", data_date)
    #  run_sqlldr("procera_agg", data_date, ctl_file=ctl_file)
    #  logger.info("Insertion done")

    #  logger.info("Validating inserted data ..")
    #  validate_loaded_data(
    #      flow_name="procera_agg",
    #      run_date=data_date,
    #      expected_rows=agg_count,
    #      logger=logger)
    #  logger.info("Inserted data validated successfully.")

    #  end_time = datetime.now()
    #  logger.info("procera job finished")
    #  logger.info(f"procera total duration: {end_time - start_time}")
    #  send_mail(
    #      subject="procera Flow Finished",
    #      content=f"""
    #          Run Date      : {run_date}
    #          Data Date     : {data_date}
    #          Procera Rows  : {procera_count}
    #          Agg Rows      : {agg_count}
    #          DB Validation : Passed
    #          Duration      : {end_time - start_time}""")

    #  return con.table("procera")

    except Exception as e:
        logger.exception(
            f"procera failed for run_date={run_date} | error={str(e)}"
        )
  #    send_mail(
  #        subject="procera Flow Failed",
  #        content=f"""
  #            Run Date : {run_date}
  #            Error:
  #            {str(e)}""")
  #    raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True, help="YYYYMMDD")
    args = parser.parse_args()
    load_procera(run_date=args.run_date)
















    what is the difference between both as the files exported from the first is correct nut ftom the second it is not 
    
