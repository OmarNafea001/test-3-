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
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr
from utils.dump_vs_database import validate_loaded_data


def export_free_traffic(run_date: str, con=None):

    logger = get_logger(
        flow="free_traffic_mismatch",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        logger.info("###########################################################")
        logger.info(f"###### FREE TRAFFIC MISMATCH for run_date= {run_date} ######")
        logger.info("###########################################################")

        start_time = datetime.now()
        pcrf_edrs_dump = f"/RAIDdata/PI/PI_PRJ/exports/pcrf_edrs_{run_date}.csv"

        con.execute(f"""
            CREATE OR REPLACE TABLE pcrf_edrs_src AS
            SELECT EDR_DATE, QUOTA_USAGE, SERVICE_NAME, QUOTA_NAME
            FROM read_csv_auto('{pcrf_edrs_dump}', ignore_errors=True)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE free_traffic_mismatch AS
            WITH Total_base AS (
                SELECT CAST(EDR_DATE AS DATE) AS EDR_DATE,
                       SUM(QUOTA_USAGE) AS Usage,
                       'TOTAL' AS TYPE
                FROM pcrf_edrs_src
                WHERE CAST(EDR_DATE AS DATE) >= strptime('{run_date}', '%Y%m%d')::DATE
                GROUP BY CAST(EDR_DATE AS DATE)
            ),

            PRE AS (
                SELECT CAST(EDR_DATE AS DATE) AS EDR_DATE,
                       SUM(QUOTA_USAGE) AS Usage,
                       'PRE' AS TYPE
                FROM pcrf_edrs_src
                WHERE CAST(EDR_DATE AS DATE) >= strptime('{run_date}', '%Y%m%d')::DATE
                  AND SERVICE_NAME IN (
                      'Serv_DSL_Generic_Promo',
                      'Serv_DSL_Seasonal_Gift',
                      'Serv_DSL_Double_Quota_Pre',
                      'Serv_DSL_Compensation_Gift'
                  )
                GROUP BY CAST(EDR_DATE AS DATE)
            ),

            POST AS (
                SELECT CAST(EDR_DATE AS DATE) AS EDR_DATE,
                       SUM(QUOTA_USAGE) AS Usage,
                       'POST' AS TYPE
                FROM pcrf_edrs_src
                WHERE CAST(EDR_DATE AS DATE) >= strptime('{run_date}', '%Y%m%d')::DATE
                  AND (
                      QUOTA_NAME IN ('DSL_Compensation_Gift')
                      OR SERVICE_NAME IN (
                          'Serv_DSL_DoubleQuota_300GB_POST',
                          'Serv_DSL_DoubleQuota_600GB_POST',
                          'Serv_DSL_DoubleQuota_200GB_POST',
                          'Serv_DSL_GC_Gift_3000G'
                      )
                  )
                GROUP BY CAST(EDR_DATE AS DATE)
            )

            SELECT
                t.EDR_DATE AS "TIME",
                ROUND(t.Usage / 1024 / 1024 / 1024, 2) AS TOTAL_TB,
                ROUND(pre.Usage / 1024 / 1024 / 1024, 2) AS PRE_FREE_TRAFFIC_TB,
                100 - ROUND((t.Usage - pre.Usage) / NULLIF(t.Usage, 0), 4) * 100 AS PRE_PERCENTAGE,
                ROUND(post.Usage / 1024 / 1024 / 1024, 2) AS POST_FREE_TRAFFIC_TB,
                100 - ROUND((t.Usage - post.Usage) / NULLIF(t.Usage, 0), 4) * 100 AS POST_PERCENTAGE
            FROM Total_base t
            JOIN PRE pre ON t.EDR_DATE = pre.EDR_DATE
            JOIN POST post ON t.EDR_DATE = post.EDR_DATE
        """)

        count = con.execute("SELECT COUNT(*) FROM free_traffic_mismatch").fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        logger.info(f"Exporting free_traffic_mismatch_{run_date}.csv ..")
        con.execute(f"""COPY free_traffic_mismatch TO '/project/PLATFORM_INTEGRITY/exports/free_traffic_mismatch_{run_date}.csv' (HEADER, DELIMITER ',')""")

        logger.info("Inserting into PI.FREE_TRAFFIC_MISMATCH table ..")
        ctl_file = prepare_ctl("free_traffic_mismatch", run_date)
        run_sqlldr("free_traffic_mismatch", run_date, ctl_file=ctl_file)
        logger.info("Insertion done")

        logger.info("Validating inserted data ..")
        validate_loaded_data(
            flow_name="free_traffic_mismatch",
            run_date=run_date,
            expected_rows=count,
            logger=logger
        )
        logger.info("Inserted data validated successfully.")

        end_time = datetime.now()
        logger.info("free_traffic_mismatch job finished")
        logger.info(f"free_traffic_mismatch total duration: {end_time - start_time}")

    except Exception as e:
        logger.exception(f"free_traffic_mismatch failed for run_date= {run_date} | error= {str(e)}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()
    export_free_traffic(run_date=args.run_date)
