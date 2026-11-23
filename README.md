import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

import argparse
from datetime import datetime
from config.duckdb_connection import duckdb_connection
from utils.logger import get_logger
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr

EXPORTS = "/project/PLATFORM_INTEGRITY/exports"          # outputs (and today's pcrf)
HISTORY_DIR = "/RAIDdata/PI/PI_PRJ/exports"               # historical dumps (bscs, charging, TE, radius, pcrf)


def clean(col, alias=None):
    """Empty / blank text -> NULL, like Oracle treats ''."""
    return f"NULLIF(TRIM({col}), '') AS {alias or col}"


def export_quota_lkp(run_date: str, con=None):

    logger = get_logger(
        flow="quota_lkp",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        logger.info("###########################################################")
        logger.info(f"###### quota_lkp for run_date= {run_date} ######")
        logger.info("###########################################################")

        start_time = datetime.now()
        run_date_iso = datetime.strptime(run_date, "%Y%m%d").strftime("%Y-%m-%d")

        bscs_dump = f"{HISTORY_DIR}/bscs_profiles_adsl_{run_date}*"   # PFM_BSCS_ADSL
        charging_dump = f"{HISTORY_DIR}/charging_adsl_{run_date}*"    # RD_T_IN_ADSL_HISTORICAL

        # PFM_PCRF (history): all pcrf_raw files in both directories;
        # only run_date and the days before it are kept (filter in the query)
        pcrf_hist_dump = f"{HISTORY_DIR}/pcrf_raw_*"
        pcrf_today_dump = f"{EXPORTS}/pcrf_raw_*"

        # ---------------------------------------------------------------
        # 1. Sources
        # ---------------------------------------------------------------
        con.execute(f"""
            CREATE OR REPLACE TABLE lkp_bscs_src AS
            SELECT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('PCRF_SERVICE_NAME_EXPECTED')},
                   {clean('"TYPE"', 'TYPE')}, {clean('TM_CODE_DESC')}
            FROM read_csv_auto('{bscs_dump}', all_varchar=true, ignore_errors=True)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE lkp_charging_src AS
            SELECT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('PCRF_SERVICE_NAME')}, {clean('PROD_TYPE_NAME')}
            FROM read_csv_auto('{charging_dump}', all_varchar=true, ignore_errors=True)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE lkp_pcrf_hist_src AS
            SELECT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('SERVICE_NAME')}, {clean('PROD_TYPE_NAME')}
            FROM read_csv_auto(['{pcrf_hist_dump}', '{pcrf_today_dump}'], all_varchar=true, union_by_name=true, ignore_errors=True)
        """)

        for t in ("lkp_bscs_src", "lkp_charging_src", "lkp_pcrf_hist_src"):
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            logger.info(f"{t}: {n} rows")

        # ---------------------------------------------------------------
        # 2. Lookup: one main quota name per MSISDN
        # ---------------------------------------------------------------
        con.execute(f"""
            CREATE OR REPLACE TABLE quota_lkp AS
            WITH
            bscs_quota AS (
                SELECT RUN_DATE AS run_date, MSISDN AS msisdn,
                       PCRF_SERVICE_NAME_EXPECTED AS service_name, 'BSCS' AS source
                FROM lkp_bscs_src
                WHERE "TYPE" = 'POST'
                  AND TM_CODE_DESC NOT LIKE 'SDSL%'
                  AND TM_CODE_DESC NOT LIKE 'Cloud%'
            ),

            in_quota AS (
                SELECT RUN_DATE AS run_date, MSISDN AS msisdn,
                       PCRF_SERVICE_NAME AS service_name, 'IN' AS source
                FROM lkp_charging_src
                WHERE PROD_TYPE_NAME = 'Main Bundle'
            ),

            -- latest Main Bundle service per MSISDN across all PCRF dumps <= run_date
            pcrf_latest AS (
                SELECT MSISDN, SERVICE_NAME,
                       ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY RUN_DATE DESC) AS r
                FROM lkp_pcrf_hist_src
                WHERE RUN_DATE <= '{run_date_iso}'          -- run_date <= $1
                  AND PROD_TYPE_NAME = 'Main Bundle'
                  AND SERVICE_NAME <> 'Serv_DSL_Generic_Promo'
                  AND SERVICE_NAME <> 'Serv_DSL_Compensation_Gift'
                  AND SERVICE_NAME <> 'Serv_DSL_Throttling'
                  AND SERVICE_NAME <> 'ADSL_OnHold'
                  AND SERVICE_NAME <> 'Serv_DSL_Double_Quota_Pre'
                  AND SERVICE_NAME NOT LIKE '%Add%'
                  AND SERVICE_NAME NOT LIKE '%DoubleQuota%'
            ),

            pcrf_quota AS (
                SELECT '{run_date_iso}' AS run_date, MSISDN AS msisdn,
                       SERVICE_NAME AS service_name, 'PCRF' AS source
                FROM pcrf_latest
                WHERE r = 1
            ),

            all_sources AS (
                SELECT * FROM bscs_quota
                UNION
                SELECT * FROM in_quota
                UNION
                SELECT * FROM pcrf_quota
            ),

            ranked AS (
                SELECT msisdn, service_name, source,
                       ROW_NUMBER() OVER (PARTITION BY msisdn ORDER BY run_date DESC) AS r
                FROM all_sources
            )

            SELECT DATE '{run_date_iso}' AS "TIME",
                   msisdn                AS MSISDN,
                   service_name          AS QUOTA_NAME,
                   source                AS SOURCE
            FROM ranked
            WHERE r = 1
        """)

        count = con.execute("SELECT COUNT(*) FROM quota_lkp").fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        if count == 0:
            raise ValueError(f"quota_lkp produced 0 rows for RUN_DATE= {run_date}; check source files and filters")

        lkp_file = f"{EXPORTS}/quota_lkp_{run_date}.csv"
        logger.info(f"Exporting {lkp_file} ..")
        con.execute(f"""COPY quota_lkp TO '{lkp_file}' (HEADER, DELIMITER ',')""")

        logger.info("Inserting into QUOTA_NAME_LKP table ..")
        ctl_file = prepare_ctl("quota_lkp", run_date)
        run_sqlldr("quota_lkp", run_date, ctl_file=ctl_file)
        logger.info("Insertion done")

        end_time = datetime.now()
        logger.info("quota_lkp job finished")
        logger.info(f"quota_lkp total duration: {end_time - start_time}")
    except Exception as e:
        logger.exception(f"quota_lkp failed for run_date= {run_date} | error= {str(e)}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()
    export_quota_lkp(run_date=args.run_date)
