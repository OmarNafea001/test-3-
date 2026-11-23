import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

import argparse
from datetime import datetime, date
from config.duckdb_connection import duckdb_connection
from utils.logger import get_logger

EXPORTS = "/project/PLATFORM_INTEGRITY/exports"          # outputs (and today's pcrf)
HISTORY_DIR = "/RAIDdata/PI/PI_PRJ/exports"               # historical dumps

MONTHS_BACK = 5                                           # search window: run_date - 5 months .. run_date


def clean(col, alias=None):
    """Empty / blank text -> NULL, like Oracle treats ''."""
    return f"NULLIF(TRIM({col}), '') AS {alias or col}"


def months_before(d: date, months: int) -> date:
    """Same day, N months earlier (e.g. 2026-11-01 -> 2026-06-01)."""
    y, m = d.year, d.month - months
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, min(d.day, 28))


def export_te_missing_speed(run_date: str, con=None):

    logger = get_logger(
        flow="te_missing_speed",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        logger.info("###########################################################")
        logger.info(f"###### te_missing_speed for run_date= {run_date} ######")
        logger.info("###########################################################")

        start_time = datetime.now()

        run_dt = datetime.strptime(run_date, "%Y%m%d").date()
        run_date_iso = run_dt.strftime("%Y-%m-%d")
        from_date_iso = months_before(run_dt, MONTHS_BACK).strftime("%Y-%m-%d")
        logger.info(f"Search window: {from_date_iso} .. {run_date_iso}")

        validation_file = f"{EXPORTS}/te_invoice_validation_{run_date}.csv"   # RD_T_TE_INVOICE_VALIDATION
        pcrf_hist_dump = f"{HISTORY_DIR}/pcrf_raw_*"                          # PFM_PCRF (history dir)
        bscs_dump = f"{HISTORY_DIR}/bscs_profiles_adsl_*"                     # PFM_BSCS_ADSL
        charging_dump = f"{HISTORY_DIR}/charging_adsl_*"                      # RD_T_IN_ADSL_HISTORICAL

        # ---------------------------------------------------------------
        # 1. TE MSISDNs with no speed in the validation result
        # ---------------------------------------------------------------
        con.execute(f"""
            CREATE OR REPLACE TABLE missing_speed_msisdn AS
            SELECT DISTINCT TE_MSISDN AS MSISDN
            FROM (
                SELECT {clean('TE_MSISDN')}, {clean('SPEED')}
                FROM read_csv_auto('{validation_file}', all_varchar=true, ignore_errors=True)
            )
            WHERE SPEED IS NULL
              AND TE_MSISDN IS NOT NULL
        """)

        missing = con.execute("SELECT COUNT(*) FROM missing_speed_msisdn").fetchone()[0]
        logger.info(f"TE MSISDNs with missing speed: {missing}")

        # ---------------------------------------------------------------
        # 2. History sources, last 5 months only, missing MSISDNs only
        # ---------------------------------------------------------------
        con.execute(f"""
            CREATE OR REPLACE TABLE speed_pcrf_src AS
            SELECT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('SERVICE_NAME')}
            FROM read_csv_auto('{pcrf_hist_dump}', all_varchar=true, union_by_name=true, ignore_errors=True)
            WHERE TRIM(RUN_DATE) BETWEEN '{from_date_iso}' AND '{run_date_iso}'
              AND TRIM(MSISDN) IN (SELECT MSISDN FROM missing_speed_msisdn)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE speed_bscs_src AS
            SELECT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('PCRF_SERVICE_NAME_EXPECTED')}
            FROM read_csv_auto('{bscs_dump}', all_varchar=true, union_by_name=true, ignore_errors=True)
            WHERE TRIM(RUN_DATE) BETWEEN '{from_date_iso}' AND '{run_date_iso}'
              AND TRIM(MSISDN) IN (SELECT MSISDN FROM missing_speed_msisdn)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE speed_charging_src AS
            SELECT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('PCRF_SERVICE_ID_EXPECTED')}
            FROM read_csv_auto('{charging_dump}', all_varchar=true, union_by_name=true, ignore_errors=True)
            WHERE TRIM(RUN_DATE) BETWEEN '{from_date_iso}' AND '{run_date_iso}'
              AND TRIM(MSISDN) IN (SELECT MSISDN FROM missing_speed_msisdn)
        """)

        for t in ("speed_pcrf_src", "speed_bscs_src", "speed_charging_src"):
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            logger.info(f"{t}: {n} rows")

        # ---------------------------------------------------------------
        # 3. The 3 Oracle queries, in one result with a SOURCE column
        # ---------------------------------------------------------------
        con.execute(f"""
            CREATE OR REPLACE TABLE te_missing_speed AS
            WITH
            -- PCRF
            pcrf_speed AS (
                SELECT DISTINCT
                       MSISDN,
                       SERVICE_NAME,
                       NULLIF(regexp_extract(SERVICE_NAME, '[0-9]+M'), '') AS SPEED
                FROM speed_pcrf_src
                WHERE SERVICE_NAME IS NOT NULL
                  AND SERVICE_NAME <> '_N'
            ),

            -- BSCS
            bscs_speed AS (
                SELECT DISTINCT
                       MSISDN,
                       PCRF_SERVICE_NAME_EXPECTED AS SERVICE_NAME,
                       NULLIF(regexp_extract(PCRF_SERVICE_NAME_EXPECTED, '[0-9]+M'), '') AS SPEED
                FROM speed_bscs_src
            ),

            -- TE_CHARGING
            charging_speed AS (
                SELECT DISTINCT
                       MSISDN,
                       PCRF_SERVICE_ID_EXPECTED AS SERVICE_NAME,
                       NULLIF(regexp_extract(PCRF_SERVICE_ID_EXPECTED, '[0-9]+M'), '') AS SPEED
                FROM speed_charging_src
            )

            SELECT DATE '{run_date_iso}' AS RUN_DATE, MSISDN, 'PCRF' AS SOURCE, SERVICE_NAME, SPEED
            FROM pcrf_speed WHERE SPEED IS NOT NULL
            UNION ALL
            SELECT DATE '{run_date_iso}' AS RUN_DATE, MSISDN, 'BSCS' AS SOURCE, SERVICE_NAME, SPEED
            FROM bscs_speed WHERE SPEED IS NOT NULL
            UNION ALL
            SELECT DATE '{run_date_iso}' AS RUN_DATE, MSISDN, 'IN' AS SOURCE, SERVICE_NAME, SPEED
            FROM charging_speed WHERE SPEED IS NOT NULL
        """)

        count = con.execute("SELECT COUNT(*) FROM te_missing_speed").fetchone()[0]
        found = con.execute("SELECT COUNT(DISTINCT MSISDN) FROM te_missing_speed").fetchone()[0]
        logger.info(f"Total processed rows: {count} | MSISDNs with a speed found: {found} of {missing}")

        out_file = f"{EXPORTS}/te_missing_speed_{run_date}.csv"
        logger.info(f"Exporting {out_file} ..")
        con.execute(f"""COPY te_missing_speed TO '{out_file}' (HEADER, DELIMITER ',')""")

        end_time = datetime.now()
        logger.info("te_missing_speed job finished")
        logger.info(f"te_missing_speed total duration: {end_time - start_time}")
    except Exception as e:
        logger.exception(f"te_missing_speed failed for run_date= {run_date} | error= {str(e)}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()
    export_te_missing_speed(run_date=args.run_date)
    
    
    TE_MISSING_SPEED_SCRIPT = "/project/PLATFORM_INTEGRITY/flows/te_missing_speed.py"
    
    
    def _load_te_missing_speed():
    if not os.path.exists(TE_MISSING_SPEED_SCRIPT):
        raise FileNotFoundError(f"Flow not found: {TE_MISSING_SPEED_SCRIPT}")
    spec = importlib.util.spec_from_file_location("te_missing_speed", TE_MISSING_SPEED_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_te_missing_speed = _load_te_missing_speed()
export_te_missing_speed = _te_missing_speed.export_te_missing_speed




        logger.info(f"Exporting te_invoice_validation_{run_date}.csv ..")
        con.execute(f"""COPY te_invoice_validation TO '{EXPORTS}/te_invoice_validation_{run_date}.csv' (HEADER, DELIMITER ',')""")

        # 4. Trigger the missing-speed flow (reads the file exported above)
        logger.info("Running te_missing_speed flow ..")
        export_te_missing_speed(run_date, con=con)
