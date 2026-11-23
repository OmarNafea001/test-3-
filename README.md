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
from utils.dump_vs_database import validate_loaded_data
import importlib.util

# full path of the lookup flow script (it lives in a different folder)
QUOTA_LKP_SCRIPT = "/project/PLATFORM_INTEGRITY/flows/quota_lkp.py"   


def _load_quota_lkp():
    if not os.path.exists(QUOTA_LKP_SCRIPT):
        raise FileNotFoundError(f"Lookup flow not found: {QUOTA_LKP_SCRIPT}")
    spec = importlib.util.spec_from_file_location("quota_lkp", QUOTA_LKP_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_quota_lkp = _load_quota_lkp()
export_quota_lkp = _quota_lkp.export_quota_lkp
HISTORY_DIR = _quota_lkp.HISTORY_DIR

EXPORTS = "/project/PLATFORM_INTEGRITY/exports"


def clean(col, alias=None):
    """Empty / blank text -> NULL, like Oracle treats ''."""
    return f"NULLIF(TRIM({col}), '') AS {alias or col}"


def export_te_invoice_validation(run_date: str, con=None):

    logger = get_logger(
        flow="te_invoice_validation",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        logger.info("###########################################################")
        logger.info(f"###### te_invoice_validation for run_date= {run_date} ######")
        logger.info("###########################################################")

        start_time = datetime.now()

        # ---------------------------------------------------------------
        # 0. Trigger the lookup flow first (writes exports/quota_lkp_{run_date}.csv)
        # ---------------------------------------------------------------
        logger.info("Running quota_lkp flow first ..")
        export_quota_lkp(run_date, con=con)

        te_dump = f"{EXPORTS}/TE_data_{run_date}*.csv"            # RD_T_TE_DATA_BASE_HISTORICAL
        bscs_dump = f"{HISTORY_DIR}/bscs_profiles_adsl_{run_date}*"   # PFM_BSCS_ADSL
        radius_dump = f"{HISTORY_DIR}/radius_{run_date}*"             # PFM_RADIUS
        charging_dump = f"{HISTORY_DIR}/charging_adsl_{run_date}*"    # RD_T_IN_ADSL_HISTORICAL
        pcrf_hist_dump = f"{HISTORY_DIR}/pcrf_filtered_{run_date}*"         # PFM_PCRF_FILTERED (history dir)
        pcrf_today_dump = f"{EXPORTS}/pcrf_filtered_{run_date}*"            # PFM_PCRF_FILTERED (exports dir)
        quota_lkp_file = f"{EXPORTS}/quota_lkp_{run_date}.csv"        # QUOTA_NAME_LKP (lookup flow output)

        # ---------------------------------------------------------------
        # 1. Sources  (SELECT DISTINCT * ... -> only the columns the query uses)
        # ---------------------------------------------------------------
        con.execute(f"""
            CREATE OR REPLACE TABLE te_src AS
            SELECT DISTINCT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('TE_SPEED')}
            FROM read_csv_auto('{te_dump}', all_varchar=true, ignore_errors=True)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE te_bscs_src AS
            SELECT DISTINCT {clean('MSISDN')}, {clean('STATUS')}, {clean('"TYPE"', 'TYPE')}
            FROM read_csv_auto('{bscs_dump}', all_varchar=true, ignore_errors=True)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE te_pcrf_src AS
            SELECT DISTINCT {clean('MSISDN')}, {clean('CUSTOMER_STATUS')}
            FROM read_csv_auto(['{pcrf_hist_dump}', '{pcrf_today_dump}'], all_varchar=true, union_by_name=true, ignore_errors=True)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE te_radius_src AS
            SELECT DISTINCT {clean('MSISDN')}, {clean('STATUS')}
            FROM read_csv_auto('{radius_dump}', all_varchar=true, ignore_errors=True)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE te_charging_src AS
            SELECT DISTINCT MSISDN, STATUS
            FROM (
                SELECT {clean('MSISDN')}, {clean('STATUS')}, {clean('PACKAGE_TYPE')}
                FROM read_csv_auto('{charging_dump}', all_varchar=true, ignore_errors=True)
            )
            WHERE PACKAGE_TYPE = 'DSL Status'
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE te_quota_src AS
            SELECT DISTINCT {clean('MSISDN')}, {clean('QUOTA_NAME')}
            FROM read_csv_auto('{quota_lkp_file}', all_varchar=true, ignore_errors=True)
        """)

        for t in ("te_src", "te_bscs_src", "te_pcrf_src", "te_radius_src", "te_charging_src", "te_quota_src"):
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            logger.info(f"{t}: {n} rows")

        # ---------------------------------------------------------------
        # 2. Status helpers (same as adsl_status_recon)
        # ---------------------------------------------------------------
        con.execute("""
            CREATE OR REPLACE TEMP MACRO pcrf_status_decode(s) AS
                UPPER(CASE s WHEN '0' THEN 'DeActive'
                             WHEN '1' THEN 'Active'
                             WHEN '2' THEN 'Suspended'
                             WHEN '3' THEN 'On-Hold'
                             ELSE s END)
        """)

        con.execute("""
            CREATE OR REPLACE TEMP MACRO radius_status_decode(s) AS
                UPPER(CASE s WHEN '0' THEN 'DeActive'
                             WHEN '1' THEN 'Active'
                             WHEN '2' THEN 'Suspended'
                             ELSE s END)
        """)

        # ---------------------------------------------------------------
        # 3. Invoice validation
        # ---------------------------------------------------------------
        con.execute("""
            CREATE OR REPLACE TABLE te_invoice_validation AS
            WITH
            -- status of every TE MSISDN in each system ('Not_Exist' if missing)
            te_status AS (
                SELECT DISTINCT
                       te.RUN_DATE                                              AS run_date,
                       te.MSISDN                                                AS te_msisdn,
                       COALESCE(radius_status_decode(radius.STATUS), 'Not_Exist') AS radius_status,
                       COALESCE(bscs.STATUS, 'Not_Exist')                       AS billing_status,
                       COALESCE(UPPER(charging.STATUS), 'Not_Exist')            AS charging_status,
                       bscs."TYPE"                                              AS customer_type,
                       te.TE_SPEED                                              AS te_speed,
                       pcrf_status_decode(pcrf.CUSTOMER_STATUS)                 AS pcrf_status
                FROM te_src te
                LEFT JOIN te_bscs_src     bscs     ON te.MSISDN = bscs.MSISDN
                LEFT JOIN te_pcrf_src     pcrf     ON te.MSISDN = pcrf.MSISDN
                LEFT JOIN te_radius_src   radius   ON te.MSISDN = radius.MSISDN
                LEFT JOIN te_charging_src charging ON te.MSISDN = charging.MSISDN
            ),

            quota AS (
                SELECT MSISDN, QUOTA_NAME
                FROM te_quota_src
            )

            SELECT DISTINCT
                s.run_date          AS RUN_DATE,
                s.te_msisdn         AS TE_MSISDN,
                s.radius_status     AS RADIUS_STATUS,
                s.billing_status    AS BILLING_STATUS,
                s.charging_status   AS CHARGING_STATUS,
                CASE
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'ACTIVE'    AND s.charging_status = 'Not_Exist' THEN 'ACTIVE'
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'Not_Exist' AND s.charging_status = 'ACTIVE'    THEN 'ACTIVE'
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'ACTIVE'    AND s.charging_status = 'ACTIVE'    THEN 'ACTIVE'
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'SUSPENDED' AND s.charging_status = 'SUSPENDED' THEN 'SUSPENDED'
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'SUSPENDED' AND s.charging_status = 'ACTIVE'    THEN 'SUSPENDED'
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'ACTIVE'    AND s.charging_status = 'SUSPENDED' THEN 'SUSPENDED'
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'SUSPENDED' AND s.charging_status = 'Not_Exist' THEN 'SUSPENDED'
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'Not_Exist' AND s.charging_status = 'SUSPENDED' THEN 'SUSPENDED'
                    WHEN s.radius_status = 'Not_Exist' AND s.billing_status = 'Not_Exist' AND s.charging_status = 'Not_Exist' THEN 'Not_Exist'
                    WHEN s.radius_status = 'DEACTIVE'  AND s.billing_status = 'Not_Exist' AND s.charging_status = 'Not_Exist' THEN 'Not_Exist/DEACTIVE'
                    WHEN s.radius_status = 'DEACTIVE'  AND s.billing_status = 'SUSPENDED' AND s.charging_status = 'Not_Exist' THEN 'MISMATCH'
                    WHEN s.radius_status = 'ACTIVE'    AND s.billing_status = 'Not_Exist' AND s.charging_status = 'Not_Exist' THEN 'MISMATCH'
                    WHEN s.radius_status = 'DEACTIVE'  AND s.billing_status = 'ACTIVE'    AND s.charging_status = 'Not_Exist' THEN 'MISMATCH'
                    WHEN s.radius_status = 'Not_Exist' AND s.billing_status = 'ACTIVE'    AND s.charging_status = 'ACTIVE'    THEN 'MISMATCH'
                    WHEN s.radius_status = 'Not_Exist' AND s.billing_status = 'ACTIVE'    AND s.charging_status = 'Not_Exist' THEN 'MISMATCH'
                    WHEN s.radius_status = 'DEACTIVE'  AND s.billing_status = 'ACTIVE'    AND s.charging_status = 'SUSPENDED' THEN 'MISMATCH'
                    WHEN s.radius_status = 'DEACTIVE'  AND s.billing_status = 'ACTIVE'    AND s.charging_status = 'ACTIVE'    THEN 'MISMATCH'
                    WHEN s.radius_status = 'DEACTIVE'  AND s.billing_status = 'Not_Exist' AND s.charging_status = 'ACTIVE'    THEN 'MISMATCH'
                    WHEN s.radius_status = 'Not_Exist' AND s.billing_status = 'SUSPENDED' AND s.charging_status = 'Not_Exist' THEN 'MISMATCH'
                    WHEN s.radius_status = 'DEACTIVE'  AND s.billing_status = 'Not_Exist' AND s.charging_status = 'SUSPENDED' THEN 'MISMATCH'
                    ELSE '_N'
                END                                              AS CASE_COMMENT,
                COALESCE(s.customer_type, 'NA')                  AS CUSTOMER_TYPE,   -- replaces the UPDATE ... SET 'NA'
                s.pcrf_status                                    AS PCRF_STATUS,
                quota.QUOTA_NAME                                 AS QUOTA_NAME,
                s.te_speed                                       AS TE_SPEED,
                NULLIF(regexp_extract(quota.QUOTA_NAME, '[0-9]+M'), '') AS SPEED  -- REGEXP_SUBSTR(QUOTA_NAME,'[0-9]+M',1,1)
            FROM te_status s
            LEFT JOIN quota
                ON s.te_msisdn = quota.MSISDN
        """)

        count = con.execute("SELECT COUNT(*) FROM te_invoice_validation").fetchone()[0]
        logger.info(f"Total processed rows: {count}")

        if count == 0:
            raise ValueError(f"te_invoice_validation produced 0 rows for RUN_DATE= {run_date}; check source files and filters")

        logger.info(f"Exporting te_invoice_validation_{run_date}.csv ..")
        con.execute(f"""COPY te_invoice_validation TO '{EXPORTS}/te_invoice_validation_{run_date}.csv' (HEADER, DELIMITER ',')""")

        #logger.info("Inserting into RD_T_TE_INVOICE_VALIDATION table ..")
        #ctl_file = prepare_ctl("te_invoice_validation", run_date)
        #run_sqlldr("te_invoice_validation", run_date, ctl_file=ctl_file)
        #logger.info("Insertion done")

        #logger.info("Validating inserted data ..")
        #validate_loaded_data(
        #    flow_name="te_invoice_validation",
        #    run_date=run_date,
        #    expected_rows=count,
        #    logger=logger
        #)
        #logger.info("Inserted data validated successfully.")
#
        #end_time = datetime.now()
        #logger.info("te_invoice_validation job finished")
        #logger.info(f"te_invoice_validation total duration: {end_time - start_time}")
    except Exception as e:
        logger.exception(f"te_invoice_validation failed for run_date= {run_date} | error= {str(e)}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()
    export_te_invoice_validation(run_date=args.run_date)




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
            SELECT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('PCRF_SERVICE_ID_EXPECTED')}, {clean('PACKAGE_TYPE')}
            FROM read_csv_auto('{charging_dump}', all_varchar=true, ignore_errors=True)
        """)

        con.execute(f"""
            CREATE OR REPLACE TABLE lkp_pcrf_hist_src AS
            SELECT {clean('RUN_DATE')}, {clean('MSISDN')}, {clean('SERVICE_NAME')}
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
                       PCRF_SERVICE_ID_EXPECTED AS service_name, 'IN' AS source
                FROM lkp_charging_src
                WHERE PACKAGE_TYPE = 'Main Bundle'
            ),

            -- latest Main Bundle service per MSISDN across all PCRF dumps <= run_date
            pcrf_latest AS (
                SELECT MSISDN, SERVICE_NAME,
                       ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY RUN_DATE DESC) AS r
                FROM lkp_pcrf_hist_src
                WHERE RUN_DATE <= '{run_date_iso}'          -- run_date <= $1
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

        #logger.info("Inserting into QUOTA_NAME_LKP table ..")
        #ctl_file = prepare_ctl("quota_lkp", run_date)
        #run_sqlldr("quota_lkp", run_date, ctl_file=ctl_file)
        #logger.info("Insertion done")

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








    thoses are the final scripts that worked ok noe if i want to export file with these queryies ok 
    
---PCRF-----
SELECT *
FROM (
    SELECT /*+ PARALLEL(b,32) */
           DISTINCT
           b.MSISDN,
           b.SERVICE_NAME,
           REGEXP_SUBSTR(b.SERVICE_NAME, '[0-9]+M', 1, 1) AS SPEED
    FROM RP7VFEPIDAT.PFM_PCRF b
    WHERE b.MSISDN IN (
        SELECT /*+ PARALLEL(32) */
               DISTINCT TE_MSISDN
        FROM RP7VFEPIDAT.RD_T_TE_INVOICE_VALIDATION
        WHERE RUN_DATE = DATE '2026-11-01'
          AND SPEED IS NULL
    )
    AND b.SERVICE_NAME IS NOT NULL
    AND b.SERVICE_NAME <> '_N'
    AND b.run_date NOT IN ('2022047','20170912')
)
WHERE SPEED IS NOT NULL;



-- BSCS
SELECT * FROM (
SELECT /*+ PARALLEL(b,32) */
DISTINCT b.MSISDN,
       b.PCRF_SERVICE_NAME_EXPECTED,
       REGEXP_SUBSTR(b.PCRF_SERVICE_NAME_EXPECTED, '[0-9]+M', 1, 1) AS SPEED
FROM RP7VFEPIDAT.PFM_BSCS_ADSL b
WHERE b.MSISDN IN (
        /*+ PARALLEL(32) */ SELECT TE_MSISDN FROM RP7VFEPIDAT.RD_T_TE_INVOICE_VALIDATION
WHERE RUN_DATE = DATE '2026-06-01'
AND SPEED IS NULL)
)
WHERE SPEED IS NOT NULL;
 
 
------- TE_CHARGING ------------
SELECT * FROM (
SELECT /*+ PARALLEL(b,32) */
      DISTINCT 
       b.MSISDN,
       b.PCRF_SERVICE_ID_EXPECTED,
       REGEXP_SUBSTR(b.PCRF_SERVICE_ID_EXPECTED, '[0-9]+M', 1, 1) AS SPEED
FROM RP7VFEPIDAT.RD_T_IN_ADSL_HISTORICAL b
WHERE b.MSISDN IN (
       /*+ PARALLEL(32) */  SELECT TE_MSISDN FROM RP7VFEPIDAT.RD_T_TE_INVOICE_VALIDATION
WHERE RUN_DATE = '01-may-26'
AND SPEED IS NULL)
)
WHERE SPEED IS NOT NULL;


to get the missing speed what do u suggest to do it? 
