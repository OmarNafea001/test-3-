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
TE_MISSING_SPEED_SCRIPT = "/project/PLATFORM_INTEGRITY/mismatches_flows/te_missing_speed.py"  


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


def _load_te_missing_speed():
    if not os.path.exists(TE_MISSING_SPEED_SCRIPT):
        raise FileNotFoundError(f"Missing speed flow not found: {TE_MISSING_SPEED_SCRIPT}")
    spec = importlib.util.spec_from_file_location("te_missing_speed", TE_MISSING_SPEED_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_te_missing_speed = _load_te_missing_speed()
export_te_missing_speed = _te_missing_speed.export_te_missing_speed

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

        # ---------------------------------------------------------------
        # 4. Trigger the missing-speed flow AFTER the validation file is exported
        #    (it reads exports/te_invoice_validation_{run_date}.csv)
        # ---------------------------------------------------------------
        logger.info("Running te_missing_speed flow ..")
        export_te_missing_speed(run_date, con=con)

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

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.insert(0, PROJECT_ROOT)

import duckdb
import argparse
from datetime import datetime, timedelta
from config.duckdb_connection import duckdb_connection
from utils.logger import get_logger
from utils.send_mail import send_mail
from utils.export_to_oracle import prepare_ctl
from utils.export_to_oracle import run_sqlldr
from utils.dump_vs_database import validate_loaded_data


def load_procera_vs_solarwinds(run_date: str, con=None):

    logger = get_logger(
        flow="procera_vs_solarwinds",
        log_dir=None,
        run_date=run_date
    )

    try:
        if con is None:
            con = duckdb_connection()

        data_dt = datetime.strptime(run_date, '%Y%m%d') - timedelta(days=1)   
        data_day = data_dt.strftime('%Y-%m-%d')
        data_date = data_dt.strftime('%Y%m%d')         # used for file names, sqlldr and validation
        run_date_formatted = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:]}"
        procera_path = f"/project/PLATFORM_INTEGRITY/exports/procera_agg_{run_date}*"      
        solarwinds_path = f"/project/PLATFORM_INTEGRITY/exports/solarwinds_{run_date}*"
        export_path = f"/project/PLATFORM_INTEGRITY/exports/procera_vs_solarwinds_{data_date}.csv"
        
        
        con.execute(f"""
        CREATE OR REPLACE TABLE procera AS
        SELECT
            CAST('{data_day}' AS DATE)              AS RUN_DATE,
            SUM(TRY_CAST(column3 AS BIGINT))        AS TOTAL_BYTES
        FROM read_csv_auto('{procera_path}', ignore_errors=True, header=false)
        GROUP BY 1
        """)

        con.execute(f"""
        CREATE OR REPLACE TABLE solarwinds AS
        SELECT
            CAST('{data_day}' AS DATE)                      AS RUN_DATE,
            SUM(RECEIVED_VOLUME)                            AS RECEIVED_VOLUME,
            SUM(TRANSMITTED_VOLUME)                         AS TRANSMITTED_VOLUME
        FROM read_csv_auto('{solarwinds_path}', ignore_errors=True)
        where FILE_NAME LIKE '%Fiber_n_Copper%'
        GROUP BY 1
        """)

        con.execute("""
        CREATE OR REPLACE TABLE procera_vs_solarwinds AS
        SELECT solarwinds.RUN_DATE  RUN_DATE,
        PROCERA.TOTAL_BYTES AS PROCERA_TOTAL,
        (solarwinds.RECEIVED_VOLUME + solarwinds.TRANSMITTED_VOLUME) AS SOLARWINDS_TOTAL
        from procera
        FULL OUTER JOIN solarwinds
        ON procera.RUN_DATE = solarwinds.RUN_DATE
        """)

        count = con.execute(
            "SELECT COUNT(*) FROM procera_vs_solarwinds"
        ).fetchall()[0][0]

        logger.info(f"Total processed rows: {count}")

        if count == 0:
            logger.info("No procera_vs_solarwinds data processed!")
            raise ValueError(f"No procera_vs_solarwinds rows for run_date={data_date}")
    
        con.execute(f"""
        COPY (
            SELECT *
            FROM procera_vs_solarwinds
        )
        TO '{export_path}'
        (DELIMITER ',', HEADER TRUE, QUOTE '"', ESCAPE '"');
        """)

        # logger.info("Inserting into RD_T_PROCERA_VS_EDRS table ..")

        # ctl_file = prepare_ctl("procera_vs_edrs", run_date)

        # run_sqlldr(
        #     "procera_vs_edrs",
        #     run_date,
        #     ctl_file=ctl_file
        # )

        # logger.info("Insertion done")

        # logger.info("Validating inserted data ..")

        # validate_loaded_data(
        #     flow_name="procera_vs_edrs",
        #     run_date=run_date,
        #     expected_rows=count,
        #     logger=logger
        # )

        # logger.info("Inserted data validated successfully.")

        # end_time = datetime.now()

        # logger.info("procera_vs_edrs job finished")
        # logger.info(
        #     f"procera_vs_edrs duration: {end_time - start_time}"
        # )

        # send_mail(
        #     subject="procera_vs_edrs Flow Finished",
        #     content=f"""
        #         Run Date        : {run_date}
        #         Data Day        : {data_day}
        #         Rows            : {count}
        #         EDRS Total      : {total_edrs}
        #         Procera Total   : {total_procera}
        #         Variance Total  : {total_variance}
        #         Percentage      : {total_pct}
        #         EDRS-only Out   : {excluded}
        #         DB Validation   : Passed
        #         Duration        : {end_time - start_time}
        #         """
        # )

        return con.table("procera_vs_solarwinds")

    except Exception as e:
        logger.exception(
            f"procera_vs_solarwinds failed for run_date={data_date} | error={str(e)}"
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", required=True)
    args = parser.parse_args()

    load_procera_vs_solarwinds(run_date=args.run_date)


    import os 
import sys 
 
PROJECT_ROOT = os.path.abspath( 
os.path.join(os.path.dirname(__file__), "..") 
) 
sys.path.insert(0, PROJECT_ROOT) 
 
import duckdb 
import argparse 
from datetime import datetime, timedelta
from config.duckdb_connection import duckdb_connection 
from utils.logger import get_logger 
from utils.export_to_oracle import prepare_ctl 
from utils.export_to_oracle import run_sqlldr 
from utils.dump_vs_database import validate_loaded_data 
 
 
 
def export_procera_vs_edrs(run_date: str, con=None): 
 
    logger = get_logger( 
        flow="adsl_procera_vs_edrs", 
        log_dir=None,   
        run_date=run_date 
    ) 
 
 
    try: 
        if con is None: 
            con = duckdb_connection() 
     
 
        logger.info(f"################################################################") 
        logger.info(f"###### ADSL PROCERA VS EDRS for run_date= {run_date} ##############") 
        logger.info(f"################################################################") 
 
        start_time = datetime.now() 
        edrs_consum_dump = f"/RAIDdata/PI/PI_PRJ/exports/pcrf_edrs_*"
        procera_dump = f"/RAIDdata/PI/PI_PRJ/exports/procera_agg_*" 
 
        con.execute(f""" 
            CREATE OR REPLACE VIEW pcrf_edrs AS 
            SELECT RUN_DATE,EDR_DATE,TIME,MSISDN,TRIGGER_TYPE,SERVICE_NAME,QUOTA_NAME,QUOTA_STATUS,EXPECTED_ORG_QUOTA,EXPECTED_TAX_QUOTA,QUOTA_CONSUMPTION,QUOTA_USAGE,QUOTA_VALUE,ACTUAL_QUOTA_USAGE,SERVICE_EXTENDED_ATT1,SOURCE_FILENAME
            FROM read_csv_auto('{edrs_consum_dump}', ignore_errors=True, union_by_name=True) 
        """) 

        count = con.execute("SELECT COUNT(*) FROM pcrf_edrs").fetchone()[0] 
        logger.info(f"Total pcrf processed rows: {count}") 
 
        con.execute(f""" 
            CREATE OR REPLACE VIEW procera AS 
            SELECT RUN_DATE,START_DATE,LANDLINE,TOTAL_BYTES,FRAUD_FLAG,PCRF_RESULT_CODE,RULE_MONITORING_KEY,SESSION_ID,CLOSE_CAUSE,PCRF_RULES_LIST            
            FROM read_csv_auto('{procera_dump}', ignore_errors=True) 
        """) 

        count = con.execute("SELECT COUNT(*) FROM procera").fetchone()[0] 
        logger.info(f"Total procera processed rows: {count}") 
 
 
        con.execute(f""" 
       CREATE OR REPLACE TABLE procera_vs_edrs AS

        SELECT
            RUN_DATE,
            EDRS_LANDLINE,
            PROCERA_LANDLINE,
            ROUND(EDRS_CONSUMPTION, 3) AS EDRS_CONSUMPTION,
            ROUND(PROCERA_CONSUMPTION, 3) AS PROCERA_CONSUMPTION,
            ROUND(VARIANCE, 3) AS VARIANCE,
            ROUND(VARIANCE / NULLIF(PROCERA_CONSUMPTION, 0), 2) AS PERCENTAGE
        FROM
        (
            SELECT
                COALESCE(PROCERA.START_DATE, EDRS.EDR_DATE) AS RUN_DATE,
                EDRS.MSISDN AS EDRS_LANDLINE,
                PROCERA.LANDLINE AS PROCERA_LANDLINE,
                EDRS.QUOTA_CONSUMPTION AS EDRS_CONSUMPTION,
                (PROCERA.TOTAL_BYTES / 1024.0) AS PROCERA_CONSUMPTION,
                (
                    COALESCE((PROCERA.TOTAL_BYTES / 1024.0), 0)
                    - COALESCE(EDRS.QUOTA_CONSUMPTION, 0)
                ) AS VARIANCE
            FROM
            (
                SELECT
                    EDR_DATE,
                    MSISDN,
                    SUM(QUOTA_USAGE) AS QUOTA_CONSUMPTION
                FROM pcrf_edrs
                WHERE EDR_DATE = STRPTIME('{run_date}', '%Y%m%d')
                GROUP BY
                    EDR_DATE,
                    MSISDN
            ) EDRS

            FULL OUTER JOIN
            (
                SELECT
                    START_DATE,
                    LANDLINE,
                    SUM(TOTAL_BYTES) AS TOTAL_BYTES
                FROM procera
                WHERE START_DATE = STRPTIME('{run_date}', '%Y%m%d')
                GROUP BY
                    START_DATE,
                    LANDLINE
            ) PROCERA

            ON CAST(EDRS.MSISDN AS VARCHAR) = CAST(PROCERA.LANDLINE AS VARCHAR)

            WHERE
                (
                    COALESCE((PROCERA.TOTAL_BYTES / 1024.0), 0)
                    - COALESCE(EDRS.QUOTA_CONSUMPTION, 0)
                ) <> 0
        );
         """ ) 
 
     
        count = con.execute("SELECT COUNT(*) FROM procera_vs_edrs").fetchone()[0] 
        logger.info(f"Total processed rows: {count}") 
 
        logger.info(f"Exporting procera_vs_edrs_{run_date}.csv ..") 
 
        con.execute(f"""COPY procera_vs_edrs TO '/project/PLATFORM_INTEGRITY/exports/procera_vs_edrs_{run_date}.csv' (HEADER, DELIMITER ',')""") 
        # logger.info(f"Inserting into PI.BSCS_IN_HLR_STATUS_MISMATCH table ..") 
        # ctl_file = prepare_ctl("bscs_in_hlr_status_mismatch", run_date) 
        # run_sqlldr("bscs_in_hlr_status_mismatch",run_date, ctl_file=ctl_file) 
        # logger.info(f"Insertion done") 
 
        # logger.info(f"Validating inserted data ..") 
        # validate_loaded_data( 
        # flow_name="bscs_in_hlr_status_mismatch", 
        # run_date=run_date, 
        # expected_rows=count, 
        # logger=logger) 
        # logger.info("Inserted data validated successfully.") 
 
        end_time = datetime.now() 
        logger.info(f"procera_vs_edrs_consum job finished") 
        logger.info(f"procera_vs_edrs_consum total duration: {end_time - start_time}") 
    except Exception as e: 
        logger.exception(f"procera_vs_edrs_consum failed for run_date= {run_date} | error= {str(e)}") 
        raise 
 
 
if __name__ == "__main__": 
    parser = argparse.ArgumentParser() 
    parser.add_argument("--run_date", required=True) 
    args = parser.parse_args() 
    export_procera_vs_edrs(run_date=args.run_date) 

TAKING inpiratiok from the above queries i want to create a duckdb script for the following query ok 

SELECT DISTINCT RUN_DATE, MSISDN,
CASE WHEN Billing_TYPE IS NULL
THEN '_N'
ELSE Billing_TYPE
END Billing_TYPE,
CASE WHEN BILLING_STATUS IS NULL
THEN '_N'
ELSE BILLING_STATUS
END BILLING_STATUS,
CASE WHEN charging_status IS NULL
THEN '_N'
ELSE charging_status
END charging_status,
CASE WHEN PCRF_STATUS IS NULL
THEN '_N'
ELSE PCRF_STATUS
END PCRF_STATUS,
CASE WHEN RADIUS_STATUS IS NULL
THEN '_N'
ELSE RADIUS_STATUS
END RADIUS_STATUS,
CASE WHEN (BILLING_TYPE IS NULL AND CHARGING_STATUS IS NULL) OR (BILLING_TYPE = 'PRE' AND CHARGING_STATUS IS NULL)
THEN '_N'
ELSE ADSL_STATUS
END ADSL_STATUS, Nopcrfservice_status
,
CASE WHEN (
PCRF_STATUS ='ACTIVE' AND RADIUS_STATUS='ACTIVE' AND (((charging_status in('SUSPENDED','DEACTIVE') or charging_status IS NULL)  AND Billing_TYPE ='PRE')
OR ((BILLING_STATUS in('SUSPENDED','DEACTIVE','NoAutoActivation')) AND Billing_TYPE='POST')
OR (BILLING_STATUS IS NULL AND charging_status IS NULL AND Billing_TYPE IS NULL )))THEN 'Revenue Loss'
WHEN RADIUS_STATUS='ACTIVE' AND ADSL_STATUS='DEACTIVE' AND  (PCRF_STATUS = 'ACTIVE' OR PCRF_STATUS IS NULL) THEN 'Revenue Loss'
WHEN RADIUS_STATUS='ACTIVE' AND (ADSL_STATUS='SUSPENDED' OR ADSL_STATUS IS NULL ) AND PCRF_STATUS IS NULL THEN 'No Impact'
WHEN RADIUS_STATUS='ACTIVE' AND (ADSL_STATUS='SUSPENDED' OR ADSL_STATUS IS NULL ) AND PCRF_STATUS ='ACTIVE' THEN 'Revenue Loss'
WHEN RADIUS_STATUS='ACTIVE' AND PCRF_STATUS ='NoPCRFService' AND (Nopcrfservice_status ='ACTIVE' or Nopcrfservice_status ='SUSPENDED') and ADSL_STATUS = 'ACTIVE' THEN 'Customer Experience'
WHEN RADIUS_STATUS='DEACTIVE' AND ADSL_STATUS='ACTIVE'  AND (PCRF_STATUS IN ('SUSPENDED','ON-HOLD') OR PCRF_STATUS IS NULL)  THEN 'Customer Experience'
WHEN RADIUS_STATUS IS NULL AND ADSL_STATUS='ACTIVE' AND  (PCRF_STATUS = 'ON-HOLD' OR PCRF_STATUS IS NULL) THEN 'Customer Experience'
WHEN RADIUS_STATUS='ACTIVE' AND ADSL_STATUS='ACTIVE' AND  (PCRF_STATUS = 'SUSPENDED' OR PCRF_STATUS IS NULL) THEN 'Customer Experience'
WHEN RADIUS_STATUS='ACTIVE' AND ADSL_STATUS='ACTIVE' AND   (PCRF_STATUS IN ('SUSPENDED','ON-HOLD') OR PCRF_STATUS IS NULL) THEN 'Customer Experience'
ELSE 'No Impact'
END IMPACT_CASE
FROM(
SELECT TO_DATE(COALESCE(Billing.RUN_DATE , TO_NUMBER(Charging.RUN_DATE), PCRF.RUN_DATE, RADIUS.RUN_DATE), 'YYYYMMDD') RUN_DATE, COALESCE(Billing.MSISDN, Charging.MSISDN, PCRF.MSISDN, RADIUS.MSISDN) MSISDN,
Billing.TYPE Billing_TYPE , case when upper(Billing.STATUS)='ACTIVE' and AUTO_ACTIVATION_FLAG =0 then 'NoAutoActivation' else upper(Billing.STATUS) end BILLING_STATUS,
upper(Charging.STATUS) charging_status,
Case when PCRF.SERVICE_NAME is Null and Customer_STATUS is not null then 'NoPCRFService' else UPPER(DECODE(Customer_STATUS,0,'DeActive',1,'Active',2,'Suspended',3,'On-Hold', Customer_STATUS))end PCRF_STATUS,
UPPER(DECODE(RADIUS.STATUS,'0','DeActive','1','Active','2','Suspended',RADIUS.STATUS)) RADIUS_STATUS,
CASE WHEN Billing.TYPE = 'PRE'
THEN upper(Charging.STATUS)
WHEN Billing.TYPE = 'POST'
THEN (case when upper(Billing.STATUS)='ACTIVE' and AUTO_ACTIVATION_FLAG =0 then 'NoAutoActivation' else upper(Billing.STATUS) end)
END ADSL_STATUS,
Case when PCRF.SERVICE_NAME is Null and Customer_STATUS is not NULL
THEN UPPER(DECODE(Customer_STATUS,0,'DeActive',1,'Active',2,'Suspended',3,'On-Hold', Customer_STATUS))
END Nopcrfservice_status
from RP7VFEPIDAT.PFM_ADSL_MSISDN ADSL
LEFT OUTER JOIN RP7VFEPIDAT.PFM_BSCS_ADSL_STG Billing ON ADSL.MSISDN = Billing.MSISDN
left outer join RP7VFEPIDAT.RD_T_IN_ADSL Charging on ADSL.MSISDN =Charging.MSISDN
left outer join (SELECT DISTINCT * FROM RP7VFEPIDAT.PFM_PCRF_STG_FILTERED) PCRF ON ADSL.MSISDN =PCRF.MSISDN
left outer join RP7VFEPIDAT.PFM_RADIUS_STG RADIUS on ADSL.MSISDN =RADIUS.MSISDN
where (Charging.PACKAGE_TYPE ='DSL Status' or Charging.PACKAGE_TYPE is null)
AND (RADIUS.STATUS IN ('1','2','0','3') OR RADIUS.STATUS IS NULL)
AND (BILLING.TMCODE_DES NOT LIKE '%SDSL%' OR BILLING.TMCODE_DES IS NULL)
AND (BILLING.MSISDN NOT LIKE '%CP%' OR BILLING.msisdn IS NULL)
AND ( BILLING.PRICE_GROUP NOT IN ('TEST PHONES',
                                    'Pre-paid Test SIM',
                                    'Test Individuals',
                                    'Dummy PG') OR BILLING.PRICE_GROUP IS NULL)
)
WHERE msisdn NOT IN (SELECT msisdn FROM (SELECT * FROM
(SELECT TO_DATE(COALESCE(Billing.RUN_DATE , TO_NUMBER(Charging.RUN_DATE), PCRF.RUN_DATE, RADIUS.RUN_DATE), 'YYYYMMDD') RUN_DATE, COALESCE(Billing.MSISDN, Charging.MSISDN, PCRF.MSISDN, RADIUS.MSISDN) MSISDN,
Billing.TYPE Billing_TYPE ,Billing.PRICE_GROUP,
case when upper(Billing.STATUS)='ACTIVE' and AUTO_ACTIVATION_FLAG =0 then 'NoAutoActivation' else upper(Billing.STATUS) end BILLING_STATUS,
upper(Charging.STATUS) charging_status ,
Case when PCRF.SERVICE_NAME is Null and Customer_STATUS is not null then 'NoPCRFService' else UPPER(DECODE(Customer_STATUS,0,'DeActive',1,'Active',2,'Suspended',3,'On-Hold', Customer_STATUS))end PCRF_STATUS,
UPPER(DECODE(RADIUS.STATUS,'0','DeActive','1','Active','2','Suspended',RADIUS.STATUS)) RADIUS_STATUS,
CASE WHEN Billing.TYPE = 'PRE'
THEN upper(Charging.STATUS)
WHEN Billing.TYPE = 'POST'
THEN (case when upper(Billing.STATUS)='ACTIVE' and AUTO_ACTIVATION_FLAG =0 then 'NoAutoActivation' else upper(Billing.STATUS) end)
END ADSL_STATUS,
Case when PCRF.SERVICE_NAME is Null and Customer_STATUS is not NULL
THEN UPPER(DECODE(Customer_STATUS,0,'DeActive',1,'Active',2,'Suspended',3,'On-Hold', Customer_STATUS))
END Nopcrfservice_status
from RP7VFEPIDAT.PFM_ADSL_MSISDN ADSL
LEFT OUTER JOIN RP7VFEPIDAT.PFM_BSCS_ADSL_STG Billing ON ADSL.MSISDN = Billing.MSISDN
left outer join RP7VFEPIDAT.RD_T_IN_ADSL Charging on ADSL.MSISDN =Charging.MSISDN
left outer join (SELECT DISTINCT * FROM RP7VFEPIDAT.PFM_PCRF_STG_FILTERED) PCRF ON ADSL.MSISDN =PCRF.MSISDN
left outer join RP7VFEPIDAT.PFM_RADIUS_STG RADIUS on ADSL.MSISDN =RADIUS.MSISDN
where (Charging.PACKAGE_TYPE ='DSL Status' or Charging.PACKAGE_TYPE is null)
AND (RADIUS.STATUS IN ('1','2','0','3') OR RADIUS.STATUS IS NULL)
AND (BILLING.TMCODE_DES NOT LIKE '%SDSL%' OR BILLING.TMCODE_DES IS NULL)
AND (BILLING.MSISDN NOT LIKE '%CP%' OR BILLING.msisdn IS NULL)
)
WHERE
( Billing_type IS null AND billing_status IS null AND CHARGING_status IS null AND pcrf_status = 'SUSPENDED' AND radius_status IS null )
OR ( Billing_type = 'PRE' AND billing_status= 'ACTIVE'  AND CHARGING_status = 'ACTIVE'  AND pcrf_status = 'ACTIVE'  AND radius_status = 'ACTIVE'  )
OR (Billing_type = 'PRE' AND billing_status= 'SUSPENDED'  AND CHARGING_status = 'SUSPENDED'  AND pcrf_status = 'SUSPENDED' AND radius_status = 'ACTIVE' )
OR (Billing_type = 'PRE' AND billing_status= 'SUSPENDED'  AND CHARGING_status IS null  AND pcrf_status IS null AND radius_status = 'ACTIVE' )
OR (Billing_type = 'PRE' AND billing_status= 'ACTIVE'  AND CHARGING_status IS NULL AND pcrf_status  IS NULL AND radius_status = 'DEACTIVE' )
OR (Billing_type = 'POST' AND billing_status= 'ACTIVE'  AND CHARGING_status IS NULL AND pcrf_status IS NULL AND radius_status = 'DEACTIVE' )
OR (Billing_type = 'POST' AND billing_status= 'ACTIVE' AND CHARGING_status IS NULL AND pcrf_status = 'ACTIVE' AND radius_status = 'ACTIVE')
OR (Billing_type IS null AND billing_status IS null AND CHARGING_status IS null AND pcrf_status IS null AND radius_status = 'DEACTIVE')
OR (Billing_type IS null AND billing_status IS null AND CHARGING_status IS null AND pcrf_status IS null AND radius_status is null)
));
COMMIT;
Exit;
END
-------------------------------------------------------------------------------------------------

#Run ALL STATUS MISMATCH Vs BSCS BASE Percentage Script
 cd /apps/raidprd/raid-prj/scripts
   ./ADSL_Status_Mismatch_Percentage.sh $1

if [ $? -ne 0 ]; then
    echo "ADSL_Status_Mismatch_Percentage.sh failed"
    exit 1
fi

exit 0

[V25OIbrahim15@DCHQRAIDAPP02-N ~]$ cd /apps/raidprd/raid-prj/scripts
[V25OIbrahim15@DCHQRAIDAPP02-N scripts]$ cat ADSL_Status_Mismatch_Percentage.sh

#!/bin/bash

file_name=/apps/raidprd/raid-prj/scripts/logs/ADSL_Status_Mismatch_Percentage.log
$ORACLE_HOME/bin/sqlplus -s > $file_name <<END
RP7VFEPIDAT/RP7VFEPIDAT@RAIDPRD
INSERT INTO RP7VFEPIDAT.ADSL_ALL_STATUS_MIS
WITH Total_Base AS
(
        SELECT to_date(run_date,'YYYYMMDD') Run_date, COUNT(DISTINCT MSISDN ) Total_Base
        FROM RP7VFEPIDAT.PFM_BSCS_ADSL
        WHERE run_date = $1
        GROUP BY run_date
),
All_Status_Mis AS
(
        SELECT RUN_DATE ,COUNT(DISTINCT MSISDN) Status_Mis_count
        FROM RP7VFEPIDAT.RD_T_ADSL_STATUS_RECON
        WHERE RUN_DATE = to_date($1, 'YYYYMMDD')
        AND IMPACT_CASES <> 'No Impact'
        GROUP BY run_date
)
SELECT B.RUN_DATE ,S.Status_Mis_count, B.Total_Base ,
(B.Total_Base - S.Status_Mis_count) AS VARIANCE,
ROUND((S.Status_Mis_count / B.Total_Base ) *100 ,2 ) AS Percentage
FROM Total_Base B JOIN All_Status_Mis S
        ON B.RUN_DATE = S.Run_Date;
COMMIT;
EXIT;



also make the below script first to export Truncate table pfm_adsl_msisdn;
Truncate table pfm_adsl_msisdn_dups;
COMMIT;

INSERT INTO PFM_ADSL_MSISDN
SELECT MSISDN,RUN_DATE FROM PFM_BSCS_ADSL_STG;

COMMIT;

INSERT INTO PFM_ADSL_MSISDN
SELECT MSISDN,TO_NUMBER(RUN_DATE) FROM RD_T_IN_ADSL;

COMMIT;

INSERT INTO PFM_ADSL_MSISDN
SELECT MSISDN,RUN_DATE FROM PFM_RADIUS_STG;

COMMIT;

INSERT INTO PFM_ADSL_MSISDN
SELECT MSISDN,RUN_DATE FROM PFM_PCRF_STG;

COMMIT;


INSERT INTO PFM_ADSL_MSISDN_DUPS
SELECT MSISDN,RUN_DATE FROM PFM_ADSL_MSISDN
GROUP BY MSISDN,RUN_DATE
HAVING count(1) > 1;

COMMIT;

DELETE FROM PFM_ADSL_MSISDN
WHERE MSISDN IN (
SELECT MSISDN FROM PFM_ADSL_MSISDN_DUPS);

COMMIT;


INSERT INTO PFM_ADSL_MSISDN
SELECT MSISDN,RUN_DATE FROM PFM_ADSL_MSISDN_DUPS;

COMMIT;


exec dbms_stats.gather_table_stats(ownname=>'RP7VFEPIDAT', tabname=>'PFM_ADSL_MSISDN', estimate_percent => 15, degree => 4, cascade => true, method_opt => 'FOR ALL COLUMNS SIZE AUTO');

EXIT;
  

