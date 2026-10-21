ALTER SESSION ENABLE PARALLEL QUERY;

WITH params AS (
    SELECT /*+ PARALLEL(f 32) */ TRUNC(MAX(f.RUN_DATE)) AS ref_date
    FROM   RP7VFEPIDAT.PFM_PCRF_CONSUM_FILTERED f
),

cycles AS (
    -- bc = bill cycle, e = END day of cycle (31 = last day of month -> LAST_DAY handles 28/29/30/31)
    SELECT '01' AS bc, 31 AS e FROM dual UNION ALL
    SELECT '02',       15      FROM dual UNION ALL   -- 16 -> 15
    SELECT '04',        3      FROM dual UNION ALL   -- 4  -> 3
    SELECT '07',        6      FROM dual UNION ALL   -- 7  -> 6
    SELECT '09',       20      FROM dual UNION ALL   -- 21 -> 20 (to be confirmed)
    SELECT '11',       10      FROM dual UNION ALL   -- 11 -> 10
    SELECT '25',       24      FROM dual             -- 25 -> 24
),

-- >>> CHOOSE THE BILL CYCLES TO RUN HERE <<<
cycle_sel AS (
    SELECT bc, e
    FROM   cycles
    WHERE  bc IN ('01')            -- e.g. ('01') or ('01','07','25')
),

cycle_cand AS (
    SELECT c.bc, c.e, p.ref_date,
           CASE WHEN c.e = 31
                THEN LAST_DAY(TRUNC(p.ref_date,'MM'))
                ELSE TRUNC(p.ref_date,'MM') + c.e - 1 END                 AS end_this_month,
           CASE WHEN c.e = 31
                THEN LAST_DAY(ADD_MONTHS(TRUNC(p.ref_date,'MM'),-1))
                ELSE ADD_MONTHS(TRUNC(p.ref_date,'MM'),-1) + c.e - 1 END  AS end_prev_month
    FROM   cycle_sel c CROSS JOIN params p
),

cycle_win AS (
    SELECT bc, cycle_end,
           CASE WHEN e = 31 THEN TRUNC(cycle_end,'MM')
                ELSE ADD_MONTHS(cycle_end,-1) + 1 END AS cycle_start
    FROM (
        SELECT bc, e,
               CASE WHEN end_this_month <= ref_date
                    THEN end_this_month ELSE end_prev_month END AS cycle_end
        FROM cycle_cand
    )
),

bounds AS (
    SELECT MIN(cycle_start) AS min_start, MAX(cycle_end) AS max_end
    FROM   cycle_win
),

billing AS (
    SELECT MSISDN, BILLCYCLE, PCRF_SERVICE_NAME_EXPECTED
    FROM (
        SELECT /*+ PARALLEL(b 32) */
               b.MSISDN, b.BILLCYCLE, b.PCRF_SERVICE_NAME_EXPECTED,
               b.TYPE, b.STATUS, b.PRICE_GROUP, b.TMCODE_DES,
               ROW_NUMBER() OVER (PARTITION BY b.MSISDN ORDER BY b.RUN_DATE DESC) AS rn
        FROM   RP7VFEPIDAT.PFM_BSCS_ADSL b
        WHERE  b.TYPE   = 'POST'
        AND    b.STATUS = 'ACTIVE'
    )
    WHERE rn = 1
    AND   BILLCYCLE IN (SELECT bc FROM cycle_sel)
    AND   NVL(PRICE_GROUP,'x') NOT IN ('TEST PHONES','Pre-paid Test SIM',
                                       'Test Individuals','Dummy PG')
    AND   UPPER(NVL(TMCODE_DES,'x')) NOT LIKE '%SDSL%'      -- exclude SDSL
),

-- postpaid Main Bundles only: used for catalogue quota AND as the list of valid PCRF services
catalogue AS (
    SELECT /*+ MATERIALIZE */
           UPPER(TRIM(PCRF_SERVICE_NAME)) AS svc_key,
           MAX(PCRF_QOUTA_VALUE_GB)       AS catalogue_quota_gb
    FROM   RP7VFEPIDAT.RD_T_DSL_PRODUCT_CATALOGUE
    WHERE  PROD_TYPE_NAME = 'Main Bundle'
    AND    UPPER(PCRF_TYPE) = 'POSTPAID'
    GROUP  BY UPPER(TRIM(PCRF_SERVICE_NAME))
),

subs AS (
    SELECT /*+ MATERIALIZE */
           b.MSISDN,
           SUBSTR(b.MSISDN, 2) AS edr_msisdn,
           b.BILLCYCLE,
           w.cycle_start, w.cycle_end,
           c.catalogue_quota_gb
    FROM   billing b
    JOIN   cycle_win w    ON w.bc = b.BILLCYCLE
    LEFT JOIN catalogue c ON c.svc_key = UPPER(TRIM(b.PCRF_SERVICE_NAME_EXPECTED))
),

win_rows AS (
    SELECT /*+ PARALLEL(f 32) USE_HASH(f) */
           s.MSISDN,
           f.SERVICE_NAME,
           f.RUN_DATE,
           f.QUOTA_SUM,
           CASE WHEN LOWER(f.SERVICE_NAME) LIKE '%double%quota%' THEN 1 ELSE 0 END AS is_double
    FROM   subs s
    JOIN   RP7VFEPIDAT.PFM_PCRF_CONSUM_FILTERED f
           ON  f.MSISDN   = s.MSISDN
           AND f.RUN_DATE >= s.cycle_start
           AND f.RUN_DATE <  s.cycle_end + 1
    WHERE  f.RUN_DATE >= (SELECT min_start   FROM bounds)
    AND    f.RUN_DATE <  (SELECT max_end + 1 FROM bounds)
    AND    LOWER(f.SERVICE_NAME) NOT LIKE '%addon%'
    AND    LOWER(f.SERVICE_NAME) NOT LIKE '%throttling%'
    -- validation: keep only services found in the postpaid Main Bundle catalogue
    -- (double quota is kept by name, since it is not a Main Bundle)
    AND   ( LOWER(f.SERVICE_NAME) LIKE '%double%quota%'
            OR UPPER(TRIM(f.SERVICE_NAME)) IN (SELECT svc_key FROM catalogue) )
),

-- last quota value of each service in the cycle (not rounded yet)
svc_last AS (
    SELECT MSISDN, SERVICE_NAME, is_double,
           MIN(RUN_DATE) AS first_dt,
           MAX(RUN_DATE) AS last_dt,
           MAX(QUOTA_SUM) KEEP (DENSE_RANK LAST ORDER BY RUN_DATE) / 1048576 AS svc_quota_gb
    FROM   win_rows
    GROUP  BY MSISDN, SERVICE_NAME, is_double
),

-- sum first, then round to whole GB
quota_agg AS (
    SELECT MSISDN,
           ROUND(SUM(CASE WHEN is_double = 0 THEN svc_quota_gb END))  AS main_quota_gb,
           COUNT(CASE WHEN is_double = 0 THEN 1 END)                  AS main_svc_cnt,
           ROUND(SUM(CASE WHEN is_double = 1 THEN svc_quota_gb END))  AS double_quota_gb,
           COUNT(CASE WHEN is_double = 1 THEN 1 END)                  AS double_svc_cnt,
           MAX(CASE WHEN is_double = 0 THEN last_dt END)              AS main_last_date,
           MAX(CASE WHEN is_double = 1 THEN last_dt END)              AS double_last_date,
           LISTAGG(CASE WHEN is_double = 0 THEN SERVICE_NAME END, ' > ')
               WITHIN GROUP (ORDER BY first_dt)                       AS main_services
    FROM   svc_last
    GROUP  BY MSISDN
),

edr_agg AS (
    SELECT /*+ PARALLEL(e 32) USE_HASH(e) */
           s.MSISDN,
           ROUND(SUM(e.QUOTA_USAGE) / 1048576) AS cons_edr_gb,
           COUNT(*)                            AS edr_sessions
    FROM   subs s
    JOIN   RP7VFEPIDAT.PFM_PCRF_EDRS_STG_CONSUM e
           ON  e.MSISDN = s.edr_msisdn
           AND e.TIME  >= s.cycle_start
           AND e.TIME   < s.cycle_end + 1
    WHERE  e.TIME >= (SELECT min_start   FROM bounds)
    AND    e.TIME <  (SELECT max_end + 1 FROM bounds)
    AND    LOWER(e.SERVICE_NAME) NOT LIKE '%addon%'
    AND    LOWER(e.SERVICE_NAME) NOT LIKE '%throttling%'
    AND    LOWER(e.SERVICE_NAME) NOT LIKE '%promo%'
    AND    LOWER(e.SERVICE_NAME) NOT LIKE '%gift%'
    GROUP  BY s.MSISDN
),

calc AS (
    SELECT s.MSISDN, s.BILLCYCLE, s.cycle_start, s.cycle_end,
           s.catalogue_quota_gb,
           q.main_last_date, q.double_last_date,
           q.main_services,
           NVL(q.main_svc_cnt,0)                                 AS main_svc_cnt,
           q.main_quota_gb,
           NVL(q.double_quota_gb,0)                              AS double_quota_gb,
           CASE WHEN NVL(q.double_svc_cnt,0) > 0 THEN 1 ELSE 0 END AS has_double,
           NVL(q.main_quota_gb,0) + NVL(q.double_quota_gb,0)     AS allocated_gb,
           NVL(e.cons_edr_gb,0)                                  AS cons_e,
           NVL(e.edr_sessions,0)                                 AS edr_sessions
    FROM   subs s
    LEFT JOIN quota_agg q ON q.MSISDN = s.MSISDN    -- keeps customers with no PCRF data
    LEFT JOIN edr_agg   e ON e.MSISDN = s.MSISDN
)

SELECT MSISDN,
       BILLCYCLE,
       cycle_start,
       cycle_end,
       TO_CHAR(cycle_end,'YYYY-MM')                                   AS cycle_month,
       TO_CHAR(cycle_end,'FMMonth YYYY','NLS_DATE_LANGUAGE=ENGLISH')  AS cycle_month_name,

       CASE WHEN main_svc_cnt > 1 THEN 'Yes' ELSE 'No' END            AS service_changed,
       main_svc_cnt                                                   AS main_services_count,
       main_services,

       main_last_date                                                 AS main_quota_date,
       main_quota_gb,
       CASE WHEN has_double = 1 THEN 'Yes' ELSE 'No' END              AS double_quota,
       double_last_date                                               AS double_quota_date,
       double_quota_gb,
       allocated_gb                                                   AS total_allocated_gb,

       catalogue_quota_gb,
       CASE WHEN catalogue_quota_gb > 0
            THEN TRUNC(main_quota_gb / catalogue_quota_gb) END        AS main_quota_renewals,

       edr_sessions,
       cons_e                                                         AS total_consumed_gb,
       ROUND(cons_e / NULLIF(allocated_gb,0) * 100, 2)                AS consumption_pct,
       allocated_gb - cons_e                                          AS variance_gb,
       ROUND((allocated_gb - cons_e) / NULLIF(allocated_gb,0) * 100, 2) AS variance_pct,

       CASE WHEN main_quota_gb IS NULL  THEN 'No PCRF data'
            WHEN cons_e >= allocated_gb THEN 'Finisher'
            ELSE 'Non-Finisher' END                                   AS finisher_status,
       CASE WHEN main_quota_gb IS NULL OR allocated_gb = 0
              OR cons_e >= allocated_gb THEN NULL
            WHEN cons_e / allocated_gb < 0.25 THEN 'CAT1'
            WHEN cons_e / allocated_gb < 0.50 THEN 'CAT2'
            WHEN cons_e / allocated_gb < 0.75 THEN 'CAT3'
            ELSE                                   'CAT4'
       END                                                            AS consumption_category
FROM   calc
ORDER  BY BILLCYCLE, MSISDN;
