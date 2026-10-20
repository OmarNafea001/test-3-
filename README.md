WITH params AS (
    SELECT /*+ PARALLEL(f 8) */ TRUNC(MAX(f.RUN_DATE)) AS ref_date
    FROM   RP7VFEPIDAT.PFM_PCRF_CONSUM_FILTERED f
),

cycles AS (
    SELECT '01' AS bc, 31 AS e FROM dual UNION ALL
    SELECT '02',       15      FROM dual UNION ALL   -- 16 -> 15
    SELECT '04',        3      FROM dual UNION ALL
    SELECT '07',        6      FROM dual UNION ALL
    SELECT '09',       20      FROM dual UNION ALL   -- 21 -> 20 (please confirm)
    SELECT '11',       10      FROM dual UNION ALL
    SELECT '25',       24      FROM dual
),

cycle_cand AS (
    SELECT c.bc, c.e, p.ref_date,
           CASE WHEN c.e = 31
                THEN LAST_DAY(TRUNC(p.ref_date,'MM'))
                ELSE TRUNC(p.ref_date,'MM') + c.e - 1 END                 AS end_this_month,
           CASE WHEN c.e = 31
                THEN LAST_DAY(ADD_MONTHS(TRUNC(p.ref_date,'MM'),-1))
                ELSE ADD_MONTHS(TRUNC(p.ref_date,'MM'),-1) + c.e - 1 END  AS end_prev_month
    FROM   cycles c CROSS JOIN params p
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
        SELECT /*+ PARALLEL(b 8) */
               b.MSISDN, b.BILLCYCLE, b.PCRF_SERVICE_NAME_EXPECTED,
               b.TYPE, b.STATUS, b.PRICE_GROUP,
               ROW_NUMBER() OVER (PARTITION BY b.MSISDN ORDER BY b.RUN_DATE DESC) AS rn
        FROM   RP7VFEPIDAT.PFM_BSCS_ADSL b
    )
    WHERE rn = 1
    AND   TYPE   = 'POST'
    AND   STATUS = 'ACTIVE'
    AND   NVL(PRICE_GROUP,'x') NOT IN ('TEST PHONES','Pre-paid Test SIM',
                                       'Test Individuals','Dummy PG')
),

catalogue AS (
    -- Used only to compare against the main QUOTA_SUM
    SELECT PCRF_SERVICE_NAME,
           MAX(PCRF_QOUTA_VALUE_GB) AS catalogue_quota_gb
    FROM   RP7VFEPIDAT.RD_T_DSL_PRODUCT_CATALOGUE
    WHERE  PROD_TYPE_NAME IN ('Main Bundle','DSL Package')
    AND    UPPER(PCRF_TYPE) = 'POSTPAID'
    GROUP  BY PCRF_SERVICE_NAME
),

subs AS (
    SELECT /*+ MATERIALIZE */
           b.MSISDN, b.BILLCYCLE,
           w.cycle_start, w.cycle_end,
           c.catalogue_quota_gb
    FROM   billing b
    JOIN   cycle_win w    ON w.bc = b.BILLCYCLE
    LEFT JOIN catalogue c ON c.PCRF_SERVICE_NAME = b.PCRF_SERVICE_NAME_EXPECTED
),

/* ---------- Allocated quota from QUOTA_SUM (single pass) ---------- */
win_rows AS (
    SELECT /*+ PARALLEL(f 8) */
           s.MSISDN,
           f.RUN_DATE,
           f.QUOTA_SUM,
           CASE WHEN LOWER(f.SERVICE_NAME) LIKE '%double%quota%' THEN 1 ELSE 0 END AS is_double,
           -- last day each type (main / double) appears in the cycle
           MAX(f.RUN_DATE) OVER (PARTITION BY s.MSISDN,
                                 CASE WHEN LOWER(f.SERVICE_NAME) LIKE '%double%quota%'
                                      THEN 1 ELSE 0 END)                           AS last_dt
    FROM   subs s
    JOIN   RP7VFEPIDAT.PFM_PCRF_CONSUM_FILTERED f
           ON  f.MSISDN   = s.MSISDN
           AND f.RUN_DATE >= s.cycle_start
           AND f.RUN_DATE <  s.cycle_end + 1
    WHERE  f.RUN_DATE >= (SELECT min_start   FROM bounds)
    AND    f.RUN_DATE <  (SELECT max_end + 1 FROM bounds)
    AND    LOWER(f.SERVICE_NAME) NOT LIKE '%addon%'
    AND    LOWER(f.SERVICE_NAME) NOT LIKE '%throttling%'
),

quota_agg AS (
    SELECT MSISDN,
           MAX(CASE WHEN is_double = 0 THEN last_dt END)                          AS main_last_date,
           MAX(CASE WHEN is_double = 1 THEN last_dt END)                          AS double_last_date,
           SUM(CASE WHEN is_double = 0 AND RUN_DATE = last_dt THEN QUOTA_SUM END) / 1048576 AS main_quota_gb,
           SUM(CASE WHEN is_double = 1 AND RUN_DATE = last_dt THEN QUOTA_SUM END) / 1048576 AS double_quota_gb
    FROM   win_rows
    GROUP  BY MSISDN
),

/* ---------- EDR: consumption over the whole cycle ---------- */
edr_agg AS (
    SELECT /*+ PARALLEL(e 8) */
           s.MSISDN,
           SUM(e.QUOTA_USAGE) / 1048576 AS cons_edr_gb,
           COUNT(*)                     AS edr_sessions
    FROM   subs s
    JOIN   RP7VFEPIDAT.PFM_PCRF_EDRS_STG_CONSUM e
           ON  e.MSISDN = SUBSTR(s.MSISDN, 2)
           AND e.TIME  >= s.cycle_start
           AND e.TIME   < s.cycle_end + 1
    WHERE  e.TIME >= (SELECT min_start   FROM bounds)
    AND    e.TIME <  (SELECT max_end + 1 FROM bounds)
    AND    LOWER(e.SERVICE_NAME) NOT LIKE '%addon%'
    AND    LOWER(e.SERVICE_NAME) NOT LIKE '%throttling%'
    GROUP  BY s.MSISDN
),

calc AS (
    SELECT s.MSISDN, s.BILLCYCLE, s.cycle_start, s.cycle_end,
           s.catalogue_quota_gb,
           q.main_last_date,
           q.double_last_date,
           q.main_quota_gb,
           NVL(q.double_quota_gb,0)                              AS double_quota_gb,
           CASE WHEN q.double_last_date IS NOT NULL THEN 1 ELSE 0 END AS has_double,
           NVL(q.main_quota_gb,0) + NVL(q.double_quota_gb,0)     AS allocated_gb,
           NVL(e.cons_edr_gb,0)                                  AS cons_e,
           NVL(e.edr_sessions,0)                                 AS edr_sessions,
           CASE WHEN s.catalogue_quota_gb > 0 AND q.main_quota_gb IS NOT NULL
                THEN q.main_quota_gb / s.catalogue_quota_gb END  AS main_ratio
    FROM   subs s
    LEFT JOIN quota_agg q ON q.MSISDN = s.MSISDN
    LEFT JOIN edr_agg   e ON e.MSISDN = s.MSISDN
)

SELECT MSISDN,
       BILLCYCLE,
       cycle_start,
       cycle_end,
       TO_CHAR(cycle_end,'YYYY-MM')                                   AS cycle_month,
       TO_CHAR(cycle_end,'FMMonth YYYY','NLS_DATE_LANGUAGE=ENGLISH')  AS cycle_month_name,

       -- Allocation (all from QUOTA_SUM)
       main_last_date                                                 AS main_quota_date,
       ROUND(main_quota_gb, 2)                                        AS main_quota_gb,
       CASE WHEN has_double = 1 THEN 'Yes' ELSE 'No' END              AS double_quota,
       double_last_date                                               AS double_quota_date,
       ROUND(double_quota_gb, 2)                                      AS double_quota_gb,
       ROUND(allocated_gb, 2)                                         AS total_allocated_gb,

       -- Main quota vs catalogue
       ROUND(catalogue_quota_gb, 2)                                   AS catalogue_quota_gb,
       ROUND(main_ratio, 2)                                           AS main_vs_catalogue_ratio,
       CASE WHEN main_quota_gb IS NULL        THEN 'No PCRF data'
            WHEN catalogue_quota_gb IS NULL   THEN 'No catalogue match'
            WHEN ROUND(main_ratio) >= 1
             AND ABS(main_ratio - ROUND(main_ratio)) <= 0.15
                                              THEN TO_CHAR(ROUND(main_ratio))
            ELSE 'Not whole (' || TO_CHAR(ROUND(main_ratio,2)) || ')'
       END                                                            AS main_quota_renewals,

       -- Consumption (EDR) and variance
       edr_sessions,
       ROUND(cons_e, 2)                                               AS total_consumed_gb,
       ROUND(cons_e / NULLIF(allocated_gb,0) * 100, 2)                AS consumption_pct,
       ROUND(allocated_gb - cons_e, 2)                                AS variance_gb,
       ROUND((allocated_gb - cons_e) / NULLIF(allocated_gb,0) * 100, 2) AS variance_pct,

       CASE WHEN allocated_gb = 0       THEN 'No Data'
            WHEN cons_e >= allocated_gb THEN 'Finisher'
            ELSE 'Non-Finisher' END                                   AS finisher_status,
       CASE WHEN allocated_gb = 0       THEN 'No Data'
            WHEN cons_e >= allocated_gb THEN 'Finisher'
            WHEN cons_e / allocated_gb < 0.35 THEN 'Cat 1: < 35%'
            WHEN cons_e / allocated_gb < 0.50 THEN 'Cat 2: 35 - <50%'
            WHEN cons_e / allocated_gb < 0.75 THEN 'Cat 3: 50 - <75%'
            ELSE                                   'Cat 4: 75 - <100%'
       END                                                            AS consumption_category
FROM   calc
ORDER  BY BILLCYCLE, MSISDN;
