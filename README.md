WITH params AS (
    SELECT TRUNC(MAX(RUN_DATE)) AS ref_date
    FROM   RP7VFEPIDAT.PFM_PCRF_CONSUM_FILTERED
),

cycles AS (
    -- bc = bill cycle, e = END day of cycle (31 = last day of month)
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
    -- Last completed cycle for each bill cycle
    SELECT bc,
           cycle_end,
           CASE WHEN e = 31 THEN TRUNC(cycle_end,'MM')
                ELSE ADD_MONTHS(cycle_end,-1) + 1 END AS cycle_start
    FROM (
        SELECT bc, e,
               CASE WHEN end_this_month <= ref_date
                    THEN end_this_month ELSE end_prev_month END AS cycle_end
        FROM cycle_cand
    )
),

billing AS (
    SELECT MSISDN, BILLCYCLE, PCRF_SERVICE_NAME_EXPECTED
    FROM (
        SELECT b.MSISDN, b.BILLCYCLE, b.PCRF_SERVICE_NAME_EXPECTED,
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
    SELECT PCRF_SERVICE_NAME,
           MAX(PCRF_QOUTA_VALUE_GB) AS main_quota_gb
    FROM   RP7VFEPIDAT.RD_T_DSL_PRODUCT_CATALOGUE
    WHERE  PROD_TYPE_NAME IN ('Main Bundle','DSL Package')
	and Upper(PCRF_TYPE) = 'POSTPAID'
    GROUP  BY PCRF_SERVICE_NAME
),

subs AS (
    SELECT b.MSISDN, b.BILLCYCLE,
           w.cycle_start, w.cycle_end,
           c.main_quota_gb
    FROM   billing b
    JOIN   catalogue c ON c.PCRF_SERVICE_NAME = b.PCRF_SERVICE_NAME_EXPECTED
    JOIN   cycle_win w ON w.bc = b.BILLCYCLE
),

win_rows AS (
    -- All main/double rows inside the subscriber's cycle window
    SELECT s.MSISDN,
           f.RUN_DATE,
           f.QUOTA_CONSUMPTION,
           CASE WHEN UPPER(f.SERVICE_NAME) LIKE '%DOUBLEQUOTA%'
                THEN 'DOUBLE' ELSE 'MAIN' END AS quota_type
    FROM   subs s
    JOIN   RP7VFEPIDAT.PFM_PCRF_CONSUM_FILTERED f
           ON  f.MSISDN   = s.MSISDN
           AND f.RUN_DATE BETWEEN s.cycle_start AND s.cycle_end
    WHERE  UPPER(f.SERVICE_NAME) NOT LIKE '%THROTTLING%'
	AND  UPPER(f.SERVICE_NAME) NOT LIKE '%ADDON%'
),

snap AS (
    -- One snapshot date per MSISDN (latest day on/before cycle end)
    -- and double flag = double quota present on ANY day of the cycle
    SELECT MSISDN,
           MAX(RUN_DATE)                                        AS snap_date,
           MAX(CASE WHEN quota_type = 'DOUBLE' THEN 1 ELSE 0 END) AS has_double
    FROM   win_rows
    GROUP  BY MSISDN
),

usage_agg AS (
    -- Consumption of ALL rows (main + double) on the snapshot date only
    SELECT w.MSISDN,
           SUM(w.QUOTA_CONSUMPTION) / 1048576 AS consumed_gb      -- KB -> GB
    FROM   win_rows w
    JOIN   snap s ON s.MSISDN = w.MSISDN AND s.snap_date = w.RUN_DATE
    GROUP  BY w.MSISDN
),

calc AS (
    SELECT s.MSISDN, s.BILLCYCLE, s.cycle_start, s.cycle_end,
           s.main_quota_gb,
           sn.snap_date,
           NVL(sn.has_double,0)                            AS has_double,
           s.main_quota_gb * (1 + NVL(sn.has_double,0))    AS allocated_gb,
           NVL(u.consumed_gb,0)                            AS consumed_gb
    FROM   subs s
    LEFT JOIN snap       sn ON sn.MSISDN = s.MSISDN
    LEFT JOIN usage_agg  u  ON u.MSISDN  = s.MSISDN
)

SELECT MSISDN,
       BILLCYCLE,
       cycle_start,
       cycle_end,
       snap_date                                                 AS snapshot_date,
       CASE WHEN snap_date = cycle_end THEN 'Yes' ELSE 'No' END  AS snapshot_is_cycle_end,
       ROUND(main_quota_gb, 2)                                   AS main_quota_gb,
       CASE WHEN has_double = 1 THEN 'Yes' ELSE 'No' END         AS double_quota,
       ROUND(allocated_gb, 2)                                    AS total_allocated_gb,
       ROUND(consumed_gb, 2)                                     AS total_consumed_gb,
       ROUND(consumed_gb / NULLIF(allocated_gb,0) * 100, 2)      AS consumption_pct,
       ROUND(allocated_gb - consumed_gb, 2)                      AS variance_gb,
       ROUND((allocated_gb - consumed_gb)
             / NULLIF(allocated_gb,0) * 100, 2)                  AS variance_pct,
       CASE WHEN consumed_gb >= allocated_gb THEN 'Finisher'
            ELSE 'Non-Finisher' END                              AS finisher_status,
       CASE
            WHEN consumed_gb >= allocated_gb                     THEN 'Finisher'
            WHEN consumed_gb / NULLIF(allocated_gb,0) < 0.35     THEN 'Cat 1: < 35%'
            WHEN consumed_gb / NULLIF(allocated_gb,0) < 0.50     THEN 'Cat 2: 35 - <50%'
            WHEN consumed_gb / NULLIF(allocated_gb,0) < 0.75     THEN 'Cat 3: 50 - <75%'
            ELSE                                                      'Cat 4: 75 - <100%'
       END                                                       AS consumption_category
FROM   calc
ORDER  BY BILLCYCLE, MSISDN;


















-------
there alot if negative variances also the double is nit appearing

my notes:   i will get the allocated quota from the quota_sum column from con_filtered tables as show and you have to ignore addons 
SELECT * FROM RP7VFEPIDAT.PFM_PCRF_CONSUM_FILTERED
WHERE RUN_DATE >= date'2026-08-25'
AND MSISDN ='20573417711'
and LOWER(SERVICE_NAME) NOT LIKE '%addon%'

as the last quota sum of the billing period id the right allocated quota 
MSISDN     |SERVICE_NAME         |RUN_DATE               |QUOTA_CONSUMPTION|QUOTA_INITIAL|QUOTA_BALANCE|QUOTA_SUM|EXPECTED_ORG_QUOTA|EXPECTED_THROT_QUOTA|EXPECTED_TAX_QUOTA|
-----------+---------------------+-----------------------+-----------------+-------------+-------------+---------+------------------+--------------------+------------------+
20573417711|Serv_DSL_Addon_50G_OD|2026-08-25 00:00:00.000|16193146         |52428800     |36235654     | 52428800|                50|                   0|                 0|
20573417711|Serv_ADSL_HS_30M_300G|2026-08-25 00:00:00.000|314578233        |314572800    |0            |314578233|               300|                   0|                 0|
20573417711|Serv_DSL_Addon_50G_OD|2026-08-26 00:00:00.000|32555987         |52428800     |19872813     | 52428800|                50|                   0|                 0|
20573417711|Serv_ADSL_HS_30M_300G|2026-08-26 00:00:00.000|314578233        |314572800    |0            |314578233|               300|                   0|                 0|
20573417711|Serv_DSL_Addon_50G_OD|2026-08-27 00:00:00.000|48387142         |52428800     |4041658      | 52428800|                50|                   0|                 0|
20573417711|Serv_ADSL_HS_30M_300G|2026-08-27 00:00:00.000|314578233        |314572800    |0            |314578233|               300|                   0|                 0|
20573417711|Serv_DSL_Addon_50G_OD|2026-08-28 00:00:00.000|52436625         |52428800     |0            | 52436625|                50|                   0|                 0|
20573417711|Serv_ADSL_HS_30M_300G|2026-08-28 00:00:00.000|314578233        |314572800    |0            |314578233|               300|                   0|                 0|


and i want to adjust it to get the quota  consumption based on the pcrf_edrs total as shown  

SELECT SUM(QUOTA_USAGE)/1024/1024 FROM   RP7VFEPIDAT.PFM_PCRF_EDRS_STG_CONSUM 
WHERE MSISDN ='0573417711'
AND TIME >= DATE '2026-09-7' AND TIME < DATE'2026-10-06'
AND LOWER(SERVICE_NAME) NOT LIKE '%addon%'


--------------------------------------------------------------------------------

let me test a double quota case and tell you what or  how to handle it? 
it is exists from the aboe as well but there is a negative values as well 
ok then in quota consum filtered table quotasum : 
has the quota value seperate if it is addons or double or something like that
so for the double i wantt to get the allocated as following i want it to be like i want to check the double if he has double
just get its value from quotasum  and go to the last day of the bill cycle and get the quota_sum and sum All

do u understand tell me what u do anderstand and what edits you'll do 
so now you'll get the quota from quota sum ok not catalouge 


also use parralel in all to make the query fast 

a sample for bill cycle 7 MSISDN     |SERVICE_NAME                   |RUN_DATE               |QUOTA_CONSUMPTION|QUOTA_INITIAL|QUOTA_BALANCE|QUOTA_SUM|EXPECTED_ORG_QUOTA|EXPECTED_THROT_QUOTA|EXPECTED_TAX_QUOTA|
-----------+-------------------------------+-----------------------+-----------------+-------------+-------------+---------+------------------+--------------------+------------------+
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-08-25 00:00:00.000|79799403         |209715200    |129915797    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-08-25 00:00:00.000|209720198        |209715200    |0            |209720198|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-08-26 00:00:00.000|104980059        |209715200    |104735141    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-08-26 00:00:00.000|209720198        |209715200    |0            |209720198|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-08-27 00:00:00.000|125967475        |209715200    |83747725     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-08-27 00:00:00.000|209720198        |209715200    |0            |209720198|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-08-28 00:00:00.000|125967475        |209715200    |83747725     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-08-28 00:00:00.000|209720198        |209715200    |0            |209720198|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-08-29 00:00:00.000|125967475        |209715200    |83747725     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-08-29 00:00:00.000|209720198        |209715200    |0            |209720198|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-08-30 00:00:00.000|125967475        |209715200    |83747725     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-08-30 00:00:00.000|209720198        |209715200    |0            |209720198|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-08-31 00:00:00.000|155360057        |209715200    |54355143     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-08-31 00:00:00.000|209720198        |209715200    |0            |209720198|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-01 00:00:00.000|188943642        |209715200    |20771558     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-01 00:00:00.000|209720198        |209715200    |0            |209720198|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-02 00:00:00.000|209721920        |209715200    |0            |209721920|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-02 00:00:00.000|209938401        |209715200    |209496997    |419435398|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-03 00:00:00.000|209721920        |209715200    |0            |209721920|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-03 00:00:00.000|235138692        |209715200    |184296706    |419435398|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-04 00:00:00.000|209721920        |209715200    |0            |209721920|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-04 00:00:00.000|251932587        |209715200    |167502811    |419435398|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-05 00:00:00.000|209721920        |209715200    |0            |209721920|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-05 00:00:00.000|256130364        |209715200    |163305034    |419435398|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-06 00:00:00.000|209721920        |209715200    |0            |209721920|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-06 00:00:00.000|256130364        |209715200    |163305034    |419435398|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-07 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-07 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-08 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-08 00:00:00.000|21000421         |209715200    |188714779    |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-09 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-09 00:00:00.000|46198303         |209715200    |163516897    |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-10 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-10 00:00:00.000|66388350         |209715200    |143326850    |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-11 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-11 00:00:00.000|83188932         |209715200    |126526268    |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-12 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-12 00:00:00.000|83188932         |209715200    |126526268    |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-13 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-13 00:00:00.000|83188932         |209715200    |126526268    |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-14 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-14 00:00:00.000|113256135        |209715200    |96459065     |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-15 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-15 00:00:00.000|133214934        |209715200    |76500266     |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-16 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-16 00:00:00.000|174086159        |209715200    |35629041     |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-17 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-17 00:00:00.000|186687991        |209715200    |23027209     |209715200|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-18 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-18 00:00:00.000|209724031        |209715200    |0            |209724031|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-19 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-19 00:00:00.000|209724031        |209715200    |0            |209724031|               200|                   0|                 0|
20237767062|Serv_DSL_DoubleQuota_200GB_POST|2026-09-20 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-20 00:00:00.000|209724031        |209715200    |0            |209724031|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-21 00:00:00.000|209724031        |209715200    |0            |209724031|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-22 00:00:00.000|232995791        |209715200    |186443440    |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-23 00:00:00.000|253995060        |209715200    |165444171    |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-24 00:00:00.000|279171390        |209715200    |140267841    |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-25 00:00:00.000|314588108        |209715200    |104851123    |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-26 00:00:00.000|314588108        |209715200    |104851123    |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-27 00:00:00.000|314588108        |209715200    |104851123    |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-28 00:00:00.000|343992293        |209715200    |75446938     |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-29 00:00:00.000|371216659        |209715200    |48222572     |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-09-30 00:00:00.000|392203310        |209715200    |27235921     |419439231|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-01 00:00:00.000|419446339        |209715200    |0            |419446339|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-02 00:00:00.000|436227134        |209715200    |192934405    |629161539|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-03 00:00:00.000|436227134        |209715200    |192934405    |629161539|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-04 00:00:00.000|436227134        |209715200    |192934405    |629161539|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-05 00:00:00.000|474026708        |209715200    |155134831    |629161539|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-06 00:00:00.000|503417331        |209715200    |125744208    |629161539|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-07 00:00:00.000|0                |209715200    |209715200    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-08 00:00:00.000|21446589         |209715200    |188268611    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-09 00:00:00.000|21446589         |209715200    |188268611    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-10 00:00:00.000|21446589         |209715200    |188268611    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-11 00:00:00.000|21446589         |209715200    |188268611    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-12 00:00:00.000|50859790         |209715200    |158855410    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-13 00:00:00.000|71863404         |209715200    |137851796    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-14 00:00:00.000|97062413         |209715200    |112652787    |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-15 00:00:00.000|117462487        |209715200    |92252713     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-16 00:00:00.000|142670092        |209715200    |67045108     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-17 00:00:00.000|142670092        |209715200    |67045108     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-18 00:00:00.000|142670092        |209715200    |67045108     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-19 00:00:00.000|184986401        |209715200    |24728799     |209715200|               200|                   0|                 0|
20237767062|Serv_ADSL_HS_30M_180G          |2026-10-20 00:00:00.000|201787961        |209715200    |7927239      |209715200|               200|                   0|                 0|
----------------------------------------------------------------
also i want to add a flage to compare quota sum with quota from catalouge and tell me if he double or triple it



