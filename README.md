cat /apps/raidprd/raid-prj/scripts/PI_INSERT_PCRF_QUOTA_CONSUMPTION_MISMATCH.sh
#!/bin/bash

file_name=/apps/raidprd/raid-prj/scripts/logs/PI_INSERT_QUOTA_CONSUMPTION_MISMATCH.log

$ORACLE_HOME/bin/sqlplus -s > $file_name <<END
RP7VFEPIDAT/RP7VFEPIDAT@RAIDPRD
INSERT INTO RD_T_PCRF_QUOTA_CONSUM_CASES
SELECT * FROM(
SELECT DISTINCT to_date(CONSUM.RUN_DATE,'YYYYMMDD'),
CONSUM.msisdn ,
CONSUM.SERVICE_NAME AS SERVICE_NAME,
ROUND(quota_sum/1024/1024,1) QUOTA_SUM_GB , EXPECTED_ORG_QUOTA , EXPECTED_TAX_QUOTA,
CASE WHEN ROUND(QUOTA_SUM/1024/1024,1) = EXPECTED_ORG_QUOTA
THEN 0
WHEN ROUND(QUOTA_SUM/1024/1024,1) = (EXPECTED_ORG_QUOTA + EXPECTED_TAX_QUOTA) AND PCRF_TYPE like '%PREPAID%' AND PROD_TYPE_NAME = 'Main Bundle'
THEN 0
WHEN ROUND(QUOTA_SUM/1024/1024,1) != (EXPECTED_ORG_QUOTA + EXPECTED_TAX_QUOTA) AND PCRF_TYPE like '%PREPAID%' AND PROD_TYPE_NAME = 'Main Bundle'
THEN (ROUND(QUOTA_SUM/1024/1024,1) - (EXPECTED_ORG_QUOTA+EXPECTED_TAX_QUOTA))
ELSE (ROUND(QUOTA_SUM/1024/1024,1) - (EXPECTED_ORG_QUOTA))
END "VARIANCE",
CASE WHEN PCRF_TYPE IS NULL THEN 'NA' ELSE PCRF_TYPE END AS PCRF_TYPE ,
case when Expected_Org_Quota ='0' and Expected_Tax_Quota='0'  and Expected_Throt_Quota='0' then 'EXCLUDED'
else 'INCLUDED' end as LOOKUP_INCLUSION,
CASE WHEN (SERVICE_NAME = 'Serv_DSL_Double_Quota_Pre'
AND Main_Double_Sum.Main_Double_Quota_Sum < (Main_Double_Sum.Main_Quota_sum*2)) THEN 'Customer Experience'
WHEN (SERVICE_NAME = 'Serv_DSL_Double_Quota_Pre' AND Main_Double_Sum.Main_Double_Quota_Sum > (Main_Double_Sum.Main_Quota_sum*2))
THEN 'Revenue Loss'
ELSE 'NOT Double Quota Case'
END Double_Quota_Cases
FROM RP7VFEPIDAT.PFM_PCRF_STG_CONSUM_FILTERED consum
LEFT OUTER join (SELECT pcrf_service_name, PCRF_TYPE, PROD_TYPE_NAME FROM RP7VFEPIDAT.RD_T_DSL_PRODUCT_CATALOGUE) catalogue
ON consum.service_name = catalogue.PCRF_SERVICE_NAME
FULL outer JOIN (SELECT doub_main.MSISDN AS MSISDN ,doub_main.Main_Double_Quota_Sum , main_only.Main_Quota_sum FROM
(SELECT MSISDN ,round(sum(QUOTA_SUM)/1024/1024,1) AS Main_Double_Quota_Sum , count(*) FROM RP7VFEPIDAT.PFM_PCRF_STG_CONSUM_FILTERED consum
LEFT OUTER JOIN (SELECT DISTINCT pcrf_service_name, PCRF_TYPE, PROD_TYPE_NAME FROM RP7VFEPIDAT.RD_T_DSL_PRODUCT_CATALOGUE) catalogue
ON consum.service_name = catalogue.PCRF_SERVICE_NAME
WHERE (SERVICE_NAME = 'Serv_DSL_Double_Quota_Pre' or prod_type_name ='Main Bundle')
GROUP BY MSISDN
HAVING count(*)=2) doub_main
FULL OUTER JOIN
(SELECT msisdn , service_name , round(QUOTA_SUM/1024/1024,1) AS Main_Quota_sum FROM RP7VFEPIDAT.PFM_PCRF_STG_CONSUM_FILTERED con
JOIN (SELECT DISTINCT pcrf_service_name, PCRF_TYPE, PROD_TYPE_NAME FROM RP7VFEPIDAT.RD_T_DSL_PRODUCT_CATALOGUE) cat
ON con.service_name = cat.PCRF_SERVICE_NAME
WHERE prod_type_name ='Main Bundle') main_only
ON doub_main.msisdn = main_only.msisdn) Main_Double_Sum
ON consum.msisdn =  Main_Double_Sum.MSISDN
WHERE consum.msisdn IN (SELECT msisdn FROM RP7VFEPIDAT.PFM_PCRF_STG_FILTERED WHERE customer_status = 1))
WHERE "VARIANCE" != 0
AND service_name NOT LIKE  '%Thrott%'
AND service_name NOT like '%Double%'
AND service_name NOT LIKE '%Gift%'
AND service_name NOT LIKE '%Promo%'
AND QUOTA_SUM_GB NOT IN (EXPECTED_ORG_QUOTA*2 ,EXPECTED_ORG_QUOTA*3,EXPECTED_ORG_QUOTA*4,EXPECTED_ORG_QUOTA*5) --Repurchase Normal Cases
AND NOT (SERVICE_NAME = 'Serv_DSL_Double_Quota_Pre' AND
msisdn IN (SELECT doub_main.MSISDN FROM
(SELECT MSISDN ,round(sum(QUOTA_SUM)/1024/1024,1) AS tot_q , count(*) FROM RP7VFEPIDAT.PFM_PCRF_STG_CONSUM_FILTERED consum
JOIN (SELECT DISTINCT pcrf_service_name, PCRF_TYPE, PROD_TYPE_NAME FROM RP7VFEPIDAT.RD_T_DSL_PRODUCT_CATALOGUE) catalogue
ON consum.service_name = catalogue.PCRF_SERVICE_NAME
WHERE (SERVICE_NAME = 'Serv_DSL_Double_Quota_Pre' or prod_type_name ='Main Bundle')
GROUP BY MSISDN
HAVING count(*)=2) doub_main
FULL OUTER JOIN
(SELECT msisdn , service_name , round(QUOTA_SUM/1024/1024,1) AS m_qo FROM RP7VFEPIDAT.PFM_PCRF_STG_CONSUM_FILTERED con
JOIN (SELECT DISTINCT pcrf_service_name, PCRF_TYPE, PROD_TYPE_NAME FROM RP7VFEPIDAT.RD_T_DSL_PRODUCT_CATALOGUE) cat
ON con.service_name = cat.PCRF_SERVICE_NAME
WHERE prod_type_name ='Main Bundle') main_only
ON doub_main.msisdn = main_only.msisdn
where doub_main.tot_q = 2* main_only.m_qo));
COMMIT;
exit;
END


-------------------------------------------------------------------------------------------------

#Run PCRF_QUOTA_CONSUM_Count.sh Percentage Script
 cd /apps/raidprd/raid-prj/scripts
   ./PCRF_QUOTA_CONSUM_Count.sh $1

if [ $? -ne 0 ]; then
    echo "PCRF_QUOTA_CONSUM_Count.sh failed"
    exit 1
fi

exit 0
[v25oibrahim15@DCHQRAIDAPP02-N ~]$ cd /apps/raidprd/raid-prj/scripts
[v25oibrahim15@DCHQRAIDAPP02-N scripts]$ cat PCRF_QUOTA_CONSUM_Count.sh
#!/bin/bash

file_name=/apps/raidprd/raid-prj/scripts/logs/PCRF_QUOTA_CONSUM_Count.log

$ORACLE_HOME/bin/sqlplus -s > $file_name <<END
RP7VFEPIDAT/RP7VFEPIDAT@RAIDPRD
INSERT INTO RP7VFEPIDAT.PCRF_QUOTA_CONSUM_Count
WITH PCRF_Total_Base AS
(
    SELECT run_date, COUNT(DISTINCT MSISDN) AS PCRF_Total_Base
    FROM RP7VFEPIDAT.PFM_PCRF_FILTERED
    WHERE run_date = TO_DATE($1, 'YYYYMMDD')
    GROUP BY run_date
),
QUOTA_CONSUM AS
(
    SELECT RUN_DATE,
           CASE
               WHEN PCRF_TYPE IN ('PREPAID', 'PREPAID-POSTPAID')
                   THEN 'PREPAID'
               ELSE PCRF_TYPE
           END AS PCRF_TYPE_GROUP,
           COUNT(DISTINCT MSISDN) AS QUOTA_CONSUM_COUNT
    FROM RP7VFEPIDAT.RD_T_PCRF_QUOTA_CONSUM_CASES
    WHERE run_date = TO_DATE($1, 'YYYYMMDD')
    GROUP BY RUN_DATE,
             CASE
                 WHEN PCRF_TYPE IN ('PREPAID', 'PREPAID-POSTPAID')
                     THEN 'PREPAID'
                 ELSE PCRF_TYPE
             END
)
SELECT
    B.RUN_DATE,
    SUM(Q.QUOTA_CONSUM_COUNT) AS QUOTA_CONSUM_COUNT,
    B.PCRF_TOTAL_BASE,
    (B.PCRF_TOTAL_BASE - SUM(Q.QUOTA_CONSUM_COUNT)) AS VARIANCE,
    ROUND((SUM(Q.QUOTA_CONSUM_COUNT) / B.PCRF_TOTAL_BASE) * 100, 3) AS PERCENTAGE,
    Q.PCRF_TYPE_GROUP AS PCRF_TYPE
FROM PCRF_Total_Base B
JOIN QUOTA_CONSUM Q
    ON B.RUN_DATE = Q.RUN_DATE
GROUP BY
    B.RUN_DATE,
    B.PCRF_TOTAL_BASE,
    Q.PCRF_TYPE_GROUP
ORDER BY Q.PCRF_TYPE_GROUP;

COMMIT;
EXIT;
