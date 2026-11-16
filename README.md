Postpaid ADSL Finisher KPI: How the Query Works
Nov 17, 2026 · @Omar
Overview
For every postpaid ADSL customer, the query compares the quota given in their last completed bill cycle with what they consumed, then labels them Finisher or Non-Finisher and puts non-finishers into CAT1 to CAT4.
It is one chain of CTEs (WITH name AS (...) blocks). Each block is a named temporary result that the next blocks read from, so the query can be read top to bottom, one step at a time.
 
query flow · 6 steps, A to F
Steps A to C decide whose cycle is measured and when; D and E measure allocated and consumed quota for those customers; F compares them.
Step A: Find each bill cycle's dates
The query always measures the last completed cycle of each bill cycle, counted back from a reference date.
1.	params sets the reference date ref_date = the latest RUN_DATE in PFM_PCRF_CONSUM_FILTERED. In the DuckDB job it is the --run_date parameter.
2.	cycles lists each bill cycle and the day its cycle ends. 31 means the last day of the month, whatever its length.
3.	cycle_sel is where you choose which cycles to run.
4.	cycle_cand works out two possible end dates per cycle: the end day in this month and in the previous month.
5.	cycle_win keeps this month's end date if it has already passed, otherwise last month's. The start is one month back plus one day; for BC01 it is the 1st of the month.
6.	bounds takes the earliest start and the latest end of all selected cycles. Later steps use it to filter the big tables first, so they scan only that period.
Example with ref_date = 2026-11-16:
Bill cycle	End day	End this month	Already passed?	Cycle measured
BC01	31 (month end)	30 Nov	No	01 Oct to 31 Oct
BC02	15	15 Nov	Yes	16 Oct to 15 Nov
BC04	3	3 Nov	Yes	04 Oct to 03 Nov
BC07	6	6 Nov	Yes	07 Oct to 06 Nov
BC09	20	20 Nov	No	21 Sep to 20 Oct
BC11	10	10 Nov	Yes	11 Oct to 10 Nov
BC25	24	24 Nov	No	25 Sep to 24 Oct
Here bounds = 21 Sep to 15 Nov, so about 56 days of data are needed.
The Oracle query also turns the dates into numbers like 20261016 (cs_num, ce_num), because RUN_DATE in PFM_BSCS_ADSL is a number. In DuckDB this is not needed: RUN_DATE is already a date.
Step B: Decide which customers count
A customer is measured only if they were postpaid and active in their own cycle, and never prepaid in it. The filters run in this order:
1.	bscs_post (billing layer 1): take the daily PFM_BSCS_ADSL rows where TYPE = 'POST', the MSISDN has no CP, and RUN_DATE is inside the window of that customer's own bill cycle. A BC02 customer is checked against the BC02 dates, a BC25 customer against the BC25 dates.
2.	billing: keep one row per MSISDN.
o	ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY RUN_DATE DESC) with rn = 1 keeps the customer's last row in the period.
o	MAX(CASE WHEN STATUS = 'ACTIVE' THEN 1 ELSE 0 END) OVER (PARTITION BY MSISDN) is 1 if the customer was ACTIVE on any day. Customers never active are dropped.
o	Test price groups (TEST PHONES, Pre-paid Test SIM, Test Individuals, Dummy PG) and SDSL tariffs are removed.
3.	bscs_pre + billing_ok (billing layer 2): if the MSISDN was TYPE = 'PRE' on any day of its own cycle, the whole customer is removed. Prepaid days after the cycle end are ignored.
NOT EXISTS is an anti-join: keep the customer only when no matching prepaid row exists. It is the safe choice here because it never duplicates rows and behaves correctly with NULLs.
Step C: Product catalogue and the customer list
catalogue reads RD_T_DSL_PRODUCT_CATALOGUE and keeps only PROD_TYPE_NAME = 'Main Bundle', without _GC_ services. It gives one row per service name (UPPER(TRIM(...))), with its quota in GB. It has two jobs:
•	give the catalogue quota, used later to count renewals, and
•	act as the list of valid PCRF services in Step D.
The condition UPPER(PCRF_TYPE) = 'POSTPAID' is commented out on purpose, because some postpaid services are listed as prepaid in the catalogue.
subs is the final customer list: one row per customer from billing_ok, with their bill cycle, cycle dates and catalogue quota. It joins the catalogue on PCRF_SERVICE_NAME_EXPECTED, the service the customer should have according to BSCS.
It also creates edr_msisdn = SUBSTR(MSISDN, 2): the EDR table stores numbers without the leading 2, so this column is used for the EDR join in Step E.
Step D: Allocated quota from PCRF
The allocated quota is the sum of each service's last quota value in the cycle, plus double quota, rounded to whole GB.
1.	win_rows takes each customer's rows from PFM_PCRF_CONSUM_FILTERED inside their cycle. It removes addon, throttling, promo and _GC_ services, and keeps only services in the Main Bundle catalogue, or double quota by name. Double quota rows get is_double = 1.
2.	svc_last keeps, for each customer and service, the QUOTA_SUM on the last day the service appears, divided by 1,048,576 (1024 × 1024) to turn KB into GB.
3.	quota_agg sums the main services and the double quota separately, rounds each to whole GB, counts the main services, and builds the service history text in date order with LISTAGG.
MAX(QUOTA_SUM) KEEP (DENSE_RANK LAST ORDER BY RUN_DATE) is Oracle for: take QUOTA_SUM from the latest RUN_DATE of the group, and the highest one if that day has several rows.
Example (invented values) for one customer in BC02, 16 Oct to 15 Nov:
Service	Days seen	Last quota	is_double
ADSL_100GB	16 Oct to 25 Oct	100 GB	0
ADSL_200GB	26 Oct to 15 Nov	200 GB	0
Double Quota	01 Nov to 15 Nov	200 GB	1
Result: main_quota_gb = 300, main_svc_cnt = 2 so service_changed = Yes, double_quota_gb = 200, total_allocated_gb = 500, main_services = ADSL_100GB > ADSL_200GB.
The query sums first and rounds after, so small fractions are not lost on each service.
Step E: Consumption from EDR
edr_agg sums every customer's QUOTA_USAGE in PFM_PCRF_EDRS_STG_CONSUM over their cycle, converts it to GB, rounds it, and counts the sessions.
•	The join uses edr_msisdn (MSISDN without the leading 2).
•	The time filter is TIME >= cycle_start AND TIME < cycle_end + 1, so the whole last day is included, up to 23:59:59.
•	It removes addon, throttling, promo, adn, gift and _GC_ services.
•	Unlike Step D, consumption is not checked against the catalogue: every remaining service counts.
Step F: Final columns, status and category
calc joins everything to subs with LEFT JOIN, so customers with no PCRF or EDR data still appear, and NVL(..., 0) turns their missing values into 0. The final SELECT then adds these columns:
Column	How it is calculated
cycle_month, cycle_month_name	From cycle_end, e.g. 2026-11 and November 2026
service_changed	Yes when more than 1 main service appeared in the cycle
main_quota_renewals	TRUNC(main_quota_gb / catalogue_quota_gb), e.g. 300 ÷ 100 = 3
total_allocated_gb	Main quota + double quota
consumption_pct	Consumed ÷ allocated × 100, with NULLIF so 0 allocated gives NULL, not an error
variance_gb, variance_pct	Allocated − consumed
 
finisher status and category · 4 checks, 5 statuses, 4 categories
The checks run top to bottom and stop at the first yes. Both values are rounded GB, so 99.6 GB consumed against 100 GB counts as a Finisher.
Things to check
1. The _GC_ filter is wider than intended. The comment says \ escapes _, but the query has no \ and no ESCAPE clause. In LIKE, _ means any single character, so '%_GC_%' matches any name with GC and at least one character on each side.
Service name	Excluded today?	Should it be?
ADSL_GC_100	Yes	Yes
BIGCAP_50 (IGCA matches _GC_)	Yes	No
The fix in Oracle is UPPER(SERVICE_NAME) NOT LIKE '%\_GC\_%' ESCAPE '\', in catalogue, win_rows and edr_agg. In DuckDB it is NOT contains(UPPER(SERVICE_NAME), '_GC_'). Fixing it may bring some services back, so compare the counts before and after. The DuckDB job keeps the current behaviour for now, so its results can be checked against Oracle first.
2. CAT1 cut-off. The query uses 25% (< 0.25). Earlier the cut-offs were 35%, 50% and 75%. One of them needs confirming.
3. Rounding before comparing. Allocated and consumed are rounded to whole GB before the Finisher check, so customers within 0.5 GB of their quota can land on either side.
Converting to DuckDB
The logic carries over almost line for line. The main change is the data: Oracle keeps history in tables, while the DuckDB job reads one file per day for every day of the period.
Oracle	DuckDB
/*+ PARALLEL ... */ hints	Removed: DuckDB runs in parallel by itself
SELECT '01', 31 FROM dual UNION ALL ...	VALUES ('01', 31), ('02', 15), ...
MAX(RUN_DATE) as reference date	The --run_date parameter
TRUNC(d, 'MM')	date_trunc('month', d)
LAST_DAY(d)	last_day(d)
ADD_MONTHS(d, -1)	d - INTERVAL 1 MONTH
TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))	Not needed: RUN_DATE is a date
NVL(x, 0)	COALESCE(x, 0)
MAX(x) KEEP (DENSE_RANK LAST ORDER BY d)	MAX(x) on the rows where d = the latest d (same tie rule)
LISTAGG(x, ' > ') WITHIN GROUP (ORDER BY d)	string_agg(x, ' > ' ORDER BY d)
TO_CHAR(d, 'YYYY-MM')	strftime(d, '%Y-%m')
TO_CHAR(d, 'FMMonth YYYY')	strftime(d, '%B %Y')
The job postpaid_quota_kpi.py runs these steps, each as its own table so you can inspect it:
1.	kpi_cycle_win: cycle dates from --run_date and --bill_cycles.
2.	kpi_bscs: the daily BSCS exports for the period.
3.	kpi_catalogue and kpi_subs: Steps B and C.
4.	kpi_win_rows: PCRF quota rows (Step D input).
5.	kpi_edr_agg: EDR consumption (Step E).
6.	postpaid_quota_kpi_data: Step F, then the CSV export and SQL*Loader into Oracle, as in the other jobs.
The job logs any day with no file, and stops if a source has no files at all.
To confirm before the first run:
•	☐ File paths for the three daily sources (BSCS export, pcrf_consum, pcrf_edrs)
•	☐ pcrf_consum is the same data as PFM_PCRF_CONSUM_FILTERED
•	☐ Daily files are kept for about two months
•	☐ Format of EDR_DATE in the EDR files
•	☐ Oracle target table and control file for postpaid_quota_kpi
