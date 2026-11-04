-- 60 - Workload numbers for the Snowflake vs Spark/Glue comparison.
-- Tag every lab query so it can be pulled out of history afterwards.
USE ROLE ACCOUNTADMIN;  -- ACCOUNT_USAGE needs it (or IMPORTED PRIVILEGES)
USE WAREHOUSE LAB_WH;

ALTER SESSION SET QUERY_TAG = 'iceberg-lab:benchmark';

-- The benchmark queries: same SQL is in spark/benchmark.sql for Spark.
SELECT region, DATE_TRUNC('month', order_date) AS m, COUNT(*) AS n, SUM(amount) AS revenue
FROM LAKEHOUSE_LAB.EXTERNAL_CATALOG.ORDERS GROUP BY 1, 2 ORDER BY 1, 2;

SELECT customer_id, SUM(amount) AS spend
FROM LAKEHOUSE_LAB.EXTERNAL_CATALOG.ORDERS
WHERE order_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1 ORDER BY spend DESC LIMIT 20;

ALTER SESSION UNSET QUERY_TAG;

-- Per-query: time, bytes and partitions scanned (live, no latency).
SELECT query_id, LEFT(query_text, 60) AS q, total_elapsed_time / 1000 AS seconds,
       bytes_scanned, partitions_scanned, partitions_total
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(WAREHOUSE_NAME => 'LAB_WH'))
WHERE query_tag = 'iceberg-lab:benchmark'
ORDER BY start_time;

-- Credits used by the lab warehouse (ACCOUNT_USAGE lags up to ~3 hours).
SELECT DATE_TRUNC('hour', start_time) AS hour, SUM(credits_used) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'LAB_WH'
GROUP BY 1 ORDER BY 1;
