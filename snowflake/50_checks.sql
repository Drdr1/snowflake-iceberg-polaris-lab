-- 50 - Interop checks. The fingerprint query computes exactly what
-- lab/checks.py computes, so engines can be compared number by number:
--   python -m lab.compare lab.orders --snowflake "<row_count>,<min>,<max>,<sum>"
USE ROLE LAB_ANALYST;
USE WAREHOUSE LAB_WH;

-- Fingerprint of the Polaris table as Snowflake sees it.
SELECT COUNT(*)                          AS row_count,
       MIN(order_id)                     AS min_order_id,
       MAX(order_id)                     AS max_order_id,
       SUM(amount)::NUMBER(20,2)         AS amount_sum
FROM LAKEHOUSE_LAB.EXTERNAL_CATALOG.ORDERS;

-- Schema as Snowflake sees it after Spark's evolve step. Expect channel
-- present, status renamed to order_status, customer_id widened to BIGINT.
DESC ICEBERG TABLE LAKEHOUSE_LAB.EXTERNAL_CATALOG.ORDERS;

-- Old rows written before the ADD COLUMN must read as NULL, not fail.
SELECT COUNT_IF(channel IS NULL) AS rows_without_channel,
       COUNT_IF(channel IS NOT NULL) AS rows_with_channel
FROM LAKEHOUSE_LAB.EXTERNAL_CATALOG.ORDERS;

-- Partition pruning across specs (month spec, then day spec after evolve).
-- Run, open the query profile, record "Partitions scanned / total".
SELECT COUNT(*) FROM LAKEHOUSE_LAB.EXTERNAL_CATALOG.ORDERS
WHERE order_date = '2026-01-15';

-- Open question for FINDINGS.md: can Snowflake time-travel to a snapshot
-- that Spark committed? Try it and record the result either way.
-- (snapshot ids: in Spark, SELECT snapshot_id FROM polaris.lab.orders.snapshots)
-- SELECT COUNT(*) FROM LAKEHOUSE_LAB.EXTERNAL_CATALOG.ORDERS AT(SNAPSHOT => '<SNAPSHOT_ID>');
