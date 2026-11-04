-- 30 - The reverse direction: Snowflake owns the table, other engines read it.
--
-- CATALOG = 'SNOWFLAKE' means Snowflake writes data + metadata to S3 through
-- LAB_S3_VOLUME and is the only writer. Outside engines read the Iceberg
-- metadata Snowflake publishes (step at the bottom).
USE ROLE LAB_ENGINEER;
USE WAREHOUSE LAB_WH;
USE SCHEMA LAKEHOUSE_LAB.SNOWFLAKE_MANAGED;

CREATE ICEBERG TABLE IF NOT EXISTS ORDERS_SF (
    order_id    NUMBER(19,0) NOT NULL,
    customer_id NUMBER(10,0),
    region      STRING,
    status      STRING,
    amount      NUMBER(10,2),
    order_date  DATE,
    updated_at  TIMESTAMP_LTZ(6)
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'LAB_S3_VOLUME'
  BASE_LOCATION = 'snowflake_managed/orders_sf';

-- Same synthetic shape as lab/data.py: deterministic, no real data.
INSERT INTO ORDERS_SF
SELECT
    SEQ8() + 1                                          AS order_id,
    UNIFORM(1, 499, RANDOM(7))                          AS customer_id,
    ARRAY_CONSTRUCT('us-east','us-west','eu-central','ap-south')[UNIFORM(0, 3, RANDOM(8))]::STRING,
    ARRAY_CONSTRUCT('placed','paid','shipped','returned')[UNIFORM(0, 3, RANDOM(9))]::STRING,
    UNIFORM(100, 99999, RANDOM(10)) / 100               AS amount,
    DATEADD(day, UNIFORM(0, 59, RANDOM(11)), '2026-01-01'::DATE),
    CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

-- Schema evolution on a Snowflake-owned table.
ALTER ICEBERG TABLE ORDERS_SF ADD COLUMN channel STRING;
ALTER ICEBERG TABLE ORDERS_SF RENAME COLUMN status TO order_status;
INSERT INTO ORDERS_SF (order_id, region, order_status, amount, order_date, channel)
  VALUES (10001, 'eu-central', 'paid', 42.00, '2026-03-01', 'web');

-- Where Snowflake wrote the current Iceberg metadata. Feed metadataLocation to:
--   python -m lab.read_metadata s3://<BUCKET>/snowflake_managed/orders_sf/metadata/<...>.metadata.json
-- which reads the table with PyIceberg (no Snowflake involved) and prints
-- the same fingerprint as 50_checks.sql.
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('LAKEHOUSE_LAB.SNOWFLAKE_MANAGED.ORDERS_SF');

-- Maintenance is Snowflake's job for managed tables: it compacts and expires
-- snapshots itself. Spark must not run rewrite_data_files / expire_snapshots
-- against this table. Record in docs/FINDINGS.md what metadata files appear
-- in S3 over a day of inserts.
