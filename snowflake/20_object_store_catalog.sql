-- 20 - Read a Spark-written table WITHOUT a live catalog connection.
--
-- OBJECT_STORE integrations point Snowflake at one metadata.json file.
-- Useful as a baseline and when the catalog is not reachable from Snowflake,
-- but nothing moves forward on its own: every new Spark commit needs a manual
-- REFRESH with the new metadata path. Compare with 21 (REST, auto-refresh).
--
-- Get the path with:   python -m lab.locate lab.orders
--   (prints the current metadata file relative to the bucket root)
USE ROLE ICEBERG_ADMIN;

CREATE CATALOG INTEGRATION IF NOT EXISTS LAB_OBJECT_STORE
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;
GRANT USAGE ON INTEGRATION LAB_OBJECT_STORE TO ROLE LAB_ENGINEER;

USE ROLE LAB_ENGINEER;
USE WAREHOUSE LAB_WH;
USE SCHEMA LAKEHOUSE_LAB.EXTERNAL_CATALOG;

CREATE ICEBERG TABLE IF NOT EXISTS ORDERS_PINNED
  EXTERNAL_VOLUME = 'LAB_S3_VOLUME'
  CATALOG = 'LAB_OBJECT_STORE'
  METADATA_FILE_PATH = '<PATH_FROM_lab.locate>';  -- e.g. lab_catalog/lab/orders/metadata/00004-....metadata.json

SELECT COUNT(*) FROM ORDERS_PINNED;

-- After Spark commits again (python -m spark.load_demo --step evolve):
-- ALTER ICEBERG TABLE ORDERS_PINNED REFRESH '<NEW_PATH_FROM_lab.locate>';
