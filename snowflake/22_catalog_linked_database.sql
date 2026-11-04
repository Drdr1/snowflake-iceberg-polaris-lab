-- 22 - Catalog-linked database: Snowflake discovers every table in a Polaris
-- namespace on its own, instead of one CREATE ICEBERG TABLE per table.
--
-- Read-only on purpose (ALLOWED_WRITE_OPERATIONS = NONE), matching the
-- read-only Polaris principal. Writing back to Polaris from Snowflake is a
-- separate experiment: it needs a principal with TABLE_WRITE_DATA and
-- ALLOWED_WRITE_OPERATIONS = ALL. Note that DROP TABLE in a writable linked
-- database drops the table in Polaris too.
USE ROLE ICEBERG_ADMIN;

CREATE DATABASE IF NOT EXISTS POLARIS_LINKED
  LINKED_CATALOG = (
    CATALOG = 'LAB_POLARIS',
    ALLOWED_NAMESPACES = ('lab'),
    ALLOWED_WRITE_OPERATIONS = NONE,
    SYNC_INTERVAL_SECONDS = 60
  );

-- Discovery status: tables found, failures, last sync time.
SELECT SYSTEM$CATALOG_LINK_STATUS('POLARIS_LINKED');

SHOW SCHEMAS IN DATABASE POLARIS_LINKED;
SHOW ICEBERG TABLES IN DATABASE POLARIS_LINKED;

CREATE DATABASE ROLE IF NOT EXISTS POLARIS_LINKED.ANALYST;
GRANT USAGE ON SCHEMA POLARIS_LINKED."lab" TO DATABASE ROLE POLARIS_LINKED.ANALYST;
GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA POLARIS_LINKED."lab" TO DATABASE ROLE POLARIS_LINKED.ANALYST;
GRANT DATABASE ROLE POLARIS_LINKED.ANALYST TO ROLE LAB_ANALYST;

-- Polaris namespaces are lower-case, so quote them.
USE ROLE LAB_ANALYST;
USE WAREHOUSE LAB_WH;
SELECT COUNT(*) FROM POLARIS_LINKED."lab"."orders";
