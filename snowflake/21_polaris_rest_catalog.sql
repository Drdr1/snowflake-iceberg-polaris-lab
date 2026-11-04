-- 21 - Snowflake reads Polaris-managed tables through the Iceberg REST API.
--
-- Needs Polaris reachable over HTTPS from Snowflake (deploy/vps/, or any
-- public endpoint). Uses the READ-ONLY principal created by
-- polaris/bootstrap.sh, so Snowflake cannot change the catalog even by mistake.
--
-- Replace: <POLARIS_HOST>, <READER_CLIENT_ID>, <READER_CLIENT_SECRET>
-- (values are in .polaris-reader.env on the machine that ran bootstrap.sh)
USE ROLE ICEBERG_ADMIN;

CREATE CATALOG INTEGRATION IF NOT EXISTS LAB_POLARIS
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'lab'
  REST_CONFIG = (
    CATALOG_URI = 'https://<POLARIS_HOST>/api/catalog'
    CATALOG_NAME = 'lab_catalog'            -- the Polaris catalog ("warehouse")
    -- Polaris vends short-lived S3 credentials scoped to each table.
    -- Switch to EXTERNAL_VOLUME_CREDENTIALS to compare (needs LAB_S3_VOLUME).
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_TOKEN_URI = 'https://<POLARIS_HOST>/api/catalog/v1/oauth/tokens'
    OAUTH_CLIENT_ID = '<READER_CLIENT_ID>'
    OAUTH_CLIENT_SECRET = '<READER_CLIENT_SECRET>'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
  )
  REFRESH_INTERVAL_SECONDS = 60
  ENABLED = TRUE;

DESC CATALOG INTEGRATION LAB_POLARIS;
-- Connectivity + auth check (returns JSON with success / error detail):
SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('LAB_POLARIS');

GRANT USAGE ON INTEGRATION LAB_POLARIS TO ROLE LAB_ENGINEER;

USE ROLE LAB_ENGINEER;
USE WAREHOUSE LAB_WH;
USE SCHEMA LAKEHOUSE_LAB.EXTERNAL_CATALOG;

-- One table, explicitly registered. AUTO_REFRESH follows Spark's commits.
-- With vended credentials no EXTERNAL_VOLUME is needed.
CREATE ICEBERG TABLE IF NOT EXISTS ORDERS
  CATALOG = 'LAB_POLARIS'
  CATALOG_TABLE_NAME = 'orders'
  AUTO_REFRESH = TRUE;

SELECT COUNT(*) FROM ORDERS;

-- Is auto-refresh keeping up? lastSnapshotId should match Spark's latest.
SELECT SYSTEM$AUTO_REFRESH_STATUS('ORDERS');

-- Manual refresh if you do not want to wait for the interval:
-- ALTER ICEBERG TABLE ORDERS REFRESH;
