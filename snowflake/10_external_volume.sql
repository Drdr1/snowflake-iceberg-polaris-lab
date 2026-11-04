-- 10 - External volume: how Snowflake reaches the S3 bucket.
--
-- Two-step handshake with AWS IAM (see docs/RUNBOOK.md step 2):
--   1. Create the volume with your role ARN and an external ID you choose.
--   2. DESC it, copy STORAGE_AWS_IAM_USER_ARN into the role's trust policy
--      (aws/snowflake-trust-policy.template.json).
--   3. Verify.
--
-- Replace: <BUCKET>, <AWS_ACCOUNT_ID>, <REGION>
USE ROLE ICEBERG_ADMIN;

CREATE EXTERNAL VOLUME IF NOT EXISTS LAB_S3_VOLUME
  STORAGE_LOCATIONS = (
    (
      NAME = 'lab-s3'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://<BUCKET>/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/snowflake-iceberg-lab'
      STORAGE_AWS_EXTERNAL_ID = 'iceberg_lab_external_id'
    )
  )
  -- Snowflake-managed tables write here; external-catalog tables only read.
  ALLOW_WRITES = TRUE;

-- Copy STORAGE_AWS_IAM_USER_ARN from the STORAGE_LOCATION_1 row into the
-- trust policy, then come back and run the verify call.
DESC EXTERNAL VOLUME LAB_S3_VOLUME;

-- Writes, reads, lists and deletes a test file. Fix IAM until this is "success".
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('LAB_S3_VOLUME');

GRANT USAGE ON EXTERNAL VOLUME LAB_S3_VOLUME TO ROLE LAB_ENGINEER;
