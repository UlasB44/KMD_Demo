-- ============================================================================
-- KMD WORKSHOP - SHARED PREREQUISITES
-- ============================================================================
-- Run ONCE per Snowflake account. Creates:
--   - Storage Integration for S3 access
--   - Shared file formats
--   - Warehouse
--
-- After running, configure S3 trust relationship with the IAM role ARN
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- WAREHOUSE
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS KMD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- ============================================================================
-- STORAGE INTEGRATION (S3 Access)
-- ============================================================================
CREATE OR REPLACE STORAGE INTEGRATION KMD_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::529088256134:role/kmd-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://ubulut-iceberg-oregon/');

-- Get the AWS IAM user ARN and External ID for trust relationship
DESC INTEGRATION KMD_S3_INTEGRATION;

-- ============================================================================
-- SHARED DATABASE FOR FILE FORMATS
-- ============================================================================
CREATE DATABASE IF NOT EXISTS KMD_SHARED;
CREATE SCHEMA IF NOT EXISTS KMD_SHARED.FORMATS;

USE SCHEMA KMD_SHARED.FORMATS;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE;

-- ============================================================================
-- GRANT USAGE TO PUBLIC (so all students can use)
-- ============================================================================
GRANT USAGE ON DATABASE KMD_SHARED TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA KMD_SHARED.FORMATS TO ROLE PUBLIC;
GRANT USAGE ON FILE FORMAT KMD_SHARED.FORMATS.CSV_FORMAT TO ROLE PUBLIC;
GRANT USAGE ON INTEGRATION KMD_S3_INTEGRATION TO ROLE PUBLIC;
GRANT USAGE ON WAREHOUSE KMD_WH TO ROLE PUBLIC;

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SHOW INTEGRATIONS LIKE 'KMD%';
SHOW FILE FORMATS IN SCHEMA KMD_SHARED.FORMATS;
