-- ============================================================================
-- KMD WORKSHOP - PREREQUISITES
-- ============================================================================
-- Run this BEFORE deployment to set up required integrations
-- These require ACCOUNTADMIN and may need AWS configuration
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Storage Integration (requires AWS IAM role setup)
-- Update the STORAGE_AWS_ROLE_ARN and STORAGE_ALLOWED_LOCATIONS for your environment
CREATE STORAGE INTEGRATION IF NOT EXISTS KMD_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT:role/your-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket/data/');

-- Get the AWS IAM user and external ID for trust policy
DESC STORAGE INTEGRATION KMD_S3_INTEGRATION;

-- 2. Warehouse
CREATE WAREHOUSE IF NOT EXISTS KMD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
