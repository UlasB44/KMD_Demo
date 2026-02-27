/*
=============================================================================
KMD Denmark - External Stage Setup
Creates S3 external stages for data ingestion from AWS
=============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE KMD_STAGING;
USE SCHEMA EXTERNAL_STAGES;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SECTION 1: Create Storage Integration (requires ACCOUNTADMIN)
-- ============================================================================

-- Note: Run this section with ACCOUNTADMIN role
-- This creates a trust relationship between Snowflake and AWS S3

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION KMD_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT>:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://ubulut-iceberg-oregon/kmd/')
    COMMENT = 'S3 integration for KMD school data';

-- Get the AWS IAM user ARN and External ID for trust policy setup
DESC STORAGE INTEGRATION KMD_S3_INTEGRATION;

-- Grant usage to SYSADMIN
GRANT USAGE ON INTEGRATION KMD_S3_INTEGRATION TO ROLE SYSADMIN;

-- ============================================================================
-- SECTION 2: Create External Stages
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE KMD_STAGING;
USE SCHEMA EXTERNAL_STAGES;

-- Main external stage for all school data
CREATE OR REPLACE STAGE KMD_S3_STAGE
    URL = 's3://ubulut-iceberg-oregon/kmd/'
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for KMD school data from S3';

-- Alternative: Stage with direct credentials (for demo without integration)
-- Use this if storage integration is not set up
CREATE OR REPLACE STAGE KMD_S3_STAGE_DEMO
    URL = 's3://ubulut-iceberg-oregon/kmd/'
    CREDENTIALS = (
        AWS_KEY_ID = '${AWS_ACCESS_KEY_ID}'
        AWS_SECRET_KEY = '${AWS_SECRET_ACCESS_KEY}'
    )
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage with direct credentials (demo only)';

-- ============================================================================
-- SECTION 3: Create Internal Stages for Local Testing
-- ============================================================================

-- Internal stage for each municipality (for workshop exercises)
CREATE OR REPLACE STAGE COPENHAGEN_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for Copenhagen municipality data';

CREATE OR REPLACE STAGE AARHUS_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for Aarhus municipality data';

CREATE OR REPLACE STAGE ODENSE_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for Odense municipality data';

CREATE OR REPLACE STAGE AALBORG_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for Aalborg municipality data';

CREATE OR REPLACE STAGE ESBJERG_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for Esbjerg municipality data';

-- Combined data stage
CREATE OR REPLACE STAGE COMBINED_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for combined/all municipality data';

-- ============================================================================
-- SECTION 4: List Stage Contents (for verification)
-- ============================================================================

-- List files in external stage
-- LIST @KMD_S3_STAGE;

-- List files in internal stages
-- LIST @COPENHAGEN_STAGE;

-- ============================================================================
-- SECTION 5: Stage Usage Examples
-- ============================================================================

/*
-- Upload files to internal stage using SnowSQL or Snowsight:
PUT file:///path/to/data/copenhagen/dim_schools.csv @COPENHAGEN_STAGE/schools/;
PUT file:///path/to/data/copenhagen/dim_students.csv @COPENHAGEN_STAGE/students/;

-- Or from Python using snowflake-connector-python:
import snowflake.connector
conn = snowflake.connector.connect(...)
cursor = conn.cursor()
cursor.execute("PUT file://./data/copenhagen/*.csv @COPENHAGEN_STAGE AUTO_COMPRESS=TRUE")

-- Copy data from stage to table:
COPY INTO KMD_STAGING.RAW.SCHOOLS_RAW
FROM @COPENHAGEN_STAGE/schools/
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';
*/

SELECT 'External stages created successfully!' AS status;

SHOW STAGES IN SCHEMA KMD_STAGING.EXTERNAL_STAGES;
