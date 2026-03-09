-- ============================================================================
-- STUDENT EXERCISE - STEP 3: SNOWPIPE (Auto-ingest from S3)
-- ============================================================================
-- Replace {MUNICIPALITY} with your assigned municipality (uppercase)
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE {MUNICIPALITY}_DB;
USE SCHEMA RAW;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SNOWPIPES (Auto-ingest when files land in S3)
-- ============================================================================
-- Pattern matches dated files: dim_students_20260310.csv, dim_students_20260311.csv

CREATE OR REPLACE PIPE STUDENTS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO STUDENTS_RAW
FROM @{MUNICIPALITY}_STAGE/
PATTERN = '.*dim_students_[0-9]+\.csv'
FILE_FORMAT = (FORMAT_NAME = 'KMD_SHARED.FORMATS.CSV_FORMAT');

CREATE OR REPLACE PIPE TEACHERS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO TEACHERS_RAW
FROM @{MUNICIPALITY}_STAGE/
PATTERN = '.*dim_teachers_[0-9]+\.csv'
FILE_FORMAT = (FORMAT_NAME = 'KMD_SHARED.FORMATS.CSV_FORMAT');

CREATE OR REPLACE PIPE CLASSES_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO CLASSES_RAW
FROM @{MUNICIPALITY}_STAGE/
PATTERN = '.*dim_classes_[0-9]+\.csv'
FILE_FORMAT = (FORMAT_NAME = 'KMD_SHARED.FORMATS.CSV_FORMAT');

-- ============================================================================
-- GET SQS ARN FOR S3 EVENT NOTIFICATION
-- ============================================================================
-- Copy the notification_channel ARN and configure S3 bucket event notification
SHOW PIPES;

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('{MUNICIPALITY}_DB.RAW.STUDENTS_PIPE');

-- ============================================================================
-- S3 EVENT NOTIFICATION SETUP (Manual Step in AWS Console)
-- ============================================================================
-- 1. Go to S3 Console → ubulut-iceberg-oregon → Properties
-- 2. Event Notifications → Create
-- 3. Prefix: data/{municipality}/
-- 4. Event types: s3:ObjectCreated:*
-- 5. Destination: SQS Queue
-- 6. SQS ARN: (paste the notification_channel from SHOW PIPES)
-- ============================================================================
