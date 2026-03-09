-- ============================================================================
-- SCHEMA EVOLUTION ERROR TESTING
-- ============================================================================
-- This guide demonstrates how Snowpipe handles schema changes and mismatches
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE ESBJERG_DB;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- TEST 1: COLUMN RENAMED (first_name → frist_name)
-- ============================================================================
-- File: 01_column_renamed_ERROR.csv
-- 
-- WHAT HAPPENS:
--   Snowpipe uses POSITIONAL mapping by default, NOT column names!
--   So this file will LOAD SUCCESSFULLY but data lands in WRONG columns.
--
-- The CSV header says: ...,frist_name,last_name,...
-- But Snowpipe ignores headers and loads by position (column 7, column 8, etc.)
-- Result: Data loads but "frist_name" value goes into first_name column anyway!
-- ============================================================================

-- Upload file to S3:
-- aws s3 cp 01_column_renamed_ERROR.csv s3://ubulut-iceberg-oregon/data/esbjerg/

-- Check if it loaded (it will succeed!)
SELECT FILE_NAME, STATUS, ROW_COUNT, FIRST_ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ESBJERG_DB.RAW.STUDENTS_RAW',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC LIMIT 5;

-- The danger: Data appears correct because position matched!
SELECT student_id, first_name, last_name 
FROM RAW.STUDENTS_RAW 
WHERE student_id LIKE 'STU-561-999%';

-- ============================================================================
-- WORKAROUND 1A: Use MATCH_BY_COLUMN_NAME (requires PARSE_HEADER)
-- ============================================================================
-- Create a file format that matches by column name:

CREATE OR REPLACE FILE FORMAT CSV_FORMAT_BY_NAME
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 0           -- Don't skip, we need to parse it
    PARSE_HEADER = TRUE       -- Use header row for column names
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '');

-- Now COPY would fail or put NULL in first_name (column doesn't exist)
-- COPY INTO test_table FROM @stage FILE_FORMAT = 'CSV_FORMAT_BY_NAME' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================================
-- TEST 2: DATATYPE MISMATCH (municipality_code: INTEGER gets VARCHAR)
-- ============================================================================
-- File: 02_datatype_mismatch_ERROR.csv
-- 
-- WHAT HAPPENS:
--   Snowpipe tries to cast 'THIS_IS_NOT_AN_INTEGER' to INTEGER
--   COPY fails with: "Numeric value 'THIS_IS_NOT_AN_INTEGER' is not recognized"
--   
-- BY DEFAULT: Entire file is rejected (ON_ERROR = 'ABORT_STATEMENT')
-- ============================================================================

-- Upload file to S3:
-- aws s3 cp 02_datatype_mismatch_ERROR.csv s3://ubulut-iceberg-oregon/data/esbjerg/

-- Check copy history - will show LOAD_FAILED
SELECT FILE_NAME, STATUS, ROW_COUNT, ERROR_COUNT, FIRST_ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ESBJERG_DB.RAW.STUDENTS_RAW',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOAD_FAILED'
ORDER BY LAST_LOAD_TIME DESC LIMIT 5;

-- ============================================================================
-- WORKAROUND 2A: ON_ERROR = 'CONTINUE' (skip bad rows)
-- ============================================================================
-- Recreate pipe with error handling:

/*
CREATE OR REPLACE PIPE STUDENTS_PIPE_SKIP_ERRORS
    AUTO_INGEST = TRUE
AS
COPY INTO RAW.STUDENTS_RAW
FROM @RAW.ESBJERG_STAGE/
PATTERN = '.*dim_students.*\.csv'
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT')
ON_ERROR = 'CONTINUE';  -- Skip bad rows, load good ones
*/

-- ============================================================================
-- WORKAROUND 2B: Load everything as VARCHAR (schema-on-read)
-- ============================================================================
-- Create a "landing" table where ALL columns are VARCHAR:

CREATE OR REPLACE TABLE RAW.STUDENTS_RAW_VARIANT (
    student_id VARCHAR,
    class_id VARCHAR,
    school_id VARCHAR,
    municipality_code VARCHAR,  -- VARCHAR not INTEGER!
    cpr_number VARCHAR,
    cpr_masked VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    birth_date VARCHAR,         -- VARCHAR not DATE!
    enrollment_date VARCHAR,
    guardian_name VARCHAR,
    guardian_phone VARCHAR,
    guardian_email VARCHAR,
    address VARCHAR,
    postal_code VARCHAR,
    special_needs VARCHAR,
    is_active VARCHAR,          -- VARCHAR not BOOLEAN!
    created_at VARCHAR,
    updated_at VARCHAR
);

-- Then use a VIEW or downstream TASK to cast with error handling:
/*
CREATE VIEW RAW.STUDENTS_RAW_TYPED AS
SELECT 
    student_id,
    TRY_CAST(municipality_code AS INTEGER) as municipality_code,
    TRY_TO_DATE(birth_date) as birth_date,
    TRY_TO_BOOLEAN(is_active) as is_active,
    -- ... etc
FROM RAW.STUDENTS_RAW_VARIANT;
*/

-- ============================================================================
-- WORKAROUND 2C: VALIDATE before loading (manual COPY)
-- ============================================================================
-- Use VALIDATION_MODE to check without loading:

/*
COPY INTO RAW.STUDENTS_RAW
FROM @RAW.ESBJERG_STAGE/02_datatype_mismatch_ERROR.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT')
VALIDATION_MODE = 'RETURN_ERRORS';
*/

-- ============================================================================
-- SUMMARY
-- ============================================================================
-- | Error Type        | Default Behavior           | Workaround                    |
-- |-------------------|----------------------------|-------------------------------|
-- | Column renamed    | Loads by POSITION (silent) | MATCH_BY_COLUMN_NAME          |
-- | Column added      | Fails if count mismatch    | ERROR_ON_COLUMN_COUNT=FALSE   |
-- | Datatype mismatch | Entire file rejected       | ON_ERROR=CONTINUE or VARCHAR  |
-- | Column removed    | Fails if count mismatch    | ERROR_ON_COLUMN_COUNT=FALSE   |
-- ============================================================================
