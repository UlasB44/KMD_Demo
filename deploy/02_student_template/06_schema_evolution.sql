-- ============================================================================
-- STUDENT EXERCISE - STEP 6: SCHEMA EVOLUTION ERROR TESTING
-- ============================================================================
-- Replace {MUNICIPALITY} with your assigned municipality (uppercase)
-- Replace {municipality} with your assigned municipality (lowercase)
-- ============================================================================
-- 
-- TEST FILES (upload to S3 to trigger errors):
--   data/schema_evolution_tests/dim_students_99990001.csv  (column renamed)
--   data/schema_evolution_tests/dim_students_99990002.csv  (datatype mismatch)
--
-- Upload command:
--   aws s3 cp dim_students_99990001.csv s3://ubulut-iceberg-oregon/data/{municipality}/
--   aws s3 cp dim_students_99990002.csv s3://ubulut-iceberg-oregon/data/{municipality}/
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE {MUNICIPALITY}_DB;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- TEST 1: COLUMN RENAMED (first_name → frist_name)
-- ============================================================================
-- File: dim_students_99990001.csv
-- 
-- WHAT HAPPENS:
--   Snowpipe uses POSITIONAL mapping by default, NOT column names!
--   So this file will LOAD SUCCESSFULLY but data lands in WRONG columns.
--
-- The CSV header says: ...,frist_name,last_name,...
-- But Snowpipe ignores headers and loads by position (column 7, column 8, etc.)
-- Result: Data loads but "frist_name" value goes into first_name column anyway!
-- ============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('{MUNICIPALITY}_DB.RAW.STUDENTS_PIPE');

-- After uploading dim_students_99990001.csv, check if it loaded (it WILL succeed!)
SELECT FILE_NAME, STATUS, ROW_COUNT, FIRST_ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => '{MUNICIPALITY}_DB.RAW.STUDENTS_RAW',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC LIMIT 5;

-- The danger: Data appears correct because position matched!
SELECT student_id, first_name, last_name 
FROM RAW.STUDENTS_RAW 
WHERE student_id LIKE 'STU-%-999%';

-- ============================================================================
-- WORKAROUND 1: Use MATCH_BY_COLUMN_NAME (requires PARSE_HEADER)
-- ============================================================================
-- Create a file format that matches by column name:

CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT_BY_NAME
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 0           -- Don't skip, we need to parse it
    PARSE_HEADER = TRUE       -- Use header row for column names
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '');

-- Now COPY would fail or put NULL in first_name (column doesn't exist)
-- Example usage:
-- COPY INTO RAW.STUDENTS_RAW 
-- FROM @RAW.{MUNICIPALITY}_STAGE/
-- FILE_FORMAT = 'RAW.CSV_FORMAT_BY_NAME' 
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================================
-- TEST 2: DATATYPE MISMATCH (municipality_code: INTEGER gets VARCHAR)
-- ============================================================================
-- File: dim_students_99990002.csv
-- 
-- WHAT HAPPENS:
--   Snowpipe tries to cast 'THIS_IS_NOT_AN_INTEGER' to INTEGER
--   COPY fails with: "Numeric value 'THIS_IS_NOT_AN_INTEGER' is not recognized"
--   
-- BY DEFAULT: Entire file is rejected (ON_ERROR = 'ABORT_STATEMENT')
-- ============================================================================

-- After uploading dim_students_99990002.csv, check copy history - will show LOAD_FAILED
SELECT FILE_NAME, STATUS, ROW_COUNT, ERROR_COUNT, FIRST_ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => '{MUNICIPALITY}_DB.RAW.STUDENTS_RAW',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'Load failed'
ORDER BY LAST_LOAD_TIME DESC LIMIT 5;

-- ============================================================================
-- WORKAROUND 2A: ON_ERROR = 'CONTINUE' (skip bad rows)
-- ============================================================================
-- Recreate pipe with error handling:

/*
CREATE OR REPLACE PIPE RAW.STUDENTS_PIPE_SKIP_ERRORS
    AUTO_INGEST = TRUE
AS
COPY INTO RAW.STUDENTS_RAW
FROM @RAW.{MUNICIPALITY}_STAGE/
PATTERN = '.*dim_students_[0-9]+\.csv'
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
CREATE OR REPLACE VIEW RAW.STUDENTS_RAW_TYPED AS
SELECT 
    student_id,
    class_id,
    school_id,
    TRY_CAST(municipality_code AS INTEGER) as municipality_code,
    cpr_number,
    cpr_masked,
    first_name,
    last_name,
    gender,
    TRY_TO_DATE(birth_date) as birth_date,
    TRY_TO_DATE(enrollment_date) as enrollment_date,
    guardian_name,
    guardian_phone,
    guardian_email,
    address,
    postal_code,
    special_needs,
    TRY_TO_BOOLEAN(is_active) as is_active,
    TRY_TO_TIMESTAMP(created_at) as created_at,
    TRY_TO_TIMESTAMP(updated_at) as updated_at
FROM RAW.STUDENTS_RAW_VARIANT;

-- ============================================================================
-- WORKAROUND 2C: VALIDATE before loading (manual COPY)
-- ============================================================================
-- Use VALIDATION_MODE to check without loading:

SELECT * FROM TABLE(
    VALIDATE(@RAW.{MUNICIPALITY}_STAGE/dim_students_99990002.csv, 
    FILE_FORMAT => 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT')
);

-- Or with COPY:
/*
COPY INTO RAW.STUDENTS_RAW
FROM @RAW.{MUNICIPALITY}_STAGE/dim_students_99990002.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT')
VALIDATION_MODE = 'RETURN_ERRORS';
*/

-- ============================================================================
-- VERIFY DATA FLOW AFTER TESTS
-- ============================================================================
SELECT 'RAW.STUDENTS_RAW' as layer, COUNT(*) as record_count 
FROM {MUNICIPALITY}_DB.RAW.STUDENTS_RAW
UNION ALL
SELECT 'CDC.STUDENTS_STREAM', COUNT(*) FROM {MUNICIPALITY}_DB.CDC.STUDENTS_STREAM
UNION ALL
SELECT 'CLEAN.STUDENTS', COUNT(*) FROM {MUNICIPALITY}_DB.CLEAN.STUDENTS;

-- ============================================================================
-- SUMMARY TABLE
-- ============================================================================
-- | Error Type        | Default Behavior           | Workaround                    |
-- |-------------------|----------------------------|-------------------------------|
-- | Column renamed    | Loads by POSITION (silent) | MATCH_BY_COLUMN_NAME          |
-- | Column added      | Fails if count mismatch    | ERROR_ON_COLUMN_COUNT=FALSE   |
-- | Datatype mismatch | Entire file rejected       | ON_ERROR=CONTINUE or VARCHAR  |
-- | Column removed    | Fails if count mismatch    | ERROR_ON_COLUMN_COUNT=FALSE   |
-- ============================================================================
