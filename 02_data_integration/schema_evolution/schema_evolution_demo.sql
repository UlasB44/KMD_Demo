/*
================================================================================
SCHEMA EVOLUTION IN SNOWFLAKE - COMPREHENSIVE WORKSHOP GUIDE
================================================================================
Understanding what happens when source CSV files change:
- Column renamed
- Column missing
- Data type changes
- New column added

Key Concepts:
- ENABLE_SCHEMA_EVOLUTION = TRUE
- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
- PARSE_HEADER = TRUE  
- ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE (required for CSV)
================================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE KMD_STAGING;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SETUP: Create schema and file format for schema evolution demos
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS SCHEMA_EVOLUTION_DEMO;
USE SCHEMA SCHEMA_EVOLUTION_DEMO;

-- File format that supports schema evolution (CSV)
CREATE OR REPLACE FILE FORMAT CSV_SCHEMA_EVOLUTION
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 0                          -- We'll use PARSE_HEADER instead
    PARSE_HEADER = TRUE                      -- Read column names from first row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;  -- CRITICAL for schema evolution!

-- Stage for our demo files
CREATE OR REPLACE STAGE SCHEMA_EVOLUTION_STAGE
    FILE_FORMAT = CSV_SCHEMA_EVOLUTION;

-- ============================================================================
-- SCENARIO 1: BASE LOAD - Original CSV Structure
-- ============================================================================

-- First, let's simulate our original CSV file structure
-- school_id,municipality_code,school_name,school_type,student_capacity,is_active

CREATE OR REPLACE TABLE SCHOOLS_EVOLUTION (
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    school_name VARCHAR(200),
    school_type VARCHAR(50),
    student_capacity INT,
    is_active BOOLEAN,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
)
ENABLE_SCHEMA_EVOLUTION = TRUE;  -- CRITICAL: Enable schema evolution

-- Simulate initial data load
INSERT INTO SCHOOLS_EVOLUTION (school_id, municipality_code, school_name, school_type, student_capacity, is_active, _source_file)
VALUES 
    ('101-SCH-001', '101', 'Kirke Folkeskole', 'Folkeskole', 529, TRUE, 'schools_v1.csv'),
    ('101-SCH-002', '101', 'Slot Skole', 'Folkeskole', 609, TRUE, 'schools_v1.csv'),
    ('751-SCH-001', '751', 'Aarhus Friskole', 'Friskole', 220, TRUE, 'schools_v1.csv');

SELECT * FROM SCHOOLS_EVOLUTION;

-- Check current schema
DESCRIBE TABLE SCHOOLS_EVOLUTION;

-- ============================================================================
-- SCENARIO 2: NEW COLUMN ADDED
-- What happens: New CSV has additional column 'email'
-- Result: Column automatically added to table
-- ============================================================================

-- In real scenario, this would be a COPY INTO with the new file:
/*
COPY INTO SCHOOLS_EVOLUTION
FROM @SCHEMA_EVOLUTION_STAGE/schools_v2.csv
FILE_FORMAT = CSV_SCHEMA_EVOLUTION
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
*/

-- For demo, let's use ALTER to show what happens
-- First, check what column would be added
SELECT 'ADD_COLUMN' as evolution_type, 
       'email' as column_name,
       'VARCHAR(200)' as data_type,
       'This happens automatically with ENABLE_SCHEMA_EVOLUTION' as note;

-- Simulate the schema evolution (what COPY INTO would do automatically)
ALTER TABLE SCHOOLS_EVOLUTION ADD COLUMN email VARCHAR(200);

-- Insert data with the new column
INSERT INTO SCHOOLS_EVOLUTION (school_id, municipality_code, school_name, school_type, student_capacity, is_active, email, _source_file)
VALUES ('101-SCH-003', '101', 'Sondre Privatskole', 'Folkeskole', 449, TRUE, 'kontor@sondreprivatskole.dk', 'schools_v2.csv');

-- Check schema evolution record
DESCRIBE TABLE SCHOOLS_EVOLUTION;

-- Notice: Old rows have NULL for email, new rows have the value
SELECT school_id, school_name, email, _source_file FROM SCHOOLS_EVOLUTION;

-- ============================================================================
-- SCENARIO 3: COLUMN MISSING IN NEW FILE
-- What happens: New CSV doesn't have 'student_capacity'
-- Result: NOT NULL constraint dropped (if exists), NULLs inserted
-- ============================================================================

SELECT 'DROP_NOT_NULL' as evolution_type,
       'student_capacity' as column_name,
       'Column missing from new file' as reason,
       'Old rows keep values, new rows get NULL' as result;

-- Simulate file without student_capacity
INSERT INTO SCHOOLS_EVOLUTION (school_id, municipality_code, school_name, school_type, is_active, email, _source_file)
VALUES ('461-SCH-001', '461', 'Odense Friskole', 'Friskole', TRUE, 'kontor@odensefriskole.dk', 'schools_v3_missing_capacity.csv');

-- Check: student_capacity is NULL for the new record
SELECT school_id, school_name, student_capacity, _source_file 
FROM SCHOOLS_EVOLUTION
ORDER BY _loaded_at DESC;

-- ============================================================================
-- SCENARIO 4: COLUMN RENAMED - THIS IS THE TRICKY ONE!
-- What happens: 'school_name' renamed to 'skole_navn' in new CSV
-- Result: Snowflake treats it as NEW column, OLD column gets NULLs
-- ============================================================================

SELECT 'COLUMN_RENAME' as scenario,
       'school_name -> skole_navn' as change,
       'Snowflake sees this as ADD new column + DROP NOT NULL on old' as behavior,
       'You must handle this manually or transform in COPY' as solution;

-- Simulate: New CSV has 'skole_navn' instead of 'school_name'
ALTER TABLE SCHOOLS_EVOLUTION ADD COLUMN skole_navn VARCHAR(200);

-- Insert with renamed column
INSERT INTO SCHOOLS_EVOLUTION (school_id, municipality_code, skole_navn, school_type, student_capacity, is_active, email, _source_file)
VALUES ('851-SCH-001', '851', 'Aalborg Nordvest Skole', 'Folkeskole', 380, TRUE, 'kontor@aalborgnordvest.dk', 'schools_v4_renamed.csv');

-- Notice the problem: school_name is NULL, skole_navn has value
SELECT school_id, school_name, skole_navn, _source_file FROM SCHOOLS_EVOLUTION;

-- ============================================================================
-- BEST PRACTICE: Handle column rename with transformation
-- ============================================================================

-- Solution 1: Use COPY INTO with column transformation
/*
COPY INTO SCHOOLS_EVOLUTION (school_id, municipality_code, school_name, ...)
FROM (
    SELECT 
        $1 as school_id,
        $2 as municipality_code,
        $3 as school_name,  -- Map skole_navn to school_name
        ...
    FROM @STAGE/schools_v4_renamed.csv
);
*/

-- Solution 2: Create a view that coalesces the columns
CREATE OR REPLACE VIEW V_SCHOOLS_UNIFIED AS
SELECT 
    school_id,
    municipality_code,
    COALESCE(school_name, skole_navn) AS school_name,  -- Handle rename
    school_type,
    student_capacity,
    is_active,
    email,
    _loaded_at,
    _source_file
FROM SCHOOLS_EVOLUTION;

SELECT * FROM V_SCHOOLS_UNIFIED;

-- ============================================================================
-- SCENARIO 5: DATA TYPE CHANGE
-- What happens: student_capacity changes from INT to VARCHAR
-- Result: COPY fails if incompatible, or silent truncation
-- ============================================================================

SELECT 'DATA_TYPE_CHANGE' as scenario,
       'student_capacity INT -> VARCHAR' as change,
       'COPY will fail with type mismatch or truncate' as behavior,
       'Use staged transformation or pre-processing' as solution;

-- Create a new table to demonstrate type handling
CREATE OR REPLACE TABLE SCHOOLS_TYPE_DEMO (
    school_id VARCHAR(50),
    student_capacity INT,  -- Original: INT
    notes VARCHAR(500)
)
ENABLE_SCHEMA_EVOLUTION = TRUE;

INSERT INTO SCHOOLS_TYPE_DEMO VALUES ('101-SCH-001', 500, 'Original data');

-- If new CSV has student_capacity as "500 students" (string):
-- Option 1: Pre-process the file
-- Option 2: Use TRY_TO_NUMBER in COPY transformation
-- Option 3: Load to VARCHAR staging and transform

/*
COPY INTO SCHOOLS_TYPE_DEMO (school_id, student_capacity, notes)
FROM (
    SELECT 
        $1,
        TRY_TO_NUMBER(REGEXP_REPLACE($2, '[^0-9]', '')),  -- Extract numbers
        $3
    FROM @STAGE/schools_type_change.csv
)
FILE_FORMAT = CSV_SCHEMA_EVOLUTION;
*/

-- ============================================================================
-- SCENARIO 6: COLUMN ORDER CHANGED
-- What happens: Columns in different order
-- Result: With MATCH_BY_COLUMN_NAME, this is handled automatically!
-- ============================================================================

SELECT 'COLUMN_ORDER_CHANGE' as scenario,
       'Original: id,name,type | New: type,name,id' as change,
       'MATCH_BY_COLUMN_NAME handles this automatically' as behavior,
       'Always use MATCH_BY_COLUMN_NAME for schema evolution' as solution;

-- This works because we match by name, not position
-- COPY INTO table
-- FROM @stage
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;  <-- This is the key!

-- ============================================================================
-- MONITORING SCHEMA EVOLUTION
-- ============================================================================

-- Check schema evolution history
SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    SCHEMA_EVOLUTION_RECORD
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'SCHEMA_EVOLUTION_DEMO'
AND SCHEMA_EVOLUTION_RECORD IS NOT NULL;

-- View all columns and their evolution status
SHOW COLUMNS IN TABLE SCHOOLS_EVOLUTION;

-- ============================================================================
-- SUMMARY: SCHEMA EVOLUTION BEHAVIOR MATRIX
-- ============================================================================

SELECT 'REFERENCE TABLE' as title;
SELECT * FROM (
    SELECT 1 as ord, 'New Column Added' as scenario, 
           'ENABLE_SCHEMA_EVOLUTION=TRUE + MATCH_BY_COLUMN_NAME' as requirement,
           'Column auto-added, old rows get NULL' as result,
           'Automatic - no action needed' as action
    UNION ALL
    SELECT 2, 'Column Missing', 
           'ENABLE_SCHEMA_EVOLUTION=TRUE + MATCH_BY_COLUMN_NAME',
           'NOT NULL dropped, new rows get NULL',
           'Automatic - monitor for data quality'
    UNION ALL
    SELECT 3, 'Column Renamed', 
           'N/A - Snowflake cannot detect renames',
           'Treated as NEW column + missing column',
           'Manual: Use COPY transformation or COALESCE in view'
    UNION ALL
    SELECT 4, 'Data Type Change', 
           'N/A - Schema evolution doesn''t change types',
           'COPY fails or data truncation',
           'Manual: Pre-process file or use TRY_TO_* functions'
    UNION ALL
    SELECT 5, 'Column Order Changed', 
           'MATCH_BY_COLUMN_NAME',
           'Handled automatically by name matching',
           'Automatic - no action needed'
    UNION ALL
    SELECT 6, 'Case Difference (School_Name vs SCHOOL_NAME)', 
           'MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE',
           'Matched automatically',
           'Automatic - use CASE_INSENSITIVE'
) ORDER BY ord;

-- ============================================================================
-- PRACTICAL EXERCISE: Load Real CSV with Schema Evolution
-- ============================================================================

-- Step 1: Create table with schema evolution enabled
CREATE OR REPLACE TABLE SCHOOLS_REAL (
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    school_name VARCHAR(200),
    school_type VARCHAR(50),
    address VARCHAR(300),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(200),
    founded_year INT,
    student_capacity INT,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500) DEFAULT METADATA$FILENAME
)
ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Step 2: Load from S3 (assuming files are uploaded)
/*
COPY INTO SCHOOLS_REAL
FROM @KMD_STAGING.EXTERNAL_STAGES.KMD_S3_STAGE/combined/dim_schools_all.csv
FILE_FORMAT = CSV_SCHEMA_EVOLUTION
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';
*/

-- Step 3: Monitor for schema changes
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, SCHEMA_EVOLUTION_RECORD
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'SCHOOLS_REAL'
ORDER BY ORDINAL_POSITION;

-- ============================================================================
-- ALERTS AND NOTIFICATIONS FOR SCHEMA EVOLUTION
-- ============================================================================

-- Create view to track schema changes
CREATE OR REPLACE VIEW V_SCHEMA_EVOLUTION_ALERTS AS
SELECT 
    TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS full_table_name,
    COLUMN_NAME,
    DATA_TYPE,
    PARSE_JSON(SCHEMA_EVOLUTION_RECORD):evolutionType::VARCHAR AS evolution_type,
    PARSE_JSON(SCHEMA_EVOLUTION_RECORD):fileName::VARCHAR AS source_file,
    PARSE_JSON(SCHEMA_EVOLUTION_RECORD):triggeringTime::TIMESTAMP AS evolution_time,
    PARSE_JSON(SCHEMA_EVOLUTION_RECORD):queryId::VARCHAR AS query_id
FROM INFORMATION_SCHEMA.COLUMNS
WHERE SCHEMA_EVOLUTION_RECORD IS NOT NULL
ORDER BY evolution_time DESC;

-- Query recent schema changes
-- SELECT * FROM V_SCHEMA_EVOLUTION_ALERTS;

-- ============================================================================
-- CLEANUP (Optional - for demo reset)
-- ============================================================================
/*
DROP TABLE IF EXISTS SCHOOLS_EVOLUTION;
DROP TABLE IF EXISTS SCHOOLS_TYPE_DEMO;
DROP TABLE IF EXISTS SCHOOLS_REAL;
DROP VIEW IF EXISTS V_SCHOOLS_UNIFIED;
DROP VIEW IF EXISTS V_SCHEMA_EVOLUTION_ALERTS;
DROP SCHEMA IF EXISTS SCHEMA_EVOLUTION_DEMO;
*/
