-- ============================================================================
-- STUDENT EXERCISE - STEP 2: EXTERNAL STAGE & RAW TABLES
-- ============================================================================
-- Replace ESBJERG with your assigned municipality (uppercase)
-- Replace esbjerg with your assigned municipality (lowercase)
-- Replace 561 with your municipality code
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE ESBJERG_DB;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- EXTERNAL STAGE (points to your municipality's S3 folder)
-- ============================================================================
USE SCHEMA RAW;

CREATE OR REPLACE STAGE ESBJERG_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/esbjerg/'
    FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

-- Verify stage contents
LIST @ESBJERG_STAGE;

-- ============================================================================
-- RAW TABLES (Landing zone - Snowpipe loads here)
-- ============================================================================

CREATE OR REPLACE TABLE STUDENTS_RAW (
    student_id VARCHAR,
    class_id VARCHAR,
    school_id VARCHAR,
    municipality_code INTEGER,
    cpr_number VARCHAR,
    cpr_masked VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    birth_date DATE,
    enrollment_date DATE,
    guardian_name VARCHAR,
    guardian_phone VARCHAR,
    guardian_email VARCHAR,
    address VARCHAR,
    postal_code VARCHAR,
    special_needs VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE TEACHERS_RAW (
    teacher_id VARCHAR,
    school_id VARCHAR,
    municipality_code INTEGER,
    cpr_number VARCHAR,
    cpr_masked VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    birth_date DATE,
    email VARCHAR,
    phone VARCHAR,
    hire_date DATE,
    subjects VARCHAR,
    salary_band VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE CLASSES_RAW (
    class_id VARCHAR,
    school_id VARCHAR,
    municipality_code INTEGER,
    grade INTEGER,
    section VARCHAR,
    class_name VARCHAR,
    academic_year VARCHAR,
    max_students INTEGER,
    classroom_number VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- ============================================================================
-- CLEAN TABLES (Deduplicated silver layer)
-- ============================================================================
USE SCHEMA CLEAN;

CREATE OR REPLACE TABLE STUDENTS (
    student_id VARCHAR PRIMARY KEY,
    class_id VARCHAR,
    school_id VARCHAR,
    municipality_code INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    full_name VARCHAR,
    gender VARCHAR,
    birth_date DATE,
    enrollment_date DATE,
    guardian_name VARCHAR,
    guardian_phone VARCHAR,
    guardian_email VARCHAR,
    address VARCHAR,
    postal_code VARCHAR,
    special_needs VARCHAR,
    is_active BOOLEAN,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE TEACHERS (
    teacher_id VARCHAR PRIMARY KEY,
    school_id VARCHAR,
    municipality_code INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    full_name VARCHAR,
    gender VARCHAR,
    birth_date DATE,
    email VARCHAR,
    phone VARCHAR,
    hire_date DATE,
    subjects VARCHAR,
    salary_band VARCHAR,
    is_active BOOLEAN,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CLASSES (
    class_id VARCHAR PRIMARY KEY,
    school_id VARCHAR,
    municipality_code INTEGER,
    grade INTEGER,
    section VARCHAR,
    class_name VARCHAR,
    academic_year VARCHAR,
    max_students INTEGER,
    classroom_number VARCHAR,
    is_active BOOLEAN,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- INITIAL DATA LOAD (Manual - before Snowpipe is set up)
-- ============================================================================
/*
-- Run this to load baseline data manually:
COPY INTO ESBJERG_DB.RAW.STUDENTS_RAW
FROM @ESBJERG_DB.RAW.ESBJERG_STAGE/
PATTERN = '.*dim_students.*\.csv';

COPY INTO ESBJERG_DB.RAW.TEACHERS_RAW
FROM @ESBJERG_DB.RAW.ESBJERG_STAGE/
PATTERN = '.*dim_teachers.*\.csv';

COPY INTO ESBJERG_DB.RAW.CLASSES_RAW
FROM @ESBJERG_DB.RAW.ESBJERG_STAGE/
PATTERN = '.*dim_classes.*\.csv';
*/

-- Verify
SELECT 'STUDENTS_RAW' as tbl, COUNT(*) FROM RAW.STUDENTS_RAW
UNION ALL SELECT 'TEACHERS_RAW', COUNT(*) FROM RAW.TEACHERS_RAW
UNION ALL SELECT 'CLASSES_RAW', COUNT(*) FROM RAW.CLASSES_RAW;
