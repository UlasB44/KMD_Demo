-- ============================================================================
-- KMD WORKSHOP - STEP 3: RAW TABLES & DATA LOAD
-- ============================================================================
-- Creates RAW tables and loads data from S3
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE KMD_STAGING;
USE SCHEMA RAW;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- RAW TABLES (Bronze Layer)
-- Column order matches CSV files exactly
-- ============================================================================

-- Schools (14 columns)
CREATE OR REPLACE TABLE SCHOOLS_RAW (
    school_id VARCHAR,
    municipality_code INTEGER,
    school_name VARCHAR,
    school_type VARCHAR,
    address VARCHAR,
    postal_code VARCHAR,
    city VARCHAR,
    phone VARCHAR,
    email VARCHAR,
    founded_year INTEGER,
    student_capacity INTEGER,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

-- Teachers (17 columns)
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

-- Students (20 columns)
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

-- Classes (12 columns)
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
-- LOAD DATA FROM S3
-- ============================================================================

COPY INTO SCHOOLS_RAW 
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_schools_all.csv 
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

COPY INTO TEACHERS_RAW 
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_teachers_all.csv 
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

COPY INTO STUDENTS_RAW 
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_students_all.csv 
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

COPY INTO CLASSES_RAW 
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_classes_all.csv 
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

-- ============================================================================
-- VERIFY LOAD
-- ============================================================================
SELECT 'SCHOOLS_RAW' as table_name, COUNT(*) as row_count FROM SCHOOLS_RAW
UNION ALL SELECT 'TEACHERS_RAW', COUNT(*) FROM TEACHERS_RAW
UNION ALL SELECT 'STUDENTS_RAW', COUNT(*) FROM STUDENTS_RAW
UNION ALL SELECT 'CLASSES_RAW', COUNT(*) FROM CLASSES_RAW;
