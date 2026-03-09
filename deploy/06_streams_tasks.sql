-- ============================================================================
-- KMD WORKSHOP - STEP 6: SNOWPIPE + STREAMS & TASKS (Complete CDC Pipeline)
-- ============================================================================
-- Creates the COMPLETE automated pipeline:
--   S3 → Snowpipe → RAW → Stream → Task → CLEAN
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE KMD_STAGING;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SNOWPIPES (Auto-ingest from S3)
-- ============================================================================
-- These pipes automatically load data when new files arrive in S3
-- Requires S3 event notification pointing to the SQS queue shown in SHOW PIPES

USE SCHEMA RAW;

CREATE OR REPLACE PIPE SCHOOLS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO SCHOOLS_RAW
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_schools_all.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

CREATE OR REPLACE PIPE TEACHERS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO TEACHERS_RAW
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_teachers_all.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

CREATE OR REPLACE PIPE STUDENTS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO STUDENTS_RAW
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_students_all.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

CREATE OR REPLACE PIPE CLASSES_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO CLASSES_RAW
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_classes_all.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

-- Get the SQS ARN for S3 event notification setup
-- Add this ARN to your S3 bucket's event notification configuration
SHOW PIPES IN SCHEMA KMD_STAGING.RAW;

-- ============================================================================
-- CLEAN TABLES (Silver Layer - destination for CDC)
-- ============================================================================
USE SCHEMA CLEAN;

CREATE OR REPLACE TABLE SCHOOLS (
    school_id VARCHAR PRIMARY KEY,
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
-- STREAMS (Track changes on RAW tables - fed by Snowpipe)
-- ============================================================================
USE SCHEMA CDC;

CREATE OR REPLACE STREAM SCHOOLS_STREAM ON TABLE KMD_STAGING.RAW.SCHOOLS_RAW;
CREATE OR REPLACE STREAM TEACHERS_STREAM ON TABLE KMD_STAGING.RAW.TEACHERS_RAW;
CREATE OR REPLACE STREAM STUDENTS_STREAM ON TABLE KMD_STAGING.RAW.STUDENTS_RAW;
CREATE OR REPLACE STREAM CLASSES_STREAM ON TABLE KMD_STAGING.RAW.CLASSES_RAW;

-- ============================================================================
-- TASKS (Process stream data into CLEAN tables)
-- ============================================================================

CREATE OR REPLACE TASK PROCESS_SCHOOLS_TASK
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('SCHOOLS_STREAM')
AS
MERGE INTO KMD_STAGING.CLEAN.SCHOOLS t
USING (SELECT * FROM SCHOOLS_STREAM) s
ON t.school_id = s.school_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET 
    t.municipality_code = s.municipality_code, t.school_name = s.school_name, t.school_type = s.school_type,
    t.address = s.address, t.postal_code = s.postal_code, t.city = s.city, t.phone = s.phone, t.email = s.email,
    t.founded_year = s.founded_year, t.student_capacity = s.student_capacity, t.is_active = s.is_active, 
    t.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (school_id, municipality_code, school_name, school_type, address, postal_code, city, phone, email, founded_year, student_capacity, is_active)
VALUES (s.school_id, s.municipality_code, s.school_name, s.school_type, s.address, s.postal_code, s.city, s.phone, s.email, s.founded_year, s.student_capacity, s.is_active);

CREATE OR REPLACE TASK PROCESS_TEACHERS_TASK
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('TEACHERS_STREAM')
AS
MERGE INTO KMD_STAGING.CLEAN.TEACHERS t
USING (SELECT *, CONCAT(first_name, ' ', last_name) as full_name FROM TEACHERS_STREAM) s
ON t.teacher_id = s.teacher_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET 
    t.school_id = s.school_id, t.municipality_code = s.municipality_code, t.first_name = s.first_name, t.last_name = s.last_name,
    t.full_name = s.full_name, t.gender = s.gender, t.birth_date = s.birth_date, t.email = s.email, t.phone = s.phone,
    t.hire_date = s.hire_date, t.subjects = s.subjects, t.salary_band = s.salary_band, t.is_active = s.is_active, 
    t.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (teacher_id, school_id, municipality_code, first_name, last_name, full_name, gender, birth_date, email, phone, hire_date, subjects, salary_band, is_active)
VALUES (s.teacher_id, s.school_id, s.municipality_code, s.first_name, s.last_name, s.full_name, s.gender, s.birth_date, s.email, s.phone, s.hire_date, s.subjects, s.salary_band, s.is_active);

CREATE OR REPLACE TASK PROCESS_STUDENTS_TASK
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STUDENTS_STREAM')
AS
MERGE INTO KMD_STAGING.CLEAN.STUDENTS t
USING (SELECT *, CONCAT(first_name, ' ', last_name) as full_name FROM STUDENTS_STREAM) s
ON t.student_id = s.student_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET 
    t.class_id = s.class_id, t.school_id = s.school_id, t.municipality_code = s.municipality_code, 
    t.first_name = s.first_name, t.last_name = s.last_name, t.full_name = s.full_name, t.gender = s.gender, 
    t.birth_date = s.birth_date, t.enrollment_date = s.enrollment_date, t.guardian_name = s.guardian_name,
    t.guardian_phone = s.guardian_phone, t.guardian_email = s.guardian_email, t.address = s.address, 
    t.postal_code = s.postal_code, t.special_needs = s.special_needs, t.is_active = s.is_active, 
    t.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (student_id, class_id, school_id, municipality_code, first_name, last_name, full_name, gender, birth_date, enrollment_date, guardian_name, guardian_phone, guardian_email, address, postal_code, special_needs, is_active)
VALUES (s.student_id, s.class_id, s.school_id, s.municipality_code, s.first_name, s.last_name, s.full_name, s.gender, s.birth_date, s.enrollment_date, s.guardian_name, s.guardian_phone, s.guardian_email, s.address, s.postal_code, s.special_needs, s.is_active);

CREATE OR REPLACE TASK PROCESS_CLASSES_TASK
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('CLASSES_STREAM')
AS
MERGE INTO KMD_STAGING.CLEAN.CLASSES t
USING (SELECT * FROM CLASSES_STREAM) s
ON t.class_id = s.class_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET 
    t.school_id = s.school_id, t.municipality_code = s.municipality_code, t.grade = s.grade, t.section = s.section,
    t.class_name = s.class_name, t.academic_year = s.academic_year, t.max_students = s.max_students, 
    t.classroom_number = s.classroom_number, t.is_active = s.is_active, t.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (class_id, school_id, municipality_code, grade, section, class_name, academic_year, max_students, classroom_number, is_active)
VALUES (s.class_id, s.school_id, s.municipality_code, s.grade, s.section, s.class_name, s.academic_year, s.max_students, s.classroom_number, s.is_active);

-- ============================================================================
-- RESUME TASKS (Enable scheduled execution)
-- ============================================================================
ALTER TASK PROCESS_SCHOOLS_TASK RESUME;
ALTER TASK PROCESS_TEACHERS_TASK RESUME;
ALTER TASK PROCESS_STUDENTS_TASK RESUME;
ALTER TASK PROCESS_CLASSES_TASK RESUME;

-- ============================================================================
-- VERIFY COMPLETE PIPELINE
-- ============================================================================
SHOW PIPES IN SCHEMA KMD_STAGING.RAW;
SHOW STREAMS IN SCHEMA KMD_STAGING.CDC;
SHOW TASKS IN SCHEMA KMD_STAGING.CDC;

-- ============================================================================
-- S3 EVENT NOTIFICATION SETUP (Manual Step)
-- ============================================================================
-- Add the SQS ARN from SHOW PIPES to your S3 bucket event notification:
-- 
-- 1. Go to S3 Console → Your Bucket → Properties → Event Notifications
-- 2. Create notification with:
--    - Event types: s3:ObjectCreated:*
--    - Destination: SQS Queue
--    - SQS ARN: (from notification_channel in SHOW PIPES output)
--
-- The complete automated pipeline flow:
--   S3 file upload → S3 Event → SQS → Snowpipe → RAW table → 
--   Stream captures → Task processes → CLEAN table
-- ============================================================================
