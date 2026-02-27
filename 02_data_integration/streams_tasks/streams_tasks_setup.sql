/*
=============================================================================
KMD Denmark - Streams and Tasks for Change Data Capture (CDC)
Incremental data processing from RAW to STAGING layer
=============================================================================
BEST PRACTICES:
1. Use APPEND_ONLY streams for insert-only scenarios (better performance)
2. Use STANDARD streams when you need to track all changes (INSERT, UPDATE, DELETE)
3. Set appropriate task schedules based on data freshness requirements
4. Implement error handling with try/catch in task SQL
5. Monitor task runs with TASK_HISTORY()
6. Use WAREHOUSE suspension to minimize costs
=============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE KMD_STAGING;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SECTION 1: Create Streams on Raw Tables
-- ============================================================================

USE SCHEMA RAW;

-- Stream on schools raw table (tracks all changes)
CREATE OR REPLACE STREAM SCHOOLS_RAW_STREAM
    ON TABLE SCHOOLS_RAW
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream for schools data changes';

-- Stream on teachers raw table
CREATE OR REPLACE STREAM TEACHERS_RAW_STREAM
    ON TABLE TEACHERS_RAW
    APPEND_ONLY = FALSE
    COMMENT = 'CDC stream for teachers data changes';

-- Stream on students raw table
CREATE OR REPLACE STREAM STUDENTS_RAW_STREAM
    ON TABLE STUDENTS_RAW
    APPEND_ONLY = FALSE
    COMMENT = 'CDC stream for students data changes';

-- Stream on classes raw table
CREATE OR REPLACE STREAM CLASSES_RAW_STREAM
    ON TABLE CLASSES_RAW
    APPEND_ONLY = FALSE
    COMMENT = 'CDC stream for classes data changes';

-- Stream on grades raw table (append-only for performance)
CREATE OR REPLACE STREAM GRADES_RAW_STREAM
    ON TABLE GRADES_RAW
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream for grades data (append-only)';

-- Stream on budgets raw table
CREATE OR REPLACE STREAM BUDGETS_RAW_STREAM
    ON TABLE BUDGETS_RAW
    APPEND_ONLY = FALSE
    COMMENT = 'CDC stream for budgets data changes';

-- ============================================================================
-- SECTION 2: Create Staging Tables (Silver Layer)
-- ============================================================================

USE SCHEMA STAGING;

-- Staging schools table (cleaned and validated)
CREATE OR REPLACE TABLE DIM_SCHOOLS_STAGING (
    school_id VARCHAR(50) NOT NULL,
    municipality_code VARCHAR(10) NOT NULL,
    school_name VARCHAR(200) NOT NULL,
    school_type VARCHAR(50),
    address VARCHAR(500),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(200),
    founded_year INT,
    student_capacity INT,
    is_active BOOLEAN DEFAULT TRUE,
    -- Audit columns
    source_created_at TIMESTAMP_NTZ,
    source_updated_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_current BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ,
    -- Primary key
    CONSTRAINT pk_schools PRIMARY KEY (school_id)
);

-- Staging teachers table
CREATE OR REPLACE TABLE DIM_TEACHERS_STAGING (
    teacher_id VARCHAR(50) NOT NULL,
    school_id VARCHAR(50) NOT NULL,
    municipality_code VARCHAR(10) NOT NULL,
    cpr_number VARCHAR(20),  -- Will be masked
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),  -- Derived field
    gender VARCHAR(1),
    birth_date DATE,
    email VARCHAR(200),
    phone VARCHAR(50),
    hire_date DATE,
    years_of_service INT,    -- Derived field
    subjects_array ARRAY,    -- Converted from CSV string
    salary_band VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_teachers PRIMARY KEY (teacher_id)
);

-- Staging students table
CREATE OR REPLACE TABLE DIM_STUDENTS_STAGING (
    student_id VARCHAR(50) NOT NULL,
    class_id VARCHAR(50),
    school_id VARCHAR(50) NOT NULL,
    municipality_code VARCHAR(10) NOT NULL,
    cpr_number VARCHAR(20),  -- Will be masked
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),  -- Derived field
    gender VARCHAR(1),
    birth_date DATE,
    age INT,                 -- Derived field
    enrollment_date DATE,
    guardian_name VARCHAR(200),
    guardian_phone VARCHAR(50),
    guardian_email VARCHAR(200),
    address VARCHAR(500),
    postal_code VARCHAR(10),
    special_needs VARCHAR(100),
    has_special_needs BOOLEAN,  -- Derived field
    is_active BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_students PRIMARY KEY (student_id)
);

-- Staging classes table
CREATE OR REPLACE TABLE DIM_CLASSES_STAGING (
    class_id VARCHAR(50) NOT NULL,
    school_id VARCHAR(50) NOT NULL,
    municipality_code VARCHAR(10) NOT NULL,
    grade VARCHAR(10),
    grade_numeric INT,       -- Derived field
    section VARCHAR(5),
    class_name VARCHAR(50),
    academic_year VARCHAR(20),
    max_students INT,
    classroom_number VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_classes PRIMARY KEY (class_id)
);

-- Staging grades fact table
CREATE OR REPLACE TABLE FACT_GRADES_STAGING (
    grade_record_id VARCHAR(50) NOT NULL,
    student_id VARCHAR(50) NOT NULL,
    class_id VARCHAR(50),
    school_id VARCHAR(50) NOT NULL,
    municipality_code VARCHAR(10) NOT NULL,
    subject VARCHAR(50),
    academic_year VARCHAR(20),
    term VARCHAR(10),
    grade_value VARCHAR(10),
    grade_numeric INT,       -- Derived field
    grade_date DATE,
    teacher_comment VARCHAR(500),
    is_final BOOLEAN,
    is_passing BOOLEAN,      -- Derived field
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_grades PRIMARY KEY (grade_record_id)
);

-- Staging budgets fact table
CREATE OR REPLACE TABLE FACT_BUDGETS_STAGING (
    budget_id VARCHAR(50) NOT NULL,
    school_id VARCHAR(50) NOT NULL,
    municipality_code VARCHAR(10) NOT NULL,
    fiscal_year INT,
    category VARCHAR(100),
    budgeted_amount DECIMAL(15,2),
    spent_amount DECIMAL(15,2),
    variance_amount DECIMAL(15,2),   -- Derived field
    variance_pct DECIMAL(10,4),      -- Derived field
    currency VARCHAR(10),
    approved_date DATE,
    approved_by VARCHAR(200),
    notes VARCHAR(1000),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_budgets PRIMARY KEY (budget_id)
);

-- ============================================================================
-- SECTION 3: Create Tasks for CDC Processing
-- ============================================================================

USE SCHEMA RAW;

-- Task to process schools stream
CREATE OR REPLACE TASK PROCESS_SCHOOLS_CDC
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Process schools CDC stream to staging'
WHEN
    SYSTEM$STREAM_HAS_DATA('SCHOOLS_RAW_STREAM')
AS
MERGE INTO KMD_STAGING.STAGING.DIM_SCHOOLS_STAGING tgt
USING (
    SELECT 
        school_id,
        municipality_code,
        TRIM(school_name) AS school_name,
        school_type,
        address,
        postal_code,
        city,
        phone,
        email,
        founded_year,
        student_capacity,
        COALESCE(is_active, TRUE) AS is_active,
        created_at AS source_created_at,
        updated_at AS source_updated_at
    FROM SCHOOLS_RAW_STREAM
    WHERE METADATA$ACTION = 'INSERT'
       OR (METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY school_id ORDER BY _loaded_at DESC) = 1
) src
ON tgt.school_id = src.school_id
WHEN MATCHED THEN UPDATE SET
    municipality_code = src.municipality_code,
    school_name = src.school_name,
    school_type = src.school_type,
    address = src.address,
    postal_code = src.postal_code,
    city = src.city,
    phone = src.phone,
    email = src.email,
    founded_year = src.founded_year,
    student_capacity = src.student_capacity,
    is_active = src.is_active,
    source_updated_at = src.source_updated_at,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    school_id, municipality_code, school_name, school_type, address,
    postal_code, city, phone, email, founded_year, student_capacity,
    is_active, source_created_at, source_updated_at
) VALUES (
    src.school_id, src.municipality_code, src.school_name, src.school_type, src.address,
    src.postal_code, src.city, src.phone, src.email, src.founded_year, src.student_capacity,
    src.is_active, src.source_created_at, src.source_updated_at
);

-- Task to process teachers stream
CREATE OR REPLACE TASK PROCESS_TEACHERS_CDC
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Process teachers CDC stream to staging'
WHEN
    SYSTEM$STREAM_HAS_DATA('TEACHERS_RAW_STREAM')
AS
MERGE INTO KMD_STAGING.STAGING.DIM_TEACHERS_STAGING tgt
USING (
    SELECT 
        teacher_id,
        school_id,
        municipality_code,
        cpr_masked AS cpr_number,  -- Use masked version
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        TRIM(first_name) || ' ' || TRIM(last_name) AS full_name,
        gender,
        birth_date,
        LOWER(email) AS email,
        phone,
        hire_date,
        DATEDIFF('year', hire_date, CURRENT_DATE()) AS years_of_service,
        SPLIT(subjects, ',') AS subjects_array,
        salary_band,
        COALESCE(is_active, TRUE) AS is_active
    FROM TEACHERS_RAW_STREAM
    WHERE METADATA$ACTION = 'INSERT'
       OR (METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY teacher_id ORDER BY _loaded_at DESC) = 1
) src
ON tgt.teacher_id = src.teacher_id
WHEN MATCHED THEN UPDATE SET
    school_id = src.school_id,
    municipality_code = src.municipality_code,
    cpr_number = src.cpr_number,
    first_name = src.first_name,
    last_name = src.last_name,
    full_name = src.full_name,
    gender = src.gender,
    birth_date = src.birth_date,
    email = src.email,
    phone = src.phone,
    hire_date = src.hire_date,
    years_of_service = src.years_of_service,
    subjects_array = src.subjects_array,
    salary_band = src.salary_band,
    is_active = src.is_active,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    teacher_id, school_id, municipality_code, cpr_number, first_name,
    last_name, full_name, gender, birth_date, email, phone,
    hire_date, years_of_service, subjects_array, salary_band, is_active
) VALUES (
    src.teacher_id, src.school_id, src.municipality_code, src.cpr_number, src.first_name,
    src.last_name, src.full_name, src.gender, src.birth_date, src.email, src.phone,
    src.hire_date, src.years_of_service, src.subjects_array, src.salary_band, src.is_active
);

-- Task to process students stream
CREATE OR REPLACE TASK PROCESS_STUDENTS_CDC
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Process students CDC stream to staging'
WHEN
    SYSTEM$STREAM_HAS_DATA('STUDENTS_RAW_STREAM')
AS
MERGE INTO KMD_STAGING.STAGING.DIM_STUDENTS_STAGING tgt
USING (
    SELECT 
        student_id,
        class_id,
        school_id,
        municipality_code,
        cpr_masked AS cpr_number,  -- Use masked version
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        TRIM(first_name) || ' ' || TRIM(last_name) AS full_name,
        gender,
        birth_date,
        DATEDIFF('year', birth_date, CURRENT_DATE()) AS age,
        enrollment_date,
        guardian_name,
        guardian_phone,
        LOWER(guardian_email) AS guardian_email,
        address,
        postal_code,
        special_needs,
        CASE WHEN special_needs IS NOT NULL AND special_needs != 'Ingen' THEN TRUE ELSE FALSE END AS has_special_needs,
        COALESCE(is_active, TRUE) AS is_active
    FROM STUDENTS_RAW_STREAM
    WHERE METADATA$ACTION = 'INSERT'
       OR (METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY _loaded_at DESC) = 1
) src
ON tgt.student_id = src.student_id
WHEN MATCHED THEN UPDATE SET
    class_id = src.class_id,
    school_id = src.school_id,
    municipality_code = src.municipality_code,
    cpr_number = src.cpr_number,
    first_name = src.first_name,
    last_name = src.last_name,
    full_name = src.full_name,
    gender = src.gender,
    birth_date = src.birth_date,
    age = src.age,
    enrollment_date = src.enrollment_date,
    guardian_name = src.guardian_name,
    guardian_phone = src.guardian_phone,
    guardian_email = src.guardian_email,
    address = src.address,
    postal_code = src.postal_code,
    special_needs = src.special_needs,
    has_special_needs = src.has_special_needs,
    is_active = src.is_active,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    student_id, class_id, school_id, municipality_code, cpr_number,
    first_name, last_name, full_name, gender, birth_date, age,
    enrollment_date, guardian_name, guardian_phone, guardian_email,
    address, postal_code, special_needs, has_special_needs, is_active
) VALUES (
    src.student_id, src.class_id, src.school_id, src.municipality_code, src.cpr_number,
    src.first_name, src.last_name, src.full_name, src.gender, src.birth_date, src.age,
    src.enrollment_date, src.guardian_name, src.guardian_phone, src.guardian_email,
    src.address, src.postal_code, src.special_needs, src.has_special_needs, src.is_active
);

-- Task to process grades stream (append-only, simpler logic)
CREATE OR REPLACE TASK PROCESS_GRADES_CDC
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Process grades CDC stream to staging'
WHEN
    SYSTEM$STREAM_HAS_DATA('GRADES_RAW_STREAM')
AS
INSERT INTO KMD_STAGING.STAGING.FACT_GRADES_STAGING (
    grade_record_id, student_id, class_id, school_id, municipality_code,
    subject, academic_year, term, grade_value, grade_numeric, grade_date,
    teacher_comment, is_final, is_passing
)
SELECT 
    grade_record_id,
    student_id,
    class_id,
    school_id,
    municipality_code,
    subject,
    academic_year,
    term,
    grade_value,
    CASE grade_value 
        WHEN '12' THEN 12 WHEN '10' THEN 10 WHEN '7' THEN 7 
        WHEN '4' THEN 4 WHEN '02' THEN 2 WHEN '00' THEN 0 
        WHEN '-3' THEN -3 ELSE NULL 
    END AS grade_numeric,
    grade_date,
    teacher_comment,
    is_final,
    CASE WHEN grade_value IN ('12', '10', '7', '4', '02') THEN TRUE ELSE FALSE END AS is_passing
FROM GRADES_RAW_STREAM;

-- ============================================================================
-- SECTION 4: Enable Tasks
-- ============================================================================

-- Resume all tasks (tasks are created in suspended state)
ALTER TASK PROCESS_SCHOOLS_CDC RESUME;
ALTER TASK PROCESS_TEACHERS_CDC RESUME;
ALTER TASK PROCESS_STUDENTS_CDC RESUME;
ALTER TASK PROCESS_GRADES_CDC RESUME;

-- ============================================================================
-- SECTION 5: Monitoring Queries
-- ============================================================================

-- Check stream status
SELECT 
    'SCHOOLS_RAW_STREAM' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('SCHOOLS_RAW_STREAM') AS has_data
UNION ALL
SELECT 
    'TEACHERS_RAW_STREAM',
    SYSTEM$STREAM_HAS_DATA('TEACHERS_RAW_STREAM')
UNION ALL
SELECT 
    'STUDENTS_RAW_STREAM',
    SYSTEM$STREAM_HAS_DATA('STUDENTS_RAW_STREAM')
UNION ALL
SELECT 
    'GRADES_RAW_STREAM',
    SYSTEM$STREAM_HAS_DATA('GRADES_RAW_STREAM');

-- View task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
WHERE NAME LIKE 'PROCESS_%'
ORDER BY SCHEDULED_TIME DESC;

-- Check task status
SHOW TASKS LIKE 'PROCESS_%';

SELECT 'Streams and Tasks setup complete!' AS status;
