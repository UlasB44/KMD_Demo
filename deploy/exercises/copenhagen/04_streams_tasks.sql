-- ============================================================================
-- STUDENT EXERCISE - STEP 4: STREAMS & TASKS (CDC Pipeline)
-- ============================================================================
-- Replace COPENHAGEN with your assigned municipality (uppercase)
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE COPENHAGEN_DB;
USE SCHEMA CDC;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- STREAMS (Track changes on RAW tables)
-- ============================================================================
CREATE OR REPLACE STREAM STUDENTS_STREAM ON TABLE COPENHAGEN_DB.RAW.STUDENTS_RAW;
CREATE OR REPLACE STREAM TEACHERS_STREAM ON TABLE COPENHAGEN_DB.RAW.TEACHERS_RAW;
CREATE OR REPLACE STREAM CLASSES_STREAM ON TABLE COPENHAGEN_DB.RAW.CLASSES_RAW;

-- ============================================================================
-- TASKS (MERGE stream data into CLEAN tables)
-- ============================================================================
-- Full-load pattern: Files contain ALL records each day
-- MERGE handles deduplication by primary key

CREATE OR REPLACE TASK PROCESS_STUDENTS_TASK
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STUDENTS_STREAM')
AS
MERGE INTO COPENHAGEN_DB.CLEAN.STUDENTS t
USING (
    SELECT *, CONCAT(first_name, ' ', last_name) as full_name 
    FROM STUDENTS_STREAM 
    WHERE METADATA$ACTION = 'INSERT'
) s
ON t.student_id = s.student_id
WHEN MATCHED THEN UPDATE SET 
    t.class_id = s.class_id, 
    t.school_id = s.school_id, 
    t.municipality_code = s.municipality_code,
    t.first_name = s.first_name, 
    t.last_name = s.last_name, 
    t.full_name = s.full_name, 
    t.gender = s.gender,
    t.birth_date = s.birth_date, 
    t.enrollment_date = s.enrollment_date, 
    t.guardian_name = s.guardian_name,
    t.guardian_phone = s.guardian_phone, 
    t.guardian_email = s.guardian_email, 
    t.address = s.address,
    t.postal_code = s.postal_code, 
    t.special_needs = s.special_needs, 
    t.is_active = s.is_active,
    t.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    student_id, class_id, school_id, municipality_code, first_name, last_name, 
    full_name, gender, birth_date, enrollment_date, guardian_name, guardian_phone, 
    guardian_email, address, postal_code, special_needs, is_active
)
VALUES (
    s.student_id, s.class_id, s.school_id, s.municipality_code, s.first_name, s.last_name,
    s.full_name, s.gender, s.birth_date, s.enrollment_date, s.guardian_name, s.guardian_phone,
    s.guardian_email, s.address, s.postal_code, s.special_needs, s.is_active
);

CREATE OR REPLACE TASK PROCESS_TEACHERS_TASK
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('TEACHERS_STREAM')
AS
MERGE INTO COPENHAGEN_DB.CLEAN.TEACHERS t
USING (
    SELECT *, CONCAT(first_name, ' ', last_name) as full_name 
    FROM TEACHERS_STREAM 
    WHERE METADATA$ACTION = 'INSERT'
) s
ON t.teacher_id = s.teacher_id
WHEN MATCHED THEN UPDATE SET 
    t.school_id = s.school_id, 
    t.municipality_code = s.municipality_code, 
    t.first_name = s.first_name, 
    t.last_name = s.last_name,
    t.full_name = s.full_name, 
    t.gender = s.gender, 
    t.birth_date = s.birth_date, 
    t.email = s.email, 
    t.phone = s.phone,
    t.hire_date = s.hire_date, 
    t.subjects = s.subjects, 
    t.salary_band = s.salary_band, 
    t.is_active = s.is_active,
    t.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    teacher_id, school_id, municipality_code, first_name, last_name, full_name, 
    gender, birth_date, email, phone, hire_date, subjects, salary_band, is_active
)
VALUES (
    s.teacher_id, s.school_id, s.municipality_code, s.first_name, s.last_name, s.full_name,
    s.gender, s.birth_date, s.email, s.phone, s.hire_date, s.subjects, s.salary_band, s.is_active
);

CREATE OR REPLACE TASK PROCESS_CLASSES_TASK
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('CLASSES_STREAM')
AS
MERGE INTO COPENHAGEN_DB.CLEAN.CLASSES t
USING (SELECT * FROM CLASSES_STREAM WHERE METADATA$ACTION = 'INSERT') s
ON t.class_id = s.class_id
WHEN MATCHED THEN UPDATE SET 
    t.school_id = s.school_id, 
    t.municipality_code = s.municipality_code, 
    t.grade = s.grade, 
    t.section = s.section,
    t.class_name = s.class_name, 
    t.academic_year = s.academic_year, 
    t.max_students = s.max_students,
    t.classroom_number = s.classroom_number, 
    t.is_active = s.is_active, 
    t.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    class_id, school_id, municipality_code, grade, section, class_name, 
    academic_year, max_students, classroom_number, is_active
)
VALUES (
    s.class_id, s.school_id, s.municipality_code, s.grade, s.section, s.class_name,
    s.academic_year, s.max_students, s.classroom_number, s.is_active
);

-- ============================================================================
-- RESUME TASKS
-- ============================================================================
ALTER TASK PROCESS_STUDENTS_TASK RESUME;
ALTER TASK PROCESS_TEACHERS_TASK RESUME;
ALTER TASK PROCESS_CLASSES_TASK RESUME;

-- ============================================================================
-- VERIFY
-- ============================================================================
SHOW STREAMS IN SCHEMA COPENHAGEN_DB.CDC;
SHOW TASKS IN SCHEMA COPENHAGEN_DB.CDC;

-- ============================================================================
-- MANUAL TASK EXECUTION (for testing)
-- ============================================================================
/*
EXECUTE TASK PROCESS_STUDENTS_TASK;
EXECUTE TASK PROCESS_TEACHERS_TASK;
EXECUTE TASK PROCESS_CLASSES_TASK;
*/
