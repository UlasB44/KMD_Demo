-- ============================================================================
-- KMD WORKSHOP - STEP 7: DYNAMIC TABLES (Analytics Layer)
-- ============================================================================
-- Creates dynamic tables for real-time analytics
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE KMD_ANALYTICS;
USE SCHEMA MARTS;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- DYNAMIC TABLES
-- ============================================================================

-- School Summary: Aggregated metrics per school
CREATE OR REPLACE DYNAMIC TABLE DT_SCHOOL_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE = KMD_WH
AS
SELECT 
    s.school_id,
    s.school_name,
    s.municipality_code,
    s.school_type,
    s.city,
    s.student_capacity,
    COUNT(DISTINCT st.student_id) as current_students,
    COUNT(DISTINCT t.teacher_id) as teacher_count,
    ROUND(COUNT(DISTINCT st.student_id) / NULLIF(COUNT(DISTINCT t.teacher_id), 0), 1) as student_teacher_ratio
FROM KMD_STAGING.RAW.SCHOOLS_RAW s
LEFT JOIN KMD_STAGING.RAW.STUDENTS_RAW st ON s.school_id = st.school_id
LEFT JOIN KMD_STAGING.RAW.TEACHERS_RAW t ON s.school_id = t.school_id
GROUP BY s.school_id, s.school_name, s.municipality_code, s.school_type, s.city, s.student_capacity;

-- Class Enrollment: Capacity and availability per class
CREATE OR REPLACE DYNAMIC TABLE DT_CLASS_ENROLLMENT
    TARGET_LAG = '1 hour'
    WAREHOUSE = KMD_WH
AS
SELECT 
    c.class_id,
    c.class_name,
    c.school_id,
    s.school_name,
    c.municipality_code,
    c.grade,
    c.academic_year,
    c.max_students,
    COUNT(st.student_id) as enrolled_students,
    c.max_students - COUNT(st.student_id) as available_seats,
    ROUND(COUNT(st.student_id) * 100.0 / NULLIF(c.max_students, 0), 1) as enrollment_pct
FROM KMD_STAGING.RAW.CLASSES_RAW c
JOIN KMD_STAGING.RAW.SCHOOLS_RAW s ON c.school_id = s.school_id
LEFT JOIN KMD_STAGING.RAW.STUDENTS_RAW st ON c.class_id = st.class_id
GROUP BY c.class_id, c.class_name, c.school_id, s.school_name, c.municipality_code, c.grade, c.academic_year, c.max_students;

-- Municipality Summary: Aggregated metrics per municipality
CREATE OR REPLACE DYNAMIC TABLE DT_MUNICIPALITY_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE = KMD_WH
AS
SELECT 
    s.municipality_code,
    CASE s.municipality_code 
        WHEN 101 THEN 'Copenhagen' 
        WHEN 751 THEN 'Aarhus' 
        WHEN 461 THEN 'Odense'
        WHEN 851 THEN 'Aalborg' 
        WHEN 561 THEN 'Esbjerg' 
        ELSE 'Unknown'
    END as municipality_name,
    COUNT(DISTINCT s.school_id) as school_count,
    COUNT(DISTINCT t.teacher_id) as teacher_count,
    COUNT(DISTINCT st.student_id) as student_count,
    SUM(s.student_capacity) as total_capacity,
    ROUND(COUNT(DISTINCT st.student_id) * 100.0 / NULLIF(SUM(s.student_capacity), 0), 1) as capacity_utilization_pct
FROM KMD_STAGING.RAW.SCHOOLS_RAW s
LEFT JOIN KMD_STAGING.RAW.TEACHERS_RAW t ON s.school_id = t.school_id
LEFT JOIN KMD_STAGING.RAW.STUDENTS_RAW st ON s.school_id = st.school_id
GROUP BY s.municipality_code;

-- ============================================================================
-- VERIFY
-- ============================================================================
SHOW DYNAMIC TABLES IN SCHEMA KMD_ANALYTICS.MARTS;

SELECT * FROM DT_MUNICIPALITY_SUMMARY ORDER BY student_count DESC;
