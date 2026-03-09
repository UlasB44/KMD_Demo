-- ============================================================================
-- STUDENT EXERCISE - STEP 7: DYNAMIC TABLES (Analytics Layer)
-- ============================================================================
-- Replace {MUNICIPALITY} with your assigned municipality (uppercase)
-- Replace {municipality_name} with your municipality name (e.g., 'Copenhagen')
-- Replace {CODE} with your municipality code
-- ============================================================================
-- Dynamic Tables automatically refresh based on TARGET_LAG
-- They provide a simple way to create derived/aggregated views that stay fresh
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE {MUNICIPALITY}_DB;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- ANALYTICS SCHEMA (for Dynamic Tables)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- DYNAMIC TABLE 1: Student Summary by Grade
-- ============================================================================
-- Aggregates student counts and demographics per grade level

CREATE OR REPLACE DYNAMIC TABLE DT_STUDENTS_BY_GRADE
    TARGET_LAG = '1 hour'
    WAREHOUSE = KMD_WH
AS
SELECT 
    c.grade,
    c.academic_year,
    COUNT(DISTINCT s.student_id) AS student_count,
    COUNT(DISTINCT CASE WHEN s.gender = 'M' THEN s.student_id END) AS male_count,
    COUNT(DISTINCT CASE WHEN s.gender = 'F' THEN s.student_id END) AS female_count,
    COUNT(DISTINCT CASE WHEN s.special_needs != 'None' THEN s.student_id END) AS special_needs_count,
    ROUND(AVG(DATEDIFF('year', s.birth_date, CURRENT_DATE())), 1) AS avg_age
FROM {MUNICIPALITY}_DB.CLEAN.STUDENTS s
JOIN {MUNICIPALITY}_DB.CLEAN.CLASSES c ON s.class_id = c.class_id
WHERE s.is_active = TRUE
GROUP BY c.grade, c.academic_year;

-- ============================================================================
-- DYNAMIC TABLE 2: Class Enrollment Summary
-- ============================================================================
-- Shows enrollment vs capacity per class

CREATE OR REPLACE DYNAMIC TABLE DT_CLASS_ENROLLMENT
    TARGET_LAG = '1 hour'
    WAREHOUSE = KMD_WH
AS
SELECT 
    c.class_id,
    c.class_name,
    c.grade,
    c.section,
    c.academic_year,
    c.max_students AS capacity,
    COUNT(s.student_id) AS enrolled_students,
    c.max_students - COUNT(s.student_id) AS available_seats,
    ROUND(COUNT(s.student_id) * 100.0 / NULLIF(c.max_students, 0), 1) AS enrollment_pct,
    CASE 
        WHEN COUNT(s.student_id) >= c.max_students THEN 'FULL'
        WHEN COUNT(s.student_id) >= c.max_students * 0.9 THEN 'NEARLY FULL'
        WHEN COUNT(s.student_id) >= c.max_students * 0.5 THEN 'MODERATE'
        ELSE 'LOW'
    END AS enrollment_status
FROM {MUNICIPALITY}_DB.CLEAN.CLASSES c
LEFT JOIN {MUNICIPALITY}_DB.CLEAN.STUDENTS s ON c.class_id = s.class_id AND s.is_active = TRUE
WHERE c.is_active = TRUE
GROUP BY c.class_id, c.class_name, c.grade, c.section, c.academic_year, c.max_students;

-- ============================================================================
-- DYNAMIC TABLE 3: Teacher Workload Summary
-- ============================================================================
-- Calculates student-to-teacher metrics

CREATE OR REPLACE DYNAMIC TABLE DT_TEACHER_WORKLOAD
    TARGET_LAG = '1 hour'
    WAREHOUSE = KMD_WH
AS
SELECT 
    t.teacher_id,
    t.full_name AS teacher_name,
    t.subjects,
    t.hire_date,
    DATEDIFF('year', t.hire_date, CURRENT_DATE()) AS years_of_service,
    t.salary_band,
    COUNT(DISTINCT s.student_id) AS student_count,
    COUNT(DISTINCT s.class_id) AS class_count,
    CASE 
        WHEN COUNT(DISTINCT s.student_id) > 100 THEN 'HIGH'
        WHEN COUNT(DISTINCT s.student_id) > 50 THEN 'MODERATE'
        ELSE 'LOW'
    END AS workload_level
FROM {MUNICIPALITY}_DB.CLEAN.TEACHERS t
LEFT JOIN {MUNICIPALITY}_DB.CLEAN.CLASSES c ON t.school_id = c.school_id
LEFT JOIN {MUNICIPALITY}_DB.CLEAN.STUDENTS s ON c.class_id = s.class_id AND s.is_active = TRUE
WHERE t.is_active = TRUE
GROUP BY t.teacher_id, t.full_name, t.subjects, t.hire_date, t.salary_band;

-- ============================================================================
-- DYNAMIC TABLE 4: Municipality Overview
-- ============================================================================
-- High-level KPIs for this municipality

CREATE OR REPLACE DYNAMIC TABLE DT_MUNICIPALITY_OVERVIEW
    TARGET_LAG = '1 hour'
    WAREHOUSE = KMD_WH
AS
SELECT 
    {CODE} AS municipality_code,
    '{municipality_name}' AS municipality_name,
    COUNT(DISTINCT s.student_id) AS total_students,
    COUNT(DISTINCT t.teacher_id) AS total_teachers,
    COUNT(DISTINCT c.class_id) AS total_classes,
    ROUND(COUNT(DISTINCT s.student_id) * 1.0 / NULLIF(COUNT(DISTINCT t.teacher_id), 0), 1) AS student_teacher_ratio,
    COUNT(DISTINCT CASE WHEN s.special_needs != 'None' THEN s.student_id END) AS special_needs_students,
    ROUND(COUNT(DISTINCT CASE WHEN s.special_needs != 'None' THEN s.student_id END) * 100.0 
          / NULLIF(COUNT(DISTINCT s.student_id), 0), 1) AS special_needs_pct,
    SUM(c.max_students) AS total_capacity,
    ROUND(COUNT(DISTINCT s.student_id) * 100.0 / NULLIF(SUM(c.max_students), 0), 1) AS capacity_utilization_pct,
    CURRENT_TIMESTAMP() AS last_refreshed
FROM {MUNICIPALITY}_DB.CLEAN.STUDENTS s
FULL OUTER JOIN {MUNICIPALITY}_DB.CLEAN.TEACHERS t ON s.municipality_code = t.municipality_code
FULL OUTER JOIN {MUNICIPALITY}_DB.CLEAN.CLASSES c ON s.class_id = c.class_id
WHERE COALESCE(s.is_active, TRUE) = TRUE;

-- ============================================================================
-- VERIFY DYNAMIC TABLES
-- ============================================================================

-- List all dynamic tables
SHOW DYNAMIC TABLES IN SCHEMA {MUNICIPALITY}_DB.ANALYTICS;

-- Check refresh status
SELECT 
    name,
    target_lag,
    refresh_mode,
    scheduling_state,
    last_completed_refresh,
    next_scheduled_refresh
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE CATALOG_NAME = '{MUNICIPALITY}_DB'
ORDER BY name;

-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

-- Students by grade
SELECT * FROM DT_STUDENTS_BY_GRADE ORDER BY grade;

-- Classes with low enrollment (potential consolidation candidates)
SELECT * FROM DT_CLASS_ENROLLMENT 
WHERE enrollment_status IN ('LOW', 'MODERATE')
ORDER BY enrollment_pct;

-- Teacher workload distribution
SELECT workload_level, COUNT(*) AS teacher_count 
FROM DT_TEACHER_WORKLOAD 
GROUP BY workload_level;

-- Municipality overview
SELECT * FROM DT_MUNICIPALITY_OVERVIEW;

-- ============================================================================
-- MANUAL REFRESH (if needed)
-- ============================================================================
/*
-- Force refresh a specific dynamic table
ALTER DYNAMIC TABLE DT_STUDENTS_BY_GRADE REFRESH;
ALTER DYNAMIC TABLE DT_CLASS_ENROLLMENT REFRESH;
ALTER DYNAMIC TABLE DT_TEACHER_WORKLOAD REFRESH;
ALTER DYNAMIC TABLE DT_MUNICIPALITY_OVERVIEW REFRESH;
*/

-- ============================================================================
-- SUSPEND/RESUME DYNAMIC TABLES (for cost control)
-- ============================================================================
/*
-- Suspend refreshes (saves compute costs)
ALTER DYNAMIC TABLE DT_STUDENTS_BY_GRADE SUSPEND;
ALTER DYNAMIC TABLE DT_CLASS_ENROLLMENT SUSPEND;
ALTER DYNAMIC TABLE DT_TEACHER_WORKLOAD SUSPEND;
ALTER DYNAMIC TABLE DT_MUNICIPALITY_OVERVIEW SUSPEND;

-- Resume refreshes
ALTER DYNAMIC TABLE DT_STUDENTS_BY_GRADE RESUME;
ALTER DYNAMIC TABLE DT_CLASS_ENROLLMENT RESUME;
ALTER DYNAMIC TABLE DT_TEACHER_WORKLOAD RESUME;
ALTER DYNAMIC TABLE DT_MUNICIPALITY_OVERVIEW RESUME;
*/

-- ============================================================================
-- APPLY MASKING POLICIES TO DYNAMIC TABLES
-- ============================================================================
-- IMPORTANT: Masking policies on source tables don't automatically apply to DTs
-- because DTs materialize data using the owner role at refresh time.
-- We must apply policies directly to DT columns for query-time masking.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Grant ANALYTICS schema access to analyst role
GRANT USAGE ON SCHEMA {MUNICIPALITY}_DB.ANALYTICS TO ROLE {MUNICIPALITY}_ANALYST;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA {MUNICIPALITY}_DB.ANALYTICS TO ROLE {MUNICIPALITY}_ANALYST;

-- DT_TEACHER_WORKLOAD contains teacher_name (PII)
-- Reuse the masking policies created in 06_security.sql
CREATE OR REPLACE MASKING POLICY NAME_MASK AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val
        ELSE CONCAT(LEFT(val, 1), '*** ', LEFT(SPLIT_PART(val, ' ', -1), 1), '***')
    END;

ALTER TABLE {MUNICIPALITY}_DB.ANALYTICS.DT_TEACHER_WORKLOAD 
    MODIFY COLUMN teacher_name SET MASKING POLICY NAME_MASK;

-- ============================================================================
-- TEST MASKING ON DYNAMIC TABLES
-- ============================================================================

-- As ACCOUNTADMIN (should see full names):
SELECT teacher_id, teacher_name, subjects, workload_level 
FROM DT_TEACHER_WORKLOAD LIMIT 5;

-- As ANALYST (should see masked names like "J*** S***"):
/*
USE ROLE {MUNICIPALITY}_ANALYST;
SELECT teacher_id, teacher_name, subjects, workload_level 
FROM DT_TEACHER_WORKLOAD LIMIT 5;
*/

-- ============================================================================
-- VERIFY: Compare source vs DT masking
-- ============================================================================
/*
-- Source table (masking from 06_security.sql)
USE ROLE {MUNICIPALITY}_ANALYST;
SELECT teacher_id, first_name, last_name, email, phone 
FROM {MUNICIPALITY}_DB.CLEAN.TEACHERS LIMIT 3;

-- Dynamic Table (masking from this script)
SELECT teacher_id, teacher_name, subjects
FROM DT_TEACHER_WORKLOAD LIMIT 3;
*/
