/*
=============================================================================
KMD Denmark - Dynamic Tables for Analytics Layer
Automated materialized views with declarative refresh
=============================================================================
BEST PRACTICES:
1. Set TARGET_LAG based on business requirements for data freshness
2. Use DOWNSTREAM for dynamic tables that depend on other dynamic tables
3. Monitor refresh costs with DYNAMIC_TABLE_REFRESH_HISTORY()
4. Consider clustering for large dynamic tables
5. Use appropriate warehouse sizing for refresh operations
=============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE KMD_ANALYTICS;
USE SCHEMA GOLD;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SECTION 1: Dimension Tables (Dynamic)
-- ============================================================================

-- Dynamic table for Schools dimension (Gold layer)
CREATE OR REPLACE DYNAMIC TABLE DIM_SCHOOLS
    TARGET_LAG = '15 minutes'
    WAREHOUSE = KMD_WH
    COMMENT = 'Gold layer - Schools dimension with enriched data'
AS
SELECT 
    s.school_id,
    s.municipality_code,
    m.municipality_name,
    m.region,
    s.school_name,
    s.school_type,
    s.address,
    s.postal_code,
    s.city,
    s.phone,
    s.email,
    s.founded_year,
    YEAR(CURRENT_DATE()) - s.founded_year AS school_age_years,
    s.student_capacity,
    s.is_active,
    s.loaded_at,
    s.updated_at
FROM KMD_STAGING.STAGING.DIM_SCHOOLS_STAGING s
LEFT JOIN KMD_SCHOOLS.SHARED.DIM_MUNICIPALITIES m 
    ON s.municipality_code = m.municipality_code;

-- Dynamic table for Teachers dimension
CREATE OR REPLACE DYNAMIC TABLE DIM_TEACHERS
    TARGET_LAG = '15 minutes'
    WAREHOUSE = KMD_WH
    COMMENT = 'Gold layer - Teachers dimension with enriched data'
AS
SELECT 
    t.teacher_id,
    t.school_id,
    s.school_name,
    t.municipality_code,
    m.municipality_name,
    t.cpr_number,  -- Will be masked based on role
    t.first_name,
    t.last_name,
    t.full_name,
    t.gender,
    CASE t.gender WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'Other' END AS gender_name,
    t.birth_date,
    t.email,
    t.phone,
    t.hire_date,
    t.years_of_service,
    CASE 
        WHEN t.years_of_service < 3 THEN 'Junior'
        WHEN t.years_of_service < 10 THEN 'Mid-Level'
        WHEN t.years_of_service < 20 THEN 'Senior'
        ELSE 'Expert'
    END AS experience_level,
    t.subjects_array,
    ARRAY_SIZE(t.subjects_array) AS num_subjects,
    t.salary_band,
    t.is_active,
    t.loaded_at
FROM KMD_STAGING.STAGING.DIM_TEACHERS_STAGING t
LEFT JOIN KMD_STAGING.STAGING.DIM_SCHOOLS_STAGING s ON t.school_id = s.school_id
LEFT JOIN KMD_SCHOOLS.SHARED.DIM_MUNICIPALITIES m ON t.municipality_code = m.municipality_code;

-- Dynamic table for Students dimension
CREATE OR REPLACE DYNAMIC TABLE DIM_STUDENTS
    TARGET_LAG = '15 minutes'
    WAREHOUSE = KMD_WH
    COMMENT = 'Gold layer - Students dimension with enriched data'
AS
SELECT 
    st.student_id,
    st.class_id,
    c.class_name,
    c.grade,
    c.grade_numeric,
    st.school_id,
    s.school_name,
    st.municipality_code,
    m.municipality_name,
    st.cpr_number,  -- Will be masked based on role
    st.first_name,
    st.last_name,
    st.full_name,
    st.gender,
    CASE st.gender WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'Other' END AS gender_name,
    st.birth_date,
    st.age,
    CASE 
        WHEN st.age BETWEEN 6 AND 9 THEN 'Indskoling (0-3)'
        WHEN st.age BETWEEN 10 AND 12 THEN 'Mellemtrin (4-6)'
        WHEN st.age BETWEEN 13 AND 16 THEN 'Udskoling (7-9)'
        ELSE 'Other'
    END AS age_group,
    st.enrollment_date,
    st.guardian_name,
    st.guardian_phone,
    st.guardian_email,
    st.address,
    st.postal_code,
    st.special_needs,
    st.has_special_needs,
    sn.support_level AS special_needs_support_level,
    st.is_active,
    st.loaded_at
FROM KMD_STAGING.STAGING.DIM_STUDENTS_STAGING st
LEFT JOIN KMD_STAGING.STAGING.DIM_CLASSES_STAGING c ON st.class_id = c.class_id
LEFT JOIN KMD_STAGING.STAGING.DIM_SCHOOLS_STAGING s ON st.school_id = s.school_id
LEFT JOIN KMD_SCHOOLS.SHARED.DIM_MUNICIPALITIES m ON st.municipality_code = m.municipality_code
LEFT JOIN KMD_SCHOOLS.SHARED.DIM_SPECIAL_NEEDS sn ON st.special_needs = sn.category_name;

-- Dynamic table for Classes dimension
CREATE OR REPLACE DYNAMIC TABLE DIM_CLASSES
    TARGET_LAG = '15 minutes'
    WAREHOUSE = KMD_WH
    COMMENT = 'Gold layer - Classes dimension with student counts'
AS
SELECT 
    c.class_id,
    c.school_id,
    s.school_name,
    c.municipality_code,
    m.municipality_name,
    c.grade,
    c.grade_numeric,
    c.section,
    c.class_name,
    c.academic_year,
    c.max_students,
    c.classroom_number,
    c.is_active,
    COUNT(DISTINCT st.student_id) AS actual_student_count,
    c.max_students - COUNT(DISTINCT st.student_id) AS available_seats,
    ROUND(COUNT(DISTINCT st.student_id) / NULLIF(c.max_students, 0) * 100, 1) AS capacity_utilization_pct,
    c.loaded_at
FROM KMD_STAGING.STAGING.DIM_CLASSES_STAGING c
LEFT JOIN KMD_STAGING.STAGING.DIM_SCHOOLS_STAGING s ON c.school_id = s.school_id
LEFT JOIN KMD_SCHOOLS.SHARED.DIM_MUNICIPALITIES m ON c.municipality_code = m.municipality_code
LEFT JOIN KMD_STAGING.STAGING.DIM_STUDENTS_STAGING st ON c.class_id = st.class_id AND st.is_active = TRUE
GROUP BY ALL;

-- ============================================================================
-- SECTION 2: Fact Tables (Dynamic)
-- ============================================================================

-- Dynamic table for Grades fact with aggregations
CREATE OR REPLACE DYNAMIC TABLE FACT_GRADES
    TARGET_LAG = '15 minutes'
    WAREHOUSE = KMD_WH
    COMMENT = 'Gold layer - Grades fact table'
AS
SELECT 
    g.grade_record_id,
    g.student_id,
    g.class_id,
    g.school_id,
    g.municipality_code,
    g.subject,
    sub.subject_category,
    sub.is_mandatory AS is_mandatory_subject,
    g.academic_year,
    g.term,
    g.grade_value,
    g.grade_numeric,
    gs.grade_name,
    gs.grade_description,
    g.grade_date,
    g.teacher_comment,
    g.is_final,
    g.is_passing,
    g.loaded_at
FROM KMD_STAGING.STAGING.FACT_GRADES_STAGING g
LEFT JOIN KMD_SCHOOLS.SHARED.DIM_GRADE_SCALE gs ON g.grade_value = gs.grade_value
LEFT JOIN KMD_SCHOOLS.SHARED.DIM_SUBJECTS sub ON g.subject = sub.subject_name;

-- ============================================================================
-- SECTION 3: Aggregate Tables (Dynamic) - For Reporting
-- ============================================================================

-- School Performance Summary
CREATE OR REPLACE DYNAMIC TABLE AGG_SCHOOL_PERFORMANCE
    TARGET_LAG = '30 minutes'
    WAREHOUSE = KMD_WH
    COMMENT = 'Aggregated school performance metrics'
AS
SELECT 
    s.municipality_code,
    m.municipality_name,
    s.school_id,
    s.school_name,
    s.school_type,
    g.academic_year,
    COUNT(DISTINCT g.student_id) AS total_students_graded,
    COUNT(g.grade_record_id) AS total_grades,
    ROUND(AVG(g.grade_numeric), 2) AS avg_grade,
    ROUND(MEDIAN(g.grade_numeric), 2) AS median_grade,
    ROUND(STDDEV(g.grade_numeric), 2) AS stddev_grade,
    SUM(CASE WHEN g.is_passing THEN 1 ELSE 0 END) AS passing_count,
    ROUND(SUM(CASE WHEN g.is_passing THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1) AS passing_rate_pct,
    MAX(g.loaded_at) AS last_updated
FROM KMD_ANALYTICS.GOLD.FACT_GRADES g
JOIN KMD_ANALYTICS.GOLD.DIM_SCHOOLS s ON g.school_id = s.school_id
JOIN KMD_SCHOOLS.SHARED.DIM_MUNICIPALITIES m ON s.municipality_code = m.municipality_code
GROUP BY ALL;

-- Municipality Performance Summary
CREATE OR REPLACE DYNAMIC TABLE AGG_MUNICIPALITY_PERFORMANCE
    TARGET_LAG = '30 minutes'
    WAREHOUSE = KMD_WH
    COMMENT = 'Aggregated municipality performance metrics'
AS
SELECT 
    m.municipality_code,
    m.municipality_name,
    m.region,
    m.population,
    g.academic_year,
    COUNT(DISTINCT s.school_id) AS num_schools,
    COUNT(DISTINCT g.student_id) AS total_students,
    COUNT(g.grade_record_id) AS total_grades,
    ROUND(AVG(g.grade_numeric), 2) AS avg_grade,
    ROUND(SUM(CASE WHEN g.is_passing THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1) AS passing_rate_pct,
    MAX(g.loaded_at) AS last_updated
FROM KMD_ANALYTICS.GOLD.FACT_GRADES g
JOIN KMD_ANALYTICS.GOLD.DIM_SCHOOLS s ON g.school_id = s.school_id
JOIN KMD_SCHOOLS.SHARED.DIM_MUNICIPALITIES m ON s.municipality_code = m.municipality_code
GROUP BY ALL;

-- Subject Performance Summary
CREATE OR REPLACE DYNAMIC TABLE AGG_SUBJECT_PERFORMANCE
    TARGET_LAG = '30 minutes'
    WAREHOUSE = KMD_WH
    COMMENT = 'Aggregated subject performance metrics'
AS
SELECT 
    g.municipality_code,
    g.subject,
    g.subject_category,
    g.academic_year,
    g.term,
    COUNT(DISTINCT g.student_id) AS total_students,
    COUNT(g.grade_record_id) AS total_grades,
    ROUND(AVG(g.grade_numeric), 2) AS avg_grade,
    ROUND(MEDIAN(g.grade_numeric), 2) AS median_grade,
    MIN(g.grade_numeric) AS min_grade,
    MAX(g.grade_numeric) AS max_grade,
    ROUND(SUM(CASE WHEN g.is_passing THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1) AS passing_rate_pct,
    MAX(g.loaded_at) AS last_updated
FROM KMD_ANALYTICS.GOLD.FACT_GRADES g
GROUP BY ALL;

-- Budget Summary
CREATE OR REPLACE DYNAMIC TABLE AGG_BUDGET_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE = KMD_WH
    COMMENT = 'Aggregated budget metrics per school'
AS
SELECT 
    b.municipality_code,
    m.municipality_name,
    b.school_id,
    s.school_name,
    b.fiscal_year,
    SUM(b.budgeted_amount) AS total_budgeted,
    SUM(b.spent_amount) AS total_spent,
    SUM(b.budgeted_amount) - SUM(b.spent_amount) AS total_variance,
    ROUND((SUM(b.spent_amount) / NULLIF(SUM(b.budgeted_amount), 0)) * 100, 1) AS budget_utilization_pct,
    MAX(b.loaded_at) AS last_updated
FROM KMD_STAGING.STAGING.FACT_BUDGETS_STAGING b
LEFT JOIN KMD_STAGING.STAGING.DIM_SCHOOLS_STAGING s ON b.school_id = s.school_id
LEFT JOIN KMD_SCHOOLS.SHARED.DIM_MUNICIPALITIES m ON b.municipality_code = m.municipality_code
GROUP BY ALL;

-- ============================================================================
-- SECTION 4: Monitor Dynamic Tables
-- ============================================================================

-- Check dynamic table status
SHOW DYNAMIC TABLES IN SCHEMA KMD_ANALYTICS.GOLD;

-- View refresh history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'KMD_ANALYTICS.GOLD.DIM_SCHOOLS'
))
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;

-- Check lag status
SELECT 
    name,
    target_lag,
    refresh_mode,
    scheduling_state,
    last_refresh_time,
    data_timestamp
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE SCHEMA_NAME = 'GOLD'
ORDER BY name;

SELECT 'Dynamic tables created successfully!' AS status;
