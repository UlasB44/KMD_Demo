/*
=============================================================================
KMD Denmark - Semantic View Setup for Cortex Analyst
Creates semantic view from YAML definition
=============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE KMD_ANALYTICS;
USE SCHEMA SEMANTIC;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SECTION 1: Create Stage for Semantic Model YAML
-- ============================================================================

CREATE OR REPLACE STAGE SEMANTIC_MODEL_STAGE
    COMMENT = 'Stage for semantic model YAML files';

-- Upload the YAML file to the stage
-- PUT file:///path/to/kmd_schools_semantic_model.yaml @SEMANTIC_MODEL_STAGE;

-- ============================================================================
-- SECTION 2: Create Semantic View (alternative to YAML approach)
-- ============================================================================

-- Note: Semantic Views can be created via SQL or by uploading YAML
-- This SQL approach creates equivalent functionality

-- Create a view that serves as the semantic layer
CREATE OR REPLACE SECURE VIEW V_SCHOOL_ANALYTICS AS
SELECT 
    s.school_id,
    s.school_name,
    s.school_type,
    s.municipality_code,
    s.municipality_name,
    s.region,
    s.city,
    s.student_capacity,
    s.school_age_years,
    s.is_active,
    st.student_id,
    st.full_name AS student_name,
    st.gender_name,
    st.grade AS student_grade,
    st.class_name,
    st.age_group,
    st.has_special_needs,
    g.grade_record_id,
    g.subject,
    g.subject_category,
    g.academic_year,
    g.term,
    g.grade_value,
    g.grade_numeric,
    g.grade_name,
    g.is_final,
    g.is_passing,
    g.grade_date
FROM KMD_ANALYTICS.GOLD.DIM_SCHOOLS s
LEFT JOIN KMD_ANALYTICS.GOLD.DIM_STUDENTS st ON s.school_id = st.school_id
LEFT JOIN KMD_ANALYTICS.GOLD.FACT_GRADES g ON st.student_id = g.student_id
WHERE s.is_active = TRUE;

-- Create aggregated view for performance queries
CREATE OR REPLACE SECURE VIEW V_SCHOOL_PERFORMANCE_SUMMARY AS
SELECT 
    municipality_code,
    municipality_name,
    school_id,
    school_name,
    school_type,
    academic_year,
    total_students_graded,
    total_grades,
    avg_grade,
    median_grade,
    stddev_grade,
    passing_count,
    passing_rate_pct,
    last_updated
FROM KMD_ANALYTICS.GOLD.AGG_SCHOOL_PERFORMANCE;

-- Create view for municipality comparison
CREATE OR REPLACE SECURE VIEW V_MUNICIPALITY_COMPARISON AS
SELECT 
    municipality_code,
    municipality_name,
    region,
    population,
    academic_year,
    num_schools,
    total_students,
    total_grades,
    avg_grade,
    passing_rate_pct,
    last_updated
FROM KMD_ANALYTICS.GOLD.AGG_MUNICIPALITY_PERFORMANCE;

-- ============================================================================
-- SECTION 3: Grant Access to Views
-- ============================================================================

GRANT SELECT ON VIEW V_SCHOOL_ANALYTICS TO ROLE KMD_ANALYST;
GRANT SELECT ON VIEW V_SCHOOL_PERFORMANCE_SUMMARY TO ROLE KMD_ANALYST;
GRANT SELECT ON VIEW V_MUNICIPALITY_COMPARISON TO ROLE KMD_ANALYST;

-- ============================================================================
-- SECTION 4: Test Queries for Cortex Analyst
-- ============================================================================

-- These queries demonstrate what Cortex Analyst should be able to answer:

-- Q: What is the average grade in Copenhagen?
SELECT 
    municipality_name,
    ROUND(AVG(grade_numeric), 2) AS avg_grade
FROM V_SCHOOL_ANALYTICS
WHERE municipality_name = 'Copenhagen'
  AND grade_numeric IS NOT NULL
GROUP BY municipality_name;

-- Q: Which schools have the highest passing rates?
SELECT 
    school_name,
    municipality_name,
    avg_grade,
    passing_rate_pct
FROM V_SCHOOL_PERFORMANCE_SUMMARY
ORDER BY passing_rate_pct DESC
LIMIT 10;

-- Q: How many students are in each municipality?
SELECT 
    municipality_name,
    COUNT(DISTINCT student_id) AS student_count
FROM V_SCHOOL_ANALYTICS
GROUP BY municipality_name
ORDER BY student_count DESC;

-- Q: What is the performance by subject?
SELECT 
    subject,
    subject_category,
    ROUND(AVG(grade_numeric), 2) AS avg_grade,
    COUNT(*) AS total_grades
FROM V_SCHOOL_ANALYTICS
WHERE grade_numeric IS NOT NULL
GROUP BY subject, subject_category
ORDER BY avg_grade DESC;

SELECT 'Semantic views created successfully!' AS status;
