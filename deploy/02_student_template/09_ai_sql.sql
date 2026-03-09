-- ============================================================================
-- STUDENT EXERCISE - STEP 9: CORTEX AI SQL FUNCTIONS
-- ============================================================================
-- Replace {MUNICIPALITY} with your assigned municipality (e.g., ESBJERG)
-- 
-- Municipality Reference:
--   COPENHAGEN  -> Code: 101
--   AARHUS      -> Code: 751
--   ODENSE      -> Code: 461
--   AALBORG     -> Code: 851
--   ESBJERG     -> Code: 561
-- ============================================================================
-- Snowflake Cortex AI functions bring LLM capabilities directly into SQL
-- No API keys, no external services - runs natively in Snowflake
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE {MUNICIPALITY}_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- 1. AI_COMPLETE - Generate Text with LLMs
-- ============================================================================
-- Use cases: Generate reports, recommendations, explanations

-- Generate a student profile summary
SELECT 
    s.student_id,
    s.full_name,
    c.grade,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-8b',
        CONCAT(
            'Generate a brief 2-sentence student profile summary for: ',
            'Name: ', s.full_name, ', ',
            'Grade: ', c.grade, ', ',
            'Class: ', c.class_name, ', ',
            'Special Needs: ', COALESCE(s.special_needs, 'None'), '. ',
            'Keep it professional and concise.'
        )
    ) AS ai_profile_summary
FROM {MUNICIPALITY}_DB.CLEAN.STUDENTS s
JOIN {MUNICIPALITY}_DB.CLEAN.CLASSES c ON s.class_id = c.class_id
WHERE s.is_active = TRUE
LIMIT 5;

-- Generate class recommendations based on enrollment
SELECT 
    class_name,
    grade,
    enrolled_students,
    capacity,
    enrollment_status,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-8b',
        CONCAT(
            'As an education administrator, provide a brief 1-sentence recommendation for this class: ',
            'Class: ', class_name, ', ',
            'Grade: ', grade, ', ',
            'Enrolled: ', enrolled_students, ' students, ',
            'Capacity: ', capacity, ', ',
            'Status: ', enrollment_status, '. ',
            'Focus on actionable advice.'
        )
    ) AS ai_recommendation
FROM DT_CLASS_ENROLLMENT
ORDER BY enrollment_pct DESC
LIMIT 5;

-- ============================================================================
-- 2. AI_CLASSIFY - Categorize Data
-- ============================================================================
-- Use cases: Risk assessment, categorization, priority assignment

-- Classify teacher workload priority
SELECT 
    teacher_name,
    student_count,
    workload_level,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        CONCAT('Teacher has ', student_count, ' students and workload is ', workload_level),
        ['Needs immediate support', 'Monitor closely', 'Balanced workload', 'Underutilized']
    ):label::VARCHAR AS ai_priority_classification
FROM DT_TEACHER_WORKLOAD
LIMIT 10;

-- Classify special needs students by support level
SELECT 
    s.full_name,
    s.special_needs,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        COALESCE(s.special_needs, 'None'),
        ['No support needed', 'Minor accommodations', 'Moderate support', 'Intensive support']
    ):label::VARCHAR AS ai_support_level
FROM {MUNICIPALITY}_DB.CLEAN.STUDENTS s
WHERE s.is_active = TRUE
LIMIT 10;

-- ============================================================================
-- 3. AI_SENTIMENT - Analyze Text Sentiment
-- ============================================================================
-- Use cases: Feedback analysis, comment evaluation
-- Note: Our dataset doesn't have natural text, so we'll demonstrate with generated content

-- Analyze sentiment of generated teacher descriptions
SELECT 
    teacher_name,
    workload_level,
    years_of_service,
    CASE 
        WHEN workload_level = 'HIGH' AND years_of_service < 2 THEN 'New teacher struggling with heavy workload'
        WHEN workload_level = 'HIGH' AND years_of_service >= 5 THEN 'Experienced teacher handling challenging load well'
        WHEN workload_level = 'LOW' AND years_of_service < 2 THEN 'New teacher getting ramped up gradually'
        WHEN workload_level = 'MODERATE' THEN 'Teacher with balanced responsibilities'
        ELSE 'Teacher workload status unclear'
    END AS situation_description,
    SNOWFLAKE.CORTEX.SENTIMENT(
        CASE 
            WHEN workload_level = 'HIGH' AND years_of_service < 2 THEN 'New teacher struggling with heavy workload'
            WHEN workload_level = 'HIGH' AND years_of_service >= 5 THEN 'Experienced teacher handling challenging load well'
            WHEN workload_level = 'LOW' AND years_of_service < 2 THEN 'New teacher getting ramped up gradually'
            WHEN workload_level = 'MODERATE' THEN 'Teacher with balanced responsibilities'
            ELSE 'Teacher workload status unclear'
        END
    ) AS ai_sentiment_score
FROM DT_TEACHER_WORKLOAD
LIMIT 10;

-- ============================================================================
-- 4. AI_SUMMARIZE - Condense Information
-- ============================================================================
-- Use cases: Report generation, executive summaries

-- Summarize municipality statistics
SELECT 
    SNOWFLAKE.CORTEX.SUMMARIZE(
        CONCAT(
            'Municipality Education Report for ', municipality_name, ' (Code: ', municipality_code, '). ',
            'Total Students: ', total_students, '. ',
            'Total Teachers: ', total_teachers, '. ',
            'Total Classes: ', total_classes, '. ',
            'Student-Teacher Ratio: ', student_teacher_ratio, '. ',
            'Special Needs Students: ', special_needs_students, ' (', special_needs_pct, '%). ',
            'Capacity Utilization: ', capacity_utilization_pct, '%. ',
            'This data represents the current state of education infrastructure.'
        )
    ) AS ai_executive_summary
FROM DT_MUNICIPALITY_OVERVIEW;

-- ============================================================================
-- 5. AI_TRANSLATE - Multi-language Support
-- ============================================================================
-- Use cases: Danish/English translations for international reports

-- Translate class information to Danish
SELECT 
    class_name,
    enrollment_status,
    SNOWFLAKE.CORTEX.TRANSLATE(
        CONCAT('Class ', class_name, ' has ', enrolled_students, ' students enrolled. Status: ', enrollment_status),
        'en',
        'da'
    ) AS danish_translation
FROM DT_CLASS_ENROLLMENT
LIMIT 5;

-- Translate student summary to Danish
SELECT 
    s.full_name,
    SNOWFLAKE.CORTEX.TRANSLATE(
        CONCAT(s.full_name, ' is a student in grade ', c.grade, ' at our school.'),
        'en',
        'da'
    ) AS danish_description
FROM {MUNICIPALITY}_DB.CLEAN.STUDENTS s
JOIN {MUNICIPALITY}_DB.CLEAN.CLASSES c ON s.class_id = c.class_id
WHERE s.is_active = TRUE
LIMIT 5;

-- ============================================================================
-- 6. AI_EXTRACT_ANSWER - Question Answering
-- ============================================================================
-- Use cases: Query data using natural language questions

-- Extract specific information from text
SELECT 
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        CONCAT(
            'The municipality of ',
            (SELECT municipality_name FROM DT_MUNICIPALITY_OVERVIEW), ' has ', 
            (SELECT total_students FROM DT_MUNICIPALITY_OVERVIEW), ' students and ',
            (SELECT total_teachers FROM DT_MUNICIPALITY_OVERVIEW), ' teachers. ',
            'The student-teacher ratio is ',
            (SELECT student_teacher_ratio FROM DT_MUNICIPALITY_OVERVIEW), '. ',
            'There are ', (SELECT special_needs_students FROM DT_MUNICIPALITY_OVERVIEW), 
            ' students with special needs.'
        ),
        'How many students have special needs?'
    ) AS ai_answer;

-- ============================================================================
-- 7. PRACTICAL USE CASE: Automated Report Generation
-- ============================================================================
-- Generate a complete class report using AI

CREATE OR REPLACE VIEW V_AI_CLASS_REPORTS AS
SELECT 
    ce.class_id,
    ce.class_name,
    ce.grade,
    ce.enrolled_students,
    ce.capacity,
    ce.enrollment_pct,
    ce.enrollment_status,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-8b',
        CONCAT(
            'Write a brief professional report (3 sentences max) for school administrators about this class:\n',
            '- Class Name: ', ce.class_name, '\n',
            '- Grade Level: ', ce.grade, '\n',
            '- Current Enrollment: ', ce.enrolled_students, ' students\n',
            '- Maximum Capacity: ', ce.capacity, '\n',
            '- Enrollment Percentage: ', ce.enrollment_pct, '%\n',
            '- Status: ', ce.enrollment_status, '\n',
            'Include any concerns or recommendations.'
        )
    ) AS ai_generated_report
FROM DT_CLASS_ENROLLMENT ce;

-- Query the AI-enhanced view
SELECT * FROM V_AI_CLASS_REPORTS LIMIT 5;

-- ============================================================================
-- 8. PRACTICAL USE CASE: Student Risk Assessment
-- ============================================================================
-- Identify students who may need additional attention

CREATE OR REPLACE VIEW V_AI_STUDENT_RISK_ASSESSMENT AS
SELECT 
    s.student_id,
    s.full_name,
    c.grade,
    s.special_needs,
    s.enrollment_date,
    DATEDIFF('day', s.enrollment_date, CURRENT_DATE()) AS days_enrolled,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        CONCAT(
            'Student profile: ',
            'Special needs: ', COALESCE(s.special_needs, 'None'), '. ',
            'Days since enrollment: ', DATEDIFF('day', s.enrollment_date, CURRENT_DATE()), '. ',
            'Grade level: ', c.grade, '.'
        ),
        ['Low risk - thriving', 'Medium risk - monitor', 'High risk - intervention needed']
    ):label::VARCHAR AS ai_risk_level
FROM {MUNICIPALITY}_DB.CLEAN.STUDENTS s
JOIN {MUNICIPALITY}_DB.CLEAN.CLASSES c ON s.class_id = c.class_id
WHERE s.is_active = TRUE;

-- Query risk assessment
SELECT ai_risk_level, COUNT(*) AS student_count
FROM V_AI_STUDENT_RISK_ASSESSMENT
GROUP BY ai_risk_level;

-- ============================================================================
-- 9. COST AWARENESS
-- ============================================================================
-- AI functions consume credits. Monitor usage:

-- Check recent Cortex usage (requires ACCOUNTADMIN)
/*
USE ROLE ACCOUNTADMIN;
SELECT 
    start_time::DATE AS usage_date,
    service_type,
    SUM(credits_used) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
WHERE service_type LIKE '%CORTEX%'
AND start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC;
*/

-- ============================================================================
-- TIPS FOR PRODUCTION USE
-- ============================================================================
/*
1. BATCH PROCESSING: Process AI calls in batches to manage costs
   - Don't run AI functions on millions of rows at once
   - Use LIMIT during development, then scale up

2. CACHING: Store AI results in tables rather than computing repeatedly
   - Create a table to cache AI-generated content
   - Refresh periodically rather than on every query

3. MODEL SELECTION:
   - llama3.1-8b: Fast, cheap, good for simple tasks
   - llama3.1-70b: Better quality, more expensive
   - mistral-large2: Best for complex reasoning

4. PROMPT ENGINEERING:
   - Be specific about output format
   - Include examples when possible
   - Limit response length to control costs
*/

-- ============================================================================
-- VERIFY
-- ============================================================================
SHOW VIEWS LIKE 'V_AI%' IN SCHEMA {MUNICIPALITY}_DB.ANALYTICS;
