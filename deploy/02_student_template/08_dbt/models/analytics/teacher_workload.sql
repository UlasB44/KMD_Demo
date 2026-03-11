{{
    config(
        alias='dbt_teacher_workload'
    )
}}

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
FROM {{ var('source_database') }}.CLEAN.TEACHERS t
LEFT JOIN {{ var('source_database') }}.CLEAN.CLASSES c ON t.school_id = c.school_id
LEFT JOIN {{ var('source_database') }}.CLEAN.STUDENTS s 
    ON c.class_id = s.class_id AND s.is_active = TRUE
WHERE t.is_active = TRUE
GROUP BY t.teacher_id, t.full_name, t.subjects, t.hire_date, t.salary_band
