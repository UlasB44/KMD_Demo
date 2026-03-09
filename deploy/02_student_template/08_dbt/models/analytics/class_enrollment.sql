{{
    config(
        alias='dbt_class_enrollment'
    )
}}

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
FROM {{ var('municipality') }}_DB.CLEAN.CLASSES c
LEFT JOIN {{ var('municipality') }}_DB.CLEAN.STUDENTS s 
    ON c.class_id = s.class_id AND s.is_active = TRUE
WHERE c.is_active = TRUE
GROUP BY c.class_id, c.class_name, c.grade, c.section, c.academic_year, c.max_students
