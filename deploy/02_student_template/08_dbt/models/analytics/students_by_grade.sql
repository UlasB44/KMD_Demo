{{
    config(
        alias='dbt_students_by_grade'
    )
}}

SELECT 
    c.grade,
    c.academic_year,
    COUNT(DISTINCT s.student_id) AS student_count,
    COUNT(DISTINCT CASE WHEN s.gender = 'M' THEN s.student_id END) AS male_count,
    COUNT(DISTINCT CASE WHEN s.gender = 'F' THEN s.student_id END) AS female_count,
    COUNT(DISTINCT CASE WHEN s.special_needs != 'None' THEN s.student_id END) AS special_needs_count,
    ROUND(AVG(DATEDIFF('year', s.birth_date, CURRENT_DATE())), 1) AS avg_age
FROM {{ var('municipality') }}_DB.CLEAN.STUDENTS s
JOIN {{ var('municipality') }}_DB.CLEAN.CLASSES c ON s.class_id = c.class_id
WHERE s.is_active = TRUE
GROUP BY c.grade, c.academic_year
