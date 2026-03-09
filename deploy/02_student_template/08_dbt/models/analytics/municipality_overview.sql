{{
    config(
        alias='dbt_municipality_overview'
    )
}}

SELECT 
    {{ var('municipality_code') }} AS municipality_code,
    '{{ var('municipality_name') }}' AS municipality_name,
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
FROM {{ var('municipality') }}_DB.CLEAN.STUDENTS s
FULL OUTER JOIN {{ var('municipality') }}_DB.CLEAN.TEACHERS t 
    ON s.municipality_code = t.municipality_code
FULL OUTER JOIN {{ var('municipality') }}_DB.CLEAN.CLASSES c 
    ON s.class_id = c.class_id
WHERE COALESCE(s.is_active, TRUE) = TRUE
