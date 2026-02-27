{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with grades as (
    select * from {{ ref('fct_grades') }}
)

select
    municipality_code,
    municipality_name,
    school_id,
    school_name,
    subject,
    term,
    grade_year,
    count(distinct student_id) as student_count,
    count(*) as grade_count,
    round(avg(grade_numeric), 2) as avg_grade,
    min(grade_numeric) as min_grade,
    max(grade_numeric) as max_grade,
    round(stddev(grade_numeric), 2) as grade_stddev,
    count(case when grade_numeric >= 7 then 1 end) as passing_count,
    round(count(case when grade_numeric >= 7 then 1 end)::float / nullif(count(*), 0) * 100, 1) as passing_rate_pct,
    count(case when grade_numeric >= 10 then 1 end) as excellent_count,
    round(count(case when grade_numeric >= 10 then 1 end)::float / nullif(count(*), 0) * 100, 1) as excellent_rate_pct,
    current_timestamp() as dbt_updated_at
from grades
group by 1, 2, 3, 4, 5, 6, 7
