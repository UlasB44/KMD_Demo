{{
    config(
        materialized='ephemeral'
    )
}}

with students as (
    select * from {{ ref('stg_students') }}
),

grades as (
    select * from {{ ref('stg_grades') }}
),

student_grade_stats as (
    select
        student_id,
        count(*) as total_grades,
        avg(
            case grade_value
                when '-3' then -3
                when '00' then 0
                when '02' then 2
                when '4' then 4
                when '7' then 7
                when '10' then 10
                when '12' then 12
            end
        ) as avg_grade,
        min(grade_date) as first_grade_date,
        max(grade_date) as last_grade_date
    from grades
    group by student_id
)

select
    s.*,
    coalesce(g.total_grades, 0) as total_grades,
    round(g.avg_grade, 2) as avg_grade,
    g.first_grade_date,
    g.last_grade_date
from students s
left join student_grade_stats g on s.student_id = g.student_id
