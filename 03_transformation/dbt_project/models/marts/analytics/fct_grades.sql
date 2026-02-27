{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with grades as (
    select * from {{ ref('stg_grades') }}
),

students as (
    select student_id, school_id, grade_level
    from {{ ref('stg_students') }}
),

schools as (
    select school_id, school_name, municipality_code
    from {{ ref('stg_schools') }}
),

municipalities as (
    select municipality_code, municipality_name
    from {{ ref('seed_municipalities') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['g.grade_id']) }} as grade_key,
    g.grade_id,
    g.student_id,
    st.school_id,
    s.school_name,
    m.municipality_code,
    m.municipality_name,
    g.class_id,
    g.subject,
    g.grade_value,
    case g.grade_value
        when '-3' then -3
        when '00' then 0
        when '02' then 2
        when '4' then 4
        when '7' then 7
        when '10' then 10
        when '12' then 12
    end as grade_numeric,
    g.grade_date,
    date_trunc('month', g.grade_date) as grade_month,
    year(g.grade_date) as grade_year,
    g.term,
    g.teacher_id,
    st.grade_level as student_grade_level,
    current_timestamp() as dbt_updated_at
from grades g
left join students st on g.student_id = st.student_id
left join schools s on st.school_id = s.school_id
left join municipalities m on s.municipality_code = m.municipality_code
