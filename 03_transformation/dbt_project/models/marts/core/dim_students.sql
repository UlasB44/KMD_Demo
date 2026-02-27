{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with students as (
    select * from {{ ref('int_students_with_grades') }}
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
    {{ dbt_utils.generate_surrogate_key(['st.student_id']) }} as student_key,
    st.student_id,
    st.school_id,
    s.school_name,
    m.municipality_code,
    m.municipality_name,
    st.class_id,
    st.first_name,
    st.last_name,
    st.full_name,
    st.cpr_number,
    st.birth_date,
    datediff('year', st.birth_date, current_date()) as current_age,
    st.enrollment_date,
    st.grade_level,
    st.is_active,
    st.total_grades,
    st.avg_grade,
    case 
        when st.avg_grade >= 10 then 'Excellent'
        when st.avg_grade >= 7 then 'Good'
        when st.avg_grade >= 4 then 'Average'
        when st.avg_grade >= 2 then 'Below Average'
        else 'Needs Improvement'
    end as performance_category,
    current_timestamp() as dbt_updated_at
from students st
left join schools s on st.school_id = s.school_id
left join municipalities m on s.municipality_code = m.municipality_code
