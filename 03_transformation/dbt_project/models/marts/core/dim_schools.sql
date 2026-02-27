{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with schools as (
    select * from {{ ref('int_schools_with_teachers') }}
),

students as (
    select * from {{ ref('stg_students') }}
),

student_counts as (
    select
        school_id,
        count(*) as total_students,
        count(case when is_active then 1 end) as active_students
    from students
    group by school_id
),

municipalities as (
    select
        municipality_code,
        municipality_name
    from {{ ref('seed_municipalities') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.school_id']) }} as school_key,
    s.school_id,
    s.municipality_code,
    m.municipality_name,
    s.school_name,
    s.school_type,
    s.address,
    s.postal_code,
    s.city,
    s.phone,
    s.email,
    s.founded_year,
    s.student_capacity,
    s.is_active,
    coalesce(sc.total_students, 0) as total_students,
    coalesce(sc.active_students, 0) as active_students,
    s.total_teachers,
    s.active_teachers,
    round(
        case when s.active_teachers > 0 
        then coalesce(sc.active_students, 0)::float / s.active_teachers 
        else 0 end, 1
    ) as student_teacher_ratio,
    round(
        case when s.student_capacity > 0 
        then coalesce(sc.active_students, 0)::float / s.student_capacity * 100 
        else 0 end, 1
    ) as capacity_utilization_pct,
    current_timestamp() as dbt_updated_at
from schools s
left join student_counts sc on s.school_id = sc.school_id
left join municipalities m on s.municipality_code = m.municipality_code
