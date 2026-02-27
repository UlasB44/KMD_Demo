{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with teachers as (
    select * from {{ ref('stg_teachers') }}
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
    {{ dbt_utils.generate_surrogate_key(['t.teacher_id']) }} as teacher_key,
    t.teacher_id,
    t.school_id,
    s.school_name,
    m.municipality_code,
    m.municipality_name,
    t.first_name,
    t.last_name,
    t.full_name,
    t.cpr_number,
    t.email,
    t.subject_specialty,
    t.employment_type,
    t.hire_date,
    datediff('year', t.hire_date, current_date()) as years_employed,
    t.is_active,
    current_timestamp() as dbt_updated_at
from teachers t
left join schools s on t.school_id = s.school_id
left join municipalities m on s.municipality_code = m.municipality_code
