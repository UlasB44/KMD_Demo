{{
    config(
        materialized='table',
        unique_key='school_id'
    )
}}

with schools as (
    select * from {{ ref('stg_schools') }}
),

municipalities as (
    select * from {{ source('shared', 'dim_municipalities') }}
),

final as (
    select
        s.school_id,
        s.municipality_code,
        m.municipality_name,
        m.region,
        s.school_name,
        s.school_type,
        s.address,
        s.postal_code,
        s.city,
        s.phone,
        s.email,
        s.founded_year,
        year(current_date()) - s.founded_year as school_age_years,
        s.student_capacity,
        s.is_active,
        current_timestamp() as dbt_loaded_at
    from schools s
    left join municipalities m 
        on s.municipality_code = m.municipality_code
)

select * from final
