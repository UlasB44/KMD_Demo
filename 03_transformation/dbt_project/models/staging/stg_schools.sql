{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'schools_raw') }}
),

cleaned as (
    select
        school_id,
        municipality_code,
        trim(school_name) as school_name,
        school_type,
        address,
        postal_code,
        city,
        phone,
        lower(email) as email,
        founded_year,
        student_capacity,
        coalesce(is_active, true) as is_active,
        created_at as source_created_at,
        updated_at as source_updated_at,
        _loaded_at,
        _source_file
    from source
    where school_id is not null
    qualify row_number() over (partition by school_id order by _loaded_at desc) = 1
)

select * from cleaned
