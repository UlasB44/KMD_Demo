{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'teachers_raw') }}
),

cleaned as (
    select
        teacher_id,
        school_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        cpr_number,
        lower(trim(email)) as email,
        subject_specialty,
        employment_type,
        hire_date,
        coalesce(is_active, true) as is_active,
        _loaded_at,
        _source_file
    from source
    where teacher_id is not null
    qualify row_number() over (partition by teacher_id order by _loaded_at desc) = 1
)

select * from cleaned
