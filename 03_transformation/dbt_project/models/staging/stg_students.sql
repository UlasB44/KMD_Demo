{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'students_raw') }}
),

cleaned as (
    select
        student_id,
        class_id,
        school_id,
        municipality_code,
        cpr_masked as cpr_number,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        gender,
        birth_date,
        datediff('year', birth_date, current_date()) as age,
        enrollment_date,
        guardian_name,
        guardian_phone,
        lower(guardian_email) as guardian_email,
        address,
        postal_code,
        special_needs,
        case 
            when special_needs is not null and special_needs != 'Ingen' 
            then true 
            else false 
        end as has_special_needs,
        coalesce(is_active, true) as is_active,
        _loaded_at
    from source
    where student_id is not null
    qualify row_number() over (partition by student_id order by _loaded_at desc) = 1
)

select * from cleaned
