{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'grades_raw') }}
),

cleaned as (
    select
        grade_record_id,
        student_id,
        class_id,
        school_id,
        municipality_code,
        subject,
        academic_year,
        term,
        grade_value,
        case grade_value 
            when '12' then 12 
            when '10' then 10 
            when '7' then 7 
            when '4' then 4 
            when '02' then 2 
            when '00' then 0 
            when '-3' then -3 
            else null 
        end as grade_numeric,
        grade_date,
        teacher_comment,
        is_final,
        case 
            when grade_value in ('12', '10', '7', '4', '02') 
            then true 
            else false 
        end as is_passing,
        _loaded_at
    from source
    where grade_record_id is not null
)

select * from cleaned
