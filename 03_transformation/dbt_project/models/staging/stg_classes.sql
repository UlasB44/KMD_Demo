{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'classes_raw') }}
),

cleaned as (
    select
        class_id,
        school_id,
        trim(class_name) as class_name,
        grade_level,
        academic_year,
        teacher_id,
        room_number,
        max_students,
        _loaded_at,
        _source_file
    from source
    where class_id is not null
    qualify row_number() over (partition by class_id order by _loaded_at desc) = 1
)

select * from cleaned
